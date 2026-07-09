//! Environment-backed configuration for OpenPlotva.

use std::{
    io,
    num::{ParseFloatError, ParseIntError, TryFromIntError},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DEFAULT_WEBAPP_HOST: &str = "0.0.0.0";

pub const DEFAULT_WEBAPP_PORT: u16 = 8080;

pub const DEFAULT_WEBAPP_URL: &str = "http://127.0.0.1:8080";

/// Soft-cutover default: settings WebApp requests without `initData` fall through to the
/// legacy signature check until the new front-end ships. Flip to harden (reject missing data).
pub const DEFAULT_SETTINGS_REQUIRE_INIT_DATA: bool = false;

pub const DEFAULT_RUNTIME_API_ENABLED: bool = true;

pub const DEFAULT_RUNTIME_API_HOST: &str = "127.0.0.1";

pub const DEFAULT_RUNTIME_API_PORT: u16 = 9091;

pub const DEFAULT_RUNTIME_API_LOG_BUFFER_SIZE: i32 = 200;

pub const DEFAULT_RUNTIME_API_SQL_TIMEOUT_MS: i32 = 10_000;

pub const DEFAULT_RUNTIME_API_SQL_ROW_LIMIT: i32 = 200;

pub const DEFAULT_RUNTIME_API_SQL_RESULT_BYTES_LIMIT: i32 = 2_621_440;

pub const DEFAULT_LLM_REQUEST_EVENTS_RETENTION_DAYS: i32 = 14;

pub const DEFAULT_LLM_RUN_BUFFER_CAPACITY: i32 = 512;

pub const DEFAULT_LLM_RAW_BODY_PERSIST_ENABLED: bool = true;

pub const DEFAULT_LLM_RAW_BODY_MAX_BYTES: i32 = 65_536;

pub const DEFAULT_LLM_RAW_BODY_RETENTION_HOURS: i32 = 48;

pub const DEFAULT_CHAT_HISTORY_RETENTION_DAYS: i32 = 8;

pub const DEFAULT_TELEGRAM_FILES_RETENTION_DAYS: i32 = 7;

pub const DEFAULT_WHITECIRCLE_CHECKS_RETENTION_DAYS: i32 = 30;

pub const DEFAULT_RUNTIME_API_CERT_FILE: &str = "";

pub const DEFAULT_RUNTIME_API_KEY_FILE: &str = "";

pub const DEFAULT_RUNTIME_API_TLS_PUBLIC_KEY_PIN: &str = "";

pub const DEFAULT_LOG_LEVEL: &str = "info";

/// Default tracing filter for local development.
pub const DEFAULT_LOG_FILTER: &str = "openplotva=info,tower_http=info";

pub const DEFAULT_POSTGRES_HOST: &str = "127.0.0.1";

pub const DEFAULT_POSTGRES_PORT: u16 = 5432;

pub const DEFAULT_POSTGRES_USER: &str = "plotva";

pub const DEFAULT_POSTGRES_PASSWORD: &str = "plotva";

pub const DEFAULT_POSTGRES_DATABASE: &str = "plotva";

pub const DEFAULT_POSTGRES_SSL_MODE: &str = "disable";

pub const STARTUP_POSTGRES_SSL_MODE: &str = "disable";

pub const DEFAULT_REDIS_HOST: &str = "127.0.0.1";

pub const DEFAULT_REDIS_PORT: u16 = 6379;

pub const DEFAULT_REDIS_DB: i64 = 0;

pub const DEFAULT_UPDATE_QUEUE_BACKEND: &str = "list";

pub const DEFAULT_CONNECT_SERVICES: bool = true;

pub const DEFAULT_RUN_MIGRATIONS: bool = false;

pub const DEFAULT_CONSUME_UPDATES: bool = true;

/// Telegram update producer stays on by default when services and `BOT_KEY` are connected.
pub const DEFAULT_PRODUCE_UPDATES: bool = true;

pub const DEFAULT_BOT_DEBUG: bool = false;

pub const DEFAULT_BOT_WEBHOOK_ENABLED: bool = false;

pub const DEFAULT_BOT_WEBHOOK_UPDATE_BUFFER_SIZE: usize = 10_000;

pub const DEFAULT_TELEGRAM_ACTIVITY_PULSE_ENABLED: bool = true;

/// Product-tuned Telegram chat-action cadence: one Bot API action every 8s.
pub const DEFAULT_TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS: i32 = 8_000;

pub const DEFAULT_TELEGRAM_ACTIVITY_PULSE_INITIAL_DELAY_MS: i32 = 0;

pub const MIN_TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS: i32 = 1_000;

pub const DEFAULT_ADMINS_ADMIN_IDS: &str = "";

pub const DEFAULT_VIP_CHAT_ID: i64 = -1001998670656;

pub const DEFAULT_SUBSCRIPTION_SYNC_ENABLED: bool = true;

pub const DEFAULT_SUBSCRIPTION_SYNC_INTERVAL_SECONDS: i32 = 3_600;

pub const DEFAULT_SUBSCRIPTION_SYNC_DRY_RUN: bool = false;

pub const DEFAULT_SUBSCRIPTION_SYNC_PAGE_LIMIT: i32 = 100;

pub const DEFAULT_PERSISTENT_QUEUE_ENABLED: bool = true;

pub const DEFAULT_PERSISTENT_QUEUE_HEARTBEAT_INTERVAL_SECONDS: i32 = 30;

pub const DEFAULT_PERSISTENT_QUEUE_RECOVERY_INTERVAL_SECONDS: i32 = 60;

pub const DEFAULT_PERSISTENT_QUEUE_CLEANUP_INTERVAL_SECONDS: i32 = 300;

pub const DEFAULT_PERSISTENT_QUEUE_DEFAULT_PROCESSING_TIMEOUT_SECONDS: i32 = 300;

pub const DEFAULT_PERSISTENT_QUEUE_MAX_RETRIES: i32 = 3;

pub const DEFAULT_PERSISTENT_QUEUE_COMPLETED_JOB_RETENTION_DAYS: i32 = 1;

pub const DEFAULT_PERSISTENT_QUEUE_MESSAGE_CLEANUP_INTERVAL_SECONDS: i32 = 300;

pub const DEFAULT_PERSISTENT_QUEUE_JOB_MESSAGE_CLEANUP_MINUTES: i32 = 30;

pub const DEFAULT_PERSISTENT_QUEUE_TEXT_WORKERS: i32 = 4;

pub const DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_WORKERS: i32 = 2;

pub const DEFAULT_PERSISTENT_QUEUE_DIALOG_WORKERS_CAP: i32 = 24;

pub const DEFAULT_PERSISTENT_QUEUE_DIALOG_UNPOOLED_SHARE: i32 = 2;

pub const DEFAULT_PERSISTENT_QUEUE_MEMORY_WORKERS_CAP: i32 = 8;

pub const DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_HIGH_WATERMARK: i32 = 30;

pub const DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_LOW_WATERMARK: i32 = 20;

pub const DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_POLL_INTERVAL_SECONDS: i32 = 1;

pub const DEFAULT_PERSISTENT_QUEUE_CONTROL_WORKERS: i32 = 2;

pub const DEFAULT_PERSISTENT_QUEUE_IMAGE_REGULAR_WORKERS: i32 = 1;

pub const DEFAULT_PERSISTENT_QUEUE_IMAGE_VIP_WORKERS: i32 = 1;

pub const DEFAULT_PERSISTENT_QUEUE_MUSIC_VIP_WORKERS: i32 = 1;

pub const DEFAULT_PERSISTENT_QUEUE_MEMORY_CONSOLIDATION_WORKERS: i32 = 1;

pub const DEFAULT_PERSISTENT_QUEUE_PLACEHOLDER_CLEANUP_INTERVAL_SECONDS: i32 = 3600;

pub const DEFAULT_PERSISTENT_QUEUE_PLACEHOLDER_MAX_AGE_SECONDS: i32 = 7200;

pub const DEFAULT_LLM_JOB_MAX_ATTEMPTS: i32 = 5;

pub const DEFAULT_MARKET_RATES_TIMEOUT_SECONDS: i32 = 15;

pub const DEFAULT_SERPER_TIMEOUT_SECONDS: i32 = 30;

pub const DEFAULT_DISCOVERY_BASE_URL: &str = "http://127.0.0.1:50051";

pub const DEFAULT_DIALOG_PROVIDER: &str = "aifarm";

pub const DEFAULT_DIALOG_FALLBACK_PROVIDER: &str = "genkit";

pub const DEFAULT_DIALOG_MODEL: &str = "Gemma 4 26B Heretic";

pub const DEFAULT_DIALOG_API_KEY: &str = "";

pub const DEFAULT_OPENROUTER_REQUEST_TIMEOUT_SECONDS: i32 = 300;

pub const DEFAULT_DIALOG_DISCOVERY_SERVICE_NAME: &str = "llm-openai";

pub const DEFAULT_DIALOG_DISCOVERY_ENDPOINT_NAME: &str = "chat_completions";

pub const DEFAULT_DIALOG_AIFARM_POOL_MODELS: &str = "";

pub const DEFAULT_DIALOG_AIFARM_POOL_BASE_URLS: &str = "";

pub const DEFAULT_LLM_PROVIDER_KIND: &str = "aifarm";

pub const DEFAULT_LLM_PROVIDER_MAX_TOKENS: i32 = 8192;

pub const DEFAULT_LLM_PROVIDER_TASK_TIMEOUT_SECONDS: i32 = 600;

pub const DEFAULT_AGENTIC_SONG_ENABLED: bool = true;

pub const DEFAULT_AGENTIC_IMAGE_ENABLED: bool = true;

pub const DEFAULT_AGENT_REASONER_PROVIDER: &str = "qwen-reasoner";

pub const DEFAULT_VISION_DISCOVERY_SERVICE_NAME: &str = DEFAULT_DIALOG_DISCOVERY_SERVICE_NAME;

pub const DEFAULT_VISION_DISCOVERY_ENDPOINT_NAME: &str = DEFAULT_DIALOG_DISCOVERY_ENDPOINT_NAME;

pub const DEFAULT_VISION_MODEL: &str = DEFAULT_DIALOG_MODEL;

pub const DEFAULT_VISION_MAX_TOKENS: i32 = 768;

pub const DEFAULT_VISION_TEMPERATURE: f64 = 0.1;

pub const DEFAULT_VISION_DIRECT_IMAGE_LIMIT: i32 = 2;

pub const DEFAULT_VISION_REQUEST_TIMEOUT_SECONDS: i32 = 120;

pub const DEFAULT_BOOGU_IMAGE_TURBO_ENABLED: bool = false;

pub const DEFAULT_BOOGU_IMAGE_TURBO_BASE_URL: &str = "https://demo-turbo.boogu.org";

pub const DEFAULT_BOOGU_IMAGE_EDIT_TURBO_ENABLED: bool = false;

pub const DEFAULT_BOOGU_IMAGE_EDIT_TURBO_BASE_URL: &str = "https://demo-edit-turbo-1k5.boogu.org";

pub const DEFAULT_BOOGU_IMAGE_TIMEOUT_SECONDS: i32 = 600;

pub const DEFAULT_BOOGU_IMAGE_TURBO_STEPS: i32 = 3;

pub const DEFAULT_BOOGU_IMAGE_TURBO_RESOLUTION: &str = "1024x1024 ( 1:1 )";

pub const DEFAULT_BOOGU_IMAGE_TURBO_WIDTH: i32 = 1024;

pub const DEFAULT_BOOGU_IMAGE_TURBO_HEIGHT: i32 = 1024;

pub const DEFAULT_BOOGU_IMAGE_EDIT_TURBO_STEPS: i32 = 3;

pub const DEFAULT_BOOGU_IMAGE_EDIT_TURBO_RESOLUTION_CATEGORY: &str = "1.5K";

pub const DEFAULT_BOOGU_IMAGE_EDIT_TURBO_RESOLUTION: &str = "1536x1536 ( 1:1 )";

pub const DEFAULT_BOOGU_IMAGE_EDIT_TURBO_WIDTH: i32 = 1536;

pub const DEFAULT_BOOGU_IMAGE_EDIT_TURBO_HEIGHT: i32 = 1536;

pub const DEFAULT_ACESTEP_ENABLED: bool = false;

pub const DEFAULT_ACESTEP_BASE_URL: &str = "http://127.0.0.1:8001";

pub const DEFAULT_ACESTEP_API_MODE: &str = "completion";

pub const DEFAULT_ACESTEP_REQUEST_TIMEOUT_SECONDS: i32 = 90;

pub const DEFAULT_ACESTEP_POLL_INTERVAL_SECONDS: i32 = 2;

pub const DEFAULT_ACESTEP_TASK_TIMEOUT_SECONDS: i32 = 600;

pub const DEFAULT_ACESTEP_AUDIO_FORMAT: &str = "mp3";

pub const DEFAULT_ACESTEP_MODEL: &str = "acemusic/acestep-v1.5-turbo";

/// Default media uploader request timeout, `PLOTVA_UPLOADER_TIMEOUT_SECONDS`.
pub const DEFAULT_UPLOADER_TIMEOUT_SECONDS: i32 = 120;

pub const DEFAULT_DIALOG_NVIDIA_URL: &str = "https://integrate.api.nvidia.com/v1/chat/completions";

pub const DEFAULT_DIALOG_NVIDIA_MODEL: &str = "google/gemma-4-31b-it";

pub const DEFAULT_DIALOG_VMLX_URL: &str = "";

pub const DEFAULT_DIALOG_VMLX_API_KEY: &str = "";

pub const DEFAULT_DIALOG_VMLX_MODEL: &str = "default";

pub const DEFAULT_HISTORY_SUMMARY_PROVIDER: &str = "aifarm";

pub const DEFAULT_HISTORY_SUMMARY_TIMEOUT_SECONDS: i32 = 600;

pub const DEFAULT_MEMORY_CONSOLIDATION_PROVIDER: &str = "aifarm";

pub const DEFAULT_MEMORY_CONSOLIDATION_MODEL: &str = "Gemma 4 26B Heretic";

pub const DEFAULT_MEMORY_MIN_MESSAGES_PER_RUN: i32 = 20;

pub const DEFAULT_MEMORY_MAX_QUEUED_RUNS: i32 = 5_000;

pub const DEFAULT_MEMORY_MAX_DAILY_ENQUEUED_RUNS: i32 = 2_000;

pub const DEFAULT_MEMORY_TOKENIZER_MODEL: &str = "google/gemma-4-26B-A4B-it";

pub const DEFAULT_MEMORY_TOKEN_ESTIMATOR_URL: &str = "http://token-estimator:12600";

/// Discovery service name the embedder registers under on the AI Farm.
pub const DEFAULT_EMBEDDER_DISCOVERY_SERVICE_NAME: &str = "embedder";
/// Discovery endpoint name the embedder exposes (`POST /encode`).
pub const DEFAULT_EMBEDDER_DISCOVERY_ENDPOINT_NAME: &str = "encode";

pub const DEFAULT_MEMORY_EMBEDDING_MODEL: &str = "jinaai/jina-embeddings-v5-text-nano";

pub const DEFAULT_MEMORY_REDACTION_CATEGORIES: &str =
    "account_number,private_date,private_email,private_phone,private_url,secret";

pub const DEFAULT_SHIELD_ENABLED: bool = true;

pub const DEFAULT_SHIELD_EMBEDDING_DIM: i32 = 512;

pub const DEFAULT_SHIELD_MAX_MATCHES: i32 = 3;

pub const DEFAULT_SHIELD_VECTOR_MIN_SCORE: f64 = 0.48;

pub const DEFAULT_SHIELD_LEXICAL_MIN_SCORE: f64 = 0.08;

pub const DEFAULT_SHIELD_QUERY_MAX_CHARS: i32 = 4000;

pub const DEFAULT_SHIELD_RETRIEVAL_TIMEOUT_SECONDS: i32 = 6;

pub const DEFAULT_SHIELD_HISTORY_TAIL_MESSAGES: i32 = 0;

/// Top-level application configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AppConfig {
    /// HTTP server configuration.
    pub server: ServerConfig,
    /// Runtime diagnostic API configuration.
    pub runtime_api: RuntimeApiConfig,
    /// Logging and tracing configuration.
    pub observability: ObservabilityConfig,
    /// Postgres configuration.
    pub database: DatabaseConfig,
    /// Redis/Dragonfly configuration.
    pub redis: RedisConfig,
    /// Telegram update queue backend configuration.
    pub update_queue: UpdateQueueConfig,
    /// Telegram bot configuration.
    pub bot: BotConfig,
    /// Telegram administrator configuration.
    pub admins: AdminConfig,
    /// VIP status configuration.
    pub vip: VipConfig,
    /// Telegram Stars subscription reconciliation configuration.
    pub subscription_sync: SubscriptionSyncConfig,
    /// Persistent task queue configuration.
    pub persistent_queue: PersistentQueueConfig,
    /// Market rates provider configuration.
    pub market_rates: MarketRatesConfig,
    /// Serper web search provider configuration.
    pub serper: SerperConfig,
    /// Translation provider configuration.
    pub translation: TranslationConfig,
    pub google_ai: GoogleAiConfig,
    pub open_router: OpenRouterConfig,
    /// Media uploader (plotva.geta.moe) configuration.
    pub uploader: UploaderConfig,
    pub white_circle: WhiteCircleConfig,
    /// Discovery/dialog LLM configuration.
    pub llm: LlmConfig,
    /// Vision captioning and direct-image configuration.
    pub vision: VisionConfig,
    /// External image provider add-ons.
    pub image_providers: ImageProvidersConfig,
    /// Music and ACE-Step configuration.
    pub music: MusicConfig,
    /// Memory pipeline configuration.
    pub memory: MemoryConfig,
    /// Shield retrieval configuration.
    pub shield: ShieldConfig,
    /// Runtime service-probe configuration.
    pub service_probe: ServiceProbeConfig,
}

/// HTTP server configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Host the HTTP server should bind, from `WEBAPP_HOST`.
    pub host: String,
    /// Port the HTTP server should bind, from `WEBAPP_PORT`.
    pub port: u16,
    /// Full bind address assembled from host/port or the Rust-only local override.
    pub bind_addr: String,
    /// Public WebApp URL, from `WEBAPP_URL`.
    pub url: String,
    /// Reject settings WebApp requests without Telegram `initData`, from
    /// `SETTINGS_REQUIRE_INIT_DATA`. Soft cutover: defaults to false (log and fall through).
    pub require_settings_init_data: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeApiConfig {
    /// Whether the runtime API service is enabled, from `RUNTIME_API_ENABLED`.
    pub enabled: bool,
    /// Runtime API bind host, from `RUNTIME_API_HOST`.
    pub host: String,
    /// Runtime API bind port, from `RUNTIME_API_PORT`.
    pub port: u16,
    /// In-memory log ring size, from `RUNTIME_API_LOG_BUFFER_SIZE`.
    pub log_buffer_size: i32,
    /// Diagnostic SQL timeout ceiling, from `RUNTIME_API_SQL_TIMEOUT_MS`.
    pub sql_timeout_ms: i32,
    /// Diagnostic SQL row ceiling, from `RUNTIME_API_SQL_ROW_LIMIT`.
    pub sql_row_limit: i32,
    /// Diagnostic SQL result byte ceiling, from `RUNTIME_API_SQL_RESULT_BYTES_LIMIT`.
    pub sql_result_bytes_limit: i32,
    /// Raw LLM request telemetry retention in days, from `LLM_REQUEST_EVENTS_RETENTION_DAYS`.
    pub llm_request_events_retention_days: i32,
    /// Agent-run ring capacity for the admin LLM Dialogs section, from `LLM_RUN_BUFFER_CAPACITY`.
    pub llm_run_buffer_capacity: i32,
    /// Whether raw LLM bodies are persisted for the detail view, from `LLM_RAW_BODY_PERSIST_ENABLED`.
    pub llm_raw_body_persist_enabled: bool,
    /// Per-body persistence size cap in bytes, from `LLM_RAW_BODY_MAX_BYTES`.
    pub llm_raw_body_max_bytes: i32,
    /// Hours before persisted raw bodies are scrubbed to NULL, from `LLM_RAW_BODY_RETENTION_HOURS`.
    pub llm_raw_body_retention_hours: i32,
    /// Optional certificate PEM file for the runtime TLS listener, from `RUNTIME_API_CERT_FILE`.
    pub cert_file: String,
    /// Optional private key PEM file for the runtime TLS listener, from `RUNTIME_API_KEY_FILE`.
    pub key_file: String,
    /// Optional operator-provided pin hint, from `RUNTIME_API_TLS_PUBLIC_KEY_PIN`.
    pub tls_public_key_pin: String,
}

/// Logging and tracing configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub log_level: String,
    /// Tracing subscriber filter expression.
    pub log_filter: String,
}

/// Database configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Postgres configuration.
    pub postgres: PostgresConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpdateQueueConfig {
    /// Telegram update queue backend, from `UPDATE_QUEUE_BACKEND`.
    pub backend: String,
}

/// Postgres configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// Postgres host.
    pub host: String,
    /// Postgres port.
    pub port: u16,
    /// Postgres user.
    pub user: String,
    /// Postgres password.
    pub password: String,
    /// Postgres database name.
    pub database: String,
    /// Configured SSL mode.
    pub ssl_mode: String,
}

impl PostgresConfig {
    pub fn startup_dsn(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode={}",
            self.user,
            self.password,
            self.host,
            self.port,
            self.database,
            STARTUP_POSTGRES_SSL_MODE
        )
    }
}

/// Redis/Dragonfly configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Redis host.
    pub host: String,
    /// Redis port.
    pub port: u16,
    /// Redis password.
    pub password: String,
    /// Redis DB number.
    pub db: i64,
}

/// Telegram bot configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BotConfig {
    pub key: Option<String>,
    /// Optional Telegram Bot API base URL for local loopback proof.
    pub api_base_url: String,
    pub webhook: BotWebhookConfig,
    pub activity_pulse: TelegramActivityPulseConfig,
    pub debug: bool,
}

/// Telegram chat-action pulse configuration for long-running active jobs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelegramActivityPulseConfig {
    pub enabled: bool,
    pub interval_ms: i32,
    pub initial_delay_ms: i32,
}

/// Telegram webhook configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BotWebhookConfig {
    /// Whether Telegram webhook mode is enabled, from `BOT_WEBHOOK_ENABLED`.
    pub enabled: bool,
    /// Public webhook URL, from `BOT_WEBHOOK_URL`.
    pub url: String,
    /// Certificate file path, from `BOT_WEBHOOK_CERT_FILE`.
    pub cert_file: String,
    /// Key file path, from `BOT_WEBHOOK_KEY_FILE`.
    pub key_file: String,
    /// Secret-token header value, from `BOT_WEBHOOK_SECRET_TOKEN`.
    pub secret_token: String,
    /// In-memory webhook update channel capacity, from `BOT_WEBHOOK_UPDATE_BUFFER_SIZE`.
    pub update_buffer_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdminConfig {
    pub admin_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VipConfig {
    /// Telegram chat whose active members receive VIP status, from `VIP_CHAT_ID` or legacy `CHAT_ID`.
    pub chat_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionSyncConfig {
    pub enabled: bool,
    pub interval_seconds: i32,
    pub dry_run: bool,
    pub page_limit: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistentQueueConfig {
    /// Whether persistent task queues are enabled, from `PERSISTENT_QUEUE_ENABLED`.
    pub enabled: bool,
    /// Heartbeat interval in seconds, from `PERSISTENT_QUEUE_HEARTBEAT_INTERVAL_SECONDS`.
    pub heartbeat_interval_seconds: i32,
    /// Recovery interval in seconds, from `PERSISTENT_QUEUE_RECOVERY_INTERVAL_SECONDS`.
    pub recovery_interval_seconds: i32,
    /// Cleanup interval in seconds, from `PERSISTENT_QUEUE_CLEANUP_INTERVAL_SECONDS`.
    pub cleanup_interval_seconds: i32,
    /// Default processing timeout in seconds.
    pub default_processing_timeout_seconds: i32,
    /// Generic non-LLM retry count, from `PERSISTENT_QUEUE_MAX_RETRIES`.
    pub max_retries: i32,
    /// Completed-job retention in days, from `PERSISTENT_QUEUE_COMPLETED_JOB_RETENTION_DAYS`.
    pub completed_job_retention_days: i32,
    /// Message cleanup interval in seconds, from `PERSISTENT_QUEUE_MESSAGE_CLEANUP_INTERVAL_SECONDS`.
    pub message_cleanup_interval_seconds: i32,
    /// Job-message cleanup age in minutes, from `PERSISTENT_QUEUE_JOB_MESSAGE_CLEANUP_MINUTES`.
    pub job_message_cleanup_minutes: i32,
    /// Worker count for the `control` queue.
    pub control_workers: i32,
    /// Worker count for the `text` queue.
    pub text_workers: i32,
    /// Worker count for the `dialog-aifarm` queue. With capacity pools active
    /// this is only the fallback when the routing table has no dialog route.
    pub dialog_aifarm_workers: i32,
    /// Upper bound for the pool-derived dialog worker count.
    pub dialog_workers_cap: i32,
    /// Slot contribution of an unpooled/unlimited-pool dialog model when
    /// deriving the worker count.
    pub dialog_unpooled_share: i32,
    pub dialog_aifarm_fallback_high_watermark: i32,
    pub dialog_aifarm_fallback_low_watermark: i32,
    pub dialog_aifarm_fallback_poll_interval_seconds: i32,
    /// Worker count for the `image-regular` queue.
    pub image_regular_workers: i32,
    /// Worker count for the `image-vip` queue.
    pub image_vip_workers: i32,
    /// Worker count for the `music-vip` queue.
    pub music_vip_workers: i32,
    /// Worker count for the `memory-consolidation` queue.
    pub memory_consolidation_workers: i32,
    /// Upper bound for the pool-derived memory-consolidation worker count.
    pub memory_workers_cap: i32,
    /// Placeholder cleanup interval in seconds, from `PERSISTENT_QUEUE_PLACEHOLDER_CLEANUP_INTERVAL_SECONDS`.
    pub placeholder_cleanup_interval_seconds: i32,
    /// Placeholder max age in seconds, from `PERSISTENT_QUEUE_PLACEHOLDER_MAX_AGE_SECONDS`.
    pub placeholder_max_age_seconds: i32,
    /// Retryable LLM job attempt limit, from `LLM_JOB_MAX_ATTEMPTS`.
    pub llm_job_max_attempts: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketRatesConfig {
    pub timeout_seconds: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SerperConfig {
    pub api_key: String,
    pub timeout_seconds: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TranslationConfig {
    /// DeepL API-compatible configuration, from `DEEPL_*`.
    pub deepl: DeeplConfig,
}

/// DeepL API-compatible translation configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeeplConfig {
    /// DeepL auth key, from `DEEPL_KEY`.
    pub key: String,
    /// DeepL endpoint prefix, from `DEEPL_URL`.
    pub url: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GoogleAiConfig {
    /// Direct Google AI API key, from `GOOGLEAI_KEY`.
    pub key: String,
    /// JSON key stats file used to choose an active key, from `GOOGLEAI_KEY_STATS_FILE`.
    pub key_stats_file: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OpenRouterConfig {
    /// API key, from `OPENROUTER_KEY`.
    pub key: String,
    /// Request timeout, from `OPENROUTER_REQUEST_TIMEOUT_SECONDS`.
    pub request_timeout_seconds: i32,
}

/// Media uploader (plotva.geta.moe) configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UploaderConfig {
    /// Public base URL, from `PLOTVA_UPLOADER_URL`.
    pub base_url: String,
    /// Shared upload secret, from `PLOTVA_UPLOADER_SECRET`.
    pub secret: String,
    /// Request timeout, from `PLOTVA_UPLOADER_TIMEOUT_SECONDS`.
    pub timeout_seconds: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WhiteCircleConfig {
    pub enabled: bool,
    pub api_key: String,
    pub deployment_id: String,
    /// Audit-log retention in days, from `WHITECIRCLE_CHECKS_RETENTION_DAYS` (0 disables).
    pub whitecircle_checks_retention_days: i32,
}

/// LLM and provider routing configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LlmConfig {
    /// Discovery service configuration.
    pub discovery: DiscoveryConfig,
    pub genkit: GenkitConfig,
    /// Dialog/provider configuration.
    pub dialog: DialogConfig,
    pub history_summary: HistorySummaryConfig,
    /// Named LLM providers selectable per agentic flow. Empty by default.
    pub providers: Vec<NamedProviderConfig>,
    /// Agentic-workflow configuration.
    pub agentic: AgenticConfig,
}

/// A named LLM provider that agentic flows can select by name. Built from the
/// parallel-array `LLM_PROVIDERS_*` env lists; only the `aifarm` kind is wired today.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NamedProviderConfig {
    pub name: String,
    pub kind: String,
    pub discovery_service_name: String,
    pub discovery_endpoint_name: String,
    pub model: String,
    pub base_url: String,
    pub url: String,
    pub api_key: String,
    pub include_reasoning: Option<bool>,
    pub enable_thinking: Option<bool>,
    pub max_tokens: i32,
    pub temperature: Option<f64>,
    pub task_timeout_seconds: i32,
}

/// Agentic-workflow configuration root.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgenticConfig {
    /// Named provider the song/image prompt agents reason with, from
    /// `LLM_AGENT_REASONER_PROVIDER`.
    pub reasoner_provider: String,
    /// When true, song requests are written by the multi-step song agent.
    pub song_enabled: bool,
    /// When true, draw requests are refined by the multi-step image-prompt agent.
    pub image_enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GenkitConfig {
    pub default_model: String,
}

/// Discovery API configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Discovery API base URL, from `DISCOVERY_BASE_URL`.
    pub base_url: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DialogConfig {
    /// Primary dialog provider, from `DIALOG_PROVIDER`.
    pub provider: String,
    /// Fallback dialog provider, from `DIALOG_FALLBACK_PROVIDER`.
    pub fallback_provider: String,
    /// Default model, from `DIALOG_MODEL`.
    pub model: String,
    /// Direct OpenAI-compatible URL, from `DIALOG_URL`.
    pub url: String,
    /// Provider API key, from `DIALOG_API_KEY`.
    pub api_key: String,
    /// Discovery service name, from `DIALOG_DISCOVERY_SERVICE_NAME`.
    pub discovery_service_name: String,
    /// Discovery endpoint name, from `DIALOG_DISCOVERY_ENDPOINT_NAME`.
    pub discovery_endpoint_name: String,
    pub aifarm_enable_thinking: bool,
    pub aifarm_max_tokens: i32,
    pub aifarm_random_max_tokens: i32,
    pub aifarm_default_max_tokens: i32,
    pub aifarm_long_max_tokens: i32,
    pub aifarm_temperature: f64,
    pub aifarm_repeat_penalty: f64,
    pub aifarm_frequency_penalty: f64,
    pub aifarm_presence_penalty: f64,
    pub aifarm_dry_multiplier: f64,
    pub aifarm_dry_base: f64,
    pub aifarm_dry_allowed_length: i32,
    pub request_timeout_seconds: i32,
    pub poll_interval_seconds: i32,
    pub task_timeout_seconds: i32,
    pub aifarm_capacity_wait_seconds: i32,
    pub aifarm_capacity_poll_seconds: i32,
    pub aifarm_pool_models: Vec<String>,
    pub aifarm_pool_base_urls: Vec<String>,
    pub aifarm_pool_api_key: String,
    pub aifarm_pool_primary_capacity_wait_ms: i32,
    /// Per-turn wall-clock budget in seconds, anchored at the job's first
    /// processing start, from `DIALOG_TURN_BUDGET_SECS`.
    pub turn_budget_secs: i32,
    /// Pending age in seconds beyond which a never-processed dialog job is
    /// resolved as expired backlog, from `DIALOG_TURN_MAX_QUEUE_AGE_SECS`.
    pub turn_max_queue_age_secs: i32,
    /// In-process duplicate-answer regenerations per turn, from
    /// `DIALOG_TURN_MAX_REGENERATIONS`.
    pub turn_max_regenerations: i32,
    /// Reaction emoji signalling a terminal dialog failure, from
    /// `DIALOG_TERMINAL_REACTION_EMOJI`. Must be in the Bot API allowed set.
    pub terminal_reaction_emoji: String,
    /// Job age in seconds beyond which terminal failures skip the user signal
    /// (prevents reaction storms on post-downtime backlogs), from
    /// `DIALOG_TERMINAL_SIGNAL_MAX_AGE_SECS`.
    pub terminal_signal_max_age_secs: i32,
    /// Image generation delivery deadline in seconds, from
    /// `DIALOG_IMAGE_DELIVERY_TIMEOUT_SECS`.
    pub image_delivery_timeout_secs: i32,
    /// Music generation delivery deadline in seconds, from
    /// `DIALOG_MUSIC_DELIVERY_TIMEOUT_SECS`.
    pub music_delivery_timeout_secs: i32,
    /// Delivery-obligation watcher poll interval in seconds, from
    /// `DIALOG_OBLIGATION_WATCH_INTERVAL_SECS`.
    pub obligation_watch_interval_secs: i32,
    /// LLM iterations per session, from `DIALOG_SESSION_MAX_ITERATIONS`.
    pub session_max_iterations: i32,
    /// Intermediate messages per session, from `DIALOG_SESSION_MAX_MESSAGES`.
    pub session_max_messages: i32,
    /// Budget extension per started toolbox call in seconds, from
    /// `DIALOG_SESSION_TOOL_EXTENSION_SECS`.
    pub session_tool_extension_secs: i32,
    /// Absolute session wall-clock ceiling in seconds, from
    /// `DIALOG_SESSION_HARD_CAP_SECS`.
    pub session_hard_cap_secs: i32,
    /// Image generations per session, from `DIALOG_SESSION_MAX_DRAWS`.
    pub session_max_draws: i32,
    /// Song generations per session, from `DIALOG_SESSION_MAX_SONGS`.
    pub session_max_songs: i32,
    pub vmlx_url: String,
    pub vmlx_api_key: String,
    pub vmlx_model: String,
    pub nvidia_url: String,
    pub nvidia_api_key: String,
    pub nvidia_model: String,
    pub nvidia_max_tokens: i32,
    pub nvidia_temperature: f64,
    pub nvidia_top_p: f64,
    pub nvidia_enable_thinking: bool,
    pub nvidia_include_reasoning: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HistorySummaryConfig {
    pub provider: String,
    pub model: String,
    pub timeout_seconds: i32,
    /// Raw chat_history_entries retention in days (drops daily partitions older
    /// than this), from `CHAT_HISTORY_RETENTION_DAYS` (0 disables). Floor is the
    /// memory pipeline window (`MEMORY_RETENTION_HOURS`/24) plus slack.
    pub chat_history_retention_days: i32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VisionConfig {
    pub discovery_service_name: String,
    pub discovery_endpoint_name: String,
    pub model: String,
    pub max_tokens: i32,
    pub temperature: f64,
    pub direct_image_limit: i32,
    pub request_timeout_seconds: i32,
    /// telegram_files media-cache retention in days by `last_seen_at`, from
    /// `TELEGRAM_FILES_RETENTION_DAYS` (0 disables).
    pub telegram_files_retention_days: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImageProvidersConfig {
    pub boogu_turbo: BooguImageTurboConfig,
    pub boogu_edit_turbo: BooguImageEditTurboConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BooguImageTurboConfig {
    pub enabled: bool,
    pub base_url: String,
    pub timeout_seconds: i32,
    pub steps: i32,
    pub resolution: String,
    pub width: i32,
    pub height: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BooguImageEditTurboConfig {
    pub enabled: bool,
    pub base_url: String,
    pub timeout_seconds: i32,
    pub steps: i32,
    pub resolution_category: String,
    pub resolution: String,
    pub width: i32,
    pub height: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MusicConfig {
    /// ACE-Step provider configuration.
    pub acestep: AceStepConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AceStepConfig {
    pub enabled: bool,
    pub base_url: String,
    pub api_key: String,
    pub api_mode: String,
    pub request_timeout_seconds: i32,
    pub poll_interval_seconds: i32,
    pub task_timeout_seconds: i32,
    pub audio_format: String,
    pub model: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryConfig {
    pub enabled: bool,
    pub retention_hours: i32,
    pub consolidation_provider: String,
    pub consolidation_model: String,
    pub consolidation_timeout_seconds: i32,
    pub daily_schedule: String,
    pub daily_window_hours: i32,
    pub worker_concurrency: i32,
    pub max_messages_per_run: i32,
    pub min_messages_per_run: i32,
    pub max_queued_runs: i32,
    pub max_daily_enqueued_runs: i32,
    pub max_input_tokens: i32,
    pub tokenizer_model: String,
    pub tokenizer_file: String,
    pub token_estimator_url: String,
    pub token_estimator_timeout_seconds: i32,
    pub embedder_service_name: String,
    pub embedder_endpoint_name: String,
    pub embedding_model: String,
    pub embedding_dim: i32,
    pub aifarm_service_name: String,
    pub aifarm_endpoint_name: String,
    pub aifarm_max_output_tokens: i32,
    pub aifarm_request_timeout_seconds: i32,
    pub aifarm_poll_interval_seconds: i32,
    pub aifarm_task_timeout_seconds: i32,
    pub aifarm_capacity_wait_seconds: i32,
    pub aifarm_capacity_poll_seconds: i32,
    pub aifarm_temperature: f64,
    pub aifarm_frequency_penalty: f64,
    pub aifarm_presence_penalty: f64,
    pub aifarm_enable_thinking: bool,
    pub subject_merge_enabled: bool,
    pub subject_merge_min_cards: i32,
    pub subject_merge_cooldown_hours: f64,
    pub subject_merge_interval_seconds: i32,
    pub redaction_enabled: bool,
    pub redaction_service_name: String,
    pub redaction_endpoint_name: String,
    pub redaction_timeout_seconds: i32,
    pub redaction_task_timeout_seconds: i32,
    pub redaction_poll_seconds: i32,
    pub redaction_capacity_wait_seconds: i32,
    pub redaction_capacity_poll_seconds: i32,
    pub redaction_categories: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShieldConfig {
    pub enabled: bool,
    pub embedder_service_name: String,
    pub embedder_endpoint_name: String,
    pub embedding_dim: i32,
    pub max_matches: i32,
    pub vector_min_score: f64,
    pub lexical_min_score: f64,
    pub query_max_chars: i32,
    pub retrieval_timeout_seconds: i32,
    pub history_tail_messages: i32,
}

/// Service-probe configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceProbeConfig {
    /// Whether startup should connect to Postgres and Redis.
    pub connect_services: bool,
    /// Whether startup should apply SQLx migrations after connecting to Postgres.
    pub run_migrations: bool,
    /// Whether startup should pull or accept Telegram updates into Redis.
    pub produce_updates: bool,
    /// Whether startup should consume decoded Telegram updates from Redis.
    pub consume_updates: bool,
}

/// Raw optional config values used by tests and environment loading.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RawConfig {
    /// Rust-only full bind-address override for local shell work.
    pub openplotva_bind_addr: Option<String>,
    /// Rust-only tracing filter override.
    pub openplotva_log_filter: Option<String>,
    /// `LOG_LEVEL`.
    pub log_level: Option<String>,
    /// `WEBAPP_HOST`.
    pub webapp_host: Option<String>,
    /// `WEBAPP_PORT`.
    pub webapp_port: Option<String>,
    /// `WEBAPP_URL`.
    pub webapp_url: Option<String>,
    /// `SETTINGS_REQUIRE_INIT_DATA`.
    pub settings_require_init_data: Option<String>,
    /// `RUNTIME_API_ENABLED`.
    pub runtime_api_enabled: Option<String>,
    /// `RUNTIME_API_HOST`.
    pub runtime_api_host: Option<String>,
    /// `RUNTIME_API_PORT`.
    pub runtime_api_port: Option<String>,
    /// `RUNTIME_API_LOG_BUFFER_SIZE`.
    pub runtime_api_log_buffer_size: Option<String>,
    /// `RUNTIME_API_SQL_TIMEOUT_MS`.
    pub runtime_api_sql_timeout_ms: Option<String>,
    /// `RUNTIME_API_SQL_ROW_LIMIT`.
    pub runtime_api_sql_row_limit: Option<String>,
    /// `RUNTIME_API_SQL_RESULT_BYTES_LIMIT`.
    pub runtime_api_sql_result_bytes_limit: Option<String>,
    /// `LLM_REQUEST_EVENTS_RETENTION_DAYS`.
    pub llm_request_events_retention_days: Option<String>,
    /// `LLM_RUN_BUFFER_CAPACITY`.
    pub llm_run_buffer_capacity: Option<String>,
    /// `LLM_RAW_BODY_PERSIST_ENABLED`.
    pub llm_raw_body_persist_enabled: Option<String>,
    /// `LLM_RAW_BODY_MAX_BYTES`.
    pub llm_raw_body_max_bytes: Option<String>,
    /// `LLM_RAW_BODY_RETENTION_HOURS`.
    pub llm_raw_body_retention_hours: Option<String>,
    /// `RUNTIME_API_CERT_FILE`.
    pub runtime_api_cert_file: Option<String>,
    /// `RUNTIME_API_KEY_FILE`.
    pub runtime_api_key_file: Option<String>,
    /// `RUNTIME_API_TLS_PUBLIC_KEY_PIN`.
    pub runtime_api_tls_public_key_pin: Option<String>,
    /// `DB_POSTGRES_HOST`.
    pub db_postgres_host: Option<String>,
    /// `DB_POSTGRES_PORT`.
    pub db_postgres_port: Option<String>,
    /// `DB_POSTGRES_USER`.
    pub db_postgres_user: Option<String>,
    /// `DB_POSTGRES_PASSWORD`.
    pub db_postgres_password: Option<String>,
    /// `DB_POSTGRES_DB`.
    pub db_postgres_db: Option<String>,
    /// `DB_POSTGRES_SSL_MODE`.
    pub db_postgres_ssl_mode: Option<String>,
    /// `REDIS_HOST`.
    pub redis_host: Option<String>,
    /// `REDIS_PORT`.
    pub redis_port: Option<String>,
    /// `REDIS_PASSWORD`.
    pub redis_password: Option<String>,
    /// `REDIS_DB`.
    pub redis_db: Option<String>,
    /// `UPDATE_QUEUE_BACKEND`.
    pub update_queue_backend: Option<String>,
    /// `BOT_KEY`.
    pub bot_key: Option<String>,
    /// `BOT_API_BASE_URL`.
    pub bot_api_base_url: Option<String>,
    /// `BOT_WEBHOOK_ENABLED`.
    pub bot_webhook_enabled: Option<String>,
    /// `BOT_WEBHOOK_URL`.
    pub bot_webhook_url: Option<String>,
    /// `BOT_WEBHOOK_CERT_FILE`.
    pub bot_webhook_cert_file: Option<String>,
    /// `BOT_WEBHOOK_KEY_FILE`.
    pub bot_webhook_key_file: Option<String>,
    /// `BOT_WEBHOOK_SECRET_TOKEN`.
    pub bot_webhook_secret_token: Option<String>,
    /// `BOT_WEBHOOK_UPDATE_BUFFER_SIZE`.
    pub bot_webhook_update_buffer_size: Option<String>,
    /// `TELEGRAM_ACTIVITY_PULSE_ENABLED`.
    pub telegram_activity_pulse_enabled: Option<String>,
    /// `TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS`.
    pub telegram_activity_pulse_interval_ms: Option<String>,
    /// `TELEGRAM_ACTIVITY_PULSE_INITIAL_DELAY_MS`.
    pub telegram_activity_pulse_initial_delay_ms: Option<String>,
    /// `BOT_DEBUG`.
    pub bot_debug: Option<String>,
    /// `ADMINS_ADMIN_IDS`.
    pub admins_admin_ids: Option<String>,
    /// `VIP_CHAT_ID`.
    pub vip_chat_id: Option<String>,
    /// Legacy Go-era VIP chat id variable, `CHAT_ID`.
    pub legacy_vip_chat_id: Option<String>,
    /// `SUBSCRIPTION_SYNC_ENABLED`.
    pub subscription_sync_enabled: Option<String>,
    /// `SUBSCRIPTION_SYNC_INTERVAL_SECONDS`.
    pub subscription_sync_interval_seconds: Option<String>,
    /// `SUBSCRIPTION_SYNC_DRY_RUN`.
    pub subscription_sync_dry_run: Option<String>,
    /// `SUBSCRIPTION_SYNC_PAGE_LIMIT`.
    pub subscription_sync_page_limit: Option<String>,
    /// `PERSISTENT_QUEUE_ENABLED`.
    pub persistent_queue_enabled: Option<String>,
    /// `PERSISTENT_QUEUE_HEARTBEAT_INTERVAL_SECONDS`.
    pub persistent_queue_heartbeat_interval_seconds: Option<String>,
    /// `PERSISTENT_QUEUE_RECOVERY_INTERVAL_SECONDS`.
    pub persistent_queue_recovery_interval_seconds: Option<String>,
    /// `PERSISTENT_QUEUE_CLEANUP_INTERVAL_SECONDS`.
    pub persistent_queue_cleanup_interval_seconds: Option<String>,
    /// `PERSISTENT_QUEUE_DEFAULT_PROCESSING_TIMEOUT_SECONDS`.
    pub persistent_queue_default_processing_timeout_seconds: Option<String>,
    /// `PERSISTENT_QUEUE_MAX_RETRIES`.
    pub persistent_queue_max_retries: Option<String>,
    /// `PERSISTENT_QUEUE_COMPLETED_JOB_RETENTION_DAYS`.
    pub persistent_queue_completed_job_retention_days: Option<String>,
    /// `PERSISTENT_QUEUE_MESSAGE_CLEANUP_INTERVAL_SECONDS`.
    pub persistent_queue_message_cleanup_interval_seconds: Option<String>,
    /// `PERSISTENT_QUEUE_JOB_MESSAGE_CLEANUP_MINUTES`.
    pub persistent_queue_job_message_cleanup_minutes: Option<String>,
    /// `PERSISTENT_QUEUE_CONTROL_WORKERS`.
    pub persistent_queue_control_workers: Option<String>,
    /// `PERSISTENT_QUEUE_TEXT_WORKERS`.
    pub persistent_queue_text_workers: Option<String>,
    /// `PERSISTENT_QUEUE_DIALOG_AIFARM_WORKERS`.
    pub persistent_queue_dialog_aifarm_workers: Option<String>,
    pub persistent_queue_dialog_workers_cap: Option<String>,
    pub persistent_queue_dialog_unpooled_share: Option<String>,
    /// `PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_HIGH_WATERMARK`.
    pub persistent_queue_dialog_aifarm_fallback_high_watermark: Option<String>,
    /// `PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_LOW_WATERMARK`.
    pub persistent_queue_dialog_aifarm_fallback_low_watermark: Option<String>,
    /// `PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_POLL_INTERVAL_SECONDS`.
    pub persistent_queue_dialog_aifarm_fallback_poll_interval_seconds: Option<String>,
    /// `PERSISTENT_QUEUE_IMAGE_REGULAR_WORKERS`.
    pub persistent_queue_image_regular_workers: Option<String>,
    /// `PERSISTENT_QUEUE_IMAGE_VIP_WORKERS`.
    pub persistent_queue_image_vip_workers: Option<String>,
    /// `PERSISTENT_QUEUE_MUSIC_VIP_WORKERS`.
    pub persistent_queue_music_vip_workers: Option<String>,
    /// `PERSISTENT_QUEUE_MEMORY_CONSOLIDATION_WORKERS`.
    pub persistent_queue_memory_consolidation_workers: Option<String>,
    pub persistent_queue_memory_workers_cap: Option<String>,
    /// `PERSISTENT_QUEUE_PLACEHOLDER_CLEANUP_INTERVAL_SECONDS`.
    pub persistent_queue_placeholder_cleanup_interval_seconds: Option<String>,
    /// `PERSISTENT_QUEUE_PLACEHOLDER_MAX_AGE_SECONDS`.
    pub persistent_queue_placeholder_max_age_seconds: Option<String>,
    /// `LLM_JOB_MAX_ATTEMPTS`.
    pub llm_job_max_attempts: Option<String>,
    /// `MARKET_RATES_TIMEOUT_SECONDS`.
    pub market_rates_timeout_seconds: Option<String>,
    /// Deprecated fallback for `MARKET_RATES_TIMEOUT_SECONDS`.
    pub rbc_timeout_seconds: Option<String>,
    /// `SERPER_API_KEY`.
    pub serper_api_key: Option<String>,
    /// `SERPER_TIMEOUT_SECONDS`.
    pub serper_timeout_seconds: Option<String>,
    /// `DEEPL_KEY`.
    pub deepl_key: Option<String>,
    /// `DEEPL_URL`.
    pub deepl_url: Option<String>,
    /// `GOOGLEAI_KEY`.
    pub googleai_key: Option<String>,
    /// `GOOGLEAI_KEY_STATS_FILE`.
    pub googleai_key_stats_file: Option<String>,
    /// `OPENROUTER_KEY`.
    pub openrouter_key: Option<String>,
    /// `OPENROUTER_REQUEST_TIMEOUT_SECONDS`.
    pub openrouter_request_timeout_seconds: Option<String>,
    /// `PLOTVA_UPLOADER_URL`.
    pub uploader_url: Option<String>,
    /// `PLOTVA_UPLOADER_SECRET`.
    pub uploader_secret: Option<String>,
    /// `PLOTVA_UPLOADER_TIMEOUT_SECONDS`.
    pub uploader_timeout_seconds: Option<String>,
    /// `WHITECIRCLE_ENABLED`.
    pub whitecircle_enabled: Option<String>,
    /// `WHITECIRCLE_API_KEY`.
    pub whitecircle_api_key: Option<String>,
    /// `WHITECIRCLE_DEPLOYMENT_ID`.
    pub whitecircle_deployment_id: Option<String>,
    /// `WHITECIRCLE_CHECKS_RETENTION_DAYS`.
    pub whitecircle_checks_retention_days: Option<String>,
    /// `DISCOVERY_BASE_URL`.
    pub discovery_base_url: Option<String>,
    /// `DIALOG_PROVIDER`.
    pub dialog_provider: Option<String>,
    /// `DIALOG_FALLBACK_PROVIDER`.
    pub dialog_fallback_provider: Option<String>,
    /// `DIALOG_MODEL`.
    pub dialog_model: Option<String>,
    /// `DIALOG_URL`.
    pub dialog_url: Option<String>,
    /// `DIALOG_API_KEY`.
    pub dialog_api_key: Option<String>,
    /// `DIALOG_DISCOVERY_SERVICE_NAME`.
    pub dialog_discovery_service_name: Option<String>,
    /// `DIALOG_DISCOVERY_ENDPOINT_NAME`.
    pub dialog_discovery_endpoint_name: Option<String>,
    /// `DIALOG_AIFARM_ENABLE_THINKING`.
    pub dialog_aifarm_enable_thinking: Option<String>,
    /// `DIALOG_AIFARM_MAX_TOKENS`.
    pub dialog_aifarm_max_tokens: Option<String>,
    /// `DIALOG_AIFARM_RANDOM_MAX_TOKENS`.
    pub dialog_aifarm_random_max_tokens: Option<String>,
    /// `DIALOG_AIFARM_DEFAULT_MAX_TOKENS`.
    pub dialog_aifarm_default_max_tokens: Option<String>,
    /// `DIALOG_AIFARM_LONG_MAX_TOKENS`.
    pub dialog_aifarm_long_max_tokens: Option<String>,
    /// `DIALOG_AIFARM_TEMPERATURE`.
    pub dialog_aifarm_temperature: Option<String>,
    /// `DIALOG_AIFARM_REPEAT_PENALTY`.
    pub dialog_aifarm_repeat_penalty: Option<String>,
    /// `DIALOG_AIFARM_FREQUENCY_PENALTY`.
    pub dialog_aifarm_frequency_penalty: Option<String>,
    /// `DIALOG_AIFARM_PRESENCE_PENALTY`.
    pub dialog_aifarm_presence_penalty: Option<String>,
    /// `DIALOG_AIFARM_DRY_MULTIPLIER`.
    pub dialog_aifarm_dry_multiplier: Option<String>,
    /// `DIALOG_AIFARM_DRY_BASE`.
    pub dialog_aifarm_dry_base: Option<String>,
    /// `DIALOG_AIFARM_DRY_ALLOWED_LENGTH`.
    pub dialog_aifarm_dry_allowed_length: Option<String>,
    /// `DIALOG_REQUEST_TIMEOUT_SECONDS`.
    pub dialog_request_timeout_seconds: Option<String>,
    /// `DIALOG_POLL_INTERVAL_SECONDS`.
    pub dialog_poll_interval_seconds: Option<String>,
    /// `DIALOG_TASK_TIMEOUT_SECONDS`.
    pub dialog_task_timeout_seconds: Option<String>,
    /// `DIALOG_AIFARM_CAPACITY_WAIT_SECONDS`.
    pub dialog_aifarm_capacity_wait_seconds: Option<String>,
    /// `DIALOG_AIFARM_CAPACITY_POLL_SECONDS`.
    pub dialog_aifarm_capacity_poll_seconds: Option<String>,
    /// `DIALOG_AIFARM_POOL_MODELS`.
    pub dialog_aifarm_pool_models: Option<String>,
    /// `DIALOG_AIFARM_POOL_BASE_URLS`.
    pub dialog_aifarm_pool_base_urls: Option<String>,
    /// `DIALOG_AIFARM_POOL_API_KEY`.
    pub dialog_aifarm_pool_api_key: Option<String>,
    /// `DIALOG_AIFARM_POOL_PRIMARY_CAPACITY_WAIT_MS`.
    pub dialog_aifarm_pool_primary_capacity_wait_ms: Option<String>,
    /// `DIALOG_TURN_BUDGET_SECS`.
    pub dialog_turn_budget_secs: Option<String>,
    /// `DIALOG_TURN_MAX_QUEUE_AGE_SECS`.
    pub dialog_turn_max_queue_age_secs: Option<String>,
    /// `DIALOG_TURN_MAX_REGENERATIONS`.
    pub dialog_turn_max_regenerations: Option<String>,
    /// `DIALOG_TERMINAL_REACTION_EMOJI`.
    pub dialog_terminal_reaction_emoji: Option<String>,
    /// `DIALOG_TERMINAL_SIGNAL_MAX_AGE_SECS`.
    pub dialog_terminal_signal_max_age_secs: Option<String>,
    /// `DIALOG_IMAGE_DELIVERY_TIMEOUT_SECS`.
    pub dialog_image_delivery_timeout_secs: Option<String>,
    /// `DIALOG_MUSIC_DELIVERY_TIMEOUT_SECS`.
    pub dialog_music_delivery_timeout_secs: Option<String>,
    /// `DIALOG_OBLIGATION_WATCH_INTERVAL_SECS`.
    pub dialog_obligation_watch_interval_secs: Option<String>,
    /// `DIALOG_SESSION_MAX_ITERATIONS`.
    pub dialog_session_max_iterations: Option<String>,
    /// `DIALOG_SESSION_MAX_MESSAGES`.
    pub dialog_session_max_messages: Option<String>,
    /// `DIALOG_SESSION_TOOL_EXTENSION_SECS`.
    pub dialog_session_tool_extension_secs: Option<String>,
    /// `DIALOG_SESSION_HARD_CAP_SECS`.
    pub dialog_session_hard_cap_secs: Option<String>,
    /// `DIALOG_SESSION_MAX_DRAWS`.
    pub dialog_session_max_draws: Option<String>,
    /// `DIALOG_SESSION_MAX_SONGS`.
    pub dialog_session_max_songs: Option<String>,
    /// `DIALOG_VMLX_URL`.
    pub dialog_vmlx_url: Option<String>,
    /// `DIALOG_VMLX_API_KEY`.
    pub dialog_vmlx_api_key: Option<String>,
    /// `DIALOG_VMLX_MODEL`.
    pub dialog_vmlx_model: Option<String>,
    /// `DIALOG_NVIDIA_URL`.
    pub dialog_nvidia_url: Option<String>,
    /// `DIALOG_NVIDIA_API_KEY`.
    pub dialog_nvidia_api_key: Option<String>,
    /// `DIALOG_NVIDIA_MODEL`.
    pub dialog_nvidia_model: Option<String>,
    /// `DIALOG_NVIDIA_MAX_TOKENS`.
    pub dialog_nvidia_max_tokens: Option<String>,
    /// `DIALOG_NVIDIA_TEMPERATURE`.
    pub dialog_nvidia_temperature: Option<String>,
    /// `DIALOG_NVIDIA_TOP_P`.
    pub dialog_nvidia_top_p: Option<String>,
    /// `DIALOG_NVIDIA_ENABLE_THINKING`.
    pub dialog_nvidia_enable_thinking: Option<String>,
    /// `DIALOG_NVIDIA_INCLUDE_REASONING`.
    pub dialog_nvidia_include_reasoning: Option<String>,
    /// `LLM_PROVIDERS_NAMES` (comma list; primary key for the parallel arrays).
    pub llm_provider_names: Option<String>,
    /// `LLM_PROVIDERS_KINDS`.
    pub llm_provider_kinds: Option<String>,
    /// `LLM_PROVIDERS_DISCOVERY_SERVICE_NAMES`.
    pub llm_provider_discovery_service_names: Option<String>,
    /// `LLM_PROVIDERS_DISCOVERY_ENDPOINT_NAMES`.
    pub llm_provider_discovery_endpoint_names: Option<String>,
    /// `LLM_PROVIDERS_MODELS`.
    pub llm_provider_models: Option<String>,
    /// `LLM_PROVIDERS_BASE_URLS`.
    pub llm_provider_base_urls: Option<String>,
    /// `LLM_PROVIDERS_URLS`.
    pub llm_provider_urls: Option<String>,
    /// `LLM_PROVIDERS_API_KEYS`.
    pub llm_provider_api_keys: Option<String>,
    /// `LLM_PROVIDERS_INCLUDE_REASONING`.
    pub llm_provider_include_reasoning: Option<String>,
    /// `LLM_PROVIDERS_ENABLE_THINKING`.
    pub llm_provider_enable_thinking: Option<String>,
    /// `LLM_PROVIDERS_MAX_TOKENS`.
    pub llm_provider_max_tokens: Option<String>,
    /// `LLM_PROVIDERS_TEMPERATURES`.
    pub llm_provider_temperatures: Option<String>,
    /// `LLM_PROVIDERS_TASK_TIMEOUT_SECONDS`.
    pub llm_provider_task_timeout_seconds: Option<String>,
    /// `LLM_AGENTIC_SONG_ENABLED`.
    pub llm_agentic_song_enabled: Option<String>,
    /// `LLM_AGENTIC_IMAGE_ENABLED`.
    pub llm_agentic_image_enabled: Option<String>,
    /// `LLM_AGENT_REASONER_PROVIDER`.
    pub llm_agent_reasoner_provider: Option<String>,
    /// `GENKIT_DEFAULT_MODEL`.
    pub genkit_default_model: Option<String>,
    /// `GENKIT_HISTORY_SUMMARY_PROVIDER`.
    pub genkit_history_summary_provider: Option<String>,
    /// `GENKIT_HISTORY_SUMMARY_MODEL`.
    pub genkit_history_summary_model: Option<String>,
    /// `GENKIT_HISTORY_SUMMARY_TIMEOUT_SECONDS`.
    pub genkit_history_summary_timeout_seconds: Option<String>,
    /// `CHAT_HISTORY_RETENTION_DAYS`.
    pub chat_history_retention_days: Option<String>,
    /// `VISION_DISCOVERY_SERVICE_NAME`.
    pub vision_discovery_service_name: Option<String>,
    /// `VISION_DISCOVERY_ENDPOINT_NAME`.
    pub vision_discovery_endpoint_name: Option<String>,
    /// `VISION_MODEL`.
    pub vision_model: Option<String>,
    /// `VISION_MAX_TOKENS`.
    pub vision_max_tokens: Option<String>,
    /// `VISION_TEMPERATURE`.
    pub vision_temperature: Option<String>,
    /// `VISION_DIRECT_IMAGE_LIMIT`.
    pub vision_direct_image_limit: Option<String>,
    /// `VISION_REQUEST_TIMEOUT_SECONDS`.
    pub vision_request_timeout_seconds: Option<String>,
    /// `TELEGRAM_FILES_RETENTION_DAYS`.
    pub telegram_files_retention_days: Option<String>,
    /// `BOOGU_IMAGE_TURBO_ENABLED`.
    pub boogu_image_turbo_enabled: Option<String>,
    /// `BOOGU_IMAGE_TURBO_BASE_URL`.
    pub boogu_image_turbo_base_url: Option<String>,
    /// `BOOGU_IMAGE_EDIT_TURBO_ENABLED`.
    pub boogu_image_edit_turbo_enabled: Option<String>,
    /// `BOOGU_IMAGE_EDIT_TURBO_BASE_URL`.
    pub boogu_image_edit_turbo_base_url: Option<String>,
    /// `BOOGU_IMAGE_TIMEOUT_SECONDS`.
    pub boogu_image_timeout_seconds: Option<String>,
    /// `BOOGU_IMAGE_TURBO_STEPS`.
    pub boogu_image_turbo_steps: Option<String>,
    /// `BOOGU_IMAGE_TURBO_RESOLUTION`.
    pub boogu_image_turbo_resolution: Option<String>,
    /// `BOOGU_IMAGE_TURBO_WIDTH`.
    pub boogu_image_turbo_width: Option<String>,
    /// `BOOGU_IMAGE_TURBO_HEIGHT`.
    pub boogu_image_turbo_height: Option<String>,
    /// `BOOGU_IMAGE_EDIT_TURBO_STEPS`.
    pub boogu_image_edit_turbo_steps: Option<String>,
    /// `BOOGU_IMAGE_EDIT_TURBO_RESOLUTION_CATEGORY`.
    pub boogu_image_edit_turbo_resolution_category: Option<String>,
    /// `BOOGU_IMAGE_EDIT_TURBO_RESOLUTION`.
    pub boogu_image_edit_turbo_resolution: Option<String>,
    /// `BOOGU_IMAGE_EDIT_TURBO_WIDTH`.
    pub boogu_image_edit_turbo_width: Option<String>,
    /// `BOOGU_IMAGE_EDIT_TURBO_HEIGHT`.
    pub boogu_image_edit_turbo_height: Option<String>,
    /// `ACESTEP_ENABLED`.
    pub acestep_enabled: Option<String>,
    /// `ACESTEP_BASE_URL`.
    pub acestep_base_url: Option<String>,
    /// `ACESTEP_API_KEY`.
    pub acestep_api_key: Option<String>,
    /// `ACESTEP_API_MODE`.
    pub acestep_api_mode: Option<String>,
    /// `ACESTEP_REQUEST_TIMEOUT_SECONDS`.
    pub acestep_request_timeout_seconds: Option<String>,
    /// `ACESTEP_POLL_INTERVAL_SECONDS`.
    pub acestep_poll_interval_seconds: Option<String>,
    /// `ACESTEP_TASK_TIMEOUT_SECONDS`.
    pub acestep_task_timeout_seconds: Option<String>,
    /// `ACESTEP_AUDIO_FORMAT`.
    pub acestep_audio_format: Option<String>,
    /// `ACESTEP_MODEL`.
    pub acestep_model: Option<String>,
    /// `MEMORY_ENABLED`.
    pub memory_enabled: Option<String>,
    /// `MEMORY_RETENTION_HOURS`.
    pub memory_retention_hours: Option<String>,
    /// `MEMORY_CONSOLIDATION_PROVIDER`.
    pub memory_consolidation_provider: Option<String>,
    /// `MEMORY_CONSOLIDATION_MODEL`.
    pub memory_consolidation_model: Option<String>,
    /// `MEMORY_CONSOLIDATION_TIMEOUT_SECONDS`.
    pub memory_consolidation_timeout_seconds: Option<String>,
    /// `MEMORY_DAILY_SCHEDULE`.
    pub memory_daily_schedule: Option<String>,
    /// `MEMORY_DAILY_WINDOW_HOURS`.
    pub memory_daily_window_hours: Option<String>,
    /// `MEMORY_WORKER_CONCURRENCY`.
    pub memory_worker_concurrency: Option<String>,
    /// `MEMORY_MAX_MESSAGES_PER_RUN`.
    pub memory_max_messages_per_run: Option<String>,
    /// `MEMORY_MIN_MESSAGES_PER_RUN`.
    pub memory_min_messages_per_run: Option<String>,
    /// `MEMORY_MAX_QUEUED_RUNS`.
    pub memory_max_queued_runs: Option<String>,
    /// `MEMORY_MAX_DAILY_ENQUEUED_RUNS`.
    pub memory_max_daily_enqueued_runs: Option<String>,
    /// `MEMORY_MAX_INPUT_TOKENS`.
    pub memory_max_input_tokens: Option<String>,
    /// `MEMORY_TOKENIZER_MODEL`.
    pub memory_tokenizer_model: Option<String>,
    /// `MEMORY_TOKENIZER_FILE`.
    pub memory_tokenizer_file: Option<String>,
    /// `MEMORY_TOKEN_ESTIMATOR_URL`.
    pub memory_token_estimator_url: Option<String>,
    /// `MEMORY_TOKEN_ESTIMATOR_TIMEOUT_SECONDS`.
    pub memory_token_estimator_timeout_seconds: Option<String>,
    /// `MEMORY_EMBEDDER_DISCOVERY_SERVICE_NAME`.
    pub memory_embedder_service_name: Option<String>,
    /// `MEMORY_EMBEDDER_DISCOVERY_ENDPOINT_NAME`.
    pub memory_embedder_endpoint_name: Option<String>,
    /// `MEMORY_EMBEDDING_MODEL`.
    pub memory_embedding_model: Option<String>,
    /// `MEMORY_EMBEDDING_DIM`.
    pub memory_embedding_dim: Option<String>,
    /// `MEMORY_AIFARM_SERVICE_NAME`.
    pub memory_aifarm_service_name: Option<String>,
    /// `MEMORY_AIFARM_ENDPOINT_NAME`.
    pub memory_aifarm_endpoint_name: Option<String>,
    /// `MEMORY_AIFARM_MAX_OUTPUT_TOKENS`.
    pub memory_aifarm_max_output_tokens: Option<String>,
    /// `MEMORY_AIFARM_REQUEST_TIMEOUT_SECONDS`.
    pub memory_aifarm_request_timeout_seconds: Option<String>,
    /// `MEMORY_AIFARM_POLL_INTERVAL_SECONDS`.
    pub memory_aifarm_poll_interval_seconds: Option<String>,
    /// `MEMORY_AIFARM_TASK_TIMEOUT_SECONDS`.
    pub memory_aifarm_task_timeout_seconds: Option<String>,
    /// `MEMORY_AIFARM_CAPACITY_WAIT_SECONDS`.
    pub memory_aifarm_capacity_wait_seconds: Option<String>,
    /// `MEMORY_AIFARM_CAPACITY_POLL_SECONDS`.
    pub memory_aifarm_capacity_poll_seconds: Option<String>,
    /// `MEMORY_AIFARM_TEMPERATURE`.
    pub memory_aifarm_temperature: Option<String>,
    /// `MEMORY_AIFARM_FREQUENCY_PENALTY`.
    pub memory_aifarm_frequency_penalty: Option<String>,
    /// `MEMORY_AIFARM_PRESENCE_PENALTY`.
    pub memory_aifarm_presence_penalty: Option<String>,
    /// `MEMORY_AIFARM_ENABLE_THINKING`.
    pub memory_aifarm_enable_thinking: Option<String>,
    /// `MEMORY_SUBJECT_MERGE_ENABLED`.
    pub memory_subject_merge_enabled: Option<String>,
    /// `MEMORY_SUBJECT_MERGE_MIN_CARDS`.
    pub memory_subject_merge_min_cards: Option<String>,
    /// `MEMORY_SUBJECT_MERGE_COOLDOWN_HOURS`.
    pub memory_subject_merge_cooldown_hours: Option<String>,
    /// `MEMORY_SUBJECT_MERGE_INTERVAL_SECONDS`.
    pub memory_subject_merge_interval_seconds: Option<String>,
    /// `MEMORY_REDACTION_ENABLED`.
    pub memory_redaction_enabled: Option<String>,
    /// `MEMORY_REDACTION_SERVICE_NAME`.
    pub memory_redaction_service_name: Option<String>,
    /// `MEMORY_REDACTION_ENDPOINT_NAME`.
    pub memory_redaction_endpoint_name: Option<String>,
    /// `MEMORY_REDACTION_TIMEOUT_SECONDS`.
    pub memory_redaction_timeout_seconds: Option<String>,
    /// `MEMORY_REDACTION_TASK_TIMEOUT_SECONDS`.
    pub memory_redaction_task_timeout_seconds: Option<String>,
    /// `MEMORY_REDACTION_POLL_SECONDS`.
    pub memory_redaction_poll_seconds: Option<String>,
    /// `MEMORY_REDACTION_CAPACITY_WAIT_SECONDS`.
    pub memory_redaction_capacity_wait_seconds: Option<String>,
    /// `MEMORY_REDACTION_CAPACITY_POLL_SECONDS`.
    pub memory_redaction_capacity_poll_seconds: Option<String>,
    /// `MEMORY_REDACTION_CATEGORIES`.
    pub memory_redaction_categories: Option<String>,
    /// `SHIELD_ENABLED`.
    pub shield_enabled: Option<String>,
    /// `SHIELD_EMBEDDER_DISCOVERY_SERVICE_NAME`.
    pub shield_embedder_service_name: Option<String>,
    /// `SHIELD_EMBEDDER_DISCOVERY_ENDPOINT_NAME`.
    pub shield_embedder_endpoint_name: Option<String>,
    /// `SHIELD_EMBEDDING_DIM`.
    pub shield_embedding_dim: Option<String>,
    /// `SHIELD_MAX_MATCHES`.
    pub shield_max_matches: Option<String>,
    /// `SHIELD_VECTOR_MIN_SCORE`.
    pub shield_vector_min_score: Option<String>,
    /// `SHIELD_LEXICAL_MIN_SCORE`.
    pub shield_lexical_min_score: Option<String>,
    /// `SHIELD_QUERY_MAX_CHARS`.
    pub shield_query_max_chars: Option<String>,
    /// `SHIELD_RETRIEVAL_TIMEOUT_SECONDS`.
    pub shield_retrieval_timeout_seconds: Option<String>,
    /// `SHIELD_HISTORY_TAIL_MESSAGES`.
    pub shield_history_tail_messages: Option<String>,
    /// `OPENPLOTVA_CONNECT_SERVICES`.
    pub openplotva_connect_services: Option<String>,
    /// `OPENPLOTVA_RUN_MIGRATIONS`.
    pub openplotva_run_migrations: Option<String>,
    /// `OPENPLOTVA_PRODUCE_UPDATES`.
    pub openplotva_produce_updates: Option<String>,
    /// `OPENPLOTVA_CONSUME_UPDATES`.
    pub openplotva_consume_updates: Option<String>,
}

/// Configuration loading failures.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// `.env` exists but could not be loaded.
    #[error("failed to load .env: {source}")]
    Dotenv {
        /// Parser or IO error from dotenvy.
        #[source]
        source: dotenvy::Error,
    },
    /// An integer environment variable failed to parse.
    #[error("invalid {name} {value:?}: {source}")]
    InvalidInteger {
        /// Environment variable name.
        name: &'static str,
        /// Raw value from the environment.
        value: String,
        /// Parser error.
        #[source]
        source: ParseIntError,
    },
    /// An integer parsed but did not fit the target type.
    #[error("invalid {name} {value}: {source}")]
    IntegerOutOfRange {
        /// Environment variable name.
        name: &'static str,
        /// Parsed value.
        value: i64,
        /// Conversion error.
        #[source]
        source: TryFromIntError,
    },
    /// A floating-point environment variable failed to parse.
    #[error("invalid {name} {value:?}: {source}")]
    InvalidFloat {
        /// Environment variable name.
        name: &'static str,
        /// Raw value from the environment.
        value: String,
        /// Parser error.
        #[source]
        source: ParseFloatError,
    },
    /// A boolean environment variable failed to parse.
    #[error("invalid {name} {value:?}: expected true/false, t/f, or 1/0")]
    InvalidBoolean {
        /// Environment variable name.
        name: &'static str,
        /// Raw value from the environment.
        value: String,
    },
    #[error("DIALOG_AIFARM_POOL_MODELS and DIALOG_AIFARM_POOL_BASE_URLS must have the same length")]
    AifarmPoolPairCount,
    #[error("invalid GENKIT_HISTORY_SUMMARY_PROVIDER value: {value}")]
    InvalidHistorySummaryProvider {
        /// Raw provider value.
        value: String,
    },
    #[error("{name} must be set when the feature is enabled")]
    InvalidEmptyValue { name: &'static str },
    #[error("{name} must be greater than zero")]
    NonPositiveInteger {
        /// Environment variable name.
        name: &'static str,
        /// Parsed value.
        value: i32,
    },
    #[error("{name} must be non-negative")]
    NegativeInteger {
        /// Environment variable name.
        name: &'static str,
        /// Parsed value.
        value: i32,
    },
    #[error("{name} must be at most {max}")]
    IntegerExceedsMaximum {
        /// Environment variable name.
        name: &'static str,
        /// Parsed value.
        value: i32,
        /// Maximum accepted value.
        max: i32,
    },
    #[error(
        "PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_LOW_WATERMARK cannot exceed PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_HIGH_WATERMARK"
    )]
    PersistentQueueFallbackWatermarkRange,
    #[error("invalid UPDATE_QUEUE_BACKEND value {value:?}: expected list or stream")]
    InvalidUpdateQueueBackend { value: String },
}

impl AppConfig {
    /// Load configuration from process environment variables.
    pub fn from_env() -> Result<Self, ConfigError> {
        load_dotenv()?;
        Self::from_raw(RawConfig::from_env())
    }

    /// Build configuration from optional raw values kept for scaffold tests.
    pub fn from_values(
        bind_addr: Option<String>,
        log_filter: Option<String>,
    ) -> Result<Self, ConfigError> {
        Self::from_raw(RawConfig {
            openplotva_bind_addr: bind_addr,
            openplotva_log_filter: log_filter,
            ..RawConfig::default()
        })
    }

    /// Build configuration from raw optional values.
    pub fn from_raw(raw: RawConfig) -> Result<Self, ConfigError> {
        let webapp_host = raw
            .webapp_host
            .unwrap_or_else(|| DEFAULT_WEBAPP_HOST.to_owned());
        let webapp_port = parse_u16("WEBAPP_PORT", raw.webapp_port, DEFAULT_WEBAPP_PORT)?;
        let runtime_api = RuntimeApiConfig {
            enabled: parse_bool(
                "RUNTIME_API_ENABLED",
                raw.runtime_api_enabled,
                DEFAULT_RUNTIME_API_ENABLED,
            )?,
            host: raw
                .runtime_api_host
                .unwrap_or_else(|| DEFAULT_RUNTIME_API_HOST.to_owned()),
            port: parse_u16(
                "RUNTIME_API_PORT",
                raw.runtime_api_port,
                DEFAULT_RUNTIME_API_PORT,
            )?,
            log_buffer_size: parse_i32(
                "RUNTIME_API_LOG_BUFFER_SIZE",
                raw.runtime_api_log_buffer_size,
                DEFAULT_RUNTIME_API_LOG_BUFFER_SIZE,
            )?,
            sql_timeout_ms: parse_i32(
                "RUNTIME_API_SQL_TIMEOUT_MS",
                raw.runtime_api_sql_timeout_ms,
                DEFAULT_RUNTIME_API_SQL_TIMEOUT_MS,
            )?,
            sql_row_limit: parse_i32(
                "RUNTIME_API_SQL_ROW_LIMIT",
                raw.runtime_api_sql_row_limit,
                DEFAULT_RUNTIME_API_SQL_ROW_LIMIT,
            )?,
            sql_result_bytes_limit: parse_i32(
                "RUNTIME_API_SQL_RESULT_BYTES_LIMIT",
                raw.runtime_api_sql_result_bytes_limit,
                DEFAULT_RUNTIME_API_SQL_RESULT_BYTES_LIMIT,
            )?,
            llm_request_events_retention_days: parse_i32(
                "LLM_REQUEST_EVENTS_RETENTION_DAYS",
                raw.llm_request_events_retention_days,
                DEFAULT_LLM_REQUEST_EVENTS_RETENTION_DAYS,
            )?,
            llm_run_buffer_capacity: parse_i32(
                "LLM_RUN_BUFFER_CAPACITY",
                raw.llm_run_buffer_capacity,
                DEFAULT_LLM_RUN_BUFFER_CAPACITY,
            )?,
            llm_raw_body_persist_enabled: parse_bool(
                "LLM_RAW_BODY_PERSIST_ENABLED",
                raw.llm_raw_body_persist_enabled,
                DEFAULT_LLM_RAW_BODY_PERSIST_ENABLED,
            )?,
            llm_raw_body_max_bytes: parse_i32(
                "LLM_RAW_BODY_MAX_BYTES",
                raw.llm_raw_body_max_bytes,
                DEFAULT_LLM_RAW_BODY_MAX_BYTES,
            )?,
            llm_raw_body_retention_hours: parse_i32(
                "LLM_RAW_BODY_RETENTION_HOURS",
                raw.llm_raw_body_retention_hours,
                DEFAULT_LLM_RAW_BODY_RETENTION_HOURS,
            )?,
            cert_file: raw
                .runtime_api_cert_file
                .unwrap_or_else(|| DEFAULT_RUNTIME_API_CERT_FILE.to_owned()),
            key_file: raw
                .runtime_api_key_file
                .unwrap_or_else(|| DEFAULT_RUNTIME_API_KEY_FILE.to_owned()),
            tls_public_key_pin: raw
                .runtime_api_tls_public_key_pin
                .unwrap_or_else(|| DEFAULT_RUNTIME_API_TLS_PUBLIC_KEY_PIN.to_owned()),
        };
        validate_runtime_api(&runtime_api)?;
        let bind_addr = raw
            .openplotva_bind_addr
            .unwrap_or_else(|| format!("{webapp_host}:{webapp_port}"));
        let log_level = raw
            .log_level
            .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_owned());
        let log_filter = raw.openplotva_log_filter.unwrap_or_else(|| {
            if log_level == DEFAULT_LOG_LEVEL {
                DEFAULT_LOG_FILTER.to_owned()
            } else {
                format!("openplotva={log_level},tower_http={log_level}")
            }
        });
        let persistent_queue = PersistentQueueConfig {
            enabled: parse_bool(
                "PERSISTENT_QUEUE_ENABLED",
                raw.persistent_queue_enabled,
                DEFAULT_PERSISTENT_QUEUE_ENABLED,
            )?,
            heartbeat_interval_seconds: parse_i32(
                "PERSISTENT_QUEUE_HEARTBEAT_INTERVAL_SECONDS",
                raw.persistent_queue_heartbeat_interval_seconds,
                DEFAULT_PERSISTENT_QUEUE_HEARTBEAT_INTERVAL_SECONDS,
            )?,
            recovery_interval_seconds: parse_i32(
                "PERSISTENT_QUEUE_RECOVERY_INTERVAL_SECONDS",
                raw.persistent_queue_recovery_interval_seconds,
                DEFAULT_PERSISTENT_QUEUE_RECOVERY_INTERVAL_SECONDS,
            )?,
            cleanup_interval_seconds: parse_i32(
                "PERSISTENT_QUEUE_CLEANUP_INTERVAL_SECONDS",
                raw.persistent_queue_cleanup_interval_seconds,
                DEFAULT_PERSISTENT_QUEUE_CLEANUP_INTERVAL_SECONDS,
            )?,
            default_processing_timeout_seconds: parse_i32(
                "PERSISTENT_QUEUE_DEFAULT_PROCESSING_TIMEOUT_SECONDS",
                raw.persistent_queue_default_processing_timeout_seconds,
                DEFAULT_PERSISTENT_QUEUE_DEFAULT_PROCESSING_TIMEOUT_SECONDS,
            )?,
            max_retries: parse_i32(
                "PERSISTENT_QUEUE_MAX_RETRIES",
                raw.persistent_queue_max_retries,
                DEFAULT_PERSISTENT_QUEUE_MAX_RETRIES,
            )?,
            completed_job_retention_days: parse_i32(
                "PERSISTENT_QUEUE_COMPLETED_JOB_RETENTION_DAYS",
                raw.persistent_queue_completed_job_retention_days,
                DEFAULT_PERSISTENT_QUEUE_COMPLETED_JOB_RETENTION_DAYS,
            )?,
            message_cleanup_interval_seconds: parse_i32(
                "PERSISTENT_QUEUE_MESSAGE_CLEANUP_INTERVAL_SECONDS",
                raw.persistent_queue_message_cleanup_interval_seconds,
                DEFAULT_PERSISTENT_QUEUE_MESSAGE_CLEANUP_INTERVAL_SECONDS,
            )?,
            job_message_cleanup_minutes: parse_i32(
                "PERSISTENT_QUEUE_JOB_MESSAGE_CLEANUP_MINUTES",
                raw.persistent_queue_job_message_cleanup_minutes,
                DEFAULT_PERSISTENT_QUEUE_JOB_MESSAGE_CLEANUP_MINUTES,
            )?,
            control_workers: parse_i32(
                "PERSISTENT_QUEUE_CONTROL_WORKERS",
                raw.persistent_queue_control_workers,
                DEFAULT_PERSISTENT_QUEUE_CONTROL_WORKERS,
            )?,
            text_workers: parse_i32(
                "PERSISTENT_QUEUE_TEXT_WORKERS",
                raw.persistent_queue_text_workers,
                DEFAULT_PERSISTENT_QUEUE_TEXT_WORKERS,
            )?,
            dialog_aifarm_workers: parse_i32(
                "PERSISTENT_QUEUE_DIALOG_AIFARM_WORKERS",
                raw.persistent_queue_dialog_aifarm_workers,
                DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_WORKERS,
            )?,
            dialog_workers_cap: parse_i32(
                "PERSISTENT_QUEUE_DIALOG_WORKERS_CAP",
                raw.persistent_queue_dialog_workers_cap,
                DEFAULT_PERSISTENT_QUEUE_DIALOG_WORKERS_CAP,
            )?,
            dialog_unpooled_share: parse_i32(
                "PERSISTENT_QUEUE_DIALOG_UNPOOLED_SHARE",
                raw.persistent_queue_dialog_unpooled_share,
                DEFAULT_PERSISTENT_QUEUE_DIALOG_UNPOOLED_SHARE,
            )?,
            dialog_aifarm_fallback_high_watermark: parse_i32(
                "PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_HIGH_WATERMARK",
                raw.persistent_queue_dialog_aifarm_fallback_high_watermark,
                DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_HIGH_WATERMARK,
            )?,
            dialog_aifarm_fallback_low_watermark: parse_i32(
                "PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_LOW_WATERMARK",
                raw.persistent_queue_dialog_aifarm_fallback_low_watermark,
                DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_LOW_WATERMARK,
            )?,
            dialog_aifarm_fallback_poll_interval_seconds: parse_i32(
                "PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_POLL_INTERVAL_SECONDS",
                raw.persistent_queue_dialog_aifarm_fallback_poll_interval_seconds,
                DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_POLL_INTERVAL_SECONDS,
            )?,
            image_regular_workers: parse_i32(
                "PERSISTENT_QUEUE_IMAGE_REGULAR_WORKERS",
                raw.persistent_queue_image_regular_workers,
                DEFAULT_PERSISTENT_QUEUE_IMAGE_REGULAR_WORKERS,
            )?,
            image_vip_workers: parse_i32(
                "PERSISTENT_QUEUE_IMAGE_VIP_WORKERS",
                raw.persistent_queue_image_vip_workers,
                DEFAULT_PERSISTENT_QUEUE_IMAGE_VIP_WORKERS,
            )?,
            music_vip_workers: parse_i32(
                "PERSISTENT_QUEUE_MUSIC_VIP_WORKERS",
                raw.persistent_queue_music_vip_workers,
                DEFAULT_PERSISTENT_QUEUE_MUSIC_VIP_WORKERS,
            )?,
            memory_consolidation_workers: parse_i32(
                "PERSISTENT_QUEUE_MEMORY_CONSOLIDATION_WORKERS",
                raw.persistent_queue_memory_consolidation_workers,
                DEFAULT_PERSISTENT_QUEUE_MEMORY_CONSOLIDATION_WORKERS,
            )?,
            memory_workers_cap: parse_i32(
                "PERSISTENT_QUEUE_MEMORY_WORKERS_CAP",
                raw.persistent_queue_memory_workers_cap,
                DEFAULT_PERSISTENT_QUEUE_MEMORY_WORKERS_CAP,
            )?,
            placeholder_cleanup_interval_seconds: parse_i32(
                "PERSISTENT_QUEUE_PLACEHOLDER_CLEANUP_INTERVAL_SECONDS",
                raw.persistent_queue_placeholder_cleanup_interval_seconds,
                DEFAULT_PERSISTENT_QUEUE_PLACEHOLDER_CLEANUP_INTERVAL_SECONDS,
            )?,
            placeholder_max_age_seconds: parse_i32(
                "PERSISTENT_QUEUE_PLACEHOLDER_MAX_AGE_SECONDS",
                raw.persistent_queue_placeholder_max_age_seconds,
                DEFAULT_PERSISTENT_QUEUE_PLACEHOLDER_MAX_AGE_SECONDS,
            )?,
            llm_job_max_attempts: parse_i32(
                "LLM_JOB_MAX_ATTEMPTS",
                raw.llm_job_max_attempts,
                DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            )?,
        };
        validate_persistent_queue_config(&persistent_queue)?;
        let dialog_aifarm_pool_models = parse_string_list_or_default(
            raw.dialog_aifarm_pool_models,
            DEFAULT_DIALOG_AIFARM_POOL_MODELS,
        );
        let dialog_aifarm_pool_base_urls = parse_string_list_or_default(
            raw.dialog_aifarm_pool_base_urls,
            DEFAULT_DIALOG_AIFARM_POOL_BASE_URLS,
        );
        if dialog_aifarm_pool_models.len() != dialog_aifarm_pool_base_urls.len() {
            return Err(ConfigError::AifarmPoolPairCount);
        }
        let llm_providers = {
            let names = parse_string_list(raw.llm_provider_names);
            if names.is_empty() {
                Vec::new()
            } else {
                let kinds = split_list_keep_empties(raw.llm_provider_kinds);
                let services = split_list_keep_empties(raw.llm_provider_discovery_service_names);
                let endpoints = split_list_keep_empties(raw.llm_provider_discovery_endpoint_names);
                let models = split_list_keep_empties(raw.llm_provider_models);
                let base_urls = split_list_keep_empties(raw.llm_provider_base_urls);
                let urls = split_list_keep_empties(raw.llm_provider_urls);
                let api_keys = split_list_keep_empties(raw.llm_provider_api_keys);
                let include_reasoning = split_list_keep_empties(raw.llm_provider_include_reasoning);
                let enable_thinking = split_list_keep_empties(raw.llm_provider_enable_thinking);
                let max_tokens = split_list_keep_empties(raw.llm_provider_max_tokens);
                let temperatures = split_list_keep_empties(raw.llm_provider_temperatures);
                let task_timeouts = split_list_keep_empties(raw.llm_provider_task_timeout_seconds);
                let mut providers = Vec::with_capacity(names.len());
                for (index, name) in names.into_iter().enumerate() {
                    let elem = |list: &[String]| -> Option<String> {
                        list.get(index)
                            .map(|value| value.trim())
                            .filter(|value| !value.is_empty())
                            .map(ToOwned::to_owned)
                    };
                    providers.push(NamedProviderConfig {
                        name,
                        kind: elem(&kinds).unwrap_or_else(|| DEFAULT_LLM_PROVIDER_KIND.to_owned()),
                        discovery_service_name: elem(&services).unwrap_or_default(),
                        discovery_endpoint_name: elem(&endpoints)
                            .unwrap_or_else(|| DEFAULT_DIALOG_DISCOVERY_ENDPOINT_NAME.to_owned()),
                        model: elem(&models).unwrap_or_default(),
                        base_url: elem(&base_urls).unwrap_or_default(),
                        url: elem(&urls).unwrap_or_default(),
                        api_key: elem(&api_keys).unwrap_or_default(),
                        include_reasoning: elem(&include_reasoning)
                            .map(|value| {
                                parse_bool("LLM_PROVIDERS_INCLUDE_REASONING", Some(value), false)
                            })
                            .transpose()?,
                        enable_thinking: elem(&enable_thinking)
                            .map(|value| {
                                parse_bool("LLM_PROVIDERS_ENABLE_THINKING", Some(value), false)
                            })
                            .transpose()?,
                        max_tokens: parse_i32(
                            "LLM_PROVIDERS_MAX_TOKENS",
                            elem(&max_tokens),
                            DEFAULT_LLM_PROVIDER_MAX_TOKENS,
                        )?,
                        temperature: elem(&temperatures)
                            .map(|value| parse_f64("LLM_PROVIDERS_TEMPERATURES", Some(value), 0.0))
                            .transpose()?,
                        task_timeout_seconds: parse_i32(
                            "LLM_PROVIDERS_TASK_TIMEOUT_SECONDS",
                            elem(&task_timeouts),
                            DEFAULT_LLM_PROVIDER_TASK_TIMEOUT_SECONDS,
                        )?,
                    });
                }
                providers
            }
        };
        let history_summary_provider = raw
            .genkit_history_summary_provider
            .unwrap_or_else(|| DEFAULT_HISTORY_SUMMARY_PROVIDER.to_owned());
        validate_history_summary_provider(&history_summary_provider)?;
        let history_summary_timeout_seconds = parse_i32(
            "GENKIT_HISTORY_SUMMARY_TIMEOUT_SECONDS",
            raw.genkit_history_summary_timeout_seconds,
            DEFAULT_HISTORY_SUMMARY_TIMEOUT_SECONDS,
        )?;
        if history_summary_timeout_seconds <= 0 {
            return Err(ConfigError::NonPositiveInteger {
                name: "GENKIT_HISTORY_SUMMARY_TIMEOUT_SECONDS",
                value: history_summary_timeout_seconds,
            });
        }
        let openrouter_request_timeout_seconds = parse_i32(
            "OPENROUTER_REQUEST_TIMEOUT_SECONDS",
            raw.openrouter_request_timeout_seconds,
            DEFAULT_OPENROUTER_REQUEST_TIMEOUT_SECONDS,
        )?;
        if raw
            .openrouter_key
            .as_deref()
            .is_some_and(|key| !key.trim().is_empty())
            && openrouter_request_timeout_seconds <= 0
        {
            return Err(ConfigError::NonPositiveInteger {
                name: "OPENROUTER_REQUEST_TIMEOUT_SECONDS",
                value: openrouter_request_timeout_seconds,
            });
        }
        let serper = SerperConfig {
            api_key: raw.serper_api_key.unwrap_or_default(),
            timeout_seconds: parse_i32(
                "SERPER_TIMEOUT_SECONDS",
                raw.serper_timeout_seconds,
                DEFAULT_SERPER_TIMEOUT_SECONDS,
            )?,
        };
        let boogu_image_timeout_seconds = validate_positive_i32(
            "BOOGU_IMAGE_TIMEOUT_SECONDS",
            parse_i32(
                "BOOGU_IMAGE_TIMEOUT_SECONDS",
                raw.boogu_image_timeout_seconds,
                DEFAULT_BOOGU_IMAGE_TIMEOUT_SECONDS,
            )?,
        )?;
        let image_providers = ImageProvidersConfig {
            boogu_turbo: BooguImageTurboConfig {
                enabled: parse_bool(
                    "BOOGU_IMAGE_TURBO_ENABLED",
                    raw.boogu_image_turbo_enabled,
                    DEFAULT_BOOGU_IMAGE_TURBO_ENABLED,
                )?,
                base_url: raw
                    .boogu_image_turbo_base_url
                    .and_then(|value| parse_scalar_value(Some(value)))
                    .unwrap_or_else(|| DEFAULT_BOOGU_IMAGE_TURBO_BASE_URL.to_owned()),
                timeout_seconds: boogu_image_timeout_seconds,
                steps: validate_positive_i32(
                    "BOOGU_IMAGE_TURBO_STEPS",
                    parse_i32(
                        "BOOGU_IMAGE_TURBO_STEPS",
                        raw.boogu_image_turbo_steps,
                        DEFAULT_BOOGU_IMAGE_TURBO_STEPS,
                    )?,
                )?,
                resolution: raw
                    .boogu_image_turbo_resolution
                    .and_then(|value| parse_scalar_value(Some(value)))
                    .unwrap_or_else(|| DEFAULT_BOOGU_IMAGE_TURBO_RESOLUTION.to_owned()),
                width: validate_positive_i32(
                    "BOOGU_IMAGE_TURBO_WIDTH",
                    parse_i32(
                        "BOOGU_IMAGE_TURBO_WIDTH",
                        raw.boogu_image_turbo_width,
                        DEFAULT_BOOGU_IMAGE_TURBO_WIDTH,
                    )?,
                )?,
                height: validate_positive_i32(
                    "BOOGU_IMAGE_TURBO_HEIGHT",
                    parse_i32(
                        "BOOGU_IMAGE_TURBO_HEIGHT",
                        raw.boogu_image_turbo_height,
                        DEFAULT_BOOGU_IMAGE_TURBO_HEIGHT,
                    )?,
                )?,
            },
            boogu_edit_turbo: BooguImageEditTurboConfig {
                enabled: parse_bool(
                    "BOOGU_IMAGE_EDIT_TURBO_ENABLED",
                    raw.boogu_image_edit_turbo_enabled,
                    DEFAULT_BOOGU_IMAGE_EDIT_TURBO_ENABLED,
                )?,
                base_url: raw
                    .boogu_image_edit_turbo_base_url
                    .and_then(|value| parse_scalar_value(Some(value)))
                    .unwrap_or_else(|| DEFAULT_BOOGU_IMAGE_EDIT_TURBO_BASE_URL.to_owned()),
                timeout_seconds: boogu_image_timeout_seconds,
                steps: validate_positive_i32(
                    "BOOGU_IMAGE_EDIT_TURBO_STEPS",
                    parse_i32(
                        "BOOGU_IMAGE_EDIT_TURBO_STEPS",
                        raw.boogu_image_edit_turbo_steps,
                        DEFAULT_BOOGU_IMAGE_EDIT_TURBO_STEPS,
                    )?,
                )?,
                resolution_category: raw
                    .boogu_image_edit_turbo_resolution_category
                    .and_then(|value| parse_scalar_value(Some(value)))
                    .unwrap_or_else(|| {
                        DEFAULT_BOOGU_IMAGE_EDIT_TURBO_RESOLUTION_CATEGORY.to_owned()
                    }),
                resolution: raw
                    .boogu_image_edit_turbo_resolution
                    .and_then(|value| parse_scalar_value(Some(value)))
                    .unwrap_or_else(|| DEFAULT_BOOGU_IMAGE_EDIT_TURBO_RESOLUTION.to_owned()),
                width: validate_positive_i32(
                    "BOOGU_IMAGE_EDIT_TURBO_WIDTH",
                    parse_i32(
                        "BOOGU_IMAGE_EDIT_TURBO_WIDTH",
                        raw.boogu_image_edit_turbo_width,
                        DEFAULT_BOOGU_IMAGE_EDIT_TURBO_WIDTH,
                    )?,
                )?,
                height: validate_positive_i32(
                    "BOOGU_IMAGE_EDIT_TURBO_HEIGHT",
                    parse_i32(
                        "BOOGU_IMAGE_EDIT_TURBO_HEIGHT",
                        raw.boogu_image_edit_turbo_height,
                        DEFAULT_BOOGU_IMAGE_EDIT_TURBO_HEIGHT,
                    )?,
                )?,
            },
        };

        let postgres = PostgresConfig {
            host: raw
                .db_postgres_host
                .unwrap_or_else(|| DEFAULT_POSTGRES_HOST.to_owned()),
            port: parse_u16(
                "DB_POSTGRES_PORT",
                raw.db_postgres_port,
                DEFAULT_POSTGRES_PORT,
            )?,
            user: raw
                .db_postgres_user
                .unwrap_or_else(|| DEFAULT_POSTGRES_USER.to_owned()),
            password: raw
                .db_postgres_password
                .unwrap_or_else(|| DEFAULT_POSTGRES_PASSWORD.to_owned()),
            database: raw
                .db_postgres_db
                .unwrap_or_else(|| DEFAULT_POSTGRES_DATABASE.to_owned()),
            ssl_mode: raw
                .db_postgres_ssl_mode
                .unwrap_or_else(|| DEFAULT_POSTGRES_SSL_MODE.to_owned()),
        };

        Ok(Self {
            server: ServerConfig {
                host: webapp_host,
                port: webapp_port,
                bind_addr,
                url: raw
                    .webapp_url
                    .unwrap_or_else(|| DEFAULT_WEBAPP_URL.to_owned()),
                require_settings_init_data: parse_bool(
                    "SETTINGS_REQUIRE_INIT_DATA",
                    raw.settings_require_init_data,
                    DEFAULT_SETTINGS_REQUIRE_INIT_DATA,
                )?,
            },
            runtime_api,
            observability: ObservabilityConfig {
                log_level,
                log_filter,
            },
            database: DatabaseConfig { postgres },
            redis: RedisConfig {
                host: raw
                    .redis_host
                    .unwrap_or_else(|| DEFAULT_REDIS_HOST.to_owned()),
                port: parse_u16("REDIS_PORT", raw.redis_port, DEFAULT_REDIS_PORT)?,
                password: raw.redis_password.unwrap_or_default(),
                db: parse_i64("REDIS_DB", raw.redis_db, DEFAULT_REDIS_DB)?,
            },
            update_queue: UpdateQueueConfig {
                backend: parse_update_queue_backend(raw.update_queue_backend)?,
            },
            bot: BotConfig {
                key: raw.bot_key.filter(|value| !value.is_empty()),
                api_base_url: raw.bot_api_base_url.unwrap_or_default(),
                webhook: BotWebhookConfig {
                    enabled: parse_bool(
                        "BOT_WEBHOOK_ENABLED",
                        raw.bot_webhook_enabled,
                        DEFAULT_BOT_WEBHOOK_ENABLED,
                    )?,
                    url: raw.bot_webhook_url.unwrap_or_default(),
                    cert_file: raw.bot_webhook_cert_file.unwrap_or_default(),
                    key_file: raw.bot_webhook_key_file.unwrap_or_default(),
                    secret_token: raw.bot_webhook_secret_token.unwrap_or_default(),
                    update_buffer_size: parse_usize(
                        "BOT_WEBHOOK_UPDATE_BUFFER_SIZE",
                        raw.bot_webhook_update_buffer_size,
                        DEFAULT_BOT_WEBHOOK_UPDATE_BUFFER_SIZE,
                    )?,
                },
                activity_pulse: TelegramActivityPulseConfig {
                    enabled: parse_bool(
                        "TELEGRAM_ACTIVITY_PULSE_ENABLED",
                        raw.telegram_activity_pulse_enabled,
                        DEFAULT_TELEGRAM_ACTIVITY_PULSE_ENABLED,
                    )?,
                    interval_ms: parse_i32(
                        "TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS",
                        raw.telegram_activity_pulse_interval_ms,
                        DEFAULT_TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS,
                    )?
                    .max(MIN_TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS),
                    initial_delay_ms: parse_i32(
                        "TELEGRAM_ACTIVITY_PULSE_INITIAL_DELAY_MS",
                        raw.telegram_activity_pulse_initial_delay_ms,
                        DEFAULT_TELEGRAM_ACTIVITY_PULSE_INITIAL_DELAY_MS,
                    )?
                    .max(0),
                },
                debug: parse_bool("BOT_DEBUG", raw.bot_debug, DEFAULT_BOT_DEBUG)?,
            },
            admins: AdminConfig {
                admin_ids: parse_i64_list_or_default(
                    "ADMINS_ADMIN_IDS",
                    raw.admins_admin_ids,
                    DEFAULT_ADMINS_ADMIN_IDS,
                )?,
            },
            vip: VipConfig {
                chat_id: {
                    let var_name = if raw.vip_chat_id.is_some() {
                        "VIP_CHAT_ID"
                    } else {
                        "CHAT_ID"
                    };
                    parse_i64(
                        var_name,
                        raw.vip_chat_id.or(raw.legacy_vip_chat_id),
                        DEFAULT_VIP_CHAT_ID,
                    )?
                },
            },
            subscription_sync: SubscriptionSyncConfig {
                enabled: parse_bool(
                    "SUBSCRIPTION_SYNC_ENABLED",
                    raw.subscription_sync_enabled,
                    DEFAULT_SUBSCRIPTION_SYNC_ENABLED,
                )?,
                interval_seconds: parse_i32(
                    "SUBSCRIPTION_SYNC_INTERVAL_SECONDS",
                    raw.subscription_sync_interval_seconds,
                    DEFAULT_SUBSCRIPTION_SYNC_INTERVAL_SECONDS,
                )?,
                dry_run: parse_bool(
                    "SUBSCRIPTION_SYNC_DRY_RUN",
                    raw.subscription_sync_dry_run,
                    DEFAULT_SUBSCRIPTION_SYNC_DRY_RUN,
                )?,
                page_limit: parse_i32(
                    "SUBSCRIPTION_SYNC_PAGE_LIMIT",
                    raw.subscription_sync_page_limit,
                    DEFAULT_SUBSCRIPTION_SYNC_PAGE_LIMIT,
                )?,
            },
            persistent_queue,
            market_rates: MarketRatesConfig {
                timeout_seconds: parse_i32(
                    "MARKET_RATES_TIMEOUT_SECONDS",
                    raw.market_rates_timeout_seconds.or(raw.rbc_timeout_seconds),
                    DEFAULT_MARKET_RATES_TIMEOUT_SECONDS,
                )?,
            },
            serper,
            translation: TranslationConfig {
                deepl: DeeplConfig {
                    key: raw.deepl_key.unwrap_or_default(),
                    url: raw.deepl_url.unwrap_or_default(),
                },
            },
            google_ai: GoogleAiConfig {
                key: raw.googleai_key.unwrap_or_default(),
                key_stats_file: raw
                    .googleai_key_stats_file
                    .unwrap_or_else(|| "googleai_key_stats.json".to_owned()),
            },
            open_router: OpenRouterConfig {
                key: raw.openrouter_key.unwrap_or_default(),
                request_timeout_seconds: openrouter_request_timeout_seconds,
            },
            uploader: UploaderConfig {
                base_url: raw.uploader_url.unwrap_or_default(),
                secret: raw.uploader_secret.unwrap_or_default(),
                timeout_seconds: parse_i32(
                    "PLOTVA_UPLOADER_TIMEOUT_SECONDS",
                    raw.uploader_timeout_seconds,
                    DEFAULT_UPLOADER_TIMEOUT_SECONDS,
                )?,
            },
            white_circle: WhiteCircleConfig {
                enabled: parse_bool("WHITECIRCLE_ENABLED", raw.whitecircle_enabled, false)?,
                api_key: raw.whitecircle_api_key.unwrap_or_default(),
                deployment_id: raw.whitecircle_deployment_id.unwrap_or_default(),
                whitecircle_checks_retention_days: parse_i32(
                    "WHITECIRCLE_CHECKS_RETENTION_DAYS",
                    raw.whitecircle_checks_retention_days,
                    DEFAULT_WHITECIRCLE_CHECKS_RETENTION_DAYS,
                )?,
            },
            llm: LlmConfig {
                discovery: DiscoveryConfig {
                    base_url: raw
                        .discovery_base_url
                        .unwrap_or_else(|| DEFAULT_DISCOVERY_BASE_URL.to_owned()),
                },
                genkit: GenkitConfig {
                    default_model: raw.genkit_default_model.unwrap_or_default(),
                },
                dialog: DialogConfig {
                    provider: raw
                        .dialog_provider
                        .unwrap_or_else(|| DEFAULT_DIALOG_PROVIDER.to_owned()),
                    fallback_provider: raw
                        .dialog_fallback_provider
                        .unwrap_or_else(|| DEFAULT_DIALOG_FALLBACK_PROVIDER.to_owned()),
                    model: raw
                        .dialog_model
                        .unwrap_or_else(|| DEFAULT_DIALOG_MODEL.to_owned()),
                    url: raw.dialog_url.unwrap_or_default(),
                    api_key: raw
                        .dialog_api_key
                        .unwrap_or_else(|| DEFAULT_DIALOG_API_KEY.to_owned()),
                    discovery_service_name: raw
                        .dialog_discovery_service_name
                        .unwrap_or_else(|| DEFAULT_DIALOG_DISCOVERY_SERVICE_NAME.to_owned()),
                    discovery_endpoint_name: raw
                        .dialog_discovery_endpoint_name
                        .unwrap_or_else(|| DEFAULT_DIALOG_DISCOVERY_ENDPOINT_NAME.to_owned()),
                    aifarm_enable_thinking: parse_bool(
                        "DIALOG_AIFARM_ENABLE_THINKING",
                        raw.dialog_aifarm_enable_thinking,
                        false,
                    )?,
                    aifarm_max_tokens: parse_i32(
                        "DIALOG_AIFARM_MAX_TOKENS",
                        raw.dialog_aifarm_max_tokens,
                        1024,
                    )?,
                    aifarm_random_max_tokens: parse_i32(
                        "DIALOG_AIFARM_RANDOM_MAX_TOKENS",
                        raw.dialog_aifarm_random_max_tokens,
                        768,
                    )?,
                    aifarm_default_max_tokens: parse_i32(
                        "DIALOG_AIFARM_DEFAULT_MAX_TOKENS",
                        raw.dialog_aifarm_default_max_tokens,
                        1024,
                    )?,
                    aifarm_long_max_tokens: parse_i32(
                        "DIALOG_AIFARM_LONG_MAX_TOKENS",
                        raw.dialog_aifarm_long_max_tokens,
                        1024,
                    )?,
                    aifarm_temperature: parse_f64(
                        "DIALOG_AIFARM_TEMPERATURE",
                        raw.dialog_aifarm_temperature,
                        0.2,
                    )?,
                    aifarm_repeat_penalty: parse_f64(
                        "DIALOG_AIFARM_REPEAT_PENALTY",
                        raw.dialog_aifarm_repeat_penalty,
                        1.1,
                    )?,
                    // Anti-loop defaults: DRY plus moderate frequency/presence penalties
                    // break the phrase-level repetition ("Главное, чтобы…" loops) that low
                    // temperature alone encourages. Temperature stays low on purpose so
                    // tool-calling remains reliable; DRY carries the anti-repetition load and
                    // the backend's default DRY sequence breakers spare structural tokens.
                    aifarm_frequency_penalty: clamp_penalty(
                        parse_f64(
                            "DIALOG_AIFARM_FREQUENCY_PENALTY",
                            raw.dialog_aifarm_frequency_penalty,
                            0.3,
                        )?,
                        0.3,
                    ),
                    aifarm_presence_penalty: clamp_penalty(
                        parse_f64(
                            "DIALOG_AIFARM_PRESENCE_PENALTY",
                            raw.dialog_aifarm_presence_penalty,
                            0.2,
                        )?,
                        0.2,
                    ),
                    aifarm_dry_multiplier: parse_f64(
                        "DIALOG_AIFARM_DRY_MULTIPLIER",
                        raw.dialog_aifarm_dry_multiplier,
                        0.8,
                    )?,
                    aifarm_dry_base: parse_f64(
                        "DIALOG_AIFARM_DRY_BASE",
                        raw.dialog_aifarm_dry_base,
                        1.75,
                    )?,
                    aifarm_dry_allowed_length: parse_i32(
                        "DIALOG_AIFARM_DRY_ALLOWED_LENGTH",
                        raw.dialog_aifarm_dry_allowed_length,
                        2,
                    )?,
                    request_timeout_seconds: parse_i32(
                        "DIALOG_REQUEST_TIMEOUT_SECONDS",
                        raw.dialog_request_timeout_seconds,
                        30,
                    )?,
                    poll_interval_seconds: parse_i32(
                        "DIALOG_POLL_INTERVAL_SECONDS",
                        raw.dialog_poll_interval_seconds,
                        1,
                    )?,
                    task_timeout_seconds: parse_i32(
                        "DIALOG_TASK_TIMEOUT_SECONDS",
                        raw.dialog_task_timeout_seconds,
                        720,
                    )?,
                    aifarm_capacity_wait_seconds: parse_i32(
                        "DIALOG_AIFARM_CAPACITY_WAIT_SECONDS",
                        raw.dialog_aifarm_capacity_wait_seconds,
                        60,
                    )?,
                    aifarm_capacity_poll_seconds: parse_i32(
                        "DIALOG_AIFARM_CAPACITY_POLL_SECONDS",
                        raw.dialog_aifarm_capacity_poll_seconds,
                        1,
                    )?,
                    aifarm_pool_models: dialog_aifarm_pool_models,
                    aifarm_pool_base_urls: dialog_aifarm_pool_base_urls,
                    aifarm_pool_api_key: raw.dialog_aifarm_pool_api_key.unwrap_or_default(),
                    aifarm_pool_primary_capacity_wait_ms: parse_i32(
                        "DIALOG_AIFARM_POOL_PRIMARY_CAPACITY_WAIT_MS",
                        raw.dialog_aifarm_pool_primary_capacity_wait_ms,
                        500,
                    )?,
                    turn_budget_secs: parse_i32(
                        "DIALOG_TURN_BUDGET_SECS",
                        raw.dialog_turn_budget_secs,
                        120,
                    )?,
                    turn_max_queue_age_secs: parse_i32(
                        "DIALOG_TURN_MAX_QUEUE_AGE_SECS",
                        raw.dialog_turn_max_queue_age_secs,
                        600,
                    )?,
                    turn_max_regenerations: parse_i32(
                        "DIALOG_TURN_MAX_REGENERATIONS",
                        raw.dialog_turn_max_regenerations,
                        2,
                    )?,
                    terminal_reaction_emoji: raw
                        .dialog_terminal_reaction_emoji
                        .map(|value| value.trim().to_owned())
                        .filter(|value| !value.is_empty())
                        .unwrap_or_else(|| "🤔".to_owned()),
                    terminal_signal_max_age_secs: parse_i32(
                        "DIALOG_TERMINAL_SIGNAL_MAX_AGE_SECS",
                        raw.dialog_terminal_signal_max_age_secs,
                        600,
                    )?,
                    image_delivery_timeout_secs: parse_i32(
                        "DIALOG_IMAGE_DELIVERY_TIMEOUT_SECS",
                        raw.dialog_image_delivery_timeout_secs,
                        1200,
                    )?,
                    music_delivery_timeout_secs: parse_i32(
                        "DIALOG_MUSIC_DELIVERY_TIMEOUT_SECS",
                        raw.dialog_music_delivery_timeout_secs,
                        1800,
                    )?,
                    obligation_watch_interval_secs: parse_i32(
                        "DIALOG_OBLIGATION_WATCH_INTERVAL_SECS",
                        raw.dialog_obligation_watch_interval_secs,
                        15,
                    )?,
                    session_max_iterations: parse_i32(
                        "DIALOG_SESSION_MAX_ITERATIONS",
                        raw.dialog_session_max_iterations,
                        8,
                    )?,
                    session_max_messages: parse_i32(
                        "DIALOG_SESSION_MAX_MESSAGES",
                        raw.dialog_session_max_messages,
                        4,
                    )?,
                    session_tool_extension_secs: parse_i32(
                        "DIALOG_SESSION_TOOL_EXTENSION_SECS",
                        raw.dialog_session_tool_extension_secs,
                        60,
                    )?,
                    session_hard_cap_secs: parse_i32(
                        "DIALOG_SESSION_HARD_CAP_SECS",
                        raw.dialog_session_hard_cap_secs,
                        300,
                    )?,
                    session_max_draws: parse_i32(
                        "DIALOG_SESSION_MAX_DRAWS",
                        raw.dialog_session_max_draws,
                        1,
                    )?,
                    session_max_songs: parse_i32(
                        "DIALOG_SESSION_MAX_SONGS",
                        raw.dialog_session_max_songs,
                        1,
                    )?,
                    vmlx_url: raw
                        .dialog_vmlx_url
                        .unwrap_or_else(|| DEFAULT_DIALOG_VMLX_URL.to_owned()),
                    vmlx_api_key: raw
                        .dialog_vmlx_api_key
                        .unwrap_or_else(|| DEFAULT_DIALOG_VMLX_API_KEY.to_owned()),
                    vmlx_model: raw
                        .dialog_vmlx_model
                        .unwrap_or_else(|| DEFAULT_DIALOG_VMLX_MODEL.to_owned()),
                    nvidia_url: raw
                        .dialog_nvidia_url
                        .unwrap_or_else(|| DEFAULT_DIALOG_NVIDIA_URL.to_owned()),
                    nvidia_api_key: raw.dialog_nvidia_api_key.unwrap_or_default(),
                    nvidia_model: raw
                        .dialog_nvidia_model
                        .unwrap_or_else(|| DEFAULT_DIALOG_NVIDIA_MODEL.to_owned()),
                    nvidia_max_tokens: parse_i32(
                        "DIALOG_NVIDIA_MAX_TOKENS",
                        raw.dialog_nvidia_max_tokens,
                        16384,
                    )?,
                    nvidia_temperature: parse_f64(
                        "DIALOG_NVIDIA_TEMPERATURE",
                        raw.dialog_nvidia_temperature,
                        1.0,
                    )?,
                    nvidia_top_p: parse_f64("DIALOG_NVIDIA_TOP_P", raw.dialog_nvidia_top_p, 0.95)?,
                    nvidia_enable_thinking: parse_bool(
                        "DIALOG_NVIDIA_ENABLE_THINKING",
                        raw.dialog_nvidia_enable_thinking,
                        true,
                    )?,
                    nvidia_include_reasoning: parse_bool(
                        "DIALOG_NVIDIA_INCLUDE_REASONING",
                        raw.dialog_nvidia_include_reasoning,
                        false,
                    )?,
                },
                history_summary: HistorySummaryConfig {
                    provider: history_summary_provider,
                    model: raw.genkit_history_summary_model.unwrap_or_default(),
                    timeout_seconds: history_summary_timeout_seconds,
                    chat_history_retention_days: parse_i32(
                        "CHAT_HISTORY_RETENTION_DAYS",
                        raw.chat_history_retention_days,
                        DEFAULT_CHAT_HISTORY_RETENTION_DAYS,
                    )?,
                },
                providers: llm_providers,
                agentic: AgenticConfig {
                    reasoner_provider: parse_scalar_value(raw.llm_agent_reasoner_provider)
                        .unwrap_or_else(|| DEFAULT_AGENT_REASONER_PROVIDER.to_owned()),
                    song_enabled: parse_bool(
                        "LLM_AGENTIC_SONG_ENABLED",
                        raw.llm_agentic_song_enabled,
                        DEFAULT_AGENTIC_SONG_ENABLED,
                    )?,
                    image_enabled: parse_bool(
                        "LLM_AGENTIC_IMAGE_ENABLED",
                        raw.llm_agentic_image_enabled,
                        DEFAULT_AGENTIC_IMAGE_ENABLED,
                    )?,
                },
            },
            vision: VisionConfig {
                discovery_service_name: raw
                    .vision_discovery_service_name
                    .unwrap_or_else(|| DEFAULT_VISION_DISCOVERY_SERVICE_NAME.to_owned()),
                discovery_endpoint_name: raw
                    .vision_discovery_endpoint_name
                    .unwrap_or_else(|| DEFAULT_VISION_DISCOVERY_ENDPOINT_NAME.to_owned()),
                model: raw
                    .vision_model
                    .unwrap_or_else(|| DEFAULT_VISION_MODEL.to_owned()),
                telegram_files_retention_days: parse_i32(
                    "TELEGRAM_FILES_RETENTION_DAYS",
                    raw.telegram_files_retention_days,
                    DEFAULT_TELEGRAM_FILES_RETENTION_DAYS,
                )?,
                max_tokens: parse_i32(
                    "VISION_MAX_TOKENS",
                    raw.vision_max_tokens,
                    DEFAULT_VISION_MAX_TOKENS,
                )?,
                temperature: parse_f64(
                    "VISION_TEMPERATURE",
                    raw.vision_temperature,
                    DEFAULT_VISION_TEMPERATURE,
                )?,
                direct_image_limit: validate_non_negative_i32(
                    "VISION_DIRECT_IMAGE_LIMIT",
                    parse_i32(
                        "VISION_DIRECT_IMAGE_LIMIT",
                        raw.vision_direct_image_limit,
                        DEFAULT_VISION_DIRECT_IMAGE_LIMIT,
                    )?,
                )?,
                request_timeout_seconds: parse_i32(
                    "VISION_REQUEST_TIMEOUT_SECONDS",
                    raw.vision_request_timeout_seconds,
                    DEFAULT_VISION_REQUEST_TIMEOUT_SECONDS,
                )?,
            },
            image_providers,
            music: MusicConfig {
                acestep: AceStepConfig {
                    enabled: parse_bool(
                        "ACESTEP_ENABLED",
                        raw.acestep_enabled,
                        DEFAULT_ACESTEP_ENABLED,
                    )?,
                    base_url: raw
                        .acestep_base_url
                        .unwrap_or_else(|| DEFAULT_ACESTEP_BASE_URL.to_owned()),
                    api_key: raw.acestep_api_key.unwrap_or_default(),
                    api_mode: raw
                        .acestep_api_mode
                        .unwrap_or_else(|| DEFAULT_ACESTEP_API_MODE.to_owned()),
                    request_timeout_seconds: parse_i32(
                        "ACESTEP_REQUEST_TIMEOUT_SECONDS",
                        raw.acestep_request_timeout_seconds,
                        DEFAULT_ACESTEP_REQUEST_TIMEOUT_SECONDS,
                    )?,
                    poll_interval_seconds: parse_i32(
                        "ACESTEP_POLL_INTERVAL_SECONDS",
                        raw.acestep_poll_interval_seconds,
                        DEFAULT_ACESTEP_POLL_INTERVAL_SECONDS,
                    )?,
                    task_timeout_seconds: parse_i32(
                        "ACESTEP_TASK_TIMEOUT_SECONDS",
                        raw.acestep_task_timeout_seconds,
                        DEFAULT_ACESTEP_TASK_TIMEOUT_SECONDS,
                    )?,
                    audio_format: raw
                        .acestep_audio_format
                        .unwrap_or_else(|| DEFAULT_ACESTEP_AUDIO_FORMAT.to_owned()),
                    model: raw
                        .acestep_model
                        .unwrap_or_else(|| DEFAULT_ACESTEP_MODEL.to_owned()),
                },
            },
            memory: MemoryConfig {
                enabled: parse_bool("MEMORY_ENABLED", raw.memory_enabled, true)?,
                retention_hours: parse_i32(
                    "MEMORY_RETENTION_HOURS",
                    raw.memory_retention_hours,
                    168,
                )?,
                consolidation_provider: raw
                    .memory_consolidation_provider
                    .unwrap_or_else(|| DEFAULT_MEMORY_CONSOLIDATION_PROVIDER.to_owned()),
                consolidation_model: raw
                    .memory_consolidation_model
                    .unwrap_or_else(|| DEFAULT_MEMORY_CONSOLIDATION_MODEL.to_owned()),
                consolidation_timeout_seconds: parse_i32(
                    "MEMORY_CONSOLIDATION_TIMEOUT_SECONDS",
                    raw.memory_consolidation_timeout_seconds,
                    600,
                )?,
                daily_schedule: raw
                    .memory_daily_schedule
                    .unwrap_or_else(|| "03:17".to_owned()),
                daily_window_hours: parse_i32(
                    "MEMORY_DAILY_WINDOW_HOURS",
                    raw.memory_daily_window_hours,
                    24,
                )?,
                worker_concurrency: parse_i32(
                    "MEMORY_WORKER_CONCURRENCY",
                    raw.memory_worker_concurrency,
                    1,
                )?,
                max_messages_per_run: parse_i32(
                    "MEMORY_MAX_MESSAGES_PER_RUN",
                    raw.memory_max_messages_per_run,
                    200,
                )?,
                min_messages_per_run: validate_positive_i32(
                    "MEMORY_MIN_MESSAGES_PER_RUN",
                    parse_i32(
                        "MEMORY_MIN_MESSAGES_PER_RUN",
                        raw.memory_min_messages_per_run,
                        DEFAULT_MEMORY_MIN_MESSAGES_PER_RUN,
                    )?,
                )?,
                max_queued_runs: validate_positive_i32(
                    "MEMORY_MAX_QUEUED_RUNS",
                    parse_i32(
                        "MEMORY_MAX_QUEUED_RUNS",
                        raw.memory_max_queued_runs,
                        DEFAULT_MEMORY_MAX_QUEUED_RUNS,
                    )?,
                )?,
                max_daily_enqueued_runs: validate_positive_i32(
                    "MEMORY_MAX_DAILY_ENQUEUED_RUNS",
                    parse_i32(
                        "MEMORY_MAX_DAILY_ENQUEUED_RUNS",
                        raw.memory_max_daily_enqueued_runs,
                        DEFAULT_MEMORY_MAX_DAILY_ENQUEUED_RUNS,
                    )?,
                )?,
                max_input_tokens: parse_i32(
                    "MEMORY_MAX_INPUT_TOKENS",
                    raw.memory_max_input_tokens,
                    10_000,
                )?,
                tokenizer_model: raw
                    .memory_tokenizer_model
                    .unwrap_or_else(|| DEFAULT_MEMORY_TOKENIZER_MODEL.to_owned()),
                tokenizer_file: raw.memory_tokenizer_file.unwrap_or_default(),
                token_estimator_url: raw
                    .memory_token_estimator_url
                    .unwrap_or_else(|| DEFAULT_MEMORY_TOKEN_ESTIMATOR_URL.to_owned()),
                token_estimator_timeout_seconds: parse_i32(
                    "MEMORY_TOKEN_ESTIMATOR_TIMEOUT_SECONDS",
                    raw.memory_token_estimator_timeout_seconds,
                    2,
                )?,
                embedder_service_name: raw
                    .memory_embedder_service_name
                    .unwrap_or_else(|| DEFAULT_EMBEDDER_DISCOVERY_SERVICE_NAME.to_owned()),
                embedder_endpoint_name: raw
                    .memory_embedder_endpoint_name
                    .unwrap_or_else(|| DEFAULT_EMBEDDER_DISCOVERY_ENDPOINT_NAME.to_owned()),
                embedding_model: raw
                    .memory_embedding_model
                    .unwrap_or_else(|| DEFAULT_MEMORY_EMBEDDING_MODEL.to_owned()),
                embedding_dim: parse_i32("MEMORY_EMBEDDING_DIM", raw.memory_embedding_dim, 512)?,
                aifarm_service_name: raw
                    .memory_aifarm_service_name
                    .unwrap_or_else(|| DEFAULT_DIALOG_DISCOVERY_SERVICE_NAME.to_owned()),
                aifarm_endpoint_name: raw
                    .memory_aifarm_endpoint_name
                    .unwrap_or_else(|| DEFAULT_DIALOG_DISCOVERY_ENDPOINT_NAME.to_owned()),
                aifarm_max_output_tokens: parse_i32(
                    "MEMORY_AIFARM_MAX_OUTPUT_TOKENS",
                    raw.memory_aifarm_max_output_tokens,
                    4000,
                )?,
                aifarm_request_timeout_seconds: parse_i32(
                    "MEMORY_AIFARM_REQUEST_TIMEOUT_SECONDS",
                    raw.memory_aifarm_request_timeout_seconds,
                    660,
                )?,
                aifarm_poll_interval_seconds: parse_i32(
                    "MEMORY_AIFARM_POLL_INTERVAL_SECONDS",
                    raw.memory_aifarm_poll_interval_seconds,
                    1,
                )?,
                aifarm_task_timeout_seconds: parse_i32(
                    "MEMORY_AIFARM_TASK_TIMEOUT_SECONDS",
                    raw.memory_aifarm_task_timeout_seconds,
                    720,
                )?,
                aifarm_capacity_wait_seconds: parse_i32(
                    "MEMORY_AIFARM_CAPACITY_WAIT_SECONDS",
                    raw.memory_aifarm_capacity_wait_seconds,
                    600,
                )?,
                aifarm_capacity_poll_seconds: parse_i32(
                    "MEMORY_AIFARM_CAPACITY_POLL_SECONDS",
                    raw.memory_aifarm_capacity_poll_seconds,
                    1,
                )?,
                aifarm_temperature: parse_f64(
                    "MEMORY_AIFARM_TEMPERATURE",
                    raw.memory_aifarm_temperature,
                    0.2,
                )?,
                aifarm_frequency_penalty: clamp_penalty(
                    parse_f64(
                        "MEMORY_AIFARM_FREQUENCY_PENALTY",
                        raw.memory_aifarm_frequency_penalty,
                        0.3,
                    )?,
                    0.3,
                ),
                aifarm_presence_penalty: clamp_penalty(
                    parse_f64(
                        "MEMORY_AIFARM_PRESENCE_PENALTY",
                        raw.memory_aifarm_presence_penalty,
                        0.3,
                    )?,
                    0.3,
                ),
                aifarm_enable_thinking: parse_bool(
                    "MEMORY_AIFARM_ENABLE_THINKING",
                    raw.memory_aifarm_enable_thinking,
                    false,
                )?,
                subject_merge_enabled: parse_bool(
                    "MEMORY_SUBJECT_MERGE_ENABLED",
                    raw.memory_subject_merge_enabled,
                    true,
                )?,
                subject_merge_min_cards: parse_i32(
                    "MEMORY_SUBJECT_MERGE_MIN_CARDS",
                    raw.memory_subject_merge_min_cards,
                    3,
                )?,
                subject_merge_cooldown_hours: parse_f64(
                    "MEMORY_SUBJECT_MERGE_COOLDOWN_HOURS",
                    raw.memory_subject_merge_cooldown_hours,
                    72.0,
                )?,
                subject_merge_interval_seconds: parse_i32(
                    "MEMORY_SUBJECT_MERGE_INTERVAL_SECONDS",
                    raw.memory_subject_merge_interval_seconds,
                    30,
                )?,
                redaction_enabled: parse_bool(
                    "MEMORY_REDACTION_ENABLED",
                    raw.memory_redaction_enabled,
                    true,
                )?,
                redaction_service_name: raw
                    .memory_redaction_service_name
                    .unwrap_or_else(|| "privacy-filter".to_owned()),
                redaction_endpoint_name: raw
                    .memory_redaction_endpoint_name
                    .unwrap_or_else(|| "redact".to_owned()),
                redaction_timeout_seconds: parse_i32(
                    "MEMORY_REDACTION_TIMEOUT_SECONDS",
                    raw.memory_redaction_timeout_seconds,
                    35,
                )?,
                redaction_task_timeout_seconds: parse_i32(
                    "MEMORY_REDACTION_TASK_TIMEOUT_SECONDS",
                    raw.memory_redaction_task_timeout_seconds,
                    35,
                )?,
                redaction_poll_seconds: parse_i32(
                    "MEMORY_REDACTION_POLL_SECONDS",
                    raw.memory_redaction_poll_seconds,
                    1,
                )?,
                redaction_capacity_wait_seconds: parse_i32(
                    "MEMORY_REDACTION_CAPACITY_WAIT_SECONDS",
                    raw.memory_redaction_capacity_wait_seconds,
                    30,
                )?,
                redaction_capacity_poll_seconds: parse_i32(
                    "MEMORY_REDACTION_CAPACITY_POLL_SECONDS",
                    raw.memory_redaction_capacity_poll_seconds,
                    1,
                )?,
                redaction_categories: parse_string_list_or_default(
                    raw.memory_redaction_categories,
                    DEFAULT_MEMORY_REDACTION_CATEGORIES,
                ),
            },
            shield: ShieldConfig {
                enabled: parse_bool("SHIELD_ENABLED", raw.shield_enabled, DEFAULT_SHIELD_ENABLED)?,
                embedder_service_name: raw
                    .shield_embedder_service_name
                    .unwrap_or_else(|| DEFAULT_EMBEDDER_DISCOVERY_SERVICE_NAME.to_owned()),
                embedder_endpoint_name: raw
                    .shield_embedder_endpoint_name
                    .unwrap_or_else(|| DEFAULT_EMBEDDER_DISCOVERY_ENDPOINT_NAME.to_owned()),
                embedding_dim: parse_i32(
                    "SHIELD_EMBEDDING_DIM",
                    raw.shield_embedding_dim,
                    DEFAULT_SHIELD_EMBEDDING_DIM,
                )?,
                max_matches: parse_i32(
                    "SHIELD_MAX_MATCHES",
                    raw.shield_max_matches,
                    DEFAULT_SHIELD_MAX_MATCHES,
                )?,
                vector_min_score: parse_f64(
                    "SHIELD_VECTOR_MIN_SCORE",
                    raw.shield_vector_min_score,
                    DEFAULT_SHIELD_VECTOR_MIN_SCORE,
                )?,
                lexical_min_score: parse_f64(
                    "SHIELD_LEXICAL_MIN_SCORE",
                    raw.shield_lexical_min_score,
                    DEFAULT_SHIELD_LEXICAL_MIN_SCORE,
                )?,
                query_max_chars: parse_i32(
                    "SHIELD_QUERY_MAX_CHARS",
                    raw.shield_query_max_chars,
                    DEFAULT_SHIELD_QUERY_MAX_CHARS,
                )?,
                retrieval_timeout_seconds: parse_i32(
                    "SHIELD_RETRIEVAL_TIMEOUT_SECONDS",
                    raw.shield_retrieval_timeout_seconds,
                    DEFAULT_SHIELD_RETRIEVAL_TIMEOUT_SECONDS,
                )?,
                history_tail_messages: parse_i32(
                    "SHIELD_HISTORY_TAIL_MESSAGES",
                    raw.shield_history_tail_messages,
                    DEFAULT_SHIELD_HISTORY_TAIL_MESSAGES,
                )?,
            },
            service_probe: ServiceProbeConfig {
                connect_services: parse_bool(
                    "OPENPLOTVA_CONNECT_SERVICES",
                    raw.openplotva_connect_services,
                    DEFAULT_CONNECT_SERVICES,
                )?,
                run_migrations: parse_bool(
                    "OPENPLOTVA_RUN_MIGRATIONS",
                    raw.openplotva_run_migrations,
                    DEFAULT_RUN_MIGRATIONS,
                )?,
                produce_updates: parse_bool(
                    "OPENPLOTVA_PRODUCE_UPDATES",
                    raw.openplotva_produce_updates,
                    DEFAULT_PRODUCE_UPDATES,
                )?,
                consume_updates: parse_bool(
                    "OPENPLOTVA_CONSUME_UPDATES",
                    raw.openplotva_consume_updates,
                    DEFAULT_CONSUME_UPDATES,
                )?,
            },
        })
    }
}

impl RawConfig {
    /// Load raw values from process environment variables.
    pub fn from_env() -> Self {
        Self {
            openplotva_bind_addr: env("OPENPLOTVA_BIND_ADDR"),
            openplotva_log_filter: env("OPENPLOTVA_LOG_FILTER"),
            log_level: env("LOG_LEVEL"),
            webapp_host: env("WEBAPP_HOST"),
            webapp_port: env("WEBAPP_PORT"),
            webapp_url: env("WEBAPP_URL"),
            settings_require_init_data: env("SETTINGS_REQUIRE_INIT_DATA"),
            runtime_api_enabled: env("RUNTIME_API_ENABLED"),
            runtime_api_host: env("RUNTIME_API_HOST"),
            runtime_api_port: env("RUNTIME_API_PORT"),
            runtime_api_log_buffer_size: env("RUNTIME_API_LOG_BUFFER_SIZE"),
            runtime_api_sql_timeout_ms: env("RUNTIME_API_SQL_TIMEOUT_MS"),
            runtime_api_sql_row_limit: env("RUNTIME_API_SQL_ROW_LIMIT"),
            runtime_api_sql_result_bytes_limit: env("RUNTIME_API_SQL_RESULT_BYTES_LIMIT"),
            llm_request_events_retention_days: env("LLM_REQUEST_EVENTS_RETENTION_DAYS"),
            llm_run_buffer_capacity: env("LLM_RUN_BUFFER_CAPACITY"),
            llm_raw_body_persist_enabled: env("LLM_RAW_BODY_PERSIST_ENABLED"),
            llm_raw_body_max_bytes: env("LLM_RAW_BODY_MAX_BYTES"),
            llm_raw_body_retention_hours: env("LLM_RAW_BODY_RETENTION_HOURS"),
            runtime_api_cert_file: env("RUNTIME_API_CERT_FILE"),
            runtime_api_key_file: env("RUNTIME_API_KEY_FILE"),
            runtime_api_tls_public_key_pin: env("RUNTIME_API_TLS_PUBLIC_KEY_PIN"),
            db_postgres_host: env("DB_POSTGRES_HOST"),
            db_postgres_port: env("DB_POSTGRES_PORT"),
            db_postgres_user: env("DB_POSTGRES_USER"),
            db_postgres_password: env("DB_POSTGRES_PASSWORD"),
            db_postgres_db: env("DB_POSTGRES_DB"),
            db_postgres_ssl_mode: env("DB_POSTGRES_SSL_MODE"),
            redis_host: env("REDIS_HOST"),
            redis_port: env("REDIS_PORT"),
            redis_password: env("REDIS_PASSWORD"),
            redis_db: env("REDIS_DB"),
            update_queue_backend: env("UPDATE_QUEUE_BACKEND"),
            bot_key: env("BOT_KEY"),
            bot_api_base_url: env("BOT_API_BASE_URL"),
            bot_webhook_enabled: env("BOT_WEBHOOK_ENABLED"),
            bot_webhook_url: env("BOT_WEBHOOK_URL"),
            bot_webhook_cert_file: env("BOT_WEBHOOK_CERT_FILE"),
            bot_webhook_key_file: env("BOT_WEBHOOK_KEY_FILE"),
            bot_webhook_secret_token: env("BOT_WEBHOOK_SECRET_TOKEN"),
            bot_webhook_update_buffer_size: env("BOT_WEBHOOK_UPDATE_BUFFER_SIZE"),
            telegram_activity_pulse_enabled: env("TELEGRAM_ACTIVITY_PULSE_ENABLED"),
            telegram_activity_pulse_interval_ms: env("TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS"),
            telegram_activity_pulse_initial_delay_ms: env(
                "TELEGRAM_ACTIVITY_PULSE_INITIAL_DELAY_MS",
            ),
            bot_debug: env("BOT_DEBUG"),
            admins_admin_ids: env("ADMINS_ADMIN_IDS"),
            vip_chat_id: env("VIP_CHAT_ID"),
            legacy_vip_chat_id: env("CHAT_ID"),
            subscription_sync_enabled: env("SUBSCRIPTION_SYNC_ENABLED"),
            subscription_sync_interval_seconds: env("SUBSCRIPTION_SYNC_INTERVAL_SECONDS"),
            subscription_sync_dry_run: env("SUBSCRIPTION_SYNC_DRY_RUN"),
            subscription_sync_page_limit: env("SUBSCRIPTION_SYNC_PAGE_LIMIT"),
            persistent_queue_enabled: env("PERSISTENT_QUEUE_ENABLED"),
            persistent_queue_heartbeat_interval_seconds: env(
                "PERSISTENT_QUEUE_HEARTBEAT_INTERVAL_SECONDS",
            ),
            persistent_queue_recovery_interval_seconds: env(
                "PERSISTENT_QUEUE_RECOVERY_INTERVAL_SECONDS",
            ),
            persistent_queue_cleanup_interval_seconds: env(
                "PERSISTENT_QUEUE_CLEANUP_INTERVAL_SECONDS",
            ),
            persistent_queue_default_processing_timeout_seconds: env(
                "PERSISTENT_QUEUE_DEFAULT_PROCESSING_TIMEOUT_SECONDS",
            ),
            persistent_queue_max_retries: env("PERSISTENT_QUEUE_MAX_RETRIES"),
            persistent_queue_completed_job_retention_days: env(
                "PERSISTENT_QUEUE_COMPLETED_JOB_RETENTION_DAYS",
            ),
            persistent_queue_message_cleanup_interval_seconds: env(
                "PERSISTENT_QUEUE_MESSAGE_CLEANUP_INTERVAL_SECONDS",
            ),
            persistent_queue_job_message_cleanup_minutes: env(
                "PERSISTENT_QUEUE_JOB_MESSAGE_CLEANUP_MINUTES",
            ),
            persistent_queue_control_workers: env("PERSISTENT_QUEUE_CONTROL_WORKERS"),
            persistent_queue_text_workers: env("PERSISTENT_QUEUE_TEXT_WORKERS"),
            persistent_queue_dialog_aifarm_workers: env("PERSISTENT_QUEUE_DIALOG_AIFARM_WORKERS"),
            persistent_queue_dialog_workers_cap: env("PERSISTENT_QUEUE_DIALOG_WORKERS_CAP"),
            persistent_queue_dialog_unpooled_share: env("PERSISTENT_QUEUE_DIALOG_UNPOOLED_SHARE"),
            persistent_queue_dialog_aifarm_fallback_high_watermark: env(
                "PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_HIGH_WATERMARK",
            ),
            persistent_queue_dialog_aifarm_fallback_low_watermark: env(
                "PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_LOW_WATERMARK",
            ),
            persistent_queue_dialog_aifarm_fallback_poll_interval_seconds: env(
                "PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_POLL_INTERVAL_SECONDS",
            ),
            persistent_queue_image_regular_workers: env("PERSISTENT_QUEUE_IMAGE_REGULAR_WORKERS"),
            persistent_queue_image_vip_workers: env("PERSISTENT_QUEUE_IMAGE_VIP_WORKERS"),
            persistent_queue_music_vip_workers: env("PERSISTENT_QUEUE_MUSIC_VIP_WORKERS"),
            persistent_queue_memory_workers_cap: env("PERSISTENT_QUEUE_MEMORY_WORKERS_CAP"),
            persistent_queue_memory_consolidation_workers: env(
                "PERSISTENT_QUEUE_MEMORY_CONSOLIDATION_WORKERS",
            ),
            persistent_queue_placeholder_cleanup_interval_seconds: env(
                "PERSISTENT_QUEUE_PLACEHOLDER_CLEANUP_INTERVAL_SECONDS",
            ),
            persistent_queue_placeholder_max_age_seconds: env(
                "PERSISTENT_QUEUE_PLACEHOLDER_MAX_AGE_SECONDS",
            ),
            llm_job_max_attempts: env("LLM_JOB_MAX_ATTEMPTS"),
            market_rates_timeout_seconds: env("MARKET_RATES_TIMEOUT_SECONDS"),
            rbc_timeout_seconds: env("RBC_TIMEOUT_SECONDS"),
            serper_api_key: env("SERPER_API_KEY"),
            serper_timeout_seconds: env("SERPER_TIMEOUT_SECONDS"),
            deepl_key: env("DEEPL_KEY"),
            deepl_url: env("DEEPL_URL"),
            googleai_key: env("GOOGLEAI_KEY"),
            googleai_key_stats_file: env("GOOGLEAI_KEY_STATS_FILE"),
            openrouter_key: env("OPENROUTER_KEY"),
            openrouter_request_timeout_seconds: env("OPENROUTER_REQUEST_TIMEOUT_SECONDS"),
            uploader_url: env("PLOTVA_UPLOADER_URL"),
            uploader_secret: env("PLOTVA_UPLOADER_SECRET"),
            uploader_timeout_seconds: env("PLOTVA_UPLOADER_TIMEOUT_SECONDS"),
            whitecircle_enabled: env("WHITECIRCLE_ENABLED"),
            whitecircle_api_key: env("WHITECIRCLE_API_KEY"),
            whitecircle_deployment_id: env("WHITECIRCLE_DEPLOYMENT_ID"),
            whitecircle_checks_retention_days: env("WHITECIRCLE_CHECKS_RETENTION_DAYS"),
            discovery_base_url: env("DISCOVERY_BASE_URL"),
            dialog_provider: env("DIALOG_PROVIDER"),
            dialog_fallback_provider: env("DIALOG_FALLBACK_PROVIDER"),
            dialog_model: env("DIALOG_MODEL"),
            dialog_url: env("DIALOG_URL"),
            dialog_api_key: env("DIALOG_API_KEY"),
            dialog_discovery_service_name: env("DIALOG_DISCOVERY_SERVICE_NAME"),
            dialog_discovery_endpoint_name: env("DIALOG_DISCOVERY_ENDPOINT_NAME"),
            dialog_aifarm_enable_thinking: env("DIALOG_AIFARM_ENABLE_THINKING"),
            dialog_aifarm_max_tokens: env("DIALOG_AIFARM_MAX_TOKENS"),
            dialog_aifarm_random_max_tokens: env("DIALOG_AIFARM_RANDOM_MAX_TOKENS"),
            dialog_aifarm_default_max_tokens: env("DIALOG_AIFARM_DEFAULT_MAX_TOKENS"),
            dialog_aifarm_long_max_tokens: env("DIALOG_AIFARM_LONG_MAX_TOKENS"),
            dialog_aifarm_temperature: env("DIALOG_AIFARM_TEMPERATURE"),
            dialog_aifarm_repeat_penalty: env("DIALOG_AIFARM_REPEAT_PENALTY"),
            dialog_aifarm_frequency_penalty: env("DIALOG_AIFARM_FREQUENCY_PENALTY"),
            dialog_aifarm_presence_penalty: env("DIALOG_AIFARM_PRESENCE_PENALTY"),
            dialog_aifarm_dry_multiplier: env("DIALOG_AIFARM_DRY_MULTIPLIER"),
            dialog_aifarm_dry_base: env("DIALOG_AIFARM_DRY_BASE"),
            dialog_aifarm_dry_allowed_length: env("DIALOG_AIFARM_DRY_ALLOWED_LENGTH"),
            dialog_request_timeout_seconds: env("DIALOG_REQUEST_TIMEOUT_SECONDS"),
            dialog_poll_interval_seconds: env("DIALOG_POLL_INTERVAL_SECONDS"),
            dialog_task_timeout_seconds: env("DIALOG_TASK_TIMEOUT_SECONDS"),
            dialog_aifarm_capacity_wait_seconds: env("DIALOG_AIFARM_CAPACITY_WAIT_SECONDS"),
            dialog_aifarm_capacity_poll_seconds: env("DIALOG_AIFARM_CAPACITY_POLL_SECONDS"),
            dialog_aifarm_pool_models: env("DIALOG_AIFARM_POOL_MODELS"),
            dialog_aifarm_pool_base_urls: env("DIALOG_AIFARM_POOL_BASE_URLS"),
            dialog_aifarm_pool_api_key: env("DIALOG_AIFARM_POOL_API_KEY"),
            dialog_aifarm_pool_primary_capacity_wait_ms: env(
                "DIALOG_AIFARM_POOL_PRIMARY_CAPACITY_WAIT_MS",
            ),
            dialog_turn_budget_secs: env("DIALOG_TURN_BUDGET_SECS"),
            dialog_turn_max_queue_age_secs: env("DIALOG_TURN_MAX_QUEUE_AGE_SECS"),
            dialog_turn_max_regenerations: env("DIALOG_TURN_MAX_REGENERATIONS"),
            dialog_terminal_reaction_emoji: env("DIALOG_TERMINAL_REACTION_EMOJI"),
            dialog_terminal_signal_max_age_secs: env("DIALOG_TERMINAL_SIGNAL_MAX_AGE_SECS"),
            dialog_image_delivery_timeout_secs: env("DIALOG_IMAGE_DELIVERY_TIMEOUT_SECS"),
            dialog_music_delivery_timeout_secs: env("DIALOG_MUSIC_DELIVERY_TIMEOUT_SECS"),
            dialog_obligation_watch_interval_secs: env("DIALOG_OBLIGATION_WATCH_INTERVAL_SECS"),
            dialog_session_max_iterations: env("DIALOG_SESSION_MAX_ITERATIONS"),
            dialog_session_max_messages: env("DIALOG_SESSION_MAX_MESSAGES"),
            dialog_session_tool_extension_secs: env("DIALOG_SESSION_TOOL_EXTENSION_SECS"),
            dialog_session_hard_cap_secs: env("DIALOG_SESSION_HARD_CAP_SECS"),
            dialog_session_max_draws: env("DIALOG_SESSION_MAX_DRAWS"),
            dialog_session_max_songs: env("DIALOG_SESSION_MAX_SONGS"),
            dialog_vmlx_url: env("DIALOG_VMLX_URL"),
            dialog_vmlx_api_key: env("DIALOG_VMLX_API_KEY"),
            dialog_vmlx_model: env("DIALOG_VMLX_MODEL"),
            dialog_nvidia_url: env("DIALOG_NVIDIA_URL"),
            dialog_nvidia_api_key: env("DIALOG_NVIDIA_API_KEY"),
            dialog_nvidia_model: env("DIALOG_NVIDIA_MODEL"),
            dialog_nvidia_max_tokens: env("DIALOG_NVIDIA_MAX_TOKENS"),
            dialog_nvidia_temperature: env("DIALOG_NVIDIA_TEMPERATURE"),
            dialog_nvidia_top_p: env("DIALOG_NVIDIA_TOP_P"),
            dialog_nvidia_enable_thinking: env("DIALOG_NVIDIA_ENABLE_THINKING"),
            dialog_nvidia_include_reasoning: env("DIALOG_NVIDIA_INCLUDE_REASONING"),
            llm_provider_names: env("LLM_PROVIDERS_NAMES"),
            llm_provider_kinds: env("LLM_PROVIDERS_KINDS"),
            llm_provider_discovery_service_names: env("LLM_PROVIDERS_DISCOVERY_SERVICE_NAMES"),
            llm_provider_discovery_endpoint_names: env("LLM_PROVIDERS_DISCOVERY_ENDPOINT_NAMES"),
            llm_provider_models: env("LLM_PROVIDERS_MODELS"),
            llm_provider_base_urls: env("LLM_PROVIDERS_BASE_URLS"),
            llm_provider_urls: env("LLM_PROVIDERS_URLS"),
            llm_provider_api_keys: env("LLM_PROVIDERS_API_KEYS"),
            llm_provider_include_reasoning: env("LLM_PROVIDERS_INCLUDE_REASONING"),
            llm_provider_enable_thinking: env("LLM_PROVIDERS_ENABLE_THINKING"),
            llm_provider_max_tokens: env("LLM_PROVIDERS_MAX_TOKENS"),
            llm_provider_temperatures: env("LLM_PROVIDERS_TEMPERATURES"),
            llm_provider_task_timeout_seconds: env("LLM_PROVIDERS_TASK_TIMEOUT_SECONDS"),
            llm_agentic_song_enabled: env("LLM_AGENTIC_SONG_ENABLED"),
            llm_agentic_image_enabled: env("LLM_AGENTIC_IMAGE_ENABLED"),
            llm_agent_reasoner_provider: env("LLM_AGENT_REASONER_PROVIDER"),
            genkit_default_model: env("GENKIT_DEFAULT_MODEL"),
            genkit_history_summary_provider: env("GENKIT_HISTORY_SUMMARY_PROVIDER"),
            genkit_history_summary_model: env("GENKIT_HISTORY_SUMMARY_MODEL"),
            genkit_history_summary_timeout_seconds: env("GENKIT_HISTORY_SUMMARY_TIMEOUT_SECONDS"),
            chat_history_retention_days: env("CHAT_HISTORY_RETENTION_DAYS"),
            vision_discovery_service_name: env("VISION_DISCOVERY_SERVICE_NAME"),
            vision_discovery_endpoint_name: env("VISION_DISCOVERY_ENDPOINT_NAME"),
            vision_model: env("VISION_MODEL"),
            vision_max_tokens: env("VISION_MAX_TOKENS"),
            vision_temperature: env("VISION_TEMPERATURE"),
            vision_direct_image_limit: env("VISION_DIRECT_IMAGE_LIMIT"),
            vision_request_timeout_seconds: env("VISION_REQUEST_TIMEOUT_SECONDS"),
            telegram_files_retention_days: env("TELEGRAM_FILES_RETENTION_DAYS"),
            boogu_image_turbo_enabled: env("BOOGU_IMAGE_TURBO_ENABLED"),
            boogu_image_turbo_base_url: env("BOOGU_IMAGE_TURBO_BASE_URL"),
            boogu_image_edit_turbo_enabled: env("BOOGU_IMAGE_EDIT_TURBO_ENABLED"),
            boogu_image_edit_turbo_base_url: env("BOOGU_IMAGE_EDIT_TURBO_BASE_URL"),
            boogu_image_timeout_seconds: env("BOOGU_IMAGE_TIMEOUT_SECONDS"),
            boogu_image_turbo_steps: env("BOOGU_IMAGE_TURBO_STEPS"),
            boogu_image_turbo_resolution: env("BOOGU_IMAGE_TURBO_RESOLUTION"),
            boogu_image_turbo_width: env("BOOGU_IMAGE_TURBO_WIDTH"),
            boogu_image_turbo_height: env("BOOGU_IMAGE_TURBO_HEIGHT"),
            boogu_image_edit_turbo_steps: env("BOOGU_IMAGE_EDIT_TURBO_STEPS"),
            boogu_image_edit_turbo_resolution_category: env(
                "BOOGU_IMAGE_EDIT_TURBO_RESOLUTION_CATEGORY",
            ),
            boogu_image_edit_turbo_resolution: env("BOOGU_IMAGE_EDIT_TURBO_RESOLUTION"),
            boogu_image_edit_turbo_width: env("BOOGU_IMAGE_EDIT_TURBO_WIDTH"),
            boogu_image_edit_turbo_height: env("BOOGU_IMAGE_EDIT_TURBO_HEIGHT"),
            acestep_enabled: env("ACESTEP_ENABLED"),
            acestep_base_url: env("ACESTEP_BASE_URL"),
            acestep_api_key: env("ACESTEP_API_KEY"),
            acestep_api_mode: env("ACESTEP_API_MODE"),
            acestep_request_timeout_seconds: env("ACESTEP_REQUEST_TIMEOUT_SECONDS"),
            acestep_poll_interval_seconds: env("ACESTEP_POLL_INTERVAL_SECONDS"),
            acestep_task_timeout_seconds: env("ACESTEP_TASK_TIMEOUT_SECONDS"),
            acestep_audio_format: env("ACESTEP_AUDIO_FORMAT"),
            acestep_model: env("ACESTEP_MODEL"),
            memory_enabled: env("MEMORY_ENABLED"),
            memory_retention_hours: env("MEMORY_RETENTION_HOURS"),
            memory_consolidation_provider: env("MEMORY_CONSOLIDATION_PROVIDER"),
            memory_consolidation_model: env("MEMORY_CONSOLIDATION_MODEL"),
            memory_consolidation_timeout_seconds: env("MEMORY_CONSOLIDATION_TIMEOUT_SECONDS"),
            memory_daily_schedule: env("MEMORY_DAILY_SCHEDULE"),
            memory_daily_window_hours: env("MEMORY_DAILY_WINDOW_HOURS"),
            memory_worker_concurrency: env("MEMORY_WORKER_CONCURRENCY"),
            memory_max_messages_per_run: env("MEMORY_MAX_MESSAGES_PER_RUN"),
            memory_min_messages_per_run: env("MEMORY_MIN_MESSAGES_PER_RUN"),
            memory_max_queued_runs: env("MEMORY_MAX_QUEUED_RUNS"),
            memory_max_daily_enqueued_runs: env("MEMORY_MAX_DAILY_ENQUEUED_RUNS"),
            memory_max_input_tokens: env("MEMORY_MAX_INPUT_TOKENS"),
            memory_tokenizer_model: env("MEMORY_TOKENIZER_MODEL"),
            memory_tokenizer_file: env("MEMORY_TOKENIZER_FILE"),
            memory_token_estimator_url: env("MEMORY_TOKEN_ESTIMATOR_URL"),
            memory_token_estimator_timeout_seconds: env("MEMORY_TOKEN_ESTIMATOR_TIMEOUT_SECONDS"),
            memory_embedder_service_name: env("MEMORY_EMBEDDER_DISCOVERY_SERVICE_NAME"),
            memory_embedder_endpoint_name: env("MEMORY_EMBEDDER_DISCOVERY_ENDPOINT_NAME"),
            memory_embedding_model: env("MEMORY_EMBEDDING_MODEL"),
            memory_embedding_dim: env("MEMORY_EMBEDDING_DIM"),
            memory_aifarm_service_name: env("MEMORY_AIFARM_SERVICE_NAME"),
            memory_aifarm_endpoint_name: env("MEMORY_AIFARM_ENDPOINT_NAME"),
            memory_aifarm_max_output_tokens: env("MEMORY_AIFARM_MAX_OUTPUT_TOKENS"),
            memory_aifarm_request_timeout_seconds: env("MEMORY_AIFARM_REQUEST_TIMEOUT_SECONDS"),
            memory_aifarm_poll_interval_seconds: env("MEMORY_AIFARM_POLL_INTERVAL_SECONDS"),
            memory_aifarm_task_timeout_seconds: env("MEMORY_AIFARM_TASK_TIMEOUT_SECONDS"),
            memory_aifarm_capacity_wait_seconds: env("MEMORY_AIFARM_CAPACITY_WAIT_SECONDS"),
            memory_aifarm_capacity_poll_seconds: env("MEMORY_AIFARM_CAPACITY_POLL_SECONDS"),
            memory_aifarm_temperature: env("MEMORY_AIFARM_TEMPERATURE"),
            memory_aifarm_frequency_penalty: env("MEMORY_AIFARM_FREQUENCY_PENALTY"),
            memory_aifarm_presence_penalty: env("MEMORY_AIFARM_PRESENCE_PENALTY"),
            memory_aifarm_enable_thinking: env("MEMORY_AIFARM_ENABLE_THINKING"),
            memory_subject_merge_enabled: env("MEMORY_SUBJECT_MERGE_ENABLED"),
            memory_subject_merge_min_cards: env("MEMORY_SUBJECT_MERGE_MIN_CARDS"),
            memory_subject_merge_cooldown_hours: env("MEMORY_SUBJECT_MERGE_COOLDOWN_HOURS"),
            memory_subject_merge_interval_seconds: env("MEMORY_SUBJECT_MERGE_INTERVAL_SECONDS"),
            memory_redaction_enabled: env("MEMORY_REDACTION_ENABLED"),
            memory_redaction_service_name: env("MEMORY_REDACTION_SERVICE_NAME"),
            memory_redaction_endpoint_name: env("MEMORY_REDACTION_ENDPOINT_NAME"),
            memory_redaction_timeout_seconds: env("MEMORY_REDACTION_TIMEOUT_SECONDS"),
            memory_redaction_task_timeout_seconds: env("MEMORY_REDACTION_TASK_TIMEOUT_SECONDS"),
            memory_redaction_poll_seconds: env("MEMORY_REDACTION_POLL_SECONDS"),
            memory_redaction_capacity_wait_seconds: env("MEMORY_REDACTION_CAPACITY_WAIT_SECONDS"),
            memory_redaction_capacity_poll_seconds: env("MEMORY_REDACTION_CAPACITY_POLL_SECONDS"),
            memory_redaction_categories: env("MEMORY_REDACTION_CATEGORIES"),
            shield_enabled: env("SHIELD_ENABLED"),
            shield_embedder_service_name: env("SHIELD_EMBEDDER_DISCOVERY_SERVICE_NAME"),
            shield_embedder_endpoint_name: env("SHIELD_EMBEDDER_DISCOVERY_ENDPOINT_NAME"),
            shield_embedding_dim: env("SHIELD_EMBEDDING_DIM"),
            shield_max_matches: env("SHIELD_MAX_MATCHES"),
            shield_vector_min_score: env("SHIELD_VECTOR_MIN_SCORE"),
            shield_lexical_min_score: env("SHIELD_LEXICAL_MIN_SCORE"),
            shield_query_max_chars: env("SHIELD_QUERY_MAX_CHARS"),
            shield_retrieval_timeout_seconds: env("SHIELD_RETRIEVAL_TIMEOUT_SECONDS"),
            shield_history_tail_messages: env("SHIELD_HISTORY_TAIL_MESSAGES"),
            openplotva_connect_services: env("OPENPLOTVA_CONNECT_SERVICES"),
            openplotva_run_migrations: env("OPENPLOTVA_RUN_MIGRATIONS"),
            openplotva_produce_updates: env("OPENPLOTVA_PRODUCE_UPDATES"),
            openplotva_consume_updates: env("OPENPLOTVA_CONSUME_UPDATES"),
        }
    }
}

fn load_dotenv() -> Result<(), ConfigError> {
    match dotenvy::from_path(".env") {
        Ok(_) => Ok(()),
        Err(dotenvy::Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ConfigError::Dotenv { source }),
    }
}

fn env(name: &'static str) -> Option<String> {
    std::env::var(name).ok()
}

fn parse_u16(name: &'static str, value: Option<String>, default: u16) -> Result<u16, ConfigError> {
    let Some(value) = parse_scalar_value(value) else {
        return Ok(default);
    };
    let parsed = value
        .parse::<i64>()
        .map_err(|source| ConfigError::InvalidInteger {
            name,
            value: value.clone(),
            source,
        })?;
    u16::try_from(parsed).map_err(|source| ConfigError::IntegerOutOfRange {
        name,
        value: parsed,
        source,
    })
}

fn parse_usize(
    name: &'static str,
    value: Option<String>,
    default: usize,
) -> Result<usize, ConfigError> {
    let Some(value) = parse_scalar_value(value) else {
        return Ok(default);
    };
    let parsed = value
        .parse::<i64>()
        .map_err(|source| ConfigError::InvalidInteger {
            name,
            value: value.clone(),
            source,
        })?;
    usize::try_from(parsed).map_err(|source| ConfigError::IntegerOutOfRange {
        name,
        value: parsed,
        source,
    })
}

fn parse_i64(name: &'static str, value: Option<String>, default: i64) -> Result<i64, ConfigError> {
    let Some(value) = parse_scalar_value(value) else {
        return Ok(default);
    };
    value
        .parse::<i64>()
        .map_err(|source| ConfigError::InvalidInteger {
            name,
            value,
            source,
        })
}

fn parse_i32(name: &'static str, value: Option<String>, default: i32) -> Result<i32, ConfigError> {
    let Some(value) = parse_scalar_value(value) else {
        return Ok(default);
    };
    let parsed = value
        .parse::<i64>()
        .map_err(|source| ConfigError::InvalidInteger {
            name,
            value: value.clone(),
            source,
        })?;
    i32::try_from(parsed).map_err(|source| ConfigError::IntegerOutOfRange {
        name,
        value: parsed,
        source,
    })
}

fn parse_f64(name: &'static str, value: Option<String>, default: f64) -> Result<f64, ConfigError> {
    let Some(value) = parse_scalar_value(value) else {
        return Ok(default);
    };
    value
        .parse::<f64>()
        .map_err(|source| ConfigError::InvalidFloat {
            name,
            value,
            source,
        })
}

// vLLM's OpenAI-compatible layer rejects repetition penalties outside
// [-2.0, 2.0] with a 400, so clamp operator overrides; a non-finite value
// falls back to the call site's own default rather than breaking every request.
fn clamp_penalty(value: f64, fallback: f64) -> f64 {
    if value.is_finite() {
        value.clamp(-2.0, 2.0)
    } else {
        fallback
    }
}

fn parse_bool(
    name: &'static str,
    value: Option<String>,
    default: bool,
) -> Result<bool, ConfigError> {
    let Some(value) = parse_scalar_value(value) else {
        return Ok(default);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "t" | "true" => Ok(true),
        "0" | "f" | "false" => Ok(false),
        _ => Err(ConfigError::InvalidBoolean { name, value }),
    }
}

fn parse_update_queue_backend(value: Option<String>) -> Result<String, ConfigError> {
    let backend = parse_scalar_value(value)
        .unwrap_or_else(|| DEFAULT_UPDATE_QUEUE_BACKEND.to_owned())
        .to_ascii_lowercase();
    match backend.as_str() {
        "list" | "stream" => Ok(backend),
        _ => Err(ConfigError::InvalidUpdateQueueBackend { value: backend }),
    }
}

fn parse_scalar_value(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

fn parse_string_list(value: Option<String>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_string_list_or_default(value: Option<String>, default: &'static str) -> Vec<String> {
    parse_string_list(Some(value.unwrap_or_else(|| default.to_owned())))
}

/// Split a comma list keeping empty positions, so parallel `LLM_PROVIDERS_*`
/// arrays stay index-aligned even when some per-provider values are omitted.
fn split_list_keep_empties(value: Option<String>) -> Vec<String> {
    match value {
        None => Vec::new(),
        Some(value) => value
            .split(',')
            .map(|item| item.trim().to_owned())
            .collect(),
    }
}

fn parse_i64_list_or_default(
    name: &'static str,
    value: Option<String>,
    default: &'static str,
) -> Result<Vec<i64>, ConfigError> {
    parse_string_list(Some(value.unwrap_or_else(|| default.to_owned())))
        .into_iter()
        .map(|value| {
            value
                .parse::<i64>()
                .map_err(|source| ConfigError::InvalidInteger {
                    name,
                    value,
                    source,
                })
        })
        .collect()
}

fn validate_history_summary_provider(value: &str) -> Result<(), ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "aifarm" | "genkit" => Ok(()),
        _ => Err(ConfigError::InvalidHistorySummaryProvider {
            value: value.to_owned(),
        }),
    }
}

fn validate_runtime_api(config: &RuntimeApiConfig) -> Result<(), ConfigError> {
    validate_non_negative_i32(
        "LLM_REQUEST_EVENTS_RETENTION_DAYS",
        config.llm_request_events_retention_days,
    )?;
    validate_positive_i32("LLM_RUN_BUFFER_CAPACITY", config.llm_run_buffer_capacity)?;
    validate_positive_i32("LLM_RAW_BODY_MAX_BYTES", config.llm_raw_body_max_bytes)?;
    validate_non_negative_i32(
        "LLM_RAW_BODY_RETENTION_HOURS",
        config.llm_raw_body_retention_hours,
    )?;

    if !config.enabled {
        return Ok(());
    }

    if config.port == 0 {
        return Err(ConfigError::NonPositiveInteger {
            name: "RUNTIME_API_PORT",
            value: 0,
        });
    }
    validate_positive_i32_at_most("RUNTIME_API_LOG_BUFFER_SIZE", config.log_buffer_size, 200)?;
    validate_positive_i32_at_most("RUNTIME_API_SQL_TIMEOUT_MS", config.sql_timeout_ms, 30_000)?;
    validate_positive_i32_at_most("RUNTIME_API_SQL_ROW_LIMIT", config.sql_row_limit, 1_000)?;
    validate_positive_i32_at_most(
        "RUNTIME_API_SQL_RESULT_BYTES_LIMIT",
        config.sql_result_bytes_limit,
        104_857_600,
    )?;
    let cert_file_present = !config.cert_file.trim().is_empty();
    let key_file_present = !config.key_file.trim().is_empty();
    if cert_file_present != key_file_present {
        return Err(ConfigError::InvalidEmptyValue {
            name: if cert_file_present {
                "RUNTIME_API_KEY_FILE"
            } else {
                "RUNTIME_API_CERT_FILE"
            },
        });
    }
    Ok(())
}

fn validate_persistent_queue_config(config: &PersistentQueueConfig) -> Result<(), ConfigError> {
    for (name, value) in [
        (
            "PERSISTENT_QUEUE_HEARTBEAT_INTERVAL_SECONDS",
            config.heartbeat_interval_seconds,
        ),
        (
            "PERSISTENT_QUEUE_RECOVERY_INTERVAL_SECONDS",
            config.recovery_interval_seconds,
        ),
        (
            "PERSISTENT_QUEUE_DEFAULT_PROCESSING_TIMEOUT_SECONDS",
            config.default_processing_timeout_seconds,
        ),
        ("PERSISTENT_QUEUE_CONTROL_WORKERS", config.control_workers),
        ("PERSISTENT_QUEUE_TEXT_WORKERS", config.text_workers),
        (
            "PERSISTENT_QUEUE_DIALOG_AIFARM_WORKERS",
            config.dialog_aifarm_workers,
        ),
        (
            "PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_HIGH_WATERMARK",
            config.dialog_aifarm_fallback_high_watermark,
        ),
        (
            "PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_LOW_WATERMARK",
            config.dialog_aifarm_fallback_low_watermark,
        ),
        (
            "PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_POLL_INTERVAL_SECONDS",
            config.dialog_aifarm_fallback_poll_interval_seconds,
        ),
        (
            "PERSISTENT_QUEUE_IMAGE_REGULAR_WORKERS",
            config.image_regular_workers,
        ),
        (
            "PERSISTENT_QUEUE_IMAGE_VIP_WORKERS",
            config.image_vip_workers,
        ),
        (
            "PERSISTENT_QUEUE_MUSIC_VIP_WORKERS",
            config.music_vip_workers,
        ),
        (
            "PERSISTENT_QUEUE_PLACEHOLDER_CLEANUP_INTERVAL_SECONDS",
            config.placeholder_cleanup_interval_seconds,
        ),
        (
            "PERSISTENT_QUEUE_PLACEHOLDER_MAX_AGE_SECONDS",
            config.placeholder_max_age_seconds,
        ),
        ("LLM_JOB_MAX_ATTEMPTS", config.llm_job_max_attempts),
    ] {
        validate_positive_i32(name, value)?;
    }
    validate_non_negative_i32(
        "PERSISTENT_QUEUE_MEMORY_CONSOLIDATION_WORKERS",
        config.memory_consolidation_workers,
    )?;
    if config.dialog_aifarm_fallback_low_watermark > config.dialog_aifarm_fallback_high_watermark {
        return Err(ConfigError::PersistentQueueFallbackWatermarkRange);
    }
    Ok(())
}

fn validate_positive_i32(name: &'static str, value: i32) -> Result<i32, ConfigError> {
    if value <= 0 {
        return Err(ConfigError::NonPositiveInteger { name, value });
    }
    Ok(value)
}

fn validate_positive_i32_at_most(
    name: &'static str,
    value: i32,
    max: i32,
) -> Result<(), ConfigError> {
    if value <= 0 {
        return Err(ConfigError::NonPositiveInteger { name, value });
    }
    if value > max {
        return Err(ConfigError::IntegerExceedsMaximum { name, value, max });
    }
    Ok(())
}

fn validate_non_negative_i32(name: &'static str, value: i32) -> Result<i32, ConfigError> {
    if value < 0 {
        return Err(ConfigError::NegativeInteger { name, value });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{
        AppConfig, DEFAULT_ACESTEP_API_MODE, DEFAULT_ACESTEP_AUDIO_FORMAT,
        DEFAULT_ACESTEP_BASE_URL, DEFAULT_ACESTEP_MODEL, DEFAULT_ACESTEP_POLL_INTERVAL_SECONDS,
        DEFAULT_ACESTEP_REQUEST_TIMEOUT_SECONDS, DEFAULT_ACESTEP_TASK_TIMEOUT_SECONDS,
        DEFAULT_BOOGU_IMAGE_EDIT_TURBO_BASE_URL, DEFAULT_BOOGU_IMAGE_EDIT_TURBO_ENABLED,
        DEFAULT_BOOGU_IMAGE_EDIT_TURBO_HEIGHT, DEFAULT_BOOGU_IMAGE_EDIT_TURBO_RESOLUTION,
        DEFAULT_BOOGU_IMAGE_EDIT_TURBO_RESOLUTION_CATEGORY, DEFAULT_BOOGU_IMAGE_EDIT_TURBO_STEPS,
        DEFAULT_BOOGU_IMAGE_EDIT_TURBO_WIDTH, DEFAULT_BOOGU_IMAGE_TIMEOUT_SECONDS,
        DEFAULT_BOOGU_IMAGE_TURBO_BASE_URL, DEFAULT_BOOGU_IMAGE_TURBO_ENABLED,
        DEFAULT_BOOGU_IMAGE_TURBO_HEIGHT, DEFAULT_BOOGU_IMAGE_TURBO_RESOLUTION,
        DEFAULT_BOOGU_IMAGE_TURBO_STEPS, DEFAULT_BOOGU_IMAGE_TURBO_WIDTH,
        DEFAULT_BOT_WEBHOOK_UPDATE_BUFFER_SIZE, DEFAULT_DIALOG_AIFARM_POOL_BASE_URLS,
        DEFAULT_DIALOG_AIFARM_POOL_MODELS, DEFAULT_DIALOG_MODEL, DEFAULT_DISCOVERY_BASE_URL,
        DEFAULT_HISTORY_SUMMARY_PROVIDER, DEFAULT_HISTORY_SUMMARY_TIMEOUT_SECONDS,
        DEFAULT_LLM_JOB_MAX_ATTEMPTS, DEFAULT_LOG_FILTER, DEFAULT_MARKET_RATES_TIMEOUT_SECONDS,
        DEFAULT_MEMORY_CONSOLIDATION_MODEL, DEFAULT_OPENROUTER_REQUEST_TIMEOUT_SECONDS,
        DEFAULT_PERSISTENT_QUEUE_CLEANUP_INTERVAL_SECONDS,
        DEFAULT_PERSISTENT_QUEUE_COMPLETED_JOB_RETENTION_DAYS,
        DEFAULT_PERSISTENT_QUEUE_CONTROL_WORKERS,
        DEFAULT_PERSISTENT_QUEUE_DEFAULT_PROCESSING_TIMEOUT_SECONDS,
        DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_HIGH_WATERMARK,
        DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_LOW_WATERMARK,
        DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_POLL_INTERVAL_SECONDS,
        DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_WORKERS,
        DEFAULT_PERSISTENT_QUEUE_HEARTBEAT_INTERVAL_SECONDS,
        DEFAULT_PERSISTENT_QUEUE_IMAGE_REGULAR_WORKERS, DEFAULT_PERSISTENT_QUEUE_IMAGE_VIP_WORKERS,
        DEFAULT_PERSISTENT_QUEUE_JOB_MESSAGE_CLEANUP_MINUTES, DEFAULT_PERSISTENT_QUEUE_MAX_RETRIES,
        DEFAULT_PERSISTENT_QUEUE_MEMORY_CONSOLIDATION_WORKERS,
        DEFAULT_PERSISTENT_QUEUE_MESSAGE_CLEANUP_INTERVAL_SECONDS,
        DEFAULT_PERSISTENT_QUEUE_MUSIC_VIP_WORKERS,
        DEFAULT_PERSISTENT_QUEUE_PLACEHOLDER_CLEANUP_INTERVAL_SECONDS,
        DEFAULT_PERSISTENT_QUEUE_PLACEHOLDER_MAX_AGE_SECONDS,
        DEFAULT_PERSISTENT_QUEUE_RECOVERY_INTERVAL_SECONDS, DEFAULT_PERSISTENT_QUEUE_TEXT_WORKERS,
        DEFAULT_RUNTIME_API_HOST, DEFAULT_RUNTIME_API_LOG_BUFFER_SIZE, DEFAULT_RUNTIME_API_PORT,
        DEFAULT_RUNTIME_API_SQL_RESULT_BYTES_LIMIT, DEFAULT_RUNTIME_API_SQL_ROW_LIMIT,
        DEFAULT_RUNTIME_API_SQL_TIMEOUT_MS, DEFAULT_SERPER_TIMEOUT_SECONDS,
        DEFAULT_SHIELD_EMBEDDING_DIM, DEFAULT_SHIELD_LEXICAL_MIN_SCORE, DEFAULT_SHIELD_MAX_MATCHES,
        DEFAULT_SHIELD_QUERY_MAX_CHARS, DEFAULT_SHIELD_RETRIEVAL_TIMEOUT_SECONDS,
        DEFAULT_SHIELD_VECTOR_MIN_SCORE, DEFAULT_TELEGRAM_ACTIVITY_PULSE_ENABLED,
        DEFAULT_TELEGRAM_ACTIVITY_PULSE_INITIAL_DELAY_MS,
        DEFAULT_TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS, DEFAULT_UPDATE_QUEUE_BACKEND,
        DEFAULT_VIP_CHAT_ID, DEFAULT_VISION_DIRECT_IMAGE_LIMIT, DEFAULT_VISION_MAX_TOKENS,
        DEFAULT_VISION_MODEL, DEFAULT_VISION_REQUEST_TIMEOUT_SECONDS, DEFAULT_WEBAPP_PORT,
        DEFAULT_WEBAPP_URL, MIN_TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS, RawConfig,
        parse_string_list_or_default,
    };

    #[test]
    fn defaults_match_service_spine_contract() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig::default())?;

        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.server.bind_addr, "0.0.0.0:8080");
        assert_eq!(config.server.url, DEFAULT_WEBAPP_URL);
        assert!(!config.server.require_settings_init_data);
        assert_eq!(config.vip.chat_id, DEFAULT_VIP_CHAT_ID);
        assert!(config.runtime_api.enabled);
        assert_eq!(config.runtime_api.host, DEFAULT_RUNTIME_API_HOST);
        assert_eq!(config.runtime_api.port, DEFAULT_RUNTIME_API_PORT);
        assert_eq!(
            config.runtime_api.log_buffer_size,
            DEFAULT_RUNTIME_API_LOG_BUFFER_SIZE
        );
        assert_eq!(
            config.runtime_api.sql_timeout_ms,
            DEFAULT_RUNTIME_API_SQL_TIMEOUT_MS
        );
        assert_eq!(
            config.runtime_api.sql_row_limit,
            DEFAULT_RUNTIME_API_SQL_ROW_LIMIT
        );
        assert_eq!(
            config.runtime_api.sql_result_bytes_limit,
            DEFAULT_RUNTIME_API_SQL_RESULT_BYTES_LIMIT
        );
        assert_eq!(config.observability.log_level, "info");
        assert_eq!(config.observability.log_filter, DEFAULT_LOG_FILTER);
        assert_eq!(
            config.database.postgres.startup_dsn(),
            "postgres://plotva:plotva@127.0.0.1:5432/plotva?sslmode=disable"
        );
        assert_eq!(config.database.postgres.ssl_mode, "disable");
        assert_eq!(config.redis.host, "127.0.0.1");
        assert_eq!(config.redis.port, 6379);
        assert_eq!(config.redis.password, "");
        assert_eq!(config.redis.db, 0);
        assert_eq!(config.update_queue.backend, DEFAULT_UPDATE_QUEUE_BACKEND);
        assert_eq!(config.translation.deepl.key, "");
        assert_eq!(config.translation.deepl.url, "");
        assert_eq!(config.bot.key, None);
        assert_eq!(config.bot.api_base_url, "");
        assert!(!config.bot.debug);
        assert!(!config.bot.webhook.enabled);
        assert_eq!(config.bot.webhook.url, "");
        assert_eq!(config.bot.webhook.cert_file, "");
        assert_eq!(config.bot.webhook.key_file, "");
        assert_eq!(config.bot.webhook.secret_token, "");
        assert_eq!(
            config.bot.webhook.update_buffer_size,
            DEFAULT_BOT_WEBHOOK_UPDATE_BUFFER_SIZE
        );
        assert_eq!(
            config.bot.activity_pulse.enabled,
            DEFAULT_TELEGRAM_ACTIVITY_PULSE_ENABLED
        );
        assert_eq!(
            config.bot.activity_pulse.interval_ms,
            DEFAULT_TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS
        );
        assert_eq!(
            config.bot.activity_pulse.initial_delay_ms,
            DEFAULT_TELEGRAM_ACTIVITY_PULSE_INITIAL_DELAY_MS
        );
        assert!(config.admins.admin_ids.is_empty());
        assert!(config.persistent_queue.enabled);
        assert_eq!(
            config.persistent_queue.heartbeat_interval_seconds,
            DEFAULT_PERSISTENT_QUEUE_HEARTBEAT_INTERVAL_SECONDS
        );
        assert_eq!(
            config.persistent_queue.recovery_interval_seconds,
            DEFAULT_PERSISTENT_QUEUE_RECOVERY_INTERVAL_SECONDS
        );
        assert_eq!(
            config.persistent_queue.cleanup_interval_seconds,
            DEFAULT_PERSISTENT_QUEUE_CLEANUP_INTERVAL_SECONDS
        );
        assert_eq!(
            config.persistent_queue.default_processing_timeout_seconds,
            DEFAULT_PERSISTENT_QUEUE_DEFAULT_PROCESSING_TIMEOUT_SECONDS
        );
        assert_eq!(
            config.persistent_queue.max_retries,
            DEFAULT_PERSISTENT_QUEUE_MAX_RETRIES
        );
        assert_eq!(
            config.persistent_queue.completed_job_retention_days,
            DEFAULT_PERSISTENT_QUEUE_COMPLETED_JOB_RETENTION_DAYS
        );
        assert_eq!(
            config.persistent_queue.message_cleanup_interval_seconds,
            DEFAULT_PERSISTENT_QUEUE_MESSAGE_CLEANUP_INTERVAL_SECONDS
        );
        assert_eq!(
            config.persistent_queue.job_message_cleanup_minutes,
            DEFAULT_PERSISTENT_QUEUE_JOB_MESSAGE_CLEANUP_MINUTES
        );
        assert_eq!(
            config.persistent_queue.control_workers,
            DEFAULT_PERSISTENT_QUEUE_CONTROL_WORKERS
        );
        assert_eq!(
            config.persistent_queue.text_workers,
            DEFAULT_PERSISTENT_QUEUE_TEXT_WORKERS
        );
        assert_eq!(
            config.persistent_queue.dialog_aifarm_workers,
            DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_WORKERS
        );
        assert_eq!(
            config
                .persistent_queue
                .dialog_aifarm_fallback_high_watermark,
            DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_HIGH_WATERMARK
        );
        assert_eq!(
            config.persistent_queue.dialog_aifarm_fallback_low_watermark,
            DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_LOW_WATERMARK
        );
        assert_eq!(
            config
                .persistent_queue
                .dialog_aifarm_fallback_poll_interval_seconds,
            DEFAULT_PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_POLL_INTERVAL_SECONDS
        );
        assert_eq!(
            config.persistent_queue.image_regular_workers,
            DEFAULT_PERSISTENT_QUEUE_IMAGE_REGULAR_WORKERS
        );
        assert_eq!(
            config.persistent_queue.image_vip_workers,
            DEFAULT_PERSISTENT_QUEUE_IMAGE_VIP_WORKERS
        );
        assert_eq!(
            config.persistent_queue.music_vip_workers,
            DEFAULT_PERSISTENT_QUEUE_MUSIC_VIP_WORKERS
        );
        assert_eq!(
            config.persistent_queue.memory_consolidation_workers,
            DEFAULT_PERSISTENT_QUEUE_MEMORY_CONSOLIDATION_WORKERS
        );
        assert_eq!(
            config.persistent_queue.placeholder_cleanup_interval_seconds,
            DEFAULT_PERSISTENT_QUEUE_PLACEHOLDER_CLEANUP_INTERVAL_SECONDS
        );
        assert_eq!(
            config.persistent_queue.placeholder_max_age_seconds,
            DEFAULT_PERSISTENT_QUEUE_PLACEHOLDER_MAX_AGE_SECONDS
        );
        assert_eq!(
            config.persistent_queue.llm_job_max_attempts,
            DEFAULT_LLM_JOB_MAX_ATTEMPTS
        );
        assert_eq!(
            config.market_rates.timeout_seconds,
            DEFAULT_MARKET_RATES_TIMEOUT_SECONDS
        );
        assert_eq!(config.serper.api_key, "");
        assert_eq!(
            config.serper.timeout_seconds,
            DEFAULT_SERPER_TIMEOUT_SECONDS
        );
        assert_eq!(config.open_router.key, "");
        assert_eq!(
            config.open_router.request_timeout_seconds,
            DEFAULT_OPENROUTER_REQUEST_TIMEOUT_SECONDS
        );
        assert_eq!(config.llm.discovery.base_url, DEFAULT_DISCOVERY_BASE_URL);
        assert_eq!(config.llm.genkit.default_model, "");
        assert_eq!(config.llm.dialog.provider, "aifarm");
        assert_eq!(config.llm.dialog.fallback_provider, "genkit");
        assert_eq!(config.llm.dialog.model, DEFAULT_DIALOG_MODEL);
        assert_eq!(config.llm.dialog.api_key, "");
        assert_eq!(config.llm.dialog.discovery_service_name, "llm-openai");
        assert_eq!(
            config.llm.dialog.discovery_endpoint_name,
            "chat_completions"
        );
        assert_eq!(config.llm.dialog.aifarm_max_tokens, 1024);
        assert_eq!(config.llm.dialog.aifarm_random_max_tokens, 768);
        assert_eq!(config.llm.dialog.aifarm_temperature, 0.2);
        assert_eq!(config.llm.dialog.aifarm_repeat_penalty, 1.1);
        // Anti-loop sampling defaults (DRY + moderate frequency/presence penalties).
        assert_eq!(config.llm.dialog.aifarm_frequency_penalty, 0.3);
        assert_eq!(config.llm.dialog.aifarm_presence_penalty, 0.2);
        assert_eq!(config.llm.dialog.aifarm_dry_multiplier, 0.8);
        assert_eq!(config.llm.dialog.aifarm_dry_base, 1.75);
        assert_eq!(config.llm.dialog.aifarm_dry_allowed_length, 2);
        assert_eq!(config.llm.dialog.request_timeout_seconds, 30);
        assert_eq!(config.llm.dialog.task_timeout_seconds, 720);
        assert_eq!(config.llm.dialog.aifarm_capacity_wait_seconds, 60);
        assert_eq!(config.llm.dialog.aifarm_capacity_poll_seconds, 1);
        assert_eq!(config.llm.dialog.aifarm_pool_primary_capacity_wait_ms, 500);
        assert_eq!(
            config.llm.dialog.aifarm_pool_models,
            parse_string_list_or_default(None, DEFAULT_DIALOG_AIFARM_POOL_MODELS)
        );
        assert_eq!(
            config.llm.dialog.aifarm_pool_base_urls,
            parse_string_list_or_default(None, DEFAULT_DIALOG_AIFARM_POOL_BASE_URLS)
        );
        assert_eq!(
            config.llm.dialog.nvidia_url,
            "https://integrate.api.nvidia.com/v1/chat/completions"
        );
        assert_eq!(config.llm.dialog.nvidia_model, "google/gemma-4-31b-it");
        assert_eq!(config.llm.dialog.nvidia_max_tokens, 16384);
        assert_eq!(config.llm.dialog.nvidia_temperature, 1.0);
        assert_eq!(config.llm.dialog.nvidia_top_p, 0.95);
        assert!(config.llm.dialog.nvidia_enable_thinking);
        assert!(!config.llm.dialog.nvidia_include_reasoning);
        assert_eq!(
            config.llm.history_summary.provider,
            DEFAULT_HISTORY_SUMMARY_PROVIDER
        );
        assert_eq!(config.llm.history_summary.model, "");
        assert_eq!(
            config.llm.history_summary.timeout_seconds,
            DEFAULT_HISTORY_SUMMARY_TIMEOUT_SECONDS
        );
        assert_eq!(config.vision.discovery_service_name, "llm-openai");
        assert_eq!(config.vision.discovery_endpoint_name, "chat_completions");
        assert_eq!(config.vision.model, DEFAULT_VISION_MODEL);
        assert_eq!(config.vision.max_tokens, DEFAULT_VISION_MAX_TOKENS);
        assert_eq!(config.vision.temperature, 0.1);
        assert_eq!(
            config.vision.direct_image_limit,
            DEFAULT_VISION_DIRECT_IMAGE_LIMIT
        );
        assert_eq!(
            config.vision.request_timeout_seconds,
            DEFAULT_VISION_REQUEST_TIMEOUT_SECONDS
        );
        // The song/image agents point at the qwen reasoner by default; the
        // qwen provider itself is auto-registered by the app, so the
        // config-level providers list stays empty.
        assert_eq!(config.llm.agentic.reasoner_provider, "qwen-reasoner");
        assert!(config.llm.providers.is_empty());
        assert!(!config.music.acestep.enabled);
        assert_eq!(config.music.acestep.base_url, DEFAULT_ACESTEP_BASE_URL);
        assert_eq!(config.music.acestep.api_key, "");
        assert_eq!(config.music.acestep.api_mode, DEFAULT_ACESTEP_API_MODE);
        assert_eq!(
            config.music.acestep.request_timeout_seconds,
            DEFAULT_ACESTEP_REQUEST_TIMEOUT_SECONDS
        );
        assert_eq!(
            config.music.acestep.poll_interval_seconds,
            DEFAULT_ACESTEP_POLL_INTERVAL_SECONDS
        );
        assert_eq!(
            config.music.acestep.task_timeout_seconds,
            DEFAULT_ACESTEP_TASK_TIMEOUT_SECONDS
        );
        assert_eq!(
            config.music.acestep.audio_format,
            DEFAULT_ACESTEP_AUDIO_FORMAT
        );
        assert_eq!(config.music.acestep.model, DEFAULT_ACESTEP_MODEL);
        assert!(config.memory.enabled);
        assert_eq!(config.memory.retention_hours, 168);
        assert_eq!(config.llm.history_summary.chat_history_retention_days, 8);
        assert_eq!(config.vision.telegram_files_retention_days, 7);
        assert_eq!(config.white_circle.whitecircle_checks_retention_days, 30);
        assert_eq!(config.memory.consolidation_provider, "aifarm");
        assert_eq!(
            config.memory.consolidation_model,
            DEFAULT_MEMORY_CONSOLIDATION_MODEL
        );
        assert_eq!(config.memory.consolidation_timeout_seconds, 600);
        assert_eq!(config.memory.daily_schedule, "03:17");
        assert_eq!(config.memory.daily_window_hours, 24);
        assert_eq!(config.memory.worker_concurrency, 1);
        assert_eq!(config.memory.max_messages_per_run, 200);
        assert_eq!(
            config.memory.min_messages_per_run,
            super::DEFAULT_MEMORY_MIN_MESSAGES_PER_RUN
        );
        assert_eq!(
            config.memory.max_queued_runs,
            super::DEFAULT_MEMORY_MAX_QUEUED_RUNS
        );
        assert_eq!(
            config.memory.max_daily_enqueued_runs,
            super::DEFAULT_MEMORY_MAX_DAILY_ENQUEUED_RUNS
        );
        assert_eq!(config.memory.max_input_tokens, 10_000);
        assert_eq!(config.memory.tokenizer_model, "google/gemma-4-26B-A4B-it");
        assert_eq!(
            config.memory.token_estimator_url,
            "http://token-estimator:12600"
        );
        assert_eq!(config.memory.token_estimator_timeout_seconds, 2);
        assert_eq!(config.memory.embedder_service_name, "embedder");
        assert_eq!(config.memory.embedder_endpoint_name, "encode");
        assert_eq!(
            config.memory.embedding_model,
            "jinaai/jina-embeddings-v5-text-nano"
        );
        assert_eq!(config.memory.embedding_dim, 512);
        assert_eq!(config.memory.aifarm_service_name, "llm-openai");
        assert_eq!(config.memory.aifarm_endpoint_name, "chat_completions");
        assert_eq!(config.memory.aifarm_max_output_tokens, 4000);
        assert_eq!(config.memory.aifarm_request_timeout_seconds, 660);
        assert_eq!(config.memory.aifarm_poll_interval_seconds, 1);
        assert_eq!(config.memory.aifarm_task_timeout_seconds, 720);
        assert_eq!(config.memory.aifarm_capacity_wait_seconds, 600);
        assert_eq!(config.memory.aifarm_capacity_poll_seconds, 1);
        assert_eq!(config.memory.aifarm_temperature, 0.2);
        assert!(!config.memory.aifarm_enable_thinking);
        assert!(config.memory.redaction_enabled);
        assert_eq!(config.memory.redaction_service_name, "privacy-filter");
        assert_eq!(config.memory.redaction_endpoint_name, "redact");
        assert_eq!(config.memory.redaction_timeout_seconds, 35);
        assert_eq!(config.memory.redaction_task_timeout_seconds, 35);
        assert_eq!(config.memory.redaction_poll_seconds, 1);
        assert_eq!(config.memory.redaction_capacity_wait_seconds, 30);
        assert_eq!(config.memory.redaction_capacity_poll_seconds, 1);
        assert_eq!(
            config.memory.redaction_categories,
            vec![
                "account_number",
                "private_date",
                "private_email",
                "private_phone",
                "private_url",
                "secret",
            ]
        );
        assert!(config.shield.enabled);
        assert_eq!(config.shield.embedder_service_name, "embedder");
        assert_eq!(config.shield.embedder_endpoint_name, "encode");
        assert_eq!(config.shield.embedding_dim, DEFAULT_SHIELD_EMBEDDING_DIM);
        assert_eq!(config.shield.max_matches, DEFAULT_SHIELD_MAX_MATCHES);
        assert_eq!(
            config.shield.vector_min_score,
            DEFAULT_SHIELD_VECTOR_MIN_SCORE
        );
        assert_eq!(
            config.shield.lexical_min_score,
            DEFAULT_SHIELD_LEXICAL_MIN_SCORE
        );
        assert_eq!(
            config.shield.query_max_chars,
            DEFAULT_SHIELD_QUERY_MAX_CHARS
        );
        assert_eq!(
            config.shield.retrieval_timeout_seconds,
            DEFAULT_SHIELD_RETRIEVAL_TIMEOUT_SECONDS
        );
        assert_eq!(config.shield.history_tail_messages, 0);
        assert!(config.service_probe.connect_services);
        assert!(!config.service_probe.run_migrations);
        assert!(config.service_probe.produce_updates);
        assert!(config.service_probe.consume_updates);

        Ok(())
    }

    #[test]
    fn telegram_activity_pulse_env_overrides_and_clamps() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            telegram_activity_pulse_enabled: Some("false".to_owned()),
            telegram_activity_pulse_interval_ms: Some("250".to_owned()),
            telegram_activity_pulse_initial_delay_ms: Some("-10".to_owned()),
            ..RawConfig::default()
        })?;

        assert!(!config.bot.activity_pulse.enabled);
        assert_eq!(
            config.bot.activity_pulse.interval_ms,
            MIN_TELEGRAM_ACTIVITY_PULSE_INTERVAL_MS
        );
        assert_eq!(config.bot.activity_pulse.initial_delay_ms, 0);
        Ok(())
    }

    #[test]
    fn telegram_activity_pulse_default_interval_is_eight_seconds() -> Result<(), super::ConfigError>
    {
        let config = AppConfig::from_raw(RawConfig::default())?;

        assert_eq!(config.bot.activity_pulse.interval_ms, 8_000);
        Ok(())
    }

    #[test]
    fn boogu_image_provider_defaults_are_disabled() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig::default())?;

        assert_eq!(
            config.image_providers.boogu_turbo.enabled,
            DEFAULT_BOOGU_IMAGE_TURBO_ENABLED
        );
        assert_eq!(
            config.image_providers.boogu_turbo.base_url,
            DEFAULT_BOOGU_IMAGE_TURBO_BASE_URL
        );
        assert_eq!(
            config.image_providers.boogu_turbo.timeout_seconds,
            DEFAULT_BOOGU_IMAGE_TIMEOUT_SECONDS
        );
        assert_eq!(
            config.image_providers.boogu_turbo.steps,
            DEFAULT_BOOGU_IMAGE_TURBO_STEPS
        );
        assert_eq!(
            config.image_providers.boogu_turbo.resolution,
            DEFAULT_BOOGU_IMAGE_TURBO_RESOLUTION
        );
        assert_eq!(
            config.image_providers.boogu_turbo.width,
            DEFAULT_BOOGU_IMAGE_TURBO_WIDTH
        );
        assert_eq!(
            config.image_providers.boogu_turbo.height,
            DEFAULT_BOOGU_IMAGE_TURBO_HEIGHT
        );
        assert_eq!(
            config.image_providers.boogu_edit_turbo.enabled,
            DEFAULT_BOOGU_IMAGE_EDIT_TURBO_ENABLED
        );
        assert_eq!(
            config.image_providers.boogu_edit_turbo.base_url,
            DEFAULT_BOOGU_IMAGE_EDIT_TURBO_BASE_URL
        );
        assert_eq!(
            config.image_providers.boogu_edit_turbo.timeout_seconds,
            DEFAULT_BOOGU_IMAGE_TIMEOUT_SECONDS
        );
        assert_eq!(
            config.image_providers.boogu_edit_turbo.resolution_category,
            DEFAULT_BOOGU_IMAGE_EDIT_TURBO_RESOLUTION_CATEGORY
        );
        assert_eq!(
            config.image_providers.boogu_edit_turbo.resolution,
            DEFAULT_BOOGU_IMAGE_EDIT_TURBO_RESOLUTION
        );
        assert_eq!(
            config.image_providers.boogu_edit_turbo.steps,
            DEFAULT_BOOGU_IMAGE_EDIT_TURBO_STEPS
        );
        assert_eq!(
            config.image_providers.boogu_edit_turbo.width,
            DEFAULT_BOOGU_IMAGE_EDIT_TURBO_WIDTH
        );
        assert_eq!(
            config.image_providers.boogu_edit_turbo.height,
            DEFAULT_BOOGU_IMAGE_EDIT_TURBO_HEIGHT
        );
        Ok(())
    }

    #[test]
    fn boogu_image_provider_env_overrides_are_parsed() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            boogu_image_turbo_enabled: Some("false".to_owned()),
            boogu_image_turbo_base_url: Some("https://draw.example.test".to_owned()),
            boogu_image_timeout_seconds: Some("321".to_owned()),
            boogu_image_turbo_steps: Some("4".to_owned()),
            boogu_image_turbo_resolution: Some("1280x720 ( 16:9 )".to_owned()),
            boogu_image_turbo_width: Some("1280".to_owned()),
            boogu_image_turbo_height: Some("720".to_owned()),
            boogu_image_edit_turbo_enabled: Some("false".to_owned()),
            boogu_image_edit_turbo_base_url: Some("https://edit.example.test".to_owned()),
            boogu_image_edit_turbo_resolution_category: Some("1K".to_owned()),
            boogu_image_edit_turbo_resolution: Some("1024x1024 ( 1:1 )".to_owned()),
            boogu_image_edit_turbo_steps: Some("4".to_owned()),
            boogu_image_edit_turbo_width: Some("1024".to_owned()),
            boogu_image_edit_turbo_height: Some("1024".to_owned()),
            ..RawConfig::default()
        })?;

        assert!(!config.image_providers.boogu_turbo.enabled);
        assert_eq!(
            config.image_providers.boogu_turbo.base_url,
            "https://draw.example.test"
        );
        assert_eq!(config.image_providers.boogu_turbo.timeout_seconds, 321);
        assert_eq!(config.image_providers.boogu_turbo.steps, 4);
        assert_eq!(
            config.image_providers.boogu_turbo.resolution,
            "1280x720 ( 16:9 )"
        );
        assert_eq!(config.image_providers.boogu_turbo.width, 1280);
        assert_eq!(config.image_providers.boogu_turbo.height, 720);
        assert!(!config.image_providers.boogu_edit_turbo.enabled);
        assert_eq!(
            config.image_providers.boogu_edit_turbo.base_url,
            "https://edit.example.test"
        );
        assert_eq!(config.image_providers.boogu_edit_turbo.timeout_seconds, 321);
        assert_eq!(
            config.image_providers.boogu_edit_turbo.resolution_category,
            "1K"
        );
        assert_eq!(
            config.image_providers.boogu_edit_turbo.resolution,
            "1024x1024 ( 1:1 )"
        );
        assert_eq!(config.image_providers.boogu_edit_turbo.steps, 4);
        assert_eq!(config.image_providers.boogu_edit_turbo.width, 1024);
        assert_eq!(config.image_providers.boogu_edit_turbo.height, 1024);
        Ok(())
    }

    #[test]
    fn vip_config_accepts_legacy_go_chat_id() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            legacy_vip_chat_id: Some("-1001998670656".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.vip.chat_id, -1001998670656);

        let config = AppConfig::from_raw(RawConfig {
            vip_chat_id: Some("-10042".to_owned()),
            legacy_vip_chat_id: Some("-1001998670656".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.vip.chat_id, -10042);
        Ok(())
    }

    #[test]
    fn runtime_api_config_loads_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            runtime_api_enabled: Some("true".to_owned()),
            runtime_api_host: Some("0.0.0.0".to_owned()),
            runtime_api_port: Some("9092".to_owned()),
            runtime_api_log_buffer_size: Some("50".to_owned()),
            runtime_api_sql_timeout_ms: Some("1000".to_owned()),
            runtime_api_sql_row_limit: Some("100".to_owned()),
            runtime_api_sql_result_bytes_limit: Some("4096".to_owned()),
            llm_request_events_retention_days: Some("9".to_owned()),
            runtime_api_cert_file: Some("/tmp/openplotva-runtime-api.crt".to_owned()),
            runtime_api_key_file: Some("/tmp/openplotva-runtime-api.key".to_owned()),
            runtime_api_tls_public_key_pin: Some("sha256//example".to_owned()),
            ..RawConfig::default()
        })?;

        assert!(config.runtime_api.enabled);
        assert_eq!(config.runtime_api.host, "0.0.0.0");
        assert_eq!(config.runtime_api.port, 9092);
        assert_eq!(config.runtime_api.log_buffer_size, 50);
        assert_eq!(config.runtime_api.sql_timeout_ms, 1000);
        assert_eq!(config.runtime_api.sql_row_limit, 100);
        assert_eq!(config.runtime_api.sql_result_bytes_limit, 4096);
        assert_eq!(config.runtime_api.llm_request_events_retention_days, 9);
        assert_eq!(
            config.runtime_api.cert_file,
            "/tmp/openplotva-runtime-api.crt"
        );
        assert_eq!(
            config.runtime_api.key_file,
            "/tmp/openplotva-runtime-api.key"
        );
        assert_eq!(config.runtime_api.tls_public_key_pin, "sha256//example");
        Ok(())
    }

    #[test]
    fn runtime_api_config_defaults_llm_raw_event_retention_to_14_days()
    -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig::default())?;

        assert_eq!(config.runtime_api.llm_request_events_retention_days, 14);
        Ok(())
    }

    #[test]
    fn runtime_api_config_allows_disabling_llm_raw_event_cleanup() -> Result<(), super::ConfigError>
    {
        let config = AppConfig::from_raw(RawConfig {
            llm_request_events_retention_days: Some("0".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.runtime_api.llm_request_events_retention_days, 0);
        Ok(())
    }

    #[test]
    fn runtime_api_enabled_accepts_generated_tls_defaults() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            runtime_api_enabled: Some("true".to_owned()),
            ..RawConfig::default()
        })?;

        assert!(config.runtime_api.enabled);
        assert_eq!(config.runtime_api.cert_file, "");
        assert_eq!(config.runtime_api.key_file, "");
        assert_eq!(config.runtime_api.tls_public_key_pin, "");
        Ok(())
    }

    #[test]
    fn runtime_api_enabled_rejects_partial_tls_files() {
        let error = AppConfig::from_raw(RawConfig {
            runtime_api_enabled: Some("true".to_owned()),
            runtime_api_cert_file: Some("/tmp/openplotva-runtime-api.crt".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::InvalidEmptyValue {
                name: "RUNTIME_API_KEY_FILE"
            })
        ));
    }

    #[test]
    fn runtime_api_disabled_accepts_zero_limits() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            runtime_api_enabled: Some("false".to_owned()),
            runtime_api_port: Some("0".to_owned()),
            runtime_api_log_buffer_size: Some("0".to_owned()),
            runtime_api_sql_timeout_ms: Some("0".to_owned()),
            runtime_api_sql_row_limit: Some("0".to_owned()),
            runtime_api_sql_result_bytes_limit: Some("0".to_owned()),
            ..RawConfig::default()
        })?;

        assert!(!config.runtime_api.enabled);
        assert_eq!(config.runtime_api.port, 0);
        assert_eq!(config.runtime_api.log_buffer_size, 0);
        Ok(())
    }

    #[test]
    fn runtime_api_enabled_rejects_non_positive_values_like_go() {
        let error = AppConfig::from_raw(RawConfig {
            runtime_api_enabled: Some("true".to_owned()),
            runtime_api_log_buffer_size: Some("0".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::NonPositiveInteger {
                name: "RUNTIME_API_LOG_BUFFER_SIZE",
                ..
            })
        ));
    }

    #[test]
    fn runtime_api_enabled_rejects_values_above_go_limits() {
        let error = AppConfig::from_raw(RawConfig {
            runtime_api_enabled: Some("true".to_owned()),
            runtime_api_sql_row_limit: Some("1001".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::IntegerExceedsMaximum {
                name: "RUNTIME_API_SQL_ROW_LIMIT",
                max: 1000,
                ..
            })
        ));
    }

    #[test]
    fn persistent_queue_config_loads_go_worker_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            persistent_queue_enabled: Some("false".to_owned()),
            persistent_queue_heartbeat_interval_seconds: Some("31".to_owned()),
            persistent_queue_recovery_interval_seconds: Some("61".to_owned()),
            persistent_queue_cleanup_interval_seconds: Some("301".to_owned()),
            persistent_queue_default_processing_timeout_seconds: Some("45".to_owned()),
            persistent_queue_max_retries: Some("10".to_owned()),
            persistent_queue_completed_job_retention_days: Some("11".to_owned()),
            persistent_queue_message_cleanup_interval_seconds: Some("302".to_owned()),
            persistent_queue_job_message_cleanup_minutes: Some("33".to_owned()),
            persistent_queue_control_workers: Some("3".to_owned()),
            persistent_queue_text_workers: Some("6".to_owned()),
            persistent_queue_dialog_aifarm_workers: Some("4".to_owned()),
            persistent_queue_dialog_aifarm_fallback_high_watermark: Some("40".to_owned()),
            persistent_queue_dialog_aifarm_fallback_low_watermark: Some("10".to_owned()),
            persistent_queue_dialog_aifarm_fallback_poll_interval_seconds: Some("2".to_owned()),
            persistent_queue_image_regular_workers: Some("2".to_owned()),
            persistent_queue_image_vip_workers: Some("5".to_owned()),
            persistent_queue_music_vip_workers: Some("7".to_owned()),
            persistent_queue_memory_consolidation_workers: Some("8".to_owned()),
            persistent_queue_placeholder_cleanup_interval_seconds: Some("3601".to_owned()),
            persistent_queue_placeholder_max_age_seconds: Some("7201".to_owned()),
            llm_job_max_attempts: Some("9".to_owned()),
            ..RawConfig::default()
        })?;

        assert!(!config.persistent_queue.enabled);
        assert_eq!(config.persistent_queue.heartbeat_interval_seconds, 31);
        assert_eq!(config.persistent_queue.recovery_interval_seconds, 61);
        assert_eq!(config.persistent_queue.cleanup_interval_seconds, 301);
        assert_eq!(
            config.persistent_queue.default_processing_timeout_seconds,
            45
        );
        assert_eq!(config.persistent_queue.max_retries, 10);
        assert_eq!(config.persistent_queue.completed_job_retention_days, 11);
        assert_eq!(
            config.persistent_queue.message_cleanup_interval_seconds,
            302
        );
        assert_eq!(config.persistent_queue.job_message_cleanup_minutes, 33);
        assert_eq!(config.persistent_queue.control_workers, 3);
        assert_eq!(config.persistent_queue.text_workers, 6);
        assert_eq!(config.persistent_queue.dialog_aifarm_workers, 4);
        assert_eq!(
            config
                .persistent_queue
                .dialog_aifarm_fallback_high_watermark,
            40
        );
        assert_eq!(
            config.persistent_queue.dialog_aifarm_fallback_low_watermark,
            10
        );
        assert_eq!(
            config
                .persistent_queue
                .dialog_aifarm_fallback_poll_interval_seconds,
            2
        );
        assert_eq!(config.persistent_queue.image_regular_workers, 2);
        assert_eq!(config.persistent_queue.image_vip_workers, 5);
        assert_eq!(config.persistent_queue.music_vip_workers, 7);
        assert_eq!(config.persistent_queue.memory_consolidation_workers, 8);
        assert_eq!(
            config.persistent_queue.placeholder_cleanup_interval_seconds,
            3601
        );
        assert_eq!(config.persistent_queue.placeholder_max_age_seconds, 7201);
        assert_eq!(config.persistent_queue.llm_job_max_attempts, 9);
        Ok(())
    }

    #[test]
    fn persistent_queue_memory_consolidation_workers_allows_zero_for_maintenance()
    -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            persistent_queue_memory_consolidation_workers: Some("0".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.persistent_queue.memory_consolidation_workers, 0);
        Ok(())
    }

    #[test]
    fn persistent_queue_config_rejects_non_positive_go_intervals() {
        let error = AppConfig::from_raw(RawConfig {
            persistent_queue_heartbeat_interval_seconds: Some("0".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::NonPositiveInteger {
                name: "PERSISTENT_QUEUE_HEARTBEAT_INTERVAL_SECONDS",
                value: 0,
            })
        ));
    }

    #[test]
    fn persistent_queue_config_rejects_negative_memory_consolidation_worker_count() {
        let error = AppConfig::from_raw(RawConfig {
            persistent_queue_memory_consolidation_workers: Some("-1".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::NegativeInteger {
                name: "PERSISTENT_QUEUE_MEMORY_CONSOLIDATION_WORKERS",
                value: -1,
            })
        ));
    }

    #[test]
    fn persistent_queue_config_rejects_inverted_fallback_watermarks() {
        let error = AppConfig::from_raw(RawConfig {
            persistent_queue_dialog_aifarm_fallback_high_watermark: Some("20".to_owned()),
            persistent_queue_dialog_aifarm_fallback_low_watermark: Some("21".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::PersistentQueueFallbackWatermarkRange)
        ));
    }

    #[test]
    fn persistent_queue_config_does_not_invent_cleanup_retry_validation()
    -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            persistent_queue_cleanup_interval_seconds: Some("0".to_owned()),
            persistent_queue_max_retries: Some("0".to_owned()),
            persistent_queue_completed_job_retention_days: Some("0".to_owned()),
            persistent_queue_message_cleanup_interval_seconds: Some("0".to_owned()),
            persistent_queue_job_message_cleanup_minutes: Some("0".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.persistent_queue.cleanup_interval_seconds, 0);
        assert_eq!(config.persistent_queue.max_retries, 0);
        assert_eq!(config.persistent_queue.completed_job_retention_days, 0);
        assert_eq!(config.persistent_queue.message_cleanup_interval_seconds, 0);
        assert_eq!(config.persistent_queue.job_message_cleanup_minutes, 0);
        Ok(())
    }

    #[test]
    fn llm_job_max_attempts_rejects_non_positive_values_like_go() {
        let error = AppConfig::from_raw(RawConfig {
            llm_job_max_attempts: Some("0".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::NonPositiveInteger {
                name: "LLM_JOB_MAX_ATTEMPTS",
                value: 0,
            })
        ));
    }

    #[test]
    fn market_rates_config_loads_timeout_env_value() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            market_rates_timeout_seconds: Some("9".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.market_rates.timeout_seconds, 9);
        Ok(())
    }

    #[test]
    fn market_rates_config_accepts_deprecated_rbc_timeout_env_value()
    -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            rbc_timeout_seconds: Some("9".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.market_rates.timeout_seconds, 9);
        Ok(())
    }

    #[test]
    fn serper_config_loads_go_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            serper_api_key: Some("serper-key".to_owned()),
            serper_timeout_seconds: Some("7".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.serper.api_key, "serper-key");
        assert_eq!(config.serper.timeout_seconds, 7);
        Ok(())
    }

    #[test]
    fn serper_config_allows_non_positive_timeout_like_go_fallback() {
        let config = AppConfig::from_raw(RawConfig {
            serper_timeout_seconds: Some("0".to_owned()),
            ..RawConfig::default()
        })
        .expect("Serper timeout falls back when timeout is not positive");

        assert_eq!(config.serper.timeout_seconds, 0);
    }

    #[test]
    fn openrouter_config_load_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            openrouter_key: Some(" openrouter-key ".to_owned()),
            openrouter_request_timeout_seconds: Some("123".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.open_router.key, " openrouter-key ");
        assert_eq!(config.open_router.request_timeout_seconds, 123);
        Ok(())
    }

    #[test]
    fn openrouter_key_rejects_non_positive_timeout_like_go() {
        let error = AppConfig::from_raw(RawConfig {
            openrouter_key: Some("key".to_owned()),
            openrouter_request_timeout_seconds: Some("0".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::NonPositiveInteger {
                name: "OPENROUTER_REQUEST_TIMEOUT_SECONDS",
                value: 0,
            })
        ));
    }

    #[test]
    fn clamp_penalty_enforces_openai_range() {
        assert_eq!(super::clamp_penalty(0.3, 0.3), 0.3);
        assert_eq!(super::clamp_penalty(2.0, 0.3), 2.0);
        assert_eq!(super::clamp_penalty(-2.0, 0.3), -2.0);
        assert_eq!(super::clamp_penalty(5.0, 0.3), 2.0);
        assert_eq!(super::clamp_penalty(-9.0, 0.3), -2.0);
        // Non-finite degrades to the call site's own default, not a hardcoded one.
        assert_eq!(super::clamp_penalty(f64::NAN, 0.3), 0.3);
        assert_eq!(super::clamp_penalty(f64::NAN, 0.2), 0.2);
        assert_eq!(super::clamp_penalty(f64::INFINITY, 0.2), 0.2);
        assert_eq!(super::clamp_penalty(f64::NEG_INFINITY, 0.3), 0.3);
    }

    #[test]
    fn memory_config_loads_go_aifarm_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            memory_enabled: Some("false".to_owned()),
            memory_retention_hours: Some("240".to_owned()),
            memory_consolidation_provider: Some("genkit".to_owned()),
            memory_consolidation_model: Some("memory-model".to_owned()),
            memory_consolidation_timeout_seconds: Some("777".to_owned()),
            memory_daily_schedule: Some("04:05".to_owned()),
            memory_daily_window_hours: Some("12".to_owned()),
            memory_worker_concurrency: Some("2".to_owned()),
            memory_max_messages_per_run: Some("55".to_owned()),
            memory_min_messages_per_run: Some("20".to_owned()),
            memory_max_queued_runs: Some("5000".to_owned()),
            memory_max_daily_enqueued_runs: Some("2000".to_owned()),
            memory_max_input_tokens: Some("6000".to_owned()),
            memory_tokenizer_model: Some("tokenizer".to_owned()),
            memory_tokenizer_file: Some("/tmp/tokenizer.json".to_owned()),
            memory_token_estimator_url: Some("http://tokens.test".to_owned()),
            memory_token_estimator_timeout_seconds: Some("4".to_owned()),
            memory_embedder_service_name: Some("memory-embedder".to_owned()),
            memory_embedding_model: Some("embedding".to_owned()),
            memory_embedding_dim: Some("512".to_owned()),
            memory_aifarm_service_name: Some("svc".to_owned()),
            memory_aifarm_endpoint_name: Some("endpoint".to_owned()),
            memory_aifarm_max_output_tokens: Some("2048".to_owned()),
            memory_aifarm_request_timeout_seconds: Some("61".to_owned()),
            memory_aifarm_poll_interval_seconds: Some("3".to_owned()),
            memory_aifarm_task_timeout_seconds: Some("99".to_owned()),
            memory_aifarm_capacity_wait_seconds: Some("0".to_owned()),
            memory_aifarm_capacity_poll_seconds: Some("5".to_owned()),
            memory_aifarm_temperature: Some("0.4".to_owned()),
            memory_aifarm_enable_thinking: Some("true".to_owned()),
            memory_redaction_enabled: Some("false".to_owned()),
            memory_redaction_service_name: Some("redactor".to_owned()),
            memory_redaction_endpoint_name: Some("redact-now".to_owned()),
            memory_redaction_timeout_seconds: Some("12".to_owned()),
            memory_redaction_task_timeout_seconds: Some("13".to_owned()),
            memory_redaction_poll_seconds: Some("2".to_owned()),
            memory_redaction_capacity_wait_seconds: Some("14".to_owned()),
            memory_redaction_capacity_poll_seconds: Some("3".to_owned()),
            memory_redaction_categories: Some(" private_email, secret ".to_owned()),
            ..RawConfig::default()
        })?;

        assert!(!config.memory.enabled);
        assert_eq!(config.memory.retention_hours, 240);
        assert_eq!(config.memory.consolidation_provider, "genkit");
        assert_eq!(config.memory.consolidation_model, "memory-model");
        assert_eq!(config.memory.consolidation_timeout_seconds, 777);
        assert_eq!(config.memory.daily_schedule, "04:05");
        assert_eq!(config.memory.daily_window_hours, 12);
        assert_eq!(config.memory.worker_concurrency, 2);
        assert_eq!(config.memory.max_messages_per_run, 55);
        assert_eq!(config.memory.min_messages_per_run, 20);
        assert_eq!(config.memory.max_queued_runs, 5000);
        assert_eq!(config.memory.max_daily_enqueued_runs, 2000);
        assert_eq!(config.memory.max_input_tokens, 6000);
        assert_eq!(config.memory.tokenizer_model, "tokenizer");
        assert_eq!(config.memory.tokenizer_file, "/tmp/tokenizer.json");
        assert_eq!(config.memory.token_estimator_url, "http://tokens.test");
        assert_eq!(config.memory.token_estimator_timeout_seconds, 4);
        assert_eq!(config.memory.embedder_service_name, "memory-embedder");
        assert_eq!(config.memory.embedding_model, "embedding");
        assert_eq!(config.memory.embedding_dim, 512);
        assert_eq!(config.memory.aifarm_service_name, "svc");
        assert_eq!(config.memory.aifarm_endpoint_name, "endpoint");
        assert_eq!(config.memory.aifarm_max_output_tokens, 2048);
        assert_eq!(config.memory.aifarm_request_timeout_seconds, 61);
        assert_eq!(config.memory.aifarm_poll_interval_seconds, 3);
        assert_eq!(config.memory.aifarm_task_timeout_seconds, 99);
        assert_eq!(config.memory.aifarm_capacity_wait_seconds, 0);
        assert_eq!(config.memory.aifarm_capacity_poll_seconds, 5);
        assert_eq!(config.memory.aifarm_temperature, 0.4);
        assert!(config.memory.aifarm_enable_thinking);
        assert!(!config.memory.redaction_enabled);
        assert_eq!(config.memory.redaction_service_name, "redactor");
        assert_eq!(config.memory.redaction_endpoint_name, "redact-now");
        assert_eq!(config.memory.redaction_timeout_seconds, 12);
        assert_eq!(config.memory.redaction_task_timeout_seconds, 13);
        assert_eq!(config.memory.redaction_poll_seconds, 2);
        assert_eq!(config.memory.redaction_capacity_wait_seconds, 14);
        assert_eq!(config.memory.redaction_capacity_poll_seconds, 3);
        assert_eq!(
            config.memory.redaction_categories,
            vec!["private_email", "secret"]
        );

        Ok(())
    }

    #[test]
    fn memory_policy_defaults_and_rejects_non_positive_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig::default())?;
        assert_eq!(config.memory.min_messages_per_run, 20);
        assert_eq!(config.memory.max_queued_runs, 5000);
        assert_eq!(config.memory.max_daily_enqueued_runs, 2000);

        let error = AppConfig::from_raw(RawConfig {
            memory_min_messages_per_run: Some("0".to_owned()),
            ..RawConfig::default()
        })
        .err();
        assert!(matches!(
            error,
            Some(super::ConfigError::NonPositiveInteger {
                name: "MEMORY_MIN_MESSAGES_PER_RUN",
                value: 0,
            })
        ));

        let error = AppConfig::from_raw(RawConfig {
            memory_max_queued_runs: Some("-1".to_owned()),
            ..RawConfig::default()
        })
        .err();
        assert!(matches!(
            error,
            Some(super::ConfigError::NonPositiveInteger {
                name: "MEMORY_MAX_QUEUED_RUNS",
                value: -1,
            })
        ));

        let error = AppConfig::from_raw(RawConfig {
            memory_max_daily_enqueued_runs: Some("0".to_owned()),
            ..RawConfig::default()
        })
        .err();
        assert!(matches!(
            error,
            Some(super::ConfigError::NonPositiveInteger {
                name: "MEMORY_MAX_DAILY_ENQUEUED_RUNS",
                value: 0,
            })
        ));

        Ok(())
    }

    #[test]
    fn shield_config_loads_go_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            shield_enabled: Some("false".to_owned()),
            shield_embedder_service_name: Some("shield-embedder".to_owned()),
            shield_embedding_dim: Some("256".to_owned()),
            shield_max_matches: Some("5".to_owned()),
            shield_vector_min_score: Some("0.51".to_owned()),
            shield_lexical_min_score: Some("0.09".to_owned()),
            shield_query_max_chars: Some("1234".to_owned()),
            shield_retrieval_timeout_seconds: Some("7".to_owned()),
            shield_history_tail_messages: Some("3".to_owned()),
            ..RawConfig::default()
        })?;

        assert!(!config.shield.enabled);
        assert_eq!(config.shield.embedder_service_name, "shield-embedder");
        assert_eq!(config.shield.embedding_dim, 256);
        assert_eq!(config.shield.max_matches, 5);
        assert_eq!(config.shield.vector_min_score, 0.51);
        assert_eq!(config.shield.lexical_min_score, 0.09);
        assert_eq!(config.shield.query_max_chars, 1234);
        assert_eq!(config.shield.retrieval_timeout_seconds, 7);
        assert_eq!(config.shield.history_tail_messages, 3);

        Ok(())
    }

    #[test]
    fn vision_config_loads_go_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            vision_discovery_service_name: Some("vlm-service".to_owned()),
            vision_discovery_endpoint_name: Some("vlm-endpoint".to_owned()),
            vision_model: Some("vision-model".to_owned()),
            vision_max_tokens: Some("321".to_owned()),
            vision_temperature: Some("0.25".to_owned()),
            vision_direct_image_limit: Some("4".to_owned()),
            vision_request_timeout_seconds: Some("90".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.vision.discovery_service_name, "vlm-service");
        assert_eq!(config.vision.discovery_endpoint_name, "vlm-endpoint");
        assert_eq!(config.vision.model, "vision-model");
        assert_eq!(config.vision.max_tokens, 321);
        assert_eq!(config.vision.temperature, 0.25);
        assert_eq!(config.vision.direct_image_limit, 4);
        assert_eq!(config.vision.request_timeout_seconds, 90);

        Ok(())
    }

    #[test]
    fn vision_direct_image_limit_rejects_negative_like_go() {
        let error = AppConfig::from_raw(RawConfig {
            vision_direct_image_limit: Some("-1".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::NegativeInteger {
                name: "VISION_DIRECT_IMAGE_LIMIT",
                value: -1,
            })
        ));
    }

    #[test]
    fn history_summary_config_loads_go_genkit_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            genkit_history_summary_provider: Some(" GENKIT ".to_owned()),
            genkit_history_summary_model: Some("summary-model".to_owned()),
            genkit_history_summary_timeout_seconds: Some("777".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.llm.history_summary.provider, " GENKIT ");
        assert_eq!(config.llm.history_summary.model, "summary-model");
        assert_eq!(config.llm.history_summary.timeout_seconds, 777);
        Ok(())
    }

    #[test]
    fn google_ai_config_loads_go_env_values() -> Result<(), super::ConfigError> {
        let default_config = AppConfig::from_raw(RawConfig::default())?;
        assert_eq!(default_config.google_ai.key, "");
        assert_eq!(
            default_config.google_ai.key_stats_file,
            "googleai_key_stats.json"
        );

        let config = AppConfig::from_raw(RawConfig {
            googleai_key: Some(" gemini-key ".to_owned()),
            googleai_key_stats_file: Some("keys.json".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.google_ai.key, " gemini-key ");
        assert_eq!(config.google_ai.key_stats_file, "keys.json");
        Ok(())
    }

    #[test]
    fn invalid_history_summary_provider_is_rejected() {
        let error = AppConfig::from_raw(RawConfig {
            genkit_history_summary_provider: Some("other".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::InvalidHistorySummaryProvider { .. })
        ));
    }

    #[test]
    fn non_positive_history_summary_timeout_is_rejected() {
        let error = AppConfig::from_raw(RawConfig {
            genkit_history_summary_timeout_seconds: Some("0".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::NonPositiveInteger {
                name: "GENKIT_HISTORY_SUMMARY_TIMEOUT_SECONDS",
                ..
            })
        ));
    }

    #[test]
    fn rust_bind_override_keeps_local_shell_flexible() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_values(
            Some("127.0.0.1:18080".to_owned()),
            Some("openplotva=debug".to_owned()),
        )?;

        assert_eq!(config.server.bind_addr, "127.0.0.1:18080");
        assert_eq!(config.observability.log_filter, "openplotva=debug");

        Ok(())
    }

    #[test]
    fn invalid_port_is_rejected() {
        let error = AppConfig::from_raw(RawConfig {
            webapp_port: Some("99999".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::IntegerOutOfRange {
                name: "WEBAPP_PORT",
                ..
            })
        ));
    }

    #[test]
    fn invalid_bool_is_rejected() {
        let error = AppConfig::from_raw(RawConfig {
            openplotva_connect_services: Some("maybe".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::InvalidBoolean {
                name: "OPENPLOTVA_CONNECT_SERVICES",
                ..
            })
        ));
    }

    #[test]
    fn update_queue_backend_is_explicitly_gated() -> Result<(), super::ConfigError> {
        let stream_config = AppConfig::from_raw(RawConfig {
            update_queue_backend: Some("STREAM".to_owned()),
            ..RawConfig::default()
        })?;
        assert_eq!(stream_config.update_queue.backend, "stream");

        let error = AppConfig::from_raw(RawConfig {
            update_queue_backend: Some("sorted-set".to_owned()),
            ..RawConfig::default()
        })
        .err();
        assert!(matches!(
            error,
            Some(super::ConfigError::InvalidUpdateQueueBackend { .. })
        ));
        Ok(())
    }

    #[test]
    fn blank_scalar_values_fall_back_to_defaults() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            webapp_port: Some(String::new()),
            runtime_api_enabled: Some(" ".to_owned()),
            redis_db: Some("\t".to_owned()),
            vision_temperature: Some(String::new()),
            acestep_request_timeout_seconds: Some(String::new()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.server.port, DEFAULT_WEBAPP_PORT);
        assert!(config.runtime_api.enabled);
        assert_eq!(config.redis.db, 0);
        assert_eq!(config.vision.temperature, 0.1);
        assert_eq!(
            config.music.acestep.request_timeout_seconds,
            DEFAULT_ACESTEP_REQUEST_TIMEOUT_SECONDS
        );

        Ok(())
    }

    #[test]
    fn service_probe_update_producer_can_be_disabled_for_live_smoke()
    -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            openplotva_connect_services: Some("true".to_owned()),
            openplotva_produce_updates: Some("false".to_owned()),
            openplotva_consume_updates: Some("true".to_owned()),
            ..RawConfig::default()
        })?;

        assert!(config.service_probe.connect_services);
        assert!(!config.service_probe.produce_updates);
        assert!(config.service_probe.consume_updates);

        Ok(())
    }

    #[test]
    fn dialog_config_loads_aifarm_pool_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            discovery_base_url: Some("http://discovery.test".to_owned()),
            dialog_model: Some("custom model".to_owned()),
            dialog_request_timeout_seconds: Some("44".to_owned()),
            dialog_task_timeout_seconds: Some("130".to_owned()),
            dialog_aifarm_capacity_wait_seconds: Some("9".to_owned()),
            dialog_aifarm_pool_models: Some("primary, secondary ".to_owned()),
            dialog_aifarm_pool_base_urls: Some("http://one.test, http://two.test".to_owned()),
            dialog_aifarm_pool_api_key: Some("pool-key".to_owned()),
            dialog_nvidia_temperature: Some("0.7".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.llm.discovery.base_url, "http://discovery.test");
        assert_eq!(config.llm.dialog.model, "custom model");
        assert_eq!(config.llm.dialog.request_timeout_seconds, 44);
        assert_eq!(config.llm.dialog.task_timeout_seconds, 130);
        assert_eq!(config.llm.dialog.aifarm_capacity_wait_seconds, 9);
        assert_eq!(
            config.llm.dialog.aifarm_pool_models,
            vec!["primary", "secondary"]
        );
        assert_eq!(
            config.llm.dialog.aifarm_pool_base_urls,
            vec!["http://one.test", "http://two.test"]
        );
        assert_eq!(config.llm.dialog.aifarm_pool_api_key, "pool-key");
        assert_eq!(config.llm.dialog.nvidia_temperature, 0.7);

        Ok(())
    }

    #[test]
    fn dialog_aifarm_pool_requires_paired_models_and_urls() {
        let error = AppConfig::from_raw(RawConfig {
            dialog_aifarm_pool_models: Some("model-a,model-b".to_owned()),
            dialog_aifarm_pool_base_urls: Some("http://one.test".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::AifarmPoolPairCount)
        ));
    }

    #[test]
    fn invalid_float_is_rejected() {
        let error = AppConfig::from_raw(RawConfig {
            dialog_aifarm_temperature: Some("hot".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::InvalidFloat {
                name: "DIALOG_AIFARM_TEMPERATURE",
                ..
            })
        ));
    }

    #[test]
    fn bot_config_loads_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            bot_key: Some("123:secret".to_owned()),
            bot_api_base_url: Some("http://127.0.0.1:18081".to_owned()),
            bot_debug: Some("true".to_owned()),
            bot_webhook_enabled: Some("true".to_owned()),
            bot_webhook_url: Some("https://example.test/telegram/webhook".to_owned()),
            bot_webhook_cert_file: Some("/cert.pem".to_owned()),
            bot_webhook_key_file: Some("/key.pem".to_owned()),
            bot_webhook_secret_token: Some("webhook-secret".to_owned()),
            bot_webhook_update_buffer_size: Some("256".to_owned()),
            admins_admin_ids: Some(" 1001, 42 ".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.bot.key.as_deref(), Some("123:secret"));
        assert_eq!(config.bot.api_base_url, "http://127.0.0.1:18081");
        assert!(config.bot.debug);
        assert!(config.bot.webhook.enabled);
        assert_eq!(
            config.bot.webhook.url,
            "https://example.test/telegram/webhook"
        );
        assert_eq!(config.bot.webhook.cert_file, "/cert.pem");
        assert_eq!(config.bot.webhook.key_file, "/key.pem");
        assert_eq!(config.bot.webhook.secret_token, "webhook-secret");
        assert_eq!(config.bot.webhook.update_buffer_size, 256);
        assert_eq!(config.admins.admin_ids, vec![1_001, 42]);

        Ok(())
    }

    #[test]
    fn subscription_sync_config_loads_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            subscription_sync_enabled: Some("false".to_owned()),
            subscription_sync_interval_seconds: Some("120".to_owned()),
            subscription_sync_dry_run: Some("true".to_owned()),
            subscription_sync_page_limit: Some("50".to_owned()),
            ..RawConfig::default()
        })?;

        assert!(!config.subscription_sync.enabled);
        assert_eq!(config.subscription_sync.interval_seconds, 120);
        assert!(config.subscription_sync.dry_run);
        assert_eq!(config.subscription_sync.page_limit, 50);

        Ok(())
    }

    #[test]
    fn invalid_admin_id_is_rejected() {
        let error = AppConfig::from_raw(RawConfig {
            admins_admin_ids: Some("1001,nope".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::InvalidInteger {
                name: "ADMINS_ADMIN_IDS",
                ..
            })
        ));
    }

    #[test]
    fn translation_config_loads_go_deepl_env_values() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig {
            deepl_key: Some("deepl-secret".to_owned()),
            deepl_url: Some("https://api-free.deepl.com/v2/".to_owned()),
            ..RawConfig::default()
        })?;

        assert_eq!(config.translation.deepl.key, "deepl-secret");
        assert_eq!(
            config.translation.deepl.url,
            "https://api-free.deepl.com/v2/"
        );
        Ok(())
    }
}
