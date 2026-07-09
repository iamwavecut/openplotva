use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use openplotva_core::{ChatAttachment, ChatMessageMeta, SENDER_TYPE_USER};
use openplotva_dialog::{
    DialogToolbox, DrawRequest, HistorySearchRequest, HistorySummaryRequest,
    IMAGE_GENERATION_NOT_SCHEDULED_MESSAGE, RatesRequest, SONG_GENERATION_NOT_SCHEDULED_MESSAGE,
    SongRequest, TOOL_RESULT_STATUS_EXECUTED, TOOL_RESULT_STATUS_FAILED, TOOL_RESULT_STATUS_NOOP,
    TOOL_RESULT_STATUS_OK, TOOL_RESULT_STATUS_QUEUED, ToolError, ToolResult, ToolSideEffect,
    ToolboxError, ToolboxFuture, VisionRequest, is_internal_not_scheduled_instruction,
    sanitize_tool_text, user_facing_not_scheduled_message,
};
use openplotva_history::{StoredSummary, SummaryContent};
use openplotva_taskman::{
    DEFAULT_PRIORITY, HIGHEST_PRIORITY, IMAGE_REGULAR_QUEUE_NAME, IMAGE_VIP_QUEUE_NAME,
    ImageEditJobParams, ImageGenJobParams, InMemoryTaskQueue, MUSIC_VIP_QUEUE_NAME,
    MusicGenJobParams, fallback_queue_time_estimate, format_go_duration, new_image_edit_job_at,
    new_image_gen_job_at, new_music_gen_job_at,
};
use serde_json::json;
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

use crate::{
    agent_runtime::HistorySearcher,
    permissions::{ChatPermissionPolicy, ChatPermissionStore},
    rate_limits::TaskEnqueueRateLimit,
    rates::{RatesFetcher, RatesSideEffectDispatcher, currency_rates_tool},
    translate::{TextTranslator, TranslationResult, translate_text_tool},
};
use openplotva_server::{ACTION_SEND_AUDIO, ACTION_SEND_IMAGE};

/// Boxed future returned by taskman queue status providers.
pub type QueueStatusFuture<'a> =
    Pin<Box<dyn Future<Output = Result<QueueStatusResult, ToolboxError>> + Send + 'a>>;

/// Boxed future returned by drawing cancellation providers.
pub type CancelDrawingFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CancelDrawingResult, ToolboxError>> + Send + 'a>>;

/// Boxed future returned by web search providers.
pub type WebSearchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<String, ToolboxError>> + Send + 'a>>;

/// Boxed future returned by URL crawl providers.
pub type CrawlUrlFuture<'a> =
    Pin<Box<dyn Future<Output = Result<String, ToolboxError>> + Send + 'a>>;

/// Boxed future returned by YouTube summary providers.
pub type YouTubeSummaryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<YouTubeSummaryResult, ToolboxError>> + Send + 'a>>;

/// Boxed future returned by image scheduling providers.
pub type DrawImageFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DrawImageScheduleResult, ToolboxError>> + Send + 'a>>;

/// Boxed future returned by image-edit file resolvers.
pub type ImageEditFileResolveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ImageEditFileSelection, ToolboxError>> + Send + 'a>>;
/// Boxed future returned by image-edit Telegram file URL providers.
pub type ImageEditFileUrlFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<String>, ToolboxError>> + Send + 'a>>;

/// Boxed future returned by VIP status checks for image scheduling.
pub type DrawImageVipStatusFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// Boxed future returned by draw-image rate-limit checks.
pub type DrawImageRateLimitFuture<'a> =
    Pin<Box<dyn Future<Output = DrawImageRateLimitReport> + Send + 'a>>;
/// Boxed future returned by draw-image rate-limit increments.
pub type DrawImageRateLimitGrowFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
/// Boxed future returned by draw-image rate-limit stores.
pub type DrawImageRateLimitStoreFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by direct draw/image permission checks.
pub type DrawImagePermissionFuture<'a> =
    Pin<Box<dyn Future<Output = Option<DrawImageScheduleRejection>> + Send + 'a>>;

/// Boxed future returned by song scheduling providers.
pub type GenerateSongFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SongScheduleResult, ToolboxError>> + Send + 'a>>;

/// Boxed future returned by direct song audio permission checks.
pub type SongAudioPermissionFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// Boxed future returned by vision providers.
pub type VisionImageFuture<'a> =
    Pin<Box<dyn Future<Output = Result<VisionDescribeResult, ToolboxError>> + Send + 'a>>;

/// Boxed future returned by chat history summarizers.
pub type ChatHistorySummaryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ChatHistorySummaryResult, ToolboxError>> + Send + 'a>>;

pub trait QueueStatusProvider: Send + Sync {
    /// Return queue status for the requesting user.
    fn queue_status<'a>(&'a self, user_id: i64) -> QueueStatusFuture<'a>;
}

pub trait DrawingJobCanceller: Send + Sync {
    /// Cancel this user's image jobs in the current chat.
    fn cancel_user_image_jobs<'a>(&'a self, user_id: i64, chat_id: i64) -> CancelDrawingFuture<'a>;
}

pub trait WebSearchProvider: Send + Sync {
    /// Search for the supplied query.
    fn search<'a>(&'a self, query: &'a str) -> WebSearchFuture<'a>;
}

pub trait UrlCrawler: Send + Sync {
    /// Crawl the supplied URL.
    fn crawl<'a>(&'a self, url: &'a str) -> CrawlUrlFuture<'a>;
}

pub trait YouTubeSummarizer: Send + Sync {
    /// Summarize a YouTube URL or video ID.
    fn summarize<'a>(&'a self, video: &'a str) -> YouTubeSummaryFuture<'a>;
}

pub trait ImageScheduler: Send + Sync {
    /// Capture media-group image references before any later edit command waits for the album.
    fn capture_media_group_image(
        &self,
        _media_group_id: &str,
        _message_id: i32,
        _attachments: &[ChatAttachment],
    ) {
    }

    /// Schedule an image generation job.
    fn schedule_image<'a>(&'a self, request: DrawImageScheduleRequest) -> DrawImageFuture<'a>;

    fn direct_image_edit_vip_status<'a>(
        &'a self,
        _user_id: i64,
    ) -> Pin<Box<dyn Future<Output = Option<bool>> + Send + 'a>> {
        Box::pin(async { None })
    }

    fn direct_draw_image_rejection<'a>(&'a self, _chat_id: i64) -> DrawImagePermissionFuture<'a> {
        Box::pin(async { None })
    }
}

pub trait DrawImageRateLimit: Send + Sync {
    /// Return whether a draw-image generation request is currently rate limited.
    fn check_draw_image_rate_limit<'a>(
        &'a self,
        prompt: &'a str,
        user_id: i64,
        has_vip: bool,
        now: OffsetDateTime,
    ) -> DrawImageRateLimitFuture<'a>;

    /// Record a successfully assigned draw-image generation request.
    fn grow_draw_image_rate_limit<'a>(
        &'a self,
        user_id: i64,
        now: OffsetDateTime,
    ) -> DrawImageRateLimitGrowFuture<'a>;
}

pub trait DrawImageRateLimitStore: Send + Sync {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load timestamp history for one user.
    fn draw_image_rate_limit_timestamps<'a>(
        &'a self,
        user_id: i64,
    ) -> DrawImageRateLimitStoreFuture<'a, Vec<OffsetDateTime>, Self::Error>;

    /// Persist timestamp history for one user.
    fn set_draw_image_rate_limit_timestamps<'a>(
        &'a self,
        user_id: i64,
        timestamps: Vec<OffsetDateTime>,
        ttl: Duration,
    ) -> DrawImageRateLimitStoreFuture<'a, (), Self::Error>;
}

impl DrawImageRateLimitStore for openplotva_storage::RedisDrawRateLimitStore {
    type Error = openplotva_storage::StorageError;

    fn draw_image_rate_limit_timestamps<'a>(
        &'a self,
        user_id: i64,
    ) -> DrawImageRateLimitStoreFuture<'a, Vec<OffsetDateTime>, Self::Error> {
        Box::pin(async move { self.draw_rate_limit_timestamps(user_id).await })
    }

    fn set_draw_image_rate_limit_timestamps<'a>(
        &'a self,
        user_id: i64,
        timestamps: Vec<OffsetDateTime>,
        ttl: Duration,
    ) -> DrawImageRateLimitStoreFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.set_draw_rate_limit_timestamps(user_id, &timestamps, ttl)
                .await
        })
    }
}

pub trait ImageEditFileResolver: Send + Sync {
    fn capture_image_attachments(
        &self,
        _media_group_id: &str,
        _message_id: i32,
        _attachments: &[ChatAttachment],
    ) {
    }

    /// Resolve dialog/history image attachments into latest Telegram file IDs.
    fn resolve_image_edit_files<'a>(
        &'a self,
        attachments: &'a [ChatAttachment],
        media_group_id: &'a str,
    ) -> ImageEditFileResolveFuture<'a>;
}

pub trait ImageEditFileUrlProvider: Send + Sync {
    /// Return a Telegram file URL for the supplied file ID.
    fn image_edit_file_url<'a>(&'a self, file_id: &'a str) -> ImageEditFileUrlFuture<'a>;
}

#[derive(Clone)]
pub struct TelegramBotFileUrlProvider {
    client: openplotva_telegram::TelegramClient,
    bot_key: String,
}

impl TelegramBotFileUrlProvider {
    /// Build a Telegram file URL provider from the runtime bot client and token.
    #[must_use]
    pub fn new(client: openplotva_telegram::TelegramClient, bot_key: impl Into<String>) -> Self {
        Self {
            client,
            bot_key: bot_key.into(),
        }
    }
}

impl fmt::Debug for TelegramBotFileUrlProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramBotFileUrlProvider")
            .finish_non_exhaustive()
    }
}

impl ImageEditFileUrlProvider for TelegramBotFileUrlProvider {
    fn image_edit_file_url<'a>(&'a self, file_id: &'a str) -> ImageEditFileUrlFuture<'a> {
        Box::pin(async move {
            let file_id = file_id.trim();
            if file_id.is_empty() {
                return Ok(None);
            }
            let file: carapax::types::File = self
                .client
                .execute(carapax::types::GetFile::new(file_id))
                .await
                .map_err(|error| Box::new(error) as ToolboxError)?;
            let Some(file_path) = file
                .file_path
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty())
            else {
                return Ok(None);
            };
            Ok(Some(telegram_file_url(&self.bot_key, file_path)))
        })
    }
}

#[derive(Clone, Debug)]
pub struct MediaGroupImageRegistry {
    groups: Arc<Mutex<HashMap<String, MediaGroupEntry>>>,
    wait_time: Duration,
    max_images: usize,
}

#[derive(Clone, Debug)]
struct MediaGroupEntry {
    items: Vec<MediaGroupImageItem>,
    created_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
struct MediaGroupImageItem {
    message_id: i32,
    file_unique_ids: Vec<String>,
}

impl Default for MediaGroupImageRegistry {
    fn default() -> Self {
        Self::go_defaults()
    }
}

impl MediaGroupImageRegistry {
    #[must_use]
    pub fn go_defaults() -> Self {
        Self::new(Duration::from_secs(1), 10)
    }

    /// Build a registry with explicit timing for tests or specialized runtimes.
    #[must_use]
    pub fn new(wait_time: Duration, max_images: usize) -> Self {
        Self {
            groups: Arc::new(Mutex::new(HashMap::new())),
            wait_time,
            max_images,
        }
    }

    /// Capture one image-bearing message for later same-media-group edit collection.
    pub fn add_image_attachments(
        &self,
        media_group_id: &str,
        message_id: i32,
        attachments: &[ChatAttachment],
    ) {
        let media_group_id = media_group_id.trim();
        if media_group_id.is_empty() || message_id == 0 {
            return;
        }
        let file_unique_ids = image_edit_file_unique_ids(attachments);
        if file_unique_ids.is_empty() {
            return;
        }

        let mut groups = lock_mutex(&self.groups);
        cleanup_old_media_groups(&mut groups, OffsetDateTime::now_utc());
        let entry = groups
            .entry(media_group_id.to_owned())
            .or_insert_with(|| MediaGroupEntry {
                items: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            });
        if entry.items.iter().any(|item| item.message_id == message_id)
            || entry.items.len() >= self.max_images
        {
            return;
        }
        entry.items.push(MediaGroupImageItem {
            message_id,
            file_unique_ids,
        });
    }

    /// Wait for nearby album messages, collect unique IDs in arrival order, then remove the group.
    pub async fn wait_collect_and_remove_unique_ids(&self, media_group_id: &str) -> Vec<String> {
        let media_group_id = media_group_id.trim();
        if media_group_id.is_empty() {
            return Vec::new();
        }
        if !self.wait_time.is_zero() {
            tokio::time::sleep(self.wait_time).await;
        }
        let Some(entry) = lock_mutex(&self.groups).remove(media_group_id) else {
            return Vec::new();
        };
        let mut unique_ids = Vec::new();
        for item in entry.items {
            for file_unique_id in item.file_unique_ids {
                push_unique_trimmed(&mut unique_ids, &file_unique_id);
            }
        }
        unique_ids
    }
}

#[derive(Clone, Debug)]
pub struct TelegramImageEditFileResolver<UrlProvider> {
    files: openplotva_storage::PostgresTelegramFileStore,
    urls: UrlProvider,
    media_groups: MediaGroupImageRegistry,
}

impl<UrlProvider> TelegramImageEditFileResolver<UrlProvider> {
    /// Build a concrete image-edit file resolver.
    #[must_use]
    pub fn new(files: openplotva_storage::PostgresTelegramFileStore, urls: UrlProvider) -> Self {
        Self {
            files,
            urls,
            media_groups: MediaGroupImageRegistry::default(),
        }
    }

    /// Override the media-group registry, mainly for deterministic tests.
    #[must_use]
    pub fn with_media_group_registry(mut self, media_groups: MediaGroupImageRegistry) -> Self {
        self.media_groups = media_groups;
        self
    }
}

impl<UrlProvider> ImageEditFileResolver for TelegramImageEditFileResolver<UrlProvider>
where
    UrlProvider: ImageEditFileUrlProvider,
{
    fn capture_image_attachments(
        &self,
        media_group_id: &str,
        message_id: i32,
        attachments: &[ChatAttachment],
    ) {
        self.media_groups
            .add_image_attachments(media_group_id, message_id, attachments);
    }

    fn resolve_image_edit_files<'a>(
        &'a self,
        attachments: &'a [ChatAttachment],
        media_group_id: &'a str,
    ) -> ImageEditFileResolveFuture<'a> {
        Box::pin(async move {
            let file_unique_ids = image_edit_file_unique_ids(attachments);
            if file_unique_ids.is_empty() {
                return Ok(ImageEditFileSelection::default());
            }
            let media_group_unique_ids = self
                .media_groups
                .wait_collect_and_remove_unique_ids(media_group_id)
                .await;

            let rows = self
                .files
                .list_files_by_unique_ids(&merge_unique_strings(
                    &file_unique_ids,
                    &media_group_unique_ids,
                ))
                .await
                .map_err(|error| Box::new(error) as ToolboxError)?;
            let selection =
                image_edit_file_id_selection(&file_unique_ids, &media_group_unique_ids, &rows);
            let mut photo_urls = Vec::new();
            for latest_file_id in &selection.url_file_ids {
                if let Ok(Some(url)) = self.urls.image_edit_file_url(latest_file_id).await {
                    photo_urls.push(url);
                }
            }

            Ok(ImageEditFileSelection {
                photo_file_id: selection.photo_file_id,
                photo_urls,
            })
        })
    }
}

impl ImageEditFileResolver for openplotva_storage::PostgresTelegramFileStore {
    fn resolve_image_edit_files<'a>(
        &'a self,
        attachments: &'a [ChatAttachment],
        _media_group_id: &'a str,
    ) -> ImageEditFileResolveFuture<'a> {
        Box::pin(async move {
            let file_unique_ids = image_edit_file_unique_ids(attachments);
            if file_unique_ids.is_empty() {
                return Ok(ImageEditFileSelection::default());
            }

            let rows = self
                .list_files_by_unique_ids(&file_unique_ids)
                .await
                .map_err(|error| Box::new(error) as ToolboxError)?;
            let latest_file_ids = latest_file_ids_in_attachment_order(&file_unique_ids, &rows);

            Ok(ImageEditFileSelection {
                photo_file_id: latest_file_ids.first().cloned().unwrap_or_default(),
                photo_urls: Vec::new(),
            })
        })
    }
}

pub trait DrawImageVipStatus: Send + Sync {
    /// Return whether the user receives VIP image-generation queue semantics.
    fn is_draw_image_vip<'a>(&'a self, user_id: i64) -> DrawImageVipStatusFuture<'a>;
}

impl<T> DrawImageVipStatus for T
where
    T: crate::payments::VipStatusChecker + Send + Sync,
{
    fn is_draw_image_vip<'a>(&'a self, user_id: i64) -> DrawImageVipStatusFuture<'a> {
        Box::pin(async move { self.is_vip_at(user_id, OffsetDateTime::now_utc()).await })
    }
}

pub trait DrawImagePermission: Send + Sync {
    fn draw_image_rejection<'a>(&'a self, chat_id: i64) -> DrawImagePermissionFuture<'a>;
}

impl<Store> DrawImagePermission for ChatPermissionPolicy<Store>
where
    Store: ChatPermissionStore + Send + Sync,
{
    fn draw_image_rejection<'a>(&'a self, chat_id: i64) -> DrawImagePermissionFuture<'a> {
        Box::pin(async move {
            let report = self
                .can_perform_action_at(chat_id, None, ACTION_SEND_IMAGE, OffsetDateTime::now_utc())
                .await;
            if report.allowed {
                None
            } else {
                Some(DrawImageScheduleRejection::DrawDisabled)
            }
        })
    }
}

pub trait SongScheduler: Send + Sync {
    /// Schedule a song generation job.
    fn schedule_song<'a>(&'a self, request: SongScheduleRequest) -> GenerateSongFuture<'a>;
}

pub trait SongAudioPermission: Send + Sync {
    /// Return whether generated audio may be sent to this chat.
    fn can_send_song_audio<'a>(&'a self, chat_id: i64) -> SongAudioPermissionFuture<'a>;
}

impl<Store> SongAudioPermission for ChatPermissionPolicy<Store>
where
    Store: ChatPermissionStore + Send + Sync,
{
    fn can_send_song_audio<'a>(&'a self, chat_id: i64) -> SongAudioPermissionFuture<'a> {
        Box::pin(async move {
            self.can_perform_action_at(chat_id, None, ACTION_SEND_AUDIO, OffsetDateTime::now_utc())
                .await
                .allowed
        })
    }
}

pub trait VisionDescriber: Send + Sync {
    /// Describe an image attachment.
    fn describe_image<'a>(&'a self, request: VisionDescribeRequest) -> VisionImageFuture<'a>;
}

pub trait ChatHistorySummarizer: Send + Sync {
    /// Generate or reuse a chat history summary.
    fn chat_history_summary<'a>(
        &'a self,
        request: HistorySummaryRequest,
    ) -> ChatHistorySummaryFuture<'a>;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueueStatusResult {
    /// Regular image queue depth.
    pub regular_queue_depth: i64,
    /// VIP image queue depth.
    pub vip_queue_depth: i64,
    /// Active image job count.
    pub active_jobs_count: i64,
    /// Active image jobs for this user.
    pub user_active_jobs: i64,
    /// Pending image jobs for this user.
    pub user_pending_jobs: i64,
    pub estimated_wait: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CancelDrawingResult {
    /// Number of cancelled image jobs.
    pub cancelled_count: i64,
    /// Cancelled image job IDs.
    pub cancelled_ids: Vec<i64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct YouTubeSummaryResult {
    /// Summary HTML/text.
    pub summary: String,
    /// Transcript text.
    pub transcript: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DrawImageScheduleRequest {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Forum topic ID.
    pub thread_id: Option<i32>,
    /// Source Telegram message ID.
    pub message_id: i32,
    /// Caller Telegram user ID.
    pub user_id: i64,
    /// Caller display name.
    pub user_full_name: String,
    /// Sanitized image prompt.
    pub prompt: String,
    /// Source message text.
    pub message_text: String,
    /// Message attachments copied from dialog metadata.
    pub attachments: Vec<ChatAttachment>,
    pub edit_media_group_id: String,
    /// Sanitized negative prompt.
    pub negative_prompt: String,
    /// Sanitized aspect ratio.
    pub aspect_ratio: String,
    /// Sanitized seed.
    pub seed: String,
    pub original_prompt: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DrawImageScheduleResult {
    /// Scheduler status string.
    pub status: String,
    /// User/model-facing scheduler message.
    pub message: String,
    /// Whether no dialog reply should be sent.
    pub no_reply: bool,
    pub rejection: Option<DrawImageScheduleRejection>,
    /// Taskman ticket id assigned to a scheduled generation job.
    pub job_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrawImageScheduleRejection {
    /// Caller is not VIP.
    VipOnly,
    /// Draw-image (generation) rate limit fired for this prompt/user.
    RateLimited,
    DrawDisabled,
    ImageNotAllowed,
    /// Caller already runs the maximum number of active image jobs.
    ActiveJobLimit,
    /// Enqueue throttled by the task-enqueue rate limit (flood gate).
    EnqueueRateLimited,
    /// The queue refused the assignment.
    QueueFull,
    /// Image-edit source photo could not be resolved.
    EditSourceUnavailable,
}

impl DrawImageScheduleRejection {
    /// Stable machine-readable rejection code carried in `ToolResult.data["reason"]`.
    #[must_use]
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::VipOnly => "vip_only",
            Self::RateLimited => "generation_rate_limited",
            Self::DrawDisabled => "draw_disabled",
            Self::ImageNotAllowed => "image_not_allowed",
            Self::ActiveJobLimit => "active_job_limit",
            Self::EnqueueRateLimited => "enqueue_rate_limited",
            Self::QueueFull => "queue_full",
            Self::EditSourceUnavailable => "edit_source_unavailable",
        }
    }

    /// Model-facing refusal description; the model composes the user-visible
    /// in-character reply from it.
    #[must_use]
    pub const fn model_facing_message(self) -> &'static str {
        match self {
            Self::VipOnly => {
                "Эта функция доступна только VIP-пользователям. Задача не поставлена в очередь."
            }
            Self::RateLimited => {
                "Сработал лимит частоты генерации изображений. Задача не поставлена в очередь."
            }
            Self::DrawDisabled => {
                "Рисование отключено в настройках этого чата. Задача не поставлена в очередь."
            }
            Self::ImageNotAllowed => {
                "В этот чат нельзя отправлять изображения. Задача не поставлена в очередь."
            }
            Self::ActiveJobLimit => {
                "У пользователя уже выполняется максимум задач генерации изображений. Новая задача не поставлена в очередь."
            }
            Self::EnqueueRateLimited => {
                "Слишком много запросов подряд, сработал лимит очереди. Задача не поставлена в очередь."
            }
            Self::QueueFull => {
                "Очередь генерации изображений сейчас переполнена. Задача не поставлена в очередь."
            }
            Self::EditSourceUnavailable => {
                "Не удалось получить исходное изображение для редактирования. Задача не поставлена в очередь."
            }
        }
    }
}

/// Draw-image rate-limit decision.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DrawImageRateLimitReport {
    /// Whether the request should be blocked.
    pub limited: bool,
    /// User-facing notice text for a limited request before VIP upsell shaping.
    pub notice_text: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageEditFileSelection {
    pub photo_file_id: String,
    pub photo_urls: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SongScheduleRequest {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Forum topic ID.
    pub thread_id: Option<i32>,
    /// Source Telegram message ID.
    pub message_id: i32,
    /// Caller Telegram user ID.
    pub user_id: i64,
    /// Caller display name.
    pub user_full_name: String,
    /// Sanitized song topic.
    pub topic: String,
    /// Source message text.
    pub message_text: String,
    /// History/dialog message metadata.
    pub message_meta: ChatMessageMeta,
    /// Telegram file ID for optional audio reference.
    pub reference_file_id: String,
    /// Telegram stable unique ID for optional audio reference.
    pub reference_file_unique_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SongScheduleResult {
    /// Scheduler status string.
    pub status: String,
    /// User/model-facing scheduler message.
    pub message: String,
    /// Whether no dialog reply should be sent.
    pub no_reply: bool,
    pub rejection: Option<SongScheduleRejection>,
    pub queue_notice: Option<SongQueueNotice>,
    /// Taskman ticket id assigned to a scheduled generation job.
    pub job_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SongScheduleRejection {
    /// Music generation runtime is not configured.
    ServiceUnavailable,
    /// Bot cannot send audio in this chat.
    AudioNotAllowed,
    /// Caller is not VIP.
    VipOnly,
    EmptyTopic,
    /// User already has the maximum number of active music jobs.
    ActiveLimit,
    /// Enqueue throttled by the task-enqueue rate limit (distinct from a capacity limit).
    RateLimited,
}

impl SongScheduleRejection {
    /// Stable machine-readable rejection code carried in `ToolResult.data["reason"]`.
    #[must_use]
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::ServiceUnavailable => "service_unavailable",
            Self::AudioNotAllowed => "audio_not_allowed",
            Self::VipOnly => "vip_only",
            Self::EmptyTopic => "empty_topic",
            Self::ActiveLimit => "active_job_limit",
            Self::RateLimited => "enqueue_rate_limited",
        }
    }

    /// Model-facing refusal description; the model composes the user-visible
    /// in-character reply from it.
    #[must_use]
    pub const fn model_facing_message(self) -> &'static str {
        match self {
            Self::ServiceUnavailable => {
                "Сервис генерации музыки сейчас недоступен. Задача не поставлена в очередь."
            }
            Self::AudioNotAllowed => {
                "В этот чат нельзя отправлять аудио. Песня не поставлена в очередь."
            }
            Self::VipOnly => {
                "Генерация песен доступна только VIP-пользователям. Задача не поставлена в очередь."
            }
            Self::EmptyTopic => {
                "Тема песни пустая, задача не поставлена в очередь. Уточни тему и повтори вызов инструмента."
            }
            Self::ActiveLimit => {
                "У пользователя уже выполняется максимум музыкальных задач. Новая задача не поставлена в очередь."
            }
            Self::RateLimited => {
                "Слишком много запросов подряд, сработал лимит очереди. Задача не поставлена в очередь."
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SongQueueNotice {
    pub position: usize,
    pub estimated_wait: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct VisionDescribeRequest {
    /// Attachment handle.
    pub file_id: String,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Source Telegram message ID.
    pub message_id: i32,
    /// Forum topic ID.
    pub thread_id: Option<i32>,
    /// History/dialog message metadata.
    pub message_meta: ChatMessageMeta,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VisionDescribeResult {
    /// Generated caption.
    pub caption: String,
    /// Caption source label.
    pub source: String,
    /// Telegram file unique ID.
    pub file_unique_id: String,
    /// Whether chat history was updated.
    pub history_updated: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChatHistorySummaryResult {
    /// Stored summary ID.
    pub summary_id: i64,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Forum topic ID, or zero for whole chat.
    pub thread_id: i32,
    /// Summary scope.
    pub scope: String,
    pub range_start_at: String,
    pub range_end_at: String,
    /// Raw selected message count.
    pub raw_message_count: i32,
    /// Covered message count.
    pub covered_message_count: i32,
    /// Number of reused summaries.
    pub reused_summary_count: i32,
    /// Reused source summary IDs.
    pub source_summary_ids: Vec<i64>,
    /// Summary HTML.
    pub summary_html: String,
    /// Summary JSON content.
    pub summary_json: SummaryContent,
    /// Model name.
    pub model: String,
}

impl ChatHistorySummaryResult {
    #[must_use]
    pub fn from_stored_summary(stored: &StoredSummary, reused_summary_count: i32) -> Self {
        Self {
            summary_id: stored.id,
            chat_id: stored.chat_id,
            thread_id: stored.thread_id,
            scope: stored.scope.as_str().to_owned(),
            range_start_at: format_go_json_time(stored.range_start_at),
            range_end_at: format_go_json_time(stored.range_end_at),
            raw_message_count: stored.raw_message_count,
            covered_message_count: stored.covered_message_count,
            reused_summary_count,
            source_summary_ids: stored.source_summary_ids.clone(),
            summary_html: stored.summary_html.clone(),
            summary_json: stored.summary_json.clone(),
            model: stored.model.clone(),
        }
    }
}

/// Dialog tool adapter backed by the shared Rust-native taskman queue core.
#[derive(Clone)]
pub struct TaskmanDialogToolAdapter {
    queue: Arc<InMemoryTaskQueue>,
    draw_image_vip_status: Option<Arc<dyn DrawImageVipStatus>>,
    draw_image_rate_limit: Option<Arc<dyn DrawImageRateLimit>>,
    task_enqueue_rate_limit: Option<Arc<dyn TaskEnqueueRateLimit>>,
    draw_image_permission: Option<Arc<dyn DrawImagePermission>>,
    image_edit_file_resolver: Option<Arc<dyn ImageEditFileResolver>>,
    song_service_available: bool,
    song_audio_permission: Option<Arc<dyn SongAudioPermission>>,
    generation_reactions: Option<crate::reactions::GenerationReactions>,
    delivery_obligations: Option<Arc<dyn crate::dialog_turn::DeliveryObligationRecorder>>,
    image_delivery_timeout: time::Duration,
    music_delivery_timeout: time::Duration,
}

impl fmt::Debug for TaskmanDialogToolAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskmanDialogToolAdapter")
            .field("queue", &self.queue)
            .field(
                "draw_image_vip_status",
                &self.draw_image_vip_status.as_ref().map(|_| "configured"),
            )
            .field(
                "draw_image_rate_limit",
                &self.draw_image_rate_limit.as_ref().map(|_| "configured"),
            )
            .field(
                "task_enqueue_rate_limit",
                &self.task_enqueue_rate_limit.as_ref().map(|_| "configured"),
            )
            .field(
                "draw_image_permission",
                &self.draw_image_permission.as_ref().map(|_| "configured"),
            )
            .field(
                "image_edit_file_resolver",
                &self.image_edit_file_resolver.as_ref().map(|_| "configured"),
            )
            .field("song_service_available", &self.song_service_available)
            .field(
                "song_audio_permission",
                &self.song_audio_permission.as_ref().map(|_| "configured"),
            )
            .field(
                "generation_reactions",
                &self.generation_reactions.as_ref().map(|_| "configured"),
            )
            .field(
                "delivery_obligations",
                &self.delivery_obligations.as_ref().map(|_| "configured"),
            )
            .finish()
    }
}

impl TaskmanDialogToolAdapter {
    /// Build a taskman-backed dialog tool adapter.
    #[must_use]
    pub fn new(queue: Arc<InMemoryTaskQueue>) -> Self {
        Self {
            queue,
            draw_image_vip_status: None,
            draw_image_rate_limit: None,
            task_enqueue_rate_limit: None,
            draw_image_permission: None,
            image_edit_file_resolver: None,
            song_service_available: true,
            song_audio_permission: None,
            generation_reactions: None,
            delivery_obligations: None,
            image_delivery_timeout: time::Duration::seconds(i64::from(
                crate::dialog_turn::DEFAULT_DIALOG_IMAGE_DELIVERY_TIMEOUT_SECS,
            )),
            music_delivery_timeout: time::Duration::seconds(i64::from(
                crate::dialog_turn::DEFAULT_DIALOG_MUSIC_DELIVERY_TIMEOUT_SECS,
            )),
        }
    }

    /// Configure delivery-obligation recording: every successfully assigned
    /// generation ticket inserts a watched obligation with a kind-specific
    /// deadline. Insert failures log a warning and never block scheduling.
    #[must_use]
    pub fn with_delivery_obligations(
        mut self,
        recorder: Arc<dyn crate::dialog_turn::DeliveryObligationRecorder>,
        image_timeout_secs: i32,
        music_timeout_secs: i32,
    ) -> Self {
        self.delivery_obligations = Some(recorder);
        self.image_delivery_timeout = time::Duration::seconds(i64::from(image_timeout_secs.max(1)));
        self.music_delivery_timeout = time::Duration::seconds(i64::from(music_timeout_secs.max(1)));
        self
    }

    /// Acknowledge queued generations with a reaction on the trigger message.
    #[must_use]
    pub fn with_generation_reactions(
        mut self,
        reactions: crate::reactions::GenerationReactions,
    ) -> Self {
        self.generation_reactions = Some(reactions);
        self
    }

    /// Best-effort queued-generation acknowledgment: a 👀 reaction on the
    /// trigger message when reactions are configured, otherwise the legacy
    /// queue-position placeholder message (draw paths only).
    async fn ack_queued_generation(&self, chat_id: i64, message_id: i32) {
        if let Some(reactions) = self.generation_reactions.as_deref() {
            reactions
                .set_queued(crate::reactions::GenerationReactionTarget {
                    chat_id,
                    message_id,
                })
                .await;
        }
    }

    #[must_use]
    pub fn with_draw_image_vip_status(mut self, vip_status: Arc<dyn DrawImageVipStatus>) -> Self {
        self.draw_image_vip_status = Some(vip_status);
        self
    }

    #[must_use]
    pub fn with_draw_image_rate_limit(mut self, rate_limit: Arc<dyn DrawImageRateLimit>) -> Self {
        self.draw_image_rate_limit = Some(rate_limit);
        self
    }

    #[must_use]
    pub fn with_task_enqueue_rate_limit(
        mut self,
        rate_limit: Arc<dyn TaskEnqueueRateLimit>,
    ) -> Self {
        self.task_enqueue_rate_limit = Some(rate_limit);
        self
    }

    #[must_use]
    pub fn with_draw_image_permission(mut self, permission: Arc<dyn DrawImagePermission>) -> Self {
        self.draw_image_permission = Some(permission);
        self
    }

    #[must_use]
    pub fn with_image_edit_file_resolver(
        mut self,
        resolver: Arc<dyn ImageEditFileResolver>,
    ) -> Self {
        self.image_edit_file_resolver = Some(resolver);
        self
    }

    #[must_use]
    pub fn with_song_service_available(mut self, available: bool) -> Self {
        self.song_service_available = available;
        self
    }

    #[must_use]
    pub fn with_song_audio_permission(mut self, permission: Arc<dyn SongAudioPermission>) -> Self {
        self.song_audio_permission = Some(permission);
        self
    }
}

impl QueueStatusProvider for TaskmanDialogToolAdapter {
    fn queue_status<'a>(&'a self, user_id: i64) -> QueueStatusFuture<'a> {
        let status = self.queue.image_queue_status(user_id);
        Box::pin(async move {
            Ok(QueueStatusResult {
                regular_queue_depth: usize_to_i64(status.regular_queue_depth),
                vip_queue_depth: usize_to_i64(status.vip_queue_depth),
                active_jobs_count: usize_to_i64(status.active_jobs_count),
                user_active_jobs: usize_to_i64(status.user_active_jobs),
                user_pending_jobs: usize_to_i64(status.user_pending_jobs),
                estimated_wait: status.go_estimated_wait_string(),
            })
        })
    }
}

impl DrawingJobCanceller for TaskmanDialogToolAdapter {
    fn cancel_user_image_jobs<'a>(&'a self, user_id: i64, chat_id: i64) -> CancelDrawingFuture<'a> {
        let cancelled_ids = self.queue.cancel_user_jobs(
            user_id,
            chat_id,
            "cancelled by user",
            OffsetDateTime::now_utc(),
        );
        Box::pin(async move {
            Ok(CancelDrawingResult {
                cancelled_count: usize_to_i64(cancelled_ids.len()),
                cancelled_ids,
            })
        })
    }
}

impl ImageScheduler for TaskmanDialogToolAdapter {
    fn capture_media_group_image(
        &self,
        media_group_id: &str,
        message_id: i32,
        attachments: &[ChatAttachment],
    ) {
        if let Some(resolver) = self.image_edit_file_resolver.as_deref() {
            resolver.capture_image_attachments(media_group_id, message_id, attachments);
        }
    }

    fn schedule_image<'a>(&'a self, request: DrawImageScheduleRequest) -> DrawImageFuture<'a> {
        Box::pin(async move {
            if request_has_image_attachment(&request) {
                return self.schedule_image_edit(request).await;
            }
            if let Some(rejection) = self.draw_image_permission_rejection(request.chat_id).await {
                return Ok(not_scheduled_draw(rejection));
            }
            let has_vip = match self.draw_image_vip_status.as_deref() {
                Some(vip_status) => vip_status.is_draw_image_vip(request.user_id).await,
                None => false,
            };
            let plan = image_gen_queue_plan(has_vip);
            if self
                .queue
                .user_active_count(plan.queue_name, request.user_id)
                >= plan.max_active_jobs
            {
                return Ok(not_scheduled_draw(
                    DrawImageScheduleRejection::ActiveJobLimit,
                ));
            }
            let base_prompt = request.prompt.trim().to_owned();
            if let Some(rate_limit) = self.draw_image_rate_limit.as_deref() {
                let report = rate_limit
                    .check_draw_image_rate_limit(
                        &base_prompt,
                        request.user_id,
                        has_vip,
                        OffsetDateTime::now_utc(),
                    )
                    .await;
                if report.limited {
                    return Ok(DrawImageScheduleResult {
                        status: "not_scheduled".to_owned(),
                        message: draw_rate_limit_response_text(&report.notice_text, has_vip),
                        no_reply: false,
                        rejection: Some(DrawImageScheduleRejection::RateLimited),
                        job_id: None,
                    });
                }
            }
            let chat_id = request.chat_id;
            let user_id = request.user_id;
            let message_id = request.message_id;
            let thread_id = request.thread_id;
            let meta = draw_image_job_meta(&request);
            let job = new_image_gen_job_at(
                ImageGenJobParams {
                    chat_id: request.chat_id,
                    message_id: request.message_id,
                    user_id: request.user_id,
                    user_full_name: request.user_full_name,
                    prompt: base_prompt.clone(),
                    original_text: base_prompt,
                    meta,
                    negative_prompt: request.negative_prompt,
                    aspect_ratio: request.aspect_ratio,
                    seed: request.seed,
                    thread_id: request.thread_id,
                    ..ImageGenJobParams::default()
                },
                OffsetDateTime::now_utc(),
            )
            .with_name("image")
            .with_priority(plan.priority);

            if self
                .task_enqueue_limited(plan.queue_name, chat_id, user_id)
                .await
            {
                return Ok(not_scheduled_draw(
                    DrawImageScheduleRejection::EnqueueRateLimited,
                ));
            }
            match self.assign_image_job(plan.queue_name, job, plan.max_active_jobs) {
                Ok(Some(job_id)) => {
                    self.grow_task_enqueue(plan.queue_name, chat_id, user_id)
                        .await;
                    if let Some(rate_limit) = self.draw_image_rate_limit.as_deref() {
                        rate_limit
                            .grow_draw_image_rate_limit(request.user_id, OffsetDateTime::now_utc())
                            .await;
                    }
                    self.record_delivery_obligation(
                        openplotva_dialog::turn::SIDE_EFFECT_KIND_IMAGE,
                        job_id,
                        chat_id,
                        thread_id,
                        user_id,
                        message_id,
                    )
                    .await;
                    self.ack_queued_generation(chat_id, message_id).await;
                    Ok(DrawImageScheduleResult {
                        status: "scheduled".to_owned(),
                        job_id: Some(job_id),
                        ..DrawImageScheduleResult::default()
                    })
                }
                Ok(None) => Ok(not_scheduled_draw(DrawImageScheduleRejection::QueueFull)),
                Err(error) => Err(Box::new(error) as ToolboxError),
            }
        })
    }

    fn direct_image_edit_vip_status<'a>(
        &'a self,
        user_id: i64,
    ) -> Pin<Box<dyn Future<Output = Option<bool>> + Send + 'a>> {
        Box::pin(async move {
            let has_vip = match self.draw_image_vip_status.as_deref() {
                Some(vip_status) => vip_status.is_draw_image_vip(user_id).await,
                None => false,
            };
            Some(has_vip)
        })
    }

    fn direct_draw_image_rejection<'a>(&'a self, chat_id: i64) -> DrawImagePermissionFuture<'a> {
        Box::pin(async move { self.draw_image_permission_rejection(chat_id).await })
    }
}

impl SongScheduler for TaskmanDialogToolAdapter {
    fn schedule_song<'a>(&'a self, request: SongScheduleRequest) -> GenerateSongFuture<'a> {
        Box::pin(async move {
            if !self.song_service_available {
                return Ok(not_scheduled_song(
                    SongScheduleRejection::ServiceUnavailable,
                ));
            }
            if let Some(permission) = self.song_audio_permission.as_deref()
                && !permission.can_send_song_audio(request.chat_id).await
            {
                return Ok(not_scheduled_song(SongScheduleRejection::AudioNotAllowed));
            }
            let has_vip = match self.draw_image_vip_status.as_deref() {
                Some(vip_status) => vip_status.is_draw_image_vip(request.user_id).await,
                None => false,
            };
            if !has_vip {
                return Ok(not_scheduled_song(SongScheduleRejection::VipOnly));
            }
            let topic = request.topic.trim().to_owned();
            if topic.is_empty() {
                return Ok(not_scheduled_song(SongScheduleRejection::EmptyTopic));
            }
            let reference_file_unique_id =
                song_reference_unique_id(&request.reference_file_unique_id, &request.message_meta);
            let meta = serde_json::to_value(request.message_meta).unwrap_or_else(|_| json!({}));
            let chat_id = request.chat_id;
            let user_id = request.user_id;
            let queue_depth = self
                .queue
                .queue_depth_for_priority_or_higher(MUSIC_VIP_QUEUE_NAME, HIGHEST_PRIORITY);
            let queue_notice = (queue_depth + 1 > 3).then(|| SongQueueNotice {
                position: queue_depth + 1,
                estimated_wait: format_go_duration(fallback_queue_time_estimate(
                    MUSIC_VIP_QUEUE_NAME,
                    queue_depth,
                )),
            });
            let job = new_music_gen_job_at(
                MusicGenJobParams {
                    chat_id: request.chat_id,
                    message_id: request.message_id,
                    user_id: request.user_id,
                    user_full_name: request.user_full_name,
                    topic,
                    reference_file_id: request.reference_file_id,
                    reference_file_unique_id,
                    meta,
                    thread_id: request.thread_id,
                    ..MusicGenJobParams::default()
                },
                OffsetDateTime::now_utc(),
            )
            .with_name("music")
            .with_priority(HIGHEST_PRIORITY);

            if self
                .task_enqueue_limited(MUSIC_VIP_QUEUE_NAME, chat_id, user_id)
                .await
            {
                return Ok(not_scheduled_song(SongScheduleRejection::RateLimited));
            }
            match self
                .queue
                .assign_with_user_limit(MUSIC_VIP_QUEUE_NAME, job, 2)
            {
                Ok(Some(job_id)) => {
                    self.grow_task_enqueue(MUSIC_VIP_QUEUE_NAME, chat_id, user_id)
                        .await;
                    self.record_delivery_obligation(
                        openplotva_dialog::turn::SIDE_EFFECT_KIND_MUSIC,
                        job_id,
                        chat_id,
                        request.thread_id,
                        user_id,
                        request.message_id,
                    )
                    .await;
                    self.ack_queued_generation(chat_id, request.message_id)
                        .await;
                    Ok(SongScheduleResult {
                        status: "scheduled".to_owned(),
                        queue_notice,
                        job_id: Some(job_id),
                        ..SongScheduleResult::default()
                    })
                }
                Ok(None) => Ok(not_scheduled_song(SongScheduleRejection::ActiveLimit)),
                Err(error) => Err(Box::new(error) as ToolboxError),
            }
        })
    }
}

impl TaskmanDialogToolAdapter {
    async fn schedule_image_edit(
        &self,
        request: DrawImageScheduleRequest,
    ) -> Result<DrawImageScheduleResult, ToolboxError> {
        let Some(resolver) = self.image_edit_file_resolver.as_deref() else {
            return Ok(not_scheduled_draw(
                DrawImageScheduleRejection::EditSourceUnavailable,
            ));
        };
        let files = resolver
            .resolve_image_edit_files(&request.attachments, &request.edit_media_group_id)
            .await?;
        if files.photo_file_id.trim().is_empty() {
            return Ok(not_scheduled_draw(
                DrawImageScheduleRejection::EditSourceUnavailable,
            ));
        }
        if let Some(rejection) = self.draw_image_permission_rejection(request.chat_id).await {
            return Ok(not_scheduled_draw(rejection));
        }
        let has_vip = match self.draw_image_vip_status.as_deref() {
            Some(vip_status) => vip_status.is_draw_image_vip(request.user_id).await,
            None => false,
        };
        if !has_vip {
            return Ok(not_scheduled_draw(DrawImageScheduleRejection::VipOnly));
        }

        let plan = image_gen_queue_plan(true);
        let prompt = request.prompt.trim().to_owned();
        let chat_id = request.chat_id;
        let user_id = request.user_id;
        let message_id = request.message_id;
        let thread_id = request.thread_id;
        let job = new_image_edit_job_at(
            ImageEditJobParams {
                chat_id: request.chat_id,
                message_id: request.message_id,
                user_id: request.user_id,
                user_full_name: request.user_full_name,
                prompt,
                photo_file_id: files.photo_file_id,
                photo_urls: files.photo_urls,
                thread_id: request.thread_id,
            },
            OffsetDateTime::now_utc(),
        )
        .with_name("image_edit")
        .with_priority(plan.priority);

        if self
            .task_enqueue_limited(plan.queue_name, chat_id, user_id)
            .await
        {
            return Ok(not_scheduled_draw(
                DrawImageScheduleRejection::EnqueueRateLimited,
            ));
        }
        match self.assign_image_job(plan.queue_name, job, plan.max_active_jobs) {
            Ok(Some(job_id)) => {
                self.grow_task_enqueue(plan.queue_name, chat_id, user_id)
                    .await;
                self.record_delivery_obligation(
                    openplotva_dialog::turn::SIDE_EFFECT_KIND_IMAGE,
                    job_id,
                    chat_id,
                    thread_id,
                    user_id,
                    message_id,
                )
                .await;
                self.ack_queued_generation(chat_id, message_id).await;
                Ok(DrawImageScheduleResult {
                    status: "scheduled".to_owned(),
                    job_id: Some(job_id),
                    ..DrawImageScheduleResult::default()
                })
            }
            Ok(None) => Ok(not_scheduled_draw(DrawImageScheduleRejection::QueueFull)),
            Err(error) => Err(Box::new(error) as ToolboxError),
        }
    }

    async fn draw_image_permission_rejection(
        &self,
        chat_id: i64,
    ) -> Option<DrawImageScheduleRejection> {
        let permission = self.draw_image_permission.as_deref()?;
        permission.draw_image_rejection(chat_id).await
    }

    async fn task_enqueue_limited(&self, queue_name: &str, chat_id: i64, user_id: i64) -> bool {
        let Some(rate_limit) = self.task_enqueue_rate_limit.as_deref() else {
            return false;
        };
        let report = rate_limit
            .check_task_enqueue_rate_limit(chat_id, user_id, OffsetDateTime::now_utc())
            .await;
        if report.limited {
            tracing::warn!(
                chat_id,
                user_id,
                queue_name,
                scope = ?report.scope,
                error = ?report.error,
                "task enqueue rate limit blocked job assignment"
            );
        }
        report.limited
    }

    async fn grow_task_enqueue(&self, queue_name: &str, chat_id: i64, user_id: i64) {
        let Some(rate_limit) = self.task_enqueue_rate_limit.as_deref() else {
            return;
        };
        let report = rate_limit
            .grow_task_enqueue_rate_limit(chat_id, user_id, OffsetDateTime::now_utc())
            .await;
        if !report.save_errors.is_empty() {
            tracing::warn!(
                chat_id,
                user_id,
                queue_name,
                errors = ?report.save_errors,
                "failed to persist task enqueue rate-limit timestamps"
            );
        }
    }

    /// Record the delivery obligation for a freshly assigned generation ticket.
    /// Runs at schedule time (punch #5): the promise is durable even if the
    /// dialog turn later fails, retries, or crashes. Failure is logged and
    /// never blocks scheduling.
    async fn record_delivery_obligation(
        &self,
        kind: &'static str,
        ticket_job_id: i64,
        chat_id: i64,
        thread_id: Option<i32>,
        user_id: i64,
        trigger_message_id: i32,
    ) {
        let Some(recorder) = self.delivery_obligations.as_deref() else {
            return;
        };
        let timeout = if kind == openplotva_dialog::turn::SIDE_EFFECT_KIND_MUSIC {
            self.music_delivery_timeout
        } else {
            self.image_delivery_timeout
        };
        let obligation = openplotva_storage::NewDeliveryObligation {
            chat_id,
            thread_id,
            user_id,
            trigger_message_id,
            dialog_job_id: openplotva_storage::DELIVERY_OBLIGATION_DIALOG_JOB_UNKNOWN,
            kind: kind.to_owned(),
            ticket_job_id,
            deadline_at: OffsetDateTime::now_utc() + timeout,
        };
        match recorder.record_delivery_obligation(obligation).await {
            Ok(_inserted) => {}
            Err(error) => {
                tracing::warn!(
                    chat_id,
                    ticket_job_id,
                    kind,
                    %error,
                    "failed to record delivery obligation; generation delivery is unwatched"
                );
            }
        }
    }

    fn assign_image_job(
        &self,
        queue_name: &str,
        job: openplotva_taskman::StatelessJobItem,
        max_active_jobs: usize,
    ) -> Result<Option<i64>, openplotva_taskman::TaskQueueError> {
        self.queue
            .assign_with_user_limit(queue_name, job, max_active_jobs)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ImageGenQueuePlan {
    queue_name: &'static str,
    priority: openplotva_taskman::Priority,
    max_active_jobs: usize,
}

fn image_gen_queue_plan(has_vip: bool) -> ImageGenQueuePlan {
    if has_vip {
        return ImageGenQueuePlan {
            queue_name: IMAGE_VIP_QUEUE_NAME,
            priority: HIGHEST_PRIORITY,
            max_active_jobs: 10,
        };
    }
    ImageGenQueuePlan {
        queue_name: IMAGE_REGULAR_QUEUE_NAME,
        priority: DEFAULT_PRIORITY,
        max_active_jobs: 5,
    }
}

fn request_has_image_attachment(request: &DrawImageScheduleRequest) -> bool {
    request.attachments.iter().any(|attachment| {
        attachment.kind.trim() == "image" && !attachment.file_unique_id.trim().is_empty()
    })
}

/// Rejections always feed back to the model (`no_reply: false`): the tool
/// result carries a model-facing refusal message plus a machine-readable
/// reason code, and the model composes the user-visible reply.
fn not_scheduled_draw(rejection: DrawImageScheduleRejection) -> DrawImageScheduleResult {
    DrawImageScheduleResult {
        status: "not_scheduled".to_owned(),
        message: rejection.model_facing_message().to_owned(),
        no_reply: false,
        rejection: Some(rejection),
        ..DrawImageScheduleResult::default()
    }
}

#[derive(Clone, Debug)]
pub struct DrawImageRateLimitPolicy<S> {
    store: S,
    whitelist: Vec<String>,
}

impl<S> DrawImageRateLimitPolicy<S> {
    #[must_use]
    pub fn new(store: S) -> Self {
        Self {
            store,
            whitelist: normalize_draw_prompt_whitelist([
                "котик",
                "собачка",
                "цветочек",
                "плотву",
                "себя",
                "меня",
            ]),
        }
    }

    #[must_use]
    pub fn with_whitelist(mut self, whitelist: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        self.whitelist = normalize_draw_prompt_whitelist(whitelist);
        self
    }
}

impl<S> DrawImageRateLimit for DrawImageRateLimitPolicy<S>
where
    S: DrawImageRateLimitStore,
{
    fn check_draw_image_rate_limit<'a>(
        &'a self,
        prompt: &'a str,
        user_id: i64,
        has_vip: bool,
        now: OffsetDateTime,
    ) -> DrawImageRateLimitFuture<'a> {
        Box::pin(async move {
            if is_draw_prompt_whitelisted(prompt, &self.whitelist) {
                return DrawImageRateLimitReport::default();
            }
            let timestamps = match self.store.draw_image_rate_limit_timestamps(user_id).await {
                Ok(timestamps) => timestamps,
                Err(error) => {
                    tracing::warn!(%error, user_id, "failed to load draw image rate-limit state");
                    return DrawImageRateLimitReport::default();
                }
            };
            let timestamps = recent_draw_rate_limit_timestamps(now, timestamps);
            let Some(remaining) =
                draw_rate_limit_remaining(now, &timestamps, draw_rate_limit_thresholds(has_vip))
            else {
                return DrawImageRateLimitReport::default();
            };
            DrawImageRateLimitReport {
                limited: true,
                notice_text: format!(
                    "Вы часто повторяете этот запрос. Попробуйте снова через {}.",
                    format_draw_rate_limit_duration(remaining)
                ),
            }
        })
    }

    fn grow_draw_image_rate_limit<'a>(
        &'a self,
        user_id: i64,
        now: OffsetDateTime,
    ) -> DrawImageRateLimitGrowFuture<'a> {
        Box::pin(async move {
            let mut timestamps = match self.store.draw_image_rate_limit_timestamps(user_id).await {
                Ok(timestamps) => recent_draw_rate_limit_timestamps(now, timestamps),
                Err(error) => {
                    tracing::warn!(%error, user_id, "failed to load draw image rate-limit state before increment");
                    Vec::new()
                }
            };
            timestamps.push(now);
            if let Err(error) = self
                .store
                .set_draw_image_rate_limit_timestamps(
                    user_id,
                    timestamps,
                    DRAW_RATE_LIMIT_HISTORY_TTL,
                )
                .await
            {
                tracing::warn!(%error, user_id, "failed to persist draw image rate-limit state");
            }
        })
    }
}

const DRAW_RATE_LIMIT_HISTORY_TTL: Duration = Duration::from_secs(30 * 60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DrawRateLimitThreshold {
    window: Duration,
    max_images: usize,
}

fn draw_rate_limit_thresholds(has_vip: bool) -> &'static [DrawRateLimitThreshold] {
    const FREE: &[DrawRateLimitThreshold] = &[
        DrawRateLimitThreshold {
            window: Duration::from_secs(10 * 60),
            max_images: 5,
        },
        DrawRateLimitThreshold {
            window: Duration::from_secs(15 * 60),
            max_images: 10,
        },
        DrawRateLimitThreshold {
            window: Duration::from_secs(30 * 60),
            max_images: 20,
        },
    ];
    const VIP: &[DrawRateLimitThreshold] = &[
        DrawRateLimitThreshold {
            window: Duration::from_secs(10 * 60),
            max_images: 10,
        },
        DrawRateLimitThreshold {
            window: Duration::from_secs(15 * 60),
            max_images: 20,
        },
        DrawRateLimitThreshold {
            window: Duration::from_secs(30 * 60),
            max_images: 40,
        },
    ];
    if has_vip { VIP } else { FREE }
}

fn recent_draw_rate_limit_timestamps(
    now: OffsetDateTime,
    timestamps: Vec<OffsetDateTime>,
) -> Vec<OffsetDateTime> {
    let cutoff = now - time::Duration::minutes(30);
    timestamps
        .into_iter()
        .filter(|timestamp| *timestamp >= cutoff)
        .collect()
}

fn draw_rate_limit_remaining(
    now: OffsetDateTime,
    timestamps: &[OffsetDateTime],
    thresholds: &[DrawRateLimitThreshold],
) -> Option<Duration> {
    if !thresholds
        .iter()
        .any(|threshold| threshold.max_images > 0 && timestamps.len() >= threshold.max_images)
    {
        return None;
    }
    for threshold in thresholds {
        if threshold.max_images == 0 || timestamps.len() < threshold.max_images {
            continue;
        }
        let cutoff = now - time_duration_from_std(threshold.window);
        let mut in_window = timestamps
            .iter()
            .copied()
            .filter(|timestamp| *timestamp > cutoff)
            .collect::<Vec<_>>();
        if in_window.len() < threshold.max_images {
            continue;
        }
        in_window.sort_unstable_by(|left, right| right.cmp(left));
        let expires_at =
            in_window[threshold.max_images - 1] + time_duration_from_std(threshold.window);
        let remaining = expires_at - now;
        if remaining.is_positive() {
            return Some(Duration::from_secs(
                u64::try_from(remaining.whole_seconds()).unwrap_or(u64::MAX),
            ));
        }
    }
    None
}

fn time_duration_from_std(duration: Duration) -> time::Duration {
    time::Duration::seconds(duration.as_secs().min(i64::MAX as u64) as i64)
}

fn draw_rate_limit_response_text(limit_msg: &str, has_vip: bool) -> String {
    if has_vip {
        return limit_msg.trim().to_owned();
    }
    format!(
        "{}\n\nПриобретите VIP-подписку c помощью /vip и получите:\n⚡ Приоритетное выполнение запросов вне очереди\n🔄 Вдвое увеличенные лимиты на рисование\n🖼️ Изображения лучшего качества с меньшей цензурой\n🎵 Генерацию песен по команде /song\n🚀 Доступ к новым функциям и фичам (в разработке)",
        limit_msg.trim()
    )
}

fn format_draw_rate_limit_duration(duration: Duration) -> String {
    if duration.is_zero() {
        return "меньше минуты".to_owned();
    }
    let total = duration.as_secs();
    let hours = total / 3600;
    let minutes = (total / 60) % 60;
    let seconds = total % 60;
    let mut parts = Vec::new();
    if hours > 0 {
        let unit = if hours == 1 {
            "час"
        } else if hours < 5 {
            "часа"
        } else {
            "часов"
        };
        parts.push(format!("{hours} {unit}"));
    }
    if minutes > 0 {
        parts.push(format!("{minutes} мин."));
    }
    if seconds > 0 && hours == 0 {
        parts.push(format!("{seconds} сек."));
    }
    if parts.is_empty() {
        "меньше минуты".to_owned()
    } else {
        parts.join(" ")
    }
}

fn normalize_draw_prompt_whitelist(
    phrases: impl IntoIterator<Item = impl AsRef<str>>,
) -> Vec<String> {
    phrases
        .into_iter()
        .map(|phrase| phrase.as_ref().to_lowercase())
        .collect()
}

fn is_draw_prompt_whitelisted(prompt: &str, whitelist: &[String]) -> bool {
    if whitelist.is_empty() {
        return false;
    }
    let prompt = prompt.to_lowercase();
    whitelist.iter().any(|phrase| prompt.contains(phrase))
}

/// Rejections always feed back to the model (`no_reply: false`): the tool
/// result carries a model-facing refusal message plus a machine-readable
/// reason code, and the model composes the user-visible reply.
fn not_scheduled_song(rejection: SongScheduleRejection) -> SongScheduleResult {
    SongScheduleResult {
        status: "not_scheduled".to_owned(),
        message: rejection.model_facing_message().to_owned(),
        no_reply: false,
        rejection: Some(rejection),
        ..SongScheduleResult::default()
    }
}

fn song_reference_unique_id(explicit: &str, meta: &ChatMessageMeta) -> String {
    let explicit = explicit.trim();
    if !explicit.is_empty() {
        return explicit.to_owned();
    }

    meta.attachments
        .iter()
        .find_map(|attachment| {
            if attachment.source.trim() != "quoted" {
                return None;
            }
            let kind = attachment.kind.trim();
            if kind != "audio" && kind != "voice" {
                return None;
            }
            let file_unique_id = attachment.file_unique_id.trim();
            (!file_unique_id.is_empty()).then(|| file_unique_id.to_owned())
        })
        .unwrap_or_default()
}

fn image_edit_file_unique_ids(attachments: &[ChatAttachment]) -> Vec<String> {
    let mut file_unique_ids = Vec::new();
    for attachment in attachments {
        if attachment.kind.trim() != "image" {
            continue;
        }
        let file_unique_id = attachment.file_unique_id.trim();
        push_unique_trimmed(&mut file_unique_ids, file_unique_id);
    }
    file_unique_ids
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ImageEditFileIdSelection {
    photo_file_id: String,
    url_file_ids: Vec<String>,
}

fn image_edit_file_id_selection(
    target_file_unique_ids: &[String],
    media_group_file_unique_ids: &[String],
    rows: &[openplotva_storage::TelegramFileRecord],
) -> ImageEditFileIdSelection {
    let target_file_ids = latest_file_ids_in_attachment_order(target_file_unique_ids, rows);
    let media_group_file_ids =
        latest_file_ids_in_attachment_order(media_group_file_unique_ids, rows);
    ImageEditFileIdSelection {
        photo_file_id: target_file_ids.first().cloned().unwrap_or_default(),
        url_file_ids: if media_group_file_ids.is_empty() {
            target_file_ids
        } else {
            media_group_file_ids
        },
    }
}

fn merge_unique_strings(first: &[String], second: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for value in first.iter().chain(second) {
        push_unique_trimmed(&mut out, value);
    }
    out
}

fn push_unique_trimmed(out: &mut Vec<String>, value: &str) {
    let value = value.trim();
    if value.is_empty() || out.iter().any(|existing| existing == value) {
        return;
    }
    out.push(value.to_owned());
}

fn latest_file_ids_in_attachment_order(
    file_unique_ids: &[String],
    rows: &[openplotva_storage::TelegramFileRecord],
) -> Vec<String> {
    let mut latest_file_ids = Vec::new();
    for file_unique_id in file_unique_ids {
        let Some(row) = rows
            .iter()
            .find(|row| row.file_unique_id.trim() == file_unique_id)
        else {
            continue;
        };
        let latest_file_id = row.latest_file_id.trim();
        if !latest_file_id.is_empty() {
            latest_file_ids.push(latest_file_id.to_owned());
        }
    }
    latest_file_ids
}

fn telegram_file_url(bot_key: &str, file_path: &str) -> String {
    format!(
        "https://api.telegram.org/file/bot{}/{}",
        bot_key.trim(),
        file_path.trim()
    )
}

fn cleanup_old_media_groups(groups: &mut HashMap<String, MediaGroupEntry>, now: OffsetDateTime) {
    let cutoff = now - time::Duration::minutes(5);
    groups.retain(|_, entry| entry.created_at >= cutoff);
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn draw_image_job_meta(request: &DrawImageScheduleRequest) -> serde_json::Value {
    let meta = ChatMessageMeta {
        message_type: "text".to_owned(),
        sender_type: SENDER_TYPE_USER.to_owned(),
        sender_id: request.user_id,
        sender_name: request.user_full_name.trim().to_owned(),
        ..ChatMessageMeta::default()
    };
    serde_json::to_value(meta).unwrap_or_else(|_| json!({}))
}

fn format_go_json_time(value: OffsetDateTime) -> String {
    value
        .to_offset(UtcOffset::UTC)
        .format(&Rfc3339)
        .unwrap_or_default()
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[derive(Clone)]
pub struct AppDialogToolbox<RatesFetcherT, RatesDispatcherT, TranslatorT> {
    rates_fetcher: Option<Arc<RatesFetcherT>>,
    _rates_dispatcher: Option<Arc<RatesDispatcherT>>,
    translator: Option<Arc<TranslatorT>>,
    queue_status: Option<Arc<dyn QueueStatusProvider>>,
    drawing_canceller: Option<Arc<dyn DrawingJobCanceller>>,
    web_searcher: Option<Arc<dyn WebSearchProvider>>,
    url_crawler: Option<Arc<dyn UrlCrawler>>,
    youtube_summarizer: Option<Arc<dyn YouTubeSummarizer>>,
    image_scheduler: Option<Arc<dyn ImageScheduler>>,
    song_scheduler: Option<Arc<dyn SongScheduler>>,
    vision_describer: Option<Arc<dyn VisionDescriber>>,
    history_summarizer: Option<Arc<dyn ChatHistorySummarizer>>,
    history_searcher: Option<Arc<dyn HistorySearcher>>,
}

impl<RatesFetcherT, RatesDispatcherT, TranslatorT>
    AppDialogToolbox<RatesFetcherT, RatesDispatcherT, TranslatorT>
{
    #[must_use]
    pub fn new(
        rates_fetcher: Option<Arc<RatesFetcherT>>,
        rates_dispatcher: Option<Arc<RatesDispatcherT>>,
        translator: Option<Arc<TranslatorT>>,
    ) -> Self {
        Self {
            rates_fetcher,
            _rates_dispatcher: rates_dispatcher,
            translator,
            queue_status: None,
            drawing_canceller: None,
            web_searcher: None,
            url_crawler: None,
            youtube_summarizer: None,
            image_scheduler: None,
            song_scheduler: None,
            vision_describer: None,
            history_summarizer: None,
            history_searcher: None,
        }
    }

    #[must_use]
    pub fn with_queue_status_provider(
        mut self,
        queue_status: Arc<dyn QueueStatusProvider>,
    ) -> Self {
        self.queue_status = Some(queue_status);
        self
    }

    #[must_use]
    pub fn with_drawing_canceller(
        mut self,
        drawing_canceller: Arc<dyn DrawingJobCanceller>,
    ) -> Self {
        self.drawing_canceller = Some(drawing_canceller);
        self
    }

    #[must_use]
    pub fn with_web_searcher(mut self, web_searcher: Arc<dyn WebSearchProvider>) -> Self {
        self.web_searcher = Some(web_searcher);
        self
    }

    #[must_use]
    pub fn with_url_crawler(mut self, url_crawler: Arc<dyn UrlCrawler>) -> Self {
        self.url_crawler = Some(url_crawler);
        self
    }

    #[must_use]
    pub fn with_youtube_summarizer(
        mut self,
        youtube_summarizer: Arc<dyn YouTubeSummarizer>,
    ) -> Self {
        self.youtube_summarizer = Some(youtube_summarizer);
        self
    }

    #[must_use]
    pub fn with_image_scheduler(mut self, image_scheduler: Arc<dyn ImageScheduler>) -> Self {
        self.image_scheduler = Some(image_scheduler);
        self
    }

    #[must_use]
    pub fn with_song_scheduler(mut self, song_scheduler: Arc<dyn SongScheduler>) -> Self {
        self.song_scheduler = Some(song_scheduler);
        self
    }

    #[must_use]
    pub fn with_vision_describer(mut self, vision_describer: Arc<dyn VisionDescriber>) -> Self {
        self.vision_describer = Some(vision_describer);
        self
    }

    #[must_use]
    pub fn with_history_summarizer(
        mut self,
        history_summarizer: Arc<dyn ChatHistorySummarizer>,
    ) -> Self {
        self.history_summarizer = Some(history_summarizer);
        self
    }

    #[must_use]
    pub fn with_history_searcher(mut self, history_searcher: Arc<dyn HistorySearcher>) -> Self {
        self.history_searcher = Some(history_searcher);
        self
    }
}

impl<RatesFetcherT, RatesDispatcherT, TranslatorT> DialogToolbox
    for AppDialogToolbox<RatesFetcherT, RatesDispatcherT, TranslatorT>
where
    RatesFetcherT: RatesFetcher + Send + Sync + 'static,
    RatesDispatcherT: RatesSideEffectDispatcher + Send + Sync + 'static,
    TranslatorT: TextTranslator + Send + Sync + 'static,
{
    fn currency_rates<'a>(&'a self, req: RatesRequest) -> ToolboxFuture<'a> {
        Box::pin(async move {
            Ok(currency_rates_tool(self.rates_fetcher.as_deref(), &req.context, &req.pairs).await)
        })
    }

    fn translate_text<'a>(&'a self, text: String, target_lang: String) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let Some(translator) = self.translator.as_deref() else {
                return Ok(ToolResult::failed(
                    "translate_text_unavailable",
                    "translation service unavailable",
                ));
            };
            let text = sanitize_tool_text(&text);
            let target_lang = sanitize_tool_text(&target_lang);
            match translate_text_tool(translator, &text, &target_lang).await {
                Ok(Some(result)) => Ok(dialog_translation_tool_result(&result)),
                Ok(None) => Ok(ToolResult::failed(
                    "translate_text_failed",
                    "translation text is empty",
                )),
                Err(error) => Ok(ToolResult::failed(
                    "translate_text_failed",
                    error.to_string(),
                )),
            }
        })
    }

    fn draw_image<'a>(&'a self, req: DrawRequest) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let Some(scheduler) = self.image_scheduler.as_deref() else {
                return Ok(ToolResult::failed(
                    "draw_image_unavailable",
                    "draw service unavailable",
                ));
            };
            let prompt = sanitize_tool_text(&req.prompt);
            let request = DrawImageScheduleRequest {
                chat_id: req.context.chat_id,
                thread_id: req.context.thread_id,
                message_id: req.context.message_id,
                user_id: req.context.user_id,
                user_full_name: req.context.user_full_name,
                prompt: prompt.clone(),
                message_text: req.context.message_text,
                attachments: req.context.message_meta.attachments,
                edit_media_group_id: String::new(),
                negative_prompt: sanitize_tool_text(&req.negative_prompt),
                aspect_ratio: sanitize_tool_text(&req.aspect_ratio),
                seed: sanitize_tool_text(&req.seed),
                original_prompt: prompt,
            };
            match scheduler.schedule_image(request).await {
                Ok(result) => Ok(dialog_draw_image_tool_result(&result)),
                Err(error) => Ok(ToolResult::failed("draw_image_failed", error.to_string())),
            }
        })
    }

    fn generate_song<'a>(&'a self, req: SongRequest) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let Some(scheduler) = self.song_scheduler.as_deref() else {
                return Ok(ToolResult::failed(
                    "generate_song_unavailable",
                    "song service unavailable",
                ));
            };
            let request = SongScheduleRequest {
                chat_id: req.context.chat_id,
                thread_id: req.context.thread_id,
                message_id: req.context.message_id,
                user_id: req.context.user_id,
                user_full_name: req.context.user_full_name,
                topic: sanitize_tool_text(&req.topic),
                message_text: req.context.message_text,
                message_meta: req.context.message_meta,
                reference_file_id: String::new(),
                reference_file_unique_id: String::new(),
            };
            match scheduler.schedule_song(request).await {
                Ok(result) => Ok(dialog_generate_song_tool_result(&result)),
                Err(error) => Ok(ToolResult::failed(
                    "generate_song_failed",
                    error.to_string(),
                )),
            }
        })
    }

    fn vision_image<'a>(&'a self, req: VisionRequest) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let Some(describer) = self.vision_describer.as_deref() else {
                return Ok(ToolResult::failed(
                    "vision_image_unavailable",
                    "vision service unavailable",
                ));
            };
            let request = VisionDescribeRequest {
                file_id: req.file_id,
                chat_id: req.context.chat_id,
                message_id: req.context.message_id,
                thread_id: req.context.thread_id,
                message_meta: req.context.message_meta,
            };
            match describer.describe_image(request).await {
                Ok(result) => Ok(dialog_vision_image_tool_result(&result)),
                Err(error) => Ok(ToolResult::failed("vision_image_failed", error.to_string())),
            }
        })
    }

    fn web_search<'a>(&'a self, query: String) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let query = sanitize_tool_text(&query);
            let Some(searcher) = self.web_searcher.as_deref() else {
                return Ok(ToolResult::failed(
                    "web_search_unavailable",
                    "search service unavailable",
                ));
            };
            match searcher.search(&query).await {
                Ok(results) => Ok(dialog_web_search_tool_result(&query, &results)),
                Err(error) => Ok(ToolResult::failed("web_search_failed", error.to_string())),
            }
        })
    }

    fn crawl_url<'a>(&'a self, url: String) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let Some(crawler) = self.url_crawler.as_deref() else {
                return Ok(ToolResult::failed(
                    "crawl_url_unavailable",
                    "crawl service unavailable",
                ));
            };
            let url = sanitize_tool_text(&url);
            match crawler.crawl(&url).await {
                Ok(content) => Ok(dialog_crawl_url_tool_result(&url, &content)),
                Err(error) => Ok(ToolResult::failed("crawl_url_failed", error.to_string())),
            }
        })
    }

    fn youtube_summary<'a>(&'a self, video: String) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let Some(summarizer) = self.youtube_summarizer.as_deref() else {
                return Ok(ToolResult::failed(
                    "youtube_summary_unavailable",
                    "youtube summarizer unavailable",
                ));
            };
            let video = sanitize_tool_text(&video);
            match summarizer.summarize(&video).await {
                Ok(summary) => Ok(dialog_youtube_summary_tool_result(&video, &summary)),
                Err(error) => Ok(ToolResult::failed(
                    "youtube_summary_failed",
                    error.to_string(),
                )),
            }
        })
    }

    fn chat_history_summary<'a>(&'a self, req: HistorySummaryRequest) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let Some(summarizer) = self.history_summarizer.as_deref() else {
                return Ok(ToolResult::failed(
                    "chat_history_summary_unavailable",
                    "history summary service unavailable",
                ));
            };
            let request = HistorySummaryRequest {
                window: sanitize_tool_text(&req.window),
                since: sanitize_tool_text(&req.since),
                scope: sanitize_tool_text(&req.scope),
                ..req
            };
            match summarizer.chat_history_summary(request).await {
                Ok(result) => Ok(dialog_chat_history_summary_tool_result(&result)),
                Err(error) => Ok(ToolResult::failed(
                    "chat_history_summary_failed",
                    error.to_string(),
                )),
            }
        })
    }

    fn history_search<'a>(&'a self, req: HistorySearchRequest) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let Some(searcher) = self.history_searcher.as_deref() else {
                return Ok(ToolResult::failed(
                    "history_search_unavailable",
                    "history search is not configured",
                ));
            };
            let query = sanitize_tool_text(&req.query);
            match searcher
                .search(req.context.chat_id, req.context.thread_id, query.clone())
                .await
            {
                Ok(results) => Ok(dialog_history_search_tool_result(&query, &results)),
                Err(error) => Ok(ToolResult::failed(
                    "history_search_failed",
                    error.to_string(),
                )),
            }
        })
    }

    fn queue_status<'a>(&'a self, user_id: i64) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let Some(provider) = self.queue_status.as_deref() else {
                return Ok(ToolResult::failed(
                    "queue_status_unavailable",
                    "queue status unavailable",
                ));
            };
            match provider.queue_status(user_id).await {
                Ok(result) => Ok(dialog_queue_status_tool_result(&result)),
                Err(error) => Ok(ToolResult::failed("queue_status_failed", error.to_string())),
            }
        })
    }

    fn cancel_drawing<'a>(&'a self, user_id: i64, chat_id: i64) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let Some(canceller) = self.drawing_canceller.as_deref() else {
                return Ok(ToolResult::failed(
                    "cancel_drawing_unavailable",
                    "cancel service unavailable",
                ));
            };
            match canceller.cancel_user_image_jobs(user_id, chat_id).await {
                Ok(result) => Ok(dialog_cancel_drawing_tool_result(&result)),
                Err(error) => Ok(ToolResult::failed(
                    "cancel_drawing_failed",
                    error.to_string(),
                )),
            }
        })
    }
}

fn dialog_draw_image_tool_result(result: &DrawImageScheduleResult) -> ToolResult {
    if result.status.trim() == "redraw_without_context" {
        return ToolResult {
            status: TOOL_RESULT_STATUS_NOOP.to_owned(),
            message: "Чтобы перерисовать изображение, опишите, какие изменения вы хотите внести."
                .to_owned(),
            no_reply: false,
            side_effect: Some(tool_side_effect("image_generation_job", "needs_context")),
            data: Some(json!({
                "status": "redraw_without_context",
            })),
            error: None,
        };
    }
    scheduled_generation_tool_result(GenerationToolResultArgs {
        scheduler_status: &result.status,
        scheduler_message: &result.message,
        scheduler_no_reply: result.no_reply,
        reason_code: result
            .rejection
            .map(DrawImageScheduleRejection::reason_code),
        side_effect_kind: "image_generation_job",
        ticket_job_id: result.job_id,
        not_scheduled_fallback: IMAGE_GENERATION_NOT_SCHEDULED_MESSAGE,
        not_scheduled_error_code: "draw_image_not_scheduled",
    })
}

fn dialog_generate_song_tool_result(result: &SongScheduleResult) -> ToolResult {
    scheduled_generation_tool_result(GenerationToolResultArgs {
        scheduler_status: &result.status,
        scheduler_message: &result.message,
        scheduler_no_reply: result.no_reply,
        reason_code: result.rejection.map(SongScheduleRejection::reason_code),
        side_effect_kind: "music_generation_job",
        ticket_job_id: result.job_id,
        not_scheduled_fallback: SONG_GENERATION_NOT_SCHEDULED_MESSAGE,
        not_scheduled_error_code: "generate_song_not_scheduled",
    })
}

struct GenerationToolResultArgs<'a> {
    scheduler_status: &'a str,
    scheduler_message: &'a str,
    scheduler_no_reply: bool,
    reason_code: Option<&'static str>,
    side_effect_kind: &'a str,
    ticket_job_id: Option<i64>,
    not_scheduled_fallback: &'a str,
    not_scheduled_error_code: &'a str,
}

/// Map a scheduler result onto the tool protocol. Queued results keep an EMPTY
/// message (silent-but-guaranteed: the artifact is the reply) and carry the
/// ticket id on the side effect; rejections stay `no_reply: false` with a
/// model-facing message plus a `data["reason"]` code so the model composes the
/// user-visible refusal.
fn scheduled_generation_tool_result(args: GenerationToolResultArgs<'_>) -> ToolResult {
    let trimmed_status = args.scheduler_status.trim();
    let mut status = TOOL_RESULT_STATUS_QUEUED;
    let mut message = args.scheduler_message.trim().to_owned();
    let no_reply = args.scheduler_no_reply || is_internal_not_scheduled_instruction(&message);
    let mut side_effect_state = "queued";
    let mut error = None;
    if no_reply {
        status = TOOL_RESULT_STATUS_NOOP;
        message.clear();
        side_effect_state = "noop";
    } else if trimmed_status.eq_ignore_ascii_case("not_scheduled") {
        status = TOOL_RESULT_STATUS_FAILED;
        message = user_facing_not_scheduled_message(&message, args.not_scheduled_fallback);
        side_effect_state = "failed";
        error = Some(ToolError {
            code: args.not_scheduled_error_code.to_owned(),
            reason: message.clone(),
            retryable: false,
        });
    }
    let mut side_effect = tool_side_effect(args.side_effect_kind, side_effect_state);
    if status == TOOL_RESULT_STATUS_QUEUED
        && let Some(ticket_job_id) = args.ticket_job_id
    {
        side_effect.ticket_id = ticket_job_id.to_string();
    }
    let mut data = serde_json::Map::new();
    data.insert("status".to_owned(), json!(trimmed_status));
    if let Some(reason) = args.reason_code {
        data.insert("reason".to_owned(), json!(reason));
    }
    ToolResult {
        status: status.to_owned(),
        message,
        no_reply,
        side_effect: Some(side_effect),
        data: Some(serde_json::Value::Object(data)),
        error,
    }
}

fn dialog_vision_image_tool_result(result: &VisionDescribeResult) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_OK.to_owned(),
        message: result.caption.trim().to_owned(),
        no_reply: false,
        side_effect: None,
        data: Some(json!({
            "source": result.source,
            "file_unique_id": result.file_unique_id,
            "history_updated": result.history_updated,
        })),
        error: None,
    }
}

fn dialog_chat_history_summary_tool_result(result: &ChatHistorySummaryResult) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_OK.to_owned(),
        message: result.summary_html.clone(),
        no_reply: false,
        side_effect: None,
        data: Some(json!({
            "summary_id": result.summary_id,
            "chat_id": result.chat_id,
            "thread_id": result.thread_id,
            "scope": result.scope,
            "range_start_at": result.range_start_at,
            "range_end_at": result.range_end_at,
            "raw_message_count": result.raw_message_count,
            "covered_message_count": result.covered_message_count,
            "reused_summary_count": result.reused_summary_count,
            "source_summary_ids": result.source_summary_ids,
            "summary_html": result.summary_html,
            "summary_json": result.summary_json,
            "model": result.model,
        })),
        error: None,
    }
}

fn dialog_history_search_tool_result(query: &str, results: &str) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_OK.to_owned(),
        message: "history search results ready".to_owned(),
        no_reply: false,
        side_effect: None,
        data: Some(json!({
            "query": query,
            "results": results,
        })),
        error: None,
    }
}

fn dialog_web_search_tool_result(query: &str, results: &str) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_OK.to_owned(),
        message: "search results ready".to_owned(),
        no_reply: false,
        side_effect: None,
        data: Some(json!({
            "query": query,
            "results": results,
        })),
        error: None,
    }
}

fn dialog_crawl_url_tool_result(url: &str, content: &str) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_OK.to_owned(),
        message: "page content fetched".to_owned(),
        no_reply: false,
        side_effect: None,
        data: Some(json!({
            "url": url,
            "content": content,
        })),
        error: None,
    }
}

fn dialog_youtube_summary_tool_result(video: &str, result: &YouTubeSummaryResult) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_OK.to_owned(),
        message: "youtube summary ready".to_owned(),
        no_reply: false,
        side_effect: None,
        data: Some(json!({
            "video": video,
            "summary": result.summary,
            "transcript": result.transcript,
        })),
        error: None,
    }
}

fn dialog_queue_status_tool_result(result: &QueueStatusResult) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_OK.to_owned(),
        message: format_dialog_queue_status(result),
        no_reply: false,
        side_effect: None,
        data: Some(json!({
            "regular_queue_depth": result.regular_queue_depth,
            "vip_queue_depth": result.vip_queue_depth,
            "active_jobs_count": result.active_jobs_count,
            "user_active_jobs": result.user_active_jobs,
            "user_pending_jobs": result.user_pending_jobs,
            "estimated_wait": result.estimated_wait,
        })),
        error: None,
    }
}

fn dialog_cancel_drawing_tool_result(result: &CancelDrawingResult) -> ToolResult {
    let status = if result.cancelled_count == 0 {
        TOOL_RESULT_STATUS_NOOP
    } else {
        TOOL_RESULT_STATUS_EXECUTED
    };
    ToolResult {
        status: status.to_owned(),
        message: format_dialog_cancel_result(result),
        no_reply: false,
        side_effect: Some(tool_side_effect(
            "image_generation_job",
            if result.cancelled_count == 0 {
                "noop"
            } else {
                "cancelled"
            },
        )),
        data: Some(json!({
            "cancelled_count": result.cancelled_count,
            "cancelled_ids": result.cancelled_ids,
        })),
        error: None,
    }
}

fn format_dialog_queue_status(result: &QueueStatusResult) -> String {
    let total_queue = result.regular_queue_depth + result.vip_queue_depth;
    format!(
        "Image generation queue status:\n\
         - Total jobs waiting in queue: {}\n\
         - Jobs currently being processed: {}\n\
         - This user's active jobs (currently generating): {}\n\
         - This user's pending jobs (waiting in queue): {}\n\
         - Estimated wait time for a new request: {} minutes",
        total_queue,
        result.active_jobs_count,
        result.user_active_jobs,
        result.user_pending_jobs,
        go_duration_minutes(&result.estimated_wait),
    )
}

fn format_dialog_cancel_result(result: &CancelDrawingResult) -> String {
    if result.cancelled_count == 0 {
        return "Cancellation result: No active or pending image generation jobs were found for this user in this chat. Nothing was cancelled.".to_owned();
    }
    format!(
        "Cancellation result: Successfully cancelled {} image generation job(s) for this user.",
        result.cancelled_count
    )
}

fn tool_side_effect(kind: &str, state: &str) -> ToolSideEffect {
    ToolSideEffect {
        kind: kind.trim().to_owned(),
        state: state.trim().to_owned(),
        ..ToolSideEffect::default()
    }
}

fn go_duration_minutes(raw: &str) -> i64 {
    let mut total_seconds = 0_i64;
    let mut number = String::new();
    for ch in raw.trim().chars() {
        if ch.is_ascii_digit() {
            number.push(ch);
            continue;
        }
        if number.is_empty() {
            continue;
        }
        let value = number.parse::<i64>().unwrap_or(0);
        number.clear();
        match ch {
            'h' => total_seconds = total_seconds.saturating_add(value.saturating_mul(3600)),
            'm' => total_seconds = total_seconds.saturating_add(value.saturating_mul(60)),
            's' => total_seconds = total_seconds.saturating_add(value),
            _ => {}
        }
    }
    total_seconds / 60
}

fn dialog_translation_tool_result(result: &TranslationResult) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_OK.to_owned(),
        message: format_dialog_translation(result),
        no_reply: false,
        side_effect: None,
        data: Some(json!({
            "original": result.original,
            "translated": result.translated,
            "source_lang": result.source_lang,
            "target_lang": result.target_lang,
        })),
        error: None,
    }
}

fn format_dialog_translation(result: &TranslationResult) -> String {
    format!(
        "source_lang={} target_lang={} translated={}",
        result.source_lang.trim(),
        result.target_lang.trim(),
        result.translated.trim(),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt,
        sync::{Arc, Mutex, MutexGuard},
    };

    use openplotva_core::ChatMessageMeta;
    use openplotva_dialog::{
        TOOL_RESULT_STATUS_EXECUTED, TOOL_RESULT_STATUS_FAILED, TOOL_RESULT_STATUS_NOOP,
        TOOL_RESULT_STATUS_QUEUED, ToolContext, ToolError, ToolSideEffect, ToolboxError,
    };
    use openplotva_taskman::{
        DEFAULT_PRIORITY, IMAGE_REGULAR_QUEUE_NAME, IMAGE_VIP_QUEUE_NAME, JobPayload, JobType,
        LOWEST_PRIORITY, StatelessJobItem, TelegramData,
    };
    use serde_json::json;

    use super::*;
    use crate::{
        rates::{
            RateQuote, RatesDispatchFuture, RatesFetchFuture, RatesFetchProblem, RatesSelection,
            RatesSideEffectDispatchResult, RatesSnapshot, RatesToolInvocationContext,
        },
        translate::TranslateProviderFuture,
    };

    #[derive(Clone, Debug)]
    struct TestError(String);

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl Error for TestError {}

    #[derive(Clone, Debug, Default)]
    struct RatesFetcherStub;

    impl RatesFetcher for RatesFetcherStub {
        type Error = TestError;

        fn fetch_rates<'a>(
            &'a self,
            selection: RatesSelection,
        ) -> RatesFetchFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut snapshot = RatesSnapshot::default();
                for id in selection.ids {
                    let (label, value, precision, unit) = match id.as_str() {
                        "usd_rub" => ("💵 USD/RUB", 90.0, 2, "₽"),
                        "eur_rub" => ("💶 EUR/RUB", 100.0, 2, "₽"),
                        "eur_usd" => ("💶/💵 EUR/USD", 1.1, 4, ""),
                        "btc_usd" => ("₿ BTC/USD", 100_000.0, 0, "$"),
                        "brent" => ("🛢 Brent", 76.5, 2, "$"),
                        other => {
                            snapshot.errors.push(RatesFetchProblem {
                                label: other.to_owned(),
                                message: "unexpected pair".to_owned(),
                            });
                            continue;
                        }
                    };
                    snapshot.rows.push(RateQuote {
                        id,
                        label: label.to_owned(),
                        value,
                        precision,
                        unit: unit.to_owned(),
                        delta: None,
                        source: "test".to_owned(),
                        timestamp: None,
                        stale: false,
                    });
                }
                Ok(snapshot)
            })
        }
    }

    #[derive(Debug, Default)]
    struct RatesDispatcherStub {
        calls: Mutex<Vec<(RatesToolInvocationContext, String)>>,
    }

    impl RatesDispatcherStub {
        fn calls(&self) -> Vec<(RatesToolInvocationContext, String)> {
            self.state().clone()
        }

        fn state(&self) -> MutexGuard<'_, Vec<(RatesToolInvocationContext, String)>> {
            match self.calls.lock() {
                Ok(calls) => calls,
                Err(err) => panic!("rates dispatcher calls poisoned: {err}"),
            }
        }
    }

    impl RatesSideEffectDispatcher for RatesDispatcherStub {
        type Error = TestError;

        fn dispatch_rates<'a>(
            &'a self,
            meta: RatesToolInvocationContext,
            text: String,
        ) -> RatesDispatchFuture<'a, Self::Error> {
            let result = {
                self.state().push((meta, text));
                Ok(RatesSideEffectDispatchResult {
                    kind: crate::rates::RATES_MESSAGE_KIND.to_owned(),
                    ticket_id: "ticket-1".to_owned(),
                    eta: String::new(),
                    state: "queued".to_owned(),
                })
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Clone, Debug)]
    struct TranslatorStub {
        result: Result<String, TestError>,
    }

    impl TextTranslator for TranslatorStub {
        type Error = TestError;

        fn translate_text<'a>(
            &'a self,
            text: &'a str,
            target_lang: &'a str,
        ) -> TranslateProviderFuture<'a, Self::Error> {
            let result = self
                .result
                .clone()
                .map(|translated| format!("{translated}:{text}:{target_lang}"));
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct QueueStatusStub {
        result: Result<QueueStatusResult, String>,
        calls: Mutex<Vec<i64>>,
    }

    impl QueueStatusStub {
        fn successful(result: QueueStatusResult) -> Self {
            Self {
                result: Ok(result),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn failed(message: impl Into<String>) -> Self {
            Self {
                result: Err(message.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<i64> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("queue status calls poisoned: {err}"),
            }
        }
    }

    impl QueueStatusProvider for QueueStatusStub {
        fn queue_status<'a>(&'a self, user_id: i64) -> QueueStatusFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push(user_id),
                    Err(err) => panic!("queue status calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(|message| Box::new(TestError(message)) as ToolboxError)
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct DrawingCancellerStub {
        result: Result<CancelDrawingResult, String>,
        calls: Mutex<Vec<(i64, i64)>>,
    }

    impl DrawingCancellerStub {
        fn successful(result: CancelDrawingResult) -> Self {
            Self {
                result: Ok(result),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn failed(message: impl Into<String>) -> Self {
            Self {
                result: Err(message.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<(i64, i64)> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("drawing canceller calls poisoned: {err}"),
            }
        }
    }

    impl DrawingJobCanceller for DrawingCancellerStub {
        fn cancel_user_image_jobs<'a>(
            &'a self,
            user_id: i64,
            chat_id: i64,
        ) -> CancelDrawingFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push((user_id, chat_id)),
                    Err(err) => panic!("drawing canceller calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(|message| Box::new(TestError(message)) as ToolboxError)
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct WebSearchStub {
        result: Result<String, String>,
        calls: Mutex<Vec<String>>,
    }

    impl WebSearchStub {
        fn successful(result: impl Into<String>) -> Self {
            Self {
                result: Ok(result.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn failed(message: impl Into<String>) -> Self {
            Self {
                result: Err(message.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<String> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("web search calls poisoned: {err}"),
            }
        }
    }

    impl WebSearchProvider for WebSearchStub {
        fn search<'a>(&'a self, query: &'a str) -> WebSearchFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push(query.to_owned()),
                    Err(err) => panic!("web search calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(|message| Box::new(TestError(message)) as ToolboxError)
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct HistorySearchStub {
        result: Result<String, String>,
        calls: Mutex<Vec<(i64, Option<i32>, String)>>,
    }

    impl HistorySearchStub {
        fn successful(result: impl Into<String>) -> Self {
            Self {
                result: Ok(result.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<(i64, Option<i32>, String)> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("history search calls poisoned: {err}"),
            }
        }
    }

    impl HistorySearcher for HistorySearchStub {
        fn search<'a>(
            &'a self,
            chat_id: i64,
            thread_id: Option<i32>,
            query: String,
        ) -> crate::agent_runtime::ContextSearchFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push((chat_id, thread_id, query)),
                    Err(err) => panic!("history search calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(openplotva_agent::AgentError::ToolDispatch)
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct UrlCrawlerStub {
        result: Result<String, String>,
        calls: Mutex<Vec<String>>,
    }

    impl UrlCrawlerStub {
        fn successful(result: impl Into<String>) -> Self {
            Self {
                result: Ok(result.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn failed(message: impl Into<String>) -> Self {
            Self {
                result: Err(message.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<String> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("url crawler calls poisoned: {err}"),
            }
        }
    }

    impl UrlCrawler for UrlCrawlerStub {
        fn crawl<'a>(&'a self, url: &'a str) -> CrawlUrlFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push(url.to_owned()),
                    Err(err) => panic!("url crawler calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(|message| Box::new(TestError(message)) as ToolboxError)
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct YouTubeSummarizerStub {
        result: Result<YouTubeSummaryResult, String>,
        calls: Mutex<Vec<String>>,
    }

    impl YouTubeSummarizerStub {
        fn successful(result: YouTubeSummaryResult) -> Self {
            Self {
                result: Ok(result),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn failed(message: impl Into<String>) -> Self {
            Self {
                result: Err(message.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<String> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("youtube summarizer calls poisoned: {err}"),
            }
        }
    }

    impl YouTubeSummarizer for YouTubeSummarizerStub {
        fn summarize<'a>(&'a self, video: &'a str) -> YouTubeSummaryFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push(video.to_owned()),
                    Err(err) => panic!("youtube summarizer calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(|message| Box::new(TestError(message)) as ToolboxError)
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct ImageSchedulerStub {
        result: Result<DrawImageScheduleResult, String>,
        calls: Mutex<Vec<DrawImageScheduleRequest>>,
    }

    impl ImageSchedulerStub {
        fn successful(result: DrawImageScheduleResult) -> Self {
            Self {
                result: Ok(result),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn failed(message: impl Into<String>) -> Self {
            Self {
                result: Err(message.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<DrawImageScheduleRequest> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("image scheduler calls poisoned: {err}"),
            }
        }
    }

    impl ImageScheduler for ImageSchedulerStub {
        fn schedule_image<'a>(&'a self, request: DrawImageScheduleRequest) -> DrawImageFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push(request),
                    Err(err) => panic!("image scheduler calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(|message| Box::new(TestError(message)) as ToolboxError)
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct DrawImageVipStatusStub {
        is_vip: bool,
        calls: Mutex<Vec<i64>>,
    }

    impl DrawImageVipStatusStub {
        fn new(is_vip: bool) -> Self {
            Self {
                is_vip,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<i64> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("draw image vip status calls poisoned: {err}"),
            }
        }
    }

    impl DrawImageVipStatus for DrawImageVipStatusStub {
        fn is_draw_image_vip<'a>(&'a self, user_id: i64) -> DrawImageVipStatusFuture<'a> {
            match self.calls.lock() {
                Ok(mut calls) => calls.push(user_id),
                Err(err) => panic!("draw image vip status calls poisoned: {err}"),
            }
            Box::pin(async move { self.is_vip })
        }
    }

    #[derive(Debug)]
    struct DrawImagePermissionStub {
        rejection: Option<DrawImageScheduleRejection>,
        calls: Mutex<Vec<i64>>,
    }

    impl DrawImagePermissionStub {
        fn new(rejection: Option<DrawImageScheduleRejection>) -> Self {
            Self {
                rejection,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<i64> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("draw image permission calls poisoned: {err}"),
            }
        }
    }

    impl DrawImagePermission for DrawImagePermissionStub {
        fn draw_image_rejection<'a>(&'a self, chat_id: i64) -> DrawImagePermissionFuture<'a> {
            match self.calls.lock() {
                Ok(mut calls) => calls.push(chat_id),
                Err(err) => panic!("draw image permission calls poisoned: {err}"),
            }
            Box::pin(async move { self.rejection })
        }
    }

    #[derive(Debug)]
    struct DrawImageRateLimitStub {
        report: DrawImageRateLimitReport,
        checks: Mutex<Vec<(String, i64, bool)>>,
        grows: Mutex<Vec<i64>>,
    }

    impl DrawImageRateLimitStub {
        fn new(report: DrawImageRateLimitReport) -> Self {
            Self {
                report,
                checks: Mutex::new(Vec::new()),
                grows: Mutex::new(Vec::new()),
            }
        }

        fn checks(&self) -> Vec<(String, i64, bool)> {
            match self.checks.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("draw image rate-limit checks poisoned: {err}"),
            }
        }

        fn grows(&self) -> Vec<i64> {
            match self.grows.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("draw image rate-limit grows poisoned: {err}"),
            }
        }
    }

    impl DrawImageRateLimit for DrawImageRateLimitStub {
        fn check_draw_image_rate_limit<'a>(
            &'a self,
            prompt: &'a str,
            user_id: i64,
            has_vip: bool,
            _now: OffsetDateTime,
        ) -> DrawImageRateLimitFuture<'a> {
            match self.checks.lock() {
                Ok(mut checks) => checks.push((prompt.to_owned(), user_id, has_vip)),
                Err(err) => panic!("draw image rate-limit checks poisoned: {err}"),
            }
            Box::pin(async move { self.report.clone() })
        }

        fn grow_draw_image_rate_limit<'a>(
            &'a self,
            user_id: i64,
            _now: OffsetDateTime,
        ) -> DrawImageRateLimitGrowFuture<'a> {
            match self.grows.lock() {
                Ok(mut grows) => grows.push(user_id),
                Err(err) => panic!("draw image rate-limit grows poisoned: {err}"),
            }
            Box::pin(async {})
        }
    }

    #[derive(Debug)]
    struct SongAudioPermissionStub {
        allowed: bool,
        calls: Mutex<Vec<i64>>,
    }

    impl SongAudioPermissionStub {
        fn new(allowed: bool) -> Self {
            Self {
                allowed,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<i64> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("song audio permission calls poisoned: {err}"),
            }
        }
    }

    impl SongAudioPermission for SongAudioPermissionStub {
        fn can_send_song_audio<'a>(&'a self, chat_id: i64) -> SongAudioPermissionFuture<'a> {
            match self.calls.lock() {
                Ok(mut calls) => calls.push(chat_id),
                Err(err) => panic!("song audio permission calls poisoned: {err}"),
            }
            Box::pin(async move { self.allowed })
        }
    }

    #[derive(Debug)]
    struct ImageEditFileResolverStub {
        result: Result<ImageEditFileSelection, String>,
        calls: Mutex<Vec<Vec<ChatAttachment>>>,
        capture_calls: Mutex<Vec<(String, i32, Vec<ChatAttachment>)>>,
    }

    impl ImageEditFileResolverStub {
        fn successful(result: ImageEditFileSelection) -> Self {
            Self {
                result: Ok(result),
                calls: Mutex::new(Vec::new()),
                capture_calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<Vec<ChatAttachment>> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("image edit file resolver calls poisoned: {err}"),
            }
        }

        fn capture_calls(&self) -> Vec<(String, i32, Vec<ChatAttachment>)> {
            match self.capture_calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("image edit file resolver capture calls poisoned: {err}"),
            }
        }
    }

    impl ImageEditFileResolver for ImageEditFileResolverStub {
        fn capture_image_attachments(
            &self,
            media_group_id: &str,
            message_id: i32,
            attachments: &[ChatAttachment],
        ) {
            match self.capture_calls.lock() {
                Ok(mut calls) => {
                    calls.push((media_group_id.to_owned(), message_id, attachments.to_vec()))
                }
                Err(err) => panic!("image edit file resolver capture calls poisoned: {err}"),
            }
        }

        fn resolve_image_edit_files<'a>(
            &'a self,
            attachments: &'a [ChatAttachment],
            _media_group_id: &'a str,
        ) -> ImageEditFileResolveFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push(attachments.to_vec()),
                    Err(err) => panic!("image edit file resolver calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(|message| Box::new(TestError(message)) as ToolboxError)
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct SongSchedulerStub {
        result: Result<SongScheduleResult, String>,
        calls: Mutex<Vec<SongScheduleRequest>>,
    }

    impl SongSchedulerStub {
        fn successful(result: SongScheduleResult) -> Self {
            Self {
                result: Ok(result),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn failed(message: impl Into<String>) -> Self {
            Self {
                result: Err(message.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<SongScheduleRequest> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("song scheduler calls poisoned: {err}"),
            }
        }
    }

    impl SongScheduler for SongSchedulerStub {
        fn schedule_song<'a>(&'a self, request: SongScheduleRequest) -> GenerateSongFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push(request),
                    Err(err) => panic!("song scheduler calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(|message| Box::new(TestError(message)) as ToolboxError)
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct VisionDescriberStub {
        result: Result<VisionDescribeResult, String>,
        calls: Mutex<Vec<VisionDescribeRequest>>,
    }

    impl VisionDescriberStub {
        fn successful(result: VisionDescribeResult) -> Self {
            Self {
                result: Ok(result),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn failed(message: impl Into<String>) -> Self {
            Self {
                result: Err(message.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<VisionDescribeRequest> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("vision describer calls poisoned: {err}"),
            }
        }
    }

    impl VisionDescriber for VisionDescriberStub {
        fn describe_image<'a>(&'a self, request: VisionDescribeRequest) -> VisionImageFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push(request),
                    Err(err) => panic!("vision describer calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(|message| Box::new(TestError(message)) as ToolboxError)
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct HistorySummarizerStub {
        result: Result<ChatHistorySummaryResult, String>,
        calls: Mutex<Vec<HistorySummaryRequest>>,
    }

    impl HistorySummarizerStub {
        fn successful(result: ChatHistorySummaryResult) -> Self {
            Self {
                result: Ok(result),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn failed(message: impl Into<String>) -> Self {
            Self {
                result: Err(message.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<HistorySummaryRequest> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(err) => panic!("history summarizer calls poisoned: {err}"),
            }
        }
    }

    impl ChatHistorySummarizer for HistorySummarizerStub {
        fn chat_history_summary<'a>(
            &'a self,
            request: HistorySummaryRequest,
        ) -> ChatHistorySummaryFuture<'a> {
            let result = {
                match self.calls.lock() {
                    Ok(mut calls) => calls.push(request),
                    Err(err) => panic!("history summarizer calls poisoned: {err}"),
                }
                self.result
                    .clone()
                    .map_err(|message| Box::new(TestError(message)) as ToolboxError)
            };
            Box::pin(async move { result })
        }
    }

    fn toolbox(
        translator: Option<TranslatorStub>,
    ) -> AppDialogToolbox<RatesFetcherStub, RatesDispatcherStub, TranslatorStub> {
        AppDialogToolbox::new(
            Some(Arc::new(RatesFetcherStub)),
            Some(Arc::new(RatesDispatcherStub::default())),
            translator.map(Arc::new),
        )
    }

    fn context() -> ToolContext {
        ToolContext {
            chat_id: -100,
            thread_id: Some(7),
            message_id: 11,
            user_id: 42,
            user_full_name: "Alice".to_owned(),
            message_text: "$".to_owned(),
            message_meta: ChatMessageMeta::default(),
        }
    }

    #[tokio::test]
    async fn app_dialog_toolbox_dispatches_currency_rates_to_llm() -> Result<(), ToolboxError> {
        let dispatcher = Arc::new(RatesDispatcherStub::default());
        let toolbox = AppDialogToolbox::new(
            Some(Arc::new(RatesFetcherStub)),
            Some(Arc::clone(&dispatcher)),
            Some(Arc::new(TranslatorStub {
                result: Ok("ignored".to_owned()),
            })),
        );

        let result = toolbox
            .currency_rates(RatesRequest {
                context: context(),
                pairs: String::new(),
            })
            .await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        assert!(!result.no_reply);
        assert!(result.side_effect.is_none());
        assert!(result.message.contains("💵 USD/RUB=90.00 ₽"));
        assert!(result.message.contains("₿ BTC/USD=100000 $"));
        assert!(result.message.contains("🛢 Brent=76.50 $"));
        assert_eq!(
            result.data.as_ref().expect("data")["rates_text"],
            result.message
        );
        assert_eq!(dispatcher.calls().len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_formats_translation_result() -> Result<(), ToolboxError> {
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("перевод".to_owned()),
        }));

        let result = toolbox
            .translate_text("  hello  ".to_owned(), String::new())
            .await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        assert_eq!(
            result.message,
            "source_lang=en target_lang=ru translated=перевод:hello:ru"
        );
        assert_eq!(
            result.data,
            Some(json!({
                "original": "hello",
                "translated": "перевод:hello:ru",
                "source_lang": "en",
                "target_lang": "ru",
            }))
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_reports_missing_translation_provider() -> Result<(), ToolboxError> {
        let result = toolbox(None)
            .translate_text("hello".to_owned(), "ru".to_owned())
            .await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(result.message, "translation service unavailable");
        assert_eq!(
            result.error,
            Some(ToolError {
                code: "translate_text_unavailable".to_owned(),
                reason: "translation service unavailable".to_owned(),
                retryable: true,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_formats_web_search_result() -> Result<(), ToolboxError> {
        let searcher = Arc::new(WebSearchStub::successful("result text"));
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_web_searcher(searcher.clone());

        let result = toolbox.web_search("  cats  ".to_owned()).await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        assert_eq!(result.message, "search results ready");
        assert_eq!(
            result.data,
            Some(json!({
                "query": "cats",
                "results": "result text",
            }))
        );
        assert_eq!(searcher.calls(), vec!["cats"]);
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_formats_history_search_result() -> Result<(), ToolboxError> {
        let searcher = Arc::new(HistorySearchStub::successful("- Cherry: hello"));
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_history_searcher(searcher.clone());

        let result = toolbox
            .history_search(HistorySearchRequest {
                context: context(),
                query: "  CherryCherry123  ".to_owned(),
            })
            .await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        assert_eq!(result.message, "history search results ready");
        assert_eq!(
            result.data,
            Some(json!({
                "query": "CherryCherry123",
                "results": "- Cherry: hello",
            }))
        );
        assert_eq!(
            searcher.calls(),
            vec![(-100, Some(7), "CherryCherry123".to_owned())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_formats_crawl_url_result() -> Result<(), ToolboxError> {
        let crawler = Arc::new(UrlCrawlerStub::successful("page text"));
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_url_crawler(crawler.clone());

        let result = toolbox
            .crawl_url("  https://example.test/page  ".to_owned())
            .await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        assert_eq!(result.message, "page content fetched");
        assert_eq!(
            result.data,
            Some(json!({
                "url": "https://example.test/page",
                "content": "page text",
            }))
        );
        assert_eq!(crawler.calls(), vec!["https://example.test/page"]);
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_formats_youtube_summary_result() -> Result<(), ToolboxError> {
        let summarizer = Arc::new(YouTubeSummarizerStub::successful(YouTubeSummaryResult {
            summary: "<b>short</b>".to_owned(),
            transcript: "full transcript".to_owned(),
        }));
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_youtube_summarizer(summarizer.clone());

        let result = toolbox.youtube_summary("  abc123  ".to_owned()).await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        assert_eq!(result.message, "youtube summary ready");
        assert_eq!(
            result.data,
            Some(json!({
                "video": "abc123",
                "summary": "<b>short</b>",
                "transcript": "full transcript",
            }))
        );
        assert_eq!(summarizer.calls(), vec!["abc123"]);
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_reports_web_tool_errors() -> Result<(), ToolboxError> {
        let failed_search = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_web_searcher(Arc::new(WebSearchStub::failed("search down")));
        let search_result = failed_search.web_search("cats".to_owned()).await?;

        assert_eq!(search_result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(search_result.message, "search down");
        assert_eq!(
            search_result.error,
            Some(ToolError {
                code: "web_search_failed".to_owned(),
                reason: "search down".to_owned(),
                retryable: true,
            })
        );

        let failed_crawl = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_url_crawler(Arc::new(UrlCrawlerStub::failed("crawl down")));
        let crawl_result = failed_crawl
            .crawl_url("https://example.test".to_owned())
            .await?;

        assert_eq!(crawl_result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(crawl_result.message, "crawl down");
        assert_eq!(
            crawl_result.error,
            Some(ToolError {
                code: "crawl_url_failed".to_owned(),
                reason: "crawl down".to_owned(),
                retryable: true,
            })
        );

        let missing_youtube = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }));
        let youtube_result = missing_youtube.youtube_summary("abc123".to_owned()).await?;

        assert_eq!(youtube_result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(youtube_result.message, "youtube summarizer unavailable");
        assert_eq!(
            youtube_result.error,
            Some(ToolError {
                code: "youtube_summary_unavailable".to_owned(),
                reason: "youtube summarizer unavailable".to_owned(),
                retryable: true,
            })
        );

        let failed_youtube = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_youtube_summarizer(Arc::new(YouTubeSummarizerStub::failed("yt down")));
        let failed_youtube_result = failed_youtube.youtube_summary("abc123".to_owned()).await?;

        assert_eq!(failed_youtube_result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(failed_youtube_result.message, "yt down");
        assert_eq!(
            failed_youtube_result.error,
            Some(ToolError {
                code: "youtube_summary_failed".to_owned(),
                reason: "yt down".to_owned(),
                retryable: true,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_schedules_draw_image_with_go_context() -> Result<(), ToolboxError> {
        let scheduler = Arc::new(ImageSchedulerStub::successful(DrawImageScheduleResult {
            status: " scheduled ".to_owned(),
            message: " image queued ".to_owned(),
            no_reply: false,
            ..DrawImageScheduleResult::default()
        }));
        let attachment = ChatAttachment {
            kind: "image".to_owned(),
            file_unique_id: "unique-1".to_owned(),
            ..ChatAttachment::default()
        };
        let meta = ChatMessageMeta {
            attachments: vec![attachment.clone()],
            ..ChatMessageMeta::default()
        };
        let request = DrawRequest {
            context: ToolContext {
                message_meta: meta,
                ..context()
            },
            prompt: "  neon castle  ".to_owned(),
            negative_prompt: "  blur  ".to_owned(),
            aspect_ratio: " 16:9 ".to_owned(),
            seed: " 42 ".to_owned(),
        };
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_image_scheduler(scheduler.clone());

        let result = toolbox.draw_image(request).await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_QUEUED);
        assert_eq!(result.message, "image queued");
        assert!(!result.no_reply);
        assert_eq!(
            result.side_effect,
            Some(ToolSideEffect {
                kind: "image_generation_job".to_owned(),
                state: "queued".to_owned(),
                ..ToolSideEffect::default()
            })
        );
        assert_eq!(
            result.data,
            Some(json!({
                "status": "scheduled",
            }))
        );
        assert_eq!(
            scheduler.calls(),
            vec![DrawImageScheduleRequest {
                chat_id: -100,
                thread_id: Some(7),
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "neon castle".to_owned(),
                message_text: "$".to_owned(),
                attachments: vec![attachment],
                edit_media_group_id: String::new(),
                negative_prompt: "blur".to_owned(),
                aspect_ratio: "16:9".to_owned(),
                seed: "42".to_owned(),
                original_prompt: "neon castle".to_owned(),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_maps_draw_scheduler_terminal_statuses() -> Result<(), ToolboxError>
    {
        let internal_message = "Запрос не поставлен в очередь. Пользователь уже получил причину (лимиты, доступ или настройки).";
        let internal_toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_image_scheduler(Arc::new(ImageSchedulerStub::successful(
            DrawImageScheduleResult {
                status: "not_scheduled".to_owned(),
                message: internal_message.to_owned(),
                no_reply: false,
                ..DrawImageScheduleResult::default()
            },
        )));

        let internal_result = internal_toolbox
            .draw_image(DrawRequest {
                context: context(),
                prompt: "p".to_owned(),
                ..DrawRequest::default()
            })
            .await?;

        assert_eq!(internal_result.status, TOOL_RESULT_STATUS_NOOP);
        assert_eq!(internal_result.message, "");
        assert!(internal_result.no_reply);
        assert_eq!(
            internal_result.side_effect,
            Some(ToolSideEffect {
                kind: "image_generation_job".to_owned(),
                state: "noop".to_owned(),
                ..ToolSideEffect::default()
            })
        );

        let redraw_toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_image_scheduler(Arc::new(ImageSchedulerStub::successful(
            DrawImageScheduleResult {
                status: "redraw_without_context".to_owned(),
                message: " keep quiet ".to_owned(),
                no_reply: false,
                ..DrawImageScheduleResult::default()
            },
        )));

        let redraw_result = redraw_toolbox
            .draw_image(DrawRequest {
                context: context(),
                prompt: "p".to_owned(),
                ..DrawRequest::default()
            })
            .await?;

        assert_eq!(redraw_result.status, TOOL_RESULT_STATUS_NOOP);
        assert_eq!(
            redraw_result.message,
            "Чтобы перерисовать изображение, опишите, какие изменения вы хотите внести."
        );
        assert!(!redraw_result.no_reply);
        assert_eq!(
            redraw_result.side_effect,
            Some(ToolSideEffect {
                kind: "image_generation_job".to_owned(),
                state: "needs_context".to_owned(),
                ..ToolSideEffect::default()
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_schedules_song_with_go_context() -> Result<(), ToolboxError> {
        let scheduler = Arc::new(SongSchedulerStub::successful(SongScheduleResult {
            status: "not_scheduled".to_owned(),
            message: " no quota ".to_owned(),
            no_reply: false,
            ..SongScheduleResult::default()
        }));
        let meta = ChatMessageMeta {
            sender_username: "alice".to_owned(),
            ..ChatMessageMeta::default()
        };
        let request = SongRequest {
            context: ToolContext {
                message_meta: meta.clone(),
                ..context()
            },
            topic: "  synth rain  ".to_owned(),
        };
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_song_scheduler(scheduler.clone());

        let result = toolbox.generate_song(request).await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(result.message, "no quota");
        assert_eq!(
            result.side_effect,
            Some(ToolSideEffect {
                kind: "music_generation_job".to_owned(),
                state: "failed".to_owned(),
                ..ToolSideEffect::default()
            })
        );
        assert_eq!(
            result.error,
            Some(ToolError {
                code: "generate_song_not_scheduled".to_owned(),
                reason: "no quota".to_owned(),
                retryable: false,
            })
        );
        assert_eq!(
            result.data,
            Some(json!({
                "status": "not_scheduled",
            }))
        );
        assert_eq!(
            scheduler.calls(),
            vec![SongScheduleRequest {
                chat_id: -100,
                thread_id: Some(7),
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                topic: "synth rain".to_owned(),
                message_text: "$".to_owned(),
                message_meta: meta,
                reference_file_id: String::new(),
                reference_file_unique_id: String::new(),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_formats_vision_result() -> Result<(), ToolboxError> {
        let describer = Arc::new(VisionDescriberStub::successful(VisionDescribeResult {
            caption: "  cat on table  ".to_owned(),
            source: "vision".to_owned(),
            file_unique_id: "file-unique".to_owned(),
            history_updated: true,
        }));
        let meta = ChatMessageMeta {
            sender_type: "user".to_owned(),
            ..ChatMessageMeta::default()
        };
        let request = VisionRequest {
            context: ToolContext {
                message_meta: meta.clone(),
                ..context()
            },
            file_id: "file-id".to_owned(),
        };
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_vision_describer(describer.clone());

        let result = toolbox.vision_image(request).await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        assert_eq!(result.message, "cat on table");
        assert_eq!(
            result.data,
            Some(json!({
                "source": "vision",
                "file_unique_id": "file-unique",
                "history_updated": true,
            }))
        );
        assert_eq!(
            describer.calls(),
            vec![VisionDescribeRequest {
                file_id: "file-id".to_owned(),
                chat_id: -100,
                message_id: 11,
                thread_id: Some(7),
                message_meta: meta,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_formats_history_summary_result() -> Result<(), ToolboxError> {
        let summarizer = Arc::new(HistorySummarizerStub::successful(
            ChatHistorySummaryResult {
                summary_id: 99,
                chat_id: -100,
                thread_id: 7,
                scope: "thread".to_owned(),
                range_start_at: "2026-05-20T10:00:00Z".to_owned(),
                range_end_at: "2026-05-20T11:00:00Z".to_owned(),
                raw_message_count: 12,
                covered_message_count: 10,
                reused_summary_count: 2,
                source_summary_ids: vec![1, 2],
                summary_html: "<b>recap</b>".to_owned(),
                summary_json: SummaryContent {
                    events: vec!["shipped".to_owned()],
                    actors: vec![openplotva_history::SummaryActor {
                        name: "Alice".to_owned(),
                        ..openplotva_history::SummaryActor::default()
                    }],
                    recap: "done".to_owned(),
                    source_style: "compact".to_owned(),
                    ..SummaryContent::default()
                },
                model: "aifarm/qwen".to_owned(),
            },
        ));
        let request = HistorySummaryRequest {
            context: context(),
            window: " today ".to_owned(),
            hours: 6,
            message_count: 50,
            since: " 2026-05-20 ".to_owned(),
            scope: " thread ".to_owned(),
        };
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_history_summarizer(summarizer.clone());

        let result = toolbox.chat_history_summary(request).await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        assert_eq!(result.message, "<b>recap</b>");
        assert_eq!(
            result.data,
            Some(json!({
                "summary_id": 99,
                "chat_id": -100,
                "thread_id": 7,
                "scope": "thread",
                "range_start_at": "2026-05-20T10:00:00Z",
                "range_end_at": "2026-05-20T11:00:00Z",
                "raw_message_count": 12,
                "covered_message_count": 10,
                "reused_summary_count": 2,
                "source_summary_ids": [1, 2],
                "summary_html": "<b>recap</b>",
                "summary_json": {
                    "events": ["shipped"],
                    "actors": [{"name": "Alice", "description": ""}],
                    "recap": "done",
                    "open_questions": [],
                    "source_style": "compact",
                },
                "model": "aifarm/qwen",
            }))
        );
        let calls = summarizer.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].window, "today");
        assert_eq!(calls[0].since, "2026-05-20");
        assert_eq!(calls[0].scope, "thread");
        assert_eq!(calls[0].hours, 6);
        assert_eq!(calls[0].message_count, 50);
        assert_eq!(calls[0].context.chat_id, -100);
        Ok(())
    }

    #[test]
    fn chat_history_summary_result_maps_stored_summary_like_go() {
        let start = OffsetDateTime::parse("2026-05-20T10:00:00+02:00", &Rfc3339).expect("start");
        let end = OffsetDateTime::parse("2026-05-20T11:00:00+02:00", &Rfc3339).expect("end");
        let stored = StoredSummary {
            id: 99,
            chat_id: -100,
            thread_id: 7,
            scope: openplotva_history::SummaryScope::Thread,
            range_start_at: start,
            range_end_at: end,
            raw_message_count: 12,
            covered_message_count: 30,
            source_summary_ids: vec![1, 2],
            summary_html: "<b>recap</b>".to_owned(),
            summary_json: SummaryContent {
                events: vec!["shipped".to_owned()],
                recap: "done".to_owned(),
                ..SummaryContent::default()
            },
            model: "aifarm/qwen".to_owned(),
            ..StoredSummary::default()
        };

        let got = ChatHistorySummaryResult::from_stored_summary(&stored, 3);

        assert_eq!(got.summary_id, 99);
        assert_eq!(got.chat_id, -100);
        assert_eq!(got.thread_id, 7);
        assert_eq!(got.scope, "thread");
        assert_eq!(got.range_start_at, "2026-05-20T08:00:00Z");
        assert_eq!(got.range_end_at, "2026-05-20T09:00:00Z");
        assert_eq!(got.raw_message_count, 12);
        assert_eq!(got.covered_message_count, 30);
        assert_eq!(got.reused_summary_count, 3);
        assert_eq!(got.source_summary_ids, vec![1, 2]);
        assert_eq!(got.summary_html, "<b>recap</b>");
        assert_eq!(got.summary_json.recap, "done");
        assert_eq!(got.model, "aifarm/qwen");
    }

    #[tokio::test]
    async fn app_dialog_toolbox_reports_media_and_summary_errors() -> Result<(), ToolboxError> {
        let draw_result = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_image_scheduler(Arc::new(ImageSchedulerStub::failed("draw down")))
        .draw_image(DrawRequest {
            context: context(),
            prompt: "p".to_owned(),
            ..DrawRequest::default()
        })
        .await?;

        assert_eq!(draw_result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(draw_result.message, "draw down");
        assert_eq!(
            draw_result.error,
            Some(ToolError {
                code: "draw_image_failed".to_owned(),
                reason: "draw down".to_owned(),
                retryable: true,
            })
        );

        let song_result = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_song_scheduler(Arc::new(SongSchedulerStub::failed("song down")))
        .generate_song(SongRequest {
            context: context(),
            topic: "t".to_owned(),
        })
        .await?;

        assert_eq!(song_result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(song_result.message, "song down");
        assert_eq!(
            song_result.error,
            Some(ToolError {
                code: "generate_song_failed".to_owned(),
                reason: "song down".to_owned(),
                retryable: true,
            })
        );

        let vision_result = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_vision_describer(Arc::new(VisionDescriberStub::failed("vision down")))
        .vision_image(VisionRequest {
            context: context(),
            file_id: "file".to_owned(),
        })
        .await?;

        assert_eq!(vision_result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(vision_result.message, "vision down");
        assert_eq!(
            vision_result.error,
            Some(ToolError {
                code: "vision_image_failed".to_owned(),
                reason: "vision down".to_owned(),
                retryable: true,
            })
        );

        let history_result = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_history_summarizer(Arc::new(HistorySummarizerStub::failed("history down")))
        .chat_history_summary(HistorySummaryRequest {
            context: context(),
            window: "today".to_owned(),
            ..HistorySummaryRequest::default()
        })
        .await?;

        assert_eq!(history_result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(history_result.message, "history down");
        assert_eq!(
            history_result.error,
            Some(ToolError {
                code: "chat_history_summary_failed".to_owned(),
                reason: "history down".to_owned(),
                retryable: true,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_formats_queue_status() -> Result<(), ToolboxError> {
        let queue_status = Arc::new(QueueStatusStub::successful(QueueStatusResult {
            regular_queue_depth: 3,
            vip_queue_depth: 2,
            active_jobs_count: 4,
            user_active_jobs: 1,
            user_pending_jobs: 2,
            estimated_wait: "1m30s".to_owned(),
        }));
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_queue_status_provider(queue_status.clone());

        let result = toolbox.queue_status(42).await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        assert_eq!(
            result.message,
            "Image generation queue status:\n\
             - Total jobs waiting in queue: 5\n\
             - Jobs currently being processed: 4\n\
             - This user's active jobs (currently generating): 1\n\
             - This user's pending jobs (waiting in queue): 2\n\
             - Estimated wait time for a new request: 1 minutes"
        );
        assert_eq!(
            result.data,
            Some(json!({
                "regular_queue_depth": 3,
                "vip_queue_depth": 2,
                "active_jobs_count": 4,
                "user_active_jobs": 1,
                "user_pending_jobs": 2,
                "estimated_wait": "1m30s",
            }))
        );
        assert_eq!(queue_status.calls(), vec![42]);
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_uses_shared_queue_for_status_and_cancel()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)
            .map_err(|err| Box::new(err) as ToolboxError)?;
        let regular = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            taskman_test_job("regular-default", DEFAULT_PRIORITY, now, 42, -100, 10),
        );
        queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            taskman_test_job("regular-low", LOWEST_PRIORITY, now, 42, -100, 11),
        );
        let vip = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            taskman_test_job("vip-default", DEFAULT_PRIORITY, now, 42, -100, 12),
        );
        queue.dequeue(IMAGE_REGULAR_QUEUE_NAME, "image-regular-worker-1", now);

        let adapter = Arc::new(TaskmanDialogToolAdapter::new(queue));
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_queue_status_provider(adapter.clone())
        .with_drawing_canceller(adapter);

        let status = toolbox.queue_status(42).await?;
        assert_eq!(
            status.message,
            "Image generation queue status:\n\
             - Total jobs waiting in queue: 1\n\
             - Jobs currently being processed: 3\n\
             - This user's active jobs (currently generating): 3\n\
             - This user's pending jobs (waiting in queue): 3\n\
             - Estimated wait time for a new request: 0 minutes"
        );
        assert_eq!(
            status.data,
            Some(json!({
                "regular_queue_depth": 0,
                "vip_queue_depth": 1,
                "active_jobs_count": 3,
                "user_active_jobs": 3,
                "user_pending_jobs": 3,
                "estimated_wait": "0s",
            }))
        );

        let cancelled = toolbox.cancel_drawing(42, -100).await?;
        assert_eq!(cancelled.status, TOOL_RESULT_STATUS_EXECUTED);
        assert_eq!(
            cancelled.message,
            "Cancellation result: Successfully cancelled 3 image generation job(s) for this user."
        );
        assert_eq!(
            cancelled.side_effect,
            Some(ToolSideEffect {
                kind: "image_generation_job".to_owned(),
                state: "cancelled".to_owned(),
                ..ToolSideEffect::default()
            })
        );
        assert_eq!(
            cancelled.data,
            Some(json!({
                "cancelled_count": 3,
                "cancelled_ids": [regular, regular + 1, vip],
            }))
        );
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_assigns_go_image_generation_job()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(false));
        let adapter = Arc::new(
            TaskmanDialogToolAdapter::new(Arc::clone(&queue))
                .with_draw_image_vip_status(vip.clone()),
        );
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_image_scheduler(adapter);

        let result = toolbox
            .draw_image(DrawRequest {
                context: context(),
                prompt: " neon\u{200f}\tcastle ".to_owned(),
                negative_prompt: " blur ".to_owned(),
                aspect_ratio: " 16:9 ".to_owned(),
                seed: " 42 ".to_owned(),
            })
            .await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_QUEUED);
        assert_eq!(result.data, Some(json!({"status": "scheduled"})));
        assert_eq!(vip.calls(), vec![42]);
        let records = queue.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].queue_name, IMAGE_REGULAR_QUEUE_NAME);
        assert_eq!(records[0].job.title, "image");
        assert_eq!(records[0].job.priority, DEFAULT_PRIORITY);
        let telegram = records[0]
            .job
            .data
            .telegram_data
            .as_ref()
            .expect("image job should carry telegram data");
        assert_eq!(telegram.chat_id, -100);
        assert_eq!(telegram.user_id, 42);
        assert_eq!(telegram.message_id, 11);
        assert_eq!(telegram.thread_message_id, Some(7));
        assert_eq!(telegram.user_full_name, "Alice");
        let image = records[0]
            .job
            .data
            .image_data
            .as_ref()
            .expect("image job should carry image data");
        assert_eq!(image.prompt, "neon castle");
        assert_eq!(image.original_text, "neon castle");
        assert_eq!(image.author, "Alice");
        assert_eq!(image.raw_negative_prompt, "blur");
        assert_eq!(image.raw_aspect_ratio, "16:9");
        assert_eq!(image.raw_seed, "42");
        assert_eq!(
            image.meta,
            json!({
                "type": "text",
                "sender_type": "user",
                "sender_id": 42,
                "sender_name": "Alice",
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_reacts_instead_of_placeholder_when_configured()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(true));
        let rich = Arc::new(crate::rich::MockRichSender::default());
        let reactions = Arc::new(crate::reactions::test_support::RecordingReactionSink::default());
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_generation_reactions(
                Arc::clone(&reactions) as crate::reactions::GenerationReactions
            )
            .with_draw_image_vip_status(vip.clone());

        let scheduled = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                thread_id: Some(7),
                ..DrawImageScheduleRequest::default()
            })
            .await?;

        assert_eq!(scheduled.status, "scheduled");
        // Reactions take precedence: no ⏳ placeholder message, a 👀 reaction
        // on the trigger instead, and no queue-position id to reuse later.
        assert!(rich.sent.lock().expect("rich sent").is_empty());
        let calls = reactions.calls.lock().expect("reaction calls").clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "queued");
        assert_eq!(calls[0].1.chat_id, -100);
        assert_eq!(calls[0].1.message_id, 11);
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_reacts_on_song_schedule() -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(true));
        let reactions = Arc::new(crate::reactions::test_support::RecordingReactionSink::default());
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_generation_reactions(
                Arc::clone(&reactions) as crate::reactions::GenerationReactions
            )
            .with_draw_image_vip_status(vip.clone());

        let scheduled = adapter
            .schedule_song(SongScheduleRequest {
                chat_id: -100,
                message_id: 12,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                topic: "night city".to_owned(),
                ..SongScheduleRequest::default()
            })
            .await?;

        assert_eq!(scheduled.status, "scheduled");
        let calls = reactions.calls.lock().expect("reaction calls").clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "queued");
        assert_eq!(calls[0].1.chat_id, -100);
        assert_eq!(calls[0].1.message_id, 12);
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_uses_vip_queue_and_user_limit() -> Result<(), ToolboxError>
    {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(true));
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone());

        let scheduled = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                thread_id: Some(7),
                ..DrawImageScheduleRequest::default()
            })
            .await?;

        assert_eq!(scheduled.status, "scheduled");
        assert_eq!(vip.calls(), vec![42]);
        assert_eq!(queue.records()[0].queue_name, IMAGE_VIP_QUEUE_NAME);
        assert_eq!(queue.records()[0].job.priority, HIGHEST_PRIORITY);

        for idx in 0..9 {
            queue.assign(
                IMAGE_VIP_QUEUE_NAME,
                taskman_test_job(
                    "vip-active",
                    HIGHEST_PRIORITY,
                    OffsetDateTime::UNIX_EPOCH,
                    42,
                    -100,
                    20 + idx,
                ),
            );
        }

        let blocked = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                message_id: 99,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "another castle".to_owned(),
                ..DrawImageScheduleRequest::default()
            })
            .await?;

        assert_eq!(blocked.status, "not_scheduled");
        assert!(!blocked.no_reply, "rejection feeds back to the model");
        assert_eq!(
            blocked.rejection,
            Some(DrawImageScheduleRejection::ActiveJobLimit)
        );
        assert_eq!(
            blocked.message,
            DrawImageScheduleRejection::ActiveJobLimit.model_facing_message()
        );
        assert_eq!(queue.records().len(), 10);
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_enforces_go_draw_rate_limit_before_assignment()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(false));
        let rate_limit = Arc::new(DrawImageRateLimitStub::new(DrawImageRateLimitReport {
            limited: true,
            notice_text: "Вы часто повторяете этот запрос. Попробуйте снова через 1 мин."
                .to_owned(),
        }));
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone())
            .with_draw_image_rate_limit(rate_limit.clone());

        let blocked = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                ..DrawImageScheduleRequest::default()
            })
            .await?;

        assert_eq!(blocked.status, "not_scheduled");
        assert!(!blocked.no_reply, "rejection feeds back to the model");
        assert_eq!(
            blocked.rejection,
            Some(DrawImageScheduleRejection::RateLimited)
        );
        assert_eq!(
            blocked.message,
            "Вы часто повторяете этот запрос. Попробуйте снова через 1 мин.\n\nПриобретите VIP-подписку c помощью /vip и получите:\n⚡ Приоритетное выполнение запросов вне очереди\n🔄 Вдвое увеличенные лимиты на рисование\n🖼️ Изображения лучшего качества с меньшей цензурой\n🎵 Генерацию песен по команде /song\n🚀 Доступ к новым функциям и фичам (в разработке)"
        );
        assert_eq!(rate_limit.checks(), vec![("castle".to_owned(), 42, false)]);
        assert!(rate_limit.grows().is_empty());
        assert!(queue.records().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_grows_go_draw_rate_limit_after_assignment()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(true));
        let rate_limit = Arc::new(DrawImageRateLimitStub::new(DrawImageRateLimitReport {
            limited: false,
            notice_text: String::new(),
        }));
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone())
            .with_draw_image_rate_limit(rate_limit.clone());

        let scheduled = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                ..DrawImageScheduleRequest::default()
            })
            .await?;

        assert_eq!(scheduled.status, "scheduled");
        assert_eq!(rate_limit.checks(), vec![("castle".to_owned(), 42, true)]);
        assert_eq!(rate_limit.grows(), vec![42]);
        assert_eq!(queue.records().len(), 1);
        Ok(())
    }

    #[test]
    fn draw_image_rate_limit_helpers_match_go_threshold_and_text_contract()
    -> Result<(), time::error::ComponentRange> {
        let now = OffsetDateTime::from_unix_timestamp(1_770_000_000)?;
        let timestamps = vec![
            now - time::Duration::minutes(9),
            now - time::Duration::minutes(8),
            now - time::Duration::minutes(7),
            now - time::Duration::minutes(6),
            now - time::Duration::minutes(1),
        ];

        assert_eq!(
            draw_rate_limit_remaining(now, &timestamps, draw_rate_limit_thresholds(false)),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            draw_rate_limit_remaining(now, &timestamps, draw_rate_limit_thresholds(true)),
            None
        );
        assert_eq!(
            format_draw_rate_limit_duration(Duration::from_secs(3661)),
            "1 час 1 мин."
        );
        assert_eq!(
            format_draw_rate_limit_duration(Duration::from_secs(61)),
            "1 мин. 1 сек."
        );
        assert_eq!(
            draw_rate_limit_response_text("подождите", true),
            "подождите"
        );
        assert!(
            draw_rate_limit_response_text("подождите", false)
                .contains("Приобретите VIP-подписку c помощью /vip")
        );
        assert!(is_draw_prompt_whitelisted(
            "Нарисуй КОТИК в шляпе",
            &normalize_draw_prompt_whitelist(["котик"])
        ));
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_rejects_draw_when_chat_draw_is_disabled()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(true));
        let permission = Arc::new(DrawImagePermissionStub::new(Some(
            DrawImageScheduleRejection::DrawDisabled,
        )));
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone())
            .with_draw_image_permission(permission.clone());

        let blocked = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                ..DrawImageScheduleRequest::default()
            })
            .await?;

        assert_eq!(blocked.status, "not_scheduled");
        assert!(!blocked.no_reply, "rejection feeds back to the model");
        assert_eq!(
            blocked.rejection,
            Some(DrawImageScheduleRejection::DrawDisabled)
        );
        assert_eq!(
            blocked.message,
            DrawImageScheduleRejection::DrawDisabled.model_facing_message()
        );
        assert_eq!(permission.calls(), vec![-100]);
        assert!(vip.calls().is_empty());
        assert!(queue.records().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_rejects_image_edit_when_image_sends_are_disabled()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(true));
        let permission = Arc::new(DrawImagePermissionStub::new(Some(
            DrawImageScheduleRejection::ImageNotAllowed,
        )));
        let resolver = Arc::new(ImageEditFileResolverStub::successful(
            ImageEditFileSelection {
                photo_file_id: "latest-photo-file".to_owned(),
                photo_urls: Vec::new(),
            },
        ));
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone())
            .with_draw_image_permission(permission.clone())
            .with_image_edit_file_resolver(resolver.clone());

        let blocked = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "make it brighter".to_owned(),
                attachments: vec![ChatAttachment {
                    kind: "image".to_owned(),
                    file_unique_id: "unique-photo".to_owned(),
                    ..ChatAttachment::default()
                }],
                ..DrawImageScheduleRequest::default()
            })
            .await?;

        assert_eq!(blocked.status, "not_scheduled");
        assert!(!blocked.no_reply, "rejection feeds back to the model");
        assert_eq!(
            blocked.rejection,
            Some(DrawImageScheduleRejection::ImageNotAllowed)
        );
        assert_eq!(resolver.calls().len(), 1);
        assert_eq!(permission.calls(), vec![-100]);
        assert!(vip.calls().is_empty());
        assert!(queue.records().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_assigns_go_music_job_and_enforces_vip_limit()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(true));
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone());

        let scheduled = adapter
            .schedule_song(SongScheduleRequest {
                chat_id: -100,
                thread_id: Some(7),
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                topic: " night city ".to_owned(),
                message_text: "song night city".to_owned(),
                message_meta: ChatMessageMeta {
                    annotation: "audio ref".to_owned(),
                    ..ChatMessageMeta::default()
                },
                reference_file_id: "audio-file".to_owned(),
                reference_file_unique_id: "audio-unique".to_owned(),
            })
            .await?;

        assert_eq!(scheduled.status, "scheduled");
        assert_eq!(vip.calls(), vec![42]);
        let records = queue.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].queue_name, MUSIC_VIP_QUEUE_NAME);
        assert_eq!(records[0].job.title, "music");
        assert_eq!(records[0].job.priority, HIGHEST_PRIORITY);
        assert_eq!(
            records[0].job.data.job_type,
            openplotva_taskman::JobType::MusicGen
        );
        let music = records[0]
            .job
            .data
            .music_data
            .as_ref()
            .expect("music job should carry music data");
        assert_eq!(music.topic, "night city");
        assert_eq!(music.meta["annotation"], "audio ref");
        assert_eq!(music.reference_file_id, "audio-file");
        assert_eq!(music.reference_file_unique_id, "audio-unique");

        queue.assign(
            MUSIC_VIP_QUEUE_NAME,
            taskman_test_job(
                "music-active",
                HIGHEST_PRIORITY,
                OffsetDateTime::UNIX_EPOCH,
                42,
                -100,
                12,
            ),
        );

        let blocked = adapter
            .schedule_song(SongScheduleRequest {
                chat_id: -100,
                message_id: 13,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                topic: "another".to_owned(),
                ..SongScheduleRequest::default()
            })
            .await?;

        assert_eq!(blocked.status, "not_scheduled");
        assert!(!blocked.no_reply, "rejection feeds back to the model");
        assert_eq!(blocked.rejection, Some(SongScheduleRejection::ActiveLimit));
        assert_eq!(
            blocked.message,
            SongScheduleRejection::ActiveLimit.model_facing_message()
        );
        assert_eq!(queue.records().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_uses_quoted_audio_meta_when_reference_is_empty()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(DrawImageVipStatusStub::new(true)));

        adapter
            .schedule_song(SongScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                topic: "night city".to_owned(),
                message_meta: ChatMessageMeta {
                    attachments: vec![ChatAttachment {
                        kind: "audio".to_owned(),
                        source: "quoted".to_owned(),
                        file_unique_id: "reply-audio-unique".to_owned(),
                        ..ChatAttachment::default()
                    }],
                    ..ChatMessageMeta::default()
                },
                ..SongScheduleRequest::default()
            })
            .await?;

        let records = queue.records();
        let music = records[0]
            .job
            .data
            .music_data
            .as_ref()
            .expect("music job should carry music data");
        assert_eq!(music.reference_file_id, "");
        assert_eq!(music.reference_file_unique_id, "reply-audio-unique");
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_rejects_song_when_service_or_audio_unavailable()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(true));
        let service_unavailable = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone())
            .with_song_service_available(false);

        let blocked_service = service_unavailable
            .schedule_song(SongScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                topic: "night city".to_owned(),
                ..SongScheduleRequest::default()
            })
            .await?;

        assert_eq!(blocked_service.status, "not_scheduled");
        assert!(
            !blocked_service.no_reply,
            "rejection feeds back to the model"
        );
        assert_eq!(
            blocked_service.rejection,
            Some(SongScheduleRejection::ServiceUnavailable)
        );
        assert!(vip.calls().is_empty());

        let permission = Arc::new(SongAudioPermissionStub::new(false));
        let audio_blocked = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone())
            .with_song_audio_permission(permission.clone());

        let blocked_audio = audio_blocked
            .schedule_song(SongScheduleRequest {
                chat_id: -100,
                message_id: 12,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                topic: "night city".to_owned(),
                ..SongScheduleRequest::default()
            })
            .await?;

        assert_eq!(blocked_audio.status, "not_scheduled");
        assert!(!blocked_audio.no_reply, "rejection feeds back to the model");
        assert_eq!(
            blocked_audio.rejection,
            Some(SongScheduleRejection::AudioNotAllowed)
        );
        assert_eq!(permission.calls(), vec![-100]);
        assert!(vip.calls().is_empty());
        assert!(queue.records().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_keeps_music_vip_only() -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(false));
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone());

        let blocked = adapter
            .schedule_song(SongScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                topic: "night city".to_owned(),
                ..SongScheduleRequest::default()
            })
            .await?;

        assert_eq!(blocked.status, "not_scheduled");
        assert!(!blocked.no_reply, "rejection feeds back to the model");
        assert_eq!(blocked.rejection, Some(SongScheduleRejection::VipOnly));
        assert_eq!(
            blocked.message,
            SongScheduleRejection::VipOnly.model_facing_message()
        );
        assert_eq!(vip.calls(), vec![42]);
        assert!(queue.records().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_does_not_fake_unported_image_edit()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue));

        let result = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "make it brighter".to_owned(),
                attachments: vec![ChatAttachment {
                    kind: "image".to_owned(),
                    file_unique_id: "unique-photo".to_owned(),
                    ..ChatAttachment::default()
                }],
                ..DrawImageScheduleRequest::default()
            })
            .await?;

        assert_eq!(result.status, "not_scheduled");
        assert!(!result.no_reply, "rejection feeds back to the model");
        assert_eq!(
            result.rejection,
            Some(DrawImageScheduleRejection::EditSourceUnavailable)
        );
        assert!(queue.records().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_assigns_go_image_edit_job_with_file_resolver()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(true));
        let resolver = Arc::new(ImageEditFileResolverStub::successful(
            ImageEditFileSelection {
                photo_file_id: "latest-photo-file".to_owned(),
                photo_urls: vec!["https://files.test/photo.png".to_owned()],
            },
        ));
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone())
            .with_image_edit_file_resolver(resolver.clone());

        let result = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: " make it brighter ".to_owned(),
                thread_id: Some(7),
                edit_media_group_id: "album-1".to_owned(),
                attachments: vec![
                    ChatAttachment {
                        kind: "image".to_owned(),
                        file_unique_id: "unique-photo".to_owned(),
                        ..ChatAttachment::default()
                    },
                    ChatAttachment {
                        kind: "image".to_owned(),
                        file_unique_id: "unique-photo".to_owned(),
                        ..ChatAttachment::default()
                    },
                ],
                ..DrawImageScheduleRequest::default()
            })
            .await?;

        assert_eq!(result.status, "scheduled");
        assert_eq!(vip.calls(), vec![42]);
        assert_eq!(resolver.calls().len(), 1);
        assert_eq!(resolver.calls()[0].len(), 2);
        assert!(resolver.capture_calls().is_empty());
        let records = queue.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].queue_name, IMAGE_VIP_QUEUE_NAME);
        assert_eq!(records[0].job.title, "image_edit");
        assert_eq!(records[0].job.priority, HIGHEST_PRIORITY);
        assert_eq!(
            records[0].job.data.job_type,
            openplotva_taskman::JobType::ImageEdit
        );
        let image = records[0]
            .job
            .data
            .image_data
            .as_ref()
            .expect("image edit job should carry image data");
        assert_eq!(image.prompt, "make it brighter");
        assert_eq!(image.image_file_id, "latest-photo-file");
        assert_eq!(
            image.image_urls,
            vec!["https://files.test/photo.png".to_owned()]
        );
        assert!(image.is_image_edit);
        Ok(())
    }

    #[test]
    fn image_edit_file_selection_preserves_attachment_order_and_go_file_url_shape() {
        let file_unique_ids = vec![
            "unique-a".to_owned(),
            "unique-b".to_owned(),
            "missing".to_owned(),
        ];
        let rows = vec![
            telegram_file_record("unique-b", "file-b"),
            telegram_file_record("unique-a", "file-a"),
            telegram_file_record("missing", " "),
        ];

        assert_eq!(
            latest_file_ids_in_attachment_order(&file_unique_ids, &rows),
            vec!["file-a".to_owned(), "file-b".to_owned()]
        );
        assert_eq!(
            telegram_file_url("123:secret", "photos/file-a.jpg"),
            "https://api.telegram.org/file/bot123:secret/photos/file-a.jpg"
        );
    }

    #[test]
    fn image_edit_file_selection_keeps_target_file_id_but_prefers_album_urls() {
        let target_unique_ids = vec!["unique-b".to_owned()];
        let media_group_unique_ids = vec!["unique-a".to_owned(), "unique-b".to_owned()];
        let rows = vec![
            telegram_file_record("unique-a", "file-a"),
            telegram_file_record("unique-b", "file-b"),
        ];

        assert_eq!(
            image_edit_file_id_selection(&target_unique_ids, &media_group_unique_ids, &rows),
            ImageEditFileIdSelection {
                photo_file_id: "file-b".to_owned(),
                url_file_ids: vec!["file-a".to_owned(), "file-b".to_owned()],
            }
        );
    }

    #[tokio::test]
    async fn media_group_registry_collects_unique_images_in_go_arrival_order() {
        let registry = MediaGroupImageRegistry::new(std::time::Duration::ZERO, 2);
        registry.add_image_attachments("album-1", 10, &[image_attachment("unique-a")]);
        registry.add_image_attachments("album-1", 10, &[image_attachment("ignored-duplicate")]);
        registry.add_image_attachments("album-1", 11, &[image_attachment("unique-b")]);
        registry.add_image_attachments("album-1", 12, &[image_attachment("ignored-over-limit")]);

        assert_eq!(
            registry.wait_collect_and_remove_unique_ids("album-1").await,
            vec!["unique-a".to_owned(), "unique-b".to_owned()]
        );
        assert!(
            registry
                .wait_collect_and_remove_unique_ids("album-1")
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_keeps_image_edit_vip_only() -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let vip = Arc::new(DrawImageVipStatusStub::new(false));
        let resolver = Arc::new(ImageEditFileResolverStub::successful(
            ImageEditFileSelection {
                photo_file_id: "latest-photo-file".to_owned(),
                photo_urls: Vec::new(),
            },
        ));
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(vip.clone())
            .with_image_edit_file_resolver(resolver.clone());

        let result = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "make it brighter".to_owned(),
                attachments: vec![ChatAttachment {
                    kind: "image".to_owned(),
                    file_unique_id: "unique-photo".to_owned(),
                    ..ChatAttachment::default()
                }],
                ..DrawImageScheduleRequest::default()
            })
            .await?;

        assert_eq!(result.status, "not_scheduled");
        assert!(!result.no_reply, "rejection feeds back to the model");
        assert_eq!(result.rejection, Some(DrawImageScheduleRejection::VipOnly));
        assert_eq!(vip.calls(), vec![42]);
        assert_eq!(resolver.calls().len(), 1);
        assert!(queue.records().is_empty());
        Ok(())
    }

    #[derive(Default)]
    struct ObligationRecorderStub {
        recorded: Mutex<Vec<openplotva_storage::NewDeliveryObligation>>,
        duplicate: bool,
        fail: bool,
    }

    impl ObligationRecorderStub {
        fn duplicate() -> Self {
            Self {
                duplicate: true,
                ..Self::default()
            }
        }

        fn failing() -> Self {
            Self {
                fail: true,
                ..Self::default()
            }
        }

        fn recorded(&self) -> Vec<openplotva_storage::NewDeliveryObligation> {
            lock_mutex(&self.recorded).clone()
        }
    }

    impl crate::dialog_turn::DeliveryObligationRecorder for ObligationRecorderStub {
        fn record_delivery_obligation<'a>(
            &'a self,
            obligation: openplotva_storage::NewDeliveryObligation,
        ) -> crate::dialog_turn::ObligationFuture<'a, bool> {
            if self.fail {
                return Box::pin(async {
                    Err(Box::new(TestError("obligations down".to_owned()))
                        as crate::dialog_turn::ObligationError)
                });
            }
            lock_mutex(&self.recorded).push(obligation);
            let inserted = !self.duplicate;
            Box::pin(async move { Ok(inserted) })
        }
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_records_obligations_at_schedule_time()
    -> Result<(), ToolboxError> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let recorder = Arc::new(ObligationRecorderStub::default());
        let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(DrawImageVipStatusStub::new(true)))
            .with_delivery_obligations(recorder.clone(), 1200, 1800);
        let before = OffsetDateTime::now_utc();

        let image = adapter
            .schedule_image(DrawImageScheduleRequest {
                chat_id: -100,
                thread_id: Some(7),
                message_id: 11,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                ..DrawImageScheduleRequest::default()
            })
            .await?;
        let song = adapter
            .schedule_song(SongScheduleRequest {
                chat_id: -100,
                message_id: 12,
                user_id: 42,
                user_full_name: "Alice".to_owned(),
                topic: "night city".to_owned(),
                ..SongScheduleRequest::default()
            })
            .await?;

        let records = queue.records();
        assert_eq!(records.len(), 2);
        assert_eq!(
            image.job_id,
            Some(records[0].id),
            "scheduler returns the ticket id"
        );
        assert_eq!(song.job_id, Some(records[1].id));
        let recorded = recorder.recorded();
        assert_eq!(recorded.len(), 2, "one obligation per assigned ticket");
        assert_eq!(recorded[0].chat_id, -100);
        assert_eq!(recorded[0].thread_id, Some(7));
        assert_eq!(recorded[0].user_id, 42);
        assert_eq!(recorded[0].trigger_message_id, 11);
        assert_eq!(
            recorded[0].dialog_job_id,
            openplotva_storage::DELIVERY_OBLIGATION_DIALOG_JOB_UNKNOWN
        );
        assert_eq!(recorded[0].kind, "image_generation_job");
        assert_eq!(recorded[0].ticket_job_id, records[0].id);
        assert!(recorded[0].deadline_at >= before + time::Duration::seconds(1200));
        assert_eq!(recorded[1].kind, "music_generation_job");
        assert_eq!(recorded[1].ticket_job_id, records[1].id);
        assert_eq!(recorded[1].trigger_message_id, 12);
        assert!(recorded[1].deadline_at >= before + time::Duration::seconds(1800));
        Ok(())
    }

    #[tokio::test]
    async fn taskman_dialog_tool_adapter_obligation_conflict_or_failure_never_blocks_scheduling()
    -> Result<(), ToolboxError> {
        for recorder in [
            Arc::new(ObligationRecorderStub::duplicate()),
            Arc::new(ObligationRecorderStub::failing()),
        ] {
            let queue = Arc::new(InMemoryTaskQueue::new());
            let adapter = TaskmanDialogToolAdapter::new(Arc::clone(&queue))
                .with_draw_image_vip_status(Arc::new(DrawImageVipStatusStub::new(true)))
                .with_delivery_obligations(recorder.clone(), 1200, 1800);

            let scheduled = adapter
                .schedule_image(DrawImageScheduleRequest {
                    chat_id: -100,
                    message_id: 11,
                    user_id: 42,
                    user_full_name: "Alice".to_owned(),
                    prompt: "castle".to_owned(),
                    ..DrawImageScheduleRequest::default()
                })
                .await?;

            assert_eq!(scheduled.status, "scheduled");
            assert!(scheduled.job_id.is_some());
            assert_eq!(queue.records().len(), 1, "scheduling always proceeds");
        }
        Ok(())
    }

    #[test]
    fn scheduled_generation_tool_result_carries_ticket_and_reason_codes() {
        let queued = dialog_draw_image_tool_result(&DrawImageScheduleResult {
            status: "scheduled".to_owned(),
            job_id: Some(777),
            ..DrawImageScheduleResult::default()
        });
        assert_eq!(queued.status, TOOL_RESULT_STATUS_QUEUED);
        assert_eq!(queued.message, "", "queued schedules stay silent");
        assert!(!queued.no_reply);
        let side_effect = queued.side_effect.expect("side effect");
        assert_eq!(side_effect.kind, "image_generation_job");
        assert_eq!(side_effect.ticket_id, "777");
        assert_eq!(side_effect.state, "queued");
        assert_eq!(queued.data, Some(json!({"status": "scheduled"})));

        let queued_song = dialog_generate_song_tool_result(&SongScheduleResult {
            status: "scheduled".to_owned(),
            job_id: Some(778),
            ..SongScheduleResult::default()
        });
        assert_eq!(queued_song.status, TOOL_RESULT_STATUS_QUEUED);
        assert_eq!(queued_song.message, "");
        assert_eq!(
            queued_song.side_effect.expect("side effect").ticket_id,
            "778"
        );

        let draw_cases = [
            (DrawImageScheduleRejection::VipOnly, "vip_only"),
            (
                DrawImageScheduleRejection::RateLimited,
                "generation_rate_limited",
            ),
            (DrawImageScheduleRejection::DrawDisabled, "draw_disabled"),
            (
                DrawImageScheduleRejection::ImageNotAllowed,
                "image_not_allowed",
            ),
            (
                DrawImageScheduleRejection::ActiveJobLimit,
                "active_job_limit",
            ),
            (
                DrawImageScheduleRejection::EnqueueRateLimited,
                "enqueue_rate_limited",
            ),
            (DrawImageScheduleRejection::QueueFull, "queue_full"),
            (
                DrawImageScheduleRejection::EditSourceUnavailable,
                "edit_source_unavailable",
            ),
        ];
        for (rejection, code) in draw_cases {
            let result = dialog_draw_image_tool_result(&not_scheduled_draw(rejection));
            assert_eq!(result.status, TOOL_RESULT_STATUS_FAILED, "code: {code}");
            assert!(
                !result.no_reply,
                "rejections feed back to the model: {code}"
            );
            assert_eq!(
                result.message,
                rejection.model_facing_message(),
                "code: {code}"
            );
            assert_eq!(
                result.data.as_ref().expect("data")["reason"],
                json!(code),
                "code: {code}"
            );
            assert_eq!(
                result.side_effect.as_ref().expect("side effect").ticket_id,
                "",
                "rejections never carry a ticket: {code}"
            );
        }

        let song_cases = [
            (
                SongScheduleRejection::ServiceUnavailable,
                "service_unavailable",
            ),
            (SongScheduleRejection::AudioNotAllowed, "audio_not_allowed"),
            (SongScheduleRejection::VipOnly, "vip_only"),
            (SongScheduleRejection::EmptyTopic, "empty_topic"),
            (SongScheduleRejection::ActiveLimit, "active_job_limit"),
            (SongScheduleRejection::RateLimited, "enqueue_rate_limited"),
        ];
        for (rejection, code) in song_cases {
            let result = dialog_generate_song_tool_result(&not_scheduled_song(rejection));
            assert_eq!(result.status, TOOL_RESULT_STATUS_FAILED, "code: {code}");
            assert!(
                !result.no_reply,
                "rejections feed back to the model: {code}"
            );
            assert_eq!(
                result.message,
                rejection.model_facing_message(),
                "code: {code}"
            );
            assert_eq!(
                result.data.as_ref().expect("data")["reason"],
                json!(code),
                "code: {code}"
            );
        }

        // Every rejection code round-trips into the turn classifier's catalog.
        for code in draw_cases
            .iter()
            .map(|(_, code)| *code)
            .chain(song_cases.iter().map(|(_, code)| *code))
        {
            assert!(
                openplotva_dialog::turn::NoReplyReason::from_code(code).is_some(),
                "classifier must recognize {code}"
            );
        }
    }

    #[tokio::test]
    async fn app_dialog_toolbox_reports_queue_status_error() -> Result<(), ToolboxError> {
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_queue_status_provider(Arc::new(QueueStatusStub::failed("db down")));

        let result = toolbox.queue_status(42).await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(result.message, "db down");
        assert_eq!(
            result.error,
            Some(ToolError {
                code: "queue_status_failed".to_owned(),
                reason: "db down".to_owned(),
                retryable: true,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_formats_cancel_drawing_success_and_noop() -> Result<(), ToolboxError>
    {
        let canceller = Arc::new(DrawingCancellerStub::successful(CancelDrawingResult {
            cancelled_count: 2,
            cancelled_ids: vec![10, 11],
        }));
        let success_toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_drawing_canceller(canceller.clone());

        let result = success_toolbox.cancel_drawing(42, -100).await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_EXECUTED);
        assert_eq!(
            result.message,
            "Cancellation result: Successfully cancelled 2 image generation job(s) for this user."
        );
        assert_eq!(
            result.side_effect,
            Some(ToolSideEffect {
                kind: "image_generation_job".to_owned(),
                state: "cancelled".to_owned(),
                ..ToolSideEffect::default()
            })
        );
        assert_eq!(
            result.data,
            Some(json!({
                "cancelled_count": 2,
                "cancelled_ids": [10, 11],
            }))
        );
        assert_eq!(canceller.calls(), vec![(42, -100)]);

        let noop_toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_drawing_canceller(Arc::new(DrawingCancellerStub::successful(
            CancelDrawingResult {
                cancelled_count: 0,
                cancelled_ids: Vec::new(),
            },
        )));

        let noop = noop_toolbox.cancel_drawing(42, -100).await?;

        assert_eq!(noop.status, TOOL_RESULT_STATUS_NOOP);
        assert_eq!(
            noop.message,
            "Cancellation result: No active or pending image generation jobs were found for this user in this chat. Nothing was cancelled."
        );
        assert_eq!(
            noop.side_effect,
            Some(ToolSideEffect {
                kind: "image_generation_job".to_owned(),
                state: "noop".to_owned(),
                ..ToolSideEffect::default()
            })
        );
        assert_eq!(
            noop.data,
            Some(json!({
                "cancelled_count": 0,
                "cancelled_ids": [],
            }))
        );
        Ok(())
    }

    #[tokio::test]
    async fn app_dialog_toolbox_reports_cancel_drawing_error() -> Result<(), ToolboxError> {
        let toolbox = toolbox(Some(TranslatorStub {
            result: Ok("ignored".to_owned()),
        }))
        .with_drawing_canceller(Arc::new(DrawingCancellerStub::failed("taskman down")));

        let result = toolbox.cancel_drawing(42, -100).await?;

        assert_eq!(result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(result.message, "taskman down");
        assert_eq!(
            result.error,
            Some(ToolError {
                code: "cancel_drawing_failed".to_owned(),
                reason: "taskman down".to_owned(),
                retryable: true,
            })
        );
        Ok(())
    }

    fn taskman_test_job(
        title: &str,
        priority: openplotva_taskman::Priority,
        created: OffsetDateTime,
        user_id: i64,
        chat_id: i64,
        message_id: i32,
    ) -> StatelessJobItem {
        StatelessJobItem {
            title: title.to_owned(),
            created,
            priority,
            processing_timeout_seconds: 0,
            data: JobPayload {
                job_type: JobType::Control,
                telegram_data: Some(TelegramData {
                    chat_id,
                    user_id,
                    message_id,
                    thread_message_id: None,
                    user_full_name: "Alice".to_owned(),
                    chat_title: "Chat".to_owned(),
                }),
                image_data: None,
                music_data: None,
                dialog_data: None,
                control_data: None,
                agent_data: None,
            },
        }
    }

    fn telegram_file_record(
        file_unique_id: &str,
        latest_file_id: &str,
    ) -> openplotva_storage::TelegramFileRecord {
        openplotva_storage::TelegramFileRecord {
            file_unique_id: file_unique_id.to_owned(),
            latest_file_id: latest_file_id.to_owned(),
            media_kind: "image".to_owned(),
            mime_type: None,
            width: None,
            height: None,
            file_size: None,
            first_seen_chat_id: None,
            first_seen_message_id: None,
            last_seen_chat_id: None,
            last_seen_message_id: None,
            last_seen_at: OffsetDateTime::UNIX_EPOCH,
            vision_status: String::new(),
            vision_caption: None,
            vision_model: None,
            vision_latency_ms: None,
            recognition_requested_at: None,
            recognition_completed_at: None,
            asr_status: openplotva_storage::TELEGRAM_FILE_ASR_STATUS_PENDING.to_owned(),
            asr_text: None,
            asr_provider: None,
            asr_model: None,
            asr_latency_ms: None,
            asr_error: None,
            asr_requested_at: None,
            asr_completed_at: None,
            extra: json!({}),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn image_attachment(file_unique_id: &str) -> ChatAttachment {
        ChatAttachment {
            kind: "image".to_owned(),
            file_unique_id: file_unique_id.to_owned(),
            ..ChatAttachment::default()
        }
    }
}
