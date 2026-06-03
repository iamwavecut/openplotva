//! App-level message-to-dialog task scheduling.

use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use carapax::types::{
    Chat as TelegramChat, MessageData as TelegramMessageData, ReplyTo as TelegramReplyTo,
    Update as TelegramUpdate, UpdateType, User as TelegramUser,
};
use openplotva_config::AppConfig;
use openplotva_core::{
    ChatMessageMeta, ChatSettings, MessageSender, SENDER_TYPE_USER, UserSettings,
};
use openplotva_dialog::PROVIDER_AIFARM;
use openplotva_server::ACTION_SEND_TEXT;
use openplotva_taskman::{
    DIALOG_AIFARM_QUEUE_NAME, DialogJobParams, HIGH_PRIORITY, InMemoryTaskQueue, TEXT_QUEUE_NAME,
    new_dialog_job_at,
};
use openplotva_telegram::{
    ChatActionRequest, ChatRef, DispatcherQueue, OutboundBuildError, PhotoMessageRequest,
    PhotoSource, ReplyMessageRef, ReplyParametersPlan, StickerMessageRequest,
    TELEGRAM_PARSE_MODE_HTML, TelegramClient, TelegramOutboundMethod, TelegramOutboundMethodKind,
    TelegramOutboundResponse, TextMessageRequest,
};
use openplotva_updates::{
    FetcherMessageContext, HistoryTextEntry, build_fetcher_message_context,
    build_history_text_entry, build_message_meta, fetcher_message_text, parse_edit_command,
    parse_if_addressed, resolve_draw_prompt_from_message, resolve_message_sender,
    should_handle_addressed_message, should_handle_random_response,
};
use rand::RngExt;
use thiserror::Error;
use time::OffsetDateTime;

use crate::{
    dialog_debounce::InMemoryDialogDebounce,
    dialog_runtime,
    dialog_tools::{
        DrawImageScheduleRejection, DrawImageScheduleRequest, ImageScheduler,
        SongScheduleRejection, SongScheduleRequest, SongScheduleResult, SongScheduler,
    },
    image_jobs::{
        ImageGenerationError, ImageGenerationRequest, ImageGenerationResult, ImageGenerator,
    },
    permissions::{self, ChatPermissionPolicy, ChatPermissionStore},
    rate_limits::{self, ChatRateLimitPolicy, RateLimitStore},
    updates::{UpdateHandler, UpdateHandlerFuture},
    virtual_messages::{
        QueueStickerRequest, QueueTextRequest, VirtualIdFactory, VirtualMessageStore,
        monotonic_virtual_id_factory, queue_sticker_message, queue_text_message_parts,
    },
};

/// Boxed future returned by dialog scheduling effects.
pub type DialogMessageFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;
type DialogMessageShortcutSchedulers<'a, Scheduler> = (
    &'a Scheduler,
    Option<&'a dyn ImageScheduler>,
    Option<&'a dyn SongScheduler>,
    Option<&'a dyn DirectDrawApiEffects>,
);
type DialogMessageShortcutEffects<'a> = (
    Option<&'a dyn ImageScheduler>,
    Option<&'a dyn SongScheduler>,
    Option<&'a dyn DirectSongNoticeEffects>,
    Option<&'a dyn DirectDrawApiEffects>,
);

#[derive(Clone, Debug, PartialEq)]
pub struct DialogMessageSchedule<'payload> {
    pub chat_id: i64,
    pub message_id: i32,
    pub sender_id: i64,
    pub user_full_name: String,
    pub message_text: &'payload str,
    pub original_text: &'payload str,
    pub thread_id: Option<i32>,
    pub meta: &'payload ChatMessageMeta,
    pub title: &'static str,
    pub priority: i32,
    pub queue_name: &'payload str,
    pub max_output_tokens: i32,
    pub processing_timeout_seconds: i32,
}

/// Result of one dialog scheduling effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogMessageScheduleReport {
    pub queue_name: String,
    pub delay: Duration,
    pub replaced: bool,
}

pub trait DialogMessageScheduler {
    type Error: fmt::Display + Send + Sync + 'static;

    fn schedule_dialog_message<'a>(
        &'a self,
        schedule: DialogMessageSchedule<'a>,
    ) -> DialogMessageFuture<'a, DialogMessageScheduleReport, Self::Error>;
}

pub trait RandomDialogSettingsStore {
    type Error: fmt::Display + Send + Sync + 'static;

    fn random_chat_settings<'a>(
        &'a self,
        chat_id: i64,
    ) -> DialogMessageFuture<'a, Option<ChatSettings>, Self::Error>;

    fn random_user_settings<'a>(
        &'a self,
        user_id: i64,
    ) -> DialogMessageFuture<'a, Option<UserSettings>, Self::Error>;
}

impl RandomDialogSettingsStore for openplotva_storage::PostgresChatSettingsStore {
    type Error = openplotva_storage::StorageError;

    fn random_chat_settings<'a>(
        &'a self,
        chat_id: i64,
    ) -> DialogMessageFuture<'a, Option<ChatSettings>, Self::Error> {
        Box::pin(async move { self.get_chat_settings(chat_id).await })
    }

    fn random_user_settings<'a>(
        &'a self,
        user_id: i64,
    ) -> DialogMessageFuture<'a, Option<UserSettings>, Self::Error> {
        Box::pin(async move { self.get_user_settings(user_id).await })
    }
}

pub trait RandomDialogRng {
    fn random_response_roll(&self) -> i32;
    fn obscenifier_roll(&self) -> i32;
    fn obscenify_variant_roll(&self) -> i32;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ThreadRandomDialogRng;

impl RandomDialogRng for ThreadRandomDialogRng {
    fn random_response_roll(&self) -> i32 {
        rand::rng().random_range(0..100)
    }

    fn obscenifier_roll(&self) -> i32 {
        rand::rng().random_range(0..4)
    }

    fn obscenify_variant_roll(&self) -> i32 {
        rand::rng().random_range(0..5)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RandomObscenifiedTextPlan {
    pub message: TextMessageRequest,
    pub reply_to: ReplyMessageRef,
}

pub trait RandomDialogEffects {
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_random_obscenified_text<'a>(
        &'a self,
        plan: RandomObscenifiedTextPlan,
    ) -> DialogMessageFuture<'a, (), Self::Error>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectSongNoticePlan {
    /// Telegram text request fields.
    pub message: TextMessageRequest,
    /// Replied-to source message.
    pub reply_to: ReplyMessageRef,
    pub delete_after: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectDrawFailurePlan {
    pub sticker: StickerMessageRequest,
    /// Replied-to source message for the sticker.
    pub sticker_reply_to: ReplyMessageRef,
    pub sticker_delete_after: Duration,
    pub failure_notice: DirectSongNoticePlan,
    pub restriction_notice: DirectSongNoticePlan,
}

/// Boxed future returned by direct song notice side effects.
pub type DirectSongNoticeFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
/// Boxed future returned by direct draw failure side effects.
pub type DirectDrawFailureFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

pub trait DirectSongNoticeEffects: Send + Sync {
    fn send_direct_song_notice<'a>(
        &'a self,
        plan: DirectSongNoticePlan,
    ) -> DirectSongNoticeFuture<'a>;

    fn send_direct_draw_failure<'a>(
        &'a self,
        _plan: DirectDrawFailurePlan,
    ) -> DirectDrawFailureFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectDrawApiRequest {
    /// Target Telegram chat ID.
    pub chat_id: i64,
    /// Source message ID to reply to.
    pub message_id: i32,
    /// Caller user ID.
    pub user_id: i64,
    /// Caller display name.
    pub user_full_name: String,
    /// Raw shortcut prompt after `%`.
    pub prompt: String,
    /// Forum topic ID.
    pub thread_id: Option<i32>,
    /// Whether the target chat is a forum.
    pub is_forum: bool,
}

/// Result of one direct `%` Draw API send attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectDrawApiResult {
    /// Whether Telegram accepted the generated photo.
    pub sent: bool,
    pub error: Option<String>,
}

/// Structured send failure from the direct `%` Draw API photo path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectDrawApiSendError {
    /// Logged error text.
    pub message: String,
    /// Telegram retry window extracted from a 429 response.
    pub retry_after: Option<Duration>,
    pub permission_error: bool,
}

impl DirectDrawApiSendError {
    fn plain(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retry_after: None,
            permission_error: false,
        }
    }

    fn from_execute(error: carapax::api::ExecuteError) -> Self {
        let retry_after = rate_limits::telegram_retry_after_from_execute_error(&error);
        let permission_error = permissions::telegram_execute_error_is_permission_error(&error);
        Self {
            message: error.to_string(),
            retry_after,
            permission_error,
        }
    }
}

/// Boxed future returned by direct `%` Draw API effects.
pub type DirectDrawApiFuture<'a> = Pin<Box<dyn Future<Output = DirectDrawApiResult> + Send + 'a>>;

pub trait DirectDrawApiEffects: Send + Sync {
    /// Generate one image through draw-api and send it as a direct Telegram photo reply.
    fn send_direct_draw_api<'a>(&'a self, request: DirectDrawApiRequest)
    -> DirectDrawApiFuture<'a>;
}

/// Sender boundary for the direct `%` Draw API photo artifact.
pub trait DirectDrawApiTelegramSender: Send + Sync {
    fn send_direct_draw_api_photo<'a>(
        &'a self,
        request: PhotoMessageRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<carapax::types::Message, DirectDrawApiSendError>>
                + Send
                + 'a,
        >,
    >;
}

impl DirectDrawApiTelegramSender for TelegramClient {
    fn send_direct_draw_api_photo<'a>(
        &'a self,
        request: PhotoMessageRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<carapax::types::Message, DirectDrawApiSendError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let method = openplotva_telegram::build_photo_message_method(&request)
                .map_err(|error| DirectDrawApiSendError::plain(error.to_string()))?;
            let response = openplotva_telegram::execute_telegram_method(
                self,
                TelegramOutboundMethod::from(method),
            )
            .await
            .map_err(DirectDrawApiSendError::from_execute)?;
            match response {
                TelegramOutboundResponse::Message(message) => Ok(*message),
                _ => Err(DirectDrawApiSendError::plain(
                    "sendPhoto returned non-message response",
                )),
            }
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectDrawApiRateLimitReport {
    /// Whether the direct photo send must be skipped before Telegram transport.
    pub rate_limited: bool,
    pub load_error: Option<String>,
}

/// Rate-limit boundary for direct `%` Draw API photo sends.
pub trait DirectDrawApiRateLimitEffects: Send + Sync {
    fn is_direct_draw_api_rate_limited<'a>(
        &'a self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> Pin<Box<dyn Future<Output = DirectDrawApiRateLimitReport> + Send + 'a>>;

    /// Store a Telegram retry window after a 429 send error.
    fn set_direct_draw_api_rate_limit<'a>(
        &'a self,
        chat_id: i64,
        retry_after: Duration,
        now: OffsetDateTime,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>>;
}

impl<S> DirectDrawApiRateLimitEffects for ChatRateLimitPolicy<S>
where
    S: RateLimitStore + Send + Sync,
{
    fn is_direct_draw_api_rate_limited<'a>(
        &'a self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> Pin<Box<dyn Future<Output = DirectDrawApiRateLimitReport> + Send + 'a>> {
        Box::pin(async move {
            let report = self.is_rate_limited_at(chat_id, now).await;
            DirectDrawApiRateLimitReport {
                rate_limited: report.rate_limited,
                load_error: report.load_error,
            }
        })
    }

    fn set_direct_draw_api_rate_limit<'a>(
        &'a self,
        chat_id: i64,
        retry_after: Duration,
        now: OffsetDateTime,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
        Box::pin(async move {
            self.set_rate_limit_at(chat_id, retry_after, now)
                .await
                .save_error
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectDrawApiPermissionReport {
    pub load_error: Option<String>,
    /// Persistent save error ignored after Telegram already rejected the send.
    pub save_error: Option<String>,
}

/// Permission-send-error boundary for direct `%` Draw API photo sends.
pub trait DirectDrawApiPermissionEffects: Send + Sync {
    fn record_direct_draw_api_permission_error<'a>(
        &'a self,
        chat_id: i64,
    ) -> Pin<Box<dyn Future<Output = DirectDrawApiPermissionReport> + Send + 'a>>;
}

impl<S> DirectDrawApiPermissionEffects for ChatPermissionPolicy<S>
where
    S: ChatPermissionStore + Send + Sync,
{
    fn record_direct_draw_api_permission_error<'a>(
        &'a self,
        chat_id: i64,
    ) -> Pin<Box<dyn Future<Output = DirectDrawApiPermissionReport> + Send + 'a>> {
        Box::pin(async move {
            let report = self
                .record_send_permission_error(chat_id, TelegramOutboundMethodKind::SendPhoto)
                .await;
            DirectDrawApiPermissionReport {
                load_error: report.load_error,
                save_error: report.save_error,
            }
        })
    }
}

pub trait DirectDrawApiHistoryStore: Send + Sync {
    fn record_direct_draw_api_history<'a>(
        &'a self,
        entry: HistoryTextEntry,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

impl DirectDrawApiHistoryStore for openplotva_storage::PostgresHistoryStore {
    fn record_direct_draw_api_history<'a>(
        &'a self,
        entry: HistoryTextEntry,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            openplotva_storage::PostgresHistoryStore::upsert_history_entry(
                self,
                openplotva_storage::HistoryEntryUpsert {
                    bucket_day: entry.occurred_at.date(),
                    chat_id: entry.chat_id,
                    thread_id: entry.thread_id,
                    message_id: entry.message_id,
                    entry_id: &entry.entry_id,
                    kind: &entry.kind,
                    role: &entry.role,
                    occurred_at: entry.occurred_at,
                    sender_id: entry.sender_id,
                    payload: &entry.payload,
                },
            )
            .await
            .map_err(|error| error.to_string())
        })
    }
}

/// Runtime direct `%` Draw API effects backed by the draw-api generator and Telegram.
#[derive(Clone)]
pub struct DirectDrawApiRuntimeEffects<Generator, Sender = TelegramClient> {
    generator: Generator,
    sender: Sender,
    history: Option<Arc<dyn DirectDrawApiHistoryStore>>,
    rate_limits: Option<Arc<dyn DirectDrawApiRateLimitEffects>>,
    permissions: Option<Arc<dyn DirectDrawApiPermissionEffects>>,
    bot_id: i64,
}

impl<Generator> DirectDrawApiRuntimeEffects<Generator, TelegramClient> {
    /// Build runtime effects with the real Telegram client.
    #[must_use]
    pub fn new(telegram: TelegramClient, generator: Generator) -> Self {
        Self {
            generator,
            sender: telegram,
            history: None,
            rate_limits: None,
            permissions: None,
            bot_id: 0,
        }
    }
}

impl<Generator, Sender> DirectDrawApiRuntimeEffects<Generator, Sender> {
    /// Build runtime effects with an injectable sender for contract tests.
    #[must_use]
    pub fn with_sender(sender: Sender, generator: Generator) -> Self {
        Self {
            generator,
            sender,
            history: None,
            rate_limits: None,
            permissions: None,
            bot_id: 0,
        }
    }

    #[must_use]
    pub fn with_history_store(
        mut self,
        history: Arc<dyn DirectDrawApiHistoryStore>,
        bot_id: i64,
    ) -> Self {
        self.history = Some(history);
        self.bot_id = bot_id;
        self
    }

    #[must_use]
    pub fn with_send_policies(
        mut self,
        rate_limits: Arc<dyn DirectDrawApiRateLimitEffects>,
        permissions: Arc<dyn DirectDrawApiPermissionEffects>,
    ) -> Self {
        self.rate_limits = Some(rate_limits);
        self.permissions = Some(permissions);
        self
    }
}

impl<Generator, Sender> fmt::Debug for DirectDrawApiRuntimeEffects<Generator, Sender> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectDrawApiRuntimeEffects")
            .finish_non_exhaustive()
    }
}

impl<Generator, Sender> DirectDrawApiEffects for DirectDrawApiRuntimeEffects<Generator, Sender>
where
    Generator: ImageGenerator + Send + Sync,
    Sender: DirectDrawApiTelegramSender,
{
    fn send_direct_draw_api<'a>(
        &'a self,
        request: DirectDrawApiRequest,
    ) -> DirectDrawApiFuture<'a> {
        Box::pin(async move {
            let generation = ImageGenerationRequest {
                chat_id: request.chat_id,
                message_id: request.message_id,
                user_id: request.user_id,
                user_full_name: request.user_full_name.clone(),
                thread_id: request.thread_id,
                prompt: request.prompt.clone(),
                caption_text: request.prompt.clone(),
                ..ImageGenerationRequest::default()
            };
            let result = match self.generator.generate_image(generation).await {
                Ok(result) => result,
                Err(ImageGenerationError::Forbidden) => {
                    return DirectDrawApiResult {
                        sent: false,
                        error: Some("forbidden".to_owned()),
                    };
                }
                Err(ImageGenerationError::Provider(message)) => {
                    return DirectDrawApiResult {
                        sent: false,
                        error: Some(message),
                    };
                }
            };
            let Some(photo) = direct_draw_api_photo_request(&request, &result) else {
                return DirectDrawApiResult {
                    sent: false,
                    error: Some("draw-api returned empty result".to_owned()),
                };
            };
            if let Some(rate_limits) = self.rate_limits.as_ref() {
                let report = rate_limits
                    .is_direct_draw_api_rate_limited(request.chat_id, OffsetDateTime::now_utc())
                    .await;
                if let Some(load_error) = report.load_error.as_deref() {
                    tracing::debug!(
                        chat_id = request.chat_id,
                        %load_error,
                        "failed to load Telegram rate-limit state before direct draw-api photo send"
                    );
                }
                if report.rate_limited {
                    return DirectDrawApiResult {
                        sent: false,
                        error: Some("rate limited".to_owned()),
                    };
                }
            }
            match self.sender.send_direct_draw_api_photo(photo).await {
                Ok(message) => {
                    if let Some(error) = self.record_sent_history(message).await {
                        tracing::debug!(%error, "failed to record direct draw-api sent history");
                    }
                    DirectDrawApiResult {
                        sent: true,
                        error: None,
                    }
                }
                Err(error) => {
                    self.record_direct_send_error(request.chat_id, &error).await;
                    DirectDrawApiResult {
                        sent: false,
                        error: Some(error.message),
                    }
                }
            }
        })
    }
}

impl<Generator, Sender> DirectDrawApiRuntimeEffects<Generator, Sender> {
    async fn record_direct_send_error(&self, chat_id: i64, error: &DirectDrawApiSendError) {
        if let (Some(retry_after), Some(rate_limits)) =
            (error.retry_after, self.rate_limits.as_ref())
            && let Some(save_error) = rate_limits
                .set_direct_draw_api_rate_limit(chat_id, retry_after, OffsetDateTime::now_utc())
                .await
        {
            tracing::warn!(
                chat_id,
                retry_after_seconds = retry_after.as_secs(),
                %save_error,
                "failed to persist Telegram rate-limit state after direct draw-api photo error"
            );
        }
        if error.permission_error
            && let Some(permissions) = self.permissions.as_ref()
        {
            let report = permissions
                .record_direct_draw_api_permission_error(chat_id)
                .await;
            if let Some(load_error) = report.load_error.as_deref() {
                tracing::warn!(
                    chat_id,
                    %load_error,
                    "failed to load chat permission state after direct draw-api photo permission error"
                );
            }
            if let Some(save_error) = report.save_error.as_deref() {
                tracing::warn!(
                    chat_id,
                    %save_error,
                    "failed to persist chat permission state after direct draw-api photo permission error"
                );
            }
        }
    }

    async fn record_sent_history(&self, message: carapax::types::Message) -> Option<String> {
        let history = self.history.as_ref()?;
        let entry = direct_draw_api_sent_history_entry(&message, self.bot_id)?;
        history.record_direct_draw_api_history(entry).await.err()
    }
}

/// Dispatcher-backed random-response effects.
#[derive(Clone)]
pub struct RandomDialogDispatcherEffects<Store> {
    store: Store,
    queue: Arc<DispatcherQueue>,
    next_virtual_id: VirtualIdFactory,
}

impl<Store> RandomDialogDispatcherEffects<Store> {
    /// Build random-response effects backed by the normal outbound dispatcher.
    #[must_use]
    pub fn new(store: Store, queue: Arc<DispatcherQueue>) -> Self {
        Self {
            store,
            queue,
            next_virtual_id: monotonic_virtual_id_factory("random-dialog-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl<Store> fmt::Debug for RandomDialogDispatcherEffects<Store>
where
    Store: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RandomDialogDispatcherEffects")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl<Store> RandomDialogEffects for RandomDialogDispatcherEffects<Store>
where
    Store: VirtualMessageStore + Send + Sync,
{
    type Error = RandomDialogEffectError;

    fn send_random_obscenified_text<'a>(
        &'a self,
        plan: RandomObscenifiedTextPlan,
    ) -> DialogMessageFuture<'a, (), Self::Error> {
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

impl<Store> DirectSongNoticeEffects for RandomDialogDispatcherEffects<Store>
where
    Store: VirtualMessageStore + Send + Sync,
{
    fn send_direct_song_notice<'a>(
        &'a self,
        plan: DirectSongNoticePlan,
    ) -> DirectSongNoticeFuture<'a> {
        Box::pin(async move {
            queue_text_message_parts(
                &self.store,
                &self.queue,
                QueueTextRequest {
                    message: &plan.message,
                    reply_to: Some(&plan.reply_to),
                    immediate_first: true,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: Some(plan.delete_after),
                },
                || (self.next_virtual_id)(),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
        })
    }

    fn send_direct_draw_failure<'a>(
        &'a self,
        plan: DirectDrawFailurePlan,
    ) -> DirectDrawFailureFuture<'a> {
        Box::pin(async move {
            queue_sticker_message(
                &self.store,
                &self.queue,
                QueueStickerRequest {
                    message: &plan.sticker,
                    reply_to: Some(&plan.sticker_reply_to),
                    immediate: true,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: Some(plan.sticker_delete_after),
                },
                || (self.next_virtual_id)(),
            )
            .await
            .map_err(|error| error.to_string())?;
            queue_text_message_parts(
                &self.store,
                &self.queue,
                QueueTextRequest {
                    message: &plan.failure_notice.message,
                    reply_to: Some(&plan.failure_notice.reply_to),
                    immediate_first: true,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: Some(plan.failure_notice.delete_after),
                },
                || (self.next_virtual_id)(),
            )
            .await
            .map_err(|error| error.to_string())?;
            queue_text_message_parts(
                &self.store,
                &self.queue,
                QueueTextRequest {
                    message: &plan.restriction_notice.message,
                    reply_to: Some(&plan.restriction_notice.reply_to),
                    immediate_first: true,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: Some(plan.restriction_notice.delete_after),
                },
                || (self.next_virtual_id)(),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
        })
    }
}

/// Errors from dispatcher-backed random-response side effects.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum RandomDialogEffectError {
    #[error("queue random obscenified text: {0}")]
    Queue(#[from] OutboundBuildError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DialogTypingActionReport {
    SkippedInvalidRequest,
    SkippedPermission,
    /// Telegram accepted `sendChatAction`.
    Sent,
    /// Telegram rejected `sendChatAction`; scheduling still proceeds.
    SendFailed {
        message: String,
    },
}

pub trait DialogTypingActionEffects {
    fn send_dialog_typing_action<'a>(
        &'a self,
        chat_id: i64,
        thread_id: Option<i32>,
    ) -> Pin<Box<dyn Future<Output = DialogTypingActionReport> + Send + 'a>>;
}

/// No-op typing effects used by tests and pure schedulers.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopDialogTypingActionEffects;

impl DialogTypingActionEffects for NoopDialogTypingActionEffects {
    fn send_dialog_typing_action<'a>(
        &'a self,
        _chat_id: i64,
        _thread_id: Option<i32>,
    ) -> Pin<Box<dyn Future<Output = DialogTypingActionReport> + Send + 'a>> {
        Box::pin(async { DialogTypingActionReport::SkippedInvalidRequest })
    }
}

/// Runtime typing effects backed by direct Telegram `sendChatAction`.
#[derive(Clone)]
pub struct DialogTypingActionRuntimeEffects<Store> {
    telegram: openplotva_telegram::TelegramClient,
    permissions: Arc<ChatPermissionPolicy<Store>>,
}

impl<Store> DialogTypingActionRuntimeEffects<Store> {
    #[must_use]
    pub fn new(
        telegram: openplotva_telegram::TelegramClient,
        permissions: Arc<ChatPermissionPolicy<Store>>,
    ) -> Self {
        Self {
            telegram,
            permissions,
        }
    }
}

impl<Store> fmt::Debug for DialogTypingActionRuntimeEffects<Store> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DialogTypingActionRuntimeEffects")
            .finish_non_exhaustive()
    }
}

impl<Store> DialogTypingActionEffects for DialogTypingActionRuntimeEffects<Store>
where
    Store: ChatPermissionStore + Send + Sync,
{
    fn send_dialog_typing_action<'a>(
        &'a self,
        chat_id: i64,
        thread_id: Option<i32>,
    ) -> Pin<Box<dyn Future<Output = DialogTypingActionReport> + Send + 'a>> {
        Box::pin(async move {
            if chat_id == 0 {
                return DialogTypingActionReport::SkippedInvalidRequest;
            }
            let report = self
                .permissions
                .can_perform_action_at(chat_id, None, ACTION_SEND_TEXT, OffsetDateTime::now_utc())
                .await;
            if let Some(load_error) = report.load_error.as_deref() {
                tracing::debug!(
                    chat_id,
                    %load_error,
                    "failed to load Telegram permission state before dialog typing action"
                );
            }
            if !report.allowed {
                return DialogTypingActionReport::SkippedPermission;
            }

            let request = ChatActionRequest {
                chat: ChatRef {
                    id: chat_id,
                    is_forum: thread_id.is_some(),
                },
                message_thread_id: i64::from(thread_id.unwrap_or_default()),
                action: "typing".to_owned(),
            };
            let Ok(method) = openplotva_telegram::build_chat_action_method(&request) else {
                return DialogTypingActionReport::SkippedInvalidRequest;
            };
            match openplotva_telegram::execute_telegram_method(
                &self.telegram,
                TelegramOutboundMethod::from(method),
            )
            .await
            {
                Ok(_) => DialogTypingActionReport::Sent,
                Err(error) => {
                    if permissions::telegram_execute_error_is_permission_error(&error) {
                        let update = self
                            .permissions
                            .record_send_permission_error(
                                chat_id,
                                TelegramOutboundMethodKind::SendChatAction,
                            )
                            .await;
                        if let Some(load_error) = update.load_error.as_deref() {
                            tracing::warn!(
                                chat_id,
                                %load_error,
                                "failed to load chat permission state after dialog typing permission error"
                            );
                        }
                        if let Some(save_error) = update.save_error.as_deref() {
                            tracing::warn!(
                                chat_id,
                                %save_error,
                                "failed to persist chat permission state after dialog typing permission error"
                            );
                        }
                    }
                    DialogTypingActionReport::SendFailed {
                        message: error.to_string(),
                    }
                }
            }
        })
    }
}

/// Concrete dialog scheduler backed by the shared Rust-native task queue and debounce store.
#[derive(Clone)]
pub struct TaskmanDialogMessageScheduler {
    queue: Arc<InMemoryTaskQueue>,
    debounce: Arc<InMemoryDialogDebounce>,
    debounce_delay: Duration,
    typing: Arc<dyn DialogTypingActionEffects + Send + Sync>,
}

impl TaskmanDialogMessageScheduler {
    #[must_use]
    pub fn new(
        queue: Arc<InMemoryTaskQueue>,
        debounce: Arc<InMemoryDialogDebounce>,
        debounce_delay: Duration,
    ) -> Self {
        Self {
            queue,
            debounce,
            debounce_delay,
            typing: Arc::new(NoopDialogTypingActionEffects),
        }
    }

    #[must_use]
    pub fn with_typing_effects(
        mut self,
        typing: Arc<dyn DialogTypingActionEffects + Send + Sync>,
    ) -> Self {
        self.typing = typing;
        self
    }
}

impl fmt::Debug for TaskmanDialogMessageScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskmanDialogMessageScheduler")
            .field("debounce_delay", &self.debounce_delay)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum TaskmanDialogMessageSchedulerError {
    #[error("serialize dialog metadata: {0}")]
    SerializeMeta(#[from] serde_json::Error),
}

impl DialogMessageScheduler for TaskmanDialogMessageScheduler {
    type Error = TaskmanDialogMessageSchedulerError;

    fn schedule_dialog_message<'a>(
        &'a self,
        schedule: DialogMessageSchedule<'a>,
    ) -> DialogMessageFuture<'a, DialogMessageScheduleReport, Self::Error> {
        Box::pin(async move {
            let mut job = new_dialog_job_at(
                DialogJobParams {
                    chat_id: schedule.chat_id,
                    message_id: schedule.message_id,
                    user_id: schedule.sender_id,
                    user_full_name: schedule.user_full_name.clone(),
                    message_text: schedule.message_text.to_owned(),
                    original_text: String::new(),
                    meta: serde_json::Value::Null,
                    max_output_tokens: 0,
                    thread_id: schedule.thread_id,
                },
                OffsetDateTime::now_utc(),
            )
            .with_name(schedule.title)
            .with_priority(schedule.priority)
            .with_processing_timeout_seconds(schedule.processing_timeout_seconds)
            .with_dialog_max_output_tokens(schedule.max_output_tokens);

            if let Some(dialog_data) = job.data.dialog_data.as_mut() {
                dialog_data.original_text = schedule.original_text.trim().to_owned();
                dialog_data.meta = serde_json::to_value(schedule.meta)?;
            }

            let typing_report = self
                .typing
                .send_dialog_typing_action(schedule.chat_id, schedule.thread_id)
                .await;
            if let DialogTypingActionReport::SendFailed { message } = typing_report {
                tracing::debug!(
                    chat_id = schedule.chat_id,
                    message_id = schedule.message_id,
                    %message,
                    "failed to send dialog typing action"
                );
            }

            let report = self.debounce.schedule(
                crate::dialog_debounce::DialogDebounceKey {
                    chat_id: schedule.chat_id,
                    user_id: schedule.sender_id,
                    thread_id: schedule.thread_id.unwrap_or_default().into(),
                },
                schedule.message_id,
                schedule.queue_name,
                job,
                self.queue.as_ref().clone(),
                self.debounce_delay,
            );

            Ok(DialogMessageScheduleReport {
                queue_name: schedule.queue_name.to_owned(),
                delay: report.delay,
                replaced: report.replaced,
            })
        })
    }
}

/// Runtime config for addressed-dialog message scheduling.
#[derive(Clone, Debug, PartialEq)]
pub struct DialogMessageUpdateConfig {
    pub bot_user: TelegramUser,
    pub queue_name: String,
    pub use_aifarm_budgets: bool,
    pub aifarm_max_tokens: i32,
    pub aifarm_random_max_tokens: i32,
    pub aifarm_default_max_tokens: i32,
    pub aifarm_long_max_tokens: i32,
    pub processing_timeout_seconds: i32,
}

impl DialogMessageUpdateConfig {
    #[must_use]
    pub fn from_app_config(config: &AppConfig, bot_user: TelegramUser) -> Self {
        let provider = dialog_runtime::configured_dialog_provider_name(config);
        let use_aifarm_budgets = is_primary_aifarm_dialog_provider(&provider);
        Self {
            bot_user,
            queue_name: if use_aifarm_budgets {
                DIALOG_AIFARM_QUEUE_NAME.to_owned()
            } else {
                TEXT_QUEUE_NAME.to_owned()
            },
            use_aifarm_budgets,
            aifarm_max_tokens: config.llm.dialog.aifarm_max_tokens,
            aifarm_random_max_tokens: config.llm.dialog.aifarm_random_max_tokens,
            aifarm_default_max_tokens: config.llm.dialog.aifarm_default_max_tokens,
            aifarm_long_max_tokens: config.llm.dialog.aifarm_long_max_tokens,
            processing_timeout_seconds: config
                .llm
                .dialog
                .task_timeout_seconds
                .max(300)
                .max(config.llm.history_summary.timeout_seconds),
        }
    }
}

/// Route chosen by the addressed-dialog update handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DialogMessageUpdateRoute {
    Delegated,
    IgnoredInvalidMessage,
    SkippedBotSampling,
    SkippedEmptyDialogTrigger,
    RandomSkippedReactivityOff,
    RandomSkippedGate,
    RandomSkippedUserDisabled,
    RandomSkippedRoll {
        reactivity: i32,
        roll: i32,
    },
    RandomObscenified {
        text: String,
        send_error: Option<String>,
    },
    Scheduled {
        queue_name: String,
        delay: Duration,
        replaced: bool,
    },
    DrawImageScheduled {
        status: String,
        no_reply: bool,
    },
    SongScheduled {
        status: String,
        no_reply: bool,
    },
    DirectDrawApi {
        sent: bool,
        error: Option<String>,
    },
    SongMissingTopic,
    ImageEditMissingPrompt,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DialogMessageUpdateError {
    #[error("schedule dialog message: {message}")]
    Schedule { message: String },
    #[error("schedule draw image: {message}")]
    ScheduleDraw { message: String },
    #[error("schedule song: {message}")]
    ScheduleSong { message: String },
    #[error("downstream update handler: {message}")]
    Downstream { message: String },
}

#[derive(Clone)]
pub struct DialogMessageUpdateHandler<Scheduler, Settings, Effects, Rng, Next> {
    scheduler: Arc<Scheduler>,
    image_scheduler: Option<Arc<dyn ImageScheduler>>,
    song_scheduler: Option<Arc<dyn SongScheduler>>,
    direct_draw_api_effects: Option<Arc<dyn DirectDrawApiEffects>>,
    settings: Arc<Settings>,
    effects: Arc<Effects>,
    rng: Arc<Rng>,
    config: DialogMessageUpdateConfig,
    next: Arc<Next>,
}

impl<Scheduler, Settings, Effects, Rng, Next> fmt::Debug
    for DialogMessageUpdateHandler<Scheduler, Settings, Effects, Rng, Next>
where
    Scheduler: fmt::Debug,
    Settings: fmt::Debug,
    Effects: fmt::Debug,
    Rng: fmt::Debug,
    Next: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DialogMessageUpdateHandler")
            .field("scheduler", &self.scheduler)
            .field(
                "image_scheduler",
                &self.image_scheduler.as_ref().map(|_| "configured"),
            )
            .field(
                "song_scheduler",
                &self.song_scheduler.as_ref().map(|_| "configured"),
            )
            .field(
                "direct_draw_api_effects",
                &self.direct_draw_api_effects.as_ref().map(|_| "configured"),
            )
            .field("settings", &self.settings)
            .field("effects", &self.effects)
            .field("rng", &self.rng)
            .field("config", &self.config)
            .field("next", &self.next)
            .finish()
    }
}

impl<Scheduler, Settings, Effects, Rng, Next>
    DialogMessageUpdateHandler<Scheduler, Settings, Effects, Rng, Next>
{
    pub fn new(
        scheduler: Arc<Scheduler>,
        settings: Arc<Settings>,
        effects: Arc<Effects>,
        rng: Arc<Rng>,
        config: DialogMessageUpdateConfig,
        next: Arc<Next>,
    ) -> Self {
        Self {
            scheduler,
            image_scheduler: None,
            song_scheduler: None,
            direct_draw_api_effects: None,
            settings,
            effects,
            rng,
            config,
            next,
        }
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
    pub fn with_direct_draw_api_effects(mut self, effects: Arc<dyn DirectDrawApiEffects>) -> Self {
        self.direct_draw_api_effects = Some(effects);
        self
    }
}

impl<Scheduler, Settings, Effects, Rng, Next> UpdateHandler
    for DialogMessageUpdateHandler<Scheduler, Settings, Effects, Rng, Next>
where
    Scheduler: DialogMessageScheduler + Send + Sync,
    Settings: RandomDialogSettingsStore + Send + Sync,
    Effects: DirectSongNoticeEffects + RandomDialogEffects + Send + Sync,
    Rng: RandomDialogRng + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = DialogMessageUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_dialog_or_random_message_update_or_else_with_image_and_direct_draw(
                (
                    self.scheduler.as_ref(),
                    self.image_scheduler.as_deref(),
                    self.song_scheduler.as_deref(),
                    self.direct_draw_api_effects.as_deref(),
                ),
                self.settings.as_ref(),
                self.effects.as_ref(),
                self.rng.as_ref(),
                &self.config,
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_dialog_or_random_message_update_or_else<
    Scheduler,
    Settings,
    Effects,
    Rng,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    scheduler: &Scheduler,
    settings: &Settings,
    effects: &Effects,
    rng: &Rng,
    config: &DialogMessageUpdateConfig,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<DialogMessageUpdateRoute, DialogMessageUpdateError>
where
    Scheduler: DialogMessageScheduler + Sync,
    Settings: RandomDialogSettingsStore + Sync,
    Effects: DirectSongNoticeEffects + RandomDialogEffects + Sync,
    Rng: RandomDialogRng + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    handle_dialog_or_random_message_update_or_else_with_image(
        (scheduler, None, None),
        settings,
        effects,
        rng,
        config,
        update,
        handle_other,
    )
    .await
}

pub async fn handle_dialog_or_random_message_update_or_else_with_image<
    Scheduler,
    Settings,
    Effects,
    Rng,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    schedulers: (
        &Scheduler,
        Option<&dyn ImageScheduler>,
        Option<&dyn SongScheduler>,
    ),
    settings: &Settings,
    effects: &Effects,
    rng: &Rng,
    config: &DialogMessageUpdateConfig,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<DialogMessageUpdateRoute, DialogMessageUpdateError>
where
    Scheduler: DialogMessageScheduler + Sync,
    Settings: RandomDialogSettingsStore + Sync,
    Effects: DirectSongNoticeEffects + RandomDialogEffects + Sync,
    Rng: RandomDialogRng + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    handle_dialog_or_random_message_update_or_else_with_image_and_direct_draw(
        (schedulers.0, schedulers.1, schedulers.2, None),
        settings,
        effects,
        rng,
        config,
        update,
        handle_other,
    )
    .await
}

async fn handle_dialog_or_random_message_update_or_else_with_image_and_direct_draw<
    Scheduler,
    Settings,
    Effects,
    Rng,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    schedulers: DialogMessageShortcutSchedulers<'_, Scheduler>,
    settings: &Settings,
    effects: &Effects,
    rng: &Rng,
    config: &DialogMessageUpdateConfig,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<DialogMessageUpdateRoute, DialogMessageUpdateError>
where
    Scheduler: DialogMessageScheduler + Sync,
    Settings: RandomDialogSettingsStore + Sync,
    Effects: DirectSongNoticeEffects + RandomDialogEffects + Sync,
    Rng: RandomDialogRng + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let UpdateType::Message(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| DialogMessageUpdateError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DialogMessageUpdateRoute::Delegated);
    };

    let chat_id: i64 = message.chat.get_id().into();
    let Ok(message_id) = i32::try_from(message.id) else {
        return Ok(DialogMessageUpdateRoute::IgnoredInvalidMessage);
    };
    if chat_id == 0 || message_id == 0 {
        return Ok(DialogMessageUpdateRoute::IgnoredInvalidMessage);
    }
    capture_media_group_image_for_message_and_reply(schedulers.1, message);

    let parsed = parse_if_addressed(message, &config.bot_user);
    let first_word_lower = parsed.first_word.trim().to_lowercase();
    let sender = resolve_message_sender(Some(message));
    let bot_username = config
        .bot_user
        .username
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    if (is_bang_song_shortcut(&first_word_lower)
        || is_song_command_for_bot(message, &first_word_lower, &bot_username)
        || !parsed.is_addressed)
        && let Some(route) = schedule_direct_song_shortcut(
            schedulers.2,
            Some(effects as &dyn DirectSongNoticeEffects),
            DirectSongShortcutRequest {
                message,
                chat_id,
                message_id,
                sender: &sender,
                is_addressed: parsed.is_addressed,
                first_word_lower: &first_word_lower,
                rest_text: &parsed.rest_text,
                bot_username: &bot_username,
            },
        )
        .await?
    {
        return Ok(route);
    }

    if parsed.is_addressed || is_bang_draw_shortcut(&first_word_lower) {
        return handle_dialog_message_update_or_else_with_image_and_song_notices(
            schedulers.0,
            (
                schedulers.1,
                schedulers.2,
                Some(effects as &dyn DirectSongNoticeEffects),
                schedulers.3,
            ),
            config,
            update,
            handle_other,
        )
        .await;
    }

    handle_random_dialog_message(schedulers.0, settings, effects, rng, config, message).await
}

pub async fn handle_dialog_message_update_or_else<Scheduler, HandleFn, HandleFuture, HandleError>(
    scheduler: &Scheduler,
    config: &DialogMessageUpdateConfig,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<DialogMessageUpdateRoute, DialogMessageUpdateError>
where
    Scheduler: DialogMessageScheduler + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    handle_dialog_message_update_or_else_with_image(
        scheduler,
        None,
        None,
        config,
        update,
        handle_other,
    )
    .await
}

pub async fn handle_dialog_message_update_or_else_with_image<
    Scheduler,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    scheduler: &Scheduler,
    image_scheduler: Option<&dyn ImageScheduler>,
    song_scheduler: Option<&dyn SongScheduler>,
    config: &DialogMessageUpdateConfig,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<DialogMessageUpdateRoute, DialogMessageUpdateError>
where
    Scheduler: DialogMessageScheduler + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    handle_dialog_message_update_or_else_with_image_and_song_notices(
        scheduler,
        (image_scheduler, song_scheduler, None, None),
        config,
        update,
        handle_other,
    )
    .await
}

async fn handle_dialog_message_update_or_else_with_image_and_song_notices<
    Scheduler,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    scheduler: &Scheduler,
    shortcuts: DialogMessageShortcutEffects<'_>,
    config: &DialogMessageUpdateConfig,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<DialogMessageUpdateRoute, DialogMessageUpdateError>
where
    Scheduler: DialogMessageScheduler + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let UpdateType::Message(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| DialogMessageUpdateError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DialogMessageUpdateRoute::Delegated);
    };

    let chat_id: i64 = message.chat.get_id().into();
    let Ok(message_id) = i32::try_from(message.id) else {
        return Ok(DialogMessageUpdateRoute::IgnoredInvalidMessage);
    };
    if chat_id == 0 || message_id == 0 {
        return Ok(DialogMessageUpdateRoute::IgnoredInvalidMessage);
    }
    capture_media_group_image_for_message_and_reply(shortcuts.0, message);

    let parsed = parse_if_addressed(message, &config.bot_user);
    let first_word_lower = parsed.first_word.trim().to_lowercase();
    let sender = resolve_message_sender(Some(message));
    if parsed.is_addressed
        && !should_handle_addressed_message(Some(message), &sender, config.bot_user.id.into())
    {
        return Ok(DialogMessageUpdateRoute::SkippedBotSampling);
    }
    let bot_username = config
        .bot_user
        .username
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    if let Some(route) = schedule_direct_song_shortcut(
        shortcuts.1,
        shortcuts.2,
        DirectSongShortcutRequest {
            message,
            chat_id,
            message_id,
            sender: &sender,
            is_addressed: parsed.is_addressed,
            first_word_lower: &first_word_lower,
            rest_text: &parsed.rest_text,
            bot_username: &bot_username,
        },
    )
    .await?
    {
        return Ok(route);
    }
    if parsed.is_addressed && is_direct_draw_api_shortcut(&first_word_lower) {
        let Some(effects) = shortcuts.3 else {
            tracing::debug!(
                chat_id,
                message_id,
                "direct draw-api shortcut skipped because effects are not configured"
            );
            return Ok(DialogMessageUpdateRoute::DirectDrawApi {
                sent: false,
                error: Some("draw api not configured".to_owned()),
            });
        };
        let report = effects
            .send_direct_draw_api(DirectDrawApiRequest {
                chat_id,
                message_id,
                user_id: sender.id,
                user_full_name: message_user_full_name(&sender),
                prompt: parsed.rest_text.clone(),
                thread_id: message
                    .message_thread_id
                    .and_then(|thread_id| i32::try_from(thread_id).ok())
                    .filter(|thread_id| *thread_id != 0),
                is_forum: message.message_thread_id.is_some(),
            })
            .await;
        if let Some(error) = report.error.as_deref() {
            tracing::debug!(
                chat_id,
                message_id,
                %error,
                "direct draw-api shortcut failed"
            );
        }
        return Ok(DialogMessageUpdateRoute::DirectDrawApi {
            sent: report.sent,
            error: report.error,
        });
    }
    if let Some(route) = schedule_direct_image_shortcut(
        shortcuts.0,
        shortcuts.2,
        DirectDrawShortcutRequest {
            message,
            chat_id,
            message_id,
            sender: &sender,
            is_addressed: parsed.is_addressed,
            first_word_lower: &first_word_lower,
            rest_text: &parsed.rest_text,
        },
    )
    .await?
    {
        return Ok(route);
    }

    if !parsed.is_addressed {
        if is_bang_draw_shortcut(&first_word_lower) {
            return Ok(DialogMessageUpdateRoute::DrawImageScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            });
        }
        handle_other(update)
            .await
            .map_err(|error| DialogMessageUpdateError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DialogMessageUpdateRoute::Delegated);
    }

    if should_delegate_react_command_alias(&parsed.first_word) {
        if is_draw_react_alias(&first_word_lower) && shortcuts.0.is_none() {
            return Ok(DialogMessageUpdateRoute::DrawImageScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            });
        }
        handle_other(update)
            .await
            .map_err(|error| DialogMessageUpdateError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DialogMessageUpdateRoute::Delegated);
    }

    if dialog_trigger_text(message, &parsed.first_word, &parsed.rest_text).is_empty() {
        return Ok(DialogMessageUpdateRoute::SkippedEmptyDialogTrigger);
    }

    let context = build_fetcher_message_context(message);
    let thread_id = message
        .message_thread_id
        .and_then(|thread_id| i32::try_from(thread_id).ok())
        .filter(|thread_id| *thread_id != 0);
    let max_output_tokens = if config.use_aifarm_budgets {
        aifarm_dialog_max_output_tokens(
            &context.text,
            &context.meta,
            config.aifarm_max_tokens,
            config.aifarm_default_max_tokens,
            config.aifarm_long_max_tokens,
        )
    } else {
        0
    };

    let report = scheduler
        .schedule_dialog_message(DialogMessageSchedule {
            chat_id,
            message_id,
            sender_id: sender.id,
            user_full_name: message_user_full_name(&sender),
            message_text: &context.text,
            original_text: &context.original_text,
            thread_id,
            meta: &context.meta,
            title: "dialog trigger",
            priority: HIGH_PRIORITY,
            queue_name: &config.queue_name,
            max_output_tokens,
            processing_timeout_seconds: config.processing_timeout_seconds,
        })
        .await
        .map_err(|error| DialogMessageUpdateError::Schedule {
            message: error.to_string(),
        })?;

    Ok(DialogMessageUpdateRoute::Scheduled {
        queue_name: report.queue_name,
        delay: report.delay,
        replaced: report.replaced,
    })
}

struct DirectDrawShortcutRequest<'a> {
    message: &'a carapax::types::Message,
    chat_id: i64,
    message_id: i32,
    sender: &'a MessageSender,
    is_addressed: bool,
    first_word_lower: &'a str,
    rest_text: &'a str,
}

struct DirectSongShortcutRequest<'a> {
    message: &'a carapax::types::Message,
    chat_id: i64,
    message_id: i32,
    sender: &'a MessageSender,
    is_addressed: bool,
    first_word_lower: &'a str,
    rest_text: &'a str,
    bot_username: &'a str,
}

async fn schedule_direct_song_shortcut(
    song_scheduler: Option<&dyn SongScheduler>,
    song_notice_effects: Option<&dyn DirectSongNoticeEffects>,
    request: DirectSongShortcutRequest<'_>,
) -> Result<Option<DialogMessageUpdateRoute>, DialogMessageUpdateError> {
    let Some(raw_topic) = direct_song_shortcut_topic(
        request.message,
        request.is_addressed,
        request.first_word_lower,
        request.rest_text,
        request.bot_username,
    ) else {
        return Ok(None);
    };
    let Some(topic) = resolve_song_topic(request.message, &raw_topic) else {
        send_direct_song_notice(
            song_notice_effects,
            request.message,
            DIRECT_SONG_TOPIC_REQUIRED_NOTICE,
            String::new(),
            Duration::from_secs(120),
        )
        .await;
        return Ok(Some(DialogMessageUpdateRoute::SongMissingTopic));
    };
    let Some(song_scheduler) = song_scheduler else {
        let result = SongScheduleResult {
            status: "not_scheduled".to_owned(),
            message: String::new(),
            no_reply: true,
            queue_notice: None,
            rejection: Some(SongScheduleRejection::ServiceUnavailable),
        };
        send_direct_song_result_notices(song_notice_effects, request.message, &result).await;
        return Ok(Some(DialogMessageUpdateRoute::SongScheduled {
            status: result.status,
            no_reply: result.no_reply,
        }));
    };

    let context = build_fetcher_message_context(request.message);
    let (reference_file_id, reference_file_unique_id) =
        song_audio_reference_from_reply(request.message).unwrap_or_default();
    let thread_id = request
        .message
        .message_thread_id
        .and_then(|thread_id| i32::try_from(thread_id).ok())
        .filter(|thread_id| *thread_id != 0);
    let result = song_scheduler
        .schedule_song(SongScheduleRequest {
            chat_id: request.chat_id,
            thread_id,
            message_id: request.message_id,
            user_id: request.sender.id,
            user_full_name: message_user_full_name(request.sender),
            topic,
            message_text: context.text,
            message_meta: context.meta,
            reference_file_id,
            reference_file_unique_id,
        })
        .await
        .map_err(|error| DialogMessageUpdateError::ScheduleSong {
            message: error.to_string(),
        })?;
    send_direct_song_result_notices(song_notice_effects, request.message, &result).await;

    Ok(Some(DialogMessageUpdateRoute::SongScheduled {
        status: result.status,
        no_reply: result.no_reply,
    }))
}

const DIRECT_SONG_TOPIC_REQUIRED_NOTICE: &str =
    "Укажите тему: /song <тема> или используйте команду в ответ на сообщение с текстом.";
const DIRECT_SONG_SERVICE_UNAVAILABLE_NOTICE: &str =
    "Сервис генерации музыки временно недоступен. Попробуйте позже.";
const DIRECT_SONG_AUDIO_NOT_ALLOWED_NOTICE: &str = "Я не могу отправлять аудио в этот чат.";
const DIRECT_SONG_EMPTY_TOPIC_NOTICE: &str = "Не удалось определить тему песни.";
const DIRECT_SONG_VIP_ONLY_NOTICE: &str = "Функция генерации музыки доступна только для <a href='https://t.me/PlotvoBot?start=vip'>VIP-пользователей</a>.";
const DIRECT_SONG_ACTIVE_LIMIT_NOTICE: &str =
    "У вас уже 2 активных музыкальных задач (лимит VIP). Дождитесь завершения и попробуйте снова.";

async fn send_direct_song_result_notices(
    effects: Option<&dyn DirectSongNoticeEffects>,
    message: &carapax::types::Message,
    result: &SongScheduleResult,
) {
    if let Some(rejection) = result.rejection {
        let (text, render_as, delete_after) = match rejection {
            SongScheduleRejection::ServiceUnavailable => (
                DIRECT_SONG_SERVICE_UNAVAILABLE_NOTICE,
                String::new(),
                Duration::from_secs(120),
            ),
            SongScheduleRejection::AudioNotAllowed => (
                DIRECT_SONG_AUDIO_NOT_ALLOWED_NOTICE,
                String::new(),
                Duration::from_secs(120),
            ),
            SongScheduleRejection::VipOnly => (
                DIRECT_SONG_VIP_ONLY_NOTICE,
                TELEGRAM_PARSE_MODE_HTML.to_owned(),
                Duration::from_secs(60),
            ),
            SongScheduleRejection::EmptyTopic => (
                DIRECT_SONG_EMPTY_TOPIC_NOTICE,
                String::new(),
                Duration::from_secs(120),
            ),
            SongScheduleRejection::ActiveLimit => (
                DIRECT_SONG_ACTIVE_LIMIT_NOTICE,
                String::new(),
                Duration::from_secs(60),
            ),
        };
        send_direct_song_notice(effects, message, text, render_as, delete_after).await;
        return;
    }

    if let Some(notice) = result.queue_notice.as_ref() {
        let text = format!(
            "Песня добавлена в очередь. Перед вами {} задач. Примерное ожидание: {}",
            notice.position, notice.estimated_wait
        );
        send_direct_song_notice(
            effects,
            message,
            &text,
            String::new(),
            Duration::from_secs(300),
        )
        .await;
    }
}

async fn send_direct_song_notice(
    effects: Option<&dyn DirectSongNoticeEffects>,
    message: &carapax::types::Message,
    text: &str,
    render_as: String,
    delete_after: Duration,
) {
    let Some(effects) = effects else {
        return;
    };
    if let Err(error) = effects
        .send_direct_song_notice(direct_song_notice_plan(
            message,
            text.to_owned(),
            render_as,
            delete_after,
        ))
        .await
    {
        tracing::debug!(%error, "failed to send direct song notice");
    }
}

fn direct_song_shortcut_topic(
    message: &carapax::types::Message,
    is_addressed: bool,
    first_word_lower: &str,
    rest_text: &str,
    bot_username: &str,
) -> Option<String> {
    let word = first_word_lower.trim();
    if is_bang_song_shortcut(word) {
        return Some(rest_text.trim().to_owned());
    }
    if is_song_command_for_bot(message, word, bot_username) {
        return Some(rest_text.trim().to_owned());
    }
    if is_addressed && is_plain_song_alias(word) {
        return Some(rest_text.trim().to_owned());
    }
    None
}

fn resolve_song_topic(message: &carapax::types::Message, raw_topic: &str) -> Option<String> {
    let topic = raw_topic.trim();
    if !topic.is_empty() {
        return Some(topic.to_owned());
    }

    let Some(TelegramReplyTo::Message(reply)) = message.reply_to.as_ref() else {
        return None;
    };
    song_topic_from_reply(reply)
}

fn song_topic_from_reply(message: &carapax::types::Message) -> Option<String> {
    if let Some(text) = message.get_text()
        && let Some(topic) = trimmed_non_empty(text.as_ref())
    {
        return Some(topic);
    }

    match &message.data {
        TelegramMessageData::Audio(audio) => {
            let audio_title = format!(
                "{} {}",
                audio.data.performer.as_deref().unwrap_or_default(),
                audio.data.title.as_deref().unwrap_or_default()
            );
            trimmed_non_empty(&audio_title)
                .or_else(|| audio.data.file_name.as_deref().and_then(trimmed_non_empty))
        }
        TelegramMessageData::Document(document) => document
            .data
            .file_name
            .as_deref()
            .and_then(trimmed_non_empty),
        _ => None,
    }
}

fn trimmed_non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn song_audio_reference_from_reply(message: &carapax::types::Message) -> Option<(String, String)> {
    let Some(TelegramReplyTo::Message(reply)) = message.reply_to.as_ref() else {
        return None;
    };
    extract_song_audio_reference(reply)
}

fn extract_song_audio_reference(message: &carapax::types::Message) -> Option<(String, String)> {
    match &message.data {
        TelegramMessageData::Audio(audio) => {
            file_reference(&audio.data.file_id, &audio.data.file_unique_id)
        }
        TelegramMessageData::Voice(voice) => {
            file_reference(&voice.data.file_id, &voice.data.file_unique_id)
        }
        TelegramMessageData::Document(document)
            if document
                .data
                .mime_type
                .as_deref()
                .is_some_and(mime_is_audio) =>
        {
            file_reference(&document.data.file_id, &document.data.file_unique_id)
        }
        _ => None,
    }
}

fn file_reference(file_id: &str, file_unique_id: &str) -> Option<(String, String)> {
    let file_id = file_id.trim();
    let file_unique_id = file_unique_id.trim();
    (!file_id.is_empty() && !file_unique_id.is_empty())
        .then(|| (file_id.to_owned(), file_unique_id.to_owned()))
}

fn mime_is_audio(mime: &str) -> bool {
    mime.trim()
        .get(.."audio/".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("audio/"))
}

fn is_song_command_for_bot(
    message: &carapax::types::Message,
    first_word_lower: &str,
    bot_username: &str,
) -> bool {
    let Some(command) = slash_command_parts(first_word_lower) else {
        return false;
    };
    if command.0 != "song" {
        return false;
    }
    if telegram_chat_is_private(&message.chat) {
        return true;
    }
    command
        .1
        .is_some_and(|target| target.eq_ignore_ascii_case(bot_username))
}

fn slash_command_parts(word: &str) -> Option<(&str, Option<&str>)> {
    let command = word.strip_prefix('/')?;
    let (name, target) = match command.split_once('@') {
        Some((name, target)) => (name, Some(target)),
        None => (command, None),
    };
    Some((name, target))
}

fn is_plain_song_alias(first_word: &str) -> bool {
    matches!(first_word.trim(), "song" | "песня")
}

fn is_bang_song_shortcut(first_word: &str) -> bool {
    matches!(first_word.trim(), "!song" | "!песня")
}

fn telegram_chat_is_private(chat: &TelegramChat) -> bool {
    matches!(chat, TelegramChat::Private(_))
}

fn is_direct_draw_api_shortcut(first_word: &str) -> bool {
    first_word.trim() == "%"
}

fn direct_draw_api_photo_request(
    request: &DirectDrawApiRequest,
    result: &ImageGenerationResult,
) -> Option<PhotoMessageRequest> {
    let photo = result
        .image_bytes
        .first()
        .cloned()
        .map(|bytes| PhotoSource::Bytes {
            file_name: "image.png".to_owned(),
            bytes,
        })
        .or_else(|| {
            if !result.image_url.is_empty() {
                return Some(PhotoSource::Url(result.image_url.clone()));
            }
            result.image_urls.first().cloned().map(PhotoSource::Url)
        })?;
    Some(PhotoMessageRequest {
        chat: ChatRef {
            id: request.chat_id,
            is_forum: request.is_forum,
        },
        message_thread_id: if request.is_forum {
            i64::from(request.thread_id.unwrap_or_default())
        } else {
            0
        },
        disable_notification: false,
        photo,
        caption: String::new(),
        render_as: String::new(),
        has_spoiler: false,
        reply_parameters: Some(ReplyParametersPlan {
            message_id: i64::from(request.message_id),
            chat_id: request.chat_id,
            allow_sending_without_reply: true,
        }),
    })
}

fn direct_draw_api_sent_history_entry(
    message: &carapax::types::Message,
    bot_id: i64,
) -> Option<HistoryTextEntry> {
    let sender = resolve_message_sender(Some(message));
    let meta = build_message_meta(Some(message), sender, &[], "");
    let original_text = fetcher_message_text(message).trim().to_owned();
    build_history_text_entry(message, &original_text, meta, bot_id)
}

async fn schedule_direct_image_shortcut(
    image_scheduler: Option<&dyn ImageScheduler>,
    notice_effects: Option<&dyn DirectSongNoticeEffects>,
    request: DirectDrawShortcutRequest<'_>,
) -> Result<Option<DialogMessageUpdateRoute>, DialogMessageUpdateError> {
    let control_context = build_fetcher_message_context(request.message);
    let reply_context = reply_image_context(request.message);
    let current_has_image = context_has_editable_image(&control_context);
    let reply_has_image = reply_context
        .as_ref()
        .is_some_and(context_has_editable_image);
    let edit_target = if current_has_image {
        Some((&control_context, true))
    } else if reply_has_image {
        reply_context.as_ref().map(|context| (context, false))
    } else {
        None
    };
    let Some(shortcut) = direct_image_shortcut(
        request.message,
        request.is_addressed,
        request.first_word_lower,
        request.rest_text,
        edit_target.is_some(),
    ) else {
        return Ok(None);
    };
    let Some(image_scheduler) = image_scheduler else {
        return Ok(None);
    };
    let has_edit_target = edit_target.is_some();
    let draw_edit_requires_vip_preflight =
        matches!(&shortcut, DirectImageShortcut::Draw(_)) && has_edit_target;
    let edit_command_requires_vip_before_file_resolution =
        matches!(&shortcut, DirectImageShortcut::Edit(_));

    let (prompt, attachments, edit_media_group_id) = match shortcut {
        DirectImageShortcut::Draw(prompt) => {
            if let Some((image_context, target_is_current)) = edit_target {
                let prompt = resolve_direct_image_edit_prompt(&prompt, image_context);
                (
                    prompt,
                    image_context.meta.attachments.clone(),
                    if target_is_current {
                        message_media_group_id(request.message)
                    } else {
                        String::new()
                    },
                )
            } else {
                (prompt, control_context.meta.attachments, String::new())
            }
        }
        DirectImageShortcut::Edit(prompt) => {
            let Some((image_context, target_is_current)) = edit_target else {
                return Ok(None);
            };
            (
                resolve_direct_image_edit_prompt(&prompt, image_context),
                image_context.meta.attachments.clone(),
                if target_is_current {
                    message_media_group_id(request.message)
                } else {
                    String::new()
                },
            )
        }
    };

    if has_edit_target
        && let Some(rejection) = image_scheduler
            .direct_draw_image_rejection(request.chat_id)
            .await
    {
        send_direct_draw_failure_notice(notice_effects, request.message, rejection).await;
        return Ok(Some(DialogMessageUpdateRoute::DrawImageScheduled {
            status: "not_scheduled".to_owned(),
            no_reply: true,
        }));
    }

    if draw_edit_requires_vip_preflight
        && matches!(
            image_scheduler
                .direct_image_edit_vip_status(request.sender.id)
                .await,
            Some(false)
        )
    {
        send_direct_image_vip_only_notice(notice_effects, request.message).await;
        return Ok(Some(DialogMessageUpdateRoute::DrawImageScheduled {
            status: "not_scheduled".to_owned(),
            no_reply: true,
        }));
    }

    if prompt.trim().is_empty() {
        send_direct_image_notice(
            notice_effects,
            request.message,
            IMAGE_EDIT_MISSING_PROMPT_TEXT,
            String::new(),
            Duration::from_secs(120),
        )
        .await;
        return Ok(Some(DialogMessageUpdateRoute::ImageEditMissingPrompt));
    }

    if edit_command_requires_vip_before_file_resolution
        && matches!(
            image_scheduler
                .direct_image_edit_vip_status(request.sender.id)
                .await,
            Some(false)
        )
    {
        send_direct_image_vip_only_notice(notice_effects, request.message).await;
        return Ok(Some(DialogMessageUpdateRoute::DrawImageScheduled {
            status: "not_scheduled".to_owned(),
            no_reply: true,
        }));
    }

    let thread_id = request
        .message
        .message_thread_id
        .and_then(|thread_id| i32::try_from(thread_id).ok())
        .filter(|thread_id| *thread_id != 0);
    let prompt = prompt.trim().to_owned();
    let result = image_scheduler
        .schedule_image(DrawImageScheduleRequest {
            chat_id: request.chat_id,
            thread_id,
            message_id: request.message_id,
            user_id: request.sender.id,
            user_full_name: message_user_full_name(request.sender),
            prompt: prompt.clone(),
            message_text: control_context.text,
            attachments,
            edit_media_group_id,
            negative_prompt: String::new(),
            aspect_ratio: String::new(),
            seed: String::new(),
            original_prompt: prompt,
        })
        .await
        .map_err(|error| DialogMessageUpdateError::ScheduleDraw {
            message: error.to_string(),
        })?;

    match result.rejection {
        Some(DrawImageScheduleRejection::VipOnly) => {
            send_direct_image_vip_only_notice(notice_effects, request.message).await;
        }
        Some(DrawImageScheduleRejection::RateLimited) => {
            send_direct_image_notice(
                notice_effects,
                request.message,
                &result.message,
                String::new(),
                Duration::from_secs(60),
            )
            .await;
        }
        Some(
            rejection @ (DrawImageScheduleRejection::DrawDisabled
            | DrawImageScheduleRejection::ImageNotAllowed),
        ) => {
            send_direct_draw_failure_notice(notice_effects, request.message, rejection).await;
        }
        None => {}
    }

    Ok(Some(DialogMessageUpdateRoute::DrawImageScheduled {
        status: result.status,
        no_reply: result.no_reply,
    }))
}

const IMAGE_EDIT_MISSING_PROMPT_TEXT: &str = "Нужно указать, что изменить в изображении.";
const DIRECT_IMAGE_EDIT_VIP_ONLY_NOTICE: &str = "Функция редактирования изображений доступна только для <a href='https://t.me/PlotvoBot?start=vip'>VIP-пользователей</a>.";
const DIRECT_DRAW_FAILURE_TEXT: &str =
    "Я не смогла написать картину, создать шедевр, потому-что у меня лапки (копытца-плавники).";
const DIRECT_DRAW_DISABLED_NOTICE: &str =
    "Извините, функция рисования отключена в настройках чата.";
const DIRECT_IMAGE_NOT_ALLOWED_NOTICE: &str = "Я не могу отправлять изображения в этот чат.";

async fn send_direct_draw_failure_notice(
    effects: Option<&dyn DirectSongNoticeEffects>,
    message: &carapax::types::Message,
    rejection: DrawImageScheduleRejection,
) {
    let Some(effects) = effects else {
        return;
    };
    let Some((restriction_text, restriction_delete_after)) =
        draw_failure_restriction_notice(rejection)
    else {
        return;
    };
    if let Err(error) = effects
        .send_direct_draw_failure(direct_draw_failure_plan(
            message,
            restriction_text.to_owned(),
            restriction_delete_after,
        ))
        .await
    {
        tracing::debug!(%error, "failed to send direct draw failure notice");
    }
}

fn draw_failure_restriction_notice(
    rejection: DrawImageScheduleRejection,
) -> Option<(&'static str, Duration)> {
    match rejection {
        DrawImageScheduleRejection::DrawDisabled => {
            Some((DIRECT_DRAW_DISABLED_NOTICE, Duration::from_secs(60)))
        }
        DrawImageScheduleRejection::ImageNotAllowed => {
            Some((DIRECT_IMAGE_NOT_ALLOWED_NOTICE, Duration::from_secs(120)))
        }
        DrawImageScheduleRejection::VipOnly | DrawImageScheduleRejection::RateLimited => None,
    }
}

async fn send_direct_image_vip_only_notice(
    effects: Option<&dyn DirectSongNoticeEffects>,
    message: &carapax::types::Message,
) {
    send_direct_image_notice(
        effects,
        message,
        DIRECT_IMAGE_EDIT_VIP_ONLY_NOTICE,
        "HTML".to_owned(),
        Duration::from_secs(60),
    )
    .await;
}

async fn send_direct_image_notice(
    effects: Option<&dyn DirectSongNoticeEffects>,
    message: &carapax::types::Message,
    text: &str,
    parse_mode: String,
    delete_after: Duration,
) {
    send_direct_song_notice(effects, message, text, parse_mode, delete_after).await;
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DirectImageShortcut {
    Draw(String),
    Edit(String),
}

fn direct_image_shortcut(
    message: &carapax::types::Message,
    is_addressed: bool,
    first_word_lower: &str,
    rest_text: &str,
    has_edit_target: bool,
) -> Option<DirectImageShortcut> {
    let edit_text = if rest_text.trim().is_empty() {
        first_word_lower.to_owned()
    } else {
        format!("{first_word_lower} {rest_text}")
    };
    if has_edit_target && let Some(prompt) = parse_edit_command(&edit_text) {
        return Some(DirectImageShortcut::Edit(prompt));
    }
    if is_bang_draw_shortcut(first_word_lower) {
        return Some(DirectImageShortcut::Draw(rest_text.trim().to_owned()));
    }
    is_addressed
        .then(|| resolve_draw_prompt_from_message(message, first_word_lower, rest_text))?
        .map(DirectImageShortcut::Draw)
}

fn reply_image_context(message: &carapax::types::Message) -> Option<FetcherMessageContext> {
    let Some(TelegramReplyTo::Message(reply)) = message.reply_to.as_ref() else {
        return None;
    };
    Some(build_fetcher_message_context(reply))
}

fn capture_media_group_image_for_message_and_reply(
    image_scheduler: Option<&dyn ImageScheduler>,
    message: &carapax::types::Message,
) {
    capture_media_group_image_for_message(image_scheduler, message);
    if let Some(TelegramReplyTo::Message(reply)) = message.reply_to.as_ref() {
        capture_media_group_image_for_message(image_scheduler, reply);
    }
}

fn capture_media_group_image_for_message(
    image_scheduler: Option<&dyn ImageScheduler>,
    message: &carapax::types::Message,
) {
    let Some(image_scheduler) = image_scheduler else {
        return;
    };
    let media_group_id = message_media_group_id(message);
    if media_group_id.is_empty() {
        return;
    }
    let Ok(message_id) = i32::try_from(message.id) else {
        return;
    };
    let context = build_fetcher_message_context(message);
    image_scheduler.capture_media_group_image(
        &media_group_id,
        message_id,
        &context.meta.attachments,
    );
}

fn message_media_group_id(message: &carapax::types::Message) -> String {
    message
        .media_group_id
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn context_has_editable_image(context: &FetcherMessageContext) -> bool {
    context.meta.attachments.iter().any(|attachment| {
        attachment.source.trim() == "message"
            && attachment.kind.trim() == "image"
            && !attachment.file_unique_id.trim().is_empty()
    })
}

fn resolve_direct_image_edit_prompt(prompt: &str, image_context: &FetcherMessageContext) -> String {
    let prompt = prompt.trim();
    if !prompt.is_empty() {
        return prompt.to_owned();
    }

    let caption = image_context.text.trim();
    if caption.is_empty() {
        return String::new();
    }
    parse_edit_command(caption)
        .unwrap_or_else(|| caption.to_owned())
        .trim()
        .to_owned()
}

async fn handle_random_dialog_message<Scheduler, Settings, Effects, Rng>(
    scheduler: &Scheduler,
    settings: &Settings,
    effects: &Effects,
    rng: &Rng,
    config: &DialogMessageUpdateConfig,
    message: &carapax::types::Message,
) -> Result<DialogMessageUpdateRoute, DialogMessageUpdateError>
where
    Scheduler: DialogMessageScheduler + Sync,
    Settings: RandomDialogSettingsStore + Sync,
    Effects: RandomDialogEffects + Sync,
    Rng: RandomDialogRng + Sync,
{
    let chat_id: i64 = message.chat.get_id().into();
    let message_id =
        i32::try_from(message.id).map_err(|error| DialogMessageUpdateError::Schedule {
            message: error.to_string(),
        })?;
    let settings_row = settings
        .random_chat_settings(chat_id)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| ChatSettings::defaults(chat_id));
    if settings_row.reactivity_percentage == GO_SETTINGS_REACTIVITY_OFF {
        return Ok(DialogMessageUpdateRoute::RandomSkippedReactivityOff);
    }

    let sender = resolve_message_sender(Some(message));
    let context = build_fetcher_message_context(message);
    if !should_handle_random_response(Some(message), &context.original_text, &sender) {
        return Ok(DialogMessageUpdateRoute::RandomSkippedGate);
    }
    if random_reactivity_disabled_for_user(settings, &sender).await {
        return Ok(DialogMessageUpdateRoute::RandomSkippedUserDisabled);
    }

    let roll = rng.random_response_roll();
    if !random_response_triggers(settings_row.reactivity_percentage, roll) {
        return Ok(DialogMessageUpdateRoute::RandomSkippedRoll {
            reactivity: settings_row.reactivity_percentage,
            roll,
        });
    }

    if settings_row.enable_obscenifier && rng.obscenifier_roll() == 0 {
        let text = obscenify_string(&context.text, rng.obscenify_variant_roll());
        let send_error = effects
            .send_random_obscenified_text(random_obscenified_text_plan(message, text.clone()))
            .await
            .err()
            .map(|error| error.to_string());
        return Ok(DialogMessageUpdateRoute::RandomObscenified { text, send_error });
    }

    let thread_id = message
        .message_thread_id
        .and_then(|thread_id| i32::try_from(thread_id).ok())
        .filter(|thread_id| *thread_id != 0);
    let max_output_tokens = if config.use_aifarm_budgets {
        config.aifarm_random_max_tokens
    } else {
        0
    };
    let report = scheduler
        .schedule_dialog_message(DialogMessageSchedule {
            chat_id,
            message_id,
            sender_id: sender.id,
            user_full_name: message_user_full_name(&sender),
            message_text: &context.text,
            original_text: &context.original_text,
            thread_id,
            meta: &context.meta,
            title: "dialog (random)",
            priority: HIGH_PRIORITY,
            queue_name: &config.queue_name,
            max_output_tokens,
            processing_timeout_seconds: config.processing_timeout_seconds,
        })
        .await
        .map_err(|error| DialogMessageUpdateError::Schedule {
            message: error.to_string(),
        })?;

    Ok(DialogMessageUpdateRoute::Scheduled {
        queue_name: report.queue_name,
        delay: report.delay,
        replaced: report.replaced,
    })
}

async fn random_reactivity_disabled_for_user<Settings>(
    settings: &Settings,
    sender: &MessageSender,
) -> bool
where
    Settings: RandomDialogSettingsStore + Sync,
{
    if sender.sender_type != SENDER_TYPE_USER || sender.is_bot || sender.id == 0 {
        return false;
    }
    settings
        .random_user_settings(sender.id)
        .await
        .ok()
        .flatten()
        .is_some_and(|settings| settings.disable_random_reactivity)
}

fn should_delegate_react_command_alias(first_word: &str) -> bool {
    let first_word = first_word.trim().to_lowercase();
    if first_word.is_empty() {
        return false;
    }
    matches!(
        first_word.as_str(),
        "song"
            | "песня"
            | "!song"
            | "!песня"
            | "нарисуй"
            | "draw"
            | "рисуй"
            | "переведи"
            | "перевод"
            | "translate"
    ) || is_bang_draw_shortcut(&first_word)
}

fn is_draw_react_alias(first_word: &str) -> bool {
    matches!(
        first_word.trim(),
        "нарисуй" | "draw" | "рисуй" | "!рис" | "!draw"
    )
}

fn is_bang_draw_shortcut(first_word: &str) -> bool {
    matches!(first_word.trim(), "!рис" | "!draw")
}

fn dialog_trigger_text(
    message: &carapax::types::Message,
    first_word_lower: &str,
    rest_text: &str,
) -> String {
    if !rest_text.is_empty() {
        return rest_text.to_owned();
    }
    if !first_word_lower.is_empty() {
        return first_word_lower.to_owned();
    }
    fetcher_message_text(message)
}

fn message_user_full_name(sender: &MessageSender) -> String {
    let full_name = sender.full_name.trim();
    if !full_name.is_empty() {
        return full_name.to_owned();
    }
    let username = sender.username.trim();
    if !username.is_empty() {
        return username.to_owned();
    }
    if sender.id != 0 {
        return sender.id.to_string();
    }
    "Telegram".to_owned()
}

fn aifarm_dialog_max_output_tokens(
    text: &str,
    meta: &ChatMessageMeta,
    max_tokens: i32,
    default_max_tokens: i32,
    long_max_tokens: i32,
) -> i32 {
    if is_explicit_full_dialog_request(text) {
        return max_tokens;
    }
    if is_long_dialog_request(text, meta) {
        return long_max_tokens;
    }
    default_max_tokens
}

fn random_response_triggers(reactivity_percentage: i32, roll: i32) -> bool {
    if reactivity_percentage == GO_SETTINGS_REACTIVITY_OFF {
        return false;
    }
    roll > 100 - reactivity_percentage
}

fn random_obscenified_text_plan(
    message: &carapax::types::Message,
    text: String,
) -> RandomObscenifiedTextPlan {
    let chat = ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    };
    RandomObscenifiedTextPlan {
        message: TextMessageRequest {
            chat: Some(chat),
            message_thread_id: message.message_thread_id.unwrap_or_default(),
            disable_notification: false,
            allow_sending_without_reply: None,
            text,
            render_as: String::new(),
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

fn direct_song_notice_plan(
    message: &carapax::types::Message,
    text: String,
    render_as: String,
    delete_after: Duration,
) -> DirectSongNoticePlan {
    let chat = ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    };
    DirectSongNoticePlan {
        message: TextMessageRequest {
            chat: Some(chat),
            message_thread_id: message.message_thread_id.unwrap_or_default(),
            disable_notification: false,
            allow_sending_without_reply: None,
            text,
            render_as,
            reply_markup: None,
        },
        reply_to: ReplyMessageRef {
            message_id: message.id,
            chat,
            is_topic_message: message.message_thread_id.is_some(),
            message_thread_id: message.message_thread_id.unwrap_or_default(),
        },
        delete_after,
    }
}

fn direct_draw_failure_plan(
    message: &carapax::types::Message,
    restriction_text: String,
    restriction_delete_after: Duration,
) -> DirectDrawFailurePlan {
    let chat = ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    };
    let reply_to = ReplyMessageRef {
        message_id: message.id,
        chat,
        is_topic_message: message.message_thread_id.is_some(),
        message_thread_id: message.message_thread_id.unwrap_or_default(),
    };
    DirectDrawFailurePlan {
        sticker: StickerMessageRequest {
            chat: None,
            message_thread_id: 0,
            disable_notification: true,
            file_id: crate::image_jobs::STICKER_DOWN_FILE_ID.to_owned(),
        },
        sticker_reply_to: reply_to,
        sticker_delete_after: Duration::from_secs(60),
        failure_notice: DirectSongNoticePlan {
            message: TextMessageRequest {
                chat: Some(chat),
                message_thread_id: message.message_thread_id.unwrap_or_default(),
                disable_notification: false,
                allow_sending_without_reply: None,
                text: DIRECT_DRAW_FAILURE_TEXT.to_owned(),
                render_as: String::new(),
                reply_markup: None,
            },
            reply_to,
            delete_after: Duration::from_secs(60),
        },
        restriction_notice: DirectSongNoticePlan {
            message: TextMessageRequest {
                chat: Some(chat),
                message_thread_id: message.message_thread_id.unwrap_or_default(),
                disable_notification: false,
                allow_sending_without_reply: None,
                text: restriction_text,
                render_as: String::new(),
                reply_markup: None,
            },
            reply_to,
            delete_after: restriction_delete_after,
        },
    }
}

fn obscenify_string(text: &str, variant_roll: i32) -> String {
    let last = text.rsplit_once(' ').map_or(text, |(_, last)| last);
    let word = last.replace('?', "!");
    let Some((vowel, suffix)) = first_obscenify_vowel_with_suffix(&word) else {
        return String::new();
    };

    let mut prefix = "ху".to_owned();
    let mut harden = true;
    match variant_roll {
        0 => harden = false,
        1 => prefix = "жоп".to_owned(),
        2 => prefix = format!("{}-шм", last.trim_end_matches(['!', '?', ',', ';', '.'])),
        3 => prefix = "пизд".to_owned(),
        4 => prefix = "шм".to_owned(),
        _ => {}
    }

    format!(
        "{prefix}{}{} ",
        transform_obscenify_vowel(vowel, harden),
        suffix
    )
}

fn first_obscenify_vowel_with_suffix(value: &str) -> Option<(&str, &str)> {
    let (index, _) = value
        .char_indices()
        .find(|(_, ch)| GO_OBSCENIFY_VOWELS.contains(*ch))?;
    let next = index + value[index..].chars().next()?.len_utf8();
    (next < value.len()).then_some((&value[index..next], &value[next..]))
}

fn transform_obscenify_vowel(vowel: &str, harden: bool) -> &str {
    if harden {
        match vowel {
            "я" => "а",
            "ё" => "о",
            "ю" => "у",
            other => other,
        }
    } else {
        match vowel {
            "а" => "я",
            "о" => "ё",
            "э" => "е",
            "у" => "ю",
            "ы" => "и",
            other => other,
        }
    }
}

fn is_explicit_full_dialog_request(text: &str) -> bool {
    contains_dialog_phrase_fold(text, EXPLICIT_FULL_DIALOG_PHRASES)
}

fn is_long_dialog_request(text: &str, meta: &ChatMessageMeta) -> bool {
    if text.chars().count() > 1200
        || !meta.attachments.is_empty()
        || !meta.vision_description.trim().is_empty()
    {
        return true;
    }
    contains_dialog_phrase_fold(text, LONG_DIALOG_PHRASES)
}

fn contains_dialog_phrase_fold(text: &str, phrases: &[&str]) -> bool {
    let text = text.trim();
    if text.is_empty() {
        return false;
    }
    let lower = text.to_lowercase();
    phrases.iter().any(|phrase| lower.contains(phrase))
}

fn is_primary_aifarm_dialog_provider(provider: &str) -> bool {
    provider
        .split_once('+')
        .map_or(provider, |(primary, _)| primary)
        .trim()
        .eq_ignore_ascii_case(PROVIDER_AIFARM)
}

const EXPLICIT_FULL_DIALOG_PHRASES: &[&str] = &[
    "максимально подробно",
    "очень подробно",
    "без сокращений",
    "полный текст",
    "полную версию",
    "comprehensive",
    "exhaustive",
    "full version",
    "no shortcuts",
];

const LONG_DIALOG_PHRASES: &[&str] = &[
    "подробно",
    "детально",
    "развернуто",
    "пошагово",
    "распиши",
    "объясни",
    "сводк",
    "резюм",
    "анализ",
    "detailed",
    "explain",
    "step by step",
    "summarize",
    "summary",
    "analyze",
    "review",
];

const GO_SETTINGS_REACTIVITY_OFF: i32 = 0;
const GO_OBSCENIFY_VOWELS: &str = "ауоыиэяюёе";

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        env, fmt,
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex, MutexGuard},
        time::{SystemTime, UNIX_EPOCH},
    };

    use openplotva_core::MessageIdMapping;
    use openplotva_taskman::{
        DEFAULT_PRIORITY, HIGHEST_PRIORITY, IMAGE_REGULAR_QUEUE_NAME, IMAGE_VIP_QUEUE_NAME,
        JobStatus, MUSIC_VIP_QUEUE_NAME, MusicGenJobParams, new_music_gen_job_at,
    };
    use openplotva_telegram::{
        DispatcherConfig, TelegramOutboundMethod, TelegramOutboundMethodKind,
        TelegramOutboundResponse,
    };
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::{Value, json};

    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateStateStore, UpdateStateStoreFuture,
        process_update_with_state_store_at,
    };

    use super::*;

    #[tokio::test]
    async fn addressed_private_message_schedules_dialog_job()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let scheduler = TaskmanDialogMessageScheduler::new(
            Arc::clone(&queue),
            Arc::clone(&debounce),
            Duration::from_millis(1),
        );
        let typing = Arc::new(TypingEffectsStub::default());
        let scheduler = scheduler.with_typing_effects(typing.clone());
        let config = test_config();

        let route = handle_dialog_message_update_or_else(
            &scheduler,
            &config,
            message_update("hello there")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::Scheduled {
                queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                delay: Duration::from_millis(1),
                replaced: false,
            }
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        let records = queue.records();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.queue_name, DIALOG_AIFARM_QUEUE_NAME);
        assert_eq!(record.status, JobStatus::Pending);
        assert_eq!(record.job.title, "dialog trigger");
        assert_eq!(record.job.priority, HIGH_PRIORITY);
        assert_eq!(record.job.processing_timeout_seconds, 720);
        let dialog = record.job.data.dialog_data.as_ref().expect("dialog");
        assert_eq!(dialog.message_text, "hello there");
        assert_eq!(dialog.original_text, "hello there");
        assert_eq!(dialog.max_output_tokens, 1024);
        assert_eq!(dialog.meta["sender_id"], 99);
        assert_eq!(typing.calls(), vec![(42, None)]);
        Ok(())
    }

    #[tokio::test]
    async fn private_unknown_command_falls_through_to_dialog_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else(
            &scheduler,
            &settings,
            &effects,
            &rng,
            &test_config(),
            message_update("/unknown payload")?,
            |_update| async { Err("private unknown command should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::Scheduled {
                queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                delay: Duration::ZERO,
                replaced: false,
            }
        );
        assert_eq!(scheduler.calls(), vec!["/unknown payload".to_owned()]);
        assert!(effects.sent_song_notices().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn private_plain_ping_falls_through_to_dialog_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else(
            &scheduler,
            &settings,
            &effects,
            &rng,
            &test_config(),
            message_update("ping hello")?,
            |_update| async { Err("plain ping should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::Scheduled {
                queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                delay: Duration::ZERO,
                replaced: false,
            },
            "/ping is handled only as a command; CommandAliases has no plain ping alias"
        );
        assert_eq!(scheduler.calls(), vec!["ping hello".to_owned()]);
        assert!(effects.sent_song_notices().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_addressed_message_schedules_and_completes_dialog_job_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-dialog:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        update_queue
            .enqueue_update(&message_update_at("hello there", now_secs as i64)?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded dialog update"))?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(InMemoryTaskQueue::new());
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let dialog_scheduler = TaskmanDialogMessageScheduler::new(
            Arc::clone(&queue),
            Arc::clone(&debounce),
            Duration::from_millis(1),
        );
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

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
            |update| async {
                let handled = handle_dialog_message_update_or_else(
                    &dialog_scheduler,
                    &test_config(),
                    update,
                    |_update| async {
                        Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                    },
                )
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 12345);
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
                "chat:42:private:Ada:ada_l".to_owned(),
                "user:99:Ada:ada_l".to_owned()
            ]
        );
        assert_eq!(
            *lock(&route),
            Some(DialogMessageUpdateRoute::Scheduled {
                queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                delay: Duration::from_millis(1),
                replaced: false,
            })
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        let job_id = {
            let records = queue.records();
            assert_eq!(records.len(), 1);
            let record = &records[0];
            assert_eq!(record.queue_name, DIALOG_AIFARM_QUEUE_NAME);
            assert_eq!(record.status, JobStatus::Pending);
            assert_eq!(record.job.title, "dialog trigger");
            assert_eq!(record.job.priority, HIGH_PRIORITY);
            let telegram = record.job.data.telegram_data.as_ref().expect("telegram");
            assert_eq!(telegram.chat_id, 42);
            assert_eq!(telegram.message_id, 77);
            assert_eq!(telegram.user_id, 99);
            let dialog = record.job.data.dialog_data.as_ref().expect("dialog");
            assert_eq!(dialog.message_text, "hello there");
            assert_eq!(dialog.original_text, "hello there");
            assert_eq!(dialog.max_output_tokens, 1024);
            record.id
        };

        let provider = DialogProviderStub::returning("  decoded <b>pong</b>  ");
        let dispatcher_queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let dispatch_store = VirtualStoreStub::default();
        let effects = crate::dialog_jobs::DialogDispatcherEffects::new(
            dispatch_store.clone(),
            Arc::clone(&dispatcher_queue),
        )
        .with_virtual_id_factory(Arc::new(|| "dialog-vmsg-1".to_owned()));
        let worker_report = crate::dialog_jobs::process_dialog_job_once_in_queue_at(
            queue.as_ref(),
            DIALOG_AIFARM_QUEUE_NAME,
            &provider,
            &effects,
            OffsetDateTime::now_utc(),
        )
        .await;
        assert_eq!(worker_report.job_id, Some(job_id));
        assert!(worker_report.dequeued);
        assert!(worker_report.sent_answer);
        assert!(worker_report.completed);
        assert!(!worker_report.failed);
        assert_eq!(
            dispatch_store.inserted(),
            vec![("dialog-vmsg-1".to_owned(), 42, None)]
        );
        let snapshot = dispatcher_queue.snapshot();
        assert_eq!(snapshot.immediate.len(), 1);
        assert!(snapshot.regular.is_empty());
        assert_eq!(snapshot.immediate[0].virtual_id, "dialog-vmsg-1");
        let mut drained = dispatcher_queue.drain_for_shutdown();
        assert_eq!(drained.immediate.len(), 1);
        let (metadata, method) = drained.immediate.remove(0).into_parts();
        assert_eq!(metadata.virtual_id, "dialog-vmsg-1");
        let method = method.expect("queued dialog answer method");
        assert_eq!(method.kind(), TelegramOutboundMethodKind::SendMessage);
        let TelegramOutboundMethod::SendMessage(method) = method else {
            panic!("expected sendMessage")
        };
        let payload = serde_json::to_value(&*method)?;
        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["text"], json!("decoded <b>pong</b>"));
        assert_eq!(payload["parse_mode"], json!("HTML"));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(77));
        assert_eq!(payload["reply_parameters"]["chat_id"], json!(42));
        assert!(
            payload["reply_parameters"]
                .get("allow_sending_without_reply")
                .is_none()
        );
        let provider_inputs = provider.inputs();
        assert_eq!(provider_inputs.len(), 1);
        assert_eq!(provider_inputs[0].context.chat_id, 42);
        assert_eq!(provider_inputs[0].message.text, "hello there");
        assert_eq!(provider_inputs[0].message.normalized, "hello there");
        assert_eq!(provider_inputs[0].max_output_tokens, 1024);
        assert_eq!(queue.records()[0].status, JobStatus::Completed);
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_random_group_message_schedules_and_completes_dialog_job_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-random-dialog:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        update_queue
            .enqueue_update(&group_message_update_at(
                "ordinary group talk",
                now_secs as i64,
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded random dialog update"))?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(InMemoryTaskQueue::new());
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let dialog_scheduler = TaskmanDialogMessageScheduler::new(
            Arc::clone(&queue),
            Arc::clone(&debounce),
            Duration::from_millis(1),
        );
        let settings = SettingsStoreStub::with_chat(random_chat_settings(100, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

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
            |update| async {
                let handled = handle_dialog_or_random_message_update_or_else(
                    &dialog_scheduler,
                    &settings,
                    &effects,
                    &rng,
                    &test_config(),
                    update,
                    |_update| async {
                        Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                    },
                )
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 22345);
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
                "chat:-100:supergroup:Group:".to_owned(),
                "user:99:Ada:ada_l".to_owned()
            ]
        );
        assert_eq!(
            *lock(&route),
            Some(DialogMessageUpdateRoute::Scheduled {
                queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                delay: Duration::from_millis(1),
                replaced: false,
            })
        );
        assert!(effects.sent_texts().is_empty());
        tokio::time::sleep(Duration::from_millis(20)).await;
        let job_id = {
            let records = queue.records();
            assert_eq!(records.len(), 1);
            let record = &records[0];
            assert_eq!(record.queue_name, DIALOG_AIFARM_QUEUE_NAME);
            assert_eq!(record.status, JobStatus::Pending);
            assert_eq!(record.job.title, "dialog (random)");
            assert_eq!(record.job.priority, HIGH_PRIORITY);
            let telegram = record.job.data.telegram_data.as_ref().expect("telegram");
            assert_eq!(telegram.chat_id, -100);
            assert_eq!(telegram.message_id, 78);
            assert_eq!(telegram.user_id, 99);
            let dialog = record.job.data.dialog_data.as_ref().expect("dialog");
            assert_eq!(dialog.message_text, "ordinary group talk");
            assert_eq!(dialog.original_text, "ordinary group talk");
            assert_eq!(dialog.max_output_tokens, 768);
            record.id
        };

        let provider = DialogProviderStub::returning("  random <b>pong</b>  ");
        let dispatcher_queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let dispatch_store = VirtualStoreStub::default();
        let effects = crate::dialog_jobs::DialogDispatcherEffects::new(
            dispatch_store.clone(),
            Arc::clone(&dispatcher_queue),
        )
        .with_virtual_id_factory(Arc::new(|| "random-dialog-vmsg-1".to_owned()));
        let worker_report = crate::dialog_jobs::process_dialog_job_once_in_queue_at(
            queue.as_ref(),
            DIALOG_AIFARM_QUEUE_NAME,
            &provider,
            &effects,
            OffsetDateTime::now_utc(),
        )
        .await;
        assert_eq!(worker_report.job_id, Some(job_id));
        assert!(worker_report.dequeued);
        assert!(worker_report.sent_answer);
        assert!(worker_report.completed);
        assert!(!worker_report.failed);
        assert_eq!(
            dispatch_store.inserted(),
            vec![("random-dialog-vmsg-1".to_owned(), -100, None)]
        );
        let snapshot = dispatcher_queue.snapshot();
        assert_eq!(snapshot.immediate.len(), 1);
        assert!(snapshot.regular.is_empty());
        assert_eq!(snapshot.immediate[0].virtual_id, "random-dialog-vmsg-1");
        let item = dispatcher_queue
            .dequeue_immediate()
            .ok_or_else(|| std::io::Error::other("expected random dialog answer"))?;
        assert_eq!(item.metadata().virtual_id, "random-dialog-vmsg-1");
        let method = item
            .into_method()
            .ok_or_else(|| std::io::Error::other("expected random dialog answer method"))?;
        assert_eq!(method.kind(), TelegramOutboundMethodKind::SendMessage);
        let payload = outbound_method_payload(&method);
        assert_eq!(payload["chat_id"], json!(-100));
        assert_eq!(payload["text"], json!("random <b>pong</b>"));
        assert_eq!(payload["parse_mode"], json!("HTML"));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(78));
        let provider_inputs = provider.inputs();
        assert_eq!(provider_inputs.len(), 1);
        assert_eq!(provider_inputs[0].context.chat_id, -100);
        assert_eq!(provider_inputs[0].message.text, "ordinary group talk");
        assert_eq!(provider_inputs[0].max_output_tokens, 768);
        assert_eq!(queue.records()[0].status, JobStatus::Completed);
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_random_obscenifier_queues_send_message_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-random-obscenifier:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        update_queue
            .enqueue_update(&group_message_update_at("что происходит", now_secs as i64)?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded random obscenifier update"))?;

        let state_store = UpdateStateStoreStub::default();
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(100, true));
        let dispatch_store = VirtualStoreStub::default();
        let dispatcher_queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let effects = RandomDialogDispatcherEffects::new(
            dispatch_store.clone(),
            Arc::clone(&dispatcher_queue),
        )
        .with_virtual_id_factory(Arc::new(|| "random-obscenifier-vmsg-1".to_owned()));
        let rng = RngStub {
            random_response: 99,
            obscenifier: 0,
            obscenify_variant: 3,
        };
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

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
            |update| async {
                let handled = handle_dialog_or_random_message_update_or_else(
                    &scheduler,
                    &settings,
                    &effects,
                    &rng,
                    &test_config(),
                    update,
                    |_update| async {
                        Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                    },
                )
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 22345);
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
                "chat:-100:supergroup:Group:".to_owned(),
                "user:99:Ada:ada_l".to_owned()
            ]
        );
        assert_eq!(
            *lock(&route),
            Some(DialogMessageUpdateRoute::RandomObscenified {
                text: "пиздоисходит ".to_owned(),
                send_error: None,
            })
        );
        assert!(scheduler.calls().is_empty());
        assert_eq!(
            dispatch_store.inserted(),
            vec![("random-obscenifier-vmsg-1".to_owned(), -100, None)]
        );

        let snapshot = dispatcher_queue.snapshot();
        assert!(snapshot.immediate.is_empty());
        assert_eq!(snapshot.regular.len(), 1);
        assert_eq!(snapshot.regular[0].virtual_id, "random-obscenifier-vmsg-1");
        assert_eq!(snapshot.regular[0].ephemeral_delete_after, None);
        let item = dispatcher_queue
            .dequeue_regular()
            .ok_or_else(|| std::io::Error::other("expected queued obscenifier text"))?;
        assert_eq!(item.metadata().virtual_id, "random-obscenifier-vmsg-1");
        assert_eq!(item.ephemeral_delete_after(), None);
        assert!(!item.bypasses_chat_restrictions());
        let method = item
            .into_method()
            .ok_or_else(|| std::io::Error::other("expected obscenifier sendMessage"))?;
        assert_eq!(method.kind(), TelegramOutboundMethodKind::SendMessage);
        let payload = outbound_method_payload(&method);
        assert_eq!(payload["chat_id"], json!(-100));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(78));
        assert_eq!(payload["text"], json!("пиздоисходит "));
        assert!(payload.get("parse_mode").is_none());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_random_gate_and_bot_sampling_consume_before_terminal_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-random-gate:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let cases = vec![
            (
                "unknown-command",
                22345,
                group_message_update_at("/unknown@PlotvaBot", now_secs as i64)?,
                DialogMessageUpdateRoute::RandomSkippedRoll {
                    reactivity: 100,
                    roll: 0,
                },
                None,
            ),
            (
                "bot-sender",
                22359,
                group_bot_message_update_at("ordinary bot chatter", now_secs as i64)?,
                DialogMessageUpdateRoute::RandomSkippedGate,
                None,
            ),
            (
                "bot-reply-sampling-skip",
                22360,
                group_bot_reply_to_plotva_update_at(
                    22360,
                    "bot reply skipped",
                    79,
                    now_secs as i64,
                )?,
                DialogMessageUpdateRoute::SkippedBotSampling,
                None,
            ),
            (
                "bot-reply-sampling-allow",
                22361,
                group_bot_reply_to_plotva_update_at(
                    22361,
                    "bot reply allowed",
                    80,
                    now_secs as i64,
                )?,
                DialogMessageUpdateRoute::Scheduled {
                    queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                    delay: Duration::ZERO,
                    replaced: false,
                },
                Some("bot reply allowed".to_owned()),
            ),
        ];
        for (_, _, update, _, _) in &cases {
            update_queue.enqueue_update(update).await?;
        }
        assert_eq!(update_queue.len().await?, cases.len() as i64);

        let state_store = UpdateStateStoreStub::default();
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(100, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        let mut expected_scheduler_calls = Vec::new();
        for (label, expected_update_id, _, expected_route, expected_scheduled_text) in cases {
            let decoded = update_queue
                .dequeue_update(Duration::from_secs(1))
                .await?
                .ok_or_else(|| {
                    std::io::Error::other(format!("expected decoded random-gate {label} update"))
                })?;
            let route = Arc::new(Mutex::new(None));
            let captured_route = Arc::clone(&route);

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
                |update| async {
                    let handled = handle_dialog_or_random_message_update_or_else_with_image(
                        (&scheduler, None, None),
                        &settings,
                        &effects,
                        &rng,
                        &test_config(),
                        update,
                        |_update| async {
                            Err::<(), std::io::Error>(std::io::Error::other(
                                "random gate update should not delegate",
                            ))
                        },
                    )
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                    *lock(&captured_route) = Some(handled);
                    Ok::<(), std::io::Error>(())
                },
            )
            .await;

            assert_eq!(report.update_id, expected_update_id, "{label}");
            assert_eq!(report.update_name, "message", "{label}");
            assert_eq!(
                report.state.outcome,
                UpdateStageOutcome::Completed,
                "{label}"
            );
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed),
                "{label}"
            );
            assert!(!report.skipped_handle, "{label}");
            assert_eq!(*lock(&route), Some(expected_route), "{label}");
            if let Some(text) = expected_scheduled_text {
                expected_scheduler_calls.push(text);
            }
        }
        assert_eq!(scheduler.calls(), expected_scheduler_calls);
        assert!(effects.sent_texts().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_non_member_service_messages_use_random_path_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-service-random:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let cases = vec![
            (
                "new-title",
                32401,
                group_service_payload_update_at(
                    32401,
                    181,
                    now_secs as i64,
                    json!({"new_chat_title": "Renamed"}),
                )?,
            ),
            (
                "delete-photo",
                32402,
                group_service_payload_update_at(
                    32402,
                    182,
                    now_secs as i64,
                    json!({"delete_chat_photo": true}),
                )?,
            ),
            (
                "group-created",
                32403,
                group_service_payload_update_at(
                    32403,
                    183,
                    now_secs as i64,
                    json!({"group_chat_created": true}),
                )?,
            ),
            (
                "supergroup-created",
                32404,
                group_service_payload_update_at(
                    32404,
                    184,
                    now_secs as i64,
                    json!({"supergroup_chat_created": true}),
                )?,
            ),
            (
                "migrate-from",
                32405,
                group_service_payload_update_at(
                    32405,
                    185,
                    now_secs as i64,
                    json!({"migrate_from_chat_id": -1001}),
                )?,
            ),
            (
                "migrate-to",
                32406,
                group_service_payload_update_at(
                    32406,
                    186,
                    now_secs as i64,
                    json!({"migrate_to_chat_id": -1002}),
                )?,
            ),
            (
                "auto-delete",
                32407,
                group_service_payload_update_at(
                    32407,
                    187,
                    now_secs as i64,
                    json!({
                        "message_auto_delete_timer_changed": {
                            "message_auto_delete_time": 86400
                        }
                    }),
                )?,
            ),
            (
                "new-chat-photo",
                32409,
                group_service_payload_update_at(
                    32409,
                    189,
                    now_secs as i64,
                    json!({
                        "new_chat_photo": [{
                            "file_id": "new-chat-photo-file",
                            "file_unique_id": "new-chat-photo-1",
                            "width": 200,
                            "height": 300
                        }]
                    }),
                )?,
            ),
            (
                "forum-topic-created",
                32410,
                group_service_payload_update_at(
                    32410,
                    190,
                    now_secs as i64,
                    json!({
                        "forum_topic_created": {
                            "name": "topic-name",
                            "icon_color": 9367192
                        }
                    }),
                )?,
            ),
            (
                "forum-topic-closed",
                32411,
                group_service_payload_update_at(
                    32411,
                    191,
                    now_secs as i64,
                    json!({"forum_topic_closed": {}}),
                )?,
            ),
            (
                "forum-topic-reopened",
                32412,
                group_service_payload_update_at(
                    32412,
                    192,
                    now_secs as i64,
                    json!({"forum_topic_reopened": {}}),
                )?,
            ),
            (
                "general-forum-hidden",
                32413,
                group_service_payload_update_at(
                    32413,
                    193,
                    now_secs as i64,
                    json!({"general_forum_topic_hidden": {}}),
                )?,
            ),
            (
                "general-forum-unhidden",
                32414,
                group_service_payload_update_at(
                    32414,
                    194,
                    now_secs as i64,
                    json!({"general_forum_topic_unhidden": {}}),
                )?,
            ),
            (
                "video-chat-started",
                32415,
                group_service_payload_update_at(
                    32415,
                    195,
                    now_secs as i64,
                    json!({"video_chat_started": {}}),
                )?,
            ),
            (
                "video-chat-ended",
                32416,
                group_service_payload_update_at(
                    32416,
                    196,
                    now_secs as i64,
                    json!({"video_chat_ended": {"duration": 100}}),
                )?,
            ),
            (
                "video-chat-scheduled",
                32417,
                group_service_payload_update_at(
                    32417,
                    197,
                    now_secs as i64,
                    json!({"video_chat_scheduled": {"start_date": now_secs as i64 + 100}}),
                )?,
            ),
            (
                "video-chat-invited",
                32418,
                group_service_payload_update_at(
                    32418,
                    198,
                    now_secs as i64,
                    json!({
                        "video_chat_participants_invited": {
                            "users": [{
                                "id": 1,
                                "is_bot": false,
                                "first_name": "User"
                            }]
                        }
                    }),
                )?,
            ),
            (
                "proximity-alert",
                32419,
                group_service_payload_update_at(
                    32419,
                    199,
                    now_secs as i64,
                    json!({
                        "proximity_alert_triggered": {
                            "distance": 100,
                            "traveler": {
                                "id": 1,
                                "is_bot": false,
                                "first_name": "Traveler"
                            },
                            "watcher": {
                                "id": 2,
                                "is_bot": false,
                                "first_name": "Watcher"
                            }
                        }
                    }),
                )?,
            ),
            (
                "users-shared",
                32420,
                group_service_payload_update_at(
                    32420,
                    200,
                    now_secs as i64,
                    json!({
                        "users_shared": {
                            "request_id": 1,
                            "users": [{"user_id": 1}]
                        }
                    }),
                )?,
            ),
            (
                "chat-shared",
                32421,
                group_service_payload_update_at(
                    32421,
                    201,
                    now_secs as i64,
                    json!({
                        "chat_shared": {
                            "request_id": 1,
                            "chat_id": -100
                        }
                    }),
                )?,
            ),
            (
                "write-access",
                32422,
                group_service_payload_update_at(
                    32422,
                    202,
                    now_secs as i64,
                    json!({"write_access_allowed": {"from_attachment_menu": true}}),
                )?,
            ),
            (
                "pinned-message",
                32423,
                group_service_payload_update_at(
                    32423,
                    203,
                    now_secs as i64,
                    json!({
                        "pinned_message": {
                            "message_id": 170,
                            "date": now_secs as i64 - 1,
                            "chat": {
                                "id": -100,
                                "type": "supergroup",
                                "title": "Group"
                            },
                            "from": {
                                "id": 99,
                                "is_bot": false,
                                "first_name": "Ada",
                                "username": "ada_l"
                            },
                            "text": "pinned text"
                        }
                    }),
                )?,
            ),
        ];
        for (_, _, update) in &cases {
            update_queue.enqueue_update(update).await?;
        }
        assert_eq!(update_queue.len().await?, cases.len() as i64);

        let state_store = UpdateStateStoreStub::default();
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(0, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        for (label, expected_update_id, _) in cases {
            let decoded = update_queue
                .dequeue_update(Duration::from_secs(1))
                .await?
                .ok_or_else(|| {
                    std::io::Error::other(format!(
                        "expected decoded service-message {label} update"
                    ))
                })?;
            let route = Arc::new(Mutex::new(None));
            let captured_route = Arc::clone(&route);

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
                |update| async {
                    let handled = handle_dialog_or_random_message_update_or_else_with_image(
                        (&scheduler, None, None),
                        &settings,
                        &effects,
                        &rng,
                        &test_config(),
                        update,
                        |_update| async {
                            Err::<(), std::io::Error>(std::io::Error::other(
                                "service message should not delegate",
                            ))
                        },
                    )
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                    *lock(&captured_route) = Some(handled);
                    Ok::<(), std::io::Error>(())
                },
            )
            .await;

            assert_eq!(report.update_id, expected_update_id, "{label}");
            assert_eq!(report.update_name, "message", "{label}");
            assert_eq!(
                report.state.outcome,
                UpdateStageOutcome::Completed,
                "{label}"
            );
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed),
                "{label}"
            );
            assert!(!report.skipped_handle, "{label}");
            assert_eq!(
                *lock(&route),
                Some(DialogMessageUpdateRoute::RandomSkippedReactivityOff),
                "{label}"
            );
        }
        let state_calls = state_store.calls();
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.starts_with("chat:-100:supergroup:Group:"))
                .count(),
            22
        );
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.starts_with("user:99:Ada:ada_l"))
                .count(),
            22
        );
        assert!(scheduler.calls().is_empty());
        assert!(effects.sent_texts().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_captionless_private_media_consumes_before_terminal_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-captionless-media:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let cases = vec![
            (
                "photo",
                32345,
                private_captionless_photo_update_at(now_secs as i64)?,
            ),
            (
                "voice",
                32355,
                private_captionless_voice_update_at(now_secs as i64)?,
            ),
            (
                "audio-empty-name",
                32362,
                private_media_payload_update_at(
                    32362,
                    96,
                    now_secs as i64,
                    media_audio_empty_json(),
                )?,
            ),
            (
                "video-empty-name",
                32363,
                private_media_payload_update_at(
                    32363,
                    97,
                    now_secs as i64,
                    media_video_empty_json(),
                )?,
            ),
            (
                "sticker-empty-emoji",
                32364,
                private_media_payload_update_at(
                    32364,
                    98,
                    now_secs as i64,
                    media_sticker_empty_emoji_json(),
                )?,
            ),
            (
                "location",
                32356,
                private_location_update_at(now_secs as i64)?,
            ),
            ("venue", 32357, private_venue_update_at(now_secs as i64)?),
            (
                "animation",
                32358,
                private_media_payload_update_at(
                    32358,
                    92,
                    now_secs as i64,
                    media_animation_json(),
                )?,
            ),
            (
                "premium-animation",
                32368,
                private_media_payload_update_at(
                    32368,
                    102,
                    now_secs as i64,
                    media_premium_animation_json(),
                )?,
            ),
            (
                "document-empty-name",
                32359,
                private_media_payload_update_at(
                    32359,
                    93,
                    now_secs as i64,
                    media_document_without_file_name_json(),
                )?,
            ),
            (
                "video-note",
                32360,
                private_media_payload_update_at(
                    32360,
                    94,
                    now_secs as i64,
                    media_video_note_json(),
                )?,
            ),
            (
                "dice",
                32361,
                private_media_payload_update_at(32361, 95, now_secs as i64, media_dice_json())?,
            ),
            (
                "paid-media",
                32365,
                private_media_payload_update_at(
                    32365,
                    99,
                    now_secs as i64,
                    media_paid_media_json(),
                )?,
            ),
            (
                "story",
                32366,
                private_media_payload_update_at(32366, 100, now_secs as i64, media_story_json())?,
            ),
            (
                "checklist",
                32367,
                private_media_payload_update_at(
                    32367,
                    101,
                    now_secs as i64,
                    media_checklist_json(),
                )?,
            ),
        ];
        for (_, _, update) in &cases {
            update_queue.enqueue_update(update).await?;
        }
        assert_eq!(update_queue.len().await?, cases.len() as i64);

        let state_store = UpdateStateStoreStub::default();
        let scheduler = SchedulerStub::default();
        let image_scheduler = ImageSchedulerCaptureStub::scheduled();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };
        let expected_case_count = cases.len();
        for (label, expected_update_id, _) in cases {
            let decoded = update_queue
                .dequeue_update(Duration::from_secs(1))
                .await?
                .ok_or_else(|| {
                    std::io::Error::other(format!("expected decoded captionless {label} update"))
                })?;
            let route = Arc::new(Mutex::new(None));
            let captured_route = Arc::clone(&route);

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
                |update| async {
                    let handled = handle_dialog_or_random_message_update_or_else_with_image(
                        (&scheduler, Some(&image_scheduler), None),
                        &settings,
                        &effects,
                        &rng,
                        &test_config(),
                        update,
                        |_update| async {
                            Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                        },
                    )
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                    *lock(&captured_route) = Some(handled);
                    Ok::<(), std::io::Error>(())
                },
            )
            .await;

            assert_eq!(report.update_id, expected_update_id, "{label}");
            assert_eq!(report.update_name, "message", "{label}");
            assert_eq!(
                report.state.outcome,
                UpdateStageOutcome::Completed,
                "{label}"
            );
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed),
                "{label}"
            );
            assert!(!report.skipped_handle, "{label}");
            assert_eq!(
                *lock(&route),
                Some(DialogMessageUpdateRoute::SkippedEmptyDialogTrigger),
                "{label}"
            );
        }
        let state_calls = state_store.calls();
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.starts_with("chat:42:private:Ada:ada_l"))
                .count(),
            expected_case_count
        );
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.starts_with("user:99:Ada:ada_l"))
                .count(),
            expected_case_count
        );
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.starts_with("file:"))
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                "file:captionless-photo-1".to_owned(),
                "file:captionless-voice-1".to_owned(),
                "file:captionless-audio-1".to_owned(),
                "file:captionless-sticker-1".to_owned(),
            ]
        );
        assert!(scheduler.calls().is_empty());
        assert!(image_scheduler.calls().is_empty());
        assert!(effects.sent_texts().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_contact_phone_fallback_schedules_dialog_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-contact-dialog:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&private_contact_phone_update()?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded contact update"))?;

        let state_store = UpdateStateStoreStub::default();
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: Duration::from_millis(1),
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(60),
                worker_limit: 1,
            },
            UNIX_EPOCH + Duration::from_secs(1_710_000_000),
            &state_store,
            |update| async {
                let handled = handle_dialog_or_random_message_update_or_else_with_image(
                    (&scheduler, None, None),
                    &settings,
                    &effects,
                    &rng,
                    &test_config(),
                    update,
                    |_update| async {
                        Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                    },
                )
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 32346);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Ada:ada_l".to_owned(),
                "user:99:Ada:ada_l".to_owned()
            ]
        );
        assert_eq!(
            *lock(&route),
            Some(DialogMessageUpdateRoute::Scheduled {
                queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                delay: Duration::ZERO,
                replaced: false,
            })
        );
        assert_eq!(scheduler.calls(), vec!["+123".to_owned()]);
        assert!(effects.sent_texts().is_empty());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_media_name_fallbacks_schedule_dialog_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-media-fallback-dialog:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let cases = [
            (
                private_media_fallback_update(32347, 81, media_audio_title_json())?,
                "Artist Title",
            ),
            (
                private_media_fallback_update(32348, 82, media_audio_file_json())?,
                "song.mp3",
            ),
            (
                private_media_fallback_update(32349, 83, media_video_json())?,
                "clip.mp4",
            ),
            (
                private_media_fallback_update(32350, 84, media_document_json())?,
                "doc.pdf",
            ),
            (
                private_media_fallback_update(32351, 85, media_sticker_json())?,
                "🙂",
            ),
            (
                private_media_fallback_update(32352, 86, media_contact_name_json())?,
                "Ada Lovelace",
            ),
        ];

        for (update, _) in &cases {
            update_queue.enqueue_update(update).await?;
        }
        assert_eq!(update_queue.len().await?, cases.len() as i64);

        let state_store = UpdateStateStoreStub::default();
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };
        let mut routes = Vec::new();

        for (index, (_, expected_text)) in cases.iter().enumerate() {
            let decoded = update_queue
                .dequeue_update(Duration::from_secs(1))
                .await?
                .ok_or_else(|| std::io::Error::other("expected decoded media update"))?;
            let route = Arc::new(Mutex::new(None));
            let captured_route = Arc::clone(&route);

            let report = process_update_with_state_store_at(
                decoded,
                UpdateConsumerConfig {
                    dequeue_timeout: Duration::from_millis(1),
                    state_timeout: Duration::from_secs(1),
                    handle_timeout: Duration::from_secs(1),
                    side_effect_max_age: Duration::from_secs(60),
                    worker_limit: 1,
                },
                UNIX_EPOCH + Duration::from_secs(1_710_000_000),
                &state_store,
                |update| async {
                    let handled = handle_dialog_or_random_message_update_or_else_with_image(
                        (&scheduler, None, None),
                        &settings,
                        &effects,
                        &rng,
                        &test_config(),
                        update,
                        |_update| async {
                            Err::<(), std::io::Error>(std::io::Error::other(
                                "media fallback should not delegate",
                            ))
                        },
                    )
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                    *lock(&captured_route) = Some(handled);
                    Ok::<(), std::io::Error>(())
                },
            )
            .await;

            assert_eq!(report.update_id, 32347 + index as i64);
            assert_eq!(report.update_name, "message");
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed),
                "{expected_text}"
            );
            assert_eq!(
                *lock(&route),
                Some(DialogMessageUpdateRoute::Scheduled {
                    queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                    delay: Duration::ZERO,
                    replaced: false,
                }),
                "{expected_text}"
            );
            routes.push(expected_text.to_string());
        }

        assert_eq!(scheduler.calls(), routes);
        let state_calls = state_store.calls();
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.starts_with("chat:42:private:Ada:ada_l"))
                .count(),
            cases.len()
        );
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.starts_with("user:99:Ada:ada_l"))
                .count(),
            cases.len()
        );
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.starts_with("file:"))
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                "file:audio-unique".to_owned(),
                "file:audio-unique".to_owned(),
                "file:sticker-unique".to_owned(),
            ]
        );
        assert!(effects.sent_texts().is_empty());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_captioned_group_video_uses_random_path_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-captioned-video-random:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&group_captioned_video_update_at(32367, 101, 1_710_000_000)?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded captioned video update"))?;

        let state_store = UpdateStateStoreStub::default();
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(100, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: Duration::from_millis(1),
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(60),
                worker_limit: 1,
            },
            UNIX_EPOCH + Duration::from_secs(1_710_000_000),
            &state_store,
            |update| async {
                let handled = handle_dialog_or_random_message_update_or_else_with_image(
                    (&scheduler, None, None),
                    &settings,
                    &effects,
                    &rng,
                    &test_config(),
                    update,
                    |_update| async {
                        Err::<(), std::io::Error>(std::io::Error::other(
                            "captioned media random route should not delegate",
                        ))
                    },
                )
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 32367);
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
                "chat:-100:supergroup:Group:".to_owned(),
                "user:99:Ada:ada_l".to_owned(),
            ]
        );
        assert_eq!(
            *lock(&route),
            Some(DialogMessageUpdateRoute::Scheduled {
                queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                delay: Duration::ZERO,
                replaced: false,
            })
        );
        assert_eq!(scheduler.calls(), vec!["смотри что тут".to_owned()]);
        assert!(effects.sent_texts().is_empty());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn addressed_draw_command_schedules_image_job_when_scheduler_is_wired()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let image_scheduler =
            crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue));
        let scheduler = SchedulerStub::default();

        let route = handle_dialog_message_update_or_else_with_image(
            &scheduler,
            Some(&image_scheduler),
            None,
            &test_config(),
            message_update("draw cat")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::DrawImageScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        assert!(scheduler.calls().is_empty());
        let records = queue.records();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.queue_name, IMAGE_REGULAR_QUEUE_NAME);
        assert_eq!(record.job.title, "image");
        assert_eq!(record.job.priority, DEFAULT_PRIORITY);
        let image = record.job.data.image_data.as_ref().expect("image");
        assert_eq!(image.prompt, "cat");
        assert_eq!(image.original_text, "cat");
        assert_eq!(image.author, "Ada");
        Ok(())
    }

    #[tokio::test]
    async fn bang_draw_shortcut_schedules_image_job_without_group_addressing()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let image_scheduler =
            crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, Some(&image_scheduler), None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("!draw neon cat")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::DrawImageScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        assert!(scheduler.calls().is_empty());
        let records = queue.records();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.queue_name, IMAGE_REGULAR_QUEUE_NAME);
        let image = record.job.data.image_data.as_ref().expect("image");
        assert_eq!(image.prompt, "neon cat");
        assert_eq!(image.original_text, "neon cat");
        assert_eq!(image.author, "Ada");
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_bang_draw_schedules_and_completes_image_job_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-bang-draw:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        update_queue
            .enqueue_update(&group_message_update_at("!draw neon cat", now_secs as i64)?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded bang draw update"))?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(InMemoryTaskQueue::new());
        let image_scheduler =
            crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

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
            |update| async {
                let handled = handle_dialog_or_random_message_update_or_else_with_image(
                    (&scheduler, Some(&image_scheduler), None),
                    &settings,
                    &effects,
                    &rng,
                    &test_config(),
                    update,
                    |_update| async {
                        Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                    },
                )
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 22345);
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
                "chat:-100:supergroup:Group:".to_owned(),
                "user:99:Ada:ada_l".to_owned()
            ]
        );
        assert_eq!(
            *lock(&route),
            Some(DialogMessageUpdateRoute::DrawImageScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            })
        );
        assert!(scheduler.calls().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        let job_id = {
            let records = queue.records();
            assert_eq!(records.len(), 1);
            let record = &records[0];
            assert_eq!(record.queue_name, IMAGE_REGULAR_QUEUE_NAME);
            assert_eq!(record.status, JobStatus::Pending);
            assert_eq!(record.job.title, "image");
            assert_eq!(record.job.priority, DEFAULT_PRIORITY);
            assert_eq!(
                record.job.data.job_type,
                openplotva_taskman::JobType::ImageGen
            );
            let telegram = record.job.data.telegram_data.as_ref().expect("telegram");
            assert_eq!(telegram.chat_id, -100);
            assert_eq!(telegram.message_id, 78);
            assert_eq!(telegram.user_id, 99);
            let image = record.job.data.image_data.as_ref().expect("image");
            assert_eq!(image.prompt, "neon cat");
            assert_eq!(image.original_text, "neon cat");
            assert_eq!(image.author, "Ada");
            assert!(!image.is_image_edit);
            record.id
        };

        let generator = DirectImageGeneratorStub::success(ImageGenerationResult {
            image_url: "https://img.test/neon-cat.png".to_owned(),
            image_urls: vec!["https://img.test/fallback.png".to_owned()],
            image_bytes: vec![b"png".to_vec()],
        });
        let image_queued = ImageQueuedStickerCapture::default();
        let image_ephemeral = ImageEphemeralCapture::default();
        let image_sender = ImageTelegramSenderCapture::new(vec![
            Ok(TelegramOutboundResponse::Message(Box::new(
                telegram_bot_message(-100, 870)?,
            ))),
            Ok(TelegramOutboundResponse::Boolean(true)),
        ]);
        let image_effects = crate::image_jobs::TelegramImageJobEffects::new(
            image_queued.clone(),
            image_ephemeral.clone(),
            image_sender.clone(),
        );
        let worker_report = crate::image_jobs::run_image_gen_queue_once(
            &queue,
            IMAGE_REGULAR_QUEUE_NAME,
            &generator,
            &image_effects,
            "image-gen-smoke-worker",
            OffsetDateTime::now_utc(),
        )
        .await;
        assert_eq!(
            worker_report,
            crate::image_jobs::ImageGenQueuePollReport {
                queue_name: IMAGE_REGULAR_QUEUE_NAME.to_owned(),
                job_id: Some(job_id),
                outcome: crate::image_jobs::ImageGenQueuePollOutcome::Completed,
                error: None,
            }
        );
        assert_eq!(
            generator.requests(),
            vec![crate::image_jobs::ImageGenerationRequest {
                chat_id: -100,
                message_id: 78,
                user_id: 99,
                user_full_name: "Ada".to_owned(),
                thread_id: None,
                prompt: "neon cat".to_owned(),
                caption_text: "neon cat".to_owned(),
                prompt_variants: Vec::new(),
                is_nsfw: false,
                negative_prompt: String::new(),
                aspect_ratio: String::new(),
                seed: String::new(),
            }]
        );
        assert_eq!(image_queued.loads(), vec![(-100, 78)]);
        assert!(image_queued.deletes().is_empty());
        assert_eq!(
            image_ephemeral.tracked(),
            vec![(-100, 870, crate::image_jobs::DRAWING_STICKER_DELETE_AFTER)]
        );
        let image_methods = image_sender.methods();
        assert_eq!(
            image_methods
                .iter()
                .map(|(kind, _)| *kind)
                .collect::<Vec<_>>(),
            vec![
                TelegramOutboundMethodKind::SendSticker,
                TelegramOutboundMethodKind::DeleteMessage,
            ]
        );
        let sticker_debug = image_methods[0].1["debug"].as_str().expect("sticker debug");
        assert!(sticker_debug.contains("-100"));
        assert!(sticker_debug.contains(crate::image_jobs::STICKER_DRAW_FILE_ID));
        assert!(sticker_debug.contains("disable_notification"));
        assert!(sticker_debug.contains("reply_parameters"));
        assert!(sticker_debug.contains("78"));
        assert_eq!(image_methods[1].1["chat_id"], json!(-100));
        assert_eq!(image_methods[1].1["message_id"], json!(870));
        let records = queue.records();
        assert_eq!(records[0].status, JobStatus::Completed);
        assert_eq!(
            &records[0]
                .job
                .data
                .image_data
                .as_ref()
                .expect("image")
                .image_urls,
            &vec!["https://img.test/neon-cat.png".to_owned()]
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn addressed_percent_shortcut_uses_direct_draw_api_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let direct_draw = DirectDrawApiEffectsStub::sent();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image_and_direct_draw(
            (&scheduler, None, None, Some(&direct_draw)),
            &settings,
            &effects,
            &rng,
            &test_config(),
            message_update("% neon koi")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::DirectDrawApi {
                sent: true,
                error: None,
            }
        );
        assert!(scheduler.calls().is_empty());
        assert_eq!(
            direct_draw.calls(),
            vec![DirectDrawApiRequest {
                chat_id: 42,
                message_id: 77,
                user_id: 99,
                user_full_name: "Ada".to_owned(),
                prompt: "neon koi".to_owned(),
                thread_id: None,
                is_forum: false,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn addressed_percent_shortcut_without_effects_consumes_like_go_nil_draw_api()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image_and_direct_draw(
            (&scheduler, None, None, None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            message_update("% neon koi")?,
            |_update| async { Err("nil draw api should be consumed") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::DirectDrawApi {
                sent: false,
                error: Some("draw api not configured".to_owned()),
            }
        );
        assert!(scheduler.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn unaddressed_percent_message_uses_random_path_not_terminal_delegate()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(0, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("% neon koi")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(route, DialogMessageUpdateRoute::RandomSkippedReactivityOff);
        assert!(scheduler.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn forum_topic_root_reply_to_plotva_uses_random_path_not_addressed_dialog()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(0, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_forum_topic_root_reply_to_plotva_update("topic root reply")?,
            |_update| async { Err("forum topic root reply should not delegate") },
        )
        .await?;

        assert_eq!(route, DialogMessageUpdateRoute::RandomSkippedReactivityOff);
        assert!(scheduler.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn unaddressed_plain_react_aliases_use_random_path_not_terminal_delegate()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(0, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        for text in [
            "draw cat",
            "нарисуй кота",
            "рисуй рыбу",
            "translate hello",
            "переведи hello",
            "перевод hello",
            "song cats",
            "песня cats",
        ] {
            let route = handle_dialog_or_random_message_update_or_else_with_image(
                (&scheduler, None, None),
                &settings,
                &effects,
                &rng,
                &test_config(),
                group_message_update(text)?,
                |_update| async { Err("should not delegate") },
            )
            .await?;

            assert_eq!(
                route,
                DialogMessageUpdateRoute::RandomSkippedReactivityOff,
                "{text}"
            );
        }
        assert!(scheduler.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn explicit_content_shortcuts_never_fall_into_terminal_when_owner_slice_is_absent()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(0, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        for text in ["$", ";"] {
            let route = handle_dialog_or_random_message_update_or_else_with_image(
                (&scheduler, None, None),
                &settings,
                &effects,
                &rng,
                &test_config(),
                group_message_update(text)?,
                |_update| async { Err("rates fallback should not hit terminal") },
            )
            .await?;

            assert_eq!(
                route,
                DialogMessageUpdateRoute::RandomSkippedReactivityOff,
                "{text}"
            );
        }

        let draw_route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("!draw castle")?,
            |_update| async { Err("bang draw without scheduler should not hit terminal") },
        )
        .await?;
        assert_eq!(
            draw_route,
            DialogMessageUpdateRoute::DrawImageScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            }
        );

        for text in ["draw castle", "нарисуй замок", "рисуй замок"] {
            let route = handle_dialog_or_random_message_update_or_else_with_image(
                (&scheduler, None, None),
                &settings,
                &effects,
                &rng,
                &test_config(),
                message_update(text)?,
                |_update| async { Err("addressed draw without scheduler should not hit terminal") },
            )
            .await?;

            assert_eq!(
                route,
                DialogMessageUpdateRoute::DrawImageScheduled {
                    status: "not_scheduled".to_owned(),
                    no_reply: true,
                },
                "{text}"
            );
        }

        let song_missing_topic = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("!song")?,
            |_update| async { Err("bang song without topic should not hit terminal") },
        )
        .await?;
        assert_eq!(
            song_missing_topic,
            DialogMessageUpdateRoute::SongMissingTopic
        );

        let song_unavailable = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("!song neon rain")?,
            |_update| async { Err("bang song without scheduler should not hit terminal") },
        )
        .await?;
        assert_eq!(
            song_unavailable,
            DialogMessageUpdateRoute::SongScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            }
        );

        assert!(scheduler.calls().is_empty());
        let notices = effects.sent_song_notices();
        assert_eq!(notices.len(), 2);
        assert_eq!(notices[0].message.text, DIRECT_SONG_TOPIC_REQUIRED_NOTICE);
        assert_eq!(
            notices[1].message.text,
            DIRECT_SONG_SERVICE_UNAVAILABLE_NOTICE
        );
        Ok(())
    }

    #[tokio::test]
    async fn unhandled_group_command_and_bot_sender_use_random_gate_not_terminal_delegate()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(100, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        let unknown_command = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("/unknown@PlotvaBot")?,
            |_update| async { Err("unknown group command should not delegate") },
        )
        .await?;
        assert_eq!(
            unknown_command,
            DialogMessageUpdateRoute::RandomSkippedRoll {
                reactivity: 100,
                roll: 0,
            }
        );

        let bot_message = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_bot_message_update("ordinary bot chatter")?,
            |_update| async { Err("bot sender random gate should not delegate") },
        )
        .await?;
        assert_eq!(bot_message, DialogMessageUpdateRoute::RandomSkippedGate);
        assert!(scheduler.calls().is_empty());
        assert!(effects.sent_texts().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn non_member_service_messages_use_random_path_not_terminal_delegate()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(0, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        for (label, service) in [
            ("new-title", json!({"new_chat_title": "Renamed"})),
            ("delete-photo", json!({"delete_chat_photo": true})),
            ("group-created", json!({"group_chat_created": true})),
            (
                "supergroup-created",
                json!({"supergroup_chat_created": true}),
            ),
            ("migrate-from", json!({"migrate_from_chat_id": -1001})),
            ("migrate-to", json!({"migrate_to_chat_id": -1002})),
            (
                "auto-delete",
                json!({
                    "message_auto_delete_timer_changed": {
                        "message_auto_delete_time": 86400
                    }
                }),
            ),
            (
                "new-chat-photo",
                json!({
                    "new_chat_photo": [{
                        "file_id": "new-chat-photo-file",
                        "file_unique_id": "new-chat-photo-1",
                        "width": 200,
                        "height": 300
                    }]
                }),
            ),
            (
                "forum-topic-created",
                json!({
                    "forum_topic_created": {
                        "name": "topic-name",
                        "icon_color": 9367192
                    }
                }),
            ),
            ("forum-topic-closed", json!({"forum_topic_closed": {}})),
            ("forum-topic-reopened", json!({"forum_topic_reopened": {}})),
            (
                "general-forum-hidden",
                json!({"general_forum_topic_hidden": {}}),
            ),
            (
                "general-forum-unhidden",
                json!({"general_forum_topic_unhidden": {}}),
            ),
            ("video-chat-started", json!({"video_chat_started": {}})),
            (
                "video-chat-ended",
                json!({"video_chat_ended": {"duration": 100}}),
            ),
            (
                "video-chat-scheduled",
                json!({"video_chat_scheduled": {"start_date": 1_710_000_100}}),
            ),
            (
                "video-chat-invited",
                json!({
                    "video_chat_participants_invited": {
                        "users": [{
                            "id": 1,
                            "is_bot": false,
                            "first_name": "User"
                        }]
                    }
                }),
            ),
            (
                "proximity-alert",
                json!({
                    "proximity_alert_triggered": {
                        "distance": 100,
                        "traveler": {
                            "id": 1,
                            "is_bot": false,
                            "first_name": "Traveler"
                        },
                        "watcher": {
                            "id": 2,
                            "is_bot": false,
                            "first_name": "Watcher"
                        }
                    }
                }),
            ),
            (
                "users-shared",
                json!({
                    "users_shared": {
                        "request_id": 1,
                        "users": [{"user_id": 1}]
                    }
                }),
            ),
            (
                "chat-shared",
                json!({
                    "chat_shared": {
                        "request_id": 1,
                        "chat_id": -100
                    }
                }),
            ),
            (
                "write-access",
                json!({"write_access_allowed": {"from_attachment_menu": true}}),
            ),
            (
                "pinned-message",
                json!({
                    "pinned_message": {
                        "message_id": 70,
                        "date": 1_709_999_900,
                        "chat": {
                            "id": -100,
                            "type": "supergroup",
                            "title": "Group"
                        },
                        "from": {
                            "id": 99,
                            "is_bot": false,
                            "first_name": "Ada",
                            "username": "ada_l"
                        },
                        "text": "pinned text"
                    }
                }),
            ),
        ] {
            let route = handle_dialog_or_random_message_update_or_else_with_image(
                (&scheduler, None, None),
                &settings,
                &effects,
                &rng,
                &test_config(),
                group_service_payload_update_at(24000, 81, 1_710_000_000, service)?,
                |_update| async { Err("service message should not delegate") },
            )
            .await?;

            assert_eq!(
                route,
                DialogMessageUpdateRoute::RandomSkippedReactivityOff,
                "{label}"
            );
        }
        assert!(scheduler.calls().is_empty());
        assert!(effects.sent_texts().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn addressed_captionless_media_consumes_like_go_without_terminal_delegate()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let image_scheduler = ImageSchedulerCaptureStub::scheduled();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, Some(&image_scheduler), None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            private_captionless_photo_update()?,
            |_update| async { Err("captionless private media should be consumed") },
        )
        .await?;

        assert_eq!(route, DialogMessageUpdateRoute::SkippedEmptyDialogTrigger);
        assert!(scheduler.calls().is_empty());
        assert!(image_scheduler.calls().is_empty());
        assert!(effects.sent_texts().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn addressed_contact_phone_fallback_schedules_dialog_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            private_contact_phone_update()?,
            |_update| async { Err("private contact should be handled as dialog text") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::Scheduled {
                queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                delay: Duration::ZERO,
                replaced: false,
            }
        );
        assert_eq!(scheduler.calls(), vec!["+123".to_owned()]);
        assert!(effects.sent_texts().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn direct_draw_api_runtime_generates_and_sends_go_photo_reply()
    -> Result<(), Box<dyn std::error::Error>> {
        let generator = DirectImageGeneratorStub::success(ImageGenerationResult {
            image_bytes: vec![b"png".to_vec()],
            image_urls: vec!["https://img.test/fallback.png".to_owned()],
            ..ImageGenerationResult::default()
        });
        let sender = DirectDrawApiSenderStub::success(sent_photo_message()?);
        let history = DirectDrawApiHistoryStoreStub::default();
        let effects = DirectDrawApiRuntimeEffects::with_sender(sender.clone(), generator.clone())
            .with_history_store(Arc::new(history.clone()), 555);

        let report = effects
            .send_direct_draw_api(DirectDrawApiRequest {
                chat_id: -100,
                message_id: 77,
                user_id: 99,
                user_full_name: "Ada".to_owned(),
                prompt: "neon koi".to_owned(),
                thread_id: Some(9),
                is_forum: true,
            })
            .await;

        assert_eq!(
            report,
            DirectDrawApiResult {
                sent: true,
                error: None,
            }
        );
        let generation_requests = generator.requests();
        assert_eq!(generation_requests.len(), 1);
        assert_eq!(generation_requests[0].prompt, "neon koi");
        assert_eq!(generation_requests[0].caption_text, "neon koi");
        assert_eq!(generation_requests[0].thread_id, Some(9));

        let photos = sender.requests();
        assert_eq!(photos.len(), 1);
        assert_eq!(photos[0].chat.id, -100);
        assert!(photos[0].chat.is_forum);
        assert_eq!(photos[0].message_thread_id, 9);
        assert_eq!(
            photos[0].photo,
            PhotoSource::Bytes {
                file_name: "image.png".to_owned(),
                bytes: b"png".to_vec(),
            }
        );
        assert_eq!(
            photos[0].reply_parameters,
            Some(ReplyParametersPlan {
                message_id: 77,
                chat_id: -100,
                allow_sending_without_reply: true,
            })
        );
        let entries = history.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].chat_id, -100);
        assert_eq!(entries[0].message_id, 500);
        assert_eq!(entries[0].sender_id, 555);
        assert_eq!(entries[0].role, openplotva_updates::HISTORY_ROLE_MODEL);
        let payload: serde_json::Value =
            serde_json::from_slice(&entries[0].payload).expect("history payload");
        assert_eq!(payload["kind"], "text");
        assert_eq!(payload["role"], openplotva_updates::HISTORY_ROLE_MODEL);
        assert_eq!(payload["meta"]["type"], "image");
        assert_eq!(payload["meta"]["sender_id"], 555);
        assert_eq!(payload["meta"]["attachments"][0]["kind"], "image");
        Ok(())
    }

    #[tokio::test]
    async fn direct_draw_api_runtime_preserves_go_rate_limit_skip_after_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let generator = DirectImageGeneratorStub::success(ImageGenerationResult {
            image_bytes: vec![b"png".to_vec()],
            ..ImageGenerationResult::default()
        });
        let sender = DirectDrawApiSenderStub::success(sent_photo_message()?);
        let rate_limits = Arc::new(DirectDrawApiRateLimitEffectsStub::limited());
        let permissions = Arc::new(DirectDrawApiPermissionEffectsStub::default());
        let effects = DirectDrawApiRuntimeEffects::with_sender(sender.clone(), generator.clone())
            .with_send_policies(rate_limits.clone(), permissions.clone());

        let report = effects
            .send_direct_draw_api(DirectDrawApiRequest {
                chat_id: -100,
                message_id: 77,
                user_id: 99,
                user_full_name: "Ada".to_owned(),
                prompt: "neon koi".to_owned(),
                thread_id: None,
                is_forum: false,
            })
            .await;

        assert_eq!(
            report,
            DirectDrawApiResult {
                sent: false,
                error: Some("rate limited".to_owned()),
            }
        );
        assert_eq!(generator.requests().len(), 1);
        assert!(sender.requests().is_empty());
        assert_eq!(rate_limits.checks(), vec![-100]);
        assert!(rate_limits.sets().is_empty());
        assert!(permissions.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn direct_draw_api_runtime_records_go_send_error_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let generator = DirectImageGeneratorStub::success(ImageGenerationResult {
            image_bytes: vec![b"png".to_vec()],
            ..ImageGenerationResult::default()
        });
        let sender = DirectDrawApiSenderStub::fail(DirectDrawApiSendError {
            message: "Forbidden: bot was kicked from the group chat".to_owned(),
            retry_after: Some(Duration::from_secs(12)),
            permission_error: true,
        });
        let rate_limits = Arc::new(DirectDrawApiRateLimitEffectsStub::default());
        let permissions = Arc::new(DirectDrawApiPermissionEffectsStub::default());
        let effects = DirectDrawApiRuntimeEffects::with_sender(sender.clone(), generator.clone())
            .with_send_policies(rate_limits.clone(), permissions.clone());

        let report = effects
            .send_direct_draw_api(DirectDrawApiRequest {
                chat_id: -100,
                message_id: 77,
                user_id: 99,
                user_full_name: "Ada".to_owned(),
                prompt: "neon koi".to_owned(),
                thread_id: None,
                is_forum: false,
            })
            .await;

        assert_eq!(
            report,
            DirectDrawApiResult {
                sent: false,
                error: Some("Forbidden: bot was kicked from the group chat".to_owned()),
            }
        );
        assert_eq!(sender.requests().len(), 1);
        assert_eq!(rate_limits.checks(), vec![-100]);
        assert_eq!(rate_limits.sets(), vec![(-100, Duration::from_secs(12))]);
        assert_eq!(permissions.calls(), vec![-100]);
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_percent_direct_draw_sends_photo_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-direct-draw:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        update_queue
            .enqueue_update(&message_update_at("% neon koi", now_secs as i64)?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded direct draw update"))?;

        let state_store = UpdateStateStoreStub::default();
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };
        let generator = DirectImageGeneratorStub::success(ImageGenerationResult {
            image_bytes: vec![b"png".to_vec()],
            image_urls: vec!["https://img.test/fallback.png".to_owned()],
            ..ImageGenerationResult::default()
        });
        let sender = TelegramMethodCapture::new(vec![Ok(TelegramOutboundResponse::Message(
            Box::new(sent_private_photo_message()?),
        ))]);
        let history = DirectDrawApiHistoryStoreStub::default();
        let direct_draw =
            DirectDrawApiRuntimeEffects::with_sender(sender.clone(), generator.clone())
                .with_history_store(Arc::new(history.clone()), 555);
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

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
            |update| async {
                let handled =
                    handle_dialog_or_random_message_update_or_else_with_image_and_direct_draw(
                        (
                            &scheduler,
                            None,
                            None,
                            Some(&direct_draw as &dyn DirectDrawApiEffects),
                        ),
                        &settings,
                        &effects,
                        &rng,
                        &test_config(),
                        update,
                        |_update| async {
                            Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                        },
                    )
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 12345);
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
                "chat:42:private:Ada:ada_l".to_owned(),
                "user:99:Ada:ada_l".to_owned()
            ]
        );
        assert_eq!(
            *lock(&route),
            Some(DialogMessageUpdateRoute::DirectDrawApi {
                sent: true,
                error: None,
            })
        );
        assert!(scheduler.calls().is_empty());

        let generation_requests = generator.requests();
        assert_eq!(generation_requests.len(), 1);
        assert_eq!(generation_requests[0].prompt, "neon koi");
        assert_eq!(generation_requests[0].caption_text, "neon koi");
        assert_eq!(generation_requests[0].chat_id, 42);
        assert_eq!(generation_requests[0].message_id, 77);

        let methods = sender.methods();
        assert_eq!(
            methods
                .iter()
                .map(|(kind, _payload)| *kind)
                .collect::<Vec<_>>(),
            vec![TelegramOutboundMethodKind::SendPhoto]
        );
        assert_eq!(methods[0].1["message_type"], "*api.PhotoConfig");
        assert_eq!(methods[0].1["message"]["ChatID"], 42);
        assert_eq!(methods[0].1["message"]["File"]["Name"], "image.png");
        assert_eq!(methods[0].1["message"]["File"]["Bytes"], "cG5n");
        assert_eq!(methods[0].1["message"]["ReplyParameters"]["message_id"], 77);
        assert_eq!(methods[0].1["message"]["ReplyParameters"]["chat_id"], 42);
        assert_eq!(
            methods[0].1["message"]["ReplyParameters"]["allow_sending_without_reply"],
            true
        );
        let entries = history.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].chat_id, 42);
        assert_eq!(entries[0].message_id, 500);
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[test]
    fn direct_draw_api_photo_request_prefers_bytes_then_url() {
        let request = DirectDrawApiRequest {
            chat_id: -100,
            message_id: 77,
            user_id: 99,
            user_full_name: "Ada".to_owned(),
            prompt: "neon koi".to_owned(),
            thread_id: Some(9),
            is_forum: true,
        };
        let bytes = direct_draw_api_photo_request(
            &request,
            &ImageGenerationResult {
                image_bytes: vec![b"png".to_vec()],
                image_urls: vec!["https://img.test/url.png".to_owned()],
                ..ImageGenerationResult::default()
            },
        )
        .expect("bytes photo");
        assert_eq!(
            bytes.photo,
            PhotoSource::Bytes {
                file_name: "image.png".to_owned(),
                bytes: b"png".to_vec(),
            }
        );

        let url = direct_draw_api_photo_request(
            &request,
            &ImageGenerationResult {
                image_urls: vec!["https://img.test/url.png".to_owned()],
                ..ImageGenerationResult::default()
            },
        )
        .expect("url photo");
        assert_eq!(
            url.photo,
            PhotoSource::Url("https://img.test/url.png".to_owned())
        );
    }

    #[tokio::test]
    async fn bang_song_shortcut_schedules_music_job_without_group_addressing()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(true)));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, Some(&song_scheduler)),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("!song neon rain")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::SongScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        assert!(scheduler.calls().is_empty());
        let records = queue.records();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.queue_name, MUSIC_VIP_QUEUE_NAME);
        assert_eq!(record.job.title, "music");
        assert_eq!(record.job.priority, HIGHEST_PRIORITY);
        let music = record.job.data.music_data.as_ref().expect("music");
        assert_eq!(music.topic, "neon rain");
        assert_eq!(music.reference_file_id, "");
        Ok(())
    }

    #[tokio::test]
    async fn targeted_group_song_command_schedules_music_job_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(true)));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, Some(&song_scheduler)),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("/song@plotva_bot neon rain")?,
            |_update| async { Err("targeted /song command should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::SongScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        assert!(scheduler.calls().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        let records = queue.records();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.queue_name, MUSIC_VIP_QUEUE_NAME);
        assert_eq!(record.job.title, "music");
        assert_eq!(record.job.priority, HIGHEST_PRIORITY);
        let music = record.job.data.music_data.as_ref().expect("music");
        assert_eq!(music.topic, "neon rain");
        assert_eq!(music.reference_file_id, "");
        Ok(())
    }

    #[tokio::test]
    async fn private_song_command_with_any_target_schedules_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(true)));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, Some(&song_scheduler)),
            &settings,
            &effects,
            &rng,
            &test_config(),
            message_update("/song@other_bot neon rain")?,
            |_update| async { Err("private targeted /song command should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::SongScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        assert!(scheduler.calls().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        let records = queue.records();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.queue_name, MUSIC_VIP_QUEUE_NAME);
        assert_eq!(record.job.title, "music");
        assert_eq!(record.job.priority, HIGHEST_PRIORITY);
        let music = record.job.data.music_data.as_ref().expect("music");
        assert_eq!(music.topic, "neon rain");
        assert_eq!(music.reference_file_id, "");
        Ok(())
    }

    #[tokio::test]
    async fn untargeted_group_song_commands_use_random_path_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(true)));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(0, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        for text in ["/song neon rain", "/song@other_bot neon rain"] {
            let route = handle_dialog_or_random_message_update_or_else_with_image(
                (&scheduler, None, Some(&song_scheduler)),
                &settings,
                &effects,
                &rng,
                &test_config(),
                group_message_update(text)?,
                |_update| async { Err("untargeted /song command should not delegate") },
            )
            .await?;

            assert_eq!(
                route,
                DialogMessageUpdateRoute::RandomSkippedReactivityOff,
                "{text}"
            );
        }
        assert!(scheduler.calls().is_empty());
        assert!(queue.records().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_song_shortcut_schedules_and_completes_music_job_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-song:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        update_queue
            .enqueue_update(&group_message_update_at(
                "!song neon rain",
                now_secs as i64,
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded song update"))?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(InMemoryTaskQueue::new());
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(true)));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

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
            |update| async {
                let handled = handle_dialog_or_random_message_update_or_else_with_image(
                    (&scheduler, None, Some(&song_scheduler)),
                    &settings,
                    &effects,
                    &rng,
                    &test_config(),
                    update,
                    |_update| async {
                        Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                    },
                )
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 22345);
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
                "chat:-100:supergroup:Group:".to_owned(),
                "user:99:Ada:ada_l".to_owned()
            ]
        );
        assert_eq!(
            *lock(&route),
            Some(DialogMessageUpdateRoute::SongScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            })
        );
        assert!(scheduler.calls().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        let job_id = {
            let records = queue.records();
            assert_eq!(records.len(), 1);
            let record = &records[0];
            assert_eq!(record.queue_name, MUSIC_VIP_QUEUE_NAME);
            assert_eq!(record.status, JobStatus::Pending);
            assert_eq!(record.job.title, "music");
            assert_eq!(record.job.priority, HIGHEST_PRIORITY);
            let telegram = record.job.data.telegram_data.as_ref().expect("telegram");
            assert_eq!(telegram.chat_id, -100);
            assert_eq!(telegram.message_id, 78);
            assert_eq!(telegram.user_id, 99);
            let music = record.job.data.music_data.as_ref().expect("music");
            assert_eq!(music.topic, "neon rain");
            assert_eq!(music.reference_file_id, "");
            record.id
        };

        let material_provider = MusicMaterialProviderStub::new(crate::music_jobs::SongMaterial {
            title: "Neon Rain".to_owned(),
            lyrics: "[Verse]\nneon rain".to_owned(),
            style: "synthwave, neon, 100 BPM".to_owned(),
            raw_style: "Synthwave, Neon, 100 bpm".to_owned(),
            vocal_language: "en".to_owned(),
        });
        let generator = MusicGeneratorStub::success(crate::music_jobs::GeneratedSongAudio {
            data: b"MP3".to_vec(),
            file_name: "neon-rain.mp3".to_owned(),
        });
        let permission_store = MusicPermissionStoreCapture::default();
        let permissions = Arc::new(crate::permissions::ChatPermissionPolicy::new(
            permission_store.clone(),
        ));
        let music_files = MusicReferenceFileStoreCapture::default();
        let music_sender = MusicTelegramSenderCapture::new(vec![
            Ok(TelegramOutboundResponse::Message(Box::new(
                telegram_bot_message(-100, 610)?,
            ))),
            Ok(TelegramOutboundResponse::Message(Box::new(
                telegram_bot_message(-100, 611)?,
            ))),
        ]);
        let music_effects = crate::music_jobs::TelegramMusicJobEffects::new(
            permissions,
            music_files.clone(),
            music_sender.clone(),
        );
        let worker_report = crate::music_jobs::run_music_queue_once(
            &queue,
            MUSIC_VIP_QUEUE_NAME,
            &material_provider,
            &generator,
            &music_effects,
            OffsetDateTime::now_utc(),
        )
        .await;
        assert_eq!(
            worker_report,
            crate::music_jobs::MusicQueuePollReport {
                queue_name: MUSIC_VIP_QUEUE_NAME.to_owned(),
                job_id: Some(job_id),
                outcome: crate::music_jobs::MusicQueuePollOutcome::Completed,
                error: None,
            }
        );
        assert_eq!(material_provider.calls(), vec!["99:neon rain".to_owned()]);
        assert_eq!(
            generator.requests(),
            vec![crate::music_jobs::MusicGenerationRequest {
                topic: "neon rain".to_owned(),
                material: crate::music_jobs::SongMaterial {
                    title: "Neon Rain".to_owned(),
                    lyrics: "[Verse]\nneon rain".to_owned(),
                    style: "synthwave, neon, 100 BPM".to_owned(),
                    raw_style: "Synthwave, Neon, 100 bpm".to_owned(),
                    vocal_language: "en".to_owned(),
                },
                reference_audio: None,
            }]
        );
        assert_eq!(permission_store.loads(), vec![-100]);
        assert!(permission_store.saves().is_empty());
        assert_eq!(music_files.lookups(), vec![String::new()]);
        assert_eq!(
            music_sender.downloads(),
            Vec::<String>::new(),
            "no reference audio should be downloaded for plain !song"
        );
        let music_methods = music_sender.methods();
        assert_eq!(
            music_methods
                .iter()
                .map(|(kind, _)| *kind)
                .collect::<Vec<_>>(),
            vec![
                TelegramOutboundMethodKind::SendAudio,
                TelegramOutboundMethodKind::SendMessage,
            ]
        );
        let audio_debug = music_methods[0].1["debug"].as_str().expect("audio debug");
        assert!(audio_debug.contains("-100"));
        assert!(audio_debug.contains("caption"));
        assert!(audio_debug.contains("<code>neon rain</code>"));
        assert!(audio_debug.contains("reply_parameters"));
        assert!(audio_debug.contains("78"));
        assert!(audio_debug.contains("HTML"));
        assert_eq!(music_methods[1].1["chat_id"], json!(-100));
        assert_eq!(music_methods[1].1["parse_mode"], json!("HTML"));
        assert_eq!(
            music_methods[1].1["reply_parameters"]["message_id"],
            json!(610)
        );
        assert!(
            music_methods[1].1["text"]
                .as_str()
                .unwrap_or_default()
                .contains("<b>Текст песни</b>")
        );
        assert!(
            music_methods[1].1["text"]
                .as_str()
                .unwrap_or_default()
                .contains("neon rain")
        );
        assert_eq!(queue.records()[0].status, JobStatus::Completed);
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_song_negative_notices_queue_ephemeral_payload_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };

        run_live_decoded_song_notice_case(LiveSongNoticeCase {
            redis_url: &redis_url,
            command: "!song",
            vip: true,
            music_available: true,
            audio_allowed: true,
            expected_route: DialogMessageUpdateRoute::SongMissingTopic,
            expected_text: DIRECT_SONG_TOPIC_REQUIRED_NOTICE,
            expected_parse_mode: None,
            expected_delete_after: Duration::from_secs(120),
            virtual_id: "song-notice-topic-vmsg",
        })
        .await?;
        run_live_decoded_song_notice_case(LiveSongNoticeCase {
            redis_url: &redis_url,
            command: "!song neon rain",
            vip: false,
            music_available: true,
            audio_allowed: true,
            expected_route: DialogMessageUpdateRoute::SongScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            },
            expected_text: DIRECT_SONG_VIP_ONLY_NOTICE,
            expected_parse_mode: Some(TELEGRAM_PARSE_MODE_HTML),
            expected_delete_after: Duration::from_secs(60),
            virtual_id: "song-notice-vip-vmsg",
        })
        .await?;
        run_live_decoded_song_notice_case(LiveSongNoticeCase {
            redis_url: &redis_url,
            command: "!song neon rain",
            vip: true,
            music_available: false,
            audio_allowed: true,
            expected_route: DialogMessageUpdateRoute::SongScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            },
            expected_text: DIRECT_SONG_SERVICE_UNAVAILABLE_NOTICE,
            expected_parse_mode: None,
            expected_delete_after: Duration::from_secs(120),
            virtual_id: "song-notice-service-vmsg",
        })
        .await?;
        run_live_decoded_song_notice_case(LiveSongNoticeCase {
            redis_url: &redis_url,
            command: "!song neon rain",
            vip: true,
            music_available: true,
            audio_allowed: false,
            expected_route: DialogMessageUpdateRoute::SongScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            },
            expected_text: DIRECT_SONG_AUDIO_NOT_ALLOWED_NOTICE,
            expected_parse_mode: None,
            expected_delete_after: Duration::from_secs(120),
            virtual_id: "song-notice-audio-vmsg",
        })
        .await?;

        Ok(())
    }

    struct LiveSongNoticeCase<'a> {
        redis_url: &'a str,
        command: &'a str,
        vip: bool,
        music_available: bool,
        audio_allowed: bool,
        expected_route: DialogMessageUpdateRoute,
        expected_text: &'a str,
        expected_parse_mode: Option<&'a str>,
        expected_delete_after: Duration,
        virtual_id: &'static str,
    }

    async fn run_live_decoded_song_notice_case(
        case: LiveSongNoticeCase<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let redis_client = redis::Client::open(case.redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!(
            "openplotva:test:decoded-song-notice:{}:{suffix}",
            case.virtual_id
        );
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        update_queue
            .enqueue_update(&group_message_update_at(case.command, now_secs as i64)?)
            .await?;
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded song notice update"))?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(InMemoryTaskQueue::new());
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(case.vip)))
            .with_song_service_available(case.music_available)
            .with_song_audio_permission(Arc::new(SongAudioPermissionStub(case.audio_allowed)));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let dispatch_store = VirtualStoreStub::default();
        let dispatcher_queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let virtual_id = case.virtual_id;
        let effects = RandomDialogDispatcherEffects::new(
            dispatch_store.clone(),
            Arc::clone(&dispatcher_queue),
        )
        .with_virtual_id_factory(Arc::new(move || virtual_id.to_owned()));
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

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
            |update| async {
                let handled = handle_dialog_or_random_message_update_or_else_with_image(
                    (&scheduler, None, Some(&song_scheduler)),
                    &settings,
                    &effects,
                    &rng,
                    &test_config(),
                    update,
                    |_update| async {
                        Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                    },
                )
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 22345);
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
                "chat:-100:supergroup:Group:".to_owned(),
                "user:99:Ada:ada_l".to_owned()
            ]
        );
        assert_eq!(*lock(&route), Some(case.expected_route));
        assert!(queue.records().is_empty());
        assert_eq!(
            dispatch_store.inserted(),
            vec![(case.virtual_id.to_owned(), -100, None)]
        );
        let snapshot = dispatcher_queue.snapshot();
        assert_eq!(snapshot.immediate.len(), 1);
        assert!(snapshot.regular.is_empty());
        assert_eq!(snapshot.immediate[0].virtual_id, case.virtual_id);
        assert_eq!(
            snapshot.immediate[0].ephemeral_delete_after,
            Some(case.expected_delete_after)
        );

        let item = dispatcher_queue
            .dequeue_immediate()
            .ok_or_else(|| std::io::Error::other("expected queued song notice"))?;
        assert_eq!(item.metadata().virtual_id, case.virtual_id);
        assert_eq!(
            item.ephemeral_delete_after(),
            Some(case.expected_delete_after)
        );
        assert!(!item.bypasses_chat_restrictions());
        let method = item
            .into_method()
            .ok_or_else(|| std::io::Error::other("expected song notice method"))?;
        assert_eq!(method.kind(), TelegramOutboundMethodKind::SendMessage);
        let payload = outbound_method_payload(&method);
        assert_eq!(payload["chat_id"], json!(-100));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(78));
        assert_eq!(payload["text"], json!(case.expected_text));
        if let Some(parse_mode) = case.expected_parse_mode {
            assert_eq!(payload["parse_mode"], json!(parse_mode));
        } else {
            assert!(payload.get("parse_mode").is_none());
        }
        assert_eq!(update_queue.len().await?, 0);
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn bang_song_shortcut_without_vip_sends_go_vip_notice()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(false)));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, Some(&song_scheduler)),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("!song neon rain")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::SongScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            }
        );
        assert!(queue.records().is_empty());
        let notices = effects.sent_song_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].message.text, DIRECT_SONG_VIP_ONLY_NOTICE);
        assert_eq!(notices[0].message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(notices[0].delete_after, Duration::from_secs(60));
        Ok(())
    }

    #[tokio::test]
    async fn bang_song_shortcut_with_backlog_sends_go_queue_notice()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        for user_id in [1, 2, 3] {
            queue.assign(
                MUSIC_VIP_QUEUE_NAME,
                new_music_gen_job_at(
                    MusicGenJobParams {
                        chat_id: -100,
                        message_id: user_id,
                        user_id: i64::from(user_id),
                        user_full_name: format!("User {user_id}"),
                        topic: "queued song".to_owned(),
                        ..MusicGenJobParams::default()
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .with_name("music")
                .with_priority(HIGHEST_PRIORITY),
            );
        }
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(true)));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, Some(&song_scheduler)),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("!song neon rain")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::SongScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        assert_eq!(queue.records().len(), 4);
        let notices = effects.sent_song_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(
            notices[0].message.text,
            "Песня добавлена в очередь. Перед вами 4 задач. Примерное ожидание: 2m24s"
        );
        assert_eq!(notices[0].delete_after, Duration::from_secs(300));
        Ok(())
    }

    #[tokio::test]
    async fn bang_song_shortcut_without_topic_sends_go_topic_notice()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(true)));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, Some(&song_scheduler)),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("!song")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(route, DialogMessageUpdateRoute::SongMissingTopic);
        assert!(queue.records().is_empty());
        let notices = effects.sent_song_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].message.text, DIRECT_SONG_TOPIC_REQUIRED_NOTICE);
        assert_eq!(notices[0].delete_after, Duration::from_secs(120));
        Ok(())
    }

    #[tokio::test]
    async fn bang_song_shortcut_without_scheduler_sends_go_service_notice_not_terminal()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, None, None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("!song neon rain")?,
            |_update| async { Ok::<(), std::convert::Infallible>(()) },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::SongScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            }
        );
        assert!(scheduler.calls().is_empty());
        let notices = effects.sent_song_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(
            notices[0].message.text,
            DIRECT_SONG_SERVICE_UNAVAILABLE_NOTICE
        );
        assert_eq!(notices[0].delete_after, Duration::from_secs(120));
        Ok(())
    }

    #[tokio::test]
    async fn addressed_song_uses_reply_audio_topic_and_reference()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(true)));
        let scheduler = SchedulerStub::default();

        let route = handle_dialog_message_update_or_else_with_image(
            &scheduler,
            None,
            Some(&song_scheduler),
            &test_config(),
            reply_audio_text_update("song")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::SongScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        assert!(scheduler.calls().is_empty());
        let records = queue.records();
        assert_eq!(records.len(), 1);
        let music = records[0].job.data.music_data.as_ref().expect("music");
        assert_eq!(music.topic, "Artist Title");
        assert_eq!(music.reference_file_id, "audio-file");
        assert_eq!(music.reference_file_unique_id, "audio-unique");
        Ok(())
    }

    #[tokio::test]
    async fn addressed_song_reply_video_filename_does_not_become_topic_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let song_scheduler = crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
            .with_draw_image_vip_status(Arc::new(VipStatusStub(true)));
        let scheduler = SchedulerStub::default();
        let effects = EffectsStub::default();

        let route = handle_dialog_message_update_or_else_with_image_and_song_notices(
            &scheduler,
            (None, Some(&song_scheduler), Some(&effects), None),
            &test_config(),
            reply_video_text_update("song")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(route, DialogMessageUpdateRoute::SongMissingTopic);
        assert!(scheduler.calls().is_empty());
        assert!(queue.records().is_empty());
        let notices = effects.sent_song_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].message.text, DIRECT_SONG_TOPIC_REQUIRED_NOTICE);
        assert_eq!(notices[0].delete_after, Duration::from_secs(120));
        Ok(())
    }

    #[tokio::test]
    async fn addressed_image_edit_command_routes_image_attachment_to_scheduler()
    -> Result<(), Box<dyn std::error::Error>> {
        let image_scheduler = ImageSchedulerCaptureStub::scheduled();
        let scheduler = SchedulerStub::default();

        let route = handle_dialog_message_update_or_else_with_image(
            &scheduler,
            Some(&image_scheduler),
            None,
            &test_config(),
            photo_caption_update("fix contrast")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::DrawImageScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        assert!(scheduler.calls().is_empty());
        let calls = image_scheduler.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].prompt, "contrast");
        assert_eq!(calls[0].message_text, "fix contrast");
        assert_eq!(calls[0].attachments.len(), 1);
        assert_eq!(calls[0].attachments[0].kind, "image");
        assert_eq!(calls[0].attachments[0].file_unique_id, "photo-1");
        assert_eq!(calls[0].attachments[0].caption, "fix contrast");
        Ok(())
    }

    #[tokio::test]
    async fn current_image_edit_captures_album_and_passes_media_group_id_to_scheduler()
    -> Result<(), Box<dyn std::error::Error>> {
        let image_scheduler = ImageSchedulerCaptureStub::scheduled();
        let scheduler = SchedulerStub::default();

        let route = handle_dialog_message_update_or_else_with_image(
            &scheduler,
            Some(&image_scheduler),
            None,
            &test_config(),
            photo_caption_media_group_update("fix contrast", "album-1")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::DrawImageScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        let captures = image_scheduler.capture_calls();
        assert_eq!(captures.len(), 1);
        assert_eq!(captures[0].0, "album-1");
        assert_eq!(captures[0].1, 77);
        assert_eq!(captures[0].2.len(), 1);
        assert_eq!(captures[0].2[0].file_unique_id, "photo-1");

        let calls = image_scheduler.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].edit_media_group_id, "album-1");
        assert_eq!(calls[0].attachments[0].file_unique_id, "photo-1");
        Ok(())
    }

    #[tokio::test]
    async fn image_edit_command_targets_replied_image_attachment()
    -> Result<(), Box<dyn std::error::Error>> {
        let image_scheduler = ImageSchedulerCaptureStub::scheduled();
        let scheduler = SchedulerStub::default();

        let route = handle_dialog_message_update_or_else_with_image(
            &scheduler,
            Some(&image_scheduler),
            None,
            &test_config(),
            reply_photo_text_update("fix brightness", "original caption")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::DrawImageScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        assert!(scheduler.calls().is_empty());
        let calls = image_scheduler.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].message_id, 79);
        assert_eq!(calls[0].prompt, "brightness");
        assert_eq!(calls[0].message_text, "fix brightness");
        assert_eq!(calls[0].attachments.len(), 1);
        assert_eq!(calls[0].attachments[0].file_unique_id, "reply-photo-1");
        assert_eq!(calls[0].attachments[0].caption, "original caption");
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_image_edit_schedules_and_completes_vip_job_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-image-edit:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        update_queue
            .enqueue_update(&photo_caption_update_at("fix contrast", now_secs as i64)?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded image edit update"))?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(InMemoryTaskQueue::new());
        let resolver = Arc::new(ImageEditFileResolverStub::successful(
            crate::dialog_tools::ImageEditFileSelection {
                photo_file_id: "latest-photo-file".to_owned(),
                photo_urls: vec!["https://files.test/photo.png".to_owned()],
            },
        ));
        let image_scheduler =
            crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
                .with_draw_image_vip_status(Arc::new(VipStatusStub(true)))
                .with_image_edit_file_resolver(resolver.clone());
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

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
            |update| async {
                let handled = handle_dialog_or_random_message_update_or_else_with_image(
                    (&scheduler, Some(&image_scheduler), None),
                    &settings,
                    &effects,
                    &rng,
                    &test_config(),
                    update,
                    |_update| async {
                        Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                    },
                )
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 12346);
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
                "chat:42:private:Ada:ada_l".to_owned(),
                "user:99:Ada:ada_l".to_owned(),
                "file:photo-1".to_owned(),
            ]
        );
        assert_eq!(
            *lock(&route),
            Some(DialogMessageUpdateRoute::DrawImageScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            })
        );
        assert!(scheduler.calls().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        assert_eq!(resolver.capture_calls().len(), 0);
        let resolver_calls = resolver.calls();
        assert_eq!(resolver_calls.len(), 1);
        assert_eq!(resolver_calls[0].0.len(), 1);
        assert_eq!(resolver_calls[0].0[0].file_unique_id, "photo-1");
        assert_eq!(resolver_calls[0].0[0].caption, "fix contrast");
        assert_eq!(resolver_calls[0].1, "");

        let job_id = {
            let records = queue.records();
            assert_eq!(records.len(), 1);
            let record = &records[0];
            assert_eq!(record.queue_name, IMAGE_VIP_QUEUE_NAME);
            assert_eq!(record.status, JobStatus::Pending);
            assert_eq!(record.job.title, "image_edit");
            assert_eq!(record.job.priority, HIGHEST_PRIORITY);
            assert_eq!(
                record.job.data.job_type,
                openplotva_taskman::JobType::ImageEdit
            );
            let telegram = record.job.data.telegram_data.as_ref().expect("telegram");
            assert_eq!(telegram.chat_id, 42);
            assert_eq!(telegram.message_id, 77);
            assert_eq!(telegram.user_id, 99);
            let image = record.job.data.image_data.as_ref().expect("image edit");
            assert_eq!(image.prompt, "contrast");
            assert_eq!(image.original_text, "contrast");
            assert_eq!(image.author, "Ada");
            assert_eq!(image.image_file_id, "latest-photo-file");
            assert_eq!(
                image.image_urls,
                vec!["https://files.test/photo.png".to_owned()]
            );
            assert!(image.is_image_edit);
            record.id
        };

        let editor = ImageEditProviderStub::success(vec![
            " https://img.test/edit-1.png ".to_owned(),
            String::new(),
            "https://img.test/edit-2.png".to_owned(),
        ]);
        let image_queued = ImageQueuedStickerCapture::default();
        let image_ephemeral = ImageEphemeralCapture::default();
        let image_sender = ImageTelegramSenderCapture::new(vec![
            Ok(TelegramOutboundResponse::Message(Box::new(
                telegram_bot_message(42, 880)?,
            ))),
            Ok(TelegramOutboundResponse::Boolean(true)),
        ]);
        let image_effects = crate::image_jobs::TelegramImageJobEffects::new(
            image_queued.clone(),
            image_ephemeral.clone(),
            image_sender.clone(),
        );
        let worker_report = crate::image_jobs::run_image_edit_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &editor,
            &image_effects,
            "image-edit-smoke-worker",
            OffsetDateTime::now_utc(),
        )
        .await;
        assert_eq!(
            worker_report,
            crate::image_jobs::ImageEditQueuePollReport {
                queue_name: IMAGE_VIP_QUEUE_NAME.to_owned(),
                job_id: Some(job_id),
                outcome: crate::image_jobs::ImageEditQueuePollOutcome::Completed,
                error: None,
            }
        );
        assert_eq!(
            editor.requests(),
            vec![crate::image_jobs::ImageEditRequest {
                chat_id: 42,
                message_id: 77,
                user_id: 99,
                user_full_name: "Ada".to_owned(),
                thread_id: None,
                prompt: "contrast".to_owned(),
                photo_file_id: "latest-photo-file".to_owned(),
                photo_urls: vec!["https://files.test/photo.png".to_owned()],
            }]
        );
        assert_eq!(image_queued.loads(), vec![(42, 77)]);
        assert!(image_queued.deletes().is_empty());
        assert_eq!(
            image_ephemeral.tracked(),
            vec![(42, 880, crate::image_jobs::DRAWING_STICKER_DELETE_AFTER)]
        );
        let image_methods = image_sender.methods();
        assert_eq!(
            image_methods
                .iter()
                .map(|(kind, _)| *kind)
                .collect::<Vec<_>>(),
            vec![
                TelegramOutboundMethodKind::SendSticker,
                TelegramOutboundMethodKind::DeleteMessage,
            ]
        );
        let sticker_debug = image_methods[0].1["debug"].as_str().expect("sticker debug");
        assert!(sticker_debug.contains("42"));
        assert!(sticker_debug.contains(crate::image_jobs::STICKER_DRAW_FILE_ID));
        assert!(sticker_debug.contains("disable_notification"));
        assert!(sticker_debug.contains("reply_parameters"));
        assert!(sticker_debug.contains("77"));
        assert_eq!(image_methods[1].1["chat_id"], json!(42));
        assert_eq!(image_methods[1].1["message_id"], json!(880));
        let records = queue.records();
        assert_eq!(records[0].status, JobStatus::Completed);
        assert_eq!(
            records[0]
                .job
                .data
                .image_data
                .as_ref()
                .expect("completed image edit")
                .image_urls,
            vec![
                "https://img.test/edit-1.png".to_owned(),
                "https://img.test/edit-2.png".to_owned(),
            ]
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_album_image_edit_captures_sibling_media_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-album-image-edit:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        update_queue
            .enqueue_update(&photo_caption_media_group_update_at(
                32368,
                77,
                "fix contrast",
                "album-1",
                "photo-file-1",
                "photo-1",
                now_secs,
            )?)
            .await?;
        update_queue
            .enqueue_update(&photo_media_group_update_at(
                32369,
                78,
                "album-1",
                "photo-file-2",
                "photo-2",
                now_secs,
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 2);

        let first = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded album edit update"))?;
        let second = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded album sibling update"))?;

        let state_store = UpdateStateStoreStub::default();
        let image_scheduler = AlbumImageSchedulerStub::default();
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };
        let first_route = Arc::new(Mutex::new(None));
        let second_route = Arc::new(Mutex::new(None));
        let image_scheduler_ref = &image_scheduler;
        let scheduler_ref = &scheduler;
        let settings_ref = &settings;
        let effects_ref = &effects;
        let rng_ref = &rng;

        let first_route_for_handler = Arc::clone(&first_route);
        let process_first = {
            process_update_with_state_store_at(
                first,
                UpdateConsumerConfig {
                    dequeue_timeout: Duration::from_millis(1),
                    state_timeout: Duration::from_secs(1),
                    handle_timeout: Duration::from_secs(1),
                    side_effect_max_age: Duration::from_secs(60),
                    worker_limit: 2,
                },
                UNIX_EPOCH + Duration::from_secs(now_secs as u64),
                &state_store,
                move |update| {
                    let first_route = Arc::clone(&first_route_for_handler);
                    async move {
                        let handled = handle_dialog_or_random_message_update_or_else_with_image(
                            (scheduler_ref, Some(image_scheduler_ref), None),
                            settings_ref,
                            effects_ref,
                            rng_ref,
                            &test_config(),
                            update,
                            |_update| async {
                                Err::<(), std::io::Error>(std::io::Error::other(
                                    "album edit update should not delegate",
                                ))
                            },
                        )
                        .await
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                        *lock(&first_route) = Some(handled);
                        Ok::<(), std::io::Error>(())
                    }
                },
            )
        };
        let second_route_for_handler = Arc::clone(&second_route);
        let process_second = {
            process_update_with_state_store_at(
                second,
                UpdateConsumerConfig {
                    dequeue_timeout: Duration::from_millis(1),
                    state_timeout: Duration::from_secs(1),
                    handle_timeout: Duration::from_secs(1),
                    side_effect_max_age: Duration::from_secs(60),
                    worker_limit: 2,
                },
                UNIX_EPOCH + Duration::from_secs(now_secs as u64),
                &state_store,
                move |update| {
                    let second_route = Arc::clone(&second_route_for_handler);
                    async move {
                        let handled = handle_dialog_or_random_message_update_or_else_with_image(
                            (scheduler_ref, Some(image_scheduler_ref), None),
                            settings_ref,
                            effects_ref,
                            rng_ref,
                            &test_config(),
                            update,
                            |_update| async {
                                Err::<(), std::io::Error>(std::io::Error::other(
                                    "album sibling media should not delegate",
                                ))
                            },
                        )
                        .await
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                        *lock(&second_route) = Some(handled);
                        Ok::<(), std::io::Error>(())
                    }
                },
            )
        };

        let (first_report, second_report) = tokio::join!(process_first, process_second);
        assert_eq!(first_report.update_id, 32368);
        assert_eq!(second_report.update_id, 32369);
        assert_eq!(first_report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(second_report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            first_report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            second_report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!first_report.skipped_handle);
        assert!(!second_report.skipped_handle);
        assert_eq!(
            *lock(&first_route),
            Some(DialogMessageUpdateRoute::DrawImageScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            })
        );
        assert_eq!(
            *lock(&second_route),
            Some(DialogMessageUpdateRoute::SkippedEmptyDialogTrigger)
        );

        let captures = image_scheduler.capture_calls();
        assert_eq!(captures.len(), 2);
        assert_eq!(captures[0].0, "album-1");
        assert_eq!(captures[0].1, 77);
        assert_eq!(captures[0].2[0].file_unique_id, "photo-1");
        assert_eq!(captures[1].0, "album-1");
        assert_eq!(captures[1].1, 78);
        assert_eq!(captures[1].2[0].file_unique_id, "photo-2");

        let calls = image_scheduler.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].message_id, 77);
        assert_eq!(calls[0].prompt, "contrast");
        assert_eq!(calls[0].edit_media_group_id, "album-1");
        assert_eq!(calls[0].attachments[0].file_unique_id, "photo-1");
        assert!(scheduler.calls().is_empty());
        assert!(effects.sent_texts().is_empty());

        let state_calls = state_store.calls();
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.as_str() == "chat:42:private:Ada:ada_l")
                .count(),
            2
        );
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.as_str() == "user:99:Ada:ada_l")
                .count(),
            2
        );
        assert!(state_calls.iter().any(|call| call == "file:photo-1"));
        assert!(state_calls.iter().any(|call| call == "file:photo-2"));
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_image_edit_negative_notices_queue_ephemeral_payload_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        run_live_decoded_image_edit_notice_case(LiveImageEditNoticeCase {
            redis_url: &redis_url,
            update: photo_caption_update_at("fix", now_secs)?,
            expected_update_id: 12346,
            vip: true,
            expected_state_calls: &[
                "chat:42:private:Ada:ada_l",
                "user:99:Ada:ada_l",
                "file:photo-1",
            ],
            expected_route: DialogMessageUpdateRoute::ImageEditMissingPrompt,
            expected_text: IMAGE_EDIT_MISSING_PROMPT_TEXT,
            expected_parse_mode: None,
            expected_delete_after: Duration::from_secs(120),
            virtual_id: "image-edit-notice-missing-prompt-vmsg",
        })
        .await?;

        run_live_decoded_image_edit_notice_case(LiveImageEditNoticeCase {
            redis_url: &redis_url,
            update: photo_caption_update_at("fix contrast", now_secs + 1)?,
            expected_update_id: 12346,
            vip: false,
            expected_state_calls: &[
                "chat:42:private:Ada:ada_l",
                "user:99:Ada:ada_l",
                "file:photo-1",
            ],
            expected_route: DialogMessageUpdateRoute::DrawImageScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            },
            expected_text: DIRECT_IMAGE_EDIT_VIP_ONLY_NOTICE,
            expected_parse_mode: Some(TELEGRAM_PARSE_MODE_HTML),
            expected_delete_after: Duration::from_secs(60),
            virtual_id: "image-edit-notice-vip-vmsg",
        })
        .await?;

        Ok(())
    }

    struct LiveImageEditNoticeCase<'a> {
        redis_url: &'a str,
        update: TelegramUpdate,
        expected_update_id: i64,
        vip: bool,
        expected_state_calls: &'a [&'a str],
        expected_route: DialogMessageUpdateRoute,
        expected_text: &'a str,
        expected_parse_mode: Option<&'a str>,
        expected_delete_after: Duration,
        virtual_id: &'static str,
    }

    async fn run_live_decoded_image_edit_notice_case(
        case: LiveImageEditNoticeCase<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let redis_client = redis::Client::open(case.redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!(
            "openplotva:test:decoded-image-edit-notice:{}:{suffix}",
            case.virtual_id
        );
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue.enqueue_update(&case.update).await?;
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| std::io::Error::other("expected decoded image-edit notice update"))?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(InMemoryTaskQueue::new());
        let resolver = Arc::new(ImageEditFileResolverStub::successful(
            crate::dialog_tools::ImageEditFileSelection {
                photo_file_id: "latest-photo-file".to_owned(),
                photo_urls: vec!["https://files.test/photo.png".to_owned()],
            },
        ));
        let image_scheduler =
            crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
                .with_draw_image_vip_status(Arc::new(VipStatusStub(case.vip)))
                .with_image_edit_file_resolver(resolver.clone());
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let dispatch_store = VirtualStoreStub::default();
        let dispatcher_queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let virtual_id = case.virtual_id;
        let effects = RandomDialogDispatcherEffects::new(
            dispatch_store.clone(),
            Arc::clone(&dispatcher_queue),
        )
        .with_virtual_id_factory(Arc::new(move || virtual_id.to_owned()));
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: Duration::from_millis(1),
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(60),
                worker_limit: 1,
            },
            SystemTime::now(),
            &state_store,
            |update| async {
                let handled = handle_dialog_or_random_message_update_or_else_with_image(
                    (&scheduler, Some(&image_scheduler), None),
                    &settings,
                    &effects,
                    &rng,
                    &test_config(),
                    update,
                    |_update| async {
                        Err::<(), std::io::Error>(std::io::Error::other("should not delegate"))
                    },
                )
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
                *lock(&captured_route) = Some(handled);
                Ok::<(), std::io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, case.expected_update_id);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!report.skipped_handle);
        assert_eq!(
            state_store.calls(),
            case.expected_state_calls
                .iter()
                .map(|call| (*call).to_owned())
                .collect::<Vec<_>>()
        );
        assert_eq!(*lock(&route), Some(case.expected_route));
        assert!(scheduler.calls().is_empty());
        assert!(queue.records().is_empty());
        assert!(resolver.calls().is_empty());
        assert!(resolver.capture_calls().is_empty());
        assert_eq!(
            dispatch_store.inserted(),
            vec![(case.virtual_id.to_owned(), 42, None)]
        );

        let snapshot = dispatcher_queue.snapshot();
        assert_eq!(snapshot.immediate.len(), 1);
        assert!(snapshot.regular.is_empty());
        assert_eq!(snapshot.immediate[0].virtual_id, case.virtual_id);
        assert_eq!(
            snapshot.immediate[0].ephemeral_delete_after,
            Some(case.expected_delete_after)
        );

        let item = dispatcher_queue
            .dequeue_immediate()
            .ok_or_else(|| std::io::Error::other("expected queued image-edit notice"))?;
        assert_eq!(item.metadata().virtual_id, case.virtual_id);
        assert_eq!(
            item.ephemeral_delete_after(),
            Some(case.expected_delete_after)
        );
        assert!(!item.bypasses_chat_restrictions());
        let method = item
            .into_method()
            .ok_or_else(|| std::io::Error::other("expected image-edit notice method"))?;
        assert_eq!(method.kind(), TelegramOutboundMethodKind::SendMessage);
        let payload = outbound_method_payload(&method);
        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(77));
        assert_eq!(payload["text"], json!(case.expected_text));
        if let Some(parse_mode) = case.expected_parse_mode {
            assert_eq!(payload["parse_mode"], json!(parse_mode));
        } else {
            assert!(payload.get("parse_mode").is_none());
        }
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn addressed_edit_without_image_schedules_dialog_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let image_scheduler = ImageSchedulerCaptureStub::scheduled();
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, Some(&image_scheduler), None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            message_update("fix contrast")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::Scheduled {
                queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                delay: Duration::ZERO,
                replaced: false,
            }
        );
        assert_eq!(scheduler.calls(), vec!["fix contrast".to_owned()]);
        assert!(image_scheduler.calls().is_empty());
        assert!(effects.sent_song_notices().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn image_edit_command_without_prompt_sends_go_error_notice()
    -> Result<(), Box<dyn std::error::Error>> {
        let image_scheduler = ImageSchedulerCaptureStub::scheduled();
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, Some(&image_scheduler), None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            photo_caption_update("fix")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(route, DialogMessageUpdateRoute::ImageEditMissingPrompt);
        assert!(scheduler.calls().is_empty());
        assert!(image_scheduler.calls().is_empty());
        let notices = effects.sent_song_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].message.text, IMAGE_EDIT_MISSING_PROMPT_TEXT);
        assert_eq!(notices[0].delete_after, Duration::from_secs(120));
        Ok(())
    }

    #[tokio::test]
    async fn draw_command_permission_rejections_send_go_failure_artifacts()
    -> Result<(), Box<dyn std::error::Error>> {
        for (rejection, restriction_text, restriction_delete_after) in [
            (
                DrawImageScheduleRejection::DrawDisabled,
                DIRECT_DRAW_DISABLED_NOTICE,
                Duration::from_secs(60),
            ),
            (
                DrawImageScheduleRejection::ImageNotAllowed,
                DIRECT_IMAGE_NOT_ALLOWED_NOTICE,
                Duration::from_secs(120),
            ),
        ] {
            let image_scheduler = ImageSchedulerCaptureStub::rejected(rejection);
            let scheduler = SchedulerStub::default();
            let effects = EffectsStub::default();

            let route = handle_dialog_message_update_or_else_with_image_and_song_notices(
                &scheduler,
                (Some(&image_scheduler), None, Some(&effects), None),
                &test_config(),
                message_update("!draw castle")?,
                |_update| async { Err("should not delegate") },
            )
            .await?;

            assert_eq!(
                route,
                DialogMessageUpdateRoute::DrawImageScheduled {
                    status: "not_scheduled".to_owned(),
                    no_reply: true,
                }
            );
            assert!(scheduler.calls().is_empty());
            assert_eq!(image_scheduler.calls().len(), 1);
            assert!(effects.sent_song_notices().is_empty());
            let failures = effects.sent_draw_failures();
            assert_eq!(failures.len(), 1);
            assert_eq!(
                failures[0].sticker.file_id,
                crate::image_jobs::STICKER_DOWN_FILE_ID
            );
            assert_eq!(failures[0].sticker_delete_after, Duration::from_secs(60));
            assert_eq!(
                failures[0].failure_notice.message.text,
                DIRECT_DRAW_FAILURE_TEXT
            );
            assert_eq!(
                failures[0].failure_notice.delete_after,
                Duration::from_secs(60)
            );
            assert_eq!(
                failures[0].restriction_notice.message.text,
                restriction_text
            );
            assert_eq!(
                failures[0].restriction_notice.delete_after,
                restriction_delete_after
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn draw_command_with_image_without_vip_sends_go_vip_notice_before_prompt_validation()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let image_scheduler =
            crate::dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&queue))
                .with_draw_image_vip_status(Arc::new(VipStatusStub(false)));
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::default();
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 0,
            obscenifier: 0,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else_with_image(
            (&scheduler, Some(&image_scheduler), None),
            &settings,
            &effects,
            &rng,
            &test_config(),
            photo_caption_update("!draw")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::DrawImageScheduled {
                status: "not_scheduled".to_owned(),
                no_reply: true,
            }
        );
        assert!(scheduler.calls().is_empty());
        assert!(queue.records().is_empty());
        let notices = effects.sent_song_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].message.text, DIRECT_IMAGE_EDIT_VIP_ONLY_NOTICE);
        assert_eq!(notices[0].delete_after, Duration::from_secs(60));
        Ok(())
    }

    #[tokio::test]
    async fn draw_command_with_replied_image_becomes_image_edit_with_caption_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let image_scheduler = ImageSchedulerCaptureStub::scheduled();
        let scheduler = SchedulerStub::default();

        let route = handle_dialog_message_update_or_else_with_image(
            &scheduler,
            Some(&image_scheduler),
            None,
            &test_config(),
            reply_photo_text_update("draw", "fix contrast")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::DrawImageScheduled {
                status: "scheduled".to_owned(),
                no_reply: false,
            }
        );
        assert!(scheduler.calls().is_empty());
        let calls = image_scheduler.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].prompt, "contrast",
            "Image-edit routing falls back to target image caption and parses edit verbs there"
        );
        assert_eq!(calls[0].attachments[0].file_unique_id, "reply-photo-1");
        Ok(())
    }

    #[tokio::test]
    async fn random_group_message_schedules_dialog_job_when_roll_triggers()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let scheduler = TaskmanDialogMessageScheduler::new(
            Arc::clone(&queue),
            Arc::clone(&debounce),
            Duration::from_millis(1),
        );
        let settings = SettingsStoreStub::with_chat(random_chat_settings(100, false));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else(
            &scheduler,
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("ordinary group talk")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::Scheduled {
                queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                delay: Duration::from_millis(1),
                replaced: false,
            }
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        let records = queue.records();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.queue_name, DIALOG_AIFARM_QUEUE_NAME);
        assert_eq!(record.job.title, "dialog (random)");
        assert_eq!(record.job.priority, HIGH_PRIORITY);
        let dialog = record.job.data.dialog_data.as_ref().expect("dialog");
        assert_eq!(dialog.message_text, "ordinary group talk");
        assert_eq!(dialog.max_output_tokens, 768);
        assert!(effects.sent_texts().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn random_group_message_skips_when_user_disabled()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings =
            SettingsStoreStub::with_chat(random_chat_settings(100, false)).with_user_disabled(true);
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 1,
            obscenify_variant: 0,
        };

        let route = handle_dialog_or_random_message_update_or_else(
            &scheduler,
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("ordinary group talk")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(route, DialogMessageUpdateRoute::RandomSkippedUserDisabled);
        assert!(scheduler.calls().is_empty());
        assert!(effects.sent_texts().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn random_obscenifier_sends_text_instead_of_dialog()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheduler = SchedulerStub::default();
        let settings = SettingsStoreStub::with_chat(random_chat_settings(100, true));
        let effects = EffectsStub::default();
        let rng = RngStub {
            random_response: 99,
            obscenifier: 0,
            obscenify_variant: 3,
        };

        let route = handle_dialog_or_random_message_update_or_else(
            &scheduler,
            &settings,
            &effects,
            &rng,
            &test_config(),
            group_message_update("что происходит")?,
            |_update| async { Err("should not delegate") },
        )
        .await?;

        assert_eq!(
            route,
            DialogMessageUpdateRoute::RandomObscenified {
                text: "пиздоисходит ".to_owned(),
                send_error: None,
            }
        );
        assert!(scheduler.calls().is_empty());
        assert_eq!(effects.sent_texts(), vec!["пиздоисходит ".to_owned()]);
        Ok(())
    }

    #[test]
    fn aifarm_token_budget_matches_go_phrase_tiers() {
        assert_eq!(
            aifarm_dialog_max_output_tokens(
                "максимально подробно расскажи",
                &ChatMessageMeta::default(),
                4096,
                1024,
                2048,
            ),
            4096
        );
        assert_eq!(
            aifarm_dialog_max_output_tokens(
                "объясни подробнее",
                &ChatMessageMeta::default(),
                4096,
                1024,
                2048,
            ),
            2048
        );
        assert_eq!(
            aifarm_dialog_max_output_tokens(
                "привет",
                &ChatMessageMeta::default(),
                4096,
                1024,
                2048,
            ),
            1024
        );
    }

    #[test]
    fn random_roll_matches_go_exclusive_bounds() {
        assert!(!random_response_triggers(3, 97));
        assert!(random_response_triggers(3, 98));
        assert!(!random_response_triggers(0, 99));
        assert!(!random_response_triggers(100, 0));
        assert!(random_response_triggers(100, 1));
    }

    #[test]
    fn obscenify_string_matches_go_variant_shapes() {
        assert_eq!(obscenify_string("что происходит", 0), "хуёисходит ");
        assert_eq!(
            obscenify_string("что происходит", 2),
            "происходит-шмоисходит "
        );
        assert_eq!(obscenify_string("без гласных", 1), "жопасных ");
        assert_eq!(obscenify_string("тсс", 1), "");
    }

    #[derive(Default)]
    struct SchedulerStub {
        calls: Mutex<Vec<String>>,
    }

    impl SchedulerStub {
        fn calls(&self) -> Vec<String> {
            lock(&self.calls).clone()
        }
    }

    impl DialogMessageScheduler for SchedulerStub {
        type Error = std::convert::Infallible;

        fn schedule_dialog_message<'a>(
            &'a self,
            schedule: DialogMessageSchedule<'a>,
        ) -> DialogMessageFuture<'a, DialogMessageScheduleReport, Self::Error> {
            Box::pin(async move {
                lock(&self.calls).push(schedule.message_text.to_owned());
                Ok(DialogMessageScheduleReport {
                    queue_name: schedule.queue_name.to_owned(),
                    delay: Duration::ZERO,
                    replaced: false,
                })
            })
        }
    }

    #[derive(Default)]
    struct ImageSchedulerCaptureStub {
        result: crate::dialog_tools::DrawImageScheduleResult,
        calls: Mutex<Vec<DrawImageScheduleRequest>>,
        capture_calls: Mutex<Vec<(String, i32, Vec<openplotva_core::ChatAttachment>)>>,
    }

    impl ImageSchedulerCaptureStub {
        fn scheduled() -> Self {
            Self {
                result: crate::dialog_tools::DrawImageScheduleResult {
                    status: "scheduled".to_owned(),
                    no_reply: false,
                    ..crate::dialog_tools::DrawImageScheduleResult::default()
                },
                ..Self::default()
            }
        }

        fn rejected(rejection: DrawImageScheduleRejection) -> Self {
            Self {
                result: crate::dialog_tools::DrawImageScheduleResult {
                    status: "not_scheduled".to_owned(),
                    no_reply: true,
                    rejection: Some(rejection),
                    ..crate::dialog_tools::DrawImageScheduleResult::default()
                },
                ..Self::default()
            }
        }

        fn calls(&self) -> Vec<DrawImageScheduleRequest> {
            lock(&self.calls).clone()
        }

        fn capture_calls(&self) -> Vec<(String, i32, Vec<openplotva_core::ChatAttachment>)> {
            lock(&self.capture_calls).clone()
        }
    }

    impl ImageScheduler for ImageSchedulerCaptureStub {
        fn capture_media_group_image(
            &self,
            media_group_id: &str,
            message_id: i32,
            attachments: &[openplotva_core::ChatAttachment],
        ) {
            let mut calls = lock(&self.capture_calls);
            if calls
                .iter()
                .any(|(group_id, id, _)| group_id == media_group_id && *id == message_id)
            {
                return;
            }
            calls.push((media_group_id.to_owned(), message_id, attachments.to_vec()));
        }

        fn schedule_image<'a>(
            &'a self,
            request: DrawImageScheduleRequest,
        ) -> crate::dialog_tools::DrawImageFuture<'a> {
            Box::pin(async move {
                lock(&self.calls).push(request);
                Ok(self.result.clone())
            })
        }
    }

    #[derive(Default)]
    struct AlbumImageSchedulerStub {
        calls: Mutex<Vec<DrawImageScheduleRequest>>,
        capture_calls: Mutex<Vec<(String, i32, Vec<openplotva_core::ChatAttachment>)>>,
    }

    impl AlbumImageSchedulerStub {
        fn calls(&self) -> Vec<DrawImageScheduleRequest> {
            lock(&self.calls).clone()
        }

        fn capture_calls(&self) -> Vec<(String, i32, Vec<openplotva_core::ChatAttachment>)> {
            lock(&self.capture_calls).clone()
        }
    }

    impl ImageScheduler for AlbumImageSchedulerStub {
        fn capture_media_group_image(
            &self,
            media_group_id: &str,
            message_id: i32,
            attachments: &[openplotva_core::ChatAttachment],
        ) {
            let mut calls = lock(&self.capture_calls);
            if calls
                .iter()
                .any(|(group_id, id, _)| group_id == media_group_id && *id == message_id)
            {
                return;
            }
            calls.push((media_group_id.to_owned(), message_id, attachments.to_vec()));
        }

        fn schedule_image<'a>(
            &'a self,
            request: DrawImageScheduleRequest,
        ) -> crate::dialog_tools::DrawImageFuture<'a> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(25)).await;
                lock(&self.calls).push(request);
                Ok(crate::dialog_tools::DrawImageScheduleResult {
                    status: "scheduled".to_owned(),
                    no_reply: false,
                    ..crate::dialog_tools::DrawImageScheduleResult::default()
                })
            })
        }
    }

    #[derive(Debug)]
    struct ImageEditFileResolverStub {
        result: crate::dialog_tools::ImageEditFileSelection,
        calls: Mutex<Vec<(Vec<openplotva_core::ChatAttachment>, String)>>,
        capture_calls: Mutex<Vec<(String, i32, Vec<openplotva_core::ChatAttachment>)>>,
    }

    impl ImageEditFileResolverStub {
        fn successful(result: crate::dialog_tools::ImageEditFileSelection) -> Self {
            Self {
                result,
                calls: Mutex::new(Vec::new()),
                capture_calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<(Vec<openplotva_core::ChatAttachment>, String)> {
            lock(&self.calls).clone()
        }

        fn capture_calls(&self) -> Vec<(String, i32, Vec<openplotva_core::ChatAttachment>)> {
            lock(&self.capture_calls).clone()
        }
    }

    impl crate::dialog_tools::ImageEditFileResolver for ImageEditFileResolverStub {
        fn capture_image_attachments(
            &self,
            media_group_id: &str,
            message_id: i32,
            attachments: &[openplotva_core::ChatAttachment],
        ) {
            lock(&self.capture_calls).push((
                media_group_id.to_owned(),
                message_id,
                attachments.to_vec(),
            ));
        }

        fn resolve_image_edit_files<'a>(
            &'a self,
            attachments: &'a [openplotva_core::ChatAttachment],
            media_group_id: &'a str,
        ) -> crate::dialog_tools::ImageEditFileResolveFuture<'a> {
            let result = self.result.clone();
            lock(&self.calls).push((attachments.to_vec(), media_group_id.to_owned()));
            Box::pin(async move { Ok(result) })
        }
    }

    #[derive(Clone, Debug)]
    struct MusicMaterialProviderStub {
        material: crate::music_jobs::SongMaterial,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl MusicMaterialProviderStub {
        fn new(material: crate::music_jobs::SongMaterial) -> Self {
            Self {
                material,
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<String> {
            lock(&self.calls).clone()
        }
    }

    impl crate::music_jobs::SongMaterialProvider for MusicMaterialProviderStub {
        fn build_song_material<'a>(
            &'a self,
            params: &'a MusicGenJobParams,
            topic: &'a str,
        ) -> crate::music_jobs::SongMaterialFuture<'a> {
            let material = self.material.clone();
            lock(&self.calls).push(format!("{}:{}", params.user_id, topic.trim()));
            Box::pin(async move { Ok(material) })
        }
    }

    #[derive(Clone, Debug)]
    struct MusicGeneratorStub {
        result: crate::music_jobs::GeneratedSongAudio,
        requests: Arc<Mutex<Vec<crate::music_jobs::MusicGenerationRequest>>>,
    }

    impl MusicGeneratorStub {
        fn success(result: crate::music_jobs::GeneratedSongAudio) -> Self {
            Self {
                result,
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<crate::music_jobs::MusicGenerationRequest> {
            lock(&self.requests).clone()
        }
    }

    impl crate::music_jobs::MusicGenerator for MusicGeneratorStub {
        fn generate_song<'a>(
            &'a self,
            request: crate::music_jobs::MusicGenerationRequest,
        ) -> crate::music_jobs::MusicGenerationFuture<'a> {
            let result = self.result.clone();
            lock(&self.requests).push(request);
            Box::pin(async move { Ok(result) })
        }
    }

    #[derive(Clone, Debug)]
    struct TestEffectError(&'static str);

    impl fmt::Display for TestEffectError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    #[derive(Clone, Debug, Default)]
    struct ImageQueuedStickerCapture {
        queued_id: Arc<Mutex<Option<i64>>>,
        loads: Arc<Mutex<Vec<(i64, i32)>>>,
        deletes: Arc<Mutex<Vec<(i64, i32)>>>,
    }

    impl ImageQueuedStickerCapture {
        fn loads(&self) -> Vec<(i64, i32)> {
            lock(&self.loads).clone()
        }

        fn deletes(&self) -> Vec<(i64, i32)> {
            lock(&self.deletes).clone()
        }
    }

    impl crate::image_jobs::QueuedStickerStore for ImageQueuedStickerCapture {
        type Error = TestEffectError;

        fn queued_sticker_message_id<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
        ) -> crate::image_jobs::ImageJobEffectFuture<'a, Result<Option<i64>, Self::Error>> {
            Box::pin(async move {
                lock(&self.loads).push((chat_id, message_id));
                Ok(*lock(&self.queued_id))
            })
        }

        fn delete_queued_sticker<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
        ) -> crate::image_jobs::ImageJobEffectFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                lock(&self.deletes).push((chat_id, message_id));
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct ImageEphemeralCapture {
        tracked: Arc<Mutex<Vec<(i64, i32, Duration)>>>,
    }

    impl ImageEphemeralCapture {
        fn tracked(&self) -> Vec<(i64, i32, Duration)> {
            lock(&self.tracked).clone()
        }
    }

    impl crate::virtual_messages::EphemeralMessageTracker for ImageEphemeralCapture {
        type Error = TestEffectError;

        fn track_ephemeral_message<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            delete_after: Duration,
            _now: OffsetDateTime,
        ) -> crate::image_jobs::ImageJobEffectFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                lock(&self.tracked).push((chat_id, message_id, delete_after));
                Ok(())
            })
        }
    }

    type ImageTelegramSenderCapture = TelegramMethodCapture;
    type MusicTelegramSenderCapture = TelegramMethodCapture;

    #[derive(Clone, Debug)]
    struct TelegramMethodCapture {
        state: Arc<Mutex<TelegramMethodCaptureState>>,
    }

    #[derive(Debug)]
    struct TelegramMethodCaptureState {
        methods: Vec<(TelegramOutboundMethodKind, Value)>,
        responses: VecDeque<Result<TelegramOutboundResponse, String>>,
        downloads: Vec<String>,
    }

    impl TelegramMethodCapture {
        fn new(responses: Vec<Result<TelegramOutboundResponse, String>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(TelegramMethodCaptureState {
                    methods: Vec::new(),
                    responses: responses.into(),
                    downloads: Vec::new(),
                })),
            }
        }

        fn methods(&self) -> Vec<(TelegramOutboundMethodKind, Value)> {
            lock(&self.state).methods.clone()
        }

        fn downloads(&self) -> Vec<String> {
            lock(&self.state).downloads.clone()
        }

        fn record_method(
            &self,
            method: &TelegramOutboundMethod,
        ) -> Result<TelegramOutboundResponse, String> {
            self.record_method_with_payload(method, outbound_method_payload(method))
        }

        fn record_method_with_payload(
            &self,
            method: &TelegramOutboundMethod,
            payload: Value,
        ) -> Result<TelegramOutboundResponse, String> {
            let mut state = lock(&self.state);
            state.methods.push((method.kind(), payload));
            state
                .responses
                .pop_front()
                .unwrap_or(Ok(TelegramOutboundResponse::Boolean(true)))
        }
    }

    impl crate::image_jobs::ImageJobTelegramSender for TelegramMethodCapture {
        fn send_image_job_method<'a>(
            &'a self,
            method: TelegramOutboundMethod,
        ) -> crate::image_jobs::ImageJobEffectFuture<'a, Result<TelegramOutboundResponse, String>>
        {
            Box::pin(async move { self.record_method(&method) })
        }
    }

    impl crate::music_jobs::MusicTelegramSender for TelegramMethodCapture {
        fn send_music_method<'a>(
            &'a self,
            method: TelegramOutboundMethod,
        ) -> crate::music_jobs::MusicJobEffectFuture<'a, Result<TelegramOutboundResponse, String>>
        {
            Box::pin(async move { self.record_method(&method) })
        }

        fn download_music_file<'a>(
            &'a self,
            file_id: &'a str,
        ) -> crate::music_jobs::MusicJobEffectFuture<'a, Result<Vec<u8>, String>> {
            Box::pin(async move {
                lock(&self.state).downloads.push(file_id.to_owned());
                Ok(Vec::new())
            })
        }
    }

    impl DirectDrawApiTelegramSender for TelegramMethodCapture {
        fn send_direct_draw_api_photo<'a>(
            &'a self,
            request: PhotoMessageRequest,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<carapax::types::Message, DirectDrawApiSendError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                let plan = openplotva_telegram::build_photo_message_plan(&request)
                    .map_err(|error| DirectDrawApiSendError::plain(error.to_string()))?;
                let payload = plan
                    .to_persistence_payload()
                    .map_err(|error| DirectDrawApiSendError::plain(error.to_string()))?;
                let method = openplotva_telegram::build_photo_message_method(&request)
                    .map_err(|error| DirectDrawApiSendError::plain(error.to_string()))?;
                let response = self
                    .record_method_with_payload(
                        &TelegramOutboundMethod::from(method),
                        persistence_payload_json(&payload)
                            .map_err(DirectDrawApiSendError::plain)?,
                    )
                    .map_err(DirectDrawApiSendError::plain)?;
                match response {
                    TelegramOutboundResponse::Message(message) => Ok(*message),
                    other => Err(DirectDrawApiSendError::plain(format!(
                        "sendPhoto returned {other:?}"
                    ))),
                }
            })
        }
    }

    fn outbound_method_payload(method: &TelegramOutboundMethod) -> Value {
        match method {
            TelegramOutboundMethod::SendMessage(method) => {
                serde_json::to_value(&**method).expect("sendMessage payload")
            }
            TelegramOutboundMethod::SendSticker(method) => {
                json!({ "debug": format!("{method:?}") })
            }
            TelegramOutboundMethod::SendPhoto(method) => {
                json!({ "debug": format!("{method:?}") })
            }
            TelegramOutboundMethod::SendAudio(method) => {
                json!({ "debug": format!("{method:?}") })
            }
            TelegramOutboundMethod::DeleteMessage(method) => {
                serde_json::to_value(&**method).expect("deleteMessage payload")
            }
            _ => Value::Null,
        }
    }

    fn persistence_payload_json(
        payload: &openplotva_telegram::DispatcherPersistencePayload,
    ) -> Result<Value, String> {
        let message: Value =
            serde_json::from_slice(&payload.message).map_err(|error| error.to_string())?;
        Ok(json!({
            "message_type": payload.message_type.clone(),
            "message": message,
        }))
    }

    #[derive(Clone, Debug, Default)]
    struct MusicPermissionStoreCapture {
        loads: Arc<Mutex<Vec<i64>>>,
        saves: Arc<Mutex<Vec<i64>>>,
    }

    impl MusicPermissionStoreCapture {
        fn loads(&self) -> Vec<i64> {
            lock(&self.loads).clone()
        }

        fn saves(&self) -> Vec<i64> {
            lock(&self.saves).clone()
        }
    }

    impl crate::permissions::ChatPermissionStore for MusicPermissionStoreCapture {
        type Error = TestEffectError;

        fn load_context<'a>(
            &'a self,
            chat_id: i64,
        ) -> crate::permissions::ChatPermissionStoreFuture<
            'a,
            Result<crate::permissions::ChatPermissionContext, Self::Error>,
        > {
            Box::pin(async move {
                lock(&self.loads).push(chat_id);
                Ok(crate::permissions::ChatPermissionContext {
                    chat_type: Some("supergroup".to_owned()),
                    settings: None,
                })
            })
        }

        fn save_settings<'a>(
            &'a self,
            update: openplotva_core::ChatSettingsUpdate,
        ) -> crate::permissions::ChatPermissionStoreFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                lock(&self.saves).push(update.chat_id);
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct MusicReferenceFileStoreCapture {
        lookups: Arc<Mutex<Vec<String>>>,
    }

    impl MusicReferenceFileStoreCapture {
        fn lookups(&self) -> Vec<String> {
            lock(&self.lookups).clone()
        }
    }

    impl crate::music_jobs::MusicReferenceFileStore for MusicReferenceFileStoreCapture {
        type Error = TestEffectError;

        fn music_file_by_unique_id<'a>(
            &'a self,
            file_unique_id: &'a str,
        ) -> crate::music_jobs::MusicJobEffectFuture<
            'a,
            Result<Option<openplotva_storage::TelegramFileRecord>, Self::Error>,
        > {
            Box::pin(async move {
                lock(&self.lookups).push(file_unique_id.to_owned());
                Ok(None)
            })
        }
    }

    #[derive(Clone, Debug)]
    struct DialogProviderStub {
        output: openplotva_dialog::DialogOutput,
        inputs: Arc<Mutex<Vec<openplotva_dialog::DialogInput>>>,
    }

    impl DialogProviderStub {
        fn returning(answer: &str) -> Self {
            Self {
                output: openplotva_dialog::DialogOutput {
                    provider: "stub".to_owned(),
                    answer: answer.to_owned(),
                    ..openplotva_dialog::DialogOutput::default()
                },
                inputs: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn inputs(&self) -> Vec<openplotva_dialog::DialogInput> {
            lock(&self.inputs).clone()
        }
    }

    impl openplotva_llm::ChatProvider for DialogProviderStub {
        fn provider_name(&self) -> &str {
            "stub"
        }

        fn run_dialog<'a>(
            &'a self,
            input: openplotva_dialog::DialogInput,
        ) -> openplotva_llm::ChatProviderFuture<'a> {
            let output = self.output.clone();
            lock(&self.inputs).push(input);
            Box::pin(async move { Ok(output) })
        }
    }

    #[derive(Clone, Default)]
    struct VirtualStoreStub {
        inserted: Arc<Mutex<VirtualInsertCalls>>,
    }

    type VirtualInsertCalls = Vec<(String, i64, Option<i32>)>;

    impl VirtualStoreStub {
        fn inserted(&self) -> Vec<(String, i64, Option<i32>)> {
            lock(&self.inserted).clone()
        }
    }

    impl crate::virtual_messages::VirtualMessageStore for VirtualStoreStub {
        type Error = std::io::Error;

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
            Box::pin(async move {
                lock(&self.inserted).push((vmsg_id, chat_id, thread_id));
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

    #[derive(Debug)]
    struct VipStatusStub(bool);

    impl crate::dialog_tools::DrawImageVipStatus for VipStatusStub {
        fn is_draw_image_vip<'a>(
            &'a self,
            _user_id: i64,
        ) -> crate::dialog_tools::DrawImageVipStatusFuture<'a> {
            Box::pin(async move { self.0 })
        }
    }

    #[derive(Debug)]
    struct SongAudioPermissionStub(bool);

    impl crate::dialog_tools::SongAudioPermission for SongAudioPermissionStub {
        fn can_send_song_audio<'a>(
            &'a self,
            _chat_id: i64,
        ) -> crate::dialog_tools::SongAudioPermissionFuture<'a> {
            Box::pin(async move { self.0 })
        }
    }

    #[derive(Default)]
    struct SettingsStoreStub {
        chat: Mutex<Option<ChatSettings>>,
        user: Mutex<Option<UserSettings>>,
    }

    impl SettingsStoreStub {
        fn with_chat(chat: ChatSettings) -> Self {
            Self {
                chat: Mutex::new(Some(chat)),
                user: Mutex::new(None),
            }
        }

        fn with_user_disabled(self, disabled: bool) -> Self {
            *lock(&self.user) = Some(UserSettings {
                user_id: 99,
                disable_random_reactivity: disabled,
                hide_original_draw_prompt: false,
                updated: OffsetDateTime::UNIX_EPOCH,
            });
            self
        }
    }

    impl RandomDialogSettingsStore for SettingsStoreStub {
        type Error = std::convert::Infallible;

        fn random_chat_settings<'a>(
            &'a self,
            _chat_id: i64,
        ) -> DialogMessageFuture<'a, Option<ChatSettings>, Self::Error> {
            Box::pin(async move { Ok(lock(&self.chat).clone()) })
        }

        fn random_user_settings<'a>(
            &'a self,
            _user_id: i64,
        ) -> DialogMessageFuture<'a, Option<UserSettings>, Self::Error> {
            Box::pin(async move { Ok(lock(&self.user).clone()) })
        }
    }

    #[derive(Clone, Copy)]
    struct RngStub {
        random_response: i32,
        obscenifier: i32,
        obscenify_variant: i32,
    }

    impl RandomDialogRng for RngStub {
        fn random_response_roll(&self) -> i32 {
            self.random_response
        }

        fn obscenifier_roll(&self) -> i32 {
            self.obscenifier
        }

        fn obscenify_variant_roll(&self) -> i32 {
            self.obscenify_variant
        }
    }

    #[derive(Default)]
    struct EffectsStub {
        texts: Mutex<Vec<String>>,
        song_notices: Mutex<Vec<DirectSongNoticePlan>>,
        draw_failures: Mutex<Vec<DirectDrawFailurePlan>>,
    }

    impl EffectsStub {
        fn sent_texts(&self) -> Vec<String> {
            lock(&self.texts).clone()
        }

        fn sent_song_notices(&self) -> Vec<DirectSongNoticePlan> {
            lock(&self.song_notices).clone()
        }

        fn sent_draw_failures(&self) -> Vec<DirectDrawFailurePlan> {
            lock(&self.draw_failures).clone()
        }
    }

    impl RandomDialogEffects for EffectsStub {
        type Error = std::convert::Infallible;

        fn send_random_obscenified_text<'a>(
            &'a self,
            plan: RandomObscenifiedTextPlan,
        ) -> DialogMessageFuture<'a, (), Self::Error> {
            Box::pin(async move {
                lock(&self.texts).push(plan.message.text);
                Ok(())
            })
        }
    }

    impl DirectSongNoticeEffects for EffectsStub {
        fn send_direct_song_notice<'a>(
            &'a self,
            plan: DirectSongNoticePlan,
        ) -> DirectSongNoticeFuture<'a> {
            Box::pin(async move {
                lock(&self.song_notices).push(plan);
                Ok(())
            })
        }

        fn send_direct_draw_failure<'a>(
            &'a self,
            plan: DirectDrawFailurePlan,
        ) -> DirectDrawFailureFuture<'a> {
            Box::pin(async move {
                lock(&self.draw_failures).push(plan);
                Ok(())
            })
        }
    }

    #[derive(Clone)]
    struct DirectImageGeneratorStub {
        result: Result<ImageGenerationResult, ImageGenerationError>,
        requests: Arc<Mutex<Vec<ImageGenerationRequest>>>,
    }

    impl DirectImageGeneratorStub {
        fn success(result: ImageGenerationResult) -> Self {
            Self {
                result: Ok(result),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<ImageGenerationRequest> {
            lock(&self.requests).clone()
        }
    }

    impl ImageGenerator for DirectImageGeneratorStub {
        fn generate_image<'a>(
            &'a self,
            request: ImageGenerationRequest,
        ) -> crate::image_jobs::ImageGenerationFuture<'a> {
            let result = self.result.clone();
            lock(&self.requests).push(request);
            Box::pin(async move { result })
        }
    }

    #[derive(Clone, Default)]
    struct DirectDrawApiSenderStub {
        requests: Arc<Mutex<Vec<PhotoMessageRequest>>>,
        error: Arc<Mutex<Option<DirectDrawApiSendError>>>,
        response: Arc<Mutex<Option<carapax::types::Message>>>,
    }

    impl DirectDrawApiSenderStub {
        fn success(response: carapax::types::Message) -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                error: Arc::new(Mutex::new(None)),
                response: Arc::new(Mutex::new(Some(response))),
            }
        }

        fn requests(&self) -> Vec<PhotoMessageRequest> {
            lock(&self.requests).clone()
        }

        fn fail(error: DirectDrawApiSendError) -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                error: Arc::new(Mutex::new(Some(error))),
                response: Arc::new(Mutex::new(None)),
            }
        }
    }

    impl DirectDrawApiTelegramSender for DirectDrawApiSenderStub {
        fn send_direct_draw_api_photo<'a>(
            &'a self,
            request: PhotoMessageRequest,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<carapax::types::Message, DirectDrawApiSendError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                lock(&self.requests).push(request);
                if let Some(error) = lock(&self.error).clone() {
                    return Err(error);
                }
                lock(&self.response)
                    .clone()
                    .ok_or_else(|| DirectDrawApiSendError::plain("missing response"))
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct DirectDrawApiRateLimitEffectsStub {
        rate_limited: bool,
        checks: Arc<Mutex<Vec<i64>>>,
        sets: Arc<Mutex<Vec<(i64, Duration)>>>,
    }

    impl DirectDrawApiRateLimitEffectsStub {
        fn limited() -> Self {
            Self {
                rate_limited: true,
                checks: Arc::new(Mutex::new(Vec::new())),
                sets: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn checks(&self) -> Vec<i64> {
            lock(&self.checks).clone()
        }

        fn sets(&self) -> Vec<(i64, Duration)> {
            lock(&self.sets).clone()
        }
    }

    impl DirectDrawApiRateLimitEffects for DirectDrawApiRateLimitEffectsStub {
        fn is_direct_draw_api_rate_limited<'a>(
            &'a self,
            chat_id: i64,
            _now: OffsetDateTime,
        ) -> Pin<Box<dyn Future<Output = DirectDrawApiRateLimitReport> + Send + 'a>> {
            Box::pin(async move {
                lock(&self.checks).push(chat_id);
                DirectDrawApiRateLimitReport {
                    rate_limited: self.rate_limited,
                    load_error: None,
                }
            })
        }

        fn set_direct_draw_api_rate_limit<'a>(
            &'a self,
            chat_id: i64,
            retry_after: Duration,
            _now: OffsetDateTime,
        ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
            Box::pin(async move {
                lock(&self.sets).push((chat_id, retry_after));
                None
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct DirectDrawApiPermissionEffectsStub {
        calls: Arc<Mutex<Vec<i64>>>,
    }

    impl DirectDrawApiPermissionEffectsStub {
        fn calls(&self) -> Vec<i64> {
            lock(&self.calls).clone()
        }
    }

    impl DirectDrawApiPermissionEffects for DirectDrawApiPermissionEffectsStub {
        fn record_direct_draw_api_permission_error<'a>(
            &'a self,
            chat_id: i64,
        ) -> Pin<Box<dyn Future<Output = DirectDrawApiPermissionReport> + Send + 'a>> {
            Box::pin(async move {
                lock(&self.calls).push(chat_id);
                DirectDrawApiPermissionReport {
                    load_error: None,
                    save_error: None,
                }
            })
        }
    }

    #[derive(Clone, Debug)]
    struct ImageEditProviderStub {
        result: Result<crate::image_jobs::ImageEditResult, crate::image_jobs::ImageEditError>,
        requests: Arc<Mutex<Vec<crate::image_jobs::ImageEditRequest>>>,
    }

    impl ImageEditProviderStub {
        fn success(image_urls: Vec<String>) -> Self {
            Self {
                result: Ok(crate::image_jobs::ImageEditResult {
                    image_urls,
                    ..crate::image_jobs::ImageEditResult::default()
                }),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<crate::image_jobs::ImageEditRequest> {
            lock(&self.requests).clone()
        }
    }

    impl crate::image_jobs::ImageEditor for ImageEditProviderStub {
        fn edit_image<'a>(
            &'a self,
            request: crate::image_jobs::ImageEditRequest,
        ) -> crate::image_jobs::ImageEditFuture<'a> {
            let result = self.result.clone();
            lock(&self.requests).push(request);
            Box::pin(async move { result })
        }
    }

    #[derive(Clone, Default)]
    struct DirectDrawApiHistoryStoreStub {
        entries: Arc<Mutex<Vec<HistoryTextEntry>>>,
    }

    impl DirectDrawApiHistoryStoreStub {
        fn entries(&self) -> Vec<HistoryTextEntry> {
            lock(&self.entries).clone()
        }
    }

    impl DirectDrawApiHistoryStore for DirectDrawApiHistoryStoreStub {
        fn record_direct_draw_api_history<'a>(
            &'a self,
            entry: HistoryTextEntry,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                lock(&self.entries).push(entry);
                Ok(())
            })
        }
    }

    #[derive(Clone)]
    struct DirectDrawApiEffectsStub {
        result: DirectDrawApiResult,
        calls: Arc<Mutex<Vec<DirectDrawApiRequest>>>,
    }

    impl DirectDrawApiEffectsStub {
        fn sent() -> Self {
            Self {
                result: DirectDrawApiResult {
                    sent: true,
                    error: None,
                },
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<DirectDrawApiRequest> {
            lock(&self.calls).clone()
        }
    }

    impl DirectDrawApiEffects for DirectDrawApiEffectsStub {
        fn send_direct_draw_api<'a>(
            &'a self,
            request: DirectDrawApiRequest,
        ) -> DirectDrawApiFuture<'a> {
            let result = self.result.clone();
            lock(&self.calls).push(request);
            Box::pin(async move { result })
        }
    }

    #[derive(Clone, Default)]
    struct UpdateStateStoreStub {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl UpdateStateStoreStub {
        fn calls(&self) -> Vec<String> {
            lock(&self.calls).clone()
        }
    }

    impl UpdateStateStore for UpdateStateStoreStub {
        type Error = std::io::Error;

        fn upsert_chat_state<'a>(
            &'a self,
            chat: &'a openplotva_core::ChatState,
        ) -> UpdateStateStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                lock(&self.calls).push(format!(
                    "chat:{}:{}:{}:{}",
                    chat.id,
                    chat.chat_type,
                    chat.title
                        .as_deref()
                        .or(chat.first_name.as_deref())
                        .unwrap_or_default(),
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
                lock(&self.calls).push(format!(
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
                lock(&self.calls).push(format!("file:{}", params.file_unique_id));
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct TypingEffectsStub {
        calls: Mutex<Vec<(i64, Option<i32>)>>,
    }

    impl TypingEffectsStub {
        fn calls(&self) -> Vec<(i64, Option<i32>)> {
            lock(&self.calls).clone()
        }
    }

    impl DialogTypingActionEffects for TypingEffectsStub {
        fn send_dialog_typing_action<'a>(
            &'a self,
            chat_id: i64,
            thread_id: Option<i32>,
        ) -> Pin<Box<dyn Future<Output = DialogTypingActionReport> + Send + 'a>> {
            Box::pin(async move {
                lock(&self.calls).push((chat_id, thread_id));
                DialogTypingActionReport::Sent
            })
        }
    }

    fn test_config() -> DialogMessageUpdateConfig {
        let mut bot_user = TelegramUser::new(555, "Plotva".to_owned(), true);
        bot_user.username = Some("plotva_bot".to_owned().into());
        DialogMessageUpdateConfig {
            bot_user,
            queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
            use_aifarm_budgets: true,
            aifarm_max_tokens: 4096,
            aifarm_random_max_tokens: 768,
            aifarm_default_max_tokens: 1024,
            aifarm_long_max_tokens: 2048,
            processing_timeout_seconds: 720,
        }
    }

    fn random_chat_settings(reactivity: i32, obscenifier: bool) -> ChatSettings {
        ChatSettings {
            reactivity_percentage: reactivity,
            enable_obscenifier: obscenifier,
            ..ChatSettings::defaults(-100)
        }
    }

    fn message_update(text: &str) -> Result<TelegramUpdate, serde_json::Error> {
        message_update_at(text, 1_710_000_000)
    }

    fn message_update_at(text: &str, date: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12345,
            "message": {
                "message_id": 77,
                "date": date,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": text
            }
        }))
    }

    fn sent_private_photo_message() -> Result<carapax::types::Message, serde_json::Error> {
        serde_json::from_value(json!({
            "message_id": 500,
            "date": 1_710_000_100,
            "chat": {
                "id": 42,
                "type": "private",
                "first_name": "Ada",
                "username": "ada_l"
            },
            "from": {
                "id": 555,
                "is_bot": true,
                "first_name": "Plotva",
                "username": "plotva_bot"
            },
            "photo": [
                {
                    "file_id": "generated-photo-file",
                    "file_unique_id": "generated-photo-unique",
                    "height": 512,
                    "width": 512
                }
            ]
        }))
    }

    fn telegram_bot_message(
        chat_id: i64,
        message_id: i64,
    ) -> Result<carapax::types::Message, serde_json::Error> {
        serde_json::from_value(json!({
            "message_id": message_id,
            "date": 1_710_000_100,
            "chat": {
                "id": chat_id,
                "type": if chat_id < 0 { "supergroup" } else { "private" },
                "title": if chat_id < 0 { "Group" } else { "" },
                "first_name": if chat_id < 0 { "" } else { "Ada" }
            },
            "from": {
                "id": 555,
                "is_bot": true,
                "first_name": "Plotva",
                "username": "plotva_bot"
            }
        }))
    }

    fn sent_photo_message() -> Result<carapax::types::Message, serde_json::Error> {
        serde_json::from_value(json!({
            "message_id": 500,
            "date": 1_710_000_100,
            "chat": {
                "id": -100,
                "type": "supergroup",
                "title": "Group",
                "is_forum": true
            },
            "from": {
                "id": 555,
                "is_bot": true,
                "first_name": "Plotva",
                "username": "plotva_bot"
            },
            "message_thread_id": 9,
            "photo": [
                {
                    "file_id": "generated-photo-file",
                    "file_unique_id": "generated-photo-unique",
                    "height": 512,
                    "width": 512
                }
            ]
        }))
    }

    fn photo_caption_update(caption: &str) -> Result<TelegramUpdate, serde_json::Error> {
        photo_caption_update_at(caption, 1_710_000_000)
    }

    fn photo_caption_update_at(
        caption: &str,
        date: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12346,
            "message": {
                "message_id": 77,
                "date": date,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "caption": caption,
                "photo": [
                    {
                        "file_id": "photo-file-small",
                        "file_unique_id": "photo-small",
                        "height": 64,
                        "width": 64
                    },
                    {
                        "file_id": "photo-file",
                        "file_unique_id": "photo-1",
                        "height": 512,
                        "width": 512
                    }
                ]
            }
        }))
    }

    fn photo_caption_media_group_update(
        caption: &str,
        media_group_id: &str,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        photo_caption_media_group_update_at(
            12348,
            77,
            caption,
            media_group_id,
            "photo-file",
            "photo-1",
            1_710_000_000,
        )
    }

    fn photo_caption_media_group_update_at(
        update_id: i64,
        message_id: i64,
        caption: &str,
        media_group_id: &str,
        file_id: &str,
        file_unique_id: &str,
        date: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "message": {
                "message_id": message_id,
                "date": date,
                "media_group_id": media_group_id,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "caption": caption,
                "photo": [
                    {
                        "file_id": "photo-file-small",
                        "file_unique_id": "photo-small",
                        "height": 64,
                        "width": 64
                    },
                    {
                        "file_id": file_id,
                        "file_unique_id": file_unique_id,
                        "height": 512,
                        "width": 512
                    }
                ]
            }
        }))
    }

    fn photo_media_group_update_at(
        update_id: i64,
        message_id: i64,
        media_group_id: &str,
        file_id: &str,
        file_unique_id: &str,
        date: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "message": {
                "message_id": message_id,
                "date": date,
                "media_group_id": media_group_id,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "photo": [
                    {
                        "file_id": "photo-file-small",
                        "file_unique_id": "photo-small",
                        "height": 64,
                        "width": 64
                    },
                    {
                        "file_id": file_id,
                        "file_unique_id": file_unique_id,
                        "height": 512,
                        "width": 512
                    }
                ]
            }
        }))
    }

    fn reply_photo_text_update(
        text: &str,
        reply_caption: &str,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12347,
            "message": {
                "message_id": 79,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": text,
                "reply_to_message": {
                    "message_id": 70,
                    "date": 1_709_999_900,
                    "chat": {
                        "id": 42,
                        "type": "private",
                        "first_name": "Ada",
                        "username": "ada_l"
                    },
                    "from": {
                        "id": 99,
                        "is_bot": false,
                        "first_name": "Ada",
                        "username": "ada_l"
                    },
                    "caption": reply_caption,
                    "photo": [
                        {
                            "file_id": "reply-photo-file-small",
                            "file_unique_id": "reply-photo-small",
                            "height": 64,
                            "width": 64
                        },
                        {
                            "file_id": "reply-photo-file",
                            "file_unique_id": "reply-photo-1",
                            "height": 512,
                            "width": 512
                        }
                    ]
                }
            }
        }))
    }

    fn reply_audio_text_update(text: &str) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12349,
            "message": {
                "message_id": 80,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": text,
                "reply_to_message": {
                    "message_id": 71,
                    "date": 1_709_999_900,
                    "chat": {
                        "id": 42,
                        "type": "private",
                        "first_name": "Ada",
                        "username": "ada_l"
                    },
                    "from": {
                        "id": 99,
                        "is_bot": false,
                        "first_name": "Ada",
                        "username": "ada_l"
                    },
                    "audio": {
                        "file_id": "audio-file",
                        "file_unique_id": "audio-unique",
                        "duration": 120,
                        "performer": "Artist",
                        "title": "Title",
                        "file_name": "song.mp3",
                        "mime_type": "audio/mpeg"
                    }
                }
            }
        }))
    }

    fn reply_video_text_update(text: &str) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12351,
            "message": {
                "message_id": 81,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": text,
                "reply_to_message": {
                    "message_id": 72,
                    "date": 1_709_999_900,
                    "chat": {
                        "id": 42,
                        "type": "private",
                        "first_name": "Ada",
                        "username": "ada_l"
                    },
                    "from": {
                        "id": 99,
                        "is_bot": false,
                        "first_name": "Ada",
                        "username": "ada_l"
                    },
                    "video": {
                        "file_id": "video-file",
                        "file_unique_id": "video-unique",
                        "width": 640,
                        "height": 480,
                        "duration": 12,
                        "file_name": "clip.mp4"
                    }
                }
            }
        }))
    }

    fn group_message_update(text: &str) -> Result<TelegramUpdate, serde_json::Error> {
        group_message_update_at(text, 1_710_000_000)
    }

    fn group_message_update_at(text: &str, date: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 22345,
            "message": {
                "message_id": 78,
                "date": date,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Group"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": text
            }
        }))
    }

    fn group_bot_message_update(text: &str) -> Result<TelegramUpdate, serde_json::Error> {
        group_bot_message_update_at(text, 1_710_000_000)
    }

    fn group_bot_message_update_at(
        text: &str,
        date: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 22359,
            "message": {
                "message_id": 79,
                "date": date,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Group"
                },
                "from": {
                    "id": 777,
                    "is_bot": true,
                    "first_name": "HelperBot",
                    "username": "helper_bot"
                },
                "text": text
            }
        }))
    }

    fn group_bot_reply_to_plotva_update_at(
        update_id: i64,
        text: &str,
        message_id: i64,
        date: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "message": {
                "message_id": message_id,
                "date": date,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Group"
                },
                "from": {
                    "id": 777,
                    "is_bot": true,
                    "first_name": "HelperBot",
                    "username": "helper_bot"
                },
                "text": text,
                "reply_to_message": {
                    "message_id": 99,
                    "date": date - 1,
                    "chat": {
                        "id": -100,
                        "type": "supergroup",
                        "title": "Group"
                    },
                    "from": {
                        "id": 555,
                        "is_bot": true,
                        "first_name": "Plotva",
                        "username": "plotva_bot"
                    },
                    "text": "old answer"
                }
            }
        }))
    }

    fn group_forum_topic_root_reply_to_plotva_update(
        text: &str,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 22362,
            "message": {
                "message_id": 100,
                "message_thread_id": 99,
                "date": 1_710_000_000,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Forum",
                    "is_forum": true
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": text,
                "reply_to_message": {
                    "message_id": 99,
                    "date": 1_709_999_999,
                    "chat": {
                        "id": -100,
                        "type": "supergroup",
                        "title": "Forum",
                        "is_forum": true
                    },
                    "from": {
                        "id": 555,
                        "is_bot": true,
                        "first_name": "Plotva",
                        "username": "plotva_bot"
                    },
                    "text": "topic starter"
                }
            }
        }))
    }

    fn group_captioned_video_update_at(
        update_id: i64,
        message_id: i64,
        date: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "message": {
                "message_id": message_id,
                "date": date,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Group"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "caption": "смотри что тут",
                "video": {
                    "file_id": "captioned-video-file",
                    "file_unique_id": "captioned-video-1",
                    "duration": 7,
                    "width": 640,
                    "height": 360,
                    "file_name": "clip.mp4"
                }
            }
        }))
    }

    fn group_service_payload_update_at(
        update_id: i64,
        message_id: i64,
        date: i64,
        service: Value,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let mut message = serde_json::Map::from_iter([
            ("message_id".to_owned(), json!(message_id)),
            ("date".to_owned(), json!(date)),
            (
                "chat".to_owned(),
                json!({
                    "id": -100,
                    "type": "supergroup",
                    "title": "Group"
                }),
            ),
            (
                "from".to_owned(),
                json!({
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                }),
            ),
        ]);
        let (key, value) = service
            .as_object()
            .and_then(|object| object.iter().next())
            .map(|(key, value)| (key.to_owned(), value.clone()))
            .expect("service-message fixture");
        message.insert(key, value);
        serde_json::from_value(json!({
            "update_id": update_id,
            "message": Value::Object(message),
        }))
    }

    fn private_captionless_photo_update() -> Result<TelegramUpdate, serde_json::Error> {
        private_captionless_photo_update_at(1_710_000_000)
    }

    fn private_captionless_photo_update_at(date: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 32345,
            "message": {
                "message_id": 79,
                "date": date,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "photo": [
                    {
                        "file_id": "captionless-photo-small",
                        "file_unique_id": "captionless-photo-small",
                        "width": 90,
                        "height": 90
                    },
                    {
                        "file_id": "captionless-photo-file",
                        "file_unique_id": "captionless-photo-1",
                        "width": 1280,
                        "height": 720,
                        "file_size": 1234
                    }
                ]
            }
        }))
    }

    fn private_captionless_voice_update_at(date: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 32355,
            "message": {
                "message_id": 89,
                "date": date,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "voice": {
                    "file_id": "captionless-voice-file",
                    "file_unique_id": "captionless-voice-1",
                    "duration": 7,
                    "file_size": 456
                }
            }
        }))
    }

    fn private_location_update_at(date: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 32356,
            "message": {
                "message_id": 90,
                "date": date,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "location": {
                    "latitude": 52.2297,
                    "longitude": 21.0122
                }
            }
        }))
    }

    fn private_venue_update_at(date: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 32357,
            "message": {
                "message_id": 91,
                "date": date,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "venue": {
                    "location": {
                        "latitude": 52.2297,
                        "longitude": 21.0122
                    },
                    "title": "Cafe",
                    "address": "Main 1"
                }
            }
        }))
    }

    fn private_contact_phone_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 32346,
            "message": {
                "message_id": 80,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "contact": {
                    "phone_number": "+123",
                    "first_name": "",
                    "last_name": ""
                }
            }
        }))
    }

    fn private_media_fallback_update(
        update_id: i64,
        message_id: i64,
        media: Value,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        private_media_payload_update_at(update_id, message_id, 1_710_000_000, media)
    }

    fn private_media_payload_update_at(
        update_id: i64,
        message_id: i64,
        date: i64,
        media: Value,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let mut message = serde_json::Map::from_iter([
            ("message_id".to_owned(), json!(message_id)),
            ("date".to_owned(), json!(date)),
            (
                "chat".to_owned(),
                json!({
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                }),
            ),
            (
                "from".to_owned(),
                json!({
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                }),
            ),
        ]);
        let (key, value) = media
            .as_object()
            .and_then(|object| object.iter().next())
            .map(|(key, value)| (key.to_owned(), value.clone()))
            .expect("media fallback fixture");
        message.insert(key, value);
        serde_json::from_value(json!({
            "update_id": update_id,
            "message": Value::Object(message),
        }))
    }

    fn media_audio_title_json() -> Value {
        json!({
            "audio": {
                "file_id": "audio-file",
                "file_unique_id": "audio-unique",
                "duration": 120,
                "performer": "Artist",
                "title": "Title",
                "file_name": "song.mp3",
                "mime_type": "audio/mpeg"
            }
        })
    }

    fn media_audio_file_json() -> Value {
        json!({
            "audio": {
                "file_id": "audio-file",
                "file_unique_id": "audio-unique",
                "duration": 120,
                "file_name": "song.mp3",
                "mime_type": "audio/mpeg"
            }
        })
    }

    fn media_audio_empty_json() -> Value {
        json!({
            "audio": {
                "file_id": "captionless-audio-file",
                "file_unique_id": "captionless-audio-1",
                "duration": 120,
                "mime_type": "audio/mpeg"
            }
        })
    }

    fn media_animation_json() -> Value {
        json!({
            "animation": {
                "file_id": "captionless-animation-file",
                "file_unique_id": "captionless-animation-1",
                "width": 320,
                "height": 240,
                "duration": 3
            }
        })
    }

    fn media_premium_animation_json() -> Value {
        json!({
            "premium_animation": {
                "file_id": "captionless-premium-animation-file",
                "file_unique_id": "captionless-premium-animation-1",
                "width": 320,
                "height": 240,
                "duration": 3
            }
        })
    }

    fn media_video_json() -> Value {
        json!({
            "video": {
                "file_id": "video-file",
                "file_unique_id": "video-unique",
                "duration": 7,
                "width": 640,
                "height": 360,
                "file_name": "clip.mp4"
            }
        })
    }

    fn media_video_empty_json() -> Value {
        json!({
            "video": {
                "file_id": "captionless-video-file",
                "file_unique_id": "captionless-video-1",
                "duration": 7,
                "width": 640,
                "height": 360
            }
        })
    }

    fn media_video_note_json() -> Value {
        json!({
            "video_note": {
                "file_id": "captionless-video-note-file",
                "file_unique_id": "captionless-video-note-1",
                "length": 240,
                "duration": 3
            }
        })
    }

    fn media_document_json() -> Value {
        json!({
            "document": {
                "file_id": "document-file",
                "file_unique_id": "document-unique",
                "file_name": "doc.pdf"
            }
        })
    }

    fn media_document_without_file_name_json() -> Value {
        json!({
            "document": {
                "file_id": "captionless-document-file",
                "file_unique_id": "captionless-document-1"
            }
        })
    }

    fn media_sticker_json() -> Value {
        json!({
            "sticker": {
                "file_id": "sticker-file",
                "file_unique_id": "sticker-unique",
                "type": "regular",
                "width": 512,
                "height": 512,
                "is_animated": false,
                "is_video": false,
                "emoji": "🙂"
            }
        })
    }

    fn media_sticker_empty_emoji_json() -> Value {
        json!({
            "sticker": {
                "file_id": "captionless-sticker-file",
                "file_unique_id": "captionless-sticker-1",
                "type": "regular",
                "width": 512,
                "height": 512,
                "is_animated": false,
                "is_video": false
            }
        })
    }

    fn media_dice_json() -> Value {
        json!({
            "dice": {
                "emoji": "🎲",
                "value": 4
            }
        })
    }

    fn media_paid_media_json() -> Value {
        json!({
            "paid_media": {
                "star_count": 5,
                "paid_media": [
                    {
                        "type": "preview",
                        "width": 640,
                        "height": 480,
                        "duration": 3
                    }
                ]
            }
        })
    }

    fn media_story_json() -> Value {
        json!({
            "story": {
                "chat": {
                    "id": -100777,
                    "type": "channel",
                    "title": "Story Source"
                },
                "id": 8
            }
        })
    }

    fn media_checklist_json() -> Value {
        json!({
            "checklist": {
                "title": "Groceries",
                "tasks": [
                    {
                        "id": 1,
                        "text": "Milk"
                    }
                ]
            }
        })
    }

    fn media_contact_name_json() -> Value {
        json!({
            "contact": {
                "phone_number": "+123",
                "first_name": "Ada",
                "last_name": "Lovelace"
            }
        })
    }

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
