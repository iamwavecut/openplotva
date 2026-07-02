//! App-level fetcher admin command behavior outside feature-specific modules.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, MessageData as TelegramMessageData,
    Text as TelegramText, TextEntity as TelegramTextEntity, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType,
};
use openplotva_server::{
    RuntimeDispatcherInspector, RuntimeDispatcherStatsData, RuntimeGeminiCachePurgeResultData,
    RuntimeGeminiCachePurger, RuntimeTaskmanInspector, RuntimeTaskmanJobListEntryData,
    RuntimeTaskmanJobsFilter, RuntimeToken, RuntimeUpdatesInspector, RuntimeUpdatesRuntimeData,
};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, OutboundBuildError, ReplyMarkup, ReplyMessageRef, RichMessageRequest,
    TELEGRAM_PARSE_MODE_HTML, TextMessageRequest, build_inline_keyboard_button_url,
    build_inline_keyboard_markup, build_inline_keyboard_row, escape_telegram_html_text,
};
use openplotva_web::settings_selection_base_url;
use thiserror::Error;
use time::{
    Duration as TimeDuration, OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339,
};

use crate::members::ChatCommunicationEffects;
use crate::runtime_api::{IssuedRuntimeToken, RuntimeTokenManager, RuntimeTokenStore};
use crate::settings::{AdminChatSettingsTarget, AdminChatTargetResolver};
use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use crate::virtual_messages::{
    QueueRichRequest, QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory,
    queue_rich_message, queue_text_message_parts,
};

const ADMIN_ENABLE_CHAT_COMMAND: &str = "admin_enable_chat";
const ADMIN_CLEAR_CACHE_COMMAND: &str = "admin_clear_cache";
const ADMIN_CLEAR_GEMINI_CACHE_COMMAND: &str = "admin_clear_gemini_cache";
const ADMIN_HELP_COMMAND: &str = "admin_help";
const ADMIN_QUEUE_STATUS_COMMAND: &str = "admin_queue_status";
const ADMIN_QUEUE_STATS_COMMAND: &str = "admin_queue_stats";
const ADMIN_RUNTIME_TOKEN_COMMAND: &str = "admin_runtime_token";
const ADMIN_RUNTIME_TOKEN_LIST_COMMAND: &str = "admin_runtime_token_list";
const ADMIN_SETTINGS_COMMAND: &str = "admin_settings";
const ADMIN_COMMAND_UNAUTHORIZED_TEXT: &str = "❌ У вас нет прав на выполнение этой команды.";
const ADMIN_SETTINGS_TEXT: &str = "Откройте панель администратора:";
const ADMIN_SETTINGS_BUTTON_TEXT: &str = "Открыть Admin Settings";
const ADMIN_ENABLE_CHAT_USAGE_TEXT: &str = "Usage: /admin_enable_chat [chat_id или @username]";
const ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER: Duration = Duration::from_secs(60);
const ADMIN_PRIVATE_COMMAND_DELETE_AFTER: Duration = Duration::from_secs(5 * 60);
const ADMIN_RUNTIME_TOKEN_DELETE_AFTER: Duration = Duration::from_secs(60 * 60);
const ADMIN_GEMINI_CACHE_UNCONFIGURED_TEXT: &str =
    "❌ Genkit runtime не инициализирован, очистка Gemini cache недоступна.";
const RUNTIME_API_UNAVAILABLE_TEXT: &str = "❌ Runtime API отключен или не инициализирован.";
const QUEUE_STATS_MISSING_METRIC_VALUE: &str = "—";

#[derive(Clone, Debug, PartialEq)]
pub struct AdminTextPlan {
    pub message: TextMessageRequest,
    /// Reply target matching the triggering command message.
    pub reply_to: ReplyMessageRef,
    pub ephemeral_delete_after: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminGeminiCacheCommandOutcome {
    /// Update was not owned by this command and went downstream.
    Delegated,
    Unauthorized,
    UnauthorizedSendError {
        message: String,
    },
    /// No Gemini cache purger/runtime was configured.
    Unconfigured,
    PurgeError {
        message: String,
    },
    Purged(RuntimeGeminiCachePurgeResultData),
    /// The command result could not be sent.
    SendError {
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminHelpCommandOutcome {
    /// Update was not owned by this command and went downstream.
    Delegated,
    Unauthorized,
    UnauthorizedSendError {
        message: String,
    },
    /// Help text was queued, optionally filtered by category.
    Helped {
        category: Option<String>,
    },
    /// Unknown category text was queued.
    UnknownCategory {
        category: String,
    },
    /// The command result could not be sent.
    SendError {
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminQueueCommandOutcome {
    /// Update was not owned by this command and went downstream.
    Delegated,
    Unauthorized,
    UnauthorizedSendError {
        message: String,
    },
    Disabled,
    /// `/admin_queue_status` text was queued.
    Status,
    /// `/admin_queue_stats` text was queued.
    Stats,
    /// The command result could not be sent.
    SendError {
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminQueueWorkerConfig {
    /// Taskman queue name.
    pub queue_name: String,
    /// Configured worker count.
    pub worker_count: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminQueueCommandConfig {
    pub persistent_queue_enabled: bool,
    pub workers: Vec<AdminQueueWorkerConfig>,
    pub default_processing_timeout: Duration,
}

impl AdminQueueCommandConfig {
    #[must_use]
    pub fn queue_names(&self) -> Vec<String> {
        self.workers
            .iter()
            .filter(|worker| worker.worker_count > 0)
            .map(|worker| worker.queue_name.clone())
            .collect()
    }

    fn worker_count(&self, queue_name: &str) -> i32 {
        self.workers
            .iter()
            .find(|worker| worker.queue_name == queue_name)
            .map_or(0, |worker| worker.worker_count)
    }
}

/// Boxed future returned by admin command effects.
pub type AdminCommandEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait AdminCommandEffects {
    /// Concrete effect error.
    type Error: fmt::Display + Send;

    fn send_admin_text<'a>(
        &'a self,
        plan: AdminTextPlan,
    ) -> AdminCommandEffectFuture<'a, Self::Error>;
}

/// Boxed future returned by runtime-token issue calls.
pub type AdminRuntimeTokenIssueFuture<'a> =
    Pin<Box<dyn Future<Output = Result<IssuedRuntimeToken, String>> + Send + 'a>>;

/// Boxed future returned by runtime-token list calls.
pub type AdminRuntimeTokenListFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<RuntimeToken>, String>> + Send + 'a>>;

pub trait AdminRuntimeTokenManager {
    /// Issue and persist a new runtime API token.
    fn issue_runtime_token<'a>(&'a self) -> AdminRuntimeTokenIssueFuture<'a>;

    /// List active runtime API token metadata.
    fn list_runtime_tokens<'a>(&'a self) -> AdminRuntimeTokenListFuture<'a>;

    fn ttl(&self) -> TimeDuration;
}

impl<Store> AdminRuntimeTokenManager for RuntimeTokenManager<Store>
where
    Store: RuntimeTokenStore + Send + Sync,
{
    fn issue_runtime_token<'a>(&'a self) -> AdminRuntimeTokenIssueFuture<'a> {
        Box::pin(async move { self.issue().await.map_err(|error| error.to_string()) })
    }

    fn list_runtime_tokens<'a>(&'a self) -> AdminRuntimeTokenListFuture<'a> {
        Box::pin(async move { self.list().await.map_err(|error| error.to_string()) })
    }

    fn ttl(&self) -> TimeDuration {
        RuntimeTokenManager::ttl(self)
    }
}

/// Boxed future returned by Redis cache flushers.
pub type AdminRedisFlushFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

pub trait AdminRedisCacheFlusher {
    /// Flush the current Redis logical database.
    fn flush_db<'a>(&'a self) -> AdminRedisFlushFuture<'a>;
}

impl AdminRedisCacheFlusher for redis::Client {
    fn flush_db<'a>(&'a self) -> AdminRedisFlushFuture<'a> {
        Box::pin(async move {
            let mut connection = self
                .get_multiplexed_async_connection()
                .await
                .map_err(|error| error.to_string())?;
            let _: String = redis::cmd("FLUSHDB")
                .query_async(&mut connection)
                .await
                .map_err(|error| error.to_string())?;
            Ok(())
        })
    }
}

#[derive(Clone)]
pub struct AdminDispatcherEffects {
    queue: Arc<DispatcherQueue>,
    next_virtual_id: VirtualIdFactory,
}

impl AdminDispatcherEffects {
    /// Build admin effects backed by the normal dispatcher.
    #[must_use]
    pub fn new(queue: Arc<DispatcherQueue>) -> Self {
        Self {
            queue,
            next_virtual_id: monotonic_virtual_id_factory("admin-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl AdminCommandEffects for AdminDispatcherEffects {
    type Error = AdminDispatchEffectError;

    fn send_admin_text<'a>(
        &'a self,
        plan: AdminTextPlan,
    ) -> AdminCommandEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            if admin_text_should_use_rich(&plan) {
                let rich = RichMessageRequest {
                    chat: plan.message.chat,
                    message_thread_id: plan.message.message_thread_id,
                    disable_notification: plan.message.disable_notification,
                    allow_sending_without_reply: plan.message.allow_sending_without_reply,
                    html: admin_text_rich_html(&plan.message),
                    reply_markup: None,
                };
                let queued = queue_rich_message(
                    &self.queue,
                    QueueRichRequest {
                        message: &rich,
                        reply_to: Some(&plan.reply_to),
                        immediate: plan.ephemeral_delete_after.is_some(),
                        bypass_chat_restrictions: false,
                        ephemeral_delete_after: plan.ephemeral_delete_after,
                        protected: false,
                        debounce_key: None,
                    },
                    || (self.next_virtual_id)(),
                )
                .await;
                if !matches!(queued, Err(OutboundBuildError::RichMessageTooLong(_, _))) {
                    queued?;
                    return Ok(());
                }
            }
            queue_text_message_parts(
                &self.queue,
                QueueTextRequest {
                    protected: false,
                    debounce_key: None,
                    message: &plan.message,
                    reply_to: Some(&plan.reply_to),
                    immediate_first: plan.ephemeral_delete_after.is_some(),
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: plan.ephemeral_delete_after,
                },
                || (self.next_virtual_id)(),
            )
            .await?;
            Ok(())
        })
    }
}

fn admin_text_should_use_rich(plan: &AdminTextPlan) -> bool {
    plan.message.reply_markup.is_none()
        && (plan.message.render_as == TELEGRAM_PARSE_MODE_HTML || plan.message.text.contains('\n'))
}

fn admin_text_rich_html(message: &TextMessageRequest) -> String {
    if message.render_as == TELEGRAM_PARSE_MODE_HTML {
        message.text.clone()
    } else {
        format!(
            "<p>{}</p>",
            escape_telegram_html_text(&message.text).replace('\n', "<br>")
        )
    }
}

/// Recoverable errors from concrete admin effects.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum AdminDispatchEffectError {
    #[error("failed to queue admin text: {0}")]
    Queue(#[from] OutboundBuildError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminRuntimeTokenCommandOutcome {
    /// Update was not owned by this command and went downstream.
    Delegated,
    Unauthorized,
    UnauthorizedSendError {
        message: String,
    },
    /// Authorized admin used a runtime-token command outside a private chat.
    PrivateChatRequired,
    /// Runtime API or token manager was not configured.
    RuntimeApiUnavailable,
    /// `/admin_runtime_token` issued a new token.
    Issued {
        id: String,
    },
    IssueError {
        message: String,
    },
    /// `/admin_runtime_token_list` found no active tokens.
    EmptyList,
    /// `/admin_runtime_token_list` returned active tokens.
    Listed {
        count: usize,
    },
    ListError {
        message: String,
    },
    /// The command result could not be sent.
    SendError {
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminSettingsCommandOutcome {
    /// Update was not owned by this command and went downstream.
    Delegated,
    Unauthorized,
    MissingWebAppUrl,
    /// The admin settings button was queued.
    Sent,
    /// The command result could not be sent.
    SendError {
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminEnableChatCommandOutcome {
    /// Update was not owned by this command and went downstream.
    Delegated,
    Unauthorized,
    UnauthorizedSendError {
        message: String,
    },
    Usage,
    ResolveError {
        message: String,
    },
    Enabled {
        chat_id: i64,
    },
    /// The command result could not be sent.
    SendError {
        message: String,
    },
}

#[derive(Clone)]
pub struct AdminEnableChatCommandUpdateHandler<Resolver, Communication, Effects, Next> {
    resolver: Arc<Resolver>,
    communication: Arc<Communication>,
    effects: Arc<Effects>,
    admin_ids: Arc<[i64]>,
    bot_username: String,
    next: Arc<Next>,
}

impl<Resolver, Communication, Effects, Next>
    AdminEnableChatCommandUpdateHandler<Resolver, Communication, Effects, Next>
{
    /// Build a handler around the real downstream update handler.
    #[must_use]
    pub fn new(
        resolver: Arc<Resolver>,
        communication: Arc<Communication>,
        effects: Arc<Effects>,
        admin_ids: Vec<i64>,
        bot_username: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            resolver,
            communication,
            effects,
            admin_ids: Arc::from(admin_ids.into_boxed_slice()),
            bot_username: bot_username.into(),
            next,
        }
    }
}

impl<Resolver, Communication, Effects, Next> UpdateHandler
    for AdminEnableChatCommandUpdateHandler<Resolver, Communication, Effects, Next>
where
    Resolver: AdminChatTargetResolver + Send + Sync,
    Communication: ChatCommunicationEffects + Send + Sync,
    Effects: AdminCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_admin_enable_chat_command_update_or_else_at(
                AdminEnableChatRuntime {
                    resolver: self.resolver.as_ref(),
                    communication: self.communication.as_ref(),
                    effects: self.effects.as_ref(),
                    admin_ids: &self.admin_ids,
                    bot_username: &self.bot_username,
                },
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Copy)]
pub struct AdminEnableChatRuntime<'a, Resolver, Communication, Effects> {
    pub resolver: &'a Resolver,
    /// Chat communication toggle boundary.
    pub communication: &'a Communication,
    /// Admin text side effects.
    pub effects: &'a Effects,
    /// Authorized Telegram admin user IDs.
    pub admin_ids: &'a [i64],
    /// Current bot username from `getMe`.
    pub bot_username: &'a str,
}

pub async fn handle_admin_enable_chat_command_update_or_else_at<
    Resolver,
    Communication,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    runtime: AdminEnableChatRuntime<'_, Resolver, Communication, Effects>,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<AdminEnableChatCommandOutcome, HandleError>
where
    Resolver: AdminChatTargetResolver + Sync,
    Communication: ChatCommunicationEffects + Sync,
    Effects: AdminCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update).await?;
        return Ok(AdminEnableChatCommandOutcome::Delegated);
    };
    let Some(command) = admin_command_for_message(message, runtime.bot_username) else {
        handle_other(update).await?;
        return Ok(AdminEnableChatCommandOutcome::Delegated);
    };
    if command.name != ADMIN_ENABLE_CHAT_COMMAND {
        handle_other(update).await?;
        return Ok(AdminEnableChatCommandOutcome::Delegated);
    }

    let actor_user_id = message
        .sender
        .get_user()
        .map(|user| i64::from(user.id))
        .unwrap_or_default();
    if !runtime.admin_ids.contains(&actor_user_id) {
        return match runtime
            .effects
            .send_admin_text(admin_text_plan(
                message,
                ADMIN_COMMAND_UNAUTHORIZED_TEXT,
                Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER),
            ))
            .await
        {
            Ok(()) => Ok(AdminEnableChatCommandOutcome::Unauthorized),
            Err(error) => {
                tracing::warn!(%error, actor_user_id, "failed to queue admin command denial");
                Ok(AdminEnableChatCommandOutcome::UnauthorizedSendError {
                    message: error.to_string(),
                })
            }
        };
    }

    let target_identifier = command.arguments.trim();
    if target_identifier.is_empty() {
        return send_admin_enable_chat_result(
            runtime.effects,
            message,
            ADMIN_ENABLE_CHAT_USAGE_TEXT.to_owned(),
            AdminEnableChatCommandOutcome::Usage,
        )
        .await;
    }

    let target = match runtime
        .resolver
        .resolve_admin_chat_target(target_identifier)
        .await
    {
        Ok(target) => target,
        Err(error) => {
            let text =
                format!("Could not find or access chat: {target_identifier}. Error: {error}");
            return send_admin_enable_chat_result(
                runtime.effects,
                message,
                text.clone(),
                AdminEnableChatCommandOutcome::ResolveError { message: text },
            )
            .await;
        }
    };

    if let Err(error) = runtime
        .communication
        .enable_chat_communication(target.id)
        .await
    {
        tracing::warn!(
            %error,
            chat_id = target.id,
            "failed to enable chat communication for admin command"
        );
    }

    let text = format!(
        "Global text and draw replies have been ENABLED for chat '{}' (ID: {}).",
        admin_enable_chat_display_title(&target),
        target.id
    );
    send_admin_enable_chat_result(
        runtime.effects,
        message,
        text,
        AdminEnableChatCommandOutcome::Enabled { chat_id: target.id },
    )
    .await
}

async fn send_admin_enable_chat_result<Effects, HandleError>(
    effects: &Effects,
    message: &TelegramMessage,
    text: String,
    success: AdminEnableChatCommandOutcome,
) -> Result<AdminEnableChatCommandOutcome, HandleError>
where
    Effects: AdminCommandEffects + Sync,
{
    match effects
        .send_admin_text(admin_text_plan(message, &text, None))
        .await
    {
        Ok(()) => Ok(success),
        Err(error) => {
            tracing::warn!(%error, "failed to queue admin enable-chat reply");
            Ok(AdminEnableChatCommandOutcome::SendError {
                message: error.to_string(),
            })
        }
    }
}

#[derive(Clone)]
pub struct AdminSettingsCommandUpdateHandler<Effects, Next> {
    effects: Arc<Effects>,
    admin_ids: Arc<[i64]>,
    bot_username: String,
    web_app_url: String,
    next: Arc<Next>,
}

impl<Effects, Next> AdminSettingsCommandUpdateHandler<Effects, Next> {
    /// Build a handler around the real downstream update handler.
    #[must_use]
    pub fn new(
        effects: Arc<Effects>,
        admin_ids: Vec<i64>,
        bot_username: impl Into<String>,
        web_app_url: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            effects,
            admin_ids: Arc::from(admin_ids.into_boxed_slice()),
            bot_username: bot_username.into(),
            web_app_url: web_app_url.into(),
            next,
        }
    }
}

impl<Effects, Next> UpdateHandler for AdminSettingsCommandUpdateHandler<Effects, Next>
where
    Effects: AdminCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_admin_settings_command_update_or_else_at(
                AdminSettingsRuntime {
                    effects: self.effects.as_ref(),
                    admin_ids: &self.admin_ids,
                    bot_username: &self.bot_username,
                    web_app_url: &self.web_app_url,
                },
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Copy)]
pub struct AdminSettingsRuntime<'a, Effects> {
    /// Admin text side effects.
    pub effects: &'a Effects,
    /// Authorized Telegram admin user IDs.
    pub admin_ids: &'a [i64],
    /// Current bot username from `getMe`.
    pub bot_username: &'a str,
    /// Configured `WEBAPP_URL`.
    pub web_app_url: &'a str,
}

pub async fn handle_admin_settings_command_update_or_else_at<
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    runtime: AdminSettingsRuntime<'_, Effects>,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<AdminSettingsCommandOutcome, HandleError>
where
    Effects: AdminCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update).await?;
        return Ok(AdminSettingsCommandOutcome::Delegated);
    };
    let Some(command) = admin_command_for_message(message, runtime.bot_username) else {
        handle_other(update).await?;
        return Ok(AdminSettingsCommandOutcome::Delegated);
    };
    if command.name != ADMIN_SETTINGS_COMMAND || !matches!(message.chat, TelegramChat::Private(_)) {
        handle_other(update).await?;
        return Ok(AdminSettingsCommandOutcome::Delegated);
    }

    let actor_user_id = message
        .sender
        .get_user()
        .map(|user| i64::from(user.id))
        .unwrap_or_default();
    if !runtime.admin_ids.contains(&actor_user_id) {
        return Ok(AdminSettingsCommandOutcome::Unauthorized);
    }

    let Some(base_url) = settings_selection_base_url(runtime.web_app_url) else {
        return Ok(AdminSettingsCommandOutcome::MissingWebAppUrl);
    };

    let mut plan = admin_text_plan(message, ADMIN_SETTINGS_TEXT, None);
    plan.message.reply_markup = Some(ReplyMarkup::InlineKeyboardMarkup(
        build_inline_keyboard_markup([build_inline_keyboard_row([
            build_inline_keyboard_button_url(
                ADMIN_SETTINGS_BUTTON_TEXT,
                format!("{base_url}admin/"),
            ),
        ])]),
    ));

    match runtime.effects.send_admin_text(plan).await {
        Ok(()) => Ok(AdminSettingsCommandOutcome::Sent),
        Err(error) => {
            tracing::warn!(%error, "failed to queue admin settings reply");
            Ok(AdminSettingsCommandOutcome::SendError {
                message: error.to_string(),
            })
        }
    }
}

#[derive(Clone)]
pub struct AdminQueueCommandUpdateHandler<Effects, Next> {
    runtime: AdminQueueRuntimeConfig,
    effects: Arc<Effects>,
    admin_ids: Arc<[i64]>,
    bot_username: String,
    next: Arc<Next>,
}

#[derive(Clone)]
pub struct AdminQueueRuntimeConfig {
    /// Persistent queue command config.
    pub config: AdminQueueCommandConfig,
    /// Taskman inspector used for queue jobs and diagnostics.
    pub taskman: Option<Arc<dyn RuntimeTaskmanInspector>>,
    /// Outbound dispatcher inspector used by `/admin_queue_stats`.
    pub dispatcher: Option<Arc<dyn RuntimeDispatcherInspector>>,
    /// Update queue inspector used by `/admin_queue_stats`.
    pub updates: Option<Arc<dyn RuntimeUpdatesInspector>>,
    /// Live dialog debounce length provider.
    pub dialog_debounce_len: Option<Arc<dyn Fn() -> usize + Send + Sync>>,
    /// Live media gate count. Zero until the Rust media gate is introduced.
    pub media_gate_in_use: Arc<dyn Fn() -> usize + Send + Sync>,
}

impl AdminQueueRuntimeConfig {
    /// Build runtime providers for queue commands.
    #[must_use]
    pub fn new(config: AdminQueueCommandConfig) -> Self {
        Self {
            config,
            taskman: None,
            dispatcher: None,
            updates: None,
            dialog_debounce_len: None,
            media_gate_in_use: Arc::new(|| 0),
        }
    }

    /// Attach taskman diagnostics.
    #[must_use]
    pub fn with_taskman(mut self, taskman: Arc<dyn RuntimeTaskmanInspector>) -> Self {
        self.taskman = Some(taskman);
        self
    }

    /// Attach dispatcher diagnostics.
    #[must_use]
    pub fn with_dispatcher(mut self, dispatcher: Arc<dyn RuntimeDispatcherInspector>) -> Self {
        self.dispatcher = Some(dispatcher);
        self
    }

    /// Attach update queue diagnostics.
    #[must_use]
    pub fn with_updates(mut self, updates: Arc<dyn RuntimeUpdatesInspector>) -> Self {
        self.updates = Some(updates);
        self
    }

    /// Attach dialog debounce length diagnostics.
    #[must_use]
    pub fn with_dialog_debounce_len(
        mut self,
        dialog_debounce_len: Arc<dyn Fn() -> usize + Send + Sync>,
    ) -> Self {
        self.dialog_debounce_len = Some(dialog_debounce_len);
        self
    }
}

impl<Effects, Next> AdminQueueCommandUpdateHandler<Effects, Next> {
    /// Build a handler around the real downstream update handler.
    #[must_use]
    pub fn new(
        runtime: AdminQueueRuntimeConfig,
        effects: Arc<Effects>,
        admin_ids: Vec<i64>,
        bot_username: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            runtime,
            effects,
            admin_ids: Arc::from(admin_ids.into_boxed_slice()),
            bot_username: bot_username.into(),
            next,
        }
    }
}

impl<Effects, Next> UpdateHandler for AdminQueueCommandUpdateHandler<Effects, Next>
where
    Effects: AdminCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_admin_queue_command_update_or_else_at(
                AdminQueueRuntime {
                    config: &self.runtime.config,
                    taskman: self.runtime.taskman.as_deref(),
                    dispatcher: self.runtime.dispatcher.as_deref(),
                    updates: self.runtime.updates.as_deref(),
                    dialog_debounce_len: self.runtime.dialog_debounce_len.as_deref(),
                    media_gate_in_use: self.runtime.media_gate_in_use.as_ref(),
                    effects: self.effects.as_ref(),
                    admin_ids: &self.admin_ids,
                    bot_username: &self.bot_username,
                },
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Copy)]
pub struct AdminQueueRuntime<'a, Effects> {
    /// Persistent queue command config.
    pub config: &'a AdminQueueCommandConfig,
    /// Taskman inspector boundary.
    pub taskman: Option<&'a dyn RuntimeTaskmanInspector>,
    /// Dispatcher inspector boundary.
    pub dispatcher: Option<&'a dyn RuntimeDispatcherInspector>,
    /// Update queue inspector boundary.
    pub updates: Option<&'a dyn RuntimeUpdatesInspector>,
    /// Dialog debounce len boundary.
    pub dialog_debounce_len: Option<&'a (dyn Fn() -> usize + Send + Sync)>,
    /// Media gate len boundary.
    pub media_gate_in_use: &'a (dyn Fn() -> usize + Send + Sync),
    /// Admin text side effects.
    pub effects: &'a Effects,
    /// Authorized Telegram admin user IDs.
    pub admin_ids: &'a [i64],
    /// Current bot username from `getMe`.
    pub bot_username: &'a str,
}

pub async fn handle_admin_queue_command_update_or_else_at<
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    runtime: AdminQueueRuntime<'_, Effects>,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<AdminQueueCommandOutcome, HandleError>
where
    Effects: AdminCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update).await?;
        return Ok(AdminQueueCommandOutcome::Delegated);
    };
    let Some(command) = admin_command_for_message(message, runtime.bot_username) else {
        handle_other(update).await?;
        return Ok(AdminQueueCommandOutcome::Delegated);
    };
    if !matches!(
        command.name,
        ADMIN_QUEUE_STATUS_COMMAND | ADMIN_QUEUE_STATS_COMMAND
    ) {
        handle_other(update).await?;
        return Ok(AdminQueueCommandOutcome::Delegated);
    }

    let actor_user_id = message
        .sender
        .get_user()
        .map(|user| i64::from(user.id))
        .unwrap_or_default();
    if !runtime.admin_ids.contains(&actor_user_id) {
        return match runtime
            .effects
            .send_admin_text(admin_text_plan(
                message,
                ADMIN_COMMAND_UNAUTHORIZED_TEXT,
                Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER),
            ))
            .await
        {
            Ok(()) => Ok(AdminQueueCommandOutcome::Unauthorized),
            Err(error) => {
                tracing::warn!(%error, actor_user_id, "failed to queue admin command denial");
                Ok(AdminQueueCommandOutcome::UnauthorizedSendError {
                    message: error.to_string(),
                })
            }
        };
    }

    if !runtime.config.persistent_queue_enabled {
        return send_admin_queue_result(
            runtime.effects,
            message,
            admin_queue_disabled_text().to_owned(),
            false,
            AdminQueueCommandOutcome::Disabled,
        )
        .await;
    }

    match command.name {
        ADMIN_QUEUE_STATUS_COMMAND => {
            let text = admin_queue_status_text(&runtime);
            send_admin_queue_result(
                runtime.effects,
                message,
                text,
                true,
                AdminQueueCommandOutcome::Status,
            )
            .await
        }
        ADMIN_QUEUE_STATS_COMMAND => {
            let text = admin_queue_stats_text(&runtime).await;
            send_admin_queue_result(
                runtime.effects,
                message,
                text,
                true,
                AdminQueueCommandOutcome::Stats,
            )
            .await
        }
        _ => {
            handle_other(update).await?;
            Ok(AdminQueueCommandOutcome::Delegated)
        }
    }
}

async fn send_admin_queue_result<Effects, HandleError>(
    effects: &Effects,
    message: &TelegramMessage,
    text: String,
    html: bool,
    success: AdminQueueCommandOutcome,
) -> Result<AdminQueueCommandOutcome, HandleError>
where
    Effects: AdminCommandEffects + Sync,
{
    let plan = if html {
        admin_html_text_plan(message, &text, None)
    } else {
        admin_text_plan(message, &text, None)
    };
    match effects.send_admin_text(plan).await {
        Ok(()) => Ok(success),
        Err(error) => {
            tracing::warn!(%error, "failed to queue admin queue reply");
            Ok(AdminQueueCommandOutcome::SendError {
                message: error.to_string(),
            })
        }
    }
}

#[derive(Clone)]
pub struct AdminHelpCommandUpdateHandler<Effects, Next> {
    effects: Arc<Effects>,
    admin_ids: Arc<[i64]>,
    bot_username: String,
    next: Arc<Next>,
}

impl<Effects, Next> AdminHelpCommandUpdateHandler<Effects, Next> {
    /// Build a handler around the real downstream update handler.
    #[must_use]
    pub fn new(
        effects: Arc<Effects>,
        admin_ids: Vec<i64>,
        bot_username: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            effects,
            admin_ids: Arc::from(admin_ids.into_boxed_slice()),
            bot_username: bot_username.into(),
            next,
        }
    }
}

impl<Effects, Next> UpdateHandler for AdminHelpCommandUpdateHandler<Effects, Next>
where
    Effects: AdminCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_admin_help_command_update_or_else_at(
                AdminHelpRuntime {
                    effects: self.effects.as_ref(),
                    admin_ids: &self.admin_ids,
                    bot_username: &self.bot_username,
                },
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Copy)]
pub struct AdminHelpRuntime<'a, Effects> {
    /// Admin text side effects.
    pub effects: &'a Effects,
    /// Authorized Telegram admin user IDs.
    pub admin_ids: &'a [i64],
    /// Current bot username from `getMe`.
    pub bot_username: &'a str,
}

pub async fn handle_admin_help_command_update_or_else_at<
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    runtime: AdminHelpRuntime<'_, Effects>,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<AdminHelpCommandOutcome, HandleError>
where
    Effects: AdminCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update).await?;
        return Ok(AdminHelpCommandOutcome::Delegated);
    };
    let Some(command) = admin_command_for_message(message, runtime.bot_username) else {
        handle_other(update).await?;
        return Ok(AdminHelpCommandOutcome::Delegated);
    };
    if command.name != ADMIN_HELP_COMMAND {
        handle_other(update).await?;
        return Ok(AdminHelpCommandOutcome::Delegated);
    }

    let actor_user_id = message
        .sender
        .get_user()
        .map(|user| i64::from(user.id))
        .unwrap_or_default();
    if !runtime.admin_ids.contains(&actor_user_id) {
        return match runtime
            .effects
            .send_admin_text(admin_text_plan(
                message,
                ADMIN_COMMAND_UNAUTHORIZED_TEXT,
                Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER),
            ))
            .await
        {
            Ok(()) => Ok(AdminHelpCommandOutcome::Unauthorized),
            Err(error) => {
                tracing::warn!(%error, actor_user_id, "failed to queue admin command denial");
                Ok(AdminHelpCommandOutcome::UnauthorizedSendError {
                    message: error.to_string(),
                })
            }
        };
    }

    let filter_category = admin_help_filter_category(command.arguments);
    let categories = admin_command_categories(filter_category);
    if categories.is_empty() && !filter_category.is_empty() {
        let text = admin_category_not_found_text(filter_category, &admin_available_categories());
        return send_admin_help_result(
            runtime.effects,
            message,
            text,
            AdminHelpCommandOutcome::UnknownCategory {
                category: filter_category.to_owned(),
            },
        )
        .await;
    }

    let text = admin_help_text(&categories, filter_category);
    send_admin_help_result(
        runtime.effects,
        message,
        text,
        AdminHelpCommandOutcome::Helped {
            category: (!filter_category.is_empty()).then(|| filter_category.to_owned()),
        },
    )
    .await
}

async fn send_admin_help_result<Effects, HandleError>(
    effects: &Effects,
    message: &TelegramMessage,
    text: String,
    success: AdminHelpCommandOutcome,
) -> Result<AdminHelpCommandOutcome, HandleError>
where
    Effects: AdminCommandEffects + Sync,
{
    match effects
        .send_admin_text(admin_html_text_plan(message, &text, None))
        .await
    {
        Ok(()) => Ok(success),
        Err(error) => {
            tracing::warn!(%error, "failed to queue admin help reply");
            Ok(AdminHelpCommandOutcome::SendError {
                message: error.to_string(),
            })
        }
    }
}

#[derive(Clone)]
pub struct AdminRuntimeTokenCommandUpdateHandler<Manager, Effects, Next> {
    manager: Option<Arc<Manager>>,
    effects: Arc<Effects>,
    admin_ids: Arc<[i64]>,
    bot_username: String,
    tls_public_key_pin: String,
    next: Arc<Next>,
}

impl<Manager, Effects, Next> AdminRuntimeTokenCommandUpdateHandler<Manager, Effects, Next> {
    /// Build a handler around the real downstream update handler.
    #[must_use]
    pub fn new(
        manager: Option<Arc<Manager>>,
        effects: Arc<Effects>,
        admin_ids: Vec<i64>,
        bot_username: impl Into<String>,
        tls_public_key_pin: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            manager,
            effects,
            admin_ids: Arc::from(admin_ids.into_boxed_slice()),
            bot_username: bot_username.into(),
            tls_public_key_pin: tls_public_key_pin.into(),
            next,
        }
    }
}

impl<Manager, Effects, Next> UpdateHandler
    for AdminRuntimeTokenCommandUpdateHandler<Manager, Effects, Next>
where
    Manager: AdminRuntimeTokenManager + Send + Sync,
    Effects: AdminCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_admin_runtime_token_command_update_or_else_at(
                AdminRuntimeTokenRuntime {
                    manager: self.manager.as_deref(),
                    effects: self.effects.as_ref(),
                    admin_ids: &self.admin_ids,
                    bot_username: &self.bot_username,
                    tls_public_key_pin: &self.tls_public_key_pin,
                },
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Copy)]
pub struct AdminRuntimeTokenRuntime<'a, Manager, Effects> {
    /// Runtime API token manager boundary.
    pub manager: Option<&'a Manager>,
    /// Admin text side effects.
    pub effects: &'a Effects,
    /// Authorized Telegram admin user IDs.
    pub admin_ids: &'a [i64],
    /// Current bot username from `getMe`.
    pub bot_username: &'a str,
    /// Runtime API TLS public-key pin shown with issued tokens.
    pub tls_public_key_pin: &'a str,
}

pub async fn handle_admin_runtime_token_command_update_or_else_at<
    Manager,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    runtime: AdminRuntimeTokenRuntime<'_, Manager, Effects>,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<AdminRuntimeTokenCommandOutcome, HandleError>
where
    Manager: AdminRuntimeTokenManager + Sync,
    Effects: AdminCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update).await?;
        return Ok(AdminRuntimeTokenCommandOutcome::Delegated);
    };
    let Some(command) = admin_command_for_message(message, runtime.bot_username) else {
        handle_other(update).await?;
        return Ok(AdminRuntimeTokenCommandOutcome::Delegated);
    };
    if !matches!(
        command.name,
        ADMIN_RUNTIME_TOKEN_COMMAND | ADMIN_RUNTIME_TOKEN_LIST_COMMAND
    ) {
        handle_other(update).await?;
        return Ok(AdminRuntimeTokenCommandOutcome::Delegated);
    }

    let actor_user_id = message
        .sender
        .get_user()
        .map(|user| i64::from(user.id))
        .unwrap_or_default();
    if !runtime.admin_ids.contains(&actor_user_id) {
        return match runtime
            .effects
            .send_admin_text(admin_text_plan(
                message,
                ADMIN_COMMAND_UNAUTHORIZED_TEXT,
                Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER),
            ))
            .await
        {
            Ok(()) => Ok(AdminRuntimeTokenCommandOutcome::Unauthorized),
            Err(error) => {
                tracing::warn!(%error, actor_user_id, "failed to queue admin command denial");
                Ok(AdminRuntimeTokenCommandOutcome::UnauthorizedSendError {
                    message: error.to_string(),
                })
            }
        };
    }

    if !matches!(message.chat, TelegramChat::Private(_)) {
        let text = admin_private_chat_required_text(runtime_token_usage(command.name));
        return send_admin_runtime_token_result(
            runtime.effects,
            message,
            text,
            Some(ADMIN_PRIVATE_COMMAND_DELETE_AFTER),
            AdminRuntimeTokenCommandOutcome::PrivateChatRequired,
        )
        .await;
    }

    let Some(manager) = runtime.manager else {
        return send_admin_runtime_token_result(
            runtime.effects,
            message,
            RUNTIME_API_UNAVAILABLE_TEXT.to_owned(),
            None,
            AdminRuntimeTokenCommandOutcome::RuntimeApiUnavailable,
        )
        .await;
    };

    match command.name {
        ADMIN_RUNTIME_TOKEN_COMMAND => match manager.issue_runtime_token().await {
            Ok(issued) => {
                let token_id = issued.meta.id.clone();
                let text = admin_runtime_token_issued_text(
                    &issued,
                    manager.ttl(),
                    runtime.tls_public_key_pin,
                );
                send_admin_runtime_token_result(
                    runtime.effects,
                    message,
                    text,
                    Some(ADMIN_RUNTIME_TOKEN_DELETE_AFTER),
                    AdminRuntimeTokenCommandOutcome::Issued { id: token_id },
                )
                .await
            }
            Err(error) => {
                let text = format!("❌ Не удалось выпустить runtime API token:\n{error}");
                send_admin_runtime_token_result(
                    runtime.effects,
                    message,
                    text,
                    None,
                    AdminRuntimeTokenCommandOutcome::IssueError { message: error },
                )
                .await
            }
        },
        ADMIN_RUNTIME_TOKEN_LIST_COMMAND => match manager.list_runtime_tokens().await {
            Ok(tokens) if tokens.is_empty() => {
                send_admin_runtime_token_result(
                    runtime.effects,
                    message,
                    "ℹ️ Активных runtime API токенов нет.".to_owned(),
                    None,
                    AdminRuntimeTokenCommandOutcome::EmptyList,
                )
                .await
            }
            Ok(tokens) => {
                let count = tokens.len();
                let text = admin_runtime_token_list_text(&tokens, manager.ttl());
                send_admin_runtime_token_result(
                    runtime.effects,
                    message,
                    text,
                    None,
                    AdminRuntimeTokenCommandOutcome::Listed { count },
                )
                .await
            }
            Err(error) => {
                let text = format!("❌ Не удалось получить список runtime API tokens:\n{error}");
                send_admin_runtime_token_result(
                    runtime.effects,
                    message,
                    text,
                    None,
                    AdminRuntimeTokenCommandOutcome::ListError { message: error },
                )
                .await
            }
        },
        _ => {
            handle_other(update).await?;
            Ok(AdminRuntimeTokenCommandOutcome::Delegated)
        }
    }
}

async fn send_admin_runtime_token_result<Effects, HandleError>(
    effects: &Effects,
    message: &TelegramMessage,
    text: String,
    ephemeral_delete_after: Option<Duration>,
    success: AdminRuntimeTokenCommandOutcome,
) -> Result<AdminRuntimeTokenCommandOutcome, HandleError>
where
    Effects: AdminCommandEffects + Sync,
{
    match effects
        .send_admin_text(admin_html_text_plan(message, &text, ephemeral_delete_after))
        .await
    {
        Ok(()) => Ok(success),
        Err(error) => {
            tracing::warn!(%error, "failed to queue admin runtime-token reply");
            Ok(AdminRuntimeTokenCommandOutcome::SendError {
                message: error.to_string(),
            })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminRedisCacheCommandOutcome {
    /// Update was not owned by this command and went downstream.
    Delegated,
    Unauthorized,
    UnauthorizedSendError {
        message: String,
    },
    Cleared,
    FlushError {
        message: String,
    },
    /// The command result could not be sent.
    SendError {
        message: String,
    },
}

#[derive(Clone)]
pub struct AdminRedisCacheCommandUpdateHandler<Flusher, Effects, Next> {
    flusher: Arc<Flusher>,
    effects: Arc<Effects>,
    admin_ids: Arc<[i64]>,
    bot_username: String,
    next: Arc<Next>,
}

impl<Flusher, Effects, Next> AdminRedisCacheCommandUpdateHandler<Flusher, Effects, Next> {
    /// Build a handler around the real downstream update handler.
    #[must_use]
    pub fn new(
        flusher: Arc<Flusher>,
        effects: Arc<Effects>,
        admin_ids: Vec<i64>,
        bot_username: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            flusher,
            effects,
            admin_ids: Arc::from(admin_ids.into_boxed_slice()),
            bot_username: bot_username.into(),
            next,
        }
    }
}

impl<Flusher, Effects, Next> UpdateHandler
    for AdminRedisCacheCommandUpdateHandler<Flusher, Effects, Next>
where
    Flusher: AdminRedisCacheFlusher + Send + Sync,
    Effects: AdminCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_admin_redis_cache_command_update_or_else_at(
                AdminRedisCacheRuntime {
                    flusher: self.flusher.as_ref(),
                    effects: self.effects.as_ref(),
                    admin_ids: &self.admin_ids,
                    bot_username: &self.bot_username,
                },
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Copy)]
pub struct AdminRedisCacheRuntime<'a, Flusher, Effects> {
    /// Redis `FLUSHDB` boundary.
    pub flusher: &'a Flusher,
    /// Admin text side effects.
    pub effects: &'a Effects,
    /// Authorized Telegram admin user IDs.
    pub admin_ids: &'a [i64],
    /// Current bot username from `getMe`.
    pub bot_username: &'a str,
}

pub async fn handle_admin_redis_cache_command_update_or_else_at<
    Flusher,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    runtime: AdminRedisCacheRuntime<'_, Flusher, Effects>,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<AdminRedisCacheCommandOutcome, HandleError>
where
    Flusher: AdminRedisCacheFlusher + Sync,
    Effects: AdminCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update).await?;
        return Ok(AdminRedisCacheCommandOutcome::Delegated);
    };
    let Some(command) = admin_command_for_message(message, runtime.bot_username) else {
        handle_other(update).await?;
        return Ok(AdminRedisCacheCommandOutcome::Delegated);
    };
    if command.name != ADMIN_CLEAR_CACHE_COMMAND {
        handle_other(update).await?;
        return Ok(AdminRedisCacheCommandOutcome::Delegated);
    }

    let actor_user_id = message
        .sender
        .get_user()
        .map(|user| i64::from(user.id))
        .unwrap_or_default();
    if !runtime.admin_ids.contains(&actor_user_id) {
        return match runtime
            .effects
            .send_admin_text(admin_text_plan(
                message,
                ADMIN_COMMAND_UNAUTHORIZED_TEXT,
                Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER),
            ))
            .await
        {
            Ok(()) => Ok(AdminRedisCacheCommandOutcome::Unauthorized),
            Err(error) => {
                tracing::warn!(%error, actor_user_id, "failed to queue admin command denial");
                Ok(AdminRedisCacheCommandOutcome::UnauthorizedSendError {
                    message: error.to_string(),
                })
            }
        };
    }

    let started = Instant::now();
    match runtime.flusher.flush_db().await {
        Ok(()) => {
            let text = admin_redis_cache_success_text(started.elapsed());
            send_admin_redis_result(
                runtime.effects,
                message,
                text,
                AdminRedisCacheCommandOutcome::Cleared,
            )
            .await
        }
        Err(error) => {
            let text = format!("❌ Ошибка очистки кеша Redis:\n{error}");
            send_admin_redis_result(
                runtime.effects,
                message,
                text,
                AdminRedisCacheCommandOutcome::FlushError { message: error },
            )
            .await
        }
    }
}

async fn send_admin_redis_result<Effects, HandleError>(
    effects: &Effects,
    message: &TelegramMessage,
    text: String,
    success: AdminRedisCacheCommandOutcome,
) -> Result<AdminRedisCacheCommandOutcome, HandleError>
where
    Effects: AdminCommandEffects + Sync,
{
    match effects
        .send_admin_text(admin_text_plan(message, &text, None))
        .await
    {
        Ok(()) => Ok(success),
        Err(error) => {
            tracing::warn!(%error, "failed to queue admin Redis cache reply");
            Ok(AdminRedisCacheCommandOutcome::SendError {
                message: error.to_string(),
            })
        }
    }
}

#[derive(Clone)]
pub struct AdminGeminiCacheCommandUpdateHandler<Purger, Effects, Next> {
    purger: Option<Arc<Purger>>,
    effects: Arc<Effects>,
    admin_ids: Arc<[i64]>,
    bot_username: String,
    next: Arc<Next>,
}

impl<Purger, Effects, Next> AdminGeminiCacheCommandUpdateHandler<Purger, Effects, Next> {
    /// Build a handler around the real downstream update handler.
    #[must_use]
    pub fn new(
        purger: Option<Arc<Purger>>,
        effects: Arc<Effects>,
        admin_ids: Vec<i64>,
        bot_username: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            purger,
            effects,
            admin_ids: Arc::from(admin_ids.into_boxed_slice()),
            bot_username: bot_username.into(),
            next,
        }
    }
}

impl<Purger, Effects, Next> UpdateHandler
    for AdminGeminiCacheCommandUpdateHandler<Purger, Effects, Next>
where
    Purger: RuntimeGeminiCachePurger + Send + Sync,
    Effects: AdminCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_admin_gemini_cache_command_update_or_else_at(
                AdminGeminiCacheRuntime {
                    purger: self.purger.as_deref(),
                    effects: self.effects.as_ref(),
                    admin_ids: &self.admin_ids,
                    bot_username: &self.bot_username,
                },
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Copy)]
pub struct AdminGeminiCacheRuntime<'a, Purger, Effects> {
    /// Gemini explicit-cache purge boundary.
    pub purger: Option<&'a Purger>,
    /// Admin text side effects.
    pub effects: &'a Effects,
    /// Authorized Telegram admin user IDs.
    pub admin_ids: &'a [i64],
    /// Current bot username from `getMe`.
    pub bot_username: &'a str,
}

pub async fn handle_admin_gemini_cache_command_update_or_else_at<
    Purger,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    runtime: AdminGeminiCacheRuntime<'_, Purger, Effects>,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<AdminGeminiCacheCommandOutcome, HandleError>
where
    Purger: RuntimeGeminiCachePurger + Sync,
    Effects: AdminCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update).await?;
        return Ok(AdminGeminiCacheCommandOutcome::Delegated);
    };
    let Some(command) = admin_command_for_message(message, runtime.bot_username) else {
        handle_other(update).await?;
        return Ok(AdminGeminiCacheCommandOutcome::Delegated);
    };
    if command.name != ADMIN_CLEAR_GEMINI_CACHE_COMMAND {
        handle_other(update).await?;
        return Ok(AdminGeminiCacheCommandOutcome::Delegated);
    }

    let actor_user_id = message
        .sender
        .get_user()
        .map(|user| i64::from(user.id))
        .unwrap_or_default();
    if !runtime.admin_ids.contains(&actor_user_id) {
        return match runtime
            .effects
            .send_admin_text(admin_text_plan(
                message,
                ADMIN_COMMAND_UNAUTHORIZED_TEXT,
                Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER),
            ))
            .await
        {
            Ok(()) => Ok(AdminGeminiCacheCommandOutcome::Unauthorized),
            Err(error) => {
                tracing::warn!(%error, actor_user_id, "failed to queue admin command denial");
                Ok(AdminGeminiCacheCommandOutcome::UnauthorizedSendError {
                    message: error.to_string(),
                })
            }
        };
    }

    let Some(purger) = runtime.purger else {
        return send_admin_gemini_result(
            runtime.effects,
            message,
            ADMIN_GEMINI_CACHE_UNCONFIGURED_TEXT.to_owned(),
            AdminGeminiCacheCommandOutcome::Unconfigured,
        )
        .await;
    };

    let started = Instant::now();
    match purger.purge().await {
        Ok(result) => {
            let text = admin_gemini_cache_success_text(&result, started.elapsed());
            send_admin_gemini_result(
                runtime.effects,
                message,
                text,
                AdminGeminiCacheCommandOutcome::Purged(result),
            )
            .await
        }
        Err(error) => {
            let text = format!("❌ Ошибка очистки Gemini explicit cache:\n{error}");
            send_admin_gemini_result(
                runtime.effects,
                message,
                text,
                AdminGeminiCacheCommandOutcome::PurgeError { message: error },
            )
            .await
        }
    }
}

async fn send_admin_gemini_result<Effects, HandleError>(
    effects: &Effects,
    message: &TelegramMessage,
    text: String,
    success: AdminGeminiCacheCommandOutcome,
) -> Result<AdminGeminiCacheCommandOutcome, HandleError>
where
    Effects: AdminCommandEffects + Sync,
{
    match effects
        .send_admin_text(admin_text_plan(message, &text, None))
        .await
    {
        Ok(()) => Ok(success),
        Err(error) => {
            tracing::warn!(%error, "failed to queue admin Gemini cache reply");
            Ok(AdminGeminiCacheCommandOutcome::SendError {
                message: error.to_string(),
            })
        }
    }
}

fn admin_gemini_cache_success_text(
    result: &RuntimeGeminiCachePurgeResultData,
    duration: Duration,
) -> String {
    let status = if result.failed > 0 { "⚠️" } else { "✅" };
    format!(
        "{status} Gemini explicit cache обработан.\n\n\
⏱️ Время выполнения: {}\n\
🔎 Проверено кэшей на API: {}\n\
🎯 Найдено кешей Plotva: {}\n\
🗑️ Удалено: {}\n\
⚠️ Ошибок удаления: {}\n\n\
ℹ️ Локальный индекс explicit cache очищен. Кэши будут автоматически пересозданы при следующих запросах.",
        go_duration_string(duration),
        result.scanned,
        result.matched,
        result.deleted,
        result.failed,
    )
}

fn admin_redis_cache_success_text(duration: Duration) -> String {
    format!(
        "✅ Кеш Redis успешно очищен!\n\n\
⏱️ Время выполнения: {}\n\
🗑️ Все ключи удалены из базы данных Redis\n\n\
ℹ️ Это включает:\n\
• Кеш промптов для изображений\n\
• Кеш оптимизации текста\n\
• Кеш системных инструкций\n\
• Временные данные приложения",
        go_duration_string(duration),
    )
}

fn admin_queue_disabled_text() -> &'static str {
    "❌ Персистентная система очередей отключена"
}

fn admin_queue_status_text<Effects>(runtime: &AdminQueueRuntime<'_, Effects>) -> String {
    let queue_names = runtime.config.queue_names();
    let pending_by_queue = queue_counts_by_status(runtime.taskman, "pending");
    let processing_by_queue = queue_counts_by_status(runtime.taskman, "processing");
    let eta_by_queue = queue_eta_seconds(runtime.taskman, &queue_names);
    let mut lines = Vec::with_capacity(queue_names.len() + 5);
    lines.push("📊 <b>Статус очередей:</b>\n".to_owned());
    for queue_name in queue_names {
        let pending = pending_by_queue
            .get(&queue_name)
            .copied()
            .unwrap_or_default();
        let processing = processing_by_queue
            .get(&queue_name)
            .copied()
            .unwrap_or_default();
        let estimated_time = eta_by_queue.get(&queue_name).copied().unwrap_or_default();
        lines.push(queue_status_line(
            &queue_name,
            pending,
            processing,
            estimated_time,
        ));
    }
    lines.push("\n📈 <b>Системные метрики:</b>".to_owned());
    lines.push("• Мониторинг очередей включен".to_owned());
    lines.push("• Статистика производительности доступна".to_owned());
    lines.join("\n")
}

async fn admin_queue_stats_text<Effects>(runtime: &AdminQueueRuntime<'_, Effects>) -> String {
    let queue_names = runtime.config.queue_names();
    let pending_by_queue = queue_counts_by_status(runtime.taskman, "pending");
    let processing_by_queue = queue_counts_by_status(runtime.taskman, "processing");
    let (total_pending, total_processing) =
        queue_stats_totals(&queue_names, &pending_by_queue, &processing_by_queue);
    let mut lines = Vec::new();
    lines.push("📈 <b>Статистика заданий:</b>\n".to_owned());
    lines.push(format!(
        "📝 Pending: {total_pending} | Processing: {total_processing}"
    ));
    lines.push("\n🔄 <b>По очередям:</b>".to_owned());
    lines.extend(queue_stats_count_lines(
        &queue_names,
        &pending_by_queue,
        &processing_by_queue,
    ));
    lines.push("\n🕒 <b>Старейшие:</b>".to_owned());
    lines.extend(queue_stats_oldest_lines(
        runtime.taskman,
        &queue_names,
        runtime.config.default_processing_timeout,
    ));
    lines.push("\n📮 <b>Очередь отправки:</b>".to_owned());
    lines.extend(queue_stats_dispatcher_lines(
        runtime
            .dispatcher
            .map(|dispatcher| dispatcher.stats())
            .unwrap_or_default(),
    ));
    lines.push("\n📬 <b>Очередь обновлений:</b>".to_owned());
    lines.push(format!(
        "• updates_queue_len: {}",
        updates_queue_len_string(runtime.updates).await
    ));
    lines.push("\n🖼️ <b>Vision:</b>".to_owned());
    lines.push(format!(
        "• media_gate_in_use: {}",
        (runtime.media_gate_in_use)()
    ));
    lines.push("\n💬 <b>Dialog debounce:</b>".to_owned());
    lines.push(format!(
        "• dialog_debounce_len: {}",
        runtime.dialog_debounce_len.map_or(0, |len| len())
    ));
    lines.push("\n⚙️ <b>Конфигурация воркеров:</b>".to_owned());
    lines.extend(queue_stats_worker_lines(runtime.config, &queue_names));
    lines.join("\n")
}

fn queue_counts_by_status(
    taskman: Option<&dyn RuntimeTaskmanInspector>,
    status: &str,
) -> BTreeMap<String, i32> {
    let Some(taskman) = taskman else {
        return BTreeMap::new();
    };
    let result = taskman.list_jobs(RuntimeTaskmanJobsFilter {
        status: vec![status.to_owned()],
        limit: 1,
        ..RuntimeTaskmanJobsFilter::default()
    });
    match result {
        Ok(result) => queue_count_map(&result.summary.by_queue),
        Err(error) => {
            tracing::warn!(%error, status, "failed to list taskman jobs for admin queue command");
            BTreeMap::new()
        }
    }
}

fn queue_count_map(value: &serde_json::Value) -> BTreeMap<String, i32> {
    value
        .as_object()
        .into_iter()
        .flat_map(|object| object.iter())
        .filter_map(|(queue_name, value)| {
            let count = value.as_i64()?;
            let fallback = if count < 0 { i32::MIN } else { i32::MAX };
            Some((queue_name.clone(), i32::try_from(count).unwrap_or(fallback)))
        })
        .collect()
}

fn queue_eta_seconds(
    taskman: Option<&dyn RuntimeTaskmanInspector>,
    queue_names: &[String],
) -> BTreeMap<String, i32> {
    let Some(taskman) = taskman else {
        return BTreeMap::new();
    };
    match taskman.queue_diagnostics(queue_names.to_vec(), openplotva_taskman::DEFAULT_PRIORITY) {
        Ok(diagnostics) => diagnostics
            .queues
            .into_iter()
            .map(|queue| (queue.queue_name, queue.eta_seconds.max(0)))
            .collect(),
        Err(error) => {
            tracing::warn!(%error, "failed to load taskman queue diagnostics for admin status");
            BTreeMap::new()
        }
    }
}

fn queue_status_line(
    queue_name: &str,
    pending: i32,
    processing: i32,
    estimated_time_seconds: i32,
) -> String {
    let time_text = if estimated_time_seconds > 0 {
        format!(
            " ({})",
            format_queue_metric_age(Duration::from_secs(estimated_time_seconds as u64))
        )
    } else {
        String::new()
    };
    format!("🔄 <b>{queue_name}:</b> {pending} pending, {processing} processing{time_text}")
}

fn queue_stats_totals(
    queue_names: &[String],
    pending_by_queue: &BTreeMap<String, i32>,
    processing_by_queue: &BTreeMap<String, i32>,
) -> (i32, i32) {
    queue_names
        .iter()
        .fold((0, 0), |(pending, processing), queue_name| {
            (
                pending
                    + pending_by_queue
                        .get(queue_name)
                        .copied()
                        .unwrap_or_default(),
                processing
                    + processing_by_queue
                        .get(queue_name)
                        .copied()
                        .unwrap_or_default(),
            )
        })
}

fn queue_stats_count_lines(
    queue_names: &[String],
    pending_by_queue: &BTreeMap<String, i32>,
    processing_by_queue: &BTreeMap<String, i32>,
) -> Vec<String> {
    queue_names
        .iter()
        .map(|queue_name| {
            format!(
                "• {queue_name}: pending {}, processing {}",
                pending_by_queue
                    .get(queue_name)
                    .copied()
                    .unwrap_or_default(),
                processing_by_queue
                    .get(queue_name)
                    .copied()
                    .unwrap_or_default()
            )
        })
        .collect()
}

fn queue_stats_oldest_lines(
    taskman: Option<&dyn RuntimeTaskmanInspector>,
    queue_names: &[String],
    timeout: Duration,
) -> Vec<String> {
    queue_names
        .iter()
        .map(|queue_name| {
            let pending_age = queue_oldest_age(taskman, queue_name, "pending", "created_at");
            let processing_age = queue_oldest_age(taskman, queue_name, "processing", "started_at");
            let processing_flag = processing_age
                .map(|age| queue_stats_processing_flag(age, timeout))
                .unwrap_or_default();
            format!(
                "• {queue_name}: pending {}, processing {}{}",
                format_queue_optional_age(pending_age),
                format_queue_optional_age(processing_age),
                processing_flag
            )
        })
        .collect()
}

fn queue_oldest_age(
    taskman: Option<&dyn RuntimeTaskmanInspector>,
    queue_name: &str,
    status: &str,
    time_field: &str,
) -> Option<Duration> {
    let taskman = taskman?;
    let result = taskman.list_jobs(RuntimeTaskmanJobsFilter {
        status: vec![status.to_owned()],
        queue: vec![queue_name.to_owned()],
        sort_by: time_field.to_owned(),
        sort_dir: "asc".to_owned(),
        limit: 1,
        ..RuntimeTaskmanJobsFilter::default()
    });
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(%error, queue_name, status, "failed to list oldest taskman job");
            return None;
        }
    };
    let item = result.items.first()?;
    let timestamp = queue_item_time(item, time_field)?;
    let now = OffsetDateTime::now_utc();
    (now > timestamp)
        .then(|| now - timestamp)
        .and_then(|duration| duration.try_into().ok())
}

fn queue_item_time(
    item: &RuntimeTaskmanJobListEntryData,
    time_field: &str,
) -> Option<OffsetDateTime> {
    let timestamp = if time_field == "started_at" {
        item.started_at.as_deref()?
    } else {
        item.created_at.as_str()
    };
    OffsetDateTime::parse(timestamp, &Rfc3339).ok()
}

fn format_queue_optional_age(age: Option<Duration>) -> String {
    age.map(format_queue_metric_age)
        .unwrap_or_else(|| QUEUE_STATS_MISSING_METRIC_VALUE.to_owned())
}

fn format_queue_metric_age(age: Duration) -> String {
    if age.is_zero() {
        return QUEUE_STATS_MISSING_METRIC_VALUE.to_owned();
    }
    format_go_user_duration(age)
}

fn queue_stats_processing_flag(age: Duration, timeout: Duration) -> &'static str {
    if !timeout.is_zero() && age > timeout {
        " ⚠️ 오래"
    } else {
        ""
    }
}

fn queue_stats_dispatcher_lines(stats: RuntimeDispatcherStatsData) -> Vec<String> {
    vec![
        format!(
            "• regular: {} (oldest {})",
            stats.regular_queue_size,
            format_queue_age_ms(stats.oldest_regular_age_ms)
        ),
        format!(
            "• immediate: {} (oldest {})",
            stats.immediate_queue_size,
            format_queue_age_ms(stats.oldest_immediate_age_ms)
        ),
        format!("• processed_total: {}", stats.processed_total),
        format!("• deduped_total: {}", stats.deduped_total),
    ]
}

fn format_queue_age_ms(age_ms: i32) -> String {
    if age_ms <= 0 {
        QUEUE_STATS_MISSING_METRIC_VALUE.to_owned()
    } else {
        format_queue_metric_age(Duration::from_millis(age_ms as u64))
    }
}

async fn updates_queue_len_string(updates: Option<&dyn RuntimeUpdatesInspector>) -> String {
    let Some(updates) = updates else {
        return QUEUE_STATS_MISSING_METRIC_VALUE.to_owned();
    };
    match updates.snapshot().await {
        Ok(RuntimeUpdatesRuntimeData {
            queue_len,
            queue_error: None,
            ..
        }) => queue_len.to_string(),
        Ok(RuntimeUpdatesRuntimeData {
            queue_error: Some(error),
            ..
        }) => {
            tracing::warn!(%error, "failed to get updates queue length");
            QUEUE_STATS_MISSING_METRIC_VALUE.to_owned()
        }
        Err(error) => {
            tracing::warn!(%error, "failed to get updates runtime snapshot");
            QUEUE_STATS_MISSING_METRIC_VALUE.to_owned()
        }
    }
}

fn queue_stats_worker_lines(
    config: &AdminQueueCommandConfig,
    queue_names: &[String],
) -> Vec<String> {
    queue_names
        .iter()
        .map(|queue_name| {
            format!(
                "• {queue_name}: {} воркеров",
                config.worker_count(queue_name)
            )
        })
        .collect()
}

fn format_go_user_duration(duration: Duration) -> String {
    if duration.is_zero() {
        return "меньше минуты".to_owned();
    }
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds / 60) % 60;
    let seconds = total_seconds % 60;
    let mut parts = Vec::new();
    if hours > 0 {
        let hour_word = match hours {
            1 => "1 час".to_owned(),
            2..=4 => format!("{hours} часа"),
            _ => format!("{hours} часов"),
        };
        parts.push(hour_word);
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdminCommandDefinition {
    name: &'static str,
    description: &'static str,
    usage: &'static str,
    category: &'static str,
}

const ADMIN_COMMAND_DEFINITIONS: &[AdminCommandDefinition] = &[
    AdminCommandDefinition {
        name: "admin_enable_chat",
        description: "Включить возможности бота для указанного чата",
        usage: "/admin_enable_chat [chat_id или @channel_username]",
        category: "Управление чатами",
    },
    AdminCommandDefinition {
        name: "admin_chat_settings",
        description: "Получить кнопку настроек чата по ID или username",
        usage: "/admin_chat_settings [chat_id или @channel_username]",
        category: "Управление чатами",
    },
    AdminCommandDefinition {
        name: ADMIN_QUEUE_STATUS_COMMAND,
        description: "Показать статус очередей задач",
        usage: "/admin_queue_status",
        category: "Мониторинг очередей",
    },
    AdminCommandDefinition {
        name: ADMIN_QUEUE_STATS_COMMAND,
        description: "Показать детальную статистику очередей и заданий",
        usage: "/admin_queue_stats",
        category: "Мониторинг очередей",
    },
    AdminCommandDefinition {
        name: "admin_refund",
        description: "Возврат платежа по ID транзакции",
        usage: "/admin_refund [telegram_payment_charge_id]",
        category: "Управление платежами",
    },
    AdminCommandDefinition {
        name: "admin_grant_vip",
        description: "Предоставить VIP-статус пользователю без оплаты",
        usage: "/admin_grant_vip [дни=1] [user_id/@username/https://t.me/username] или ответ на сообщение",
        category: "Управление подписками",
    },
    AdminCommandDefinition {
        name: "admin_cancel_vip",
        description: "Отозвать VIP-статус у пользователя",
        usage: "/admin_cancel_vip [user_id или @username]",
        category: "Управление подписками",
    },
    AdminCommandDefinition {
        name: ADMIN_HELP_COMMAND,
        description: "Показать список доступных команд администратора",
        usage: "/admin_help [категория]",
        category: "Справка",
    },
    AdminCommandDefinition {
        name: ADMIN_CLEAR_CACHE_COMMAND,
        description: "Очистить весь кеш Redis (промпты, системные инструкции, временные данные)",
        usage: "/admin_clear_cache",
        category: "Системное администрирование",
    },
    AdminCommandDefinition {
        name: ADMIN_CLEAR_GEMINI_CACHE_COMMAND,
        description: "Удалить удаленные explicit-кеши Gemini, созданные Plotva",
        usage: "/admin_clear_gemini_cache",
        category: "Системное администрирование",
    },
    AdminCommandDefinition {
        name: ADMIN_RUNTIME_TOKEN_COMMAND,
        description: "Выпустить новый bearer token для runtime debug API",
        usage: "/admin_runtime_token",
        category: "Runtime API",
    },
    AdminCommandDefinition {
        name: ADMIN_RUNTIME_TOKEN_LIST_COMMAND,
        description: "Показать активные bearer tokens для runtime debug API",
        usage: "/admin_runtime_token_list",
        category: "Runtime API",
    },
];

#[derive(Debug, Eq, PartialEq)]
struct AdminCommandCategory {
    name: &'static str,
    commands: Vec<&'static AdminCommandDefinition>,
}

fn admin_help_filter_category(args: &str) -> &str {
    args.split_whitespace().next().unwrap_or("")
}

fn admin_command_categories(filter_category: &str) -> Vec<AdminCommandCategory> {
    let mut categories: Vec<AdminCommandCategory> = Vec::new();
    for command in ADMIN_COMMAND_DEFINITIONS {
        if !filter_category.is_empty() && !unicode_case_eq(command.category, filter_category) {
            continue;
        }
        if let Some(category) = categories
            .iter_mut()
            .find(|category| category.name == command.category)
        {
            category.commands.push(command);
        } else {
            categories.push(AdminCommandCategory {
                name: command.category,
                commands: vec![command],
            });
        }
    }
    categories
}

fn admin_available_categories() -> Vec<&'static str> {
    let mut categories = Vec::new();
    for command in ADMIN_COMMAND_DEFINITIONS {
        if !categories.contains(&command.category) {
            categories.push(command.category);
        }
    }
    categories
}

fn admin_category_not_found_text(filter_category: &str, available_categories: &[&str]) -> String {
    format!(
        "❌ Категория \"{filter_category}\" не найдена.\n\n\
<b>Доступные категории:</b>\n\
• {}\n\n\
<i>Используйте</i> <code>/admin_help</code> <i>для просмотра всех команд.</i>",
        available_categories.join("\n• ")
    )
}

fn admin_help_text(categories: &[AdminCommandCategory], filter_category: &str) -> String {
    let mut help_text = String::from("<b>📋 Команды администратора</b>\n\n");
    for (index, category) in categories.iter().enumerate() {
        if index > 0 {
            help_text.push('\n');
        }
        help_text.push_str(&format!("<b>{}:</b>\n", category.name));
        for command in &category.commands {
            help_text.push_str(&format!(
                "• <code>{}</code>\n  <i>{}</i>\n",
                command.usage, command.description
            ));
        }
    }
    if filter_category.is_empty() {
        help_text.push_str(
            "\n<i>💡 Используйте</i> <code>/admin_help [категория]</code> <i>для фильтрации команд по категории.</i>",
        );
    }
    help_text
}

fn unicode_case_eq(lhs: &str, rhs: &str) -> bool {
    lhs == rhs || lhs.to_lowercase() == rhs.to_lowercase()
}

fn runtime_token_usage(command_name: &str) -> &'static str {
    match command_name {
        ADMIN_RUNTIME_TOKEN_LIST_COMMAND => "/admin_runtime_token_list",
        _ => "/admin_runtime_token",
    }
}

fn admin_private_chat_required_text(usage: &str) -> String {
    format!("🔒 Эту команду нужно выполнять в личном чате с ботом.\n\nИспользование: {usage}")
}

fn admin_runtime_token_issued_text(
    issued: &IssuedRuntimeToken,
    ttl: TimeDuration,
    tls_pin: &str,
) -> String {
    let created_at = runtime_token_created_at(&issued.meta);
    let expires_at = created_at + ttl;
    format!(
        "🔐 <b>Runtime API token выпущен</b>\n\n\
<b>Токен показывается только один раз:</b>\n\
<code>{}</code>\n\n\
<b>ID:</b> <code>{}</code>\n\
<b>Создан:</b> <code>{}</code>\n\
<b>Истекает:</b> <code>{}</code>\n\n\
<b>Authorization header:</b>\n\
<code>Authorization: Bearer &lt;token&gt;</code>\n\n\
<b>TLS public key pin:</b>\n\
<code>{tls_pin}</code>",
        issued.token,
        issued.meta.id,
        format_runtime_token_timestamp(created_at),
        format_runtime_token_timestamp(expires_at),
    )
}

fn admin_runtime_token_list_text(tokens: &[RuntimeToken], ttl: TimeDuration) -> String {
    let mut lines = Vec::with_capacity(tokens.len() + 2);
    lines.push("🔐 <b>Активные runtime API токены</b>".to_owned());
    lines.push(String::new());
    for token in tokens {
        let created_at = runtime_token_created_at(token);
        let expires_at = created_at + ttl;
        lines.push(format!(
            "• <code>{}</code> — created <code>{}</code>, expires <code>{}</code>",
            token.id,
            format_runtime_token_timestamp(created_at),
            format_runtime_token_timestamp(expires_at),
        ));
    }
    lines.join("\n")
}

fn runtime_token_created_at(token: &RuntimeToken) -> OffsetDateTime {
    token.created_at.unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn format_runtime_token_timestamp(value: OffsetDateTime) -> String {
    let utc = value.to_offset(UtcOffset::UTC);
    utc.format(&Rfc3339).unwrap_or_else(|_| utc.to_string())
}

fn admin_html_text_plan(
    message: &TelegramMessage,
    text: &str,
    ephemeral_delete_after: Option<Duration>,
) -> AdminTextPlan {
    let mut plan = admin_text_plan(message, text, ephemeral_delete_after);
    plan.message.render_as = TELEGRAM_PARSE_MODE_HTML.to_owned();
    plan
}

fn admin_text_plan(
    message: &TelegramMessage,
    text: &str,
    ephemeral_delete_after: Option<Duration>,
) -> AdminTextPlan {
    let chat = ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    };
    AdminTextPlan {
        message: TextMessageRequest {
            chat: Some(chat),
            message_thread_id: message.message_thread_id.unwrap_or_default(),
            disable_notification: false,
            allow_sending_without_reply: None,
            text: text.to_owned(),
            render_as: String::new(),
            reply_markup: None,
        },
        reply_to: ReplyMessageRef {
            message_id: message.id,
            chat,
            is_topic_message: message.message_thread_id.is_some(),
            message_thread_id: message.message_thread_id.unwrap_or_default(),
        },
        ephemeral_delete_after,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdminCommand<'a> {
    name: &'a str,
    arguments: &'a str,
}

fn admin_command_for_message<'a>(
    message: &'a TelegramMessage,
    bot_username: &str,
) -> Option<AdminCommand<'a>> {
    let command = leading_command_from_message(message)?;
    if matches!(message.chat, TelegramChat::Private(_)) {
        return Some(AdminCommand {
            name: command.name,
            arguments: command.arguments,
        });
    }
    (command.target == Some(bot_username)).then_some(AdminCommand {
        name: command.name,
        arguments: command.arguments,
    })
}

fn admin_enable_chat_display_title(target: &AdminChatSettingsTarget) -> String {
    if !target.title.is_empty() {
        return target.title.clone();
    }
    if !target.username.is_empty() {
        return format!("@{}", target.username);
    }
    if !target.first_name.is_empty() {
        if !target.last_name.is_empty() {
            return format!("{} {}", target.first_name, target.last_name);
        }
        return target.first_name.clone();
    }
    target.id.to_string()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedCommand<'a> {
    name: &'a str,
    target: Option<&'a str>,
    arguments: &'a str,
}

fn leading_command_from_message(message: &TelegramMessage) -> Option<ParsedCommand<'_>> {
    let TelegramMessageData::Text(text) = &message.data else {
        return None;
    };
    leading_command_from_text(text)
}

fn leading_command_from_text(text: &TelegramText) -> Option<ParsedCommand<'_>> {
    let first = text.entities.as_ref()?.into_iter().next()?;
    let TelegramTextEntity::BotCommand(position) = first else {
        return None;
    };
    if position.offset != 0 {
        return None;
    }
    let command_end = utf16_index_to_byte_index(&text.data, position.length)?;
    let command_with_slash = text.data.get(..command_end)?;
    let command_with_at = command_with_slash.strip_prefix('/')?;
    let arguments = text.data.get(command_end..)?;
    let (name, target) = command_with_at
        .split_once('@')
        .map_or((command_with_at, None), |(name, target)| {
            (name, Some(target))
        });
    Some(ParsedCommand {
        name,
        target,
        arguments,
    })
}

fn utf16_index_to_byte_index(text: &str, utf16_units: u32) -> Option<usize> {
    let mut consumed = 0_u32;
    for (byte_index, ch) in text.char_indices() {
        if consumed == utf16_units {
            return Some(byte_index);
        }
        consumed = consumed.checked_add(u32::try_from(ch.len_utf16()).ok()?)?;
        if consumed > utf16_units {
            return None;
        }
    }
    (consumed == utf16_units).then_some(text.len())
}

fn go_duration_string(duration: Duration) -> String {
    let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return format_go_fraction(nanos, 1_000, "µs");
    }
    if nanos < 1_000_000_000 {
        return format_go_fraction(nanos, 1_000_000, "ms");
    }

    let total_seconds = nanos / 1_000_000_000;
    let remainder = nanos % 1_000_000_000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    let seconds_part = if remainder == 0 {
        format!("{seconds}s")
    } else {
        format_go_fraction(seconds * 1_000_000_000 + remainder, 1_000_000_000, "s")
    };
    if hours > 0 {
        return format!("{hours}h{minutes}m{seconds_part}");
    }
    if minutes > 0 {
        return format!("{minutes}m{seconds_part}");
    }
    seconds_part
}

fn format_go_fraction(value: u64, unit: u64, suffix: &str) -> String {
    let whole = value / unit;
    let mut fraction = format!("{:03}", (value % unit) * 1_000 / unit);
    while fraction.ends_with('0') {
        fraction.pop();
    }
    if fraction.is_empty() {
        format!("{whole}{suffix}")
    } else {
        format!("{whole}.{fraction}{suffix}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_server::{
        RuntimeGeminiCachePurgerFuture, RuntimeTaskmanDiagnosticsData,
        RuntimeTaskmanJobListResultData, RuntimeTaskmanJobSummaryData,
        RuntimeTaskmanQueueDiagnosticsData, RuntimeUpdatesInspectorFuture,
    };
    use serde_json::json;
    use std::{
        convert::Infallible,
        env, io,
        sync::{Mutex, MutexGuard},
        time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
    };

    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
        UpdateStateStoreFuture, process_update_with_state_store_at,
    };
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};

    fn sample_command_update(
        text: &str,
        from_id: i64,
        chat: serde_json::Value,
        thread_id: Option<i64>,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let command_len = text.split_whitespace().next().map_or(text.len(), str::len);
        let mut value = serde_json::json!({
            "update_id": 500,
            "message": {
                "message_id": 10,
                "date": 1_710_000_000,
                "chat": chat,
                "from": {"id": from_id, "is_bot": false, "first_name": "Ada"},
                "text": text,
                "entities": [{"offset": 0, "length": command_len, "type": "bot_command"}]
            }
        });
        if let Some(thread_id) = thread_id {
            value["message"]["message_thread_id"] = serde_json::json!(thread_id);
        }
        serde_json::from_value(value)
    }

    fn private_chat() -> serde_json::Value {
        serde_json::json!({"id": 42, "type": "private", "first_name": "Ada"})
    }

    fn group_chat() -> serde_json::Value {
        serde_json::json!({"id": -42, "type": "supergroup", "title": "Group"})
    }

    #[tokio::test]
    async fn admin_settings_private_command_matches_go_silent_gates_and_button()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let authorized = sample_command_update("/admin_settings", 5, private_chat(), None)?;

        let outcome = handle_admin_settings_command_update_or_else_at(
            AdminSettingsRuntime {
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
                web_app_url: "https://plotva.example/base",
            },
            authorized,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(outcome, AdminSettingsCommandOutcome::Sent);
        let plans = effects.plans();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].message.text, ADMIN_SETTINGS_TEXT);
        assert_eq!(plans[0].message.render_as, "");
        assert_eq!(plans[0].ephemeral_delete_after, None);
        let markup = serde_json::to_value(plans[0].message.reply_markup.as_ref())?;
        assert_eq!(
            markup["inline_keyboard"][0][0]["text"],
            ADMIN_SETTINGS_BUTTON_TEXT
        );
        assert_eq!(
            markup["inline_keyboard"][0][0]["url"],
            "https://plotva.example/base/admin/"
        );

        let unauthorized = sample_command_update("/admin_settings", 9, private_chat(), None)?;
        let outcome = handle_admin_settings_command_update_or_else_at(
            AdminSettingsRuntime {
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
                web_app_url: "https://plotva.example/base",
            },
            unauthorized,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminSettingsCommandOutcome::Unauthorized);
        assert_eq!(effects.plans().len(), 1);

        let missing_url = sample_command_update("/admin_settings", 5, private_chat(), None)?;
        let outcome = handle_admin_settings_command_update_or_else_at(
            AdminSettingsRuntime {
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
                web_app_url: "",
            },
            missing_url,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminSettingsCommandOutcome::MissingWebAppUrl);
        assert_eq!(effects.plans().len(), 1);

        let targeted_group =
            sample_command_update("/admin_settings@PlotvaBot", 5, group_chat(), Some(7))?;
        let outcome = handle_admin_settings_command_update_or_else_at(
            AdminSettingsRuntime {
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
                web_app_url: "https://plotva.example/base",
            },
            targeted_group,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminSettingsCommandOutcome::Delegated);
        assert_eq!(effects.plans().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn admin_enable_chat_command_matches_go_auth_resolution_and_texts()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let resolver = AdminChatTargetResolverStub::with_target(AdminChatSettingsTarget {
            id: -100777,
            title: "Target Lab".to_owned(),
            username: "target_lab".to_owned(),
            first_name: String::new(),
            last_name: String::new(),
        });
        let communication = ChatCommunicationEffectsStub::default();
        let authorized =
            sample_command_update("/admin_enable_chat @target_lab", 5, private_chat(), None)?;

        let outcome = handle_admin_enable_chat_command_update_or_else_at(
            AdminEnableChatRuntime {
                resolver: &resolver,
                communication: &communication,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            authorized,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(
            outcome,
            AdminEnableChatCommandOutcome::Enabled { chat_id: -100777 }
        );
        assert_eq!(resolver.calls(), vec!["@target_lab".to_owned()]);
        assert_eq!(communication.enabled(), vec![-100777]);
        let plans = effects.plans();
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].message.text,
            "Global text and draw replies have been ENABLED for chat 'Target Lab' (ID: -100777)."
        );
        assert_eq!(plans[0].ephemeral_delete_after, None);

        let usage = sample_command_update("/admin_enable_chat   ", 5, private_chat(), None)?;
        let outcome = handle_admin_enable_chat_command_update_or_else_at(
            AdminEnableChatRuntime {
                resolver: &resolver,
                communication: &communication,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            usage,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminEnableChatCommandOutcome::Usage);
        assert_eq!(
            effects.plans()[1].message.text,
            ADMIN_ENABLE_CHAT_USAGE_TEXT
        );

        let unauthorized =
            sample_command_update("/admin_enable_chat -100", 9, private_chat(), None)?;
        let outcome = handle_admin_enable_chat_command_update_or_else_at(
            AdminEnableChatRuntime {
                resolver: &resolver,
                communication: &communication,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            unauthorized,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminEnableChatCommandOutcome::Unauthorized);
        assert_eq!(
            effects.plans()[2].ephemeral_delete_after,
            Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER)
        );

        let failing_resolver = AdminChatTargetResolverStub::failing("no chat");
        let resolve_error =
            sample_command_update("/admin_enable_chat missing_chat", 5, private_chat(), None)?;
        let outcome = handle_admin_enable_chat_command_update_or_else_at(
            AdminEnableChatRuntime {
                resolver: &failing_resolver,
                communication: &communication,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            resolve_error,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(
            outcome,
            AdminEnableChatCommandOutcome::ResolveError {
                message: "Could not find or access chat: missing_chat. Error: no chat".to_owned()
            }
        );
        assert_eq!(
            effects.plans()[3].message.text,
            "Could not find or access chat: missing_chat. Error: no chat"
        );

        let untargeted_group =
            sample_command_update("/admin_enable_chat -100", 5, group_chat(), Some(7))?;
        let outcome = handle_admin_enable_chat_command_update_or_else_at(
            AdminEnableChatRuntime {
                resolver: &resolver,
                communication: &communication,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            untargeted_group,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminEnableChatCommandOutcome::Delegated);
        assert_eq!(effects.plans().len(), 4);
        Ok(())
    }

    #[test]
    fn admin_gemini_cache_success_text_matches_go_shape() {
        let text = admin_gemini_cache_success_text(
            &RuntimeGeminiCachePurgeResultData {
                scanned: 7,
                matched: 3,
                deleted: 2,
                failed: 1,
            },
            Duration::from_millis(1234),
        );

        assert!(text.starts_with("⚠️ Gemini explicit cache обработан."));
        assert!(text.contains("⏱️ Время выполнения: 1.234s"));
        assert!(text.contains("🔎 Проверено кэшей на API: 7"));
        assert!(text.contains("🎯 Найдено кешей Plotva: 3"));
        assert!(text.contains("🗑️ Удалено: 2"));
        assert!(text.contains("⚠️ Ошибок удаления: 1"));
        assert!(text.contains("Локальный индекс explicit cache очищен"));
    }

    #[test]
    fn admin_redis_cache_success_text_matches_go_shape() {
        let text = admin_redis_cache_success_text(Duration::from_millis(250));

        assert!(text.starts_with("✅ Кеш Redis успешно очищен!"));
        assert!(text.contains("⏱️ Время выполнения: 250ms"));
        assert!(text.contains("🗑️ Все ключи удалены из базы данных Redis"));
        assert!(text.contains("• Кеш промптов для изображений"));
        assert!(text.contains("• Временные данные приложения"));
    }

    #[test]
    fn admin_help_catalog_and_text_match_go_shape() {
        assert_eq!(admin_help_filter_category("  Справка ignored"), "Справка");
        assert_eq!(admin_help_filter_category("   \t "), "");

        let categories = admin_command_categories("справка");
        assert_eq!(categories.len(), 1);
        assert_eq!(categories[0].name, "Справка");
        assert_eq!(categories[0].commands[0].name, ADMIN_HELP_COMMAND);

        let all = admin_command_categories("");
        let text = admin_help_text(&all, "");
        assert!(text.starts_with("<b>📋 Команды администратора</b>"));
        assert!(text.contains("<b>Управление чатами:</b>"));
        assert!(text.contains("• <code>/admin_help [категория]</code>"));
        assert!(text.contains("Показать список доступных команд администратора"));
        assert!(text.contains("для фильтрации команд по категории"));

        let missing = admin_category_not_found_text("missing", &admin_available_categories());
        assert!(missing.contains("Категория \"missing\" не найдена"));
        assert!(missing.contains("• Управление чатами"));
        assert!(missing.contains("• Runtime API"));
    }

    #[test]
    fn admin_command_catalog_matches_expected_catalog() {
        let got: Vec<_> = ADMIN_COMMAND_DEFINITIONS
            .iter()
            .map(|command| {
                (
                    command.name,
                    command.description,
                    command.usage,
                    command.category,
                )
            })
            .collect();

        assert_eq!(
            got,
            vec![
                (
                    "admin_enable_chat",
                    "Включить возможности бота для указанного чата",
                    "/admin_enable_chat [chat_id или @channel_username]",
                    "Управление чатами",
                ),
                (
                    "admin_chat_settings",
                    "Получить кнопку настроек чата по ID или username",
                    "/admin_chat_settings [chat_id или @channel_username]",
                    "Управление чатами",
                ),
                (
                    "admin_queue_status",
                    "Показать статус очередей задач",
                    "/admin_queue_status",
                    "Мониторинг очередей",
                ),
                (
                    "admin_queue_stats",
                    "Показать детальную статистику очередей и заданий",
                    "/admin_queue_stats",
                    "Мониторинг очередей",
                ),
                (
                    "admin_refund",
                    "Возврат платежа по ID транзакции",
                    "/admin_refund [telegram_payment_charge_id]",
                    "Управление платежами",
                ),
                (
                    "admin_grant_vip",
                    "Предоставить VIP-статус пользователю без оплаты",
                    "/admin_grant_vip [дни=1] [user_id/@username/https://t.me/username] или ответ на сообщение",
                    "Управление подписками",
                ),
                (
                    "admin_cancel_vip",
                    "Отозвать VIP-статус у пользователя",
                    "/admin_cancel_vip [user_id или @username]",
                    "Управление подписками",
                ),
                (
                    "admin_help",
                    "Показать список доступных команд администратора",
                    "/admin_help [категория]",
                    "Справка",
                ),
                (
                    "admin_clear_cache",
                    "Очистить весь кеш Redis (промпты, системные инструкции, временные данные)",
                    "/admin_clear_cache",
                    "Системное администрирование",
                ),
                (
                    "admin_clear_gemini_cache",
                    "Удалить удаленные explicit-кеши Gemini, созданные Plotva",
                    "/admin_clear_gemini_cache",
                    "Системное администрирование",
                ),
                (
                    "admin_runtime_token",
                    "Выпустить новый bearer token для runtime debug API",
                    "/admin_runtime_token",
                    "Runtime API",
                ),
                (
                    "admin_runtime_token_list",
                    "Показать активные bearer tokens для runtime debug API",
                    "/admin_runtime_token_list",
                    "Runtime API",
                ),
            ]
        );
    }

    #[tokio::test]
    async fn admin_help_authorized_sends_html_and_filters_category()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let update = sample_command_update("/admin_help Справка ignored", 5, private_chat(), None)?;

        let outcome = handle_admin_help_command_update_or_else_at(
            AdminHelpRuntime {
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(
            outcome,
            AdminHelpCommandOutcome::Helped {
                category: Some("Справка".to_owned())
            }
        );
        let plans = effects.plans();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert!(plans[0].message.text.contains("<b>Справка:</b>"));
        assert!(plans[0].message.text.contains("/admin_help [категория]"));
        assert!(!plans[0].message.text.contains("/admin_clear_cache"));
        assert_eq!(plans[0].ephemeral_delete_after, None);
        Ok(())
    }

    #[tokio::test]
    async fn admin_help_unknown_and_auth_group_rules_match_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let unauthorized = sample_command_update("/admin_help", 9, private_chat(), None)?;
        let outcome = handle_admin_help_command_update_or_else_at(
            AdminHelpRuntime {
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            unauthorized,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminHelpCommandOutcome::Unauthorized);
        assert_eq!(
            effects.plans()[0].message.text,
            ADMIN_COMMAND_UNAUTHORIZED_TEXT
        );
        assert_eq!(
            effects.plans()[0].ephemeral_delete_after,
            Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER)
        );

        let unknown = sample_command_update("/admin_help missing", 5, private_chat(), None)?;
        let outcome = handle_admin_help_command_update_or_else_at(
            AdminHelpRuntime {
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            unknown,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(
            outcome,
            AdminHelpCommandOutcome::UnknownCategory {
                category: "missing".to_owned()
            }
        );
        assert!(
            effects.plans()[1]
                .message
                .text
                .contains("Категория \"missing\" не найдена")
        );

        let untargeted_group = sample_command_update("/admin_help", 5, group_chat(), Some(7))?;
        let outcome = handle_admin_help_command_update_or_else_at(
            AdminHelpRuntime {
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            untargeted_group,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminHelpCommandOutcome::Delegated);

        let targeted_group =
            sample_command_update("/admin_help@PlotvaBot missing", 5, group_chat(), Some(7))?;
        let outcome = handle_admin_help_command_update_or_else_at(
            AdminHelpRuntime {
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            targeted_group,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(
            outcome,
            AdminHelpCommandOutcome::UnknownCategory {
                category: "missing".to_owned()
            }
        );
        assert_eq!(effects.plans()[2].reply_to.message_thread_id, 7);
        Ok(())
    }

    #[tokio::test]
    async fn admin_queue_status_matches_go_html_shape() -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let taskman = TaskmanInspectorStub::new()
            .with_pending("text", 2)
            .with_processing("text", 1)
            .with_eta("text", 90);
        let update = sample_command_update("/admin_queue_status", 5, private_chat(), None)?;

        let outcome = handle_admin_queue_command_update_or_else_at(
            AdminQueueRuntime {
                config: &admin_queue_config(true),
                taskman: Some(&taskman),
                dispatcher: None,
                updates: None,
                dialog_debounce_len: None,
                media_gate_in_use: &zero_usize,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(outcome, AdminQueueCommandOutcome::Status);
        let plans = effects.plans();
        assert_eq!(plans[0].message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert!(
            plans[0]
                .message
                .text
                .starts_with("📊 <b>Статус очередей:</b>")
        );
        assert!(
            plans[0]
                .message
                .text
                .contains("🔄 <b>text:</b> 2 pending, 1 processing (1 мин. 30 сек.)")
        );
        assert!(
            plans[0]
                .message
                .text
                .contains("• Мониторинг очередей включен")
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_queue_stats_matches_go_sections_and_configured_totals()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let taskman = TaskmanInspectorStub::new()
            .with_pending("control", 1)
            .with_pending("text", 2)
            .with_pending("other", 100)
            .with_processing("text", 3)
            .with_processing("other", 100)
            .with_oldest_pending(
                "text",
                OffsetDateTime::now_utc() - TimeDuration::seconds(75),
            )
            .with_oldest_processing(
                "text",
                OffsetDateTime::now_utc() - TimeDuration::seconds(400),
            );
        let dispatcher = DispatcherInspectorStub {
            stats: RuntimeDispatcherStatsData {
                regular_queue_size: 4,
                immediate_queue_size: 1,
                processed_total: 9,
                deduped_total: 2,
                oldest_regular_age_ms: 61_000,
                oldest_immediate_age_ms: 0,
            },
        };
        let updates = UpdatesInspectorStub { queue_len: 12 };
        let update = sample_command_update("/admin_queue_stats", 5, private_chat(), None)?;

        let outcome = handle_admin_queue_command_update_or_else_at(
            AdminQueueRuntime {
                config: &admin_queue_config(true),
                taskman: Some(&taskman),
                dispatcher: Some(&dispatcher),
                updates: Some(&updates),
                dialog_debounce_len: Some(&three_usize),
                media_gate_in_use: &two_usize,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(outcome, AdminQueueCommandOutcome::Stats);
        let text = &effects.plans()[0].message.text;
        assert!(text.starts_with("📈 <b>Статистика заданий:</b>"));
        assert!(text.contains("📝 Pending: 3 | Processing: 3"));
        assert!(text.contains("• control: pending 1, processing 0"));
        assert!(text.contains("• text: pending 2, processing 3"));
        assert!(text.contains("• text: pending 1 мин. 15 сек., processing 6 мин. 40 сек. ⚠️ 오래"));
        assert!(text.contains("• regular: 4 (oldest 1 мин. 1 сек.)"));
        assert!(text.contains("• immediate: 1 (oldest —)"));
        assert!(text.contains("• updates_queue_len: 12"));
        assert!(text.contains("• media_gate_in_use: 2"));
        assert!(text.contains("• dialog_debounce_len: 3"));
        assert!(text.contains("• control: 2 воркеров"));
        assert!(!text.contains("other: pending"));
        Ok(())
    }

    #[tokio::test]
    async fn admin_queue_auth_disabled_and_group_target_rules_match_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let taskman = TaskmanInspectorStub::new();
        let unauthorized = sample_command_update("/admin_queue_status", 9, private_chat(), None)?;
        let outcome = handle_admin_queue_command_update_or_else_at(
            AdminQueueRuntime {
                config: &admin_queue_config(true),
                taskman: Some(&taskman),
                dispatcher: None,
                updates: None,
                dialog_debounce_len: None,
                media_gate_in_use: &zero_usize,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            unauthorized,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminQueueCommandOutcome::Unauthorized);
        assert_eq!(
            effects.plans()[0].message.text,
            ADMIN_COMMAND_UNAUTHORIZED_TEXT
        );

        let disabled = sample_command_update("/admin_queue_stats", 5, private_chat(), None)?;
        let outcome = handle_admin_queue_command_update_or_else_at(
            AdminQueueRuntime {
                config: &admin_queue_config(false),
                taskman: Some(&taskman),
                dispatcher: None,
                updates: None,
                dialog_debounce_len: None,
                media_gate_in_use: &zero_usize,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            disabled,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminQueueCommandOutcome::Disabled);
        assert_eq!(effects.plans()[1].message.text, admin_queue_disabled_text());
        assert_eq!(effects.plans()[1].message.render_as, "");

        let untargeted_group =
            sample_command_update("/admin_queue_status", 5, group_chat(), Some(7))?;
        let outcome = handle_admin_queue_command_update_or_else_at(
            AdminQueueRuntime {
                config: &admin_queue_config(true),
                taskman: Some(&taskman),
                dispatcher: None,
                updates: None,
                dialog_debounce_len: None,
                media_gate_in_use: &zero_usize,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            untargeted_group,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminQueueCommandOutcome::Delegated);

        let targeted_group =
            sample_command_update("/admin_queue_status@PlotvaBot", 5, group_chat(), Some(7))?;
        let outcome = handle_admin_queue_command_update_or_else_at(
            AdminQueueRuntime {
                config: &admin_queue_config(true),
                taskman: Some(&taskman),
                dispatcher: None,
                updates: None,
                dialog_debounce_len: None,
                media_gate_in_use: &zero_usize,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            targeted_group,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminQueueCommandOutcome::Status);
        assert_eq!(effects.plans()[2].reply_to.message_thread_id, 7);
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_admin_help_and_queue_status_commands_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-admin:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&sample_command_update(
                "/admin_help Справка",
                5,
                private_chat(),
                None,
            )?)
            .await?;
        update_queue
            .enqueue_update(&sample_command_update(
                "/admin_queue_status@PlotvaBot",
                5,
                group_chat(),
                Some(7),
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 2);

        let effects = Arc::new(EffectsStub::default());
        let terminal = Arc::new(TerminalStub::default());
        let help = Arc::new(AdminHelpCommandUpdateHandler::new(
            Arc::clone(&effects),
            vec![5],
            "PlotvaBot",
            Arc::clone(&terminal),
        ));
        let queue = Arc::new(AdminQueueCommandUpdateHandler::new(
            AdminQueueRuntimeConfig::new(admin_queue_config(true)).with_taskman(Arc::new(
                TaskmanInspectorStub::new()
                    .with_pending(openplotva_taskman::TEXT_QUEUE_NAME, 2)
                    .with_processing(openplotva_taskman::TEXT_QUEUE_NAME, 1)
                    .with_eta(openplotva_taskman::TEXT_QUEUE_NAME, 90),
            )),
            Arc::clone(&effects),
            vec![5],
            "PlotvaBot",
            help,
        ));
        let state_store = UpdateStateStoreStub::default();
        let config = UpdateConsumerConfig {
            dequeue_timeout: StdDuration::from_millis(1),
            state_timeout: StdDuration::from_secs(1),
            handle_timeout: StdDuration::from_secs(1),
            side_effect_max_age: StdDuration::from_secs(60),
            worker_limit: 1,
        };
        let now = UNIX_EPOCH + StdDuration::from_secs(1_710_000_010);

        let help_update = update_queue
            .dequeue_update(StdDuration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected admin_help update"))?;
        let help_report =
            process_update_with_state_store_at(help_update, config, now, &state_store, |update| {
                queue.handle_update(update)
            })
            .await;
        assert_eq!(help_report.update_name, "message");
        assert_eq!(help_report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            help_report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );

        let queue_update = update_queue
            .dequeue_update(StdDuration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected admin_queue_status update"))?;
        let queue_report =
            process_update_with_state_store_at(queue_update, config, now, &state_store, |update| {
                queue.handle_update(update)
            })
            .await;
        assert_eq!(queue_report.update_name, "message");
        assert_eq!(queue_report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            queue_report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );

        let plans = effects.plans();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert!(plans[0].message.text.contains("<b>Справка:</b>"));
        assert!(plans[0].message.text.contains("/admin_help [категория]"));
        assert_eq!(plans[0].reply_to.message_id, 10);
        assert_eq!(plans[0].ephemeral_delete_after, None);
        assert_eq!(plans[1].message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert!(
            plans[1]
                .message
                .text
                .starts_with("📊 <b>Статус очередей:</b>")
        );
        assert!(
            plans[1]
                .message
                .text
                .contains("🔄 <b>text:</b> 2 pending, 1 processing (1 мин. 30 сек.)")
        );
        assert_eq!(plans[1].reply_to.message_thread_id, 7);
        assert_eq!(terminal.calls(), 0);
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private".to_owned(),
                "user:5:Ada".to_owned(),
                "chat:-42:supergroup".to_owned(),
                "user:5:Ada".to_owned(),
            ]
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_admin_runtime_and_cache_commands_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-admin-cache:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&sample_command_update(
                "/admin_runtime_token",
                5,
                private_chat(),
                None,
            )?)
            .await?;
        update_queue
            .enqueue_update(&sample_command_update(
                "/admin_clear_cache",
                5,
                private_chat(),
                None,
            )?)
            .await?;
        update_queue
            .enqueue_update(&sample_command_update(
                "/admin_clear_gemini_cache@PlotvaBot",
                5,
                group_chat(),
                Some(7),
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 3);

        let effects = Arc::new(EffectsStub::default());
        let terminal = Arc::new(TerminalStub::default());
        let purger = Arc::new(PurgerStub::ok(RuntimeGeminiCachePurgeResultData {
            scanned: 3,
            matched: 2,
            deleted: 1,
            failed: 0,
        }));
        let flusher = Arc::new(RedisFlusherStub::ok());
        let manager = Arc::new(RuntimeTokenManagerStub::new().with_issue(issued_token("abc")));
        let gemini = Arc::new(AdminGeminiCacheCommandUpdateHandler::new(
            Some(Arc::clone(&purger)),
            Arc::clone(&effects),
            vec![5],
            "PlotvaBot",
            Arc::clone(&terminal),
        ));
        let redis_cache = Arc::new(AdminRedisCacheCommandUpdateHandler::new(
            Arc::clone(&flusher),
            Arc::clone(&effects),
            vec![5],
            "PlotvaBot",
            gemini,
        ));
        let runtime = Arc::new(AdminRuntimeTokenCommandUpdateHandler::new(
            Some(Arc::clone(&manager)),
            Arc::clone(&effects),
            vec![5],
            "PlotvaBot",
            "sha256//pin",
            redis_cache,
        ));
        let state_store = UpdateStateStoreStub::default();
        let config = UpdateConsumerConfig {
            dequeue_timeout: StdDuration::from_millis(1),
            state_timeout: StdDuration::from_secs(1),
            handle_timeout: StdDuration::from_secs(1),
            side_effect_max_age: StdDuration::from_secs(60),
            worker_limit: 1,
        };
        let now = UNIX_EPOCH + StdDuration::from_secs(1_710_000_010);

        for expected in [
            "expected runtime-token update",
            "expected redis-cache update",
            "expected gemini-cache update",
        ] {
            let update = update_queue
                .dequeue_update(StdDuration::from_secs(1))
                .await?
                .ok_or_else(|| io::Error::other(expected))?;
            let report =
                process_update_with_state_store_at(update, config, now, &state_store, |update| {
                    runtime.handle_update(update)
                })
                .await;
            assert_eq!(report.update_name, "message");
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed)
            );
        }

        let plans = effects.plans();
        assert_eq!(plans.len(), 3);
        assert!(plans[0].message.text.contains("Runtime API token выпущен"));
        assert!(
            plans[0]
                .message
                .text
                .contains("<code>prt_abc.secret</code>")
        );
        assert_eq!(
            plans[0].ephemeral_delete_after,
            Some(ADMIN_RUNTIME_TOKEN_DELETE_AFTER)
        );
        assert!(
            plans[1]
                .message
                .text
                .starts_with("✅ Кеш Redis успешно очищен!")
        );
        assert_eq!(plans[1].ephemeral_delete_after, None);
        assert!(
            plans[2]
                .message
                .text
                .starts_with("✅ Gemini explicit cache обработан.")
        );
        assert!(
            plans[2]
                .message
                .text
                .contains("🔎 Проверено кэшей на API: 3")
        );
        assert_eq!(plans[2].reply_to.message_thread_id, 7);
        assert_eq!(manager.issue_calls(), 1);
        assert_eq!(manager.list_calls(), 0);
        assert_eq!(flusher.calls(), 1);
        assert_eq!(purger.calls(), 1);
        assert_eq!(terminal.calls(), 0);
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private".to_owned(),
                "user:5:Ada".to_owned(),
                "chat:42:private".to_owned(),
                "user:5:Ada".to_owned(),
                "chat:-42:supergroup".to_owned(),
                "user:5:Ada".to_owned(),
            ]
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_admin_settings_and_enable_chat_commands_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-admin-utility:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&sample_command_update(
                "/admin_settings",
                5,
                private_chat(),
                None,
            )?)
            .await?;
        update_queue
            .enqueue_update(&sample_command_update(
                "/admin_enable_chat@PlotvaBot @target_lab",
                5,
                group_chat(),
                Some(7),
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 2);

        let effects = Arc::new(EffectsStub::default());
        let resolver = Arc::new(AdminChatTargetResolverStub::with_target(
            AdminChatSettingsTarget {
                id: -100777,
                title: "Target Lab".to_owned(),
                username: "target_lab".to_owned(),
                first_name: String::new(),
                last_name: String::new(),
            },
        ));
        let communication = Arc::new(ChatCommunicationEffectsStub::default());
        let terminal = Arc::new(TerminalStub::default());
        let enable_chat = Arc::new(AdminEnableChatCommandUpdateHandler::new(
            Arc::clone(&resolver),
            Arc::clone(&communication),
            Arc::clone(&effects),
            vec![5],
            "PlotvaBot",
            Arc::clone(&terminal),
        ));
        let settings = Arc::new(AdminSettingsCommandUpdateHandler::new(
            Arc::clone(&effects),
            vec![5],
            "PlotvaBot",
            "https://plotva.example/base",
            enable_chat,
        ));
        let state_store = UpdateStateStoreStub::default();
        let config = UpdateConsumerConfig {
            dequeue_timeout: StdDuration::from_millis(1),
            state_timeout: StdDuration::from_secs(1),
            handle_timeout: StdDuration::from_secs(1),
            side_effect_max_age: StdDuration::from_secs(60),
            worker_limit: 1,
        };
        let now = UNIX_EPOCH + StdDuration::from_secs(1_710_000_010);

        for expected in [
            "expected admin_settings update",
            "expected admin_enable_chat update",
        ] {
            let update = update_queue
                .dequeue_update(StdDuration::from_secs(1))
                .await?
                .ok_or_else(|| io::Error::other(expected))?;
            let report =
                process_update_with_state_store_at(update, config, now, &state_store, |update| {
                    settings.handle_update(update)
                })
                .await;
            assert_eq!(report.update_name, "message");
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed)
            );
        }

        let plans = effects.plans();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].message.text, ADMIN_SETTINGS_TEXT);
        assert_eq!(plans[0].reply_to.message_id, 10);
        assert_eq!(plans[0].ephemeral_delete_after, None);
        let markup = serde_json::to_value(plans[0].message.reply_markup.as_ref())?;
        assert_eq!(
            markup["inline_keyboard"][0][0]["text"],
            ADMIN_SETTINGS_BUTTON_TEXT
        );
        assert_eq!(
            markup["inline_keyboard"][0][0]["url"],
            "https://plotva.example/base/admin/"
        );
        assert_eq!(
            plans[1].message.text,
            "Global text and draw replies have been ENABLED for chat 'Target Lab' (ID: -100777)."
        );
        assert_eq!(plans[1].reply_to.message_thread_id, 7);
        assert_eq!(plans[1].ephemeral_delete_after, None);
        assert_eq!(resolver.calls(), vec!["@target_lab".to_owned()]);
        assert_eq!(communication.enabled(), vec![-100777]);
        assert_eq!(terminal.calls(), 0);
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private".to_owned(),
                "user:5:Ada".to_owned(),
                "chat:-42:supergroup".to_owned(),
                "user:5:Ada".to_owned(),
            ]
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn admin_redis_cache_private_authorized_flushes_and_sends_report()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let flusher = RedisFlusherStub::ok();
        let update = sample_command_update("/admin_clear_cache", 5, private_chat(), None)?;

        let outcome = handle_admin_redis_cache_command_update_or_else_at(
            AdminRedisCacheRuntime {
                flusher: &flusher,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(outcome, AdminRedisCacheCommandOutcome::Cleared);
        assert_eq!(flusher.calls(), 1);
        let plans = effects.plans();
        assert_eq!(plans.len(), 1);
        assert!(
            plans[0]
                .message
                .text
                .starts_with("✅ Кеш Redis успешно очищен!")
        );
        assert_eq!(plans[0].reply_to.message_id, 10);
        assert_eq!(plans[0].ephemeral_delete_after, None);
        Ok(())
    }

    #[tokio::test]
    async fn admin_redis_cache_unauthorized_and_group_target_rules_match_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let flusher = RedisFlusherStub::ok();
        let unauthorized = sample_command_update("/admin_clear_cache", 9, private_chat(), None)?;

        let outcome = handle_admin_redis_cache_command_update_or_else_at(
            AdminRedisCacheRuntime {
                flusher: &flusher,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            unauthorized,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(outcome, AdminRedisCacheCommandOutcome::Unauthorized);
        assert_eq!(
            effects.plans()[0].message.text,
            ADMIN_COMMAND_UNAUTHORIZED_TEXT
        );
        assert_eq!(flusher.calls(), 0);

        let untargeted_group =
            sample_command_update("/admin_clear_cache", 5, group_chat(), Some(7))?;
        let outcome = handle_admin_redis_cache_command_update_or_else_at(
            AdminRedisCacheRuntime {
                flusher: &flusher,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            untargeted_group,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminRedisCacheCommandOutcome::Delegated);

        let targeted_group =
            sample_command_update("/admin_clear_cache@PlotvaBot", 5, group_chat(), Some(7))?;
        let outcome = handle_admin_redis_cache_command_update_or_else_at(
            AdminRedisCacheRuntime {
                flusher: &flusher,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            targeted_group,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminRedisCacheCommandOutcome::Cleared);
        assert_eq!(effects.plans()[1].reply_to.message_thread_id, 7);
        Ok(())
    }

    #[tokio::test]
    async fn admin_redis_cache_reports_flush_error() -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let flusher = RedisFlusherStub::err("redis boom");
        let update = sample_command_update("/admin_clear_cache", 5, private_chat(), None)?;

        let outcome = handle_admin_redis_cache_command_update_or_else_at(
            AdminRedisCacheRuntime {
                flusher: &flusher,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(
            outcome,
            AdminRedisCacheCommandOutcome::FlushError {
                message: "redis boom".to_owned()
            }
        );
        assert_eq!(
            effects.plans()[0].message.text,
            "❌ Ошибка очистки кеша Redis:\nredis boom"
        );
        Ok(())
    }

    #[test]
    fn admin_runtime_token_texts_match_go_html_shape() {
        let created = OffsetDateTime::parse("2026-05-21T10:00:00Z", &Rfc3339).expect("created");
        let issued = IssuedRuntimeToken {
            token: "prt_abc.secret".to_owned(),
            meta: RuntimeToken {
                id: "abc".to_owned(),
                created_at: Some(created),
            },
        };

        let issue_text =
            admin_runtime_token_issued_text(&issued, TimeDuration::hours(24), "sha256//pin");
        assert!(issue_text.starts_with("🔐 <b>Runtime API token выпущен</b>"));
        assert!(issue_text.contains("<code>prt_abc.secret</code>"));
        assert!(issue_text.contains("<b>ID:</b> <code>abc</code>"));
        assert!(issue_text.contains("<b>Создан:</b> <code>2026-05-21T10:00:00Z</code>"));
        assert!(issue_text.contains("<b>Истекает:</b> <code>2026-05-22T10:00:00Z</code>"));
        assert!(issue_text.contains("<code>Authorization: Bearer &lt;token&gt;</code>"));
        assert!(issue_text.contains("<code>sha256//pin</code>"));

        let list_text = admin_runtime_token_list_text(
            &[RuntimeToken {
                id: "abc".to_owned(),
                created_at: Some(created),
            }],
            TimeDuration::hours(24),
        );
        assert_eq!(
            list_text,
            "🔐 <b>Активные runtime API токены</b>\n\n• <code>abc</code> — created <code>2026-05-21T10:00:00Z</code>, expires <code>2026-05-22T10:00:00Z</code>"
        );
        assert_eq!(
            admin_private_chat_required_text("/admin_runtime_token"),
            "🔒 Эту команду нужно выполнять в личном чате с ботом.\n\nИспользование: /admin_runtime_token"
        );
    }

    #[tokio::test]
    async fn admin_runtime_token_private_authorized_issues_ephemeral_html()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let manager = RuntimeTokenManagerStub::new().with_issue(issued_token("abc"));
        let update = sample_command_update("/admin_runtime_token", 5, private_chat(), None)?;

        let outcome = handle_admin_runtime_token_command_update_or_else_at(
            AdminRuntimeTokenRuntime {
                manager: Some(&manager),
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
                tls_public_key_pin: "sha256//pin",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(
            outcome,
            AdminRuntimeTokenCommandOutcome::Issued {
                id: "abc".to_owned()
            }
        );
        assert_eq!(manager.issue_calls(), 1);
        let plans = effects.plans();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(
            plans[0].ephemeral_delete_after,
            Some(ADMIN_RUNTIME_TOKEN_DELETE_AFTER)
        );
        assert!(plans[0].message.text.contains("prt_abc.secret"));
        assert!(plans[0].message.text.contains("sha256//pin"));
        Ok(())
    }

    #[tokio::test]
    async fn admin_runtime_token_list_empty_and_nonempty_match_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let manager = RuntimeTokenManagerStub::new().with_list(vec![runtime_token("abc")]);
        let update = sample_command_update("/admin_runtime_token_list", 5, private_chat(), None)?;

        let outcome = handle_admin_runtime_token_command_update_or_else_at(
            AdminRuntimeTokenRuntime {
                manager: Some(&manager),
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
                tls_public_key_pin: "sha256//pin",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(
            outcome,
            AdminRuntimeTokenCommandOutcome::Listed { count: 1 }
        );
        assert_eq!(manager.list_calls(), 1);
        assert_eq!(
            effects.plans()[0].message.render_as,
            TELEGRAM_PARSE_MODE_HTML
        );
        assert!(
            effects.plans()[0]
                .message
                .text
                .contains("Активные runtime API токены")
        );

        let empty_effects = EffectsStub::default();
        let empty_manager = RuntimeTokenManagerStub::new().with_list(Vec::new());
        let update = sample_command_update("/admin_runtime_token_list", 5, private_chat(), None)?;
        let outcome = handle_admin_runtime_token_command_update_or_else_at(
            AdminRuntimeTokenRuntime {
                manager: Some(&empty_manager),
                effects: &empty_effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
                tls_public_key_pin: "sha256//pin",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminRuntimeTokenCommandOutcome::EmptyList);
        assert_eq!(
            empty_effects.plans()[0].message.text,
            "ℹ️ Активных runtime API токенов нет."
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_runtime_token_auth_private_and_unavailable_gates_match_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let manager = RuntimeTokenManagerStub::new().with_issue(issued_token("abc"));
        let unauthorized = sample_command_update("/admin_runtime_token", 9, private_chat(), None)?;
        let outcome = handle_admin_runtime_token_command_update_or_else_at(
            AdminRuntimeTokenRuntime {
                manager: Some(&manager),
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
                tls_public_key_pin: "sha256//pin",
            },
            unauthorized,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(outcome, AdminRuntimeTokenCommandOutcome::Unauthorized);
        assert_eq!(
            effects.plans()[0].message.text,
            ADMIN_COMMAND_UNAUTHORIZED_TEXT
        );
        assert_eq!(manager.issue_calls(), 0);

        let group =
            sample_command_update("/admin_runtime_token@PlotvaBot", 5, group_chat(), Some(7))?;
        let outcome = handle_admin_runtime_token_command_update_or_else_at(
            AdminRuntimeTokenRuntime {
                manager: Some(&manager),
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
                tls_public_key_pin: "sha256//pin",
            },
            group,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(
            outcome,
            AdminRuntimeTokenCommandOutcome::PrivateChatRequired
        );
        assert_eq!(
            effects.plans()[1].ephemeral_delete_after,
            Some(ADMIN_PRIVATE_COMMAND_DELETE_AFTER)
        );
        assert_eq!(effects.plans()[1].reply_to.message_thread_id, 7);
        assert!(
            effects.plans()[1]
                .message
                .text
                .contains("/admin_runtime_token")
        );

        let unavailable_effects = EffectsStub::default();
        let update = sample_command_update("/admin_runtime_token", 5, private_chat(), None)?;
        let outcome = handle_admin_runtime_token_command_update_or_else_at(
            AdminRuntimeTokenRuntime {
                manager: None::<&RuntimeTokenManagerStub>,
                effects: &unavailable_effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
                tls_public_key_pin: "sha256//pin",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(
            outcome,
            AdminRuntimeTokenCommandOutcome::RuntimeApiUnavailable
        );
        assert_eq!(
            unavailable_effects.plans()[0].message.text,
            RUNTIME_API_UNAVAILABLE_TEXT
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_gemini_cache_private_authorized_runs_purger_and_sends_report()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let purger = PurgerStub::ok(RuntimeGeminiCachePurgeResultData {
            scanned: 2,
            matched: 1,
            deleted: 1,
            failed: 0,
        });
        let update = sample_command_update("/admin_clear_gemini_cache", 5, private_chat(), None)?;

        let outcome = handle_admin_gemini_cache_command_update_or_else_at(
            AdminGeminiCacheRuntime {
                purger: Some(&purger),
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(
            outcome,
            AdminGeminiCacheCommandOutcome::Purged(RuntimeGeminiCachePurgeResultData {
                scanned: 2,
                matched: 1,
                deleted: 1,
                failed: 0,
            })
        );
        assert_eq!(purger.calls(), 1);
        let plans = effects.plans();
        assert_eq!(plans.len(), 1);
        assert!(
            plans[0]
                .message
                .text
                .starts_with("✅ Gemini explicit cache обработан.")
        );
        assert_eq!(plans[0].reply_to.message_id, 10);
        assert_eq!(plans[0].ephemeral_delete_after, None);
        Ok(())
    }

    #[tokio::test]
    async fn admin_gemini_cache_unauthorized_uses_go_ephemeral_denial()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let update = sample_command_update("/admin_clear_gemini_cache", 9, private_chat(), None)?;

        let outcome = handle_admin_gemini_cache_command_update_or_else_at(
            AdminGeminiCacheRuntime {
                purger: None::<&PurgerStub>,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(outcome, AdminGeminiCacheCommandOutcome::Unauthorized);
        let plans = effects.plans();
        assert_eq!(plans[0].message.text, ADMIN_COMMAND_UNAUTHORIZED_TEXT);
        assert_eq!(
            plans[0].ephemeral_delete_after,
            Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER)
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_gemini_cache_requires_group_bot_target() -> Result<(), Box<dyn std::error::Error>>
    {
        let effects = EffectsStub::default();
        let update = sample_command_update("/admin_clear_gemini_cache", 5, group_chat(), Some(7))?;

        let outcome = handle_admin_gemini_cache_command_update_or_else_at(
            AdminGeminiCacheRuntime {
                purger: None::<&PurgerStub>,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(outcome, AdminGeminiCacheCommandOutcome::Delegated);
        assert!(effects.plans().is_empty());

        let targeted = sample_command_update(
            "/admin_clear_gemini_cache@PlotvaBot",
            5,
            group_chat(),
            Some(7),
        )?;
        let outcome = handle_admin_gemini_cache_command_update_or_else_at(
            AdminGeminiCacheRuntime {
                purger: None::<&PurgerStub>,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            targeted,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;

        assert_eq!(outcome, AdminGeminiCacheCommandOutcome::Unconfigured);
        assert_eq!(effects.plans()[0].reply_to.message_thread_id, 7);
        Ok(())
    }

    #[tokio::test]
    async fn admin_gemini_cache_reports_unconfigured_and_purge_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let update = sample_command_update("/admin_clear_gemini_cache", 5, private_chat(), None)?;
        let unconfigured = handle_admin_gemini_cache_command_update_or_else_at(
            AdminGeminiCacheRuntime {
                purger: None::<&PurgerStub>,
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(unconfigured, AdminGeminiCacheCommandOutcome::Unconfigured);
        assert_eq!(
            effects.plans()[0].message.text,
            ADMIN_GEMINI_CACHE_UNCONFIGURED_TEXT
        );

        let update = sample_command_update("/admin_clear_gemini_cache", 5, private_chat(), None)?;
        let error = handle_admin_gemini_cache_command_update_or_else_at(
            AdminGeminiCacheRuntime {
                purger: Some(&PurgerStub::err("api boom")),
                effects: &effects,
                admin_ids: &[5],
                bot_username: "PlotvaBot",
            },
            update,
            |_| async { Ok::<(), Infallible>(()) },
        )
        .await?;
        assert_eq!(
            error,
            AdminGeminiCacheCommandOutcome::PurgeError {
                message: "api boom".to_owned()
            }
        );
        assert_eq!(
            effects.plans()[1].message.text,
            "❌ Ошибка очистки Gemini explicit cache:\napi boom"
        );
        Ok(())
    }

    fn zero_usize() -> usize {
        0
    }

    fn two_usize() -> usize {
        2
    }

    fn three_usize() -> usize {
        3
    }

    fn admin_queue_config(enabled: bool) -> AdminQueueCommandConfig {
        AdminQueueCommandConfig {
            persistent_queue_enabled: enabled,
            default_processing_timeout: Duration::from_secs(300),
            workers: vec![
                AdminQueueWorkerConfig {
                    queue_name: openplotva_taskman::CONTROL_QUEUE_NAME.to_owned(),
                    worker_count: 2,
                },
                AdminQueueWorkerConfig {
                    queue_name: openplotva_taskman::TEXT_QUEUE_NAME.to_owned(),
                    worker_count: 4,
                },
                AdminQueueWorkerConfig {
                    queue_name: openplotva_taskman::DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                    worker_count: 0,
                },
            ],
        }
    }

    #[derive(Clone, Default)]
    struct TaskmanInspectorStub {
        pending: BTreeMap<String, i32>,
        processing: BTreeMap<String, i32>,
        eta: BTreeMap<String, i32>,
        oldest_pending: BTreeMap<String, OffsetDateTime>,
        oldest_processing: BTreeMap<String, OffsetDateTime>,
    }

    impl TaskmanInspectorStub {
        fn new() -> Self {
            Self::default()
        }

        fn with_pending(mut self, queue_name: &str, count: i32) -> Self {
            self.pending.insert(queue_name.to_owned(), count);
            self
        }

        fn with_processing(mut self, queue_name: &str, count: i32) -> Self {
            self.processing.insert(queue_name.to_owned(), count);
            self
        }

        fn with_eta(mut self, queue_name: &str, seconds: i32) -> Self {
            self.eta.insert(queue_name.to_owned(), seconds);
            self
        }

        fn with_oldest_pending(mut self, queue_name: &str, created_at: OffsetDateTime) -> Self {
            self.oldest_pending
                .insert(queue_name.to_owned(), created_at);
            self
        }

        fn with_oldest_processing(mut self, queue_name: &str, started_at: OffsetDateTime) -> Self {
            self.oldest_processing
                .insert(queue_name.to_owned(), started_at);
            self
        }
    }

    impl RuntimeTaskmanInspector for TaskmanInspectorStub {
        fn list_jobs(
            &self,
            filter: RuntimeTaskmanJobsFilter,
        ) -> Result<RuntimeTaskmanJobListResultData, String> {
            let status = filter.status.first().map(String::as_str).unwrap_or("");
            let by_queue = match status {
                "pending" => self.pending.clone(),
                "processing" => self.processing.clone(),
                _ => BTreeMap::new(),
            };
            let items = if let Some(queue_name) = filter.queue.first() {
                match status {
                    "pending" => self
                        .oldest_pending
                        .get(queue_name)
                        .map(|created_at| taskman_entry(queue_name, status, *created_at, None))
                        .into_iter()
                        .collect(),
                    "processing" => self
                        .oldest_processing
                        .get(queue_name)
                        .map(|started_at| {
                            taskman_entry(queue_name, status, *started_at, Some(*started_at))
                        })
                        .into_iter()
                        .collect(),
                    _ => Vec::new(),
                }
            } else {
                Vec::new()
            };
            Ok(RuntimeTaskmanJobListResultData {
                total: items.len() as i32,
                offset: 0,
                limit: filter.limit,
                summary: RuntimeTaskmanJobSummaryData {
                    by_status: json!({status: by_queue.values().sum::<i32>()}),
                    by_queue: json!(by_queue),
                },
                items,
            })
        }

        fn job<'a>(&'a self, _id: i64) -> openplotva_server::RuntimeTaskmanJobFuture<'a> {
            Box::pin(async { Ok(None) })
        }

        fn queue_diagnostics(
            &self,
            queues: Vec<String>,
            priority: i32,
        ) -> Result<RuntimeTaskmanDiagnosticsData, String> {
            Ok(RuntimeTaskmanDiagnosticsData {
                running: true,
                queues: queues
                    .into_iter()
                    .map(|queue_name| RuntimeTaskmanQueueDiagnosticsData {
                        queue_name: queue_name.clone(),
                        priority,
                        pending: 0,
                        pending_or_higher: 0,
                        active: 0,
                        worker_count: 0,
                        eta_seconds: self.eta.get(&queue_name).copied().unwrap_or_default(),
                    })
                    .collect(),
                ..RuntimeTaskmanDiagnosticsData::default()
            })
        }
    }

    fn taskman_entry(
        queue_name: &str,
        status: &str,
        created_at: OffsetDateTime,
        started_at: Option<OffsetDateTime>,
    ) -> openplotva_server::RuntimeTaskmanJobListEntryData {
        let timestamp = created_at
            .format(&Rfc3339)
            .expect("test timestamp should format");
        openplotva_server::RuntimeTaskmanJobListEntryData {
            id: 1,
            queue_name: queue_name.to_owned(),
            priority: 0,
            title: "job".to_owned(),
            job_type: "dialog".to_owned(),
            status: status.to_owned(),
            user_id: 7,
            chat_id: 42,
            trigger_message_id: 10,
            thread_message_id: None,
            progress_message_id: None,
            queue_position_message_id: None,
            result_message_id: None,
            worker_id: None,
            created_at: timestamp.clone(),
            started_at: started_at.map(|value| {
                value
                    .format(&Rfc3339)
                    .expect("test timestamp should format")
            }),
            completed_at: None,
            error_message: None,
            processing_timeout_seconds: 300,
            prompt_hash: None,
            estimated_processing_time: None,
            actual_processing_time: None,
            preview: None,
        }
    }

    struct DispatcherInspectorStub {
        stats: RuntimeDispatcherStatsData,
    }

    impl RuntimeDispatcherInspector for DispatcherInspectorStub {
        fn stats(&self) -> RuntimeDispatcherStatsData {
            self.stats.clone()
        }
    }

    struct UpdatesInspectorStub {
        queue_len: i32,
    }

    impl RuntimeUpdatesInspector for UpdatesInspectorStub {
        fn snapshot<'a>(&'a self) -> RuntimeUpdatesInspectorFuture<'a> {
            Box::pin(async move {
                Ok(RuntimeUpdatesRuntimeData {
                    queue_len: self.queue_len,
                    ..RuntimeUpdatesRuntimeData::default()
                })
            })
        }
    }

    #[derive(Clone)]
    struct PurgerStub {
        state: Arc<Mutex<PurgerState>>,
    }

    struct PurgerState {
        result: Result<RuntimeGeminiCachePurgeResultData, String>,
        calls: usize,
    }

    impl PurgerStub {
        fn ok(result: RuntimeGeminiCachePurgeResultData) -> Self {
            Self {
                state: Arc::new(Mutex::new(PurgerState {
                    result: Ok(result),
                    calls: 0,
                })),
            }
        }

        fn err(message: &str) -> Self {
            Self {
                state: Arc::new(Mutex::new(PurgerState {
                    result: Err(message.to_owned()),
                    calls: 0,
                })),
            }
        }

        fn calls(&self) -> usize {
            self.lock().calls
        }

        fn lock(&self) -> MutexGuard<'_, PurgerState> {
            self.state.lock().expect("purger state")
        }
    }

    impl RuntimeGeminiCachePurger for PurgerStub {
        fn purge<'a>(&'a self) -> RuntimeGeminiCachePurgerFuture<'a> {
            Box::pin(async move {
                let mut state = self.lock();
                state.calls += 1;
                state.result.clone()
            })
        }
    }

    fn runtime_token(id: &str) -> RuntimeToken {
        RuntimeToken {
            id: id.to_owned(),
            created_at: Some(
                OffsetDateTime::parse("2026-05-21T10:00:00Z", &Rfc3339).expect("created"),
            ),
        }
    }

    fn issued_token(id: &str) -> IssuedRuntimeToken {
        IssuedRuntimeToken {
            token: format!("prt_{id}.secret"),
            meta: runtime_token(id),
        }
    }

    #[derive(Clone)]
    struct RuntimeTokenManagerStub {
        state: Arc<Mutex<RuntimeTokenManagerState>>,
        ttl: TimeDuration,
    }

    struct RuntimeTokenManagerState {
        issue: Result<IssuedRuntimeToken, String>,
        list: Result<Vec<RuntimeToken>, String>,
        issue_calls: usize,
        list_calls: usize,
    }

    impl RuntimeTokenManagerStub {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(RuntimeTokenManagerState {
                    issue: Ok(issued_token("default")),
                    list: Ok(Vec::new()),
                    issue_calls: 0,
                    list_calls: 0,
                })),
                ttl: TimeDuration::hours(24),
            }
        }

        fn with_issue(self, issued: IssuedRuntimeToken) -> Self {
            self.lock().issue = Ok(issued);
            self
        }

        fn with_list(self, tokens: Vec<RuntimeToken>) -> Self {
            self.lock().list = Ok(tokens);
            self
        }

        fn issue_calls(&self) -> usize {
            self.lock().issue_calls
        }

        fn list_calls(&self) -> usize {
            self.lock().list_calls
        }

        fn lock(&self) -> MutexGuard<'_, RuntimeTokenManagerState> {
            self.state.lock().expect("runtime token manager state")
        }
    }

    impl AdminRuntimeTokenManager for RuntimeTokenManagerStub {
        fn issue_runtime_token<'a>(&'a self) -> AdminRuntimeTokenIssueFuture<'a> {
            Box::pin(async move {
                let mut state = self.lock();
                state.issue_calls += 1;
                state.issue.clone()
            })
        }

        fn list_runtime_tokens<'a>(&'a self) -> AdminRuntimeTokenListFuture<'a> {
            Box::pin(async move {
                let mut state = self.lock();
                state.list_calls += 1;
                state.list.clone()
            })
        }

        fn ttl(&self) -> TimeDuration {
            self.ttl
        }
    }

    #[derive(Clone)]
    struct RedisFlusherStub {
        state: Arc<Mutex<RedisFlusherState>>,
    }

    struct RedisFlusherState {
        result: Result<(), String>,
        calls: usize,
    }

    impl RedisFlusherStub {
        fn ok() -> Self {
            Self {
                state: Arc::new(Mutex::new(RedisFlusherState {
                    result: Ok(()),
                    calls: 0,
                })),
            }
        }

        fn err(message: &str) -> Self {
            Self {
                state: Arc::new(Mutex::new(RedisFlusherState {
                    result: Err(message.to_owned()),
                    calls: 0,
                })),
            }
        }

        fn calls(&self) -> usize {
            self.lock().calls
        }

        fn lock(&self) -> MutexGuard<'_, RedisFlusherState> {
            self.state.lock().expect("redis flusher state")
        }
    }

    impl AdminRedisCacheFlusher for RedisFlusherStub {
        fn flush_db<'a>(&'a self) -> AdminRedisFlushFuture<'a> {
            Box::pin(async move {
                let mut state = self.lock();
                state.calls += 1;
                state.result.clone()
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct TerminalStub {
        calls: Arc<Mutex<usize>>,
    }

    impl TerminalStub {
        fn calls(&self) -> usize {
            *self.calls.lock().expect("terminal calls")
        }
    }

    impl UpdateHandler for TerminalStub {
        type Error = Infallible;

        fn handle_update<'a>(
            &'a self,
            _update: TelegramUpdate,
        ) -> UpdateHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                *self.calls.lock().expect("terminal calls") += 1;
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug, Default)]
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
                    .push(format!("chat:{}:{}", chat.id, chat.chat_type));
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
                    .push(format!("user:{}:{}", user.id, user.first_name));
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

    #[derive(Clone, Default)]
    struct AdminChatTargetResolverStub {
        state: Arc<Mutex<AdminChatTargetResolverState>>,
    }

    #[derive(Default)]
    struct AdminChatTargetResolverState {
        target: Option<AdminChatSettingsTarget>,
        error: Option<String>,
        calls: Vec<String>,
    }

    impl AdminChatTargetResolverStub {
        fn with_target(target: AdminChatSettingsTarget) -> Self {
            Self {
                state: Arc::new(Mutex::new(AdminChatTargetResolverState {
                    target: Some(target),
                    error: None,
                    calls: Vec::new(),
                })),
            }
        }

        fn failing(error: &str) -> Self {
            Self {
                state: Arc::new(Mutex::new(AdminChatTargetResolverState {
                    target: None,
                    error: Some(error.to_owned()),
                    calls: Vec::new(),
                })),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.state.lock().expect("resolver").calls.clone()
        }
    }

    impl AdminChatTargetResolver for AdminChatTargetResolverStub {
        type Error = String;

        fn resolve_admin_chat_target<'a>(
            &'a self,
            target_identifier: &'a str,
        ) -> crate::settings::AdminChatTargetFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("resolver");
                state.calls.push(target_identifier.to_owned());
                if let Some(error) = &state.error {
                    return Err(error.clone());
                }
                state
                    .target
                    .clone()
                    .ok_or_else(|| "missing target".to_owned())
            })
        }
    }

    #[derive(Clone, Default)]
    struct ChatCommunicationEffectsStub {
        state: Arc<Mutex<ChatCommunicationState>>,
    }

    #[derive(Default)]
    struct ChatCommunicationState {
        enabled: Vec<i64>,
        disabled: Vec<i64>,
    }

    impl ChatCommunicationEffectsStub {
        fn enabled(&self) -> Vec<i64> {
            self.state.lock().expect("communication").enabled.clone()
        }
    }

    impl ChatCommunicationEffects for ChatCommunicationEffectsStub {
        type Error = String;

        fn disable_chat_communication<'a>(
            &'a self,
            chat_id: i64,
        ) -> crate::members::ChatCommunicationFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("communication")
                    .disabled
                    .push(chat_id);
                Ok(())
            })
        }

        fn enable_chat_communication<'a>(
            &'a self,
            chat_id: i64,
        ) -> crate::members::ChatCommunicationFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("communication")
                    .enabled
                    .push(chat_id);
                Ok(())
            })
        }
    }

    #[derive(Clone, Default)]
    struct EffectsStub {
        plans: Arc<Mutex<Vec<AdminTextPlan>>>,
    }

    impl EffectsStub {
        fn plans(&self) -> Vec<AdminTextPlan> {
            self.plans.lock().expect("plans").clone()
        }
    }

    impl AdminCommandEffects for EffectsStub {
        type Error = Infallible;

        fn send_admin_text<'a>(
            &'a self,
            plan: AdminTextPlan,
        ) -> AdminCommandEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.plans.lock().expect("plans").push(plan);
                Ok(())
            })
        }
    }
}
