use std::{convert::Infallible, fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, MessageData as TelegramMessageData,
    Update as TelegramUpdate, UpdateType as TelegramUpdateType,
};
use openplotva_server::ACTION_SEND_TEXT;
use thiserror::Error;
use time::OffsetDateTime;

use crate::{
    permissions::{ChatPermissionPolicy, ChatPermissionStore},
    rate_limits::{ChatRateLimitPolicy, RateLimitStore},
    updates::{UpdateHandler, UpdateHandlerFuture},
};

/// Boxed future returned by message-gate policy checks.
pub type MessageGateFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

pub trait MessageGateRateLimit {
    /// Error returned by the concrete rate-limit policy.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Return whether the chat should be skipped as currently rate-limited.
    fn is_message_gate_rate_limited<'a>(
        &'a self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> MessageGateFuture<'a, bool, Self::Error>;
}

impl<S> MessageGateRateLimit for ChatRateLimitPolicy<S>
where
    S: RateLimitStore + Send + Sync,
{
    type Error = Infallible;

    fn is_message_gate_rate_limited<'a>(
        &'a self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> MessageGateFuture<'a, bool, Self::Error> {
        Box::pin(async move { Ok(self.is_rate_limited_at(chat_id, now).await.rate_limited) })
    }
}

pub trait MessageGatePermission {
    /// Error returned by the concrete permission policy.
    type Error: fmt::Display + Send + Sync + 'static;

    fn can_message_gate_send_text<'a>(
        &'a self,
        chat_id: i64,
        chat_type: &'static str,
        now: OffsetDateTime,
    ) -> MessageGateFuture<'a, bool, Self::Error>;
}

impl<S> MessageGatePermission for ChatPermissionPolicy<S>
where
    S: ChatPermissionStore + Send + Sync,
{
    type Error = Infallible;

    fn can_message_gate_send_text<'a>(
        &'a self,
        chat_id: i64,
        chat_type: &'static str,
        now: OffsetDateTime,
    ) -> MessageGateFuture<'a, bool, Self::Error> {
        Box::pin(async move {
            Ok(self
                .can_perform_action_at(chat_id, Some(chat_type), ACTION_SEND_TEXT, now)
                .await
                .allowed)
        })
    }
}

pub trait MessageGateBlockedChat {
    /// Error returned by the concrete blocked-chat store.
    type Error: fmt::Display + Send + Sync + 'static;

    fn is_message_gate_chat_blocked<'a>(
        &'a self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> MessageGateFuture<'a, bool, Self::Error>;
}

impl MessageGateBlockedChat for openplotva_storage::RedisBlockedChatStore {
    type Error = openplotva_storage::StorageError;

    fn is_message_gate_chat_blocked<'a>(
        &'a self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> MessageGateFuture<'a, bool, Self::Error> {
        Box::pin(async move { self.is_chat_blocked_at(chat_id, now).await })
    }
}

pub trait TextReplySettingsStore {
    /// Error returned by the concrete settings store.
    type Error: fmt::Display + Send + Sync + 'static;

    fn text_reply_chat_settings<'a>(
        &'a self,
        chat_id: i64,
    ) -> MessageGateFuture<'a, Option<openplotva_core::ChatSettings>, Self::Error>;
}

impl TextReplySettingsStore for openplotva_storage::PostgresChatSettingsStore {
    type Error = openplotva_storage::StorageError;

    fn text_reply_chat_settings<'a>(
        &'a self,
        chat_id: i64,
    ) -> MessageGateFuture<'a, Option<openplotva_core::ChatSettings>, Self::Error> {
        Box::pin(async move { self.get_chat_settings(chat_id).await })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageGateDecision {
    /// Continue into the fetcher route chain.
    Continue,
    Skip(MessageGateSkipReason),
}

/// Reason one decoded message was skipped before routing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageGateSkipReason {
    ZeroChat,
    RateLimited,
    PermissionDenied,
    BlockedChat,
}

/// Fatal errors from policy checks.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MessageGateCheckError {
    /// Rate-limit policy failed.
    #[error("rate-limit check failed: {message}")]
    RateLimit {
        /// Error text.
        message: String,
    },
    /// Permission policy failed.
    #[error("permission check failed: {message}")]
    Permission {
        /// Error text.
        message: String,
    },
}

/// Fatal errors from the update-handler adapter.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MessageGateUpdateError {
    /// Policy check failed before delegation.
    #[error(transparent)]
    Check(#[from] MessageGateCheckError),
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

/// Fatal errors from the post-service blocked-chat adapter.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PostServiceBlockedChatUpdateError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

/// Fatal errors from the post-command text-reply settings adapter.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TextReplySettingsGateUpdateError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct MessageGateUpdateHandler<RateLimit, Permission, BlockedChat, Next> {
    rate_limit: Arc<RateLimit>,
    permission: Arc<Permission>,
    blocked_chat: Arc<BlockedChat>,
    bot_username: String,
    next: Arc<Next>,
}

impl<RateLimit, Permission, BlockedChat, Next>
    MessageGateUpdateHandler<RateLimit, Permission, BlockedChat, Next>
{
    /// Build a message gate around the real downstream update handler.
    #[must_use]
    pub fn new(
        rate_limit: Arc<RateLimit>,
        permission: Arc<Permission>,
        blocked_chat: Arc<BlockedChat>,
        bot_username: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            rate_limit,
            permission,
            blocked_chat,
            bot_username: bot_username.into(),
            next,
        }
    }
}

impl<RateLimit, Permission, BlockedChat, Next> UpdateHandler
    for MessageGateUpdateHandler<RateLimit, Permission, BlockedChat, Next>
where
    RateLimit: MessageGateRateLimit + Send + Sync,
    Permission: MessageGatePermission + Send + Sync,
    BlockedChat: MessageGateBlockedChat + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = MessageGateUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            match message_gate_decision_at(
                self.rate_limit.as_ref(),
                self.permission.as_ref(),
                self.blocked_chat.as_ref(),
                &update,
                &self.bot_username,
                OffsetDateTime::now_utc(),
            )
            .await?
            {
                MessageGateDecision::Continue => {
                    self.next.handle_update(update).await.map_err(|error| {
                        MessageGateUpdateError::Downstream {
                            message: error.to_string(),
                        }
                    })
                }
                MessageGateDecision::Skip(reason) => {
                    let (chat_id, chat_type) = message_gate_update_chat_fields(&update);
                    tracing::debug!(
                        ?reason,
                        update_id = update.id,
                        chat_id,
                        chat_type,
                        "Telegram message skipped before fetcher routing"
                    );
                    Ok(())
                }
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct PostServiceBlockedChatUpdateHandler<BlockedChat, Next> {
    blocked_chat: Arc<BlockedChat>,
    next: Arc<Next>,
}

impl<BlockedChat, Next> PostServiceBlockedChatUpdateHandler<BlockedChat, Next> {
    /// Build a post-service blocked-chat gate around the remaining message routes.
    #[must_use]
    pub fn new(blocked_chat: Arc<BlockedChat>, next: Arc<Next>) -> Self {
        Self { blocked_chat, next }
    }
}

impl<BlockedChat, Next> UpdateHandler for PostServiceBlockedChatUpdateHandler<BlockedChat, Next>
where
    BlockedChat: MessageGateBlockedChat + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = PostServiceBlockedChatUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            if post_service_blocked_chat_skip_at(
                self.blocked_chat.as_ref(),
                &update,
                OffsetDateTime::now_utc(),
            )
            .await
            {
                tracing::debug!(
                    update_id = update.id,
                    "Telegram service message skipped by blocked-chat gate after service routing"
                );
                return Ok(());
            }
            self.next.handle_update(update).await.map_err(|error| {
                PostServiceBlockedChatUpdateError::Downstream {
                    message: error.to_string(),
                }
            })
        })
    }
}

#[derive(Clone, Debug)]
pub struct TextReplySettingsGateUpdateHandler<Settings, Next> {
    settings: Arc<Settings>,
    next: Arc<Next>,
}

impl<Settings, Next> TextReplySettingsGateUpdateHandler<Settings, Next> {
    /// Build a text-reply settings gate around the remaining content routes.
    #[must_use]
    pub fn new(settings: Arc<Settings>, next: Arc<Next>) -> Self {
        Self { settings, next }
    }
}

impl<Settings, Next> UpdateHandler for TextReplySettingsGateUpdateHandler<Settings, Next>
where
    Settings: TextReplySettingsStore + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = TextReplySettingsGateUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            if text_reply_settings_gate_should_skip(self.settings.as_ref(), &update).await {
                tracing::debug!(
                    update_id = update.id,
                    "Telegram message skipped by disabled global text-reply settings"
                );
                return Ok(());
            }
            self.next.handle_update(update).await.map_err(|error| {
                TextReplySettingsGateUpdateError::Downstream {
                    message: error.to_string(),
                }
            })
        })
    }
}

pub async fn message_gate_decision_at<RateLimit, Permission, BlockedChat>(
    rate_limit: &RateLimit,
    permission: &Permission,
    blocked_chat: &BlockedChat,
    update: &TelegramUpdate,
    bot_username: &str,
    now: OffsetDateTime,
) -> Result<MessageGateDecision, MessageGateCheckError>
where
    RateLimit: MessageGateRateLimit + Sync,
    Permission: MessageGatePermission + Sync,
    BlockedChat: MessageGateBlockedChat + Sync,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        return Ok(MessageGateDecision::Continue);
    };

    let chat_id: i64 = message.chat.get_id().into();
    if chat_id == 0 {
        return Ok(MessageGateDecision::Skip(MessageGateSkipReason::ZeroChat));
    }
    let rate_limited = rate_limit
        .is_message_gate_rate_limited(chat_id, now)
        .await
        .map_err(|error| MessageGateCheckError::RateLimit {
            message: error.to_string(),
        })?;
    if rate_limited {
        return Ok(MessageGateDecision::Skip(
            MessageGateSkipReason::RateLimited,
        ));
    }
    if openplotva_updates::is_settings_command_message(message, bot_username) {
        return Ok(MessageGateDecision::Continue);
    }
    let allowed = permission
        .can_message_gate_send_text(chat_id, chat_type_name(&message.chat), now)
        .await
        .map_err(|error| MessageGateCheckError::Permission {
            message: error.to_string(),
        })?;
    if !allowed {
        return Ok(MessageGateDecision::Skip(
            MessageGateSkipReason::PermissionDenied,
        ));
    }
    if is_go_pre_block_service_message(message) {
        return Ok(MessageGateDecision::Continue);
    }
    let blocked = blocked_chat
        .is_message_gate_chat_blocked(chat_id, now)
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(
                chat_id,
                %error,
                "failed to check blocked chat status; continuing with fail-open behavior"
            );
            false
        });
    if blocked {
        return Ok(MessageGateDecision::Skip(
            MessageGateSkipReason::BlockedChat,
        ));
    }

    Ok(MessageGateDecision::Continue)
}

pub async fn text_reply_settings_gate_should_skip<Settings>(
    settings: &Settings,
    update: &TelegramUpdate,
) -> bool
where
    Settings: TextReplySettingsStore + Sync,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        return false;
    };
    if chat_type_name(&message.chat) == "private" {
        return false;
    }
    let chat_id: i64 = message.chat.get_id().into();
    if chat_id == 0 {
        return false;
    }
    match settings.text_reply_chat_settings(chat_id).await {
        Ok(Some(settings)) => !settings.enable_global_text_reply,
        Ok(None) => false,
        Err(error) => {
            tracing::warn!(
                chat_id,
                %error,
                "failed to load chat settings for text-reply gate; continuing with defaults"
            );
            false
        }
    }
}

pub async fn post_service_blocked_chat_skip_at<BlockedChat>(
    blocked_chat: &BlockedChat,
    update: &TelegramUpdate,
    now: OffsetDateTime,
) -> bool
where
    BlockedChat: MessageGateBlockedChat + Sync,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        return false;
    };
    if !is_go_pre_block_service_message(message) {
        return false;
    }
    let chat_id: i64 = message.chat.get_id().into();
    blocked_chat
        .is_message_gate_chat_blocked(chat_id, now)
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(
                chat_id,
                %error,
                "failed to check blocked chat status after service routing; continuing with fail-open behavior"
            );
            false
        })
}

fn is_go_pre_block_service_message(message: &TelegramMessage) -> bool {
    matches!(
        &message.data,
        TelegramMessageData::NewChatMembers(_)
            | TelegramMessageData::LeftChatMember(_)
            | TelegramMessageData::SuccessfulPayment(_)
    )
}

fn chat_type_name(chat: &TelegramChat) -> &'static str {
    match chat {
        TelegramChat::Channel(_) => "channel",
        TelegramChat::Group(_) => "group",
        TelegramChat::Private(_) => "private",
        TelegramChat::Supergroup(_) => "supergroup",
    }
}

fn message_gate_update_chat_fields(update: &TelegramUpdate) -> (i64, &'static str) {
    let TelegramUpdateType::Message(message) = &update.update_type else {
        return (0, "");
    };
    (message.chat.get_id().into(), chat_type_name(&message.chat))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
        UpdateStateStoreFuture, process_update_with_state_store_at,
    };
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::json;
    use std::{
        env,
        error::Error,
        io,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    #[tokio::test]
    async fn message_gate_delegates_non_message_updates() -> Result<(), Box<dyn Error>> {
        let rate = RateLimitStub::default();
        let permission = PermissionStub::allow();
        let blocked = BlockedChatStub::not_blocked();
        let update = serde_json::from_value(json!({
            "update_id": 9,
            "chosen_inline_result": {
                "result_id": "result-1",
                "from": {
                    "id": 456,
                    "is_bot": false,
                    "first_name": "User"
                },
                "query": "hello"
            }
        }))?;

        assert_eq!(
            message_gate_decision_at(&rate, &permission, &blocked, &update, "PlotvaBot", now())
                .await?,
            MessageGateDecision::Continue
        );
        assert_eq!(blocked.calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn message_gate_skips_rate_limited_before_permission() -> Result<(), Box<dyn Error>> {
        let rate = RateLimitStub::limited();
        let permission = PermissionStub::deny();
        let blocked = BlockedChatStub::blocked();

        let decision = message_gate_decision_at(
            &rate,
            &permission,
            &blocked,
            &message_update("/reset"),
            "PlotvaBot",
            now(),
        )
        .await?;

        assert_eq!(
            decision,
            MessageGateDecision::Skip(MessageGateSkipReason::RateLimited)
        );
        assert_eq!(permission.calls(), 0);
        assert_eq!(blocked.calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn message_gate_lets_settings_bypass_permission_and_blocked_chat_after_rate_check()
    -> Result<(), Box<dyn Error>> {
        let rate = RateLimitStub::default();
        let permission = PermissionStub::deny();
        let blocked = BlockedChatStub::blocked();

        let decision = message_gate_decision_at(
            &rate,
            &permission,
            &blocked,
            &message_update("/settings@PlotvaBot"),
            "PlotvaBot",
            now(),
        )
        .await?;

        assert_eq!(decision, MessageGateDecision::Continue);
        assert_eq!(permission.calls(), 0);
        assert_eq!(blocked.calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn message_gate_skips_permission_denied_non_settings() -> Result<(), Box<dyn Error>> {
        let rate = RateLimitStub::default();
        let permission = PermissionStub::deny();
        let blocked = BlockedChatStub::blocked();

        let decision = message_gate_decision_at(
            &rate,
            &permission,
            &blocked,
            &message_update("/reset"),
            "PlotvaBot",
            now(),
        )
        .await?;

        assert_eq!(
            decision,
            MessageGateDecision::Skip(MessageGateSkipReason::PermissionDenied)
        );
        assert_eq!(
            permission.observed(),
            vec![(123, "supergroup", ACTION_SEND_TEXT)]
        );
        assert_eq!(blocked.calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn message_gate_skips_blocked_chat_after_permission() -> Result<(), Box<dyn Error>> {
        let rate = RateLimitStub::default();
        let permission = PermissionStub::allow();
        let blocked = BlockedChatStub::blocked();

        let decision = message_gate_decision_at(
            &rate,
            &permission,
            &blocked,
            &message_update("hello"),
            "PlotvaBot",
            now(),
        )
        .await?;

        assert_eq!(
            decision,
            MessageGateDecision::Skip(MessageGateSkipReason::BlockedChat)
        );
        assert_eq!(permission.calls(), 1);
        assert_eq!(blocked.observed(), vec![123]);
        Ok(())
    }

    #[tokio::test]
    async fn message_gate_lets_go_service_messages_reach_pre_block_side_effects()
    -> Result<(), Box<dyn Error>> {
        for update in [
            new_chat_members_update(),
            left_chat_member_update(),
            successful_payment_update(),
        ] {
            let rate = RateLimitStub::default();
            let permission = PermissionStub::allow();
            let blocked = BlockedChatStub::blocked();

            let decision =
                message_gate_decision_at(&rate, &permission, &blocked, &update, "PlotvaBot", now())
                    .await?;

            assert_eq!(decision, MessageGateDecision::Continue);
            assert_eq!(permission.calls(), 1);
            assert_eq!(
                blocked.calls(),
                0,
                "Blocked-chat checks run after service-message handlers"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn message_gate_blocked_chat_lookup_fails_open_like_go() -> Result<(), Box<dyn Error>> {
        let rate = RateLimitStub::default();
        let permission = PermissionStub::allow();
        let blocked = BlockedChatStub::failed();

        let decision = message_gate_decision_at(
            &rate,
            &permission,
            &blocked,
            &message_update("hello"),
            "PlotvaBot",
            now(),
        )
        .await?;

        assert_eq!(decision, MessageGateDecision::Continue);
        assert_eq!(permission.calls(), 1);
        assert_eq!(blocked.observed(), vec![123]);
        Ok(())
    }

    #[tokio::test]
    async fn message_gate_handler_skips_without_downstream() -> Result<(), Box<dyn Error>> {
        let next = Arc::new(NextStub::default());
        let handler = MessageGateUpdateHandler::new(
            Arc::new(RateLimitStub::limited()),
            Arc::new(PermissionStub::allow()),
            Arc::new(BlockedChatStub::blocked()),
            "PlotvaBot",
            Arc::clone(&next),
        );

        handler.handle_update(message_update("/reset")).await?;

        assert_eq!(next.calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn post_service_blocked_chat_gate_skips_service_messages_after_side_effects()
    -> Result<(), Box<dyn Error>> {
        let next = Arc::new(NextStub::default());
        let blocked = Arc::new(BlockedChatStub::blocked());
        let handler = PostServiceBlockedChatUpdateHandler::new(Arc::clone(&blocked), next.clone());

        handler.handle_update(new_chat_members_update()).await?;

        assert_eq!(blocked.observed(), vec![123]);
        assert_eq!(next.calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn post_service_blocked_chat_gate_delegates_non_service_messages()
    -> Result<(), Box<dyn Error>> {
        let next = Arc::new(NextStub::default());
        let blocked = Arc::new(BlockedChatStub::blocked());
        let handler = PostServiceBlockedChatUpdateHandler::new(blocked, next.clone());

        handler.handle_update(message_update("hello")).await?;

        assert_eq!(next.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn text_reply_settings_gate_skips_group_content_when_disabled_like_go()
    -> Result<(), Box<dyn Error>> {
        let settings = TextReplySettingsStub::disabled();

        assert!(text_reply_settings_gate_should_skip(&settings, &message_update("hello")).await);
        assert_eq!(settings.observed(), vec![123]);
        Ok(())
    }

    #[tokio::test]
    async fn text_reply_settings_gate_continues_private_missing_and_error_like_go_defaults()
    -> Result<(), Box<dyn Error>> {
        let disabled = TextReplySettingsStub::disabled();
        assert!(
            !text_reply_settings_gate_should_skip(&disabled, &private_message_update("hello"))
                .await
        );
        assert_eq!(disabled.calls(), 0);

        let missing = TextReplySettingsStub::missing();
        assert!(!text_reply_settings_gate_should_skip(&missing, &message_update("hello")).await);
        assert_eq!(missing.observed(), vec![123]);

        let failed = TextReplySettingsStub::failed();
        assert!(!text_reply_settings_gate_should_skip(&failed, &message_update("hello")).await);
        assert_eq!(failed.observed(), vec![123]);
        Ok(())
    }

    #[tokio::test]
    async fn text_reply_settings_gate_handler_skips_without_downstream()
    -> Result<(), Box<dyn Error>> {
        let next = Arc::new(NextStub::default());
        let handler = TextReplySettingsGateUpdateHandler::new(
            Arc::new(TextReplySettingsStub::disabled()),
            next.clone(),
        );

        handler.handle_update(message_update("hello")).await?;

        assert_eq!(next.calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_blocked_chat_gate_skips_non_settings_and_bypasses_settings_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-message-gate:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let blocked_store = openplotva_storage::RedisBlockedChatStore::with_key_prefix(
            redis_client.clone(),
            format!("openplotva:test:blocked-chat:{suffix}:"),
        );
        let blocked_key = blocked_store.key_for_chat(123);
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .arg(&blocked_key)
            .query_async(&mut redis)
            .await?;

        blocked_store
            .block_chat_until_with_ttl(
                123,
                OffsetDateTime::now_utc() + ::time::Duration::minutes(10),
                Duration::from_secs(600),
            )
            .await?;
        update_queue
            .enqueue_update(&message_update("hello"))
            .await?;
        update_queue
            .enqueue_update(&message_update("/settings@PlotvaBot"))
            .await?;
        assert_eq!(update_queue.len().await?, 2);

        let state_store = UpdateStateStoreStub::default();
        let terminal = Arc::new(NextStub::default());
        let handler = MessageGateUpdateHandler::new(
            Arc::new(RateLimitStub::default()),
            Arc::new(PermissionStub::allow()),
            Arc::new(blocked_store.clone()),
            "PlotvaBot",
            Arc::clone(&terminal),
        );
        let config = UpdateConsumerConfig {
            dequeue_timeout: Duration::from_millis(1),
            state_timeout: Duration::from_secs(1),
            handle_timeout: Duration::from_secs(1),
            side_effect_max_age: Duration::from_secs(60),
            worker_limit: 1,
        };
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_010);

        let blocked_update = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected blocked message update"))?;
        let blocked_report = process_update_with_state_store_at(
            blocked_update,
            config,
            now,
            &state_store,
            |update| handler.handle_update(update),
        )
        .await;
        assert_eq!(blocked_report.update_name, "message");
        assert_eq!(blocked_report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            blocked_report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(terminal.calls(), 0);

        let settings_update = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected settings message update"))?;
        let settings_report = process_update_with_state_store_at(
            settings_update,
            config,
            now,
            &state_store,
            |update| handler.handle_update(update),
        )
        .await;
        assert_eq!(settings_report.update_name, "message");
        assert_eq!(settings_report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            settings_report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(terminal.calls(), 1);
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:123:supergroup".to_owned(),
                "user:456:User".to_owned(),
                "chat:123:supergroup".to_owned(),
                "user:456:User".to_owned(),
            ]
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .arg(&blocked_key)
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    struct BlockedChatStub {
        blocked: bool,
        error: Option<&'static str>,
        calls: AtomicUsize,
        observed: Mutex<Vec<i64>>,
    }

    impl BlockedChatStub {
        fn not_blocked() -> Self {
            Self::new(false, None)
        }

        fn blocked() -> Self {
            Self::new(true, None)
        }

        fn failed() -> Self {
            Self::new(false, Some("redis down"))
        }

        fn new(blocked: bool, error: Option<&'static str>) -> Self {
            Self {
                blocked,
                error,
                calls: AtomicUsize::new(0),
                observed: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        fn observed(&self) -> Vec<i64> {
            self.observed.lock().expect("lock observed").clone()
        }
    }

    impl MessageGateBlockedChat for BlockedChatStub {
        type Error = &'static str;

        fn is_message_gate_chat_blocked<'a>(
            &'a self,
            chat_id: i64,
            _now: OffsetDateTime,
        ) -> MessageGateFuture<'a, bool, Self::Error> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.observed.lock().expect("lock observed").push(chat_id);
                if let Some(error) = self.error {
                    Err(error)
                } else {
                    Ok(self.blocked)
                }
            })
        }
    }

    enum TextReplySettingsResponse {
        Missing,
        Disabled,
        Failed,
    }

    struct TextReplySettingsStub {
        response: TextReplySettingsResponse,
        calls: AtomicUsize,
        observed: Mutex<Vec<i64>>,
    }

    impl TextReplySettingsStub {
        fn missing() -> Self {
            Self::new(TextReplySettingsResponse::Missing)
        }

        fn disabled() -> Self {
            Self::new(TextReplySettingsResponse::Disabled)
        }

        fn failed() -> Self {
            Self::new(TextReplySettingsResponse::Failed)
        }

        fn new(response: TextReplySettingsResponse) -> Self {
            Self {
                response,
                calls: AtomicUsize::new(0),
                observed: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        fn observed(&self) -> Vec<i64> {
            self.observed.lock().expect("lock observed").clone()
        }
    }

    impl TextReplySettingsStore for TextReplySettingsStub {
        type Error = &'static str;

        fn text_reply_chat_settings<'a>(
            &'a self,
            chat_id: i64,
        ) -> MessageGateFuture<'a, Option<openplotva_core::ChatSettings>, Self::Error> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.observed.lock().expect("lock observed").push(chat_id);
                match self.response {
                    TextReplySettingsResponse::Missing => Ok(None),
                    TextReplySettingsResponse::Disabled => {
                        let mut settings = openplotva_core::ChatSettings::defaults(chat_id);
                        settings.enable_global_text_reply = false;
                        Ok(Some(settings))
                    }
                    TextReplySettingsResponse::Failed => Err("settings down"),
                }
            })
        }
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp")
    }

    fn message_update(text: &str) -> TelegramUpdate {
        let command_len = text
            .split_whitespace()
            .next()
            .map(|command| command.encode_utf16().count())
            .unwrap_or_default();
        serde_json::from_value(json!({
            "update_id": 1,
            "message": {
                "message_id": 7,
                "date": 1_700_000_000,
                "chat": {
                    "id": 123,
                    "type": "supergroup",
                    "title": "group"
                },
                "from": {
                    "id": 456,
                    "is_bot": false,
                    "first_name": "User"
                },
                "text": text,
                "entities": [
                    {
                        "offset": 0,
                        "length": command_len,
                        "type": "bot_command"
                    }
                ]
            }
        }))
        .expect("message update JSON should parse")
    }

    fn private_message_update(text: &str) -> TelegramUpdate {
        serde_json::from_value(json!({
            "update_id": 11,
            "message": {
                "message_id": 17,
                "date": 1_700_000_000,
                "chat": {
                    "id": 456,
                    "type": "private",
                    "first_name": "User"
                },
                "from": {
                    "id": 456,
                    "is_bot": false,
                    "first_name": "User"
                },
                "text": text
            }
        }))
        .expect("private message update JSON should parse")
    }

    fn new_chat_members_update() -> TelegramUpdate {
        serde_json::from_value(json!({
            "update_id": 2,
            "message": {
                "message_id": 8,
                "date": 1_700_000_000,
                "chat": {
                    "id": 123,
                    "type": "supergroup",
                    "title": "group"
                },
                "from": {
                    "id": 456,
                    "is_bot": false,
                    "first_name": "User"
                },
                "new_chat_members": [
                    {
                        "id": 789,
                        "is_bot": false,
                        "first_name": "New"
                    }
                ]
            }
        }))
        .expect("new chat members update JSON should parse")
    }

    fn left_chat_member_update() -> TelegramUpdate {
        serde_json::from_value(json!({
            "update_id": 3,
            "message": {
                "message_id": 9,
                "date": 1_700_000_000,
                "chat": {
                    "id": 123,
                    "type": "supergroup",
                    "title": "group"
                },
                "from": {
                    "id": 456,
                    "is_bot": false,
                    "first_name": "User"
                },
                "left_chat_member": {
                    "id": 789,
                    "is_bot": false,
                    "first_name": "Gone"
                }
            }
        }))
        .expect("left chat member update JSON should parse")
    }

    fn successful_payment_update() -> TelegramUpdate {
        serde_json::from_value(json!({
            "update_id": 4,
            "message": {
                "message_id": 10,
                "date": 1_700_000_000,
                "chat": {
                    "id": 123,
                    "type": "supergroup",
                    "title": "group"
                },
                "from": {
                    "id": 456,
                    "is_bot": false,
                    "first_name": "User"
                },
                "successful_payment": {
                    "currency": "XTR",
                    "total_amount": 300,
                    "invoice_payload": "vip_subscription:456",
                    "telegram_payment_charge_id": "charge-1",
                    "provider_payment_charge_id": "provider-charge-1"
                }
            }
        }))
        .expect("successful payment update JSON should parse")
    }

    #[derive(Default)]
    struct RateLimitStub {
        limited: bool,
    }

    impl RateLimitStub {
        fn limited() -> Self {
            Self { limited: true }
        }
    }

    impl MessageGateRateLimit for RateLimitStub {
        type Error = Infallible;

        fn is_message_gate_rate_limited<'a>(
            &'a self,
            _chat_id: i64,
            _now: OffsetDateTime,
        ) -> MessageGateFuture<'a, bool, Self::Error> {
            Box::pin(async move { Ok(self.limited) })
        }
    }

    struct PermissionStub {
        allowed: bool,
        calls: AtomicUsize,
        observed: Mutex<Vec<(i64, &'static str, &'static str)>>,
    }

    impl PermissionStub {
        fn allow() -> Self {
            Self::new(true)
        }

        fn deny() -> Self {
            Self::new(false)
        }

        fn new(allowed: bool) -> Self {
            Self {
                allowed,
                calls: AtomicUsize::new(0),
                observed: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        fn observed(&self) -> Vec<(i64, &'static str, &'static str)> {
            self.observed.lock().expect("lock observed").clone()
        }
    }

    impl MessageGatePermission for PermissionStub {
        type Error = Infallible;

        fn can_message_gate_send_text<'a>(
            &'a self,
            chat_id: i64,
            chat_type: &'static str,
            _now: OffsetDateTime,
        ) -> MessageGateFuture<'a, bool, Self::Error> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.observed.lock().expect("lock observed").push((
                    chat_id,
                    chat_type,
                    ACTION_SEND_TEXT,
                ));
                Ok(self.allowed)
            })
        }
    }

    #[derive(Default)]
    struct NextStub {
        calls: AtomicUsize,
    }

    impl NextStub {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl UpdateHandler for NextStub {
        type Error = Infallible;

        fn handle_update<'a>(
            &'a self,
            _update: TelegramUpdate,
        ) -> UpdateHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
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
}
