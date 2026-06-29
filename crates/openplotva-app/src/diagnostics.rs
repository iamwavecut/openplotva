//! App-level fetcher diagnostics command behavior.

use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use carapax::types::{
    Message as TelegramMessage, MessageData as TelegramMessageData, ReplyTo as TelegramReplyTo,
    Text as TelegramText, TextEntity as TelegramTextEntity,
    TextEntityPosition as TelegramTextEntityPosition, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType,
};
use futures_util::StreamExt;
use openplotva_telegram::{
    ChatRef, DispatcherQueue, OutboundBuildError, ReplyMessageRef, TELEGRAM_PARSE_MODE_HTML,
    TelegramClient, TelegramOutboundMethod, TextMessageRequest, build_text_message_methods,
    execute_telegram_method,
};
use redis::AsyncCommands;
use serde::Deserialize;
use thiserror::Error;
use time::OffsetDateTime;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use crate::virtual_messages::{
    QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory, queue_text_message_parts,
};

const PING_COMMAND: &str = "ping";
const DEBUG_COMMAND: &str = "debug";
const PING_START_TEXT: &str = "Пингин!";
const DEBUG_NEEDS_REPLY_TEXT: &str = "Please reply to a message to debug it.";
const DEBUG_SERIALIZE_ERROR_TEXT: &str = "Failed to serialize message.";
const PING_TIMEOUT: Duration = Duration::from_secs(1);
const ERROR_DELETE_AFTER: Duration = Duration::from_secs(120);
const AI_PING_CHANNEL: &str = "plotai/ping";
const AI_PING_REPLY_CHANNEL: &str = "plotai/ping_reply";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DiagnosticsBotIdentity {
    /// Bot username from `getMe`.
    pub username: String,
}

/// Planned diagnostic text send.
#[derive(Clone, Debug, PartialEq)]
pub struct DiagnosticsTextPlan {
    pub message: TextMessageRequest,
    pub reply_to: ReplyMessageRef,
    pub direct_chattable: bool,
    pub ephemeral_delete_after: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PingSequencePlan {
    /// Initial text send.
    pub start: DiagnosticsTextPlan,
    pub timeout: Duration,
}

impl PingSequencePlan {
    #[must_use]
    pub fn result_text_plan(&self, result: bool) -> DiagnosticsTextPlan {
        let mut plan = self.start.clone();
        plan.message.text = format!("Понг! {result}");
        plan
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DiagnosticsCommandRoute {
    /// Message does not belong to this slice.
    NotHandled,
    Ping(PingSequencePlan),
    Send(DiagnosticsTextPlan),
}

/// Boxed future returned by diagnostic command effects.
pub type DiagnosticsEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait DiagnosticsCommandEffects {
    /// Concrete effect error.
    type Error: fmt::Display + Send;

    fn send_diagnostics_text<'a>(
        &'a self,
        plan: DiagnosticsTextPlan,
    ) -> DiagnosticsEffectFuture<'a, Self::Error>;

    fn start_ping_sequence<'a>(
        &'a self,
        plan: PingSequencePlan,
    ) -> DiagnosticsEffectFuture<'a, Self::Error>;
}

/// Generator for virtual-message IDs used by queued diagnostic text sends.
pub type DiagnosticsVirtualIdFactory = VirtualIdFactory;

#[derive(Clone)]
pub struct DiagnosticsDispatcherEffects {
    queue: Arc<DispatcherQueue>,
    telegram: TelegramClient,
    redis: redis::Client,
    next_virtual_id: DiagnosticsVirtualIdFactory,
}

impl DiagnosticsDispatcherEffects {
    /// Build diagnostics effects backed by the normal dispatcher, direct Telegram, and Redis ping.
    #[must_use]
    pub fn new(
        queue: Arc<DispatcherQueue>,
        telegram: TelegramClient,
        redis: redis::Client,
    ) -> Self {
        Self {
            queue,
            telegram,
            redis,
            next_virtual_id: monotonic_virtual_id_factory("diagnostic-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: DiagnosticsVirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl DiagnosticsCommandEffects for DiagnosticsDispatcherEffects {
    type Error = DiagnosticsDispatchEffectError;

    fn send_diagnostics_text<'a>(
        &'a self,
        plan: DiagnosticsTextPlan,
    ) -> DiagnosticsEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            send_diagnostics_text_plan(&self.queue, &self.telegram, &self.next_virtual_id, plan)
                .await
        })
    }

    fn start_ping_sequence<'a>(
        &'a self,
        plan: PingSequencePlan,
    ) -> DiagnosticsEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            send_diagnostics_text_plan(
                &self.queue,
                &self.telegram,
                &self.next_virtual_id,
                plan.start.clone(),
            )
            .await?;

            let queue = Arc::clone(&self.queue);
            let telegram = self.telegram.clone();
            let redis = self.redis.clone();
            let next_virtual_id = Arc::clone(&self.next_virtual_id);
            tokio::spawn(async move {
                let result = match ping_ai_through_redis(&redis, plan.timeout).await {
                    Ok(result) => result,
                    Err(error) => {
                        tracing::warn!(%error, "AI ping failed; reporting false result");
                        false
                    }
                };
                if let Err(error) = send_diagnostics_text_plan(
                    &queue,
                    &telegram,
                    &next_virtual_id,
                    plan.result_text_plan(result),
                )
                .await
                {
                    tracing::warn!(%error, "failed to send AI ping result");
                }
            });
            Ok(())
        })
    }
}

/// Recoverable errors from concrete diagnostics effects.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum DiagnosticsDispatchEffectError {
    #[error("failed to queue diagnostic text: {0}")]
    Queue(#[from] OutboundBuildError),
    /// Direct Telegram `SendChattable`-style send failed.
    #[error("failed to send direct diagnostic text: {0}")]
    Direct(String),
}

async fn send_diagnostics_text_plan(
    queue: &DispatcherQueue,
    telegram: &TelegramClient,
    next_virtual_id: &DiagnosticsVirtualIdFactory,
    plan: DiagnosticsTextPlan,
) -> Result<(), DiagnosticsDispatchEffectError> {
    if plan.direct_chattable {
        let methods = build_text_message_methods(&plan.message, Some(&plan.reply_to))?;
        for method in methods {
            execute_telegram_method(telegram, TelegramOutboundMethod::from(method))
                .await
                .map_err(|error| DiagnosticsDispatchEffectError::Direct(error.to_string()))?;
        }
        return Ok(());
    }

    queue_text_message_parts(
        queue,
        QueueTextRequest {
            message: &plan.message,
            reply_to: Some(&plan.reply_to),
            immediate_first: plan.ephemeral_delete_after.is_some(),
            bypass_chat_restrictions: false,
            ephemeral_delete_after: plan.ephemeral_delete_after,
        },
        || next_virtual_id(),
    )
    .await?;
    Ok(())
}

/// Errors returned by the Redis-backed AI ping probe.
#[derive(Debug, Error)]
pub enum AiPingError {
    /// Redis command failed.
    #[error("redis AI ping command: {0}")]
    Redis(#[from] redis::RedisError),
}

pub async fn ping_ai_through_redis(
    redis: &redis::Client,
    timeout: Duration,
) -> Result<bool, AiPingError> {
    let ping = format!("ping.{}", OffsetDateTime::now_utc().unix_timestamp_nanos());
    let mut pubsub = redis.get_async_pubsub().await?;
    pubsub.subscribe(AI_PING_REPLY_CHANNEL).await?;

    let mut connection = redis.get_multiplexed_async_connection().await?;
    let _: usize = connection.publish(AI_PING_CHANNEL, &ping).await?;

    match tokio::time::timeout(timeout, pubsub.on_message().next()).await {
        Ok(Some(message)) => Ok(ai_ping_payload_matches(
            message
                .get_payload::<String>()
                .as_deref()
                .unwrap_or_default(),
            &ping,
        )),
        Ok(None) | Err(_) => Ok(false),
    }
}

#[derive(Debug, Deserialize)]
struct AiPingReplyPayload {
    success: Option<String>,
    data: Option<String>,
}

fn ai_ping_payload_matches(payload: &str, ping: &str) -> bool {
    let Ok(reply) = serde_json::from_str::<AiPingReplyPayload>(payload) else {
        return false;
    };
    if reply.success.as_deref() == Some("false") {
        return false;
    }
    reply.data.as_deref() == Some(ping)
}

/// Result of one decoded update through the diagnostics wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiagnosticsCommandOutcome {
    /// Update was not diagnostics-owned and went downstream.
    Delegated,
    /// `/ping` was scheduled.
    PingStarted,
    /// A diagnostic text message was sent.
    Sent {
        direct_chattable: bool,
        /// Ephemeral delete timing, when any.
        ephemeral_delete_after: Option<Duration>,
    },
    SendError {
        /// Error text.
        message: String,
    },
}

/// Fatal errors from the diagnostics update wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DiagnosticsCommandError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct DiagnosticsCommandUpdateHandler<Effects, Next> {
    bot: DiagnosticsBotIdentity,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Effects, Next> DiagnosticsCommandUpdateHandler<Effects, Next> {
    /// Build a diagnostics handler around the real downstream update handler.
    pub fn new(bot: DiagnosticsBotIdentity, effects: Arc<Effects>, next: Arc<Next>) -> Self {
        Self { bot, effects, next }
    }
}

impl<Effects, Next> UpdateHandler for DiagnosticsCommandUpdateHandler<Effects, Next>
where
    Effects: DiagnosticsCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = DiagnosticsCommandError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_diagnostics_command_update_or_else(
                &self.bot,
                self.effects.as_ref(),
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_diagnostics_command_update_or_else<
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    bot: &DiagnosticsBotIdentity,
    effects: &Effects,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<DiagnosticsCommandOutcome, DiagnosticsCommandError>
where
    Effects: DiagnosticsCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| DiagnosticsCommandError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DiagnosticsCommandOutcome::Delegated);
    };

    match route_diagnostics_command(bot, message) {
        DiagnosticsCommandRoute::NotHandled => {
            handle_other(update)
                .await
                .map_err(|error| DiagnosticsCommandError::Downstream {
                    message: error.to_string(),
                })?;
            Ok(DiagnosticsCommandOutcome::Delegated)
        }
        DiagnosticsCommandRoute::Ping(plan) => match effects.start_ping_sequence(plan).await {
            Ok(()) => Ok(DiagnosticsCommandOutcome::PingStarted),
            Err(error) => Ok(DiagnosticsCommandOutcome::SendError {
                message: error.to_string(),
            }),
        },
        DiagnosticsCommandRoute::Send(plan) => {
            let direct_chattable = plan.direct_chattable;
            let ephemeral_delete_after = plan.ephemeral_delete_after;
            match effects.send_diagnostics_text(plan).await {
                Ok(()) => Ok(DiagnosticsCommandOutcome::Sent {
                    direct_chattable,
                    ephemeral_delete_after,
                }),
                Err(error) => Ok(DiagnosticsCommandOutcome::SendError {
                    message: error.to_string(),
                }),
            }
        }
    }
}

#[must_use]
pub fn route_diagnostics_command(
    bot: &DiagnosticsBotIdentity,
    message: &TelegramMessage,
) -> DiagnosticsCommandRoute {
    let Some(command) = leading_bot_command(message) else {
        return DiagnosticsCommandRoute::NotHandled;
    };

    if !command_for_bot(message, &command, &bot.username) {
        return DiagnosticsCommandRoute::NotHandled;
    }

    match command.command.as_str() {
        PING_COMMAND => DiagnosticsCommandRoute::Ping(ping_sequence_plan(message)),
        DEBUG_COMMAND => DiagnosticsCommandRoute::Send(debug_text_plan(message)),
        _ => DiagnosticsCommandRoute::NotHandled,
    }
}

#[must_use]
pub fn ping_sequence_plan(message: &TelegramMessage) -> PingSequencePlan {
    PingSequencePlan {
        start: DiagnosticsTextPlan {
            message: base_text_request(message, PING_START_TEXT, String::new()),
            reply_to: reply_ref(message),
            direct_chattable: false,
            ephemeral_delete_after: None,
        },
        timeout: PING_TIMEOUT,
    }
}

#[must_use]
pub fn debug_text_plan(message: &TelegramMessage) -> DiagnosticsTextPlan {
    let Some(TelegramReplyTo::Message(reply)) = message.reply_to.as_ref() else {
        return error_text_plan(message, DEBUG_NEEDS_REPLY_TEXT);
    };

    match serde_json::to_string_pretty(reply) {
        Ok(json) => DiagnosticsTextPlan {
            message: base_text_request(
                message,
                &format!(
                    "<pre><code class=\"language-json\">{}</code></pre>",
                    escape_go_html_string(&json)
                ),
                TELEGRAM_PARSE_MODE_HTML.to_owned(),
            ),
            reply_to: reply_ref_without_thread(message),
            direct_chattable: true,
            ephemeral_delete_after: None,
        },
        Err(_) => error_text_plan(message, DEBUG_SERIALIZE_ERROR_TEXT),
    }
}

fn error_text_plan(message: &TelegramMessage, text: &str) -> DiagnosticsTextPlan {
    DiagnosticsTextPlan {
        message: base_text_request(message, text, String::new()),
        reply_to: reply_ref(message),
        direct_chattable: false,
        ephemeral_delete_after: Some(ERROR_DELETE_AFTER),
    }
}

fn base_text_request(
    message: &TelegramMessage,
    text: &str,
    render_as: String,
) -> TextMessageRequest {
    TextMessageRequest {
        chat: Some(message_chat_ref(message)),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: text.to_owned(),
        render_as,
        reply_markup: None,
    }
}

fn command_for_bot(
    message: &TelegramMessage,
    command: &BotCommandInMessage,
    bot_username: &str,
) -> bool {
    if telegram_chat_is_private(message) {
        return true;
    }
    command.target.as_deref() == Some(bot_username)
}

fn telegram_chat_is_private(message: &TelegramMessage) -> bool {
    matches!(message.chat, carapax::types::Chat::Private(_))
}

fn message_chat_ref(message: &TelegramMessage) -> ChatRef {
    ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    }
}

fn reply_ref(message: &TelegramMessage) -> ReplyMessageRef {
    ReplyMessageRef {
        message_id: message.id,
        chat: message_chat_ref(message),
        is_topic_message: message.message_thread_id.is_some(),
        message_thread_id: message.message_thread_id.unwrap_or_default(),
    }
}

fn reply_ref_without_thread(message: &TelegramMessage) -> ReplyMessageRef {
    ReplyMessageRef {
        message_id: message.id,
        chat: message_chat_ref(message),
        is_topic_message: false,
        message_thread_id: 0,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BotCommandInMessage {
    command: String,
    target: Option<String>,
}

fn leading_bot_command(message: &TelegramMessage) -> Option<BotCommandInMessage> {
    let TelegramMessageData::Text(text) = &message.data else {
        return None;
    };
    leading_bot_command_from_text(text)
}

fn leading_bot_command_from_text(text: &TelegramText) -> Option<BotCommandInMessage> {
    let first = text.entities.as_ref()?.into_iter().next()?;
    let TelegramTextEntity::BotCommand(position) = first else {
        return None;
    };
    if position.offset != 0 {
        return None;
    }

    let command_with_slash = text_entity_content(&text.data, *position)?;
    let command_with_target = command_with_slash.strip_prefix('/')?;
    let (command, target) = match command_with_target.split_once('@') {
        Some((command, target)) => (command, Some(target.to_owned())),
        None => (command_with_target, None),
    };

    Some(BotCommandInMessage {
        command: command.to_owned(),
        target,
    })
}

fn text_entity_content(text: &str, position: TelegramTextEntityPosition) -> Option<String> {
    let offset = usize::try_from(position.offset).ok()?;
    let length = usize::try_from(position.length).ok()?;
    Some(String::from_utf16_lossy(
        &text
            .encode_utf16()
            .skip(offset)
            .take(length)
            .collect::<Vec<u16>>(),
    ))
}

fn escape_go_html_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&#34;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_telegram::{DispatcherConfig, TelegramOutboundMethodKind};
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use std::{
        env, io,
        sync::{
            Arc, Mutex, MutexGuard,
            atomic::{AtomicUsize, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateStateStore, UpdateStateStoreFuture,
        process_update_with_state_store_at,
    };

    fn bot() -> DiagnosticsBotIdentity {
        DiagnosticsBotIdentity {
            username: "PlotvaBot".to_owned(),
        }
    }

    fn sample_message(value: serde_json::Value) -> Result<TelegramMessage, serde_json::Error> {
        serde_json::from_value(value)
    }

    fn command_message(
        chat: serde_json::Value,
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
        reply: Option<serde_json::Value>,
    ) -> Result<TelegramMessage, serde_json::Error> {
        command_message_at(chat, text, command_len, thread_id, reply, 1_710_000_000)
    }

    fn command_message_at(
        chat: serde_json::Value,
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
        reply: Option<serde_json::Value>,
        date: i64,
    ) -> Result<TelegramMessage, serde_json::Error> {
        let mut value = serde_json::json!({
            "message_id": 10,
            "date": date,
            "chat": chat,
            "from": {"id": 5, "is_bot": false, "first_name": "Ada"},
            "text": text,
            "entities": [{"offset": 0, "length": command_len, "type": "bot_command"}]
        });
        if let Some(thread_id) = thread_id {
            value["message_thread_id"] = serde_json::json!(thread_id);
        }
        if let Some(reply) = reply {
            value["reply_to_message"] = reply;
        }
        sample_message(value)
    }

    fn private_command_message(
        text: &str,
        command_len: i64,
        reply: Option<serde_json::Value>,
    ) -> Result<TelegramMessage, serde_json::Error> {
        command_message(
            serde_json::json!({"id": 42, "type": "private", "first_name": "Ada"}),
            text,
            command_len,
            None,
            reply,
        )
    }

    fn group_command_message(
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
    ) -> Result<TelegramMessage, serde_json::Error> {
        command_message(
            serde_json::json!({"id": -42, "type": "supergroup", "title": "Group"}),
            text,
            command_len,
            thread_id,
            None,
        )
    }

    fn group_command_message_at(
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
        date: i64,
    ) -> Result<TelegramMessage, serde_json::Error> {
        command_message_at(
            serde_json::json!({"id": -42, "type": "supergroup", "title": "Group"}),
            text,
            command_len,
            thread_id,
            None,
            date,
        )
    }

    fn update_from_message(message: TelegramMessage) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "update_id": 500,
            "message": serde_json::to_value(message)?,
        }))
    }

    #[test]
    fn diagnostics_command_gate_matches_go_command_for_bot()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            route_diagnostics_command(&bot(), &private_command_message("/ping", 5, None)?),
            DiagnosticsCommandRoute::Ping(_)
        ));
        assert!(matches!(
            route_diagnostics_command(
                &bot(),
                &private_command_message("/debug@OtherBot", 15, None)?
            ),
            DiagnosticsCommandRoute::Send(_)
        ));
        assert!(matches!(
            route_diagnostics_command(&bot(), &group_command_message("/ping", 5, None)?),
            DiagnosticsCommandRoute::NotHandled
        ));
        assert!(matches!(
            route_diagnostics_command(
                &bot(),
                &group_command_message("/ping@PlotvaBot", 15, Some(7))?
            ),
            DiagnosticsCommandRoute::Ping(_)
        ));
        assert!(matches!(
            route_diagnostics_command(&bot(), &group_command_message("/debug@OtherBot", 15, None)?),
            DiagnosticsCommandRoute::NotHandled
        ));
        Ok(())
    }

    #[test]
    fn ping_sequence_matches_go_text_and_timeout() -> Result<(), Box<dyn std::error::Error>> {
        let DiagnosticsCommandRoute::Ping(plan) = route_diagnostics_command(
            &bot(),
            &group_command_message("/ping@PlotvaBot", 15, Some(7))?,
        ) else {
            panic!("expected ping route");
        };

        assert_eq!(plan.timeout, Duration::from_secs(1));
        assert_eq!(plan.start.message.text, "Пингин!");
        assert_eq!(plan.start.message.message_thread_id, 0);
        assert_eq!(plan.start.reply_to.message_thread_id, 7);
        assert_eq!(plan.start.ephemeral_delete_after, None);
        assert!(!plan.start.direct_chattable);

        let result = plan.result_text_plan(true);
        assert_eq!(result.message.text, "Понг! true");
        assert_eq!(result.reply_to, plan.start.reply_to);
        Ok(())
    }

    #[test]
    fn debug_without_reply_uses_go_two_minute_ephemeral_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let DiagnosticsCommandRoute::Send(plan) =
            route_diagnostics_command(&bot(), &private_command_message("/debug", 6, None)?)
        else {
            panic!("expected debug route");
        };

        assert_eq!(plan.message.text, DEBUG_NEEDS_REPLY_TEXT);
        assert_eq!(plan.message.render_as, "");
        assert_eq!(plan.ephemeral_delete_after, Some(Duration::from_secs(120)));
        assert!(!plan.direct_chattable);
        Ok(())
    }

    #[test]
    fn debug_with_reply_serializes_reply_as_go_html_code_block()
    -> Result<(), Box<dyn std::error::Error>> {
        let reply = serde_json::json!({
            "message_id": 9,
            "date": 1_710_000_000,
            "chat": {"id": 42, "type": "private", "first_name": "Ada"},
            "from": {"id": 6, "is_bot": false, "first_name": "Bob"},
            "text": "<json & quotes \" '>",
        });
        let DiagnosticsCommandRoute::Send(plan) =
            route_diagnostics_command(&bot(), &private_command_message("/debug", 6, Some(reply))?)
        else {
            panic!("expected debug route");
        };

        assert!(
            plan.message
                .text
                .starts_with("<pre><code class=\"language-json\">")
        );
        assert!(plan.message.text.ends_with("</code></pre>"));
        assert!(plan.message.text.contains("&lt;json &amp; quotes"));
        assert!(plan.message.text.contains("&#34;"));
        assert!(plan.message.text.contains("&#39;&gt;"));
        assert_eq!(plan.message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(plan.ephemeral_delete_after, None);
        assert!(plan.direct_chattable);
        assert_eq!(plan.reply_to.message_id, 10);
        Ok(())
    }

    #[test]
    fn debug_direct_chattable_reply_omits_topic_like_go_assignment()
    -> Result<(), Box<dyn std::error::Error>> {
        let reply = serde_json::json!({
            "message_id": 9,
            "date": 1_710_000_000,
            "chat": {"id": -42, "type": "supergroup", "title": "Group"},
            "text": "reply",
        });
        let message = command_message(
            serde_json::json!({"id": -42, "type": "supergroup", "title": "Group"}),
            "/debug@PlotvaBot",
            16,
            Some(99),
            Some(reply),
        )?;
        let DiagnosticsCommandRoute::Send(plan) = route_diagnostics_command(&bot(), &message)
        else {
            panic!("expected debug route");
        };

        assert!(plan.direct_chattable);
        assert_eq!(plan.reply_to.message_thread_id, 0);
        assert!(!plan.reply_to.is_topic_message);
        Ok(())
    }

    #[test]
    fn ai_ping_payload_matching_preserves_go_success_and_data_rules() {
        assert!(ai_ping_payload_matches(
            r#"{"success":"true","data":"ping.1"}"#,
            "ping.1"
        ));
        assert!(!ai_ping_payload_matches(
            r#"{"success":"false","data":"ping.1"}"#,
            "ping.1"
        ));
        assert!(!ai_ping_payload_matches(
            r#"{"success":"true","data":"ping.2"}"#,
            "ping.1"
        ));
        assert!(!ai_ping_payload_matches("not json", "ping.1"));
    }

    #[tokio::test]
    async fn diagnostics_handler_consumes_ping_before_wrapped_handler()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = DiagnosticsCommandUpdateHandler::new(bot(), effects.clone(), next.clone());

        handler
            .handle_update(update_from_message(private_command_message(
                "/ping", 5, None,
            )?)?)
            .await?;

        assert_eq!(next.handled_count(), 0);
        assert_eq!(effects.pings().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn diagnostics_handler_delegates_unknown_command()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());

        let outcome = handle_diagnostics_command_update_or_else(
            &bot(),
            effects.as_ref(),
            update_from_message(private_command_message("/reset", 6, None)?)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DiagnosticsCommandOutcome::Delegated);
        assert_eq!(next.handled_count(), 1);
        assert!(effects.pings().is_empty());
        assert!(effects.texts().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_debug_without_reply_queues_ephemeral_error_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-debug:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let telegram = openplotva_telegram::telegram_client("123456:TEST")?;
        let effects = Arc::new(
            DiagnosticsDispatcherEffects::new(Arc::clone(&queue), telegram, redis_client.clone())
                .with_virtual_id_factory(Arc::new(|| "diagnostics-live-vmsg-1".to_owned())),
        );
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = DiagnosticsCommandUpdateHandler::new(bot(), effects, Arc::clone(&next));

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let update = update_from_message(group_command_message_at(
            "/debug@PlotvaBot",
            16,
            Some(9),
            now_secs as i64,
        )?)?;
        update_queue.enqueue_update(&update).await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded diagnostics update"))?;

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
            |update| handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 500);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!report.skipped_handle);
        assert_eq!(
            state_store.calls(),
            vec!["chat:-42:supergroup::".to_owned(), "user:5:Ada:".to_owned()]
        );
        assert_eq!(next.handled_count(), 0);
        let item = queue
            .dequeue_immediate()
            .ok_or_else(|| io::Error::other("expected debug error payload"))?;
        assert_eq!(item.metadata().virtual_id, "diagnostics-live-vmsg-1");
        assert_eq!(
            item.method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        assert_eq!(item.ephemeral_delete_after(), Some(ERROR_DELETE_AFTER));
        let value = method_as_value(
            item.into_method()
                .ok_or_else(|| io::Error::other("expected sendMessage method"))?,
        )?;
        assert_eq!(value["chat_id"], serde_json::json!(-42));
        assert_eq!(value["text"], serde_json::json!(DEBUG_NEEDS_REPLY_TEXT));
        assert_eq!(
            value["reply_parameters"]["message_id"],
            serde_json::json!(10)
        );
        assert_eq!(value["reply_parameters"]["chat_id"], serde_json::json!(-42));
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            serde_json::json!(true)
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_ping_queues_start_and_false_result_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-ping:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let telegram = openplotva_telegram::telegram_client("123456:TEST")?;
        let next_id = Arc::new(AtomicUsize::new(0));
        let effects = Arc::new(
            DiagnosticsDispatcherEffects::new(Arc::clone(&queue), telegram, redis_client.clone())
                .with_virtual_id_factory(Arc::new(move || {
                    let id = next_id.fetch_add(1, Ordering::Relaxed) + 1;
                    format!("diagnostics-ping-live-vmsg-{id}")
                })),
        );
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = DiagnosticsCommandUpdateHandler::new(bot(), effects, Arc::clone(&next));

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let update = update_from_message(group_command_message_at(
            "/ping@PlotvaBot",
            15,
            Some(9),
            now_secs as i64,
        )?)?;
        update_queue.enqueue_update(&update).await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded diagnostics ping update"))?;

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
            |update| handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 500);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!report.skipped_handle);
        assert_eq!(
            state_store.calls(),
            vec!["chat:-42:supergroup::".to_owned(), "user:5:Ada:".to_owned()]
        );
        assert_eq!(next.handled_count(), 0);

        for _ in 0..80 {
            if queue.snapshot().regular.len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let start = queue
            .dequeue_regular()
            .ok_or_else(|| io::Error::other("expected ping start payload"))?;
        assert_eq!(start.metadata().virtual_id, "diagnostics-ping-live-vmsg-1");
        let start_value = method_as_value(
            start
                .into_method()
                .ok_or_else(|| io::Error::other("expected ping start sendMessage"))?,
        )?;
        assert_eq!(start_value["chat_id"], serde_json::json!(-42));
        assert_eq!(start_value["text"], serde_json::json!(PING_START_TEXT));
        assert_eq!(
            start_value["reply_parameters"]["message_id"],
            serde_json::json!(10)
        );

        let result = queue
            .dequeue_regular()
            .ok_or_else(|| io::Error::other("expected ping result payload"))?;
        assert_eq!(result.metadata().virtual_id, "diagnostics-ping-live-vmsg-2");
        let result_value = method_as_value(
            result
                .into_method()
                .ok_or_else(|| io::Error::other("expected ping result sendMessage"))?,
        )?;
        assert_eq!(result_value["chat_id"], serde_json::json!(-42));
        assert_eq!(result_value["text"], serde_json::json!("Понг! false"));
        assert_eq!(
            result_value["reply_parameters"]["message_id"],
            serde_json::json!(10)
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[derive(Clone, Default)]
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
                    .push(format!(
                        "chat:{}:{}:{}:{}",
                        chat.id,
                        chat.chat_type,
                        chat.first_name.as_deref().unwrap_or_default(),
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
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!(
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
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!("file:{}", params.file_unique_id));
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct EffectsStub {
        pings: Mutex<Vec<PingSequencePlan>>,
        texts: Mutex<Vec<DiagnosticsTextPlan>>,
    }

    impl EffectsStub {
        fn pings(&self) -> Vec<PingSequencePlan> {
            self.pings.lock().expect("pings").clone()
        }

        fn texts(&self) -> Vec<DiagnosticsTextPlan> {
            self.texts.lock().expect("texts").clone()
        }

        fn lock_pings(&self) -> MutexGuard<'_, Vec<PingSequencePlan>> {
            self.pings.lock().expect("pings")
        }

        fn lock_texts(&self) -> MutexGuard<'_, Vec<DiagnosticsTextPlan>> {
            self.texts.lock().expect("texts")
        }
    }

    impl DiagnosticsCommandEffects for EffectsStub {
        type Error = io::Error;

        fn send_diagnostics_text<'a>(
            &'a self,
            plan: DiagnosticsTextPlan,
        ) -> DiagnosticsEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock_texts().push(plan);
                Ok(())
            })
        }

        fn start_ping_sequence<'a>(
            &'a self,
            plan: PingSequencePlan,
        ) -> DiagnosticsEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock_pings().push(plan);
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct UpdateHandlerStub {
        handled: Mutex<usize>,
    }

    impl UpdateHandlerStub {
        fn handled_count(&self) -> usize {
            *self.handled.lock().expect("handled")
        }
    }

    impl UpdateHandler for UpdateHandlerStub {
        type Error = io::Error;

        fn handle_update<'a>(
            &'a self,
            _update: TelegramUpdate,
        ) -> UpdateHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                *self.handled.lock().expect("handled") += 1;
                Ok(())
            })
        }
    }

    fn method_as_value(
        method: openplotva_telegram::TelegramOutboundMethod,
    ) -> io::Result<serde_json::Value> {
        match method {
            openplotva_telegram::TelegramOutboundMethod::SendMessage(method) => {
                serde_json::to_value(method.as_ref()).map_err(io::Error::other)
            }
            other => Err(io::Error::other(format!(
                "unexpected Telegram method: {}",
                other.method_name()
            ))),
        }
    }
}
