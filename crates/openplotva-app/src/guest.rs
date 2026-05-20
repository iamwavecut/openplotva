//! App-level guest-message routing.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, MessageSender as TelegramMessageSender,
    Update as TelegramUpdate, UpdateType, User as TelegramUser,
};
use openplotva_updates::{
    GuestChainMessage, GuestChainRole, build_guest_dialog_text, build_guest_shield_query_text,
    guest_current_request_text, guest_message_reject_reason, guest_request_has_visible_text,
    guest_visible_text, is_guest_unsupported_feature_request,
};
use sha1::{Digest, Sha1};
use thiserror::Error;
use time::OffsetDateTime;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};

pub const GUEST_DIALOG_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(35);
pub const GUEST_DIALOG_MAX_OUTPUT_TOKENS: usize = 512;
pub const GUEST_CHAIN_TTL: Duration = Duration::from_secs(24 * 60 * 60);
pub const GUEST_CHAIN_MAX_MESSAGES: usize = 15;

pub type GuestMessageFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuestChainLoadRequest {
    pub chat_id: i64,
    pub reply_text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuestShieldRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub query: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuestDialogInput {
    pub chat_id: i64,
    pub chat_title: String,
    pub bot_name: String,
    pub locale: String,
    pub user_id: i64,
    pub user_full_name: String,
    pub message_id: i64,
    pub text: String,
    pub normalized: String,
    pub original_text: String,
    pub reply_to_id: i64,
    pub reply_to_name: String,
    pub shield_context: String,
    pub chain: Vec<GuestChainMessage>,
    pub max_output_tokens: usize,
    pub guest_mode: bool,
    pub disable_tools: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GuestDialogOutput {
    pub answer: String,
    pub response: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuestChainRememberRequest {
    pub chat_id: i64,
    pub user_text: String,
    pub user_name: String,
    pub assistant_text: String,
    pub assistant_name: String,
    pub base_chain: Vec<GuestChainMessage>,
}

#[derive(Clone)]
pub struct GuestChainCache {
    inner: Arc<GuestChainCacheInner>,
}

struct GuestChainCacheInner {
    ttl: Duration,
    max_messages: usize,
    entries: Mutex<HashMap<String, GuestChainEntry>>,
    now: Box<dyn Fn() -> OffsetDateTime + Send + Sync>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GuestChainEntry {
    messages: Vec<GuestChainMessage>,
    expires_at: OffsetDateTime,
}

impl Default for GuestChainCache {
    fn default() -> Self {
        Self::new(GUEST_CHAIN_TTL, GUEST_CHAIN_MAX_MESSAGES)
    }
}

impl fmt::Debug for GuestChainCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GuestChainCache")
            .field("ttl", &self.inner.ttl)
            .field("max_messages", &self.inner.max_messages)
            .finish_non_exhaustive()
    }
}

impl GuestChainCache {
    #[must_use]
    pub fn new(ttl: Duration, max_messages: usize) -> Self {
        Self::with_clock(ttl, max_messages, OffsetDateTime::now_utc)
    }

    #[must_use]
    pub fn with_clock(
        ttl: Duration,
        max_messages: usize,
        now: impl Fn() -> OffsetDateTime + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(GuestChainCacheInner {
                ttl: if ttl.is_zero() { GUEST_CHAIN_TTL } else { ttl },
                max_messages: if max_messages == 0 {
                    GUEST_CHAIN_MAX_MESSAGES
                } else {
                    max_messages
                },
                entries: Mutex::new(HashMap::new()),
                now: Box::new(now),
            }),
        }
    }

    #[must_use]
    pub fn load(&self, request: &GuestChainLoadRequest) -> Vec<GuestChainMessage> {
        let Some(key) = guest_chain_key(request.chat_id, &request.reply_text) else {
            return Vec::new();
        };

        let now = self.current_time();
        let mut entries = self.inner.entries.lock().expect("guest chain cache lock");
        let Some(entry) = entries.get(&key).cloned() else {
            return Vec::new();
        };
        if now >= entry.expires_at {
            entries.remove(&key);
            return Vec::new();
        }

        let messages = trim_guest_chain_messages_for_ttl(
            &entry.messages,
            self.inner.max_messages,
            now,
            self.inner.ttl,
        );
        if messages.is_empty() {
            entries.remove(&key);
            return Vec::new();
        }
        messages
    }

    pub fn remember(&self, request: &GuestChainRememberRequest) -> bool {
        let user_text = request.user_text.trim();
        let assistant_text = request.assistant_text.trim();
        if user_text.is_empty() || assistant_text.is_empty() {
            return false;
        }

        let Some(key) = guest_chain_key(request.chat_id, assistant_text) else {
            return false;
        };
        let now = self.current_time();
        let mut messages = trim_guest_chain_messages_for_ttl(
            &request.base_chain,
            self.inner.max_messages,
            now,
            self.inner.ttl,
        );
        messages.push(GuestChainMessage {
            role: GuestChainRole::User,
            name: guest_chain_cache_name(request.user_name.trim(), GuestChainRole::User),
            text: user_text.to_owned(),
            at: Some(now),
        });
        messages.push(GuestChainMessage {
            role: GuestChainRole::Assistant,
            name: guest_chain_cache_name(request.assistant_name.trim(), GuestChainRole::Assistant),
            text: assistant_text.to_owned(),
            at: Some(now),
        });
        let messages = trim_guest_chain_messages_for_ttl(
            &messages,
            self.inner.max_messages,
            now,
            self.inner.ttl,
        );
        if messages.is_empty() {
            return false;
        }

        let mut entries = self.inner.entries.lock().expect("guest chain cache lock");
        prune_expired_guest_chain_entries(&mut entries, now);
        entries.insert(
            key,
            GuestChainEntry {
                messages,
                expires_at: now + self.inner.ttl,
            },
        );
        true
    }

    fn current_time(&self) -> OffsetDateTime {
        (self.inner.now)()
    }
}

pub trait GuestMessageEffects {
    type Error: fmt::Display + Send + Sync + 'static;

    fn answer_guest_html<'a>(
        &'a self,
        request: openplotva_telegram::GuestHtmlAnswerRequest,
    ) -> GuestMessageFuture<'a, (), Self::Error>;

    fn load_guest_chain<'a>(
        &'a self,
        request: GuestChainLoadRequest,
    ) -> GuestMessageFuture<'a, Vec<GuestChainMessage>, Self::Error>;

    fn retrieve_guest_shield_context<'a>(
        &'a self,
        request: GuestShieldRequest,
    ) -> GuestMessageFuture<'a, String, Self::Error>;

    fn run_guest_dialog<'a>(
        &'a self,
        input: GuestDialogInput,
    ) -> GuestMessageFuture<'a, GuestDialogOutput, Self::Error>;

    fn remember_guest_chain_turn<'a>(
        &'a self,
        request: GuestChainRememberRequest,
    ) -> GuestMessageFuture<'a, (), Self::Error>;
}

#[derive(Clone, Debug)]
pub struct GuestMessageConfig {
    pub bot_user: TelegramUser,
    pub shield_query_max_chars: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GuestMessageUpdateRoute {
    Delegated,
    Rejected {
        reason: &'static str,
    },
    EmptyRequest,
    UnsupportedFeature {
        answer_error: Option<String>,
    },
    DialogAnswered {
        answer: String,
        remembered: bool,
        suppressed_errors: Vec<String>,
        answer_error: Option<String>,
    },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GuestMessageUpdateError {
    #[error("downstream update handler: {message}")]
    Downstream { message: String },
}

#[derive(Clone, Debug)]
pub struct GuestMessageUpdateHandler<Effects, Next> {
    effects: Arc<Effects>,
    config: GuestMessageConfig,
    next: Arc<Next>,
}

impl<Effects, Next> GuestMessageUpdateHandler<Effects, Next> {
    pub fn new(effects: Arc<Effects>, config: GuestMessageConfig, next: Arc<Next>) -> Self {
        Self {
            effects,
            config,
            next,
        }
    }
}

impl<Effects, Next> UpdateHandler for GuestMessageUpdateHandler<Effects, Next>
where
    Effects: GuestMessageEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = GuestMessageUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_guest_message_update_or_else(
                self.effects.as_ref(),
                &self.config,
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_guest_message_update_or_else<Effects, HandleFn, HandleFuture, HandleError>(
    effects: &Effects,
    config: &GuestMessageConfig,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<GuestMessageUpdateRoute, GuestMessageUpdateError>
where
    Effects: GuestMessageEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let UpdateType::GuestMessage(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| GuestMessageUpdateError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(GuestMessageUpdateRoute::Delegated);
    };

    if let Some(reason) = guest_message_reject_reason(Some(message), Some(&config.bot_user)) {
        return Ok(GuestMessageUpdateRoute::Rejected {
            reason: reason.as_str(),
        });
    }

    let bot_username = guest_bot_username(&config.bot_user);
    if !guest_request_has_visible_text(Some(message), &bot_username) {
        return Ok(GuestMessageUpdateRoute::EmptyRequest);
    }

    if is_guest_unsupported_feature_request(Some(message), &bot_username) {
        let answer_error = send_guest_html(
            effects,
            unsupported_feature_request(message, &bot_username),
            "answer unsupported guest feature",
        )
        .await
        .err();
        return Ok(GuestMessageUpdateRoute::UnsupportedFeature { answer_error });
    }

    let mut suppressed_errors = Vec::new();
    let chain = suppress_guest_effect(
        effects.load_guest_chain(guest_chain_load_request(message)),
        &mut suppressed_errors,
        "load guest chain",
    )
    .await
    .unwrap_or_default();
    let shield_context = suppress_guest_effect(
        effects.retrieve_guest_shield_context(guest_shield_request(
            message,
            config.shield_query_max_chars,
            &chain,
        )),
        &mut suppressed_errors,
        "retrieve guest shield context",
    )
    .await
    .unwrap_or_default();

    let dialog_input = guest_dialog_input(message, &config.bot_user, shield_context, chain.clone());
    let dialog_output = suppress_guest_effect(
        effects.run_guest_dialog(dialog_input),
        &mut suppressed_errors,
        "run guest dialog",
    )
    .await
    .unwrap_or_default();
    let mut answer = dialog_output.answer.trim().to_owned();
    if answer.is_empty() {
        answer = dialog_output.response.trim().to_owned();
    }
    if answer.is_empty() {
        answer = openplotva_telegram::guest_dialog_fallback_html(&bot_username);
    }

    let answer_error = send_guest_html(
        effects,
        dialog_answer_request(message, &bot_username, &answer),
        "answer guest dialog",
    )
    .await
    .err();
    let remembered = if answer_error.is_none() {
        let request = guest_chain_remember_request(message, &config.bot_user, &chain, &answer);
        suppress_guest_effect(
            effects.remember_guest_chain_turn(request),
            &mut suppressed_errors,
            "remember guest chain turn",
        )
        .await
        .is_some()
    } else {
        false
    };

    Ok(GuestMessageUpdateRoute::DialogAnswered {
        answer,
        remembered,
        suppressed_errors,
        answer_error,
    })
}

fn unsupported_feature_request(
    message: &TelegramMessage,
    bot_username: &str,
) -> openplotva_telegram::GuestHtmlAnswerRequest {
    openplotva_telegram::GuestHtmlAnswerRequest {
        guest_query_id: message.guest_query_id.clone().unwrap_or_default(),
        message_id: message.id,
        title: "Добавьте Плотву в чат".to_owned(),
        html_text: openplotva_telegram::guest_unsupported_feature_html(bot_username),
        bot_username: bot_username.to_owned(),
        reply_markup: Some(openplotva_telegram::build_guest_add_to_chat_markup(
            bot_username,
        )),
    }
}

fn dialog_answer_request(
    message: &TelegramMessage,
    bot_username: &str,
    answer: &str,
) -> openplotva_telegram::GuestHtmlAnswerRequest {
    openplotva_telegram::GuestHtmlAnswerRequest {
        guest_query_id: message.guest_query_id.clone().unwrap_or_default(),
        message_id: message.id,
        title: "Плотва отвечает".to_owned(),
        html_text: answer.to_owned(),
        bot_username: bot_username.to_owned(),
        reply_markup: None,
    }
}

fn guest_chain_load_request(message: &TelegramMessage) -> GuestChainLoadRequest {
    let reply_text = reply_message(message)
        .map(|reply| guest_visible_text(Some(reply)))
        .unwrap_or_default();
    GuestChainLoadRequest {
        chat_id: message.chat.get_id().into(),
        reply_text,
    }
}

fn guest_shield_request(
    message: &TelegramMessage,
    max_chars: usize,
    chain: &[GuestChainMessage],
) -> GuestShieldRequest {
    GuestShieldRequest {
        chat_id: message.chat.get_id().into(),
        message_id: message.id,
        query: build_guest_shield_query_text(Some(message), max_chars, chain),
    }
}

fn guest_dialog_input(
    message: &TelegramMessage,
    bot_user: &TelegramUser,
    shield_context: String,
    chain: Vec<GuestChainMessage>,
) -> GuestDialogInput {
    let bot_username = guest_bot_username(bot_user);
    let caller = guest_caller_user(message).or_else(|| message.sender.get_user().cloned());
    let (user_id, user_full_name, locale) = match caller.as_ref() {
        Some(user) => (
            user.id.into(),
            telegram_user_display_name(user),
            user.language_code
                .as_deref()
                .map(str::trim)
                .filter(|locale| !locale.is_empty())
                .unwrap_or("ru")
                .to_owned(),
        ),
        None => (0, "Telegram".to_owned(), "ru".to_owned()),
    };
    let (reply_to_id, reply_to_name) = reply_message(message).map_or((0, String::new()), |reply| {
        (reply.id, telegram_message_sender_name(reply))
    });

    GuestDialogInput {
        chat_id: message.chat.get_id().into(),
        chat_title: telegram_chat_title(&message.chat),
        bot_name: guest_bot_name(bot_user),
        locale,
        user_id,
        user_full_name,
        message_id: message.id,
        text: build_guest_dialog_text(Some(message), &bot_username, &chain),
        normalized: guest_current_request_text(Some(message), &bot_username),
        original_text: guest_visible_text(Some(message)),
        reply_to_id,
        reply_to_name,
        shield_context: shield_context.trim().to_owned(),
        chain,
        max_output_tokens: GUEST_DIALOG_MAX_OUTPUT_TOKENS,
        guest_mode: true,
        disable_tools: true,
    }
}

fn guest_chain_remember_request(
    message: &TelegramMessage,
    bot_user: &TelegramUser,
    base_chain: &[GuestChainMessage],
    answer: &str,
) -> GuestChainRememberRequest {
    let bot_username = guest_bot_username(bot_user);
    let mut user_text = guest_current_request_text(Some(message), &bot_username);
    if user_text.trim().is_empty() {
        user_text = guest_visible_text(Some(message));
    }
    if user_text.trim().is_empty() {
        user_text = reply_message(message)
            .map(|reply| guest_visible_text(Some(reply)))
            .unwrap_or_default();
    }

    GuestChainRememberRequest {
        chat_id: message.chat.get_id().into(),
        user_text: user_text.trim().to_owned(),
        user_name: guest_chain_user_name(message),
        assistant_text: guest_chain_assistant_text(answer, &bot_username),
        assistant_name: guest_chain_assistant_name(bot_user),
        base_chain: base_chain.to_vec(),
    }
}

async fn send_guest_html<Effects>(
    effects: &Effects,
    request: openplotva_telegram::GuestHtmlAnswerRequest,
    context: &'static str,
) -> Result<(), String>
where
    Effects: GuestMessageEffects + Sync,
{
    effects.answer_guest_html(request).await.map_err(|error| {
        let message = error.to_string();
        tracing::warn!(message, context, "guest answer failed");
        message
    })
}

async fn suppress_guest_effect<T, E>(
    effect: GuestMessageFuture<'_, T, E>,
    suppressed_errors: &mut Vec<String>,
    context: &'static str,
) -> Option<T>
where
    E: fmt::Display,
{
    match effect.await {
        Ok(value) => Some(value),
        Err(error) => {
            let message = error.to_string();
            tracing::warn!(message, context, "guest effect failed");
            suppressed_errors.push(message);
            None
        }
    }
}

fn reply_message(message: &TelegramMessage) -> Option<&TelegramMessage> {
    let carapax::types::ReplyTo::Message(reply) = message.reply_to.as_ref()? else {
        return None;
    };
    Some(reply)
}

fn guest_bot_username(bot_user: &TelegramUser) -> String {
    bot_user
        .username
        .as_ref()
        .map(ToString::to_string)
        .map(|username| username.trim().to_owned())
        .filter(|username| !username.is_empty())
        .unwrap_or_else(|| openplotva_telegram::DEFAULT_GUEST_BOT_USERNAME.to_owned())
        .trim_start_matches('@')
        .to_owned()
}

fn guest_bot_name(bot_user: &TelegramUser) -> String {
    let name = bot_user.first_name.trim();
    if name.is_empty() {
        "Plotva".to_owned()
    } else {
        name.to_owned()
    }
}

fn guest_caller_user(message: &TelegramMessage) -> Option<TelegramUser> {
    let guest_bot = message.guest_bot.as_ref()?;
    let value = serde_json::to_value(guest_bot).ok()?;
    serde_json::from_value(value.get("guest_bot_caller_user")?.clone()).ok()
}

fn telegram_user_display_name(user: &TelegramUser) -> String {
    let name = format!(
        "{} {}",
        user.first_name.trim(),
        user.last_name.as_deref().unwrap_or_default().trim()
    )
    .trim()
    .to_owned();
    if !name.is_empty() {
        return name;
    }
    if let Some(username) = user
        .username
        .as_ref()
        .map(ToString::to_string)
        .map(|username| username.trim().to_owned())
        && !username.is_empty()
    {
        return username;
    }
    let id: i64 = user.id.into();
    if id != 0 {
        return id.to_string();
    }
    "Telegram".to_owned()
}

fn telegram_message_sender_name(message: &TelegramMessage) -> String {
    match &message.sender {
        TelegramMessageSender::User(user) => telegram_user_display_name(user),
        TelegramMessageSender::Chat(chat) => telegram_chat_title(chat),
        TelegramMessageSender::Unknown => String::new(),
    }
}

fn telegram_chat_title(chat: &TelegramChat) -> String {
    match chat {
        TelegramChat::Channel(chat) => chat.title.trim().to_owned(),
        TelegramChat::Group(chat) => chat.title.trim().to_owned(),
        TelegramChat::Supergroup(chat) => chat.title.trim().to_owned(),
        TelegramChat::Private(_) => String::new(),
    }
}

fn guest_chain_user_name(message: &TelegramMessage) -> String {
    guest_caller_user(message)
        .as_ref()
        .or_else(|| message.sender.get_user())
        .map(telegram_user_display_name)
        .unwrap_or_else(|| "Telegram".to_owned())
}

fn guest_chain_assistant_name(bot_user: &TelegramUser) -> String {
    let first_name = bot_user.first_name.trim();
    if !first_name.is_empty() {
        return first_name.to_owned();
    }
    let username = guest_bot_username(bot_user);
    if username.is_empty() {
        "Plotva".to_owned()
    } else {
        username
    }
}

fn guest_chain_assistant_text(answer: &str, bot_username: &str) -> String {
    let mut prepared = openplotva_telegram::prepare_guest_html(answer);
    if prepared.is_empty() {
        prepared = openplotva_telegram::prepare_guest_html(
            &openplotva_telegram::guest_dialog_fallback_html(bot_username),
        );
    }
    openplotva_telegram::strip_telegram_html(&prepared)
        .trim()
        .to_owned()
}

fn trim_guest_chain_messages_for_ttl(
    messages: &[GuestChainMessage],
    max_messages: usize,
    now: OffsetDateTime,
    ttl: Duration,
) -> Vec<GuestChainMessage> {
    let max_messages = if max_messages == 0 {
        GUEST_CHAIN_MAX_MESSAGES
    } else {
        max_messages
    };
    let cutoff = now - ttl;
    let mut out = Vec::with_capacity(messages.len().min(max_messages));
    for message in messages {
        if let Some(normalized) = normalize_guest_chain_cache_message(message, cutoff) {
            out.push(normalized);
        }
    }
    if out.len() <= max_messages {
        return out;
    }
    out[out.len() - max_messages..].to_vec()
}

fn normalize_guest_chain_cache_message(
    message: &GuestChainMessage,
    cutoff: OffsetDateTime,
) -> Option<GuestChainMessage> {
    if message.at.is_some_and(|at| at < cutoff) {
        return None;
    }
    let text = message.text.trim();
    if text.is_empty() {
        return None;
    }
    Some(GuestChainMessage {
        role: message.role,
        name: guest_chain_cache_name(message.name.trim(), message.role),
        text: text.to_owned(),
        at: message.at,
    })
}

fn prune_expired_guest_chain_entries(
    entries: &mut HashMap<String, GuestChainEntry>,
    now: OffsetDateTime,
) {
    entries.retain(|_, entry| now < entry.expires_at);
}

fn guest_chain_cache_name(name: &str, role: GuestChainRole) -> String {
    if !name.is_empty() {
        return name.to_owned();
    }
    match role {
        GuestChainRole::Assistant => "Plotva".to_owned(),
        GuestChainRole::User => "Telegram".to_owned(),
    }
}

fn guest_chain_key(chat_id: i64, text: &str) -> Option<String> {
    let normalized = guest_chain_key_text(text);
    if normalized.is_empty() {
        return None;
    }
    let mut hasher = Sha1::new();
    hasher.update(normalized.as_bytes());
    Some(format!("{chat_id}:{}", hex::encode(hasher.finalize())))
}

fn guest_chain_key_text(text: &str) -> String {
    compact_whitespace(&openplotva_telegram::strip_telegram_html(text))
}

fn compact_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        io,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use carapax::types::{Update as TelegramUpdate, User as TelegramUser};
    use openplotva_updates::{GuestChainMessage, GuestChainRole};
    use serde_json::json;
    use time::macros::datetime;

    use crate::updates::{UpdateHandler, UpdateHandlerFuture};

    use super::{
        GuestChainCache, GuestChainLoadRequest, GuestChainRememberRequest, GuestDialogInput,
        GuestDialogOutput, GuestMessageConfig, GuestMessageEffects, GuestMessageFuture,
        GuestMessageUpdateHandler, GuestMessageUpdateRoute, GuestShieldRequest,
        handle_guest_message_update_or_else,
    };

    #[tokio::test]
    async fn guest_message_rejects_missing_query_before_effects() -> Result<(), Box<dyn Error>> {
        let effects = GuestEffectsStub::default();
        let next = UpdateHandlerStub::default();
        let route = handle_guest_message_update_or_else(
            &effects,
            &guest_config()?,
            guest_update_without_query()?,
            |update| next.handle_update(update),
        )
        .await?;
        assert_eq!(
            route,
            GuestMessageUpdateRoute::Rejected {
                reason: "missing_guest_query_id"
            }
        );
        assert!(effects.answers().is_empty());
        assert_eq!(next.handled_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn guest_unsupported_feature_sends_add_to_chat_answer() -> Result<(), Box<dyn Error>> {
        let effects = GuestEffectsStub::default();
        let next = UpdateHandlerStub::default();
        let route = handle_guest_message_update_or_else(
            &effects,
            &guest_config()?,
            guest_update("нарисуй кота")?,
            |update| next.handle_update(update),
        )
        .await?;
        assert_eq!(
            route,
            GuestMessageUpdateRoute::UnsupportedFeature { answer_error: None }
        );
        let answers = effects.answers();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].title, "Добавьте Плотву в чат");
        assert!(answers[0].html_text.contains("Некоторые функции Плотвы"));
        assert!(answers[0].reply_markup.is_some());
        assert!(effects.dialog_inputs().is_empty());
        assert_eq!(next.handled_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn guest_dialog_runs_shield_answers_and_remembers_chain() -> Result<(), Box<dyn Error>> {
        let effects = GuestEffectsStub::default()
            .with_chain(vec![GuestChainMessage {
                role: GuestChainRole::Assistant,
                name: "Plotva".to_owned(),
                text: "старый ответ".to_owned(),
                at: None,
            }])
            .with_dialog_answer("новый <b>ответ</b>");
        let next = UpdateHandlerStub::default();
        let route = handle_guest_message_update_or_else(
            &effects,
            &guest_config()?,
            guest_reply_update("@PlotvaBot привет", "старый ответ")?,
            |update| next.handle_update(update),
        )
        .await?;
        assert_eq!(
            route,
            GuestMessageUpdateRoute::DialogAnswered {
                answer: "новый <b>ответ</b>".to_owned(),
                remembered: true,
                suppressed_errors: vec![],
                answer_error: None,
            }
        );
        assert_eq!(
            effects.loads(),
            vec![GuestChainLoadRequest {
                chat_id: 42,
                reply_text: "старый ответ".to_owned(),
            }]
        );
        let shield = effects.shield_requests();
        assert_eq!(shield.len(), 1);
        assert!(shield[0].query.contains("current: @PlotvaBot привет"));
        assert!(shield[0].query.contains("chain: Plotva: старый ответ"));
        let input = effects.dialog_inputs().pop().expect("dialog input");
        assert_eq!(input.normalized, "привет");
        assert!(input.text.contains("Гостевая цепочка за последние сутки"));
        assert_eq!(input.user_full_name, "Ada Lovelace");
        assert_eq!(input.locale, "en");
        assert_eq!(input.reply_to_name, "Grace");
        assert_eq!(effects.answers()[0].title, "Плотва отвечает");
        let remembered = effects.remembered().pop().expect("remembered chain");
        assert_eq!(remembered.user_text, "привет");
        assert_eq!(remembered.assistant_text, "новый ответ");
        assert_eq!(next.handled_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn guest_dialog_error_falls_back_and_answer_error_skips_remember()
    -> Result<(), Box<dyn Error>> {
        let effects = GuestEffectsStub::default()
            .failing_dialog()
            .failing_answers();
        let next = UpdateHandlerStub::default();
        let route = handle_guest_message_update_or_else(
            &effects,
            &guest_config()?,
            guest_update("привет")?,
            |update| next.handle_update(update),
        )
        .await?;
        let GuestMessageUpdateRoute::DialogAnswered {
            answer,
            remembered,
            suppressed_errors,
            answer_error,
        } = route
        else {
            panic!("unexpected route")
        };
        assert!(answer.contains("Не успела ответить"));
        assert!(!remembered);
        assert_eq!(suppressed_errors, vec!["dialog failed".to_owned()]);
        assert_eq!(answer_error, Some("answer failed".to_owned()));
        assert!(effects.remembered().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn non_guest_update_delegates_once() -> Result<(), Box<dyn Error>> {
        let effects = GuestEffectsStub::default();
        let next = UpdateHandlerStub::default();
        let route = handle_guest_message_update_or_else(
            &effects,
            &guest_config()?,
            text_update()?,
            |update| next.handle_update(update),
        )
        .await?;
        assert_eq!(route, GuestMessageUpdateRoute::Delegated);
        assert_eq!(next.handled_count(), 1);
        assert!(effects.answers().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn handler_adapter_consumes_guest_messages() -> Result<(), Box<dyn Error>> {
        let effects = Arc::new(GuestEffectsStub::default().with_dialog_answer("ответ"));
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = GuestMessageUpdateHandler::new(Arc::clone(&effects), guest_config()?, next);
        handler.handle_update(guest_update("привет")?).await?;
        assert_eq!(effects.answers().len(), 1);
        Ok(())
    }

    #[test]
    fn guest_chain_cache_loads_by_reply_answer_key_and_trims_to_go_limit() {
        let now = datetime!(2026-05-20 12:00 UTC);
        let cache = GuestChainCache::with_clock(Duration::from_secs(24 * 60 * 60), 15, move || now);
        let mut base_chain = Vec::new();
        for index in 0..14 {
            base_chain.push(GuestChainMessage {
                role: GuestChainRole::User,
                name: format!("user {index}"),
                text: format!("old {index}"),
                at: Some(now),
            });
        }
        let stored = cache.remember(&GuestChainRememberRequest {
            chat_id: -100,
            user_text: " вопрос ".to_owned(),
            user_name: " Alice ".to_owned(),
            assistant_text: " <b>Ответ</b>   с   пробелами ".to_owned(),
            assistant_name: " Plotva ".to_owned(),
            base_chain,
        });
        assert!(stored);

        let loaded = cache.load(&GuestChainLoadRequest {
            chat_id: -100,
            reply_text: "Ответ с пробелами".to_owned(),
        });
        assert_eq!(loaded.len(), 15);
        assert_eq!(loaded[0].text, "old 1");
        assert_eq!(loaded[13].text, "вопрос");
        assert_eq!(loaded[13].name, "Alice");
        assert_eq!(loaded[14].text, "<b>Ответ</b>   с   пробелами");
        assert_eq!(loaded[14].name, "Plotva");
        assert_eq!(loaded[14].at, Some(now));
    }

    #[test]
    fn guest_chain_cache_expires_entries_and_drops_stale_messages() {
        let now = datetime!(2026-05-20 12:00 UTC);
        let cache = GuestChainCache::with_clock(Duration::from_secs(24 * 60 * 60), 15, move || now);
        assert!(cache.remember(&GuestChainRememberRequest {
            chat_id: 42,
            user_text: "fresh".to_owned(),
            user_name: String::new(),
            assistant_text: "answer".to_owned(),
            assistant_name: String::new(),
            base_chain: vec![GuestChainMessage {
                role: GuestChainRole::User,
                name: String::new(),
                text: "too old".to_owned(),
                at: Some(now - Duration::from_secs(25 * 60 * 60)),
            }],
        }));

        let loaded = cache.load(&GuestChainLoadRequest {
            chat_id: 42,
            reply_text: "answer".to_owned(),
        });
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].name, "Telegram");
        assert_eq!(loaded[1].name, "Plotva");

        let clock = Arc::new(Mutex::new(now));
        let cache_clock = Arc::clone(&clock);
        let expired_cache = GuestChainCache::with_clock(Duration::from_secs(1), 15, move || {
            *cache_clock.lock().expect("clock")
        });
        assert!(expired_cache.remember(&GuestChainRememberRequest {
            chat_id: 42,
            user_text: "fresh".to_owned(),
            user_name: "Alice".to_owned(),
            assistant_text: "short".to_owned(),
            assistant_name: "Plotva".to_owned(),
            base_chain: Vec::new(),
        }));
        *clock.lock().expect("clock") = now + Duration::from_secs(1);
        assert!(
            expired_cache
                .load(&GuestChainLoadRequest {
                    chat_id: 42,
                    reply_text: "short".to_owned(),
                })
                .is_empty()
        );
    }

    #[test]
    fn guest_chain_cache_rejects_empty_key_or_turn_text() {
        let cache = GuestChainCache::default();
        assert!(!cache.remember(&GuestChainRememberRequest {
            chat_id: 42,
            user_text: " ".to_owned(),
            user_name: "Alice".to_owned(),
            assistant_text: "answer".to_owned(),
            assistant_name: "Plotva".to_owned(),
            base_chain: Vec::new(),
        }));
        assert!(
            cache
                .load(&GuestChainLoadRequest {
                    chat_id: 42,
                    reply_text: " <b> </b> ".to_owned(),
                })
                .is_empty()
        );
    }

    #[derive(Clone, Debug, Default)]
    struct GuestEffectsStub {
        state: Arc<Mutex<GuestEffectsState>>,
    }

    #[derive(Debug, Default)]
    struct GuestEffectsState {
        answers: Vec<openplotva_telegram::GuestHtmlAnswerRequest>,
        loads: Vec<GuestChainLoadRequest>,
        shield_requests: Vec<GuestShieldRequest>,
        dialog_inputs: Vec<GuestDialogInput>,
        remembered: Vec<GuestChainRememberRequest>,
        chain: Vec<GuestChainMessage>,
        dialog_answer: String,
        fail_dialog: bool,
        fail_answers: bool,
    }

    impl GuestEffectsStub {
        fn with_chain(self, chain: Vec<GuestChainMessage>) -> Self {
            self.state.lock().expect("state").chain = chain;
            self
        }

        fn with_dialog_answer(self, answer: &str) -> Self {
            self.state.lock().expect("state").dialog_answer = answer.to_owned();
            self
        }

        fn failing_dialog(self) -> Self {
            self.state.lock().expect("state").fail_dialog = true;
            self
        }

        fn failing_answers(self) -> Self {
            self.state.lock().expect("state").fail_answers = true;
            self
        }

        fn answers(&self) -> Vec<openplotva_telegram::GuestHtmlAnswerRequest> {
            self.state.lock().expect("state").answers.clone()
        }

        fn loads(&self) -> Vec<GuestChainLoadRequest> {
            self.state.lock().expect("state").loads.clone()
        }

        fn shield_requests(&self) -> Vec<GuestShieldRequest> {
            self.state.lock().expect("state").shield_requests.clone()
        }

        fn dialog_inputs(&self) -> Vec<GuestDialogInput> {
            self.state.lock().expect("state").dialog_inputs.clone()
        }

        fn remembered(&self) -> Vec<GuestChainRememberRequest> {
            self.state.lock().expect("state").remembered.clone()
        }
    }

    impl GuestMessageEffects for GuestEffectsStub {
        type Error = io::Error;

        fn answer_guest_html<'a>(
            &'a self,
            request: openplotva_telegram::GuestHtmlAnswerRequest,
        ) -> GuestMessageFuture<'a, (), Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("state");
                state.answers.push(request);
                if state.fail_answers {
                    return Err(io::Error::other("answer failed"));
                }
                Ok(())
            })
        }

        fn load_guest_chain<'a>(
            &'a self,
            request: GuestChainLoadRequest,
        ) -> GuestMessageFuture<'a, Vec<GuestChainMessage>, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("state");
                state.loads.push(request);
                Ok(state.chain.clone())
            })
        }

        fn retrieve_guest_shield_context<'a>(
            &'a self,
            request: GuestShieldRequest,
        ) -> GuestMessageFuture<'a, String, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("state");
                state.shield_requests.push(request);
                Ok("shield context".to_owned())
            })
        }

        fn run_guest_dialog<'a>(
            &'a self,
            input: GuestDialogInput,
        ) -> GuestMessageFuture<'a, GuestDialogOutput, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("state");
                state.dialog_inputs.push(input);
                if state.fail_dialog {
                    return Err(io::Error::other("dialog failed"));
                }
                Ok(GuestDialogOutput {
                    answer: state.dialog_answer.clone(),
                    response: String::new(),
                })
            })
        }

        fn remember_guest_chain_turn<'a>(
            &'a self,
            request: GuestChainRememberRequest,
        ) -> GuestMessageFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state.lock().expect("state").remembered.push(request);
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct UpdateHandlerStub {
        handled: Arc<Mutex<usize>>,
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

    fn guest_config() -> Result<GuestMessageConfig, serde_json::Error> {
        Ok(GuestMessageConfig {
            bot_user: sample_bot_user()?,
            shield_query_max_chars: 500,
        })
    }

    fn sample_bot_user() -> Result<TelegramUser, serde_json::Error> {
        serde_json::from_value(json!({
            "id": 777,
            "is_bot": true,
            "first_name": "Плотва",
            "username": "PlotvaBot"
        }))
    }

    fn text_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 300,
            "message": {
                "message_id": 1,
                "date": 1_710_000_000,
                "chat": {"id": 42, "type": "private", "first_name": "Ada"},
                "from": sample_user_json(),
                "text": "hello"
            }
        }))
    }

    fn guest_update(text: &str) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 301,
            "guest_message": guest_message_json(text)
        }))
    }

    fn guest_update_without_query() -> Result<TelegramUpdate, serde_json::Error> {
        let mut message = guest_message_json("hello");
        message
            .as_object_mut()
            .expect("object")
            .remove("guest_query_id");
        serde_json::from_value(json!({
            "update_id": 302,
            "guest_message": message
        }))
    }

    fn guest_reply_update(
        text: &str,
        reply_text: &str,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let mut message = guest_message_json(text);
        message["reply_to_message"] = json!({
            "message_id": 70,
            "date": 1_710_000_000,
            "chat": {"id": 42, "type": "private", "first_name": "Ada"},
            "from": {
                "id": 222,
                "is_bot": false,
                "first_name": "Grace"
            },
            "text": reply_text
        });
        serde_json::from_value(json!({
            "update_id": 303,
            "guest_message": message
        }))
    }

    fn guest_message_json(text: &str) -> serde_json::Value {
        json!({
            "message_id": 77,
            "date": 1_710_000_000,
            "guest_query_id": "guest-query",
            "chat": {"id": 42, "type": "private", "first_name": "Ada"},
            "from": sample_user_json(),
            "text": text,
            "guest_bot": {
                "guest_bot_caller_user": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada",
                    "last_name": "Lovelace",
                    "language_code": "en"
                }
            }
        })
    }

    fn sample_user_json() -> serde_json::Value {
        json!({
            "id": 111,
            "is_bot": false,
            "first_name": "Ada",
            "last_name": "Lovelace",
            "username": "ada",
            "language_code": "en"
        })
    }
}
