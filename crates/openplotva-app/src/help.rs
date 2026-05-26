//! App-level fetcher help and `/start` command planning.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, MessageData as TelegramMessageData,
    Text as TelegramText, TextEntity as TelegramTextEntity,
    TextEntityPosition as TelegramTextEntityPosition, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType,
};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, HELP_COMMAND, OutboundBuildError, ReplyMarkup, ReplyMessageRef,
    TELEGRAM_PARSE_MODE_HTML, TelegramClient, TelegramOutboundMethod, TextMessageRequest,
    build_inline_keyboard_button_url, build_inline_keyboard_markup, build_inline_keyboard_row,
    build_text_message_methods, escape_telegram_html_text, execute_telegram_method,
};
use thiserror::Error;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use crate::virtual_messages::{
    QueueTextRequest, VirtualIdFactory, VirtualMessageStore, monotonic_virtual_id_factory,
    queue_text_message_parts,
};

const START_COMMAND: &str = "start";
const SETTINGS_START_PAYLOAD: &str = "settings";
const VIP_START_PAYLOAD: &str = "vip";
const DONATE_START_PAYLOAD: &str = "donate";
const DONATE_START_PAYLOAD_PREFIX: &str = "donate_";
const HELP_INTRO_BUTTON_TEXT: &str = "📖 Помощь";
const HELP_REDIRECT_BUTTON_TEXT: &str = "📖 Открыть справку";
const FALLBACK_BOT_NAME: &str = "бот";

pub const HELP_INTRO_MESSAGE_TEMPLATE: &str = "Привет! Я {{.BotName}} — я тут для общения, рисования и полезных задач. Хочешь познакомиться с возможностями? Нажми кнопку ниже.";

pub const HELP_REDIRECT_MESSAGE: &str =
    "Чтобы получить справку, откройте личный чат со мной и нажмите кнопку ниже.";

pub const HELP_MESSAGE_TEMPLATE: &str = r#"<b>Привет! Я {{.BotName}}</b> — я тут для общения, рисунков и полезных задач. Я стараюсь быть живой и понятной.

<b>Как со мной общаться:</b>
• В личке отвечаю всегда — просто напиши, как человеку.
• В группе можно написать «{{.BotName}}, ...» или ответить на любое моё сообщение.
• Я могу иногда отвечать случайно на сообщения в группах — это настраивается в /settings (реактивность).

<b>Рисование и правки:</b>
Я могу рисовать по твоим запросам: <code>{{.BotName}}, нарисуй кота в космосе</code>
Правки: ответь на картинку или пришли новую и напиши, что изменить.
Плюс: я улучшаю описания сама, этого не видно. Минус: есть очереди и лимиты, чтобы всем хватало ресурсов.
Общие ограничения: примерно 5/10/20 картинок на пользователя в течение окна в 10/15/30 минут.

<b>Полезности:</b>
Ищу в интернете, читаю ссылки, кратко пересказываю YouTube, перевожу тексты и показываю курсы валют. Важный контекст теперь собирается фоном, а не в каждом ответе.

<b>Пересказы чата:</b>
Можешь попросить: «о чём говорили за сутки», «перескажи последние 6 часов» или «что было в последних 300 сообщениях».

<b>Роли и стиль:</b>
Иногда я говорю в разных ролях — это ежедневные, уникальные для чата персонажи. Можно оставить как есть или задать свою персону в /settings.

<b>VIP:</b>
Если хочется быстрее и больше: приоритетная VIP очередь, в 2 раза больше генераций, лучшее качество и генерация песен по команде <code>/song</code>. Оформить можно через /vip.

<b>Быстрый старт:</b>
«Придумай текст для поста», «сделай обложку», «переведи на английский», «расскажи про эту ссылку», «перескажи, что было за день».

Поддержать меня можно через /donate."#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HelpBotIdentity {
    /// Bot first name from `getMe`.
    pub first_name: String,
    /// Bot username from `getMe`.
    pub username: String,
    pub token: String,
}

/// Planned outbound help text.
#[derive(Clone, Debug, PartialEq)]
pub struct HelpTextPlan {
    pub message: TextMessageRequest,
    pub reply_to: ReplyMessageRef,
    pub direct_chattable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrivateStartDelegation {
    Settings,
    Vip,
    Donate,
    DonatePayload(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum HelpCommandRoute {
    /// Message does not belong to this slice.
    NotHandled,
    ConsumedWithoutReply,
    DelegatePrivateStart(PrivateStartDelegation),
    Send(HelpTextPlan),
}

/// Boxed future returned by app help command effects.
pub type HelpCommandEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait HelpCommandEffects {
    /// Concrete effect error.
    type Error: fmt::Display + Send;

    fn send_help_text<'a>(&'a self, plan: HelpTextPlan)
    -> HelpCommandEffectFuture<'a, Self::Error>;

    /// Delegate private `/start ...` payloads to their owning command modules.
    fn delegate_private_start<'a>(
        &'a self,
        message: &'a TelegramMessage,
        delegation: PrivateStartDelegation,
    ) -> HelpCommandEffectFuture<'a, Self::Error>;
}

/// Generator for virtual-message IDs used by queued help text sends.
pub type HelpVirtualIdFactory = VirtualIdFactory;

#[derive(Clone)]
pub struct HelpDispatcherEffects<Store, Next> {
    store: Store,
    queue: Arc<DispatcherQueue>,
    telegram: TelegramClient,
    delegated_next: Arc<Next>,
    next_virtual_id: HelpVirtualIdFactory,
}

impl<Store, Next> HelpDispatcherEffects<Store, Next> {
    /// Build help effects backed by the normal dispatcher and the next command handler.
    #[must_use]
    pub fn new(
        store: Store,
        queue: Arc<DispatcherQueue>,
        telegram: TelegramClient,
        delegated_next: Arc<Next>,
    ) -> Self {
        Self {
            store,
            queue,
            telegram,
            delegated_next,
            next_virtual_id: monotonic_virtual_id_factory("help-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: HelpVirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl<Store, Next> HelpCommandEffects for HelpDispatcherEffects<Store, Next>
where
    Store: VirtualMessageStore + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = HelpDispatchEffectError;

    fn send_help_text<'a>(
        &'a self,
        plan: HelpTextPlan,
    ) -> HelpCommandEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            if plan.direct_chattable {
                let methods = build_text_message_methods(&plan.message, Some(&plan.reply_to))?;
                for method in methods {
                    execute_telegram_method(&self.telegram, TelegramOutboundMethod::from(method))
                        .await
                        .map_err(|error| HelpDispatchEffectError::Direct(error.to_string()))?;
                }
                return Ok(());
            }

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

    fn delegate_private_start<'a>(
        &'a self,
        message: &'a TelegramMessage,
        delegation: PrivateStartDelegation,
    ) -> HelpCommandEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            let update = delegated_private_start_update(message, &delegation);
            self.delegated_next
                .handle_update(update)
                .await
                .map_err(|error| HelpDispatchEffectError::Delegate(error.to_string()))
        })
    }
}

/// Recoverable errors from concrete help effects.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum HelpDispatchEffectError {
    #[error("failed to queue help text: {0}")]
    Queue(#[from] OutboundBuildError),
    /// Direct Telegram `SendChattable`-style send failed.
    #[error("failed to send direct help text: {0}")]
    Direct(String),
    /// Delegating private `/start ...` to the owning command module failed.
    #[error("failed to delegate private start command: {0}")]
    Delegate(String),
}

/// Result of one decoded update through the help/start wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HelpCommandUpdateOutcome {
    /// Update was not handled by this slice and went downstream.
    Delegated,
    ConsumedWithoutReply,
    /// Help text was sent.
    Sent {
        direct_chattable: bool,
    },
    SendError {
        direct_chattable: bool,
        message: String,
    },
    /// Private `/start ...` was delegated to an owning module.
    DelegatedPrivateStart(PrivateStartDelegation),
    DelegatePrivateStartError {
        delegation: PrivateStartDelegation,
        message: String,
    },
}

/// Fatal errors from the help/start update wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum HelpCommandUpdateError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct HelpCommandUpdateHandler<Effects, Next> {
    bot: HelpBotIdentity,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Effects, Next> HelpCommandUpdateHandler<Effects, Next> {
    /// Build a help/start handler around the real downstream update handler.
    pub fn new(bot: HelpBotIdentity, effects: Arc<Effects>, next: Arc<Next>) -> Self {
        Self { bot, effects, next }
    }
}

impl<Effects, Next> UpdateHandler for HelpCommandUpdateHandler<Effects, Next>
where
    Effects: HelpCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = HelpCommandUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_help_command_update_or_else(&self.bot, self.effects.as_ref(), update, |update| {
                self.next.handle_update(update)
            })
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_help_command_update_or_else<Effects, HandleFn, HandleFuture, HandleError>(
    bot: &HelpBotIdentity,
    effects: &Effects,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<HelpCommandUpdateOutcome, HelpCommandUpdateError>
where
    Effects: HelpCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| HelpCommandUpdateError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(HelpCommandUpdateOutcome::Delegated);
    };

    match route_start_or_help_command(bot, message) {
        HelpCommandRoute::NotHandled => {
            handle_other(update)
                .await
                .map_err(|error| HelpCommandUpdateError::Downstream {
                    message: error.to_string(),
                })?;
            Ok(HelpCommandUpdateOutcome::Delegated)
        }
        HelpCommandRoute::ConsumedWithoutReply => {
            Ok(HelpCommandUpdateOutcome::ConsumedWithoutReply)
        }
        HelpCommandRoute::Send(plan) => {
            let direct_chattable = plan.direct_chattable;
            match effects.send_help_text(plan).await {
                Ok(()) => Ok(HelpCommandUpdateOutcome::Sent { direct_chattable }),
                Err(error) => Ok(HelpCommandUpdateOutcome::SendError {
                    direct_chattable,
                    message: error.to_string(),
                }),
            }
        }
        HelpCommandRoute::DelegatePrivateStart(delegation) => {
            match effects
                .delegate_private_start(message, delegation.clone())
                .await
            {
                Ok(()) => Ok(HelpCommandUpdateOutcome::DelegatedPrivateStart(delegation)),
                Err(error) => Ok(HelpCommandUpdateOutcome::DelegatePrivateStartError {
                    delegation,
                    message: error.to_string(),
                }),
            }
        }
    }
}

#[must_use]
pub fn resolve_help_bot_name(bot: &HelpBotIdentity) -> String {
    let first_name = bot.first_name.trim();
    if !first_name.is_empty() {
        return first_name.to_owned();
    }
    let username = bot.username.trim();
    if !username.is_empty() {
        return username.to_owned();
    }
    FALLBACK_BOT_NAME.to_owned()
}

#[must_use]
pub fn help_deep_link(bot: &HelpBotIdentity) -> Option<String> {
    (!bot.username.is_empty())
        .then(|| format!("https://t.me/{}?start={HELP_COMMAND}", bot.username))
}

#[must_use]
pub fn render_help_intro_text(bot: &HelpBotIdentity) -> String {
    render_help_template(HELP_INTRO_MESSAGE_TEMPLATE, bot)
}

#[must_use]
pub fn render_help_message_html(bot: &HelpBotIdentity) -> String {
    render_help_template(HELP_MESSAGE_TEMPLATE, bot)
}

#[must_use]
pub fn help_intro_plan(bot: &HelpBotIdentity, message: &TelegramMessage) -> HelpTextPlan {
    help_text_plan(
        render_help_intro_text(bot),
        String::new(),
        help_deep_link(bot).map(|url| help_markup(HELP_INTRO_BUTTON_TEXT, url)),
        None,
        false,
        message,
    )
}

#[must_use]
pub fn private_help_plan(bot: &HelpBotIdentity, message: &TelegramMessage) -> HelpTextPlan {
    help_text_plan(
        render_help_message_html(bot),
        TELEGRAM_PARSE_MODE_HTML.to_owned(),
        None,
        None,
        false,
        message,
    )
}

#[must_use]
pub fn group_help_redirect_plan(bot: &HelpBotIdentity, message: &TelegramMessage) -> HelpTextPlan {
    help_text_plan(
        HELP_REDIRECT_MESSAGE.to_owned(),
        String::new(),
        help_deep_link(bot).map(|url| help_markup(HELP_REDIRECT_BUTTON_TEXT, url)),
        Some(false),
        true,
        message,
    )
}

#[must_use]
pub fn route_start_or_help_command(
    bot: &HelpBotIdentity,
    message: &TelegramMessage,
) -> HelpCommandRoute {
    let Some(command) = leading_bot_command(message) else {
        return HelpCommandRoute::NotHandled;
    };
    let private_chat = telegram_chat_is_private(&message.chat);

    if command.command == START_COMMAND {
        if private_chat {
            return route_private_start_payload(bot, message, &command.arguments);
        }
        if command.target.as_deref() == Some(bot.token.as_str()) {
            return HelpCommandRoute::ConsumedWithoutReply;
        }
        return HelpCommandRoute::NotHandled;
    }

    if command.command != HELP_COMMAND || !command_for_bot(&command, private_chat, &bot.username) {
        return HelpCommandRoute::NotHandled;
    }

    if private_chat {
        HelpCommandRoute::Send(private_help_plan(bot, message))
    } else {
        HelpCommandRoute::Send(group_help_redirect_plan(bot, message))
    }
}

fn route_private_start_payload(
    bot: &HelpBotIdentity,
    message: &TelegramMessage,
    args: &str,
) -> HelpCommandRoute {
    match args {
        "" => HelpCommandRoute::Send(help_intro_plan(bot, message)),
        HELP_COMMAND => HelpCommandRoute::Send(private_help_plan(bot, message)),
        SETTINGS_START_PAYLOAD => {
            HelpCommandRoute::DelegatePrivateStart(PrivateStartDelegation::Settings)
        }
        VIP_START_PAYLOAD => HelpCommandRoute::DelegatePrivateStart(PrivateStartDelegation::Vip),
        DONATE_START_PAYLOAD => {
            HelpCommandRoute::DelegatePrivateStart(PrivateStartDelegation::Donate)
        }
        payload if payload.starts_with(DONATE_START_PAYLOAD_PREFIX) => {
            HelpCommandRoute::DelegatePrivateStart(PrivateStartDelegation::DonatePayload(
                payload.to_owned(),
            ))
        }
        _ => HelpCommandRoute::ConsumedWithoutReply,
    }
}

fn delegated_private_start_update(
    message: &TelegramMessage,
    delegation: &PrivateStartDelegation,
) -> TelegramUpdate {
    let text = delegated_private_start_text(delegation);
    let command_len = text
        .split_ascii_whitespace()
        .next()
        .map(|command| command.encode_utf16().count() as u32)
        .unwrap_or_default();
    let mut delegated = message.clone();
    delegated.data = TelegramMessageData::Text(TelegramText {
        data: text,
        entities: Some(
            [TelegramTextEntity::BotCommand((0..command_len).into())]
                .into_iter()
                .collect(),
        ),
    });
    TelegramUpdate {
        id: 0,
        update_type: TelegramUpdateType::Message(Box::new(delegated)),
    }
}

fn delegated_private_start_text(delegation: &PrivateStartDelegation) -> String {
    match delegation {
        PrivateStartDelegation::Settings => "/settings".to_owned(),
        PrivateStartDelegation::Vip => "/vip".to_owned(),
        PrivateStartDelegation::Donate => "/donate".to_owned(),
        PrivateStartDelegation::DonatePayload(payload) => payload
            .strip_prefix(DONATE_START_PAYLOAD_PREFIX)
            .and_then(|amount| {
                amount
                    .parse::<i64>()
                    .ok()
                    .filter(|amount| (10..=10000).contains(amount))
                    .map(|_| format!("/donate {amount}"))
            })
            .unwrap_or_else(|| "/donate".to_owned()),
    }
}

fn help_text_plan(
    text: String,
    render_as: String,
    reply_markup: Option<ReplyMarkup>,
    allow_sending_without_reply: Option<bool>,
    direct_chattable: bool,
    message: &TelegramMessage,
) -> HelpTextPlan {
    let chat = message_chat_ref(message);
    HelpTextPlan {
        message: TextMessageRequest {
            chat: Some(chat),
            message_thread_id: 0,
            disable_notification: false,
            allow_sending_without_reply,
            text,
            render_as,
            reply_markup,
        },
        reply_to: ReplyMessageRef {
            message_id: message.id,
            chat,
            is_topic_message: message.message_thread_id.is_some(),
            message_thread_id: message.message_thread_id.unwrap_or_default(),
        },
        direct_chattable,
    }
}

fn render_help_template(template: &str, bot: &HelpBotIdentity) -> String {
    template.replace(
        "{{.BotName}}",
        &escape_telegram_html_text(&resolve_help_bot_name(bot)),
    )
}

fn help_markup(text: &str, url: String) -> ReplyMarkup {
    ReplyMarkup::from(build_inline_keyboard_markup([build_inline_keyboard_row([
        build_inline_keyboard_button_url(text, url),
    ])]))
}

fn message_chat_ref(message: &TelegramMessage) -> ChatRef {
    ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    }
}

fn telegram_chat_is_private(chat: &TelegramChat) -> bool {
    matches!(chat, TelegramChat::Private(_))
}

fn command_for_bot(command: &BotCommandInMessage, private_chat: bool, bot_username: &str) -> bool {
    if private_chat {
        return true;
    }
    command.target.as_deref() == Some(bot_username)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BotCommandInMessage {
    command: String,
    target: Option<String>,
    arguments: String,
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
        arguments: command_arguments(text, *position)?,
    })
}

fn command_arguments(text: &TelegramText, position: TelegramTextEntityPosition) -> Option<String> {
    let offset = usize::try_from(position.offset).ok()?;
    let length = usize::try_from(position.length).ok()?;
    let end = utf16_units_to_byte_index(&text.data, offset.checked_add(length)?)?;
    if end == text.data.len() {
        return Some(String::new());
    }

    let tail = &text.data[end..];
    let mut chars = tail.char_indices();
    chars.next()?;
    Some(match chars.next() {
        Some((index, _)) => tail[index..].to_owned(),
        None => String::new(),
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

fn utf16_units_to_byte_index(text: &str, units: usize) -> Option<usize> {
    let mut consumed = 0usize;
    for (index, ch) in text.char_indices() {
        if consumed == units {
            return Some(index);
        }
        consumed = consumed.checked_add(ch.len_utf16())?;
        if consumed > units {
            return None;
        }
    }
    (consumed == units).then_some(text.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_telegram::build_text_message_methods;
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::{Value, json};
    use std::{
        env, io,
        sync::{Mutex, MutexGuard},
        time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
    };

    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateStateStore, UpdateStateStoreFuture,
        process_update_with_state_store_at,
    };

    fn bot() -> HelpBotIdentity {
        HelpBotIdentity {
            first_name: "Пло<тва>".to_owned(),
            username: "PlotvaBot".to_owned(),
            token: "123:token".to_owned(),
        }
    }

    fn sample_message(value: Value) -> Result<TelegramMessage, serde_json::Error> {
        serde_json::from_value(value)
    }

    fn private_command(text: &str, length: i64) -> Result<TelegramMessage, serde_json::Error> {
        sample_message(json!({
            "message_id": 10,
            "date": 1_710_000_000,
            "chat": {"id": 42, "type": "private", "first_name": "Ada"},
            "from": {"id": 5, "is_bot": false, "first_name": "Ada"},
            "text": text,
            "entities": [{"offset": 0, "length": length, "type": "bot_command"}]
        }))
    }

    fn group_command(text: &str, length: i64) -> Result<TelegramMessage, serde_json::Error> {
        sample_message(json!({
            "message_id": 11,
            "date": 1_710_000_000,
            "chat": {"id": -42, "type": "group", "title": "Group"},
            "from": {"id": 5, "is_bot": false, "first_name": "Ada"},
            "text": text,
            "entities": [{"offset": 0, "length": length, "type": "bot_command"}]
        }))
    }

    fn text_update(text: &str, length: Option<i64>) -> Result<TelegramUpdate, serde_json::Error> {
        let mut message = json!({
            "message_id": 12,
            "date": 1_710_000_000,
            "chat": {"id": 42, "type": "private", "first_name": "Ada"},
            "from": {"id": 5, "is_bot": false, "first_name": "Ada"},
            "text": text
        });
        if let Some(length) = length {
            message["entities"] = json!([{"offset": 0, "length": length, "type": "bot_command"}]);
        }
        serde_json::from_value(json!({
            "update_id": 400,
            "message": message,
        }))
    }

    fn command_update(
        update_id: i64,
        message_id: i64,
        text: &str,
        length: i64,
        chat: Value,
        thread_id: Option<i64>,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let mut message = json!({
            "message_id": message_id,
            "date": 1_710_000_000,
            "chat": chat,
            "from": {"id": 5, "is_bot": false, "first_name": "Ada"},
            "text": text,
            "entities": [{"offset": 0, "length": length, "type": "bot_command"}]
        });
        if let Some(thread_id) = thread_id {
            message["message_thread_id"] = json!(thread_id);
        }
        serde_json::from_value(json!({
            "update_id": update_id,
            "message": message,
        }))
    }

    fn private_chat() -> Value {
        json!({"id": 42, "type": "private", "first_name": "Ada"})
    }

    fn group_chat() -> Value {
        json!({"id": -42, "type": "supergroup", "title": "Group"})
    }

    #[test]
    fn intro_plan_matches_go_private_start_help_intro() -> Result<(), Box<dyn std::error::Error>> {
        let message = private_command("/start", 6)?;
        let plan = help_intro_plan(&bot(), &message);

        assert_eq!(plan.reply_to.message_id, 10);
        assert!(!plan.direct_chattable);
        assert_eq!(plan.message.render_as, "");
        assert_eq!(plan.message.allow_sending_without_reply, None);
        assert!(plan.message.text.contains("Пло&lt;тва&gt;"));

        let methods = build_text_message_methods(&plan.message, Some(&plan.reply_to))?;
        let payload = serde_json::to_value(&methods[0])?;
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!(HELP_INTRO_BUTTON_TEXT)
        );
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["url"],
            json!("https://t.me/PlotvaBot?start=help")
        );
        assert_eq!(
            payload["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );

        Ok(())
    }

    #[test]
    fn private_help_plan_matches_go_html_message() -> Result<(), Box<dyn std::error::Error>> {
        let message = private_command("/help", 5)?;
        let plan = private_help_plan(&bot(), &message);

        assert_eq!(plan.message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert!(
            plan.message
                .text
                .starts_with("<b>Привет! Я Пло&lt;тва&gt;</b>")
        );
        assert!(
            plan.message
                .text
                .contains("Поддержать меня можно через /donate.")
        );

        let methods = build_text_message_methods(&plan.message, Some(&plan.reply_to))?;
        let payload = serde_json::to_value(&methods[0])?;
        assert_eq!(payload["parse_mode"], json!("HTML"));

        Ok(())
    }

    #[test]
    fn group_help_redirect_matches_go_direct_chattable_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let message = group_command("/help@PlotvaBot", 15)?;
        let plan = group_help_redirect_plan(&bot(), &message);

        assert!(plan.direct_chattable);
        assert_eq!(plan.message.text, HELP_REDIRECT_MESSAGE);
        assert_eq!(plan.message.allow_sending_without_reply, Some(false));

        let methods = build_text_message_methods(&plan.message, Some(&plan.reply_to))?;
        let payload = serde_json::to_value(&methods[0])?;
        assert_eq!(payload["chat_id"], json!(-42));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(11));
        assert!(
            payload["reply_parameters"]
                .get("allow_sending_without_reply")
                .is_none()
        );
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!(HELP_REDIRECT_BUTTON_TEXT)
        );

        Ok(())
    }

    #[test]
    fn route_start_or_help_command_matches_go_command_gates()
    -> Result<(), Box<dyn std::error::Error>> {
        let bot = bot();

        assert!(matches!(
            route_start_or_help_command(&bot, &private_command("/start", 6)?),
            HelpCommandRoute::Send(_)
        ));
        assert!(matches!(
            route_start_or_help_command(&bot, &private_command("/start help", 6)?),
            HelpCommandRoute::Send(plan)
                if plan.message.render_as == TELEGRAM_PARSE_MODE_HTML
        ));
        assert_eq!(
            route_start_or_help_command(&bot, &private_command("/start  help", 6)?),
            HelpCommandRoute::ConsumedWithoutReply
        );
        assert_eq!(
            route_start_or_help_command(&bot, &private_command("/start settings", 6)?),
            HelpCommandRoute::DelegatePrivateStart(PrivateStartDelegation::Settings)
        );
        assert_eq!(
            route_start_or_help_command(&bot, &private_command("/start donate_25", 6)?),
            HelpCommandRoute::DelegatePrivateStart(PrivateStartDelegation::DonatePayload(
                "donate_25".to_owned()
            ))
        );
        assert!(matches!(
            route_start_or_help_command(&bot, &group_command("/help@PlotvaBot", 15)?),
            HelpCommandRoute::Send(plan) if plan.direct_chattable
        ));
        assert_eq!(
            route_start_or_help_command(&bot, &group_command("/help", 5)?),
            HelpCommandRoute::NotHandled
        );
        assert_eq!(
            route_start_or_help_command(&bot, &group_command("/start@123:token", 16)?),
            HelpCommandRoute::ConsumedWithoutReply
        );

        Ok(())
    }

    #[test]
    fn bot_name_resolution_matches_go_fallback_order() {
        assert_eq!(resolve_help_bot_name(&bot()), "Пло<тва>");
        assert_eq!(
            resolve_help_bot_name(&HelpBotIdentity {
                first_name: " ".to_owned(),
                username: "PlotvaBot".to_owned(),
                token: String::new(),
            }),
            "PlotvaBot"
        );
        assert_eq!(
            resolve_help_bot_name(&HelpBotIdentity::default()),
            FALLBACK_BOT_NAME
        );
    }

    #[tokio::test]
    async fn help_update_handler_sends_private_start_without_downstream()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = Arc::new(HelpEffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = HelpCommandUpdateHandler::new(bot(), effects.clone(), next.clone());

        handler
            .handle_update(text_update("/start", Some(6))?)
            .await?;

        assert_eq!(next.handled_count(), 0);
        let sent = effects.sent();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].message.text.contains("Хочешь познакомиться"));

        Ok(())
    }

    #[tokio::test]
    async fn help_update_handler_delegates_non_help_update()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = Arc::new(HelpEffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = HelpCommandUpdateHandler::new(bot(), effects.clone(), next.clone());

        handler.handle_update(text_update("hello", None)?).await?;

        assert_eq!(next.handled_count(), 1);
        assert!(effects.sent().is_empty());
        assert!(effects.delegations().is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn private_start_delegation_is_consumed_by_effect()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = Arc::new(HelpEffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let outcome = handle_help_command_update_or_else(
            &bot(),
            effects.as_ref(),
            text_update("/start settings", Some(6))?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            outcome,
            HelpCommandUpdateOutcome::DelegatedPrivateStart(PrivateStartDelegation::Settings)
        );
        assert_eq!(next.handled_count(), 0);
        assert_eq!(
            effects.delegations(),
            vec![PrivateStartDelegation::Settings]
        );

        Ok(())
    }

    #[test]
    fn private_start_delegation_routes_to_owner_commands() {
        assert_eq!(
            delegated_private_start_text(&PrivateStartDelegation::Settings),
            "/settings"
        );
        assert_eq!(
            delegated_private_start_text(&PrivateStartDelegation::Vip),
            "/vip"
        );
        assert_eq!(
            delegated_private_start_text(&PrivateStartDelegation::Donate),
            "/donate"
        );
        assert_eq!(
            delegated_private_start_text(&PrivateStartDelegation::DonatePayload(
                "donate_25".to_owned()
            )),
            "/donate 25"
        );
        assert_eq!(
            delegated_private_start_text(&PrivateStartDelegation::DonatePayload(
                "donate_nope".to_owned()
            )),
            "/donate"
        );
    }

    #[tokio::test]
    async fn help_send_errors_are_suppressed_like_go_logs() -> Result<(), Box<dyn std::error::Error>>
    {
        let effects = HelpEffectsStub {
            send_error: Some("telegram down".to_owned()),
            ..HelpEffectsStub::default()
        };
        let next = UpdateHandlerStub::default();
        let outcome = handle_help_command_update_or_else(
            &bot(),
            &effects,
            text_update("/help", Some(5))?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            outcome,
            HelpCommandUpdateOutcome::SendError {
                direct_chattable: false,
                message: "telegram down".to_owned(),
            }
        );
        assert_eq!(next.handled_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_help_start_commands_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-help-start:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&command_update(501, 21, "/help", 5, private_chat(), None)?)
            .await?;
        update_queue
            .enqueue_update(&command_update(
                502,
                22,
                "/help@PlotvaBot",
                15,
                group_chat(),
                Some(7),
            )?)
            .await?;
        update_queue
            .enqueue_update(&command_update(503, 23, "/start", 6, private_chat(), None)?)
            .await?;
        update_queue
            .enqueue_update(&command_update(
                504,
                24,
                "/start settings",
                6,
                private_chat(),
                None,
            )?)
            .await?;
        update_queue
            .enqueue_update(&command_update(
                505,
                25,
                "/start hello",
                6,
                private_chat(),
                None,
            )?)
            .await?;
        update_queue
            .enqueue_update(&command_update(
                506,
                26,
                "/start@123:token",
                16,
                group_chat(),
                Some(8),
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 6);

        let effects = Arc::new(HelpEffectsStub::default());
        let terminal = Arc::new(UpdateHandlerStub::default());
        let handler = HelpCommandUpdateHandler::new(bot(), Arc::clone(&effects), terminal.clone());
        let state_store = UpdateStateStoreStub::default();
        let config = UpdateConsumerConfig {
            dequeue_timeout: StdDuration::from_millis(1),
            state_timeout: StdDuration::from_secs(1),
            handle_timeout: StdDuration::from_secs(1),
            side_effect_max_age: StdDuration::from_secs(60),
            worker_limit: 1,
        };
        let now = UNIX_EPOCH + StdDuration::from_secs(1_710_000_010);

        for expected in [501, 502, 503, 504, 505, 506] {
            let update = update_queue
                .dequeue_update(StdDuration::from_secs(1))
                .await?
                .ok_or_else(|| io::Error::other("expected decoded help/start update"))?;
            let report =
                process_update_with_state_store_at(update, config, now, &state_store, |update| {
                    handler.handle_update(update)
                })
                .await;
            assert_eq!(report.update_id, expected);
            assert_eq!(report.update_name, "message");
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed)
            );
            assert!(!report.skipped_handle);
        }

        assert_eq!(terminal.handled_count(), 0);
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Ada:".to_owned(),
                "user:5:Ada:".to_owned(),
                "chat:-42:supergroup::".to_owned(),
                "user:5:Ada:".to_owned(),
                "chat:42:private:Ada:".to_owned(),
                "user:5:Ada:".to_owned(),
                "chat:42:private:Ada:".to_owned(),
                "user:5:Ada:".to_owned(),
                "chat:42:private:Ada:".to_owned(),
                "user:5:Ada:".to_owned(),
                "chat:-42:supergroup::".to_owned(),
                "user:5:Ada:".to_owned(),
            ]
        );

        let sent = effects.sent();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[0].reply_to.message_id, 21);
        assert!(!sent[0].direct_chattable);
        assert_eq!(sent[0].message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert!(
            sent[0]
                .message
                .text
                .starts_with("<b>Привет! Я Пло&lt;тва&gt;</b>")
        );
        assert_eq!(sent[1].reply_to.message_id, 22);
        assert_eq!(sent[1].reply_to.message_thread_id, 7);
        assert!(sent[1].direct_chattable);
        assert_eq!(sent[1].message.text, HELP_REDIRECT_MESSAGE);
        assert_eq!(sent[1].message.allow_sending_without_reply, Some(false));
        assert_eq!(sent[2].reply_to.message_id, 23);
        assert!(!sent[2].direct_chattable);
        assert!(sent[2].message.text.contains("Хочешь познакомиться"));
        assert_eq!(
            effects.delegations(),
            vec![PrivateStartDelegation::Settings]
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[derive(Default)]
    struct HelpEffectsStub {
        sent: Mutex<Vec<HelpTextPlan>>,
        delegations: Mutex<Vec<PrivateStartDelegation>>,
        send_error: Option<String>,
        delegate_error: Option<String>,
    }

    impl HelpEffectsStub {
        fn sent(&self) -> Vec<HelpTextPlan> {
            self.sent.lock().expect("sent").clone()
        }

        fn delegations(&self) -> Vec<PrivateStartDelegation> {
            self.delegations.lock().expect("delegations").clone()
        }

        fn lock_delegations(&self) -> MutexGuard<'_, Vec<PrivateStartDelegation>> {
            self.delegations.lock().expect("delegations")
        }
    }

    impl HelpCommandEffects for HelpEffectsStub {
        type Error = io::Error;

        fn send_help_text<'a>(
            &'a self,
            plan: HelpTextPlan,
        ) -> HelpCommandEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                if let Some(error) = &self.send_error {
                    return Err(io::Error::other(error.clone()));
                }
                self.sent.lock().expect("sent").push(plan);
                Ok(())
            })
        }

        fn delegate_private_start<'a>(
            &'a self,
            _message: &'a TelegramMessage,
            delegation: PrivateStartDelegation,
        ) -> HelpCommandEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                if let Some(error) = &self.delegate_error {
                    return Err(io::Error::other(error.clone()));
                }
                self.lock_delegations().push(delegation);
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
}
