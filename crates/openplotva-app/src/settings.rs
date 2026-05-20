//! App-level fetcher settings command behavior.

use std::{fmt, future::Future, pin::Pin};

use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, Update as TelegramUpdate, UpdateType,
    User as TelegramUser,
};
use openplotva_core::{SENDER_TYPE_CHANNEL, SENDER_TYPE_SAME_CHAT};
use openplotva_taskman::{
    CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, HIGH_PRIORITY,
    StatelessJobItem, new_control_job_at,
};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, OutboundBuildError, ReplyMarkup, ReplyMessageRef, TextMessageRequest,
};
use time::OffsetDateTime;

use crate::virtual_messages::{
    QueueTextReport, QueueTextRequest, VirtualMessageStore, queue_text_message_parts,
};

const GROUP_SETTINGS_CONTROL_JOB_TITLE: &str = "group settings";
const GROUP_SETTINGS_WAIT_NOTICE_TEXT: &str = "⏳ Проверяю права и готовлю ссылку на настройки...";
const SETTINGS_SAME_CHAT_DECLINE_TEXT: &str = "❌ Невозможно подтвердить права владельца чата при отправке от имени чата.\n\nДля доступа к настройкам отправьте команду от имени владельца чата (не анонимно).";
const SETTINGS_CHANNEL_DECLINE_TEXT: &str = "❌ Сообщения от имени канала не могут быть проверены как владелец чата.\n\nДля доступа к настройкам отправьте команду от имени владельца чата (не анонимно).";
const SETTINGS_QUEUE_ERROR_TEXT: &str = "❌ Не удалось поставить задачу в очередь.";
const GROUP_SETTINGS_CHECK_FAILED_TEXT: &str = "❌ Не удалось проверить права. Попробуйте позже.";
const GROUP_SETTINGS_PERMISSION_DENIED_TEXT: &str =
    "❌ У вас нет прав для управления настройками этого чата.";
const GROUP_SETTINGS_OPEN_PRIVATE_TEXT: &str =
    "Откройте личный чат со мной, чтобы выбрать чат для настройки:";
const GROUP_SETTINGS_OPEN_BUTTON_TEXT: &str = "⚙️ Открыть настройки";

/// Boxed future returned by settings taskman assignment calls.
pub type SettingsControlJobQueueFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by group settings executor permission checks.
pub type GroupSettingsControlJobFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by Go `syncChatAdmins` equivalents.
pub type GroupSettingsSyncFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Queue boundary for Go settings-owned taskman control jobs.
pub trait SettingsControlJobQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Assign a control job to a named taskman queue.
    fn assign_settings_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> SettingsControlJobQueueFuture<'a, Self::Error>;
}

/// Side-effect boundary for Go group settings control jobs.
pub trait GroupSettingsControlJobEffects {
    /// Error returned by permission checks.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Check Go `canOpenGroupSettings`.
    fn can_open_group_settings<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> GroupSettingsControlJobFuture<'a, bool, Self::Error>;

    /// Run Go `syncChatAdmins`; it logs internally and does not affect the job result.
    fn sync_chat_admins<'a>(&'a self, chat_id: i64) -> GroupSettingsSyncFuture<'a>;
}

/// Result of handling a decoded `/settings` update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SettingsCommandOutcome {
    /// The update was not a Telegram message carrying Go's `/settings` command.
    NotSettingsCommand,
    /// The command was not in a private chat; group handling is a later taskman slice.
    NonPrivateChat,
    /// Go logs and returns when `WEBAPP_URL` is blank.
    WebAppUrlMissing,
    /// The private settings link was queued through Go's text-send path.
    Queued(QueueTextReport),
}

/// Result of building Go's group `/settings` control job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupSettingsControlJobBuild {
    /// The update was not a Telegram message carrying Go's `/settings` command.
    NotSettingsCommand,
    /// Private chats are handled by `handlePrivateSettingsCommand`.
    PrivateChat,
    /// Go declines anonymous admins sent as the chat itself.
    UnsupportedSameChatSender,
    /// Go declines linked-channel senders because owner rights cannot be checked.
    UnsupportedChannelSender,
    /// Go logs and returns when the caller user ID is absent.
    MissingCaller,
    /// Telegram identifiers did not fit the Go-shaped taskman payload.
    InvalidMessage,
    /// Go-shaped taskman job for the control queue.
    Job(Box<StatelessJobItem>),
}

/// Result of handling Go's group `/settings` command path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupSettingsCommandOutcome {
    /// The update was not a Telegram message carrying Go's `/settings` command.
    NotSettingsCommand,
    /// Private chats are handled by `handlePrivateSettingsCommand`.
    PrivateChat,
    /// Same-chat sender decline text was queued.
    UnsupportedSameChatSender(QueueTextReport),
    /// Channel sender decline text was queued.
    UnsupportedChannelSender(QueueTextReport),
    /// Go logs and returns when the caller user ID is absent.
    MissingCaller,
    /// Telegram identifiers did not fit the Go-shaped taskman payload.
    InvalidMessage,
    /// Assigning the control job failed; Go queues the failure text.
    QueueError(QueueTextReport),
    /// The control job was assigned and the wait notice was queued.
    Queued { notice: QueueTextReport },
}

/// Result of executing Go's group-settings control-job slice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupSettingsControlJobOutcome {
    /// The job was not the group-settings control kind.
    UnsupportedKind(ControlKind),
    /// Permission check failed; Go sends a retry-later notice and returns the check error.
    PermissionCheckFailed(String),
    /// The caller is not allowed to manage group settings.
    PermissionDenied,
    /// Go returns an error before sync/send when the bot username is unavailable.
    BotUsernameMissing,
    /// Admin sync ran and the settings deep-link notice was queued.
    SentLink,
}

/// Handle Go's private `/settings` command path.
pub async fn handle_private_settings_command_update<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    update: &TelegramUpdate,
    bot_username: &str,
    web_app_url: &str,
    next_virtual_id: NextId,
) -> Result<SettingsCommandOutcome, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let UpdateType::Message(message) = &update.update_type else {
        return Ok(SettingsCommandOutcome::NotSettingsCommand);
    };
    if !openplotva_updates::is_settings_command_message(message, bot_username) {
        return Ok(SettingsCommandOutcome::NotSettingsCommand);
    }
    if !matches!(message.chat, TelegramChat::Private(_)) {
        return Ok(SettingsCommandOutcome::NonPrivateChat);
    }
    if web_app_url.is_empty() {
        return Ok(SettingsCommandOutcome::WebAppUrlMissing);
    }

    let chat = ChatRef {
        id: message.chat.get_id().into(),
        is_forum: false,
    };
    let user_id = message
        .sender
        .get_user()
        .map(|user| user.id.into())
        .unwrap_or_default();
    let url = openplotva_web::private_settings_web_app_url(web_app_url, user_id);
    let keyboard = openplotva_telegram::build_private_settings_keyboard(url);
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: "Откройте настройки бота:".to_owned(),
        render_as: String::new(),
        reply_markup: Some(ReplyMarkup::InlineKeyboardMarkup(keyboard)),
    };
    let reply_to = ReplyMessageRef {
        message_id: message.id,
        chat,
        is_topic_message: false,
        message_thread_id: 0,
    };
    let report = queue_text_message_parts(
        store,
        queue,
        QueueTextRequest {
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: true,
            bypass_chat_restrictions: true,
        },
        next_virtual_id,
    )
    .await?;

    Ok(SettingsCommandOutcome::Queued(report))
}

/// Execute Go's group-settings control-job behavior up to Telegram dispatch queueing.
pub async fn execute_group_settings_control_job_at<S, Effects, NextId>(
    store: &S,
    dispatcher_queue: &DispatcherQueue,
    effects: &Effects,
    params: &ControlJobParams,
    bot_username: &str,
    next_virtual_id: NextId,
) -> Result<GroupSettingsControlJobOutcome, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    Effects: GroupSettingsControlJobEffects + Sync,
    NextId: FnMut() -> String,
{
    if params.data.kind != ControlKind::GroupSettings {
        return Ok(GroupSettingsControlJobOutcome::UnsupportedKind(
            params.data.kind,
        ));
    }

    match effects
        .can_open_group_settings(params.chat_id, params.user_id)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            queue_group_settings_control_text(
                store,
                dispatcher_queue,
                params,
                GROUP_SETTINGS_PERMISSION_DENIED_TEXT,
                None,
                true,
                next_virtual_id,
            )
            .await?;
            return Ok(GroupSettingsControlJobOutcome::PermissionDenied);
        }
        Err(error) => {
            let error = error.to_string();
            queue_group_settings_control_text(
                store,
                dispatcher_queue,
                params,
                GROUP_SETTINGS_CHECK_FAILED_TEXT,
                None,
                true,
                next_virtual_id,
            )
            .await?;
            return Ok(GroupSettingsControlJobOutcome::PermissionCheckFailed(error));
        }
    }

    if bot_username.is_empty() {
        return Ok(GroupSettingsControlJobOutcome::BotUsernameMissing);
    }

    effects.sync_chat_admins(params.chat_id).await;

    let deep_link = format!("https://t.me/{bot_username}?start=settings");
    let button = openplotva_telegram::build_inline_keyboard_button_url(
        GROUP_SETTINGS_OPEN_BUTTON_TEXT,
        &deep_link,
    );
    queue_group_settings_control_text(
        store,
        dispatcher_queue,
        params,
        GROUP_SETTINGS_OPEN_PRIVATE_TEXT,
        Some(ReplyMarkup::from([[button]])),
        true,
        next_virtual_id,
    )
    .await?;
    Ok(GroupSettingsControlJobOutcome::SentLink)
}

/// Build the Go taskman control job produced by group `/settings`.
#[must_use]
pub fn group_settings_control_job_from_update_at(
    update: &TelegramUpdate,
    bot_username: &str,
    created: OffsetDateTime,
) -> GroupSettingsControlJobBuild {
    let UpdateType::Message(message) = &update.update_type else {
        return GroupSettingsControlJobBuild::NotSettingsCommand;
    };
    group_settings_control_job_from_message_at(message, bot_username, created)
}

/// Handle Go's group `/settings` command path up to taskman assignment.
pub async fn handle_group_settings_command_update_at<S, Queue, NextId>(
    store: &S,
    dispatcher_queue: &DispatcherQueue,
    control_queue: &Queue,
    update: &TelegramUpdate,
    bot_username: &str,
    created: OffsetDateTime,
    next_virtual_id: NextId,
) -> Result<GroupSettingsCommandOutcome, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    Queue: SettingsControlJobQueue + Sync,
    NextId: FnMut() -> String,
{
    let UpdateType::Message(message) = &update.update_type else {
        return Ok(GroupSettingsCommandOutcome::NotSettingsCommand);
    };

    match group_settings_control_job_from_message_at(message, bot_username, created) {
        GroupSettingsControlJobBuild::NotSettingsCommand => {
            Ok(GroupSettingsCommandOutcome::NotSettingsCommand)
        }
        GroupSettingsControlJobBuild::PrivateChat => Ok(GroupSettingsCommandOutcome::PrivateChat),
        GroupSettingsControlJobBuild::UnsupportedSameChatSender => {
            let report = queue_group_settings_notice(
                store,
                dispatcher_queue,
                message,
                SETTINGS_SAME_CHAT_DECLINE_TEXT,
                true,
                next_virtual_id,
            )
            .await?;
            Ok(GroupSettingsCommandOutcome::UnsupportedSameChatSender(
                report,
            ))
        }
        GroupSettingsControlJobBuild::UnsupportedChannelSender => {
            let report = queue_group_settings_notice(
                store,
                dispatcher_queue,
                message,
                SETTINGS_CHANNEL_DECLINE_TEXT,
                true,
                next_virtual_id,
            )
            .await?;
            Ok(GroupSettingsCommandOutcome::UnsupportedChannelSender(
                report,
            ))
        }
        GroupSettingsControlJobBuild::MissingCaller => {
            Ok(GroupSettingsCommandOutcome::MissingCaller)
        }
        GroupSettingsControlJobBuild::InvalidMessage => {
            Ok(GroupSettingsCommandOutcome::InvalidMessage)
        }
        GroupSettingsControlJobBuild::Job(job) => {
            match control_queue
                .assign_settings_control_job(CONTROL_QUEUE_NAME, *job)
                .await
            {
                Ok(()) => {
                    let notice = queue_group_settings_notice(
                        store,
                        dispatcher_queue,
                        message,
                        GROUP_SETTINGS_WAIT_NOTICE_TEXT,
                        true,
                        next_virtual_id,
                    )
                    .await?;
                    Ok(GroupSettingsCommandOutcome::Queued { notice })
                }
                Err(error) => {
                    tracing::warn!(%error, "failed to assign group settings control job");
                    let report = queue_group_settings_notice(
                        store,
                        dispatcher_queue,
                        message,
                        SETTINGS_QUEUE_ERROR_TEXT,
                        false,
                        next_virtual_id,
                    )
                    .await?;
                    Ok(GroupSettingsCommandOutcome::QueueError(report))
                }
            }
        }
    }
}

fn group_settings_control_job_from_message_at(
    message: &TelegramMessage,
    bot_username: &str,
    created: OffsetDateTime,
) -> GroupSettingsControlJobBuild {
    if !openplotva_updates::is_settings_command_message(message, bot_username) {
        return GroupSettingsControlJobBuild::NotSettingsCommand;
    }
    if matches!(message.chat, TelegramChat::Private(_)) {
        return GroupSettingsControlJobBuild::PrivateChat;
    }

    let sender = openplotva_updates::resolve_message_sender(Some(message));
    match sender.sender_type.as_str() {
        SENDER_TYPE_SAME_CHAT => return GroupSettingsControlJobBuild::UnsupportedSameChatSender,
        SENDER_TYPE_CHANNEL => return GroupSettingsControlJobBuild::UnsupportedChannelSender,
        _ => {}
    }

    let Some(user) = message.sender.get_user() else {
        return GroupSettingsControlJobBuild::MissingCaller;
    };
    let user_id = i64::from(user.id);
    if user_id == 0 {
        return GroupSettingsControlJobBuild::MissingCaller;
    }

    let Ok(message_id) = i32::try_from(message.id) else {
        return GroupSettingsControlJobBuild::InvalidMessage;
    };
    let thread_id = message
        .message_thread_id
        .and_then(|thread_id| i32::try_from(thread_id).ok())
        .filter(|thread_id| *thread_id != 0);
    let mut data = control_data_from_settings_message(message);
    data.kind = ControlKind::GroupSettings;

    let user_full_name = user_full_name(user);
    let user_full_name = if user_full_name.trim().is_empty() {
        sender.display_name()
    } else {
        user_full_name
    };
    let params = ControlJobParams {
        chat_id: message.chat.get_id().into(),
        message_id,
        user_id,
        user_full_name,
        thread_id,
        data,
    };
    GroupSettingsControlJobBuild::Job(Box::new(
        new_control_job_at(params, created)
            .with_name(GROUP_SETTINGS_CONTROL_JOB_TITLE)
            .with_priority(HIGH_PRIORITY),
    ))
}

fn control_data_from_settings_message(message: &TelegramMessage) -> ControlJobData {
    let mut data = ControlJobData {
        chat_type: chat_type_name(&message.chat).to_owned(),
        ..ControlJobData::default()
    };
    if let Some(user) = message.sender.get_user() {
        data.user_name = user
            .username
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        data.first_name = user.first_name.clone();
        data.last_name = user.last_name.clone().unwrap_or_default();
        data.language_code = user.language_code.clone().unwrap_or_default();
        data.is_premium = user.is_premium.unwrap_or_default();
    }
    data
}

async fn queue_group_settings_notice<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    message: &TelegramMessage,
    text: &str,
    bypass_chat_restrictions: bool,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let chat = message_chat_ref(message);
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: text.to_owned(),
        render_as: String::new(),
        reply_markup: None,
    };
    let reply_to = ReplyMessageRef {
        message_id: message.id,
        chat,
        is_topic_message: message.message_thread_id.is_some(),
        message_thread_id: message.message_thread_id.unwrap_or_default(),
    };
    queue_text_message_parts(
        store,
        queue,
        QueueTextRequest {
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: true,
            bypass_chat_restrictions,
        },
        next_virtual_id,
    )
    .await
}

async fn queue_group_settings_control_text<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    params: &ControlJobParams,
    text: &str,
    reply_markup: Option<ReplyMarkup>,
    bypass_chat_restrictions: bool,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let chat = ChatRef {
        id: params.chat_id,
        is_forum: false,
    };
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: text.to_owned(),
        render_as: String::new(),
        reply_markup,
    };
    let reply_to = ReplyMessageRef {
        message_id: i64::from(params.message_id),
        chat,
        is_topic_message: false,
        message_thread_id: 0,
    };
    queue_text_message_parts(
        store,
        queue,
        QueueTextRequest {
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: true,
            bypass_chat_restrictions,
        },
        next_virtual_id,
    )
    .await
}

fn message_chat_ref(message: &TelegramMessage) -> ChatRef {
    ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    }
}

fn chat_type_name(chat: &TelegramChat) -> &'static str {
    match chat {
        TelegramChat::Channel(_) => "channel",
        TelegramChat::Group(_) => "group",
        TelegramChat::Private(_) => "private",
        TelegramChat::Supergroup(_) => "supergroup",
    }
}

fn user_full_name(user: &TelegramUser) -> String {
    let name = format!(
        "{} {}",
        user.first_name,
        user.last_name.as_deref().unwrap_or_default()
    )
    .trim()
    .to_owned();
    if name.is_empty() {
        user.username
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default()
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt,
        future::Future,
        io,
        pin::Pin,
        sync::{Arc, Mutex},
    };

    use carapax::types::Update as TelegramUpdate;
    use openplotva_core::MessageIdMapping;
    use openplotva_taskman::{
        CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, HIGH_PRIORITY, JobType,
        StatelessJobItem,
    };
    use openplotva_telegram::{DispatcherConfig, DispatcherQueue, TelegramOutboundMethodKind};
    use serde_json::{Value, json};
    use time::OffsetDateTime;

    use crate::virtual_messages::VirtualMessageStore;

    use super::{
        GroupSettingsCommandOutcome, GroupSettingsControlJobBuild, SettingsCommandOutcome,
        SettingsControlJobQueue, SettingsControlJobQueueFuture,
        execute_group_settings_control_job_at, group_settings_control_job_from_update_at,
        handle_group_settings_command_update_at, handle_private_settings_command_update,
    };

    #[tokio::test]
    async fn private_settings_command_queues_go_webapp_button_with_bypass_and_immediate()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let update = private_settings_update()?;

        let outcome = handle_private_settings_command_update(
            &store,
            &queue,
            &update,
            "PlotvaBot",
            "https://plotva.example",
            || "settings-v1".to_owned(),
        )
        .await?;

        let SettingsCommandOutcome::Queued(report) = outcome else {
            return Err(format!("expected queued settings command, got {outcome:?}").into());
        };
        assert_eq!(report.enqueued_count(), 1);
        assert_eq!(report.parts[0].virtual_id, "settings-v1");
        assert!(report.parts[0].immediate);
        assert_eq!(store.inserted(), vec![("settings-v1".to_owned(), 42, None)]);

        let item = queue
            .dequeue_immediate()
            .expect("private settings command should enqueue immediate text");
        assert!(item.bypasses_chat_restrictions());
        assert_eq!(
            item.method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        let method = item.into_method().expect("queued settings method");
        let value = serde_json::to_value(method_as_value(method)?)?;

        assert_eq!(value["chat_id"], json!(42));
        assert_eq!(value["text"], json!("Откройте настройки бота:"));
        assert_eq!(value["reply_parameters"]["message_id"], json!(77));
        assert_eq!(value["reply_parameters"]["chat_id"], json!(42));
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!("⚙️ Настройки")
        );
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["web_app"]["url"],
            json!("https://plotva.example/settings/index.html?signature=780e28cf")
        );

        Ok(())
    }

    #[tokio::test]
    async fn private_settings_command_skips_blank_webapp_url_like_go() -> Result<(), Box<dyn Error>>
    {
        let store = StoreStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let update = private_settings_update()?;

        let outcome = handle_private_settings_command_update(
            &store,
            &queue,
            &update,
            "PlotvaBot",
            "",
            || "settings-v1".to_owned(),
        )
        .await?;

        assert_eq!(outcome, SettingsCommandOutcome::WebAppUrlMissing);
        assert!(queue.snapshot().immediate.is_empty());
        assert!(store.inserted().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn private_settings_handler_leaves_group_settings_to_group_path()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let update = group_settings_update()?;

        let outcome = handle_private_settings_command_update(
            &store,
            &queue,
            &update,
            "PlotvaBot",
            "https://plotva.example",
            || "settings-v1".to_owned(),
        )
        .await?;

        assert_eq!(outcome, SettingsCommandOutcome::NonPrivateChat);
        assert!(queue.snapshot().immediate.is_empty());
        assert!(store.inserted().is_empty());
        Ok(())
    }

    #[test]
    fn group_settings_command_builds_go_control_job_payload() -> Result<(), Box<dyn Error>> {
        let update = group_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let GroupSettingsControlJobBuild::Job(job) =
            group_settings_control_job_from_update_at(&update, "PlotvaBot", created)
        else {
            return Err("expected group settings control job".into());
        };

        assert_eq!(job.title, "group settings");
        assert_eq!(job.created, created);
        assert_eq!(job.priority, HIGH_PRIORITY);
        assert_eq!(job.data.job_type, JobType::Control);

        let telegram_data = job.data.telegram_data.as_ref().expect("telegram metadata");
        assert_eq!(telegram_data.chat_id, -10042);
        assert_eq!(telegram_data.message_id, 78);
        assert_eq!(telegram_data.user_id, 42);
        assert_eq!(telegram_data.user_full_name, "Ada Lovelace");
        assert_eq!(telegram_data.thread_message_id, Some(99));
        assert_eq!(telegram_data.chat_title, "");

        let control_data = job.data.control_data.as_ref().expect("control data");
        assert_eq!(control_data.kind, ControlKind::GroupSettings);
        assert_eq!(control_data.chat_type, "supergroup");
        assert_eq!(control_data.user_name, "ada_l");
        assert_eq!(control_data.first_name, "Ada");
        assert_eq!(control_data.last_name, "Lovelace");
        assert_eq!(control_data.language_code, "en");
        assert!(control_data.is_premium);

        Ok(())
    }

    #[tokio::test]
    async fn group_settings_command_assigns_control_job_and_sends_wait_notice()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::default();
        let update = group_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = handle_group_settings_command_update_at(
            &store,
            &dispatcher,
            &control_queue,
            &update,
            "PlotvaBot",
            created,
            || "group-settings-v1".to_owned(),
        )
        .await?;

        let GroupSettingsCommandOutcome::Queued { notice } = outcome else {
            return Err(format!("expected queued group settings command, got {outcome:?}").into());
        };
        assert_eq!(notice.enqueued_count(), 1);

        let assigned = control_queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "group settings");

        assert_eq!(
            store.inserted(),
            vec![("group-settings-v1".to_owned(), -10042, Some(0))]
        );
        let item = dispatcher
            .dequeue_immediate()
            .expect("group settings wait notice should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        assert_eq!(
            item.method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        let method = item.into_method().expect("queued wait notice");
        let value = serde_json::to_value(method_as_value(method)?)?;

        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(
            value["text"],
            json!("⏳ Проверяю права и готовлю ссылку на настройки...")
        );
        assert_eq!(value["message_thread_id"], json!(99));
        assert_eq!(value["reply_parameters"]["message_id"], json!(78));
        assert_eq!(value["reply_parameters"]["chat_id"], json!(-10042));
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );

        Ok(())
    }

    #[tokio::test]
    async fn group_settings_command_declines_same_chat_sender_without_queueing_job()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::default();
        let update = same_chat_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = handle_group_settings_command_update_at(
            &store,
            &dispatcher,
            &control_queue,
            &update,
            "PlotvaBot",
            created,
            || "settings-decline-v1".to_owned(),
        )
        .await?;

        let GroupSettingsCommandOutcome::UnsupportedSameChatSender(report) = outcome else {
            return Err(format!("expected same-chat decline, got {outcome:?}").into());
        };
        assert_eq!(report.enqueued_count(), 1);
        assert!(control_queue.assigned().is_empty());

        let item = dispatcher
            .dequeue_immediate()
            .expect("same-chat decline should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued same-chat decline");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(
            value["text"],
            json!(
                "❌ Невозможно подтвердить права владельца чата при отправке от имени чата.\n\nДля доступа к настройкам отправьте команду от имени владельца чата (не анонимно)."
            )
        );
        assert_eq!(value["reply_parameters"]["message_id"], json!(78));
        assert!(store.inserted().len() == 1);
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_command_queue_error_sends_go_failure_notice()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::failing();
        let update = group_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = handle_group_settings_command_update_at(
            &store,
            &dispatcher,
            &control_queue,
            &update,
            "PlotvaBot",
            created,
            || "settings-error-v1".to_owned(),
        )
        .await?;

        let GroupSettingsCommandOutcome::QueueError(report) = outcome else {
            return Err(format!("expected queue failure notice, got {outcome:?}").into());
        };
        assert_eq!(report.enqueued_count(), 1);
        assert_eq!(control_queue.assigned().len(), 1);

        let item = dispatcher
            .dequeue_immediate()
            .expect("queue failure notice should enqueue immediately");
        assert!(!item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued failure notice");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(
            value["text"],
            json!("❌ Не удалось поставить задачу в очередь.")
        );
        assert_eq!(value["reply_parameters"]["message_id"], json!(78));
        Ok(())
    }

    #[test]
    fn group_settings_command_without_caller_is_not_queued_like_go() -> Result<(), Box<dyn Error>> {
        let update = missing_caller_group_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = group_settings_control_job_from_update_at(&update, "PlotvaBot", created);

        assert_eq!(outcome, GroupSettingsControlJobBuild::MissingCaller);
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_executor_syncs_admins_and_sends_deep_link_when_allowed()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::allowing();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "PlotvaBot",
            || "settings-link-v1".to_owned(),
        )
        .await?;

        assert_eq!(outcome, super::GroupSettingsControlJobOutcome::SentLink);
        assert_eq!(effects.permission_checks(), vec![(-10042, 42)]);
        assert_eq!(effects.synced_admin_chats(), vec![-10042]);
        assert_eq!(
            store.inserted(),
            vec![("settings-link-v1".to_owned(), -10042, None)]
        );

        let item = dispatcher
            .dequeue_immediate()
            .expect("settings deep-link should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued settings deep-link");
        let value = serde_json::to_value(method_as_value(method)?)?;

        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(
            value["text"],
            json!("Откройте личный чат со мной, чтобы выбрать чат для настройки:")
        );
        assert_eq!(value["reply_parameters"]["message_id"], json!(78));
        assert_eq!(value["reply_parameters"]["chat_id"], json!(-10042));
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        assert!(value.get("message_thread_id").is_none());
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!("⚙️ Открыть настройки")
        );
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["url"],
            json!("https://t.me/PlotvaBot?start=settings")
        );

        Ok(())
    }

    #[tokio::test]
    async fn group_settings_executor_sends_rights_decline_when_not_allowed()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::denying();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "PlotvaBot",
            || "settings-denied-v1".to_owned(),
        )
        .await?;

        assert_eq!(
            outcome,
            super::GroupSettingsControlJobOutcome::PermissionDenied
        );
        assert_eq!(effects.permission_checks(), vec![(-10042, 42)]);
        assert!(effects.synced_admin_chats().is_empty());

        let item = dispatcher
            .dequeue_immediate()
            .expect("settings rights decline should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued settings rights decline");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(
            value["text"],
            json!("❌ У вас нет прав для управления настройками этого чата.")
        );
        assert!(value.get("reply_markup").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_executor_sends_check_failure_and_reports_error()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::failing_permission_check();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "PlotvaBot",
            || "settings-check-failed-v1".to_owned(),
        )
        .await?;

        assert_eq!(
            outcome,
            super::GroupSettingsControlJobOutcome::PermissionCheckFailed(
                "request failed".to_owned()
            )
        );
        assert_eq!(effects.permission_checks(), vec![(-10042, 42)]);
        assert!(effects.synced_admin_chats().is_empty());

        let item = dispatcher
            .dequeue_immediate()
            .expect("settings permission-check failure should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued settings check failure");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(
            value["text"],
            json!("❌ Не удалось проверить права. Попробуйте позже.")
        );
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_executor_rejects_blank_bot_username_before_sync_or_send()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::allowing();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "",
            || "settings-link-v1".to_owned(),
        )
        .await?;

        assert_eq!(
            outcome,
            super::GroupSettingsControlJobOutcome::BotUsernameMissing
        );
        assert_eq!(effects.permission_checks(), vec![(-10042, 42)]);
        assert!(effects.synced_admin_chats().is_empty());
        assert!(dispatcher.snapshot().immediate.is_empty());
        assert!(store.inserted().is_empty());
        Ok(())
    }

    type RecordedVirtualInsert = (String, i64, Option<i32>);

    #[derive(Clone, Default)]
    struct StoreStub {
        inserted: Arc<Mutex<Vec<RecordedVirtualInsert>>>,
    }

    impl StoreStub {
        fn inserted(&self) -> Vec<RecordedVirtualInsert> {
            self.inserted.lock().expect("inserted virtual ids").clone()
        }
    }

    impl VirtualMessageStore for StoreStub {
        type Error = io::Error;

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
                self.inserted
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?
                    .push((vmsg_id, chat_id, thread_id));
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

    fn method_as_value(method: openplotva_telegram::TelegramOutboundMethod) -> io::Result<Value> {
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

    fn private_settings_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12345,
            "message": {
                "message_id": 77,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": "/settings",
                "entities": [
                    {
                        "offset": 0,
                        "length": 9,
                        "type": "bot_command"
                    }
                ]
            }
        }))
    }

    fn group_settings_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12346,
            "message": {
                "message_id": 78,
                "message_thread_id": 99,
                "date": 1_710_000_000,
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab"
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Ada",
                    "last_name": "Lovelace",
                    "username": "ada_l",
                    "language_code": "en",
                    "is_premium": true
                },
                "text": "/settings@PlotvaBot",
                "entities": [
                    {
                        "offset": 0,
                        "length": 19,
                        "type": "bot_command"
                    }
                ]
            }
        }))
    }

    fn same_chat_settings_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12347,
            "message": {
                "message_id": 78,
                "date": 1_710_000_000,
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab",
                    "username": "plotva_lab"
                },
                "sender_chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab",
                    "username": "plotva_lab"
                },
                "text": "/settings@PlotvaBot",
                "entities": [
                    {
                        "offset": 0,
                        "length": 19,
                        "type": "bot_command"
                    }
                ]
            }
        }))
    }

    fn missing_caller_group_settings_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12348,
            "message": {
                "message_id": 78,
                "date": 1_710_000_000,
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab"
                },
                "text": "/settings@PlotvaBot",
                "entities": [
                    {
                        "offset": 0,
                        "length": 19,
                        "type": "bot_command"
                    }
                ]
            }
        }))
    }

    fn group_settings_control_params() -> ControlJobParams {
        ControlJobParams {
            chat_id: -10042,
            message_id: 78,
            user_id: 42,
            user_full_name: "Ada Lovelace".to_owned(),
            thread_id: Some(99),
            data: ControlJobData {
                kind: ControlKind::GroupSettings,
                chat_type: "supergroup".to_owned(),
                user_name: "ada_l".to_owned(),
                first_name: "Ada".to_owned(),
                last_name: "Lovelace".to_owned(),
                language_code: "en".to_owned(),
                is_premium: true,
                ..ControlJobData::default()
            },
        }
    }

    #[derive(Clone, Default)]
    struct SettingsControlJobQueueStub {
        state: Arc<Mutex<SettingsControlJobQueueState>>,
    }

    #[derive(Default)]
    struct SettingsControlJobQueueState {
        assigned: Vec<(&'static str, StatelessJobItem)>,
        error: Option<StubError>,
    }

    impl SettingsControlJobQueueStub {
        fn failing() -> Self {
            Self {
                state: Arc::new(Mutex::new(SettingsControlJobQueueState {
                    error: Some(StubError),
                    ..SettingsControlJobQueueState::default()
                })),
            }
        }

        fn assigned(&self) -> Vec<(&'static str, StatelessJobItem)> {
            self.state
                .lock()
                .expect("settings control queue state")
                .assigned
                .clone()
        }
    }

    impl SettingsControlJobQueue for SettingsControlJobQueueStub {
        type Error = StubError;

        fn assign_settings_control_job<'a>(
            &'a self,
            queue_name: &'static str,
            job: StatelessJobItem,
        ) -> SettingsControlJobQueueFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("settings control queue state");
                state.assigned.push((queue_name, job));
                match state.error.take() {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            })
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct StubError;

    impl fmt::Display for StubError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("request failed")
        }
    }

    #[derive(Clone)]
    struct GroupSettingsEffectsStub {
        state: Arc<Mutex<GroupSettingsEffectsState>>,
    }

    #[derive(Default)]
    struct GroupSettingsEffectsState {
        allow: bool,
        fail_permission_check: bool,
        permission_checks: Vec<(i64, i64)>,
        synced_admin_chats: Vec<i64>,
    }

    impl GroupSettingsEffectsStub {
        fn allowing() -> Self {
            Self::with_state(GroupSettingsEffectsState {
                allow: true,
                ..GroupSettingsEffectsState::default()
            })
        }

        fn denying() -> Self {
            Self::with_state(GroupSettingsEffectsState::default())
        }

        fn failing_permission_check() -> Self {
            Self::with_state(GroupSettingsEffectsState {
                fail_permission_check: true,
                ..GroupSettingsEffectsState::default()
            })
        }

        fn with_state(state: GroupSettingsEffectsState) -> Self {
            Self {
                state: Arc::new(Mutex::new(state)),
            }
        }

        fn permission_checks(&self) -> Vec<(i64, i64)> {
            self.state
                .lock()
                .expect("group settings effects state")
                .permission_checks
                .clone()
        }

        fn synced_admin_chats(&self) -> Vec<i64> {
            self.state
                .lock()
                .expect("group settings effects state")
                .synced_admin_chats
                .clone()
        }
    }

    impl super::GroupSettingsControlJobEffects for GroupSettingsEffectsStub {
        type Error = StubError;

        fn can_open_group_settings<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> super::GroupSettingsControlJobFuture<'a, bool, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("group settings effects state");
                state.permission_checks.push((chat_id, user_id));
                if state.fail_permission_check {
                    Err(StubError)
                } else {
                    Ok(state.allow)
                }
            })
        }

        fn sync_chat_admins<'a>(&'a self, chat_id: i64) -> super::GroupSettingsSyncFuture<'a> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("group settings effects state")
                    .synced_admin_chats
                    .push(chat_id);
            })
        }
    }
}
