//! App-level fetcher settings command behavior.

use carapax::types::{Chat as TelegramChat, Update as TelegramUpdate, UpdateType};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, OutboundBuildError, ReplyMarkup, ReplyMessageRef, TextMessageRequest,
};

use crate::virtual_messages::{
    QueueTextReport, QueueTextRequest, VirtualMessageStore, queue_text_message_parts,
};

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

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        future::Future,
        io,
        pin::Pin,
        sync::{Arc, Mutex},
    };

    use carapax::types::Update as TelegramUpdate;
    use openplotva_core::MessageIdMapping;
    use openplotva_telegram::{DispatcherConfig, DispatcherQueue, TelegramOutboundMethodKind};
    use serde_json::{Value, json};

    use crate::virtual_messages::VirtualMessageStore;

    use super::{SettingsCommandOutcome, handle_private_settings_command_update};

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
    async fn settings_command_in_group_is_left_for_taskman_slice() -> Result<(), Box<dyn Error>> {
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
                    "username": "ada_l"
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
}
