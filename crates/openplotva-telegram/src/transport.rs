use carapax::{
    api::{Client, ExecuteError},
    types::{
        EditMessageMedia, EditMessageResult, EditMessageText, Message, SendAudio, SendMediaGroup,
        SendMessage, SendPhoto, SendSticker,
    },
};

use crate::DispatcherSendStatus;

/// Concrete outbound Telegram methods currently queued by the Rust dispatcher.
#[derive(Debug)]
pub enum TelegramOutboundMethod {
    /// Telegram `sendMessage`.
    SendMessage(Box<SendMessage>),
    /// Telegram `sendSticker`.
    SendSticker(Box<SendSticker>),
    /// Telegram `sendPhoto`.
    SendPhoto(Box<SendPhoto>),
    /// Telegram `sendAudio`.
    SendAudio(Box<SendAudio>),
    /// Telegram `sendMediaGroup`.
    SendMediaGroup(Box<SendMediaGroup>),
    /// Telegram `editMessageText`.
    EditMessageText(Box<EditMessageText>),
    /// Telegram `editMessageMedia`.
    EditMessageMedia(Box<EditMessageMedia>),
}

/// Stable method discriminator for tests, metrics, and future persistence metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramOutboundMethodKind {
    /// Telegram `sendMessage`.
    SendMessage,
    /// Telegram `sendSticker`.
    SendSticker,
    /// Telegram `sendPhoto`.
    SendPhoto,
    /// Telegram `sendAudio`.
    SendAudio,
    /// Telegram `sendMediaGroup`.
    SendMediaGroup,
    /// Telegram `editMessageText`.
    EditMessageText,
    /// Telegram `editMessageMedia`.
    EditMessageMedia,
}

/// Response shape returned by a concrete Telegram outbound method.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramOutboundResponseKind {
    /// Methods returning one Telegram message.
    Message,
    /// Methods returning multiple Telegram messages.
    Messages,
    /// Telegram edit methods returning either a message or `true`.
    EditMessage,
}

/// Concrete successful response from executing an outbound Telegram method.
#[derive(Clone, Debug)]
pub enum TelegramOutboundResponse {
    /// One Telegram message.
    Message(Box<Message>),
    /// Multiple Telegram messages from an album send.
    Messages(Vec<Message>),
    /// Edit result from `editMessage*`.
    EditMessage(EditMessageResult),
}

impl TelegramOutboundMethod {
    /// Return the stable method discriminator.
    pub fn kind(&self) -> TelegramOutboundMethodKind {
        match self {
            Self::SendMessage(_) => TelegramOutboundMethodKind::SendMessage,
            Self::SendSticker(_) => TelegramOutboundMethodKind::SendSticker,
            Self::SendPhoto(_) => TelegramOutboundMethodKind::SendPhoto,
            Self::SendAudio(_) => TelegramOutboundMethodKind::SendAudio,
            Self::SendMediaGroup(_) => TelegramOutboundMethodKind::SendMediaGroup,
            Self::EditMessageText(_) => TelegramOutboundMethodKind::EditMessageText,
            Self::EditMessageMedia(_) => TelegramOutboundMethodKind::EditMessageMedia,
        }
    }

    /// Return the Bot API method name used by `tgbot::api::Method::into_payload`.
    pub fn method_name(&self) -> &'static str {
        match self {
            Self::SendMessage(_) => "sendMessage",
            Self::SendSticker(_) => "sendSticker",
            Self::SendPhoto(_) => "sendPhoto",
            Self::SendAudio(_) => "sendAudio",
            Self::SendMediaGroup(_) => "sendMediaGroup",
            Self::EditMessageText(_) => "editMessageText",
            Self::EditMessageMedia(_) => "editMessageMedia",
        }
    }

    /// Return the expected successful response shape.
    pub fn response_kind(&self) -> TelegramOutboundResponseKind {
        match self {
            Self::SendMessage(_)
            | Self::SendSticker(_)
            | Self::SendPhoto(_)
            | Self::SendAudio(_) => TelegramOutboundResponseKind::Message,
            Self::SendMediaGroup(_) => TelegramOutboundResponseKind::Messages,
            Self::EditMessageText(_) | Self::EditMessageMedia(_) => {
                TelegramOutboundResponseKind::EditMessage
            }
        }
    }

    /// Execute this method with a `carapax` API client.
    pub async fn execute_with(
        self,
        client: &Client,
    ) -> Result<TelegramOutboundResponse, ExecuteError> {
        execute_telegram_method(client, self).await
    }
}

/// Execute one concrete outbound Telegram method through `carapax`.
pub async fn execute_telegram_method(
    client: &Client,
    method: TelegramOutboundMethod,
) -> Result<TelegramOutboundResponse, ExecuteError> {
    match method {
        TelegramOutboundMethod::SendMessage(method) => client
            .execute(*method)
            .await
            .map(|message| TelegramOutboundResponse::Message(Box::new(message))),
        TelegramOutboundMethod::SendSticker(method) => client
            .execute(*method)
            .await
            .map(|message| TelegramOutboundResponse::Message(Box::new(message))),
        TelegramOutboundMethod::SendPhoto(method) => client
            .execute(*method)
            .await
            .map(|message| TelegramOutboundResponse::Message(Box::new(message))),
        TelegramOutboundMethod::SendAudio(method) => client
            .execute(*method)
            .await
            .map(|message| TelegramOutboundResponse::Message(Box::new(message))),
        TelegramOutboundMethod::SendMediaGroup(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Messages),
        TelegramOutboundMethod::EditMessageText(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::EditMessage),
        TelegramOutboundMethod::EditMessageMedia(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::EditMessage),
    }
}

/// Execute one outbound method and collapse the result into dispatcher accounting status.
pub async fn send_telegram_method_status(
    client: &Client,
    method: TelegramOutboundMethod,
) -> DispatcherSendStatus {
    match execute_telegram_method(client, method).await {
        Ok(_) => DispatcherSendStatus::Sent,
        Err(_) => DispatcherSendStatus::Failed,
    }
}

impl From<SendMessage> for TelegramOutboundMethod {
    fn from(value: SendMessage) -> Self {
        Self::SendMessage(Box::new(value))
    }
}

impl From<SendSticker> for TelegramOutboundMethod {
    fn from(value: SendSticker) -> Self {
        Self::SendSticker(Box::new(value))
    }
}

impl From<SendPhoto> for TelegramOutboundMethod {
    fn from(value: SendPhoto) -> Self {
        Self::SendPhoto(Box::new(value))
    }
}

impl From<SendAudio> for TelegramOutboundMethod {
    fn from(value: SendAudio) -> Self {
        Self::SendAudio(Box::new(value))
    }
}

impl From<SendMediaGroup> for TelegramOutboundMethod {
    fn from(value: SendMediaGroup) -> Self {
        Self::SendMediaGroup(Box::new(value))
    }
}

impl From<EditMessageText> for TelegramOutboundMethod {
    fn from(value: EditMessageText) -> Self {
        Self::EditMessageText(Box::new(value))
    }
}

impl From<EditMessageMedia> for TelegramOutboundMethod {
    fn from(value: EditMessageMedia) -> Self {
        Self::EditMessageMedia(Box::new(value))
    }
}

#[cfg(test)]
mod tests {
    use super::{TelegramOutboundMethod, TelegramOutboundMethodKind, TelegramOutboundResponseKind};
    use crate::{
        AudioMessageRequest, AudioSource, ChatRef, EditMediaMessageRequest, EditTextMessageRequest,
        MediaGroupMessageRequest, MediaGroupPhotoItem, PhotoMessageRequest, PhotoSource,
        StickerMessageRequest, TextMessageRequest, build_audio_message_method,
        build_edit_media_message_method, build_edit_text_message_method,
        build_media_group_message_method, build_photo_message_method, build_sticker_message_method,
        build_text_message_method,
    };

    fn chat(id: i64) -> ChatRef {
        ChatRef {
            id,
            is_forum: false,
        }
    }

    fn photo_item(file_id: &str) -> MediaGroupPhotoItem {
        MediaGroupPhotoItem {
            photo: PhotoSource::FileId(file_id.to_owned()),
            caption: String::new(),
            render_as: String::new(),
            has_spoiler: false,
        }
    }

    #[test]
    fn outbound_method_metadata_names_the_real_bot_api_methods_and_response_shapes() {
        let text = TelegramOutboundMethod::from(
            build_text_message_method(
                &TextMessageRequest {
                    chat: Some(chat(42)),
                    message_thread_id: 0,
                    disable_notification: false,
                    allow_sending_without_reply: None,
                    text: "hello".to_owned(),
                    render_as: String::new(),
                    reply_markup: None,
                },
                chat(42),
                None,
                "hello",
                true,
            )
            .expect("text method"),
        );
        let sticker = TelegramOutboundMethod::from(
            build_sticker_message_method(
                &StickerMessageRequest {
                    chat: Some(chat(42)),
                    message_thread_id: 0,
                    disable_notification: false,
                    file_id: "sticker-id".to_owned(),
                },
                None,
            )
            .expect("sticker method"),
        );
        let photo = TelegramOutboundMethod::from(
            build_photo_message_method(&PhotoMessageRequest {
                chat: chat(42),
                message_thread_id: 0,
                disable_notification: false,
                photo: PhotoSource::FileId("photo-id".to_owned()),
                caption: String::new(),
                render_as: String::new(),
                has_spoiler: false,
                reply_parameters: None,
            })
            .expect("photo method"),
        );
        let audio = TelegramOutboundMethod::from(
            build_audio_message_method(&AudioMessageRequest {
                chat: chat(42),
                message_thread_id: 0,
                disable_notification: false,
                audio: AudioSource::FileId("audio-id".to_owned()),
                caption: String::new(),
                render_as: String::new(),
                reply_parameters: None,
            })
            .expect("audio method"),
        );
        let media_group = TelegramOutboundMethod::from(
            build_media_group_message_method(&MediaGroupMessageRequest {
                chat: chat(42),
                message_thread_id: 0,
                disable_notification: false,
                items: vec![photo_item("photo-1"), photo_item("photo-2")],
                reply_parameters: None,
            })
            .expect("media group method"),
        );
        let edit_text = TelegramOutboundMethod::from(
            build_edit_text_message_method(&EditTextMessageRequest {
                chat: chat(42),
                message_id: 7,
                text: "edited".to_owned(),
                render_as: String::new(),
                reply_markup: None,
            })
            .expect("edit text method"),
        );
        let edit_media = TelegramOutboundMethod::from(
            build_edit_media_message_method(&EditMediaMessageRequest {
                chat: chat(42),
                message_id: 7,
                media: photo_item("replacement-photo"),
            })
            .expect("edit media method"),
        );

        let cases = [
            (
                text,
                TelegramOutboundMethodKind::SendMessage,
                "sendMessage",
                TelegramOutboundResponseKind::Message,
            ),
            (
                sticker,
                TelegramOutboundMethodKind::SendSticker,
                "sendSticker",
                TelegramOutboundResponseKind::Message,
            ),
            (
                photo,
                TelegramOutboundMethodKind::SendPhoto,
                "sendPhoto",
                TelegramOutboundResponseKind::Message,
            ),
            (
                audio,
                TelegramOutboundMethodKind::SendAudio,
                "sendAudio",
                TelegramOutboundResponseKind::Message,
            ),
            (
                media_group,
                TelegramOutboundMethodKind::SendMediaGroup,
                "sendMediaGroup",
                TelegramOutboundResponseKind::Messages,
            ),
            (
                edit_text,
                TelegramOutboundMethodKind::EditMessageText,
                "editMessageText",
                TelegramOutboundResponseKind::EditMessage,
            ),
            (
                edit_media,
                TelegramOutboundMethodKind::EditMessageMedia,
                "editMessageMedia",
                TelegramOutboundResponseKind::EditMessage,
            ),
        ];

        for (method, kind, name, response_kind) in cases {
            assert_eq!(method.kind(), kind);
            assert_eq!(method.method_name(), name);
            assert_eq!(method.response_kind(), response_kind);
        }
    }
}
