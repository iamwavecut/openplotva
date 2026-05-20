use carapax::{
    api::{Client, ExecuteError},
    types::{
        AnswerCallbackQuery, AnswerGuestQuery, AnswerInlineQuery, DeleteMessage,
        EditMessageCaption, EditMessageMedia, EditMessageReplyMarkup, EditMessageResult,
        EditMessageText, Message, SendAudio, SendChatAction, SendMediaGroup, SendMessage,
        SendPhoto, SendSticker, SentGuestMessage,
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
    /// Telegram `sendChatAction`.
    SendChatAction(Box<SendChatAction>),
    /// Telegram `answerCallbackQuery`.
    AnswerCallbackQuery(Box<AnswerCallbackQuery>),
    /// Telegram `answerInlineQuery`.
    AnswerInlineQuery(Box<AnswerInlineQuery>),
    /// Telegram `answerGuestQuery`.
    AnswerGuestQuery(Box<AnswerGuestQuery>),
    /// Telegram `editMessageText`.
    EditMessageText(Box<EditMessageText>),
    /// Telegram `editMessageCaption`.
    EditMessageCaption(Box<EditMessageCaption>),
    /// Telegram `editMessageReplyMarkup`.
    EditMessageReplyMarkup(Box<EditMessageReplyMarkup>),
    /// Telegram `editMessageMedia`.
    EditMessageMedia(Box<EditMessageMedia>),
    /// Telegram `deleteMessage`.
    DeleteMessage(Box<DeleteMessage>),
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
    /// Telegram `sendChatAction`.
    SendChatAction,
    /// Telegram `answerCallbackQuery`.
    AnswerCallbackQuery,
    /// Telegram `answerInlineQuery`.
    AnswerInlineQuery,
    /// Telegram `answerGuestQuery`.
    AnswerGuestQuery,
    /// Telegram `editMessageText`.
    EditMessageText,
    /// Telegram `editMessageCaption`.
    EditMessageCaption,
    /// Telegram `editMessageReplyMarkup`.
    EditMessageReplyMarkup,
    /// Telegram `editMessageMedia`.
    EditMessageMedia,
    /// Telegram `deleteMessage`.
    DeleteMessage,
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
    /// Methods returning a boolean success flag.
    Boolean,
    /// Telegram `answerGuestQuery` sent-message result.
    SentGuestMessage,
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
    /// Boolean success flag.
    Boolean(bool),
    /// Inline guest message response from `answerGuestQuery`.
    SentGuestMessage(Box<SentGuestMessage>),
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
            Self::SendChatAction(_) => TelegramOutboundMethodKind::SendChatAction,
            Self::AnswerCallbackQuery(_) => TelegramOutboundMethodKind::AnswerCallbackQuery,
            Self::AnswerInlineQuery(_) => TelegramOutboundMethodKind::AnswerInlineQuery,
            Self::AnswerGuestQuery(_) => TelegramOutboundMethodKind::AnswerGuestQuery,
            Self::EditMessageText(_) => TelegramOutboundMethodKind::EditMessageText,
            Self::EditMessageCaption(_) => TelegramOutboundMethodKind::EditMessageCaption,
            Self::EditMessageReplyMarkup(_) => TelegramOutboundMethodKind::EditMessageReplyMarkup,
            Self::EditMessageMedia(_) => TelegramOutboundMethodKind::EditMessageMedia,
            Self::DeleteMessage(_) => TelegramOutboundMethodKind::DeleteMessage,
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
            Self::SendChatAction(_) => "sendChatAction",
            Self::AnswerCallbackQuery(_) => "answerCallbackQuery",
            Self::AnswerInlineQuery(_) => "answerInlineQuery",
            Self::AnswerGuestQuery(_) => "answerGuestQuery",
            Self::EditMessageText(_) => "editMessageText",
            Self::EditMessageCaption(_) => "editMessageCaption",
            Self::EditMessageReplyMarkup(_) => "editMessageReplyMarkup",
            Self::EditMessageMedia(_) => "editMessageMedia",
            Self::DeleteMessage(_) => "deleteMessage",
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
            Self::SendChatAction(_) | Self::AnswerCallbackQuery(_) | Self::AnswerInlineQuery(_) => {
                TelegramOutboundResponseKind::Boolean
            }
            Self::AnswerGuestQuery(_) => TelegramOutboundResponseKind::SentGuestMessage,
            Self::EditMessageText(_)
            | Self::EditMessageCaption(_)
            | Self::EditMessageReplyMarkup(_)
            | Self::EditMessageMedia(_) => TelegramOutboundResponseKind::EditMessage,
            Self::DeleteMessage(_) => TelegramOutboundResponseKind::Boolean,
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
        TelegramOutboundMethod::SendChatAction(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
        TelegramOutboundMethod::AnswerCallbackQuery(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
        TelegramOutboundMethod::AnswerInlineQuery(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
        TelegramOutboundMethod::AnswerGuestQuery(method) => client
            .execute(*method)
            .await
            .map(|message| TelegramOutboundResponse::SentGuestMessage(Box::new(message))),
        TelegramOutboundMethod::EditMessageText(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::EditMessage),
        TelegramOutboundMethod::EditMessageCaption(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::EditMessage),
        TelegramOutboundMethod::EditMessageReplyMarkup(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::EditMessage),
        TelegramOutboundMethod::EditMessageMedia(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::EditMessage),
        TelegramOutboundMethod::DeleteMessage(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
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

impl From<SendChatAction> for TelegramOutboundMethod {
    fn from(value: SendChatAction) -> Self {
        Self::SendChatAction(Box::new(value))
    }
}

impl From<AnswerCallbackQuery> for TelegramOutboundMethod {
    fn from(value: AnswerCallbackQuery) -> Self {
        Self::AnswerCallbackQuery(Box::new(value))
    }
}

impl From<AnswerInlineQuery> for TelegramOutboundMethod {
    fn from(value: AnswerInlineQuery) -> Self {
        Self::AnswerInlineQuery(Box::new(value))
    }
}

impl From<AnswerGuestQuery> for TelegramOutboundMethod {
    fn from(value: AnswerGuestQuery) -> Self {
        Self::AnswerGuestQuery(Box::new(value))
    }
}

impl From<EditMessageText> for TelegramOutboundMethod {
    fn from(value: EditMessageText) -> Self {
        Self::EditMessageText(Box::new(value))
    }
}

impl From<EditMessageCaption> for TelegramOutboundMethod {
    fn from(value: EditMessageCaption) -> Self {
        Self::EditMessageCaption(Box::new(value))
    }
}

impl From<EditMessageReplyMarkup> for TelegramOutboundMethod {
    fn from(value: EditMessageReplyMarkup) -> Self {
        Self::EditMessageReplyMarkup(Box::new(value))
    }
}

impl From<EditMessageMedia> for TelegramOutboundMethod {
    fn from(value: EditMessageMedia) -> Self {
        Self::EditMessageMedia(Box::new(value))
    }
}

impl From<DeleteMessage> for TelegramOutboundMethod {
    fn from(value: DeleteMessage) -> Self {
        Self::DeleteMessage(Box::new(value))
    }
}

#[cfg(test)]
mod tests {
    use super::{TelegramOutboundMethod, TelegramOutboundMethodKind, TelegramOutboundResponseKind};
    use crate::{
        AudioMessageRequest, AudioSource, CallbackAnswerRequest, ChatActionRequest, ChatRef,
        EditCaptionMessageRequest, EditMediaMessageRequest, EditReplyMarkupMessageRequest,
        EditTextMessageRequest, GuestQueryAnswerRequest, InlineArticleRequest,
        InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryAnswerRequest,
        MediaGroupMessageRequest, MediaGroupPhotoItem, PhotoMessageRequest, PhotoSource,
        StickerMessageRequest, TELEGRAM_PARSE_MODE_HTML, TextMessageRequest,
        build_audio_message_method, build_callback_answer_method, build_chat_action_method,
        build_edit_caption_message_method, build_edit_media_message_method,
        build_edit_reply_markup_message_method, build_edit_text_message_method,
        build_guest_query_answer_method, build_inline_query_answer_method,
        build_inline_query_result_article, build_media_group_message_method,
        build_photo_message_method, build_sticker_message_method, build_text_message_method,
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
        let edit_caption = TelegramOutboundMethod::from(
            build_edit_caption_message_method(&EditCaptionMessageRequest {
                chat: chat(42),
                message_id: 7,
                caption: "<b>caption</b>".to_owned(),
                render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                reply_markup: None,
            })
            .expect("edit caption method"),
        );
        let edit_reply_markup = TelegramOutboundMethod::from(
            build_edit_reply_markup_message_method(&EditReplyMarkupMessageRequest {
                chat: chat(42),
                message_id: 7,
                reply_markup: InlineKeyboardMarkup::from([[
                    InlineKeyboardButton::for_callback_data("ok", "ok"),
                ]]),
            })
            .expect("edit reply markup method"),
        );
        let chat_action = TelegramOutboundMethod::from(
            build_chat_action_method(&ChatActionRequest {
                chat: chat(42),
                message_thread_id: 0,
                action: "typing".to_owned(),
            })
            .expect("chat action method"),
        );
        let callback_answer =
            TelegramOutboundMethod::from(build_callback_answer_method(&CallbackAnswerRequest {
                callback_query_id: "query-id".to_owned(),
                text: "ack".to_owned(),
                show_alert: false,
                url: String::new(),
                cache_time: 0,
            }));
        let inline_article = build_inline_query_result_article(&InlineArticleRequest {
            id: "inline-id".to_owned(),
            title: "Шевелись, Плотва!".to_owned(),
            message_text: "raw query".to_owned(),
            render_as: String::new(),
            description: String::new(),
            reply_markup: None,
        })
        .expect("inline article");
        let inline_answer = TelegramOutboundMethod::from(build_inline_query_answer_method(
            &InlineQueryAnswerRequest {
                inline_query_id: "inline-id".to_owned(),
                results: vec![inline_article.clone()],
                cache_time: 1,
                is_personal: true,
                next_offset: String::new(),
            },
        ));
        let guest_answer = TelegramOutboundMethod::from(build_guest_query_answer_method(
            &GuestQueryAnswerRequest {
                guest_query_id: "guest-query".to_owned(),
                result: inline_article,
            },
        ));
        let delete_message =
            TelegramOutboundMethod::from(carapax::types::DeleteMessage::new(42, 7));

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
            (
                edit_caption,
                TelegramOutboundMethodKind::EditMessageCaption,
                "editMessageCaption",
                TelegramOutboundResponseKind::EditMessage,
            ),
            (
                edit_reply_markup,
                TelegramOutboundMethodKind::EditMessageReplyMarkup,
                "editMessageReplyMarkup",
                TelegramOutboundResponseKind::EditMessage,
            ),
            (
                chat_action,
                TelegramOutboundMethodKind::SendChatAction,
                "sendChatAction",
                TelegramOutboundResponseKind::Boolean,
            ),
            (
                callback_answer,
                TelegramOutboundMethodKind::AnswerCallbackQuery,
                "answerCallbackQuery",
                TelegramOutboundResponseKind::Boolean,
            ),
            (
                inline_answer,
                TelegramOutboundMethodKind::AnswerInlineQuery,
                "answerInlineQuery",
                TelegramOutboundResponseKind::Boolean,
            ),
            (
                guest_answer,
                TelegramOutboundMethodKind::AnswerGuestQuery,
                "answerGuestQuery",
                TelegramOutboundResponseKind::SentGuestMessage,
            ),
            (
                delete_message,
                TelegramOutboundMethodKind::DeleteMessage,
                "deleteMessage",
                TelegramOutboundResponseKind::Boolean,
            ),
        ];

        for (method, kind, name, response_kind) in cases {
            assert_eq!(method.kind(), kind);
            assert_eq!(method.method_name(), name);
            assert_eq!(method.response_kind(), response_kind);
        }
    }
}
