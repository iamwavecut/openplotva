//! App-level fetcher `/delete_drawing` command behavior.

use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use carapax::types::{
    CallbackQuery as TelegramCallbackQuery, ChatMember as TelegramChatMember,
    MaybeInaccessibleMessage, Message as TelegramMessage, MessageData as TelegramMessageData,
    Text as TelegramText, TextEntity as TelegramTextEntity,
    TextEntityPosition as TelegramTextEntityPosition, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType,
};
use openplotva_telegram::{
    CallbackActionParse, CallbackAnswerRequest, ChatRef, DeleteMessageRequest, DispatcherQueue,
    EditCaptionMessageRequest, EditReplyMarkupMessageRequest, EditTextMessageRequest,
    OutboundBuildError, ReplyMarkup, ReplyMessageRef, TELEGRAM_PARSE_MODE_HTML,
    TelegramOutboundMethod, TextMessageRequest, build_callback_answer_method,
    build_delete_drawing_confirm_keyboard, build_delete_drawing_frame_confirm_keyboard,
    build_delete_drawing_frame_picker_keyboard, build_delete_drawing_initial_keyboard,
    build_delete_message_method, build_edit_caption_message_method,
    build_edit_reply_markup_message_method, build_edit_text_message_method,
    build_get_chat_member_method, parse_callback_action, parse_callback_i64,
};
use thiserror::Error;
use time::OffsetDateTime;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use crate::virtual_messages::{
    QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory, queue_text_message_parts,
};

const DELETE_DRAWING_COMMAND: &str = "delete_drawing";
const DELETE_DRAWING_NO_GENERATION_TEXT: &str = "❌ У вас нет недавних генераций в этом чате.";
const DELETE_DRAWING_AUTH_ALERT_TEXT: &str =
    "Только автор или администратор может удалить это изображение";
const DELETE_DRAWING_NOT_FOUND_TEXT: &str = "Генерация не найдена";
const DELETE_DRAWING_VIP_ALERT_TEXT: &str =
    "Удаление отдельных кадров доступно только VIP пользователям";
const DELETE_DRAWING_ALL_DELETED_TEXT: &str = "✅ Удалено";
const DELETE_DRAWING_ALL_FRAMES_DELETED_TEXT: &str = "✅ Все кадры удалены";
const DELETE_DRAWING_INVALID_FRAME_TEXT: &str = "Ошибка: неверный номер кадра";
const DELETE_DRAWING_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LastGeneration {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Generated Telegram message IDs, in frame order.
    pub message_ids: Vec<i64>,
    /// Generation caption, used by callback handling.
    pub caption: String,
    pub created_at: i64,
}

/// Planned `/delete_drawing` text send.
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteDrawingTextPlan {
    pub message: TextMessageRequest,
    pub reply_to: ReplyMessageRef,
    pub ephemeral_delete_after: Duration,
}

/// Routing result after consulting the last-generation store.
#[derive(Clone, Debug, PartialEq)]
pub enum DeleteDrawingCommandPlan {
    /// Message does not belong to this slice.
    NotHandled,
    NoGeneration(DeleteDrawingTextPlan),
    Control(DeleteDrawingTextPlan),
}

/// Boxed future returned by last-generation stores.
pub type DeleteDrawingStoreFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Option<LastGeneration>, E>> + Send + 'a>>;

/// Boxed future returned by last-generation write stores.
pub type DeleteDrawingStoreWriteFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait DeleteDrawingLastGenerationStore {
    /// Concrete store error.
    type Error: fmt::Display + Send;

    /// Load the last generation for `(chat_id, user_id)`.
    fn last_generation<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> DeleteDrawingStoreFuture<'a, Self::Error>;
}

pub trait DeleteDrawingCallbackStore: DeleteDrawingLastGenerationStore {
    /// Persist an updated last-generation value.
    fn set_last_generation<'a>(
        &'a self,
        generation: LastGeneration,
    ) -> DeleteDrawingStoreWriteFuture<'a, Self::Error>;

    /// Delete the last-generation value for `(chat_id, user_id)`.
    fn delete_last_generation<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> DeleteDrawingStoreWriteFuture<'a, Self::Error>;
}

impl DeleteDrawingLastGenerationStore for openplotva_storage::RedisLastGenerationStore {
    type Error = openplotva_storage::StorageError;

    fn last_generation<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> DeleteDrawingStoreFuture<'a, Self::Error> {
        Box::pin(async move {
            openplotva_storage::RedisLastGenerationStore::last_generation(self, chat_id, user_id)
                .await
                .map(|generation| {
                    generation.map(|generation| LastGeneration {
                        chat_id: generation.chat_id,
                        user_id: generation.user_id,
                        message_ids: generation.message_ids,
                        caption: generation.caption,
                        created_at: generation.created_at,
                    })
                })
        })
    }
}

impl DeleteDrawingCallbackStore for openplotva_storage::RedisLastGenerationStore {
    fn set_last_generation<'a>(
        &'a self,
        generation: LastGeneration,
    ) -> DeleteDrawingStoreWriteFuture<'a, Self::Error> {
        Box::pin(async move {
            openplotva_storage::RedisLastGenerationStore::set_last_generation(
                self,
                &openplotva_storage::LastGenerationRecord {
                    chat_id: generation.chat_id,
                    user_id: generation.user_id,
                    message_ids: generation.message_ids,
                    caption: generation.caption,
                    created_at: generation.created_at,
                },
            )
            .await
        })
    }

    fn delete_last_generation<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> DeleteDrawingStoreWriteFuture<'a, Self::Error> {
        Box::pin(async move {
            openplotva_storage::RedisLastGenerationStore::delete_last_generation(
                self, chat_id, user_id,
            )
            .await
        })
    }
}

/// Boxed future returned by Telegram membership APIs.
pub type DeleteDrawingMemberFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<TelegramChatMember, E>> + Send + 'a>>;

pub trait DeleteDrawingMemberApi {
    /// Concrete API error.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Fetch one Telegram chat member.
    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> DeleteDrawingMemberFuture<'a, Self::Error>;
}

impl DeleteDrawingMemberApi for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> DeleteDrawingMemberFuture<'a, Self::Error> {
        Box::pin(async move {
            self.execute(build_get_chat_member_method(chat_id, user_id))
                .await
        })
    }
}

/// Boxed future returned by VIP status lookups.
pub type DeleteDrawingVipFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

pub trait DeleteDrawingVipStatus {
    /// Return whether the user has VIP capabilities.
    fn is_delete_drawing_vip<'a>(&'a self, user_id: i64) -> DeleteDrawingVipFuture<'a>;
}

impl<T> DeleteDrawingVipStatus for T
where
    T: crate::payments::VipStatusChecker + Sync,
{
    fn is_delete_drawing_vip<'a>(&'a self, user_id: i64) -> DeleteDrawingVipFuture<'a> {
        Box::pin(async move { self.is_vip_at(user_id, OffsetDateTime::now_utc()).await })
    }
}

/// Boxed future returned by delete-drawing effects.
pub type DeleteDrawingEffectFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait DeleteDrawingCommandEffects {
    /// Concrete effect error.
    type Error: fmt::Display + Send;

    fn send_delete_drawing_text<'a>(
        &'a self,
        plan: DeleteDrawingTextPlan,
    ) -> DeleteDrawingEffectFuture<'a, Self::Error>;
}

#[derive(Clone)]
pub struct DeleteDrawingCommandDispatcherEffects {
    queue: Arc<DispatcherQueue>,
    next_virtual_id: VirtualIdFactory,
}

impl DeleteDrawingCommandDispatcherEffects {
    /// Build delete-drawing command effects over the normal outbound dispatcher.
    #[must_use]
    pub fn new(queue: Arc<DispatcherQueue>) -> Self {
        Self {
            queue,
            next_virtual_id: monotonic_virtual_id_factory("delete-drawing-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl DeleteDrawingCommandEffects for DeleteDrawingCommandDispatcherEffects {
    type Error = OutboundBuildError;

    fn send_delete_drawing_text<'a>(
        &'a self,
        plan: DeleteDrawingTextPlan,
    ) -> DeleteDrawingEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            queue_text_message_parts(
                &self.queue,
                QueueTextRequest {
                    message: &plan.message,
                    reply_to: Some(&plan.reply_to),
                    immediate_first: true,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: Some(plan.ephemeral_delete_after),
                },
                || (self.next_virtual_id)(),
            )
            .await?;
            Ok(())
        })
    }
}

pub trait DeleteDrawingCallbackEffects {
    /// Concrete effect error.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Execute one direct Telegram Bot API method.
    fn execute_delete_drawing_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> DeleteDrawingEffectFuture<'a, Self::Error>;
}

impl DeleteDrawingCallbackEffects for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn execute_delete_drawing_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> DeleteDrawingEffectFuture<'a, Self::Error> {
        Box::pin(async move { method.execute_with(self).await.map(|_| ()) })
    }
}

/// Result of one decoded update through the delete-drawing wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteDrawingCommandOutcome {
    /// Update was not `/delete_drawing` and went downstream.
    Delegated,
    /// No-generation error was sent.
    NoGenerationSent,
    /// Delete control was sent.
    ControlSent,
    StoreErrorNoGeneration {
        message: String,
    },
    SendError {
        message: String,
    },
}

/// Fatal errors from the delete-drawing update wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DeleteDrawingCommandError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteDrawingCallbackData {
    pub action: String,
    pub user_id: i64,
    pub chat_id: i64,
    pub frame_num: i64,
}

/// Control message attached to a callback query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteDrawingControlMessage {
    /// Control message chat.
    pub chat: ChatRef,
    /// Control message ID.
    pub message_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteDrawingFramePlan {
    /// Telegram message ID selected for deletion.
    pub message_id: i64,
    /// Remaining generated frame message IDs.
    pub remaining_message_ids: Vec<i64>,
    /// Caption transfer target when deleting the first captioned frame.
    pub transfer_caption_to_message_id: Option<i64>,
    /// Whether the selected frame was the last remaining frame.
    pub delete_all: bool,
}

/// Result of one delete-drawing callback update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteDrawingCallbackOutcome {
    /// Update did not belong to this slice and was delegated.
    Delegated,
    /// Callback author was neither the generation author nor an admin with delete rights.
    Unauthorized,
    /// Generation lookup failed or returned no usable generation.
    NotFound,
    /// Initial delete UI was replaced with confirm/frame picker controls.
    InitShown {
        /// Whether the frame picker was shown instead of the all-delete confirm button.
        frame_picker: bool,
    },
    /// All generated frame messages were deleted.
    ConfirmedAll,
    /// VIP frame-confirm UI was shown.
    FramePickShown,
    /// Frame number was outside the current generation.
    InvalidFrame,
    /// One frame was deleted and the control message was updated.
    FrameDeleted {
        frame_num: i64,
        /// Remaining frame count.
        remaining: usize,
    },
    /// The final remaining frame was deleted.
    AllFramesDeleted,
    /// Control message was closed.
    Closed,
    UnknownAck,
}

/// Fatal errors from the delete-drawing callback wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DeleteDrawingCallbackError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct DeleteDrawingCallbackUpdateHandler<Store, MemberApi, Vip, Effects, Next> {
    store: Arc<Store>,
    member_api: Arc<MemberApi>,
    vip: Arc<Vip>,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Store, MemberApi, Vip, Effects, Next>
    DeleteDrawingCallbackUpdateHandler<Store, MemberApi, Vip, Effects, Next>
{
    /// Build a delete-drawing callback handler around the real downstream update handler.
    pub fn new(
        store: Arc<Store>,
        member_api: Arc<MemberApi>,
        vip: Arc<Vip>,
        effects: Arc<Effects>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            store,
            member_api,
            vip,
            effects,
            next,
        }
    }
}

impl<Store, MemberApi, Vip, Effects, Next> UpdateHandler
    for DeleteDrawingCallbackUpdateHandler<Store, MemberApi, Vip, Effects, Next>
where
    Store: DeleteDrawingCallbackStore + Send + Sync,
    MemberApi: DeleteDrawingMemberApi + Send + Sync,
    Vip: DeleteDrawingVipStatus + Send + Sync,
    Effects: DeleteDrawingCallbackEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = DeleteDrawingCallbackError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_delete_drawing_callback_update_or_else(
                self.store.as_ref(),
                self.member_api.as_ref(),
                self.vip.as_ref(),
                self.effects.as_ref(),
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Debug)]
pub struct DeleteDrawingCommandUpdateHandler<Store, Effects, Next> {
    store: Arc<Store>,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Store, Effects, Next> DeleteDrawingCommandUpdateHandler<Store, Effects, Next> {
    /// Build a delete-drawing handler around the real downstream update handler.
    pub fn new(store: Arc<Store>, effects: Arc<Effects>, next: Arc<Next>) -> Self {
        Self {
            store,
            effects,
            next,
        }
    }
}

impl<Store, Effects, Next> UpdateHandler for DeleteDrawingCommandUpdateHandler<Store, Effects, Next>
where
    Store: DeleteDrawingLastGenerationStore + Send + Sync,
    Effects: DeleteDrawingCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = DeleteDrawingCommandError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_delete_drawing_command_update_or_else(
                self.store.as_ref(),
                self.effects.as_ref(),
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_delete_drawing_callback_update_or_else<
    Store,
    MemberApi,
    Vip,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    store: &Store,
    member_api: &MemberApi,
    vip: &Vip,
    effects: &Effects,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<DeleteDrawingCallbackOutcome, DeleteDrawingCallbackError>
where
    Store: DeleteDrawingCallbackStore + Sync,
    MemberApi: DeleteDrawingMemberApi + Sync,
    Vip: DeleteDrawingVipStatus + Sync,
    Effects: DeleteDrawingCallbackEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let data = match &update.update_type {
        TelegramUpdateType::CallbackQuery(query) => delete_drawing_callback_data_from_query(query)
            .zip(delete_drawing_control_message(query)),
        _ => None,
    };

    let Some((data, control_message)) = data else {
        handle_other(update)
            .await
            .map_err(|error| DeleteDrawingCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DeleteDrawingCallbackOutcome::Delegated);
    };

    let TelegramUpdateType::CallbackQuery(query) = &update.update_type else {
        unreachable!("callback query was already matched");
    };

    Ok(handle_delete_drawing_callback(
        store,
        member_api,
        vip,
        effects,
        query,
        control_message,
        data,
    )
    .await)
}

pub async fn handle_delete_drawing_callback<Store, MemberApi, Vip, Effects>(
    store: &Store,
    member_api: &MemberApi,
    vip: &Vip,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    control_message: DeleteDrawingControlMessage,
    data: DeleteDrawingCallbackData,
) -> DeleteDrawingCallbackOutcome
where
    Store: DeleteDrawingCallbackStore + Sync,
    MemberApi: DeleteDrawingMemberApi + Sync,
    Vip: DeleteDrawingVipStatus + Sync,
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    if !is_authorized_for_delete_drawing(member_api, query, data.user_id, data.chat_id).await {
        try_answer_callback(
            effects,
            &query.id,
            DELETE_DRAWING_AUTH_ALERT_TEXT,
            true,
            "auth alert",
        )
        .await;
        return DeleteDrawingCallbackOutcome::Unauthorized;
    }

    match data.action.as_str() {
        openplotva_telegram::DELETE_DRAWING_ACTION_INIT => {
            handle_delete_drawing_init(store, vip, effects, query, control_message, &data).await
        }
        openplotva_telegram::DELETE_DRAWING_ACTION_CONFIRM => {
            handle_delete_drawing_confirm(store, effects, query, control_message, &data).await
        }
        openplotva_telegram::DELETE_DRAWING_ACTION_FRAME_PICK => {
            handle_delete_drawing_frame_pick(vip, effects, query, control_message, &data).await
        }
        openplotva_telegram::DELETE_DRAWING_ACTION_FRAME_CONFIRM => {
            handle_delete_drawing_frame_confirm(store, vip, effects, query, control_message, &data)
                .await
        }
        openplotva_telegram::DELETE_DRAWING_ACTION_CLOSE => {
            handle_delete_drawing_close(effects, query, control_message).await
        }
        _ => {
            try_answer_callback(effects, &query.id, "", false, "unknown delete action ack").await;
            DeleteDrawingCallbackOutcome::UnknownAck
        }
    }
}

pub async fn handle_delete_drawing_command_update_or_else<
    Store,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    store: &Store,
    effects: &Effects,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<DeleteDrawingCommandOutcome, DeleteDrawingCommandError>
where
    Store: DeleteDrawingLastGenerationStore + Sync,
    Effects: DeleteDrawingCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| DeleteDrawingCommandError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DeleteDrawingCommandOutcome::Delegated);
    };

    if !is_delete_drawing_command(message) {
        handle_other(update)
            .await
            .map_err(|error| DeleteDrawingCommandError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DeleteDrawingCommandOutcome::Delegated);
    }

    let Some(user_id) = message.sender.get_user().map(|user| i64::from(user.id)) else {
        return send_delete_drawing_plan(
            effects,
            DeleteDrawingCommandPlan::NoGeneration(no_generation_plan(message)),
            None,
        )
        .await;
    };

    let chat_id = message.chat.get_id().into();
    let generation = match store.last_generation(chat_id, user_id).await {
        Ok(generation) => generation,
        Err(error) => {
            return send_delete_drawing_plan(
                effects,
                DeleteDrawingCommandPlan::NoGeneration(no_generation_plan(message)),
                Some(error.to_string()),
            )
            .await;
        }
    };

    let plan = delete_drawing_command_plan(message, user_id, generation.as_ref());
    send_delete_drawing_plan(effects, plan, None).await
}

async fn send_delete_drawing_plan<Effects>(
    effects: &Effects,
    plan: DeleteDrawingCommandPlan,
    store_error: Option<String>,
) -> Result<DeleteDrawingCommandOutcome, DeleteDrawingCommandError>
where
    Effects: DeleteDrawingCommandEffects + Sync,
{
    match plan {
        DeleteDrawingCommandPlan::NotHandled => Ok(DeleteDrawingCommandOutcome::Delegated),
        DeleteDrawingCommandPlan::NoGeneration(plan) => {
            match effects.send_delete_drawing_text(plan).await {
                Ok(()) => match store_error {
                    Some(message) => {
                        Ok(DeleteDrawingCommandOutcome::StoreErrorNoGeneration { message })
                    }
                    None => Ok(DeleteDrawingCommandOutcome::NoGenerationSent),
                },
                Err(error) => Ok(DeleteDrawingCommandOutcome::SendError {
                    message: error.to_string(),
                }),
            }
        }
        DeleteDrawingCommandPlan::Control(plan) => {
            match effects.send_delete_drawing_text(plan).await {
                Ok(()) => Ok(DeleteDrawingCommandOutcome::ControlSent),
                Err(error) => Ok(DeleteDrawingCommandOutcome::SendError {
                    message: error.to_string(),
                }),
            }
        }
    }
}

async fn handle_delete_drawing_init<Store, Vip, Effects>(
    store: &Store,
    vip: &Vip,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    control_message: DeleteDrawingControlMessage,
    data: &DeleteDrawingCallbackData,
) -> DeleteDrawingCallbackOutcome
where
    Store: DeleteDrawingCallbackStore + Sync,
    Vip: DeleteDrawingVipStatus + Sync,
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    let Some(generation) = load_callback_generation(store, data, true).await else {
        try_answer_callback(
            effects,
            &query.id,
            DELETE_DRAWING_NOT_FOUND_TEXT,
            true,
            "not found",
        )
        .await;
        return DeleteDrawingCallbackOutcome::NotFound;
    };

    let is_vip = vip.is_delete_drawing_vip(i64::from(query.from.id)).await;
    let frame_picker = is_vip && generation.message_ids.len() > 1;
    let reply_markup = if frame_picker {
        build_delete_drawing_frame_picker_keyboard(
            data.user_id,
            data.chat_id,
            generation.message_ids.len() as i64,
        )
    } else {
        build_delete_drawing_confirm_keyboard(data.user_id, data.chat_id)
    };

    try_edit_reply_markup(
        effects,
        EditReplyMarkupMessageRequest {
            chat: control_message.chat,
            message_id: control_message.message_id,
            reply_markup,
        },
        "delete drawing keyboard",
    )
    .await;
    try_answer_callback(effects, &query.id, "", false, "delete init ack").await;
    DeleteDrawingCallbackOutcome::InitShown { frame_picker }
}

async fn handle_delete_drawing_confirm<Store, Effects>(
    store: &Store,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    control_message: DeleteDrawingControlMessage,
    data: &DeleteDrawingCallbackData,
) -> DeleteDrawingCallbackOutcome
where
    Store: DeleteDrawingCallbackStore + Sync,
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    let Some(generation) = load_callback_generation(store, data, false).await else {
        try_answer_callback(
            effects,
            &query.id,
            DELETE_DRAWING_NOT_FOUND_TEXT,
            true,
            "not found",
        )
        .await;
        return DeleteDrawingCallbackOutcome::NotFound;
    };

    for message_id in generation.message_ids {
        try_delete_message(effects, data.chat_id, message_id, "delete generated frame").await;
    }
    try_delete_message(
        effects,
        control_message.chat.id,
        control_message.message_id,
        "delete drawing control",
    )
    .await;
    try_delete_last_generation(store, data.chat_id, data.user_id).await;
    try_answer_callback(
        effects,
        &query.id,
        DELETE_DRAWING_ALL_DELETED_TEXT,
        false,
        "delete confirm ack",
    )
    .await;
    DeleteDrawingCallbackOutcome::ConfirmedAll
}

async fn handle_delete_drawing_frame_pick<Vip, Effects>(
    vip: &Vip,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    control_message: DeleteDrawingControlMessage,
    data: &DeleteDrawingCallbackData,
) -> DeleteDrawingCallbackOutcome
where
    Vip: DeleteDrawingVipStatus + Sync,
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    if !vip.is_delete_drawing_vip(i64::from(query.from.id)).await {
        try_answer_callback(
            effects,
            &query.id,
            DELETE_DRAWING_VIP_ALERT_TEXT,
            true,
            "VIP alert",
        )
        .await;
        return DeleteDrawingCallbackOutcome::Unauthorized;
    }

    try_edit_reply_markup(
        effects,
        EditReplyMarkupMessageRequest {
            chat: control_message.chat,
            message_id: control_message.message_id,
            reply_markup: build_delete_drawing_frame_confirm_keyboard(
                data.user_id,
                data.chat_id,
                data.frame_num,
            ),
        },
        "frame confirm keyboard",
    )
    .await;
    try_answer_callback(effects, &query.id, "", false, "frame pick ack").await;
    DeleteDrawingCallbackOutcome::FramePickShown
}

async fn handle_delete_drawing_frame_confirm<Store, Vip, Effects>(
    store: &Store,
    vip: &Vip,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    control_message: DeleteDrawingControlMessage,
    data: &DeleteDrawingCallbackData,
) -> DeleteDrawingCallbackOutcome
where
    Store: DeleteDrawingCallbackStore + Sync,
    Vip: DeleteDrawingVipStatus + Sync,
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    if !vip.is_delete_drawing_vip(i64::from(query.from.id)).await {
        try_answer_callback(
            effects,
            &query.id,
            DELETE_DRAWING_VIP_ALERT_TEXT,
            true,
            "VIP alert",
        )
        .await;
        return DeleteDrawingCallbackOutcome::Unauthorized;
    }

    let Some(generation) = load_callback_generation(store, data, true).await else {
        try_answer_callback(
            effects,
            &query.id,
            DELETE_DRAWING_NOT_FOUND_TEXT,
            true,
            "not found",
        )
        .await;
        return DeleteDrawingCallbackOutcome::NotFound;
    };

    let Some(plan) = plan_delete_drawing_frame(&generation, data.frame_num) else {
        try_answer_callback(
            effects,
            &query.id,
            DELETE_DRAWING_INVALID_FRAME_TEXT,
            false,
            "invalid frame ack",
        )
        .await;
        return DeleteDrawingCallbackOutcome::InvalidFrame;
    };

    if let Some(message_id) = plan.transfer_caption_to_message_id {
        try_edit_caption(
            effects,
            EditCaptionMessageRequest {
                chat: ChatRef {
                    id: data.chat_id,
                    is_forum: false,
                },
                message_id,
                caption: generation.caption.clone(),
                render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                reply_markup: None,
            },
            "transfer caption",
        )
        .await;
    }

    try_delete_message(effects, data.chat_id, plan.message_id, "delete frame").await;

    if plan.delete_all {
        try_delete_message(
            effects,
            control_message.chat.id,
            control_message.message_id,
            "delete drawing control",
        )
        .await;
        try_delete_last_generation(store, data.chat_id, data.user_id).await;
        try_answer_callback(
            effects,
            &query.id,
            DELETE_DRAWING_ALL_FRAMES_DELETED_TEXT,
            false,
            "all frames deleted ack",
        )
        .await;
        return DeleteDrawingCallbackOutcome::AllFramesDeleted;
    }

    let remaining = plan.remaining_message_ids.len();
    try_set_last_generation(
        store,
        LastGeneration {
            chat_id: data.chat_id,
            user_id: data.user_id,
            message_ids: plan.remaining_message_ids,
            caption: generation.caption,
            created_at: OffsetDateTime::now_utc().unix_timestamp(),
        },
    )
    .await;
    try_edit_text(
        effects,
        EditTextMessageRequest {
            chat: control_message.chat,
            message_id: control_message.message_id,
            text: format!("🗑 Управление генерацией ({remaining} кадр.)"),
            render_as: String::new(),
            reply_markup: Some(build_delete_drawing_frame_picker_keyboard(
                data.user_id,
                data.chat_id,
                remaining as i64,
            )),
        },
        "update frame picker",
    )
    .await;
    try_answer_callback(
        effects,
        &query.id,
        &format!("✅ Кадр #{} удалён", data.frame_num),
        false,
        "frame delete ack",
    )
    .await;
    DeleteDrawingCallbackOutcome::FrameDeleted {
        frame_num: data.frame_num,
        remaining,
    }
}

async fn handle_delete_drawing_close<Effects>(
    effects: &Effects,
    query: &TelegramCallbackQuery,
    control_message: DeleteDrawingControlMessage,
) -> DeleteDrawingCallbackOutcome
where
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    try_delete_message(
        effects,
        control_message.chat.id,
        control_message.message_id,
        "close delete drawing control",
    )
    .await;
    try_answer_callback(effects, &query.id, "", false, "close ack").await;
    DeleteDrawingCallbackOutcome::Closed
}

async fn load_callback_generation<Store>(
    store: &Store,
    data: &DeleteDrawingCallbackData,
    require_non_empty: bool,
) -> Option<LastGeneration>
where
    Store: DeleteDrawingLastGenerationStore + Sync,
{
    match store.last_generation(data.chat_id, data.user_id).await {
        Ok(Some(generation)) if !require_non_empty || !generation.message_ids.is_empty() => {
            Some(generation)
        }
        Ok(_) => None,
        Err(error) => {
            tracing::warn!(
                message = %error,
                chat_id = data.chat_id,
                user_id = data.user_id,
                "failed to load last generation for delete drawing"
            );
            None
        }
    }
}

async fn try_set_last_generation<Store>(store: &Store, generation: LastGeneration)
where
    Store: DeleteDrawingCallbackStore + Sync,
{
    let chat_id = generation.chat_id;
    let user_id = generation.user_id;
    if let Err(error) = store.set_last_generation(generation).await {
        tracing::warn!(
            message = %error,
            chat_id,
            user_id,
            "failed to update last generation after frame delete"
        );
    }
}

async fn try_delete_last_generation<Store>(store: &Store, chat_id: i64, user_id: i64)
where
    Store: DeleteDrawingCallbackStore + Sync,
{
    if let Err(error) = store.delete_last_generation(chat_id, user_id).await {
        tracing::warn!(
            message = %error,
            chat_id,
            user_id,
            "failed to delete last generation after drawing delete"
        );
    }
}

async fn try_answer_callback<Effects>(
    effects: &Effects,
    query_id: &str,
    text: &str,
    show_alert: bool,
    context: &'static str,
) where
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    let request = CallbackAnswerRequest {
        callback_query_id: query_id.to_owned(),
        text: text.to_owned(),
        show_alert,
        url: String::new(),
        cache_time: 0,
    };
    try_execute_delete_drawing_method(
        effects,
        build_callback_answer_method(&request).into(),
        context,
    )
    .await;
}

async fn try_edit_reply_markup<Effects>(
    effects: &Effects,
    request: EditReplyMarkupMessageRequest,
    context: &'static str,
) where
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    match build_edit_reply_markup_message_method(&request) {
        Ok(method) => try_execute_delete_drawing_method(effects, method.into(), context).await,
        Err(error) => tracing::warn!(message = %error, "failed to build delete drawing method"),
    }
}

async fn try_edit_text<Effects>(
    effects: &Effects,
    request: EditTextMessageRequest,
    context: &'static str,
) where
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    match build_edit_text_message_method(&request) {
        Ok(method) => try_execute_delete_drawing_method(effects, method.into(), context).await,
        Err(error) => tracing::warn!(message = %error, "failed to build delete drawing method"),
    }
}

async fn try_edit_caption<Effects>(
    effects: &Effects,
    request: EditCaptionMessageRequest,
    context: &'static str,
) where
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    match build_edit_caption_message_method(&request) {
        Ok(method) => try_execute_delete_drawing_method(effects, method.into(), context).await,
        Err(error) => tracing::warn!(message = %error, "failed to build delete drawing method"),
    }
}

async fn try_delete_message<Effects>(
    effects: &Effects,
    chat_id: i64,
    message_id: i64,
    context: &'static str,
) where
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    match build_delete_message_method(&DeleteMessageRequest {
        chat_id,
        message_id,
    }) {
        Ok(method) => try_execute_delete_drawing_method(effects, method.into(), context).await,
        Err(error) => tracing::warn!(message = %error, "failed to build delete drawing method"),
    }
}

async fn try_execute_delete_drawing_method<Effects>(
    effects: &Effects,
    method: TelegramOutboundMethod,
    context: &'static str,
) where
    Effects: DeleteDrawingCallbackEffects + Sync,
{
    let method_name = method.method_name();
    if let Err(error) = effects.execute_delete_drawing_method(method).await {
        tracing::warn!(
            message = %error,
            method = method_name,
            context,
            "delete drawing callback Telegram side effect failed"
        );
    }
}

async fn is_authorized_for_delete_drawing<MemberApi>(
    member_api: &MemberApi,
    query: &TelegramCallbackQuery,
    data_user_id: i64,
    data_chat_id: i64,
) -> bool
where
    MemberApi: DeleteDrawingMemberApi + Sync,
{
    let requester_id = i64::from(query.from.id);
    if requester_id == data_user_id {
        return true;
    }

    match member_api.get_chat_member(data_chat_id, requester_id).await {
        Ok(member) => telegram_member_can_delete_drawing(&member),
        Err(error) => {
            tracing::warn!(
                message = %error,
                chat_id = data_chat_id,
                user_id = requester_id,
                "failed to authorize delete drawing callback"
            );
            false
        }
    }
}

#[must_use]
pub fn telegram_member_can_delete_drawing(member: &TelegramChatMember) -> bool {
    match member {
        TelegramChatMember::Creator(_) => true,
        TelegramChatMember::Administrator(admin) => admin.can_delete_messages,
        TelegramChatMember::Kicked(_)
        | TelegramChatMember::Left(_)
        | TelegramChatMember::Member { .. }
        | TelegramChatMember::Restricted(_) => false,
    }
}

#[must_use]
pub fn plan_delete_drawing_frame(
    generation: &LastGeneration,
    frame_num: i64,
) -> Option<DeleteDrawingFramePlan> {
    let frame_index = usize::try_from(frame_num.checked_sub(1)?).ok()?;
    let message_id = generation.message_ids.get(frame_index).copied()?;
    let remaining_message_ids = generation
        .message_ids
        .iter()
        .enumerate()
        .filter_map(|(index, id)| (index != frame_index).then_some(*id))
        .collect::<Vec<_>>();
    let transfer_caption_to_message_id =
        (frame_index == 0 && generation.message_ids.len() > 1 && !generation.caption.is_empty())
            .then_some(generation.message_ids[1]);

    Some(DeleteDrawingFramePlan {
        message_id,
        delete_all: remaining_message_ids.is_empty(),
        remaining_message_ids,
        transfer_caption_to_message_id,
    })
}

fn delete_drawing_callback_data_from_query(
    query: &TelegramCallbackQuery,
) -> Option<DeleteDrawingCallbackData> {
    delete_drawing_callback_data_from_raw(query.data.as_deref().unwrap_or_default())
}

fn delete_drawing_callback_data_from_raw(raw: &str) -> Option<DeleteDrawingCallbackData> {
    let CallbackActionParse::Action { data, action } = parse_callback_action(raw) else {
        return None;
    };
    if !matches!(
        action.as_str(),
        openplotva_telegram::DELETE_DRAWING_ACTION_INIT
            | openplotva_telegram::DELETE_DRAWING_ACTION_CONFIRM
            | openplotva_telegram::DELETE_DRAWING_ACTION_FRAME_PICK
            | openplotva_telegram::DELETE_DRAWING_ACTION_FRAME_CONFIRM
            | openplotva_telegram::DELETE_DRAWING_ACTION_CLOSE
    ) {
        return None;
    }
    let user_id = data.get("u").map_or(0, |raw| parse_callback_i64(raw));
    let chat_id = data.get("c").map_or(0, |raw| parse_callback_i64(raw));
    let frame_num = data.get("n").map_or(0, |raw| parse_callback_i64(raw));

    Some(DeleteDrawingCallbackData {
        action,
        user_id,
        chat_id,
        frame_num,
    })
}

fn delete_drawing_control_message(
    query: &TelegramCallbackQuery,
) -> Option<DeleteDrawingControlMessage> {
    match query.message.as_ref()? {
        MaybeInaccessibleMessage::Message(message) => Some(DeleteDrawingControlMessage {
            chat: ChatRef {
                id: message.chat.get_id().into(),
                is_forum: message.message_thread_id.is_some(),
            },
            message_id: message.id,
        }),
        MaybeInaccessibleMessage::InaccessibleMessage(message) => {
            Some(DeleteDrawingControlMessage {
                chat: ChatRef {
                    id: message.chat.get_id().into(),
                    is_forum: false,
                },
                message_id: message.message_id,
            })
        }
    }
}

#[must_use]
pub fn delete_drawing_command_plan(
    message: &TelegramMessage,
    user_id: i64,
    generation: Option<&LastGeneration>,
) -> DeleteDrawingCommandPlan {
    let Some(generation) = generation.filter(|generation| !generation.message_ids.is_empty())
    else {
        return DeleteDrawingCommandPlan::NoGeneration(no_generation_plan(message));
    };

    DeleteDrawingCommandPlan::Control(control_plan(message, user_id, generation))
}

/// Return whether a message is a delete-drawing command.
/// so group commands targeted at any bot are still consumed.
#[must_use]
pub fn is_delete_drawing_command(message: &TelegramMessage) -> bool {
    leading_bot_command(message).is_some_and(|command| command.command == DELETE_DRAWING_COMMAND)
}

fn no_generation_plan(message: &TelegramMessage) -> DeleteDrawingTextPlan {
    DeleteDrawingTextPlan {
        message: base_text_request(
            message,
            DELETE_DRAWING_NO_GENERATION_TEXT,
            TELEGRAM_PARSE_MODE_HTML.to_owned(),
            None,
        ),
        reply_to: reply_ref(message),
        ephemeral_delete_after: DELETE_DRAWING_TIMEOUT,
    }
}

fn control_plan(
    message: &TelegramMessage,
    user_id: i64,
    generation: &LastGeneration,
) -> DeleteDrawingTextPlan {
    DeleteDrawingTextPlan {
        message: base_text_request(
            message,
            &format!(
                "🗑 Управление генерацией ({} кадр.)",
                generation.message_ids.len()
            ),
            TELEGRAM_PARSE_MODE_HTML.to_owned(),
            Some(ReplyMarkup::from(build_delete_drawing_initial_keyboard(
                user_id,
                message.chat.get_id().into(),
            ))),
        ),
        reply_to: ReplyMessageRef {
            message_id: generation.message_ids[0],
            chat: message_chat_ref(message),
            is_topic_message: false,
            message_thread_id: 0,
        },
        ephemeral_delete_after: DELETE_DRAWING_TIMEOUT,
    }
}

fn base_text_request(
    message: &TelegramMessage,
    text: &str,
    render_as: String,
    reply_markup: Option<ReplyMarkup>,
) -> TextMessageRequest {
    TextMessageRequest {
        chat: Some(message_chat_ref(message)),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: text.to_owned(),
        render_as,
        reply_markup,
    }
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct BotCommandInMessage {
    command: String,
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
    let command = command_with_target
        .split_once('@')
        .map_or(command_with_target, |(command, _)| command);

    Some(BotCommandInMessage {
        command: command.to_owned(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use carapax::types::{ChatMember, ChatMemberAdministrator, ChatMemberCreator, User};
    use openplotva_telegram::{CallbackHandlerKind, DispatcherConfig, TelegramOutboundMethodKind};
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use std::{
        env, io,
        sync::{Mutex, MutexGuard},
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::callbacks::{
        CallbackQueryEffects, CallbackQueryFuture, CallbackQueryUpdateRoute,
        CallbackRateLimitFuture, CallbackRateLimitPolicy, handle_callback_query_update_or_else,
    };
    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateStateStore, UpdateStateStoreFuture,
        process_update_with_state_store_at,
    };

    fn sample_message(value: serde_json::Value) -> Result<TelegramMessage, serde_json::Error> {
        serde_json::from_value(value)
    }

    fn command_message(
        chat: serde_json::Value,
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
    ) -> Result<TelegramMessage, serde_json::Error> {
        command_message_at(chat, text, command_len, thread_id, 1_710_000_000)
    }

    fn command_message_at(
        chat: serde_json::Value,
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
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
        sample_message(value)
    }

    fn private_delete_message() -> Result<TelegramMessage, serde_json::Error> {
        command_message(
            serde_json::json!({"id": 42, "type": "private", "first_name": "Ada"}),
            "/delete_drawing",
            15,
            None,
        )
    }

    fn group_delete_message(
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
    ) -> Result<TelegramMessage, serde_json::Error> {
        group_delete_message_at(text, command_len, thread_id, 1_710_000_000)
    }

    fn group_delete_message_at(
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
            date,
        )
    }

    fn update_from_message(message: TelegramMessage) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "update_id": 500,
            "message": serde_json::to_value(message)?,
        }))
    }

    fn delete_callback_update(
        action: &str,
        user_id: i64,
        chat_id: i64,
        frame_num: i64,
        from_user_id: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "update_id": 6161,
            "callback_query": {
                "id": "delete-callback-id",
                "from": {"id": from_user_id, "is_bot": false, "first_name": "Ada"},
                "chat_instance": "chat-instance",
                "data": openplotva_telegram::delete_drawing_callback_data(
                    action,
                    user_id,
                    chat_id,
                    frame_num
                ),
                "message": {
                    "message_id": 55,
                    "date": 1_710_000_000,
                    "chat": {"id": -10042, "type": "supergroup", "title": "Plotva Lab"},
                    "text": "controls"
                }
            }
        }))
    }

    fn generation(message_ids: Vec<i64>) -> LastGeneration {
        LastGeneration {
            chat_id: -42,
            user_id: 5,
            message_ids,
            caption: "caption".to_owned(),
            created_at: 1_710_000_000,
        }
    }

    #[test]
    fn delete_drawing_command_gate_matches_go_broad_command_check()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(is_delete_drawing_command(&private_delete_message()?));
        assert!(is_delete_drawing_command(&command_message(
            serde_json::json!({"id": 42, "type": "private", "first_name": "Ada"}),
            "/delete_drawing@OtherBot",
            24,
            None,
        )?));
        assert!(is_delete_drawing_command(&group_delete_message(
            "/delete_drawing",
            15,
            None,
        )?));
        assert!(is_delete_drawing_command(&group_delete_message(
            "/delete_drawing@OtherBot",
            24,
            None,
        )?));
        assert!(!is_delete_drawing_command(&group_delete_message(
            "/reset", 6, None,
        )?));
        Ok(())
    }

    #[test]
    fn delete_drawing_no_generation_plan_matches_go_ephemeral_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let DeleteDrawingCommandPlan::NoGeneration(plan) =
            delete_drawing_command_plan(&private_delete_message()?, 5, None)
        else {
            panic!("expected no generation plan");
        };

        assert_eq!(plan.message.text, DELETE_DRAWING_NO_GENERATION_TEXT);
        assert_eq!(plan.message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(plan.ephemeral_delete_after, Duration::from_secs(120));
        assert_eq!(plan.reply_to.message_id, 10);
        Ok(())
    }

    #[test]
    fn delete_drawing_control_plan_matches_go_text_keyboard_and_reply()
    -> Result<(), Box<dyn std::error::Error>> {
        let message = group_delete_message("/delete_drawing", 15, Some(99))?;
        let DeleteDrawingCommandPlan::Control(plan) =
            delete_drawing_command_plan(&message, 5, Some(&generation(vec![101, 102, 103])))
        else {
            panic!("expected control plan");
        };

        assert_eq!(plan.message.text, "🗑 Управление генерацией (3 кадр.)");
        assert_eq!(plan.message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert!(plan.message.reply_markup.is_some());
        assert_eq!(plan.ephemeral_delete_after, Duration::from_secs(120));
        assert_eq!(plan.reply_to.message_id, 101);
        assert_eq!(plan.reply_to.message_thread_id, 0);
        assert!(!plan.reply_to.is_topic_message);
        Ok(())
    }

    #[tokio::test]
    async fn delete_drawing_update_sends_no_generation_after_store_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(StoreStub::with_error("redis down"));
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());

        let outcome = handle_delete_drawing_command_update_or_else(
            store.as_ref(),
            effects.as_ref(),
            update_from_message(private_delete_message()?)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            outcome,
            DeleteDrawingCommandOutcome::StoreErrorNoGeneration {
                message: "redis down".to_owned()
            }
        );
        assert_eq!(next.handled_count(), 0);
        assert_eq!(effects.texts().len(), 1);
        assert_eq!(
            effects.texts()[0].message.text,
            DELETE_DRAWING_NO_GENERATION_TEXT
        );
        Ok(())
    }

    #[tokio::test]
    async fn delete_drawing_handler_consumes_command_before_wrapped_handler()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(StoreStub::with_generation(generation(vec![101])));
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = DeleteDrawingCommandUpdateHandler::new(store, effects.clone(), next.clone());

        handler
            .handle_update(update_from_message(private_delete_message()?)?)
            .await?;

        assert_eq!(next.handled_count(), 0);
        assert_eq!(effects.texts().len(), 1);
        assert_eq!(
            effects.texts()[0].message.text,
            "🗑 Управление генерацией (1 кадр.)"
        );
        Ok(())
    }

    #[tokio::test]
    async fn delete_drawing_handler_delegates_non_delete_command()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(StoreStub::default());
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());

        let outcome = handle_delete_drawing_command_update_or_else(
            store.as_ref(),
            effects.as_ref(),
            update_from_message(group_delete_message("/reset", 6, None)?)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteDrawingCommandOutcome::Delegated);
        assert_eq!(next.handled_count(), 1);
        assert!(effects.texts().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_delete_drawing_no_generation_queues_ephemeral_error_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-delete-drawing:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let last_generation_store = Arc::new(StoreStub::default());
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let effects = Arc::new(
            DeleteDrawingCommandDispatcherEffects::new(Arc::clone(&queue))
                .with_virtual_id_factory(Arc::new(|| "delete-drawing-live-vmsg-1".to_owned())),
        );
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = DeleteDrawingCommandUpdateHandler::new(
            last_generation_store,
            effects,
            Arc::clone(&next),
        );

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let update = update_from_message(group_delete_message_at(
            "/delete_drawing@OtherBot",
            24,
            Some(9),
            now_secs as i64,
        )?)?;
        update_queue.enqueue_update(&update).await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded delete_drawing update"))?;

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
            .ok_or_else(|| io::Error::other("expected delete_drawing error payload"))?;
        assert_eq!(item.metadata().virtual_id, "delete-drawing-live-vmsg-1");
        assert_eq!(
            item.method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        assert_eq!(item.ephemeral_delete_after(), Some(DELETE_DRAWING_TIMEOUT));
        let value = method_payload(
            item.into_method()
                .ok_or_else(|| io::Error::other("expected sendMessage method"))?,
        )?;
        assert_eq!(value["chat_id"], serde_json::json!(-42));
        assert_eq!(
            value["text"],
            serde_json::json!(DELETE_DRAWING_NO_GENERATION_TEXT)
        );
        assert_eq!(
            value["parse_mode"],
            serde_json::json!(TELEGRAM_PARSE_MODE_HTML)
        );
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
    async fn live_redis_decoded_delete_drawing_callback_delegates_and_closes_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-delete-drawing-callback:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&delete_callback_update(
                openplotva_telegram::DELETE_DRAWING_ACTION_CLOSE,
                5,
                -42,
                0,
                5,
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded delete_drawing callback update"))?;

        let state_store = UpdateStateStoreStub::default();
        let callback_effects = CallbackAckEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::default();
        let store = StoreStub::with_generation(generation(vec![101]));
        let member_api = MemberApiStub::default();
        let vip = VipStub::default();
        let delete_effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();
        let prehandler_route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&prehandler_route);
        let delete_outcome = Arc::new(Mutex::new(None));
        let captured_outcome = Arc::clone(&delete_outcome);

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
                let route = handle_callback_query_update_or_else(
                    &callback_effects,
                    &rate_limit,
                    update,
                    |update| async {
                        let outcome = handle_delete_drawing_callback_update_or_else(
                            &store,
                            &member_api,
                            &vip,
                            &delete_effects,
                            update,
                            |update| next.handle_update(update),
                        )
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))?;
                        *captured_outcome.lock().expect("delete drawing outcome") = Some(outcome);
                        Ok::<(), io::Error>(())
                    },
                )
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
                *captured_route.lock().expect("callback route") = Some(route);
                Ok::<(), io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 6161);
        assert_eq!(report.update_name, "callback_query");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:-10042:supergroup::".to_owned(),
                "user:5:Ada:".to_owned()
            ]
        );
        assert_eq!(
            *prehandler_route.lock().expect("callback route"),
            Some(CallbackQueryUpdateRoute::HandlerDelegated {
                handler: CallbackHandlerKind::DeleteDrawing,
                action: openplotva_telegram::DELETE_DRAWING_ACTION_CLOSE.to_owned(),
            })
        );
        assert_eq!(
            *delete_outcome.lock().expect("delete drawing outcome"),
            Some(DeleteDrawingCallbackOutcome::Closed)
        );
        assert!(callback_effects.methods().is_empty());
        assert_eq!(member_api.calls(), Vec::<(i64, i64)>::new());
        assert_eq!(next.handled_count(), 0);

        let methods = delete_effects.methods();
        assert_eq!(
            method_names(&methods),
            vec!["deleteMessage", "answerCallbackQuery"]
        );
        assert_eq!(methods[0].1["chat_id"], serde_json::json!(-10042));
        assert_eq!(methods[0].1["message_id"], serde_json::json!(55));
        assert_eq!(
            methods[1].1["callback_query_id"],
            serde_json::json!("delete-callback-id")
        );
        assert!(methods[1].1.get("text").is_none());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[test]
    fn delete_drawing_callback_parse_matches_go_zero_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let data =
            delete_drawing_callback_data_from_raw(r#"{"a":"del_fc","u":"bad","c":"","n":"wat"}"#)
                .ok_or_else(|| io::Error::other("expected delete callback data"))?;

        assert_eq!(
            data,
            DeleteDrawingCallbackData {
                action: "del_fc".to_owned(),
                user_id: 0,
                chat_id: 0,
                frame_num: 0,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn delete_drawing_callback_denies_non_author_without_admin_rights()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = StoreStub::with_generation(generation(vec![101]));
        let member_api = MemberApiStub::default();
        let vip = VipStub::default();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_drawing_callback_update_or_else(
            &store,
            &member_api,
            &vip,
            &effects,
            delete_callback_update(
                openplotva_telegram::DELETE_DRAWING_ACTION_CONFIRM,
                5,
                -42,
                0,
                7,
            )?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteDrawingCallbackOutcome::Unauthorized);
        assert_eq!(member_api.calls(), vec![(-42, 7)]);
        assert_eq!(next.handled_count(), 0);
        let methods = effects.methods();
        assert_eq!(methods[0].0, "answerCallbackQuery");
        assert_eq!(methods[0].1["text"], DELETE_DRAWING_AUTH_ALERT_TEXT);
        assert_eq!(methods[0].1["show_alert"], true);
        Ok(())
    }

    #[tokio::test]
    async fn delete_drawing_callback_init_shows_vip_frame_picker()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = StoreStub::with_generation(generation(vec![101, 102]));
        let member_api = MemberApiStub::default();
        let vip = VipStub::vip();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_drawing_callback_update_or_else(
            &store,
            &member_api,
            &vip,
            &effects,
            delete_callback_update(
                openplotva_telegram::DELETE_DRAWING_ACTION_INIT,
                5,
                -42,
                0,
                5,
            )?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            outcome,
            DeleteDrawingCallbackOutcome::InitShown { frame_picker: true }
        );
        assert_eq!(member_api.calls(), Vec::<(i64, i64)>::new());
        let methods = effects.methods();
        assert_eq!(methods[0].0, "editMessageReplyMarkup");
        assert_eq!(methods[0].1["chat_id"], -10042);
        assert_eq!(methods[0].1["message_id"], 55);
        assert_eq!(
            methods[0].1["reply_markup"]["inline_keyboard"][0][0]["text"],
            "#1"
        );
        assert_eq!(
            methods[0].1["reply_markup"]["inline_keyboard"][0][2]["text"],
            "Всё"
        );
        assert_eq!(methods[1].0, "answerCallbackQuery");
        assert!(methods[1].1.get("text").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn delete_drawing_callback_confirm_deletes_all_and_store_value()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = StoreStub::with_generation(generation(vec![101, 102]));
        let member_api = MemberApiStub::with_member(deleting_admin_member(7));
        let vip = VipStub::default();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_drawing_callback_update_or_else(
            &store,
            &member_api,
            &vip,
            &effects,
            delete_callback_update(
                openplotva_telegram::DELETE_DRAWING_ACTION_CONFIRM,
                5,
                -42,
                0,
                7,
            )?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteDrawingCallbackOutcome::ConfirmedAll);
        assert_eq!(store.deletes(), vec![(-42, 5)]);
        let methods = effects.methods();
        assert_eq!(
            method_names(&methods),
            vec![
                "deleteMessage",
                "deleteMessage",
                "deleteMessage",
                "answerCallbackQuery"
            ]
        );
        assert_eq!(methods[0].1["chat_id"], -42);
        assert_eq!(methods[0].1["message_id"], 101);
        assert_eq!(methods[2].1["chat_id"], -10042);
        assert_eq!(methods[2].1["message_id"], 55);
        assert_eq!(methods[3].1["text"], DELETE_DRAWING_ALL_DELETED_TEXT);
        Ok(())
    }

    #[tokio::test]
    async fn delete_drawing_callback_frame_confirm_transfers_caption_and_updates_picker()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = StoreStub::with_generation(generation(vec![101, 102, 103]));
        let member_api = MemberApiStub::default();
        let vip = VipStub::vip();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_drawing_callback_update_or_else(
            &store,
            &member_api,
            &vip,
            &effects,
            delete_callback_update(
                openplotva_telegram::DELETE_DRAWING_ACTION_FRAME_CONFIRM,
                5,
                -42,
                1,
                5,
            )?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            outcome,
            DeleteDrawingCallbackOutcome::FrameDeleted {
                frame_num: 1,
                remaining: 2,
            }
        );
        assert_eq!(store.sets()[0].message_ids, vec![102, 103]);
        let methods = effects.methods();
        assert_eq!(
            method_names(&methods),
            vec![
                "editMessageCaption",
                "deleteMessage",
                "editMessageText",
                "answerCallbackQuery"
            ]
        );
        assert_eq!(methods[0].1["chat_id"], -42);
        assert_eq!(methods[0].1["message_id"], 102);
        assert_eq!(methods[0].1["caption"], "caption");
        assert_eq!(methods[0].1["parse_mode"], "HTML");
        assert_eq!(methods[2].1["text"], "🗑 Управление генерацией (2 кадр.)");
        assert_eq!(methods[3].1["text"], "✅ Кадр #1 удалён");
        Ok(())
    }

    #[tokio::test]
    async fn delete_drawing_callback_close_deletes_control_and_acks()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = StoreStub::with_generation(generation(vec![101]));
        let member_api = MemberApiStub::default();
        let vip = VipStub::default();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_drawing_callback_update_or_else(
            &store,
            &member_api,
            &vip,
            &effects,
            delete_callback_update(
                openplotva_telegram::DELETE_DRAWING_ACTION_CLOSE,
                5,
                -42,
                0,
                5,
            )?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteDrawingCallbackOutcome::Closed);
        let methods = effects.methods();
        assert_eq!(methods[0].0, "deleteMessage");
        assert_eq!(methods[0].1["chat_id"], -10042);
        assert_eq!(methods[0].1["message_id"], 55);
        assert_eq!(methods[1].0, "answerCallbackQuery");
        assert!(methods[1].1.get("text").is_none());
        Ok(())
    }

    #[derive(Default)]
    struct StoreStub {
        generation: Mutex<Option<LastGeneration>>,
        error: Mutex<Option<String>>,
        sets: Mutex<Vec<LastGeneration>>,
        deletes: Mutex<Vec<(i64, i64)>>,
    }

    impl StoreStub {
        fn with_generation(generation: LastGeneration) -> Self {
            Self {
                generation: Mutex::new(Some(generation)),
                ..Self::default()
            }
        }

        fn with_error(error: &str) -> Self {
            Self {
                generation: Mutex::new(None),
                error: Mutex::new(Some(error.to_owned())),
                ..Self::default()
            }
        }

        fn sets(&self) -> Vec<LastGeneration> {
            self.sets.lock().expect("sets").clone()
        }

        fn deletes(&self) -> Vec<(i64, i64)> {
            self.deletes.lock().expect("deletes").clone()
        }
    }

    impl DeleteDrawingLastGenerationStore for StoreStub {
        type Error = io::Error;

        fn last_generation<'a>(
            &'a self,
            _chat_id: i64,
            _user_id: i64,
        ) -> DeleteDrawingStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                if let Some(error) = self.error.lock().expect("error").clone() {
                    return Err(io::Error::other(error));
                }
                Ok(self.generation.lock().expect("generation").clone())
            })
        }
    }

    impl DeleteDrawingCallbackStore for StoreStub {
        fn set_last_generation<'a>(
            &'a self,
            generation: LastGeneration,
        ) -> DeleteDrawingStoreWriteFuture<'a, Self::Error> {
            Box::pin(async move {
                self.sets.lock().expect("sets").push(generation.clone());
                *self.generation.lock().expect("generation") = Some(generation);
                Ok(())
            })
        }

        fn delete_last_generation<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> DeleteDrawingStoreWriteFuture<'a, Self::Error> {
            Box::pin(async move {
                self.deletes
                    .lock()
                    .expect("deletes")
                    .push((chat_id, user_id));
                *self.generation.lock().expect("generation") = None;
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

    #[derive(Default)]
    struct EffectsStub {
        texts: Mutex<Vec<DeleteDrawingTextPlan>>,
        methods: Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl EffectsStub {
        fn texts(&self) -> Vec<DeleteDrawingTextPlan> {
            self.texts.lock().expect("texts").clone()
        }

        fn methods(&self) -> Vec<(String, serde_json::Value)> {
            self.methods.lock().expect("methods").clone()
        }

        fn lock_texts(&self) -> MutexGuard<'_, Vec<DeleteDrawingTextPlan>> {
            self.texts.lock().expect("texts")
        }

        fn lock_methods(&self) -> MutexGuard<'_, Vec<(String, serde_json::Value)>> {
            self.methods.lock().expect("methods")
        }
    }

    impl DeleteDrawingCommandEffects for EffectsStub {
        type Error = io::Error;

        fn send_delete_drawing_text<'a>(
            &'a self,
            plan: DeleteDrawingTextPlan,
        ) -> DeleteDrawingEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock_texts().push(plan);
                Ok(())
            })
        }
    }

    impl DeleteDrawingCallbackEffects for EffectsStub {
        type Error = io::Error;

        fn execute_delete_drawing_method<'a>(
            &'a self,
            method: TelegramOutboundMethod,
        ) -> DeleteDrawingEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                let name = method.method_name().to_owned();
                let payload = method_payload(method)?;
                self.lock_methods().push((name, payload));
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct CallbackAckEffectsStub {
        methods: Mutex<Vec<(TelegramOutboundMethodKind, serde_json::Value)>>,
    }

    impl CallbackAckEffectsStub {
        fn methods(&self) -> Vec<(TelegramOutboundMethodKind, serde_json::Value)> {
            self.methods.lock().expect("callback ack methods").clone()
        }
    }

    impl CallbackQueryEffects for CallbackAckEffectsStub {
        type Error = io::Error;

        fn answer_callback_query<'a>(
            &'a self,
            method: TelegramOutboundMethod,
        ) -> CallbackQueryFuture<'a, Self::Error> {
            Box::pin(async move {
                let kind = method.kind();
                let payload = match method {
                    TelegramOutboundMethod::AnswerCallbackQuery(method) => {
                        serde_json::to_value(method.as_ref()).map_err(io::Error::other)?
                    }
                    _ => return Err(io::Error::other("unexpected callback ack method")),
                };
                self.methods
                    .lock()
                    .expect("callback ack methods")
                    .push((kind, payload));
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct CallbackRateLimitStub {
        limited_chat_id: Option<i64>,
    }

    impl CallbackRateLimitPolicy for CallbackRateLimitStub {
        fn is_callback_chat_rate_limited<'a>(
            &'a self,
            chat_id: i64,
        ) -> CallbackRateLimitFuture<'a> {
            Box::pin(async move { self.limited_chat_id == Some(chat_id) })
        }
    }

    #[derive(Default)]
    struct MemberApiStub {
        member: Mutex<Option<TelegramChatMember>>,
        calls: Mutex<Vec<(i64, i64)>>,
    }

    impl MemberApiStub {
        fn with_member(member: TelegramChatMember) -> Self {
            Self {
                member: Mutex::new(Some(member)),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<(i64, i64)> {
            self.calls.lock().expect("member calls").clone()
        }
    }

    impl DeleteDrawingMemberApi for MemberApiStub {
        type Error = io::Error;

        fn get_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> DeleteDrawingMemberFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("member calls")
                    .push((chat_id, user_id));
                self.member
                    .lock()
                    .expect("member")
                    .clone()
                    .ok_or_else(|| io::Error::other("missing member"))
            })
        }
    }

    #[derive(Default)]
    struct VipStub {
        is_vip: bool,
    }

    impl VipStub {
        fn vip() -> Self {
            Self { is_vip: true }
        }
    }

    impl DeleteDrawingVipStatus for VipStub {
        fn is_delete_drawing_vip<'a>(&'a self, _user_id: i64) -> DeleteDrawingVipFuture<'a> {
            Box::pin(async move { self.is_vip })
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

    fn method_payload(method: TelegramOutboundMethod) -> Result<serde_json::Value, io::Error> {
        match method {
            TelegramOutboundMethod::AnswerCallbackQuery(method) => {
                serde_json::to_value(method.as_ref())
            }
            TelegramOutboundMethod::EditMessageReplyMarkup(method) => {
                serde_json::to_value(method.as_ref())
            }
            TelegramOutboundMethod::EditMessageText(method) => {
                serde_json::to_value(method.as_ref())
            }
            TelegramOutboundMethod::EditMessageCaption(method) => {
                serde_json::to_value(method.as_ref())
            }
            TelegramOutboundMethod::DeleteMessage(method) => serde_json::to_value(method.as_ref()),
            TelegramOutboundMethod::SendMessage(method) => serde_json::to_value(method.as_ref()),
            _ => Err(serde_json::Error::io(io::Error::other("unexpected method"))),
        }
        .map_err(io::Error::other)
    }

    fn method_names(methods: &[(String, serde_json::Value)]) -> Vec<&str> {
        methods.iter().map(|(name, _)| name.as_str()).collect()
    }

    fn deleting_admin_member(user_id: i64) -> TelegramChatMember {
        ChatMember::Administrator(
            ChatMemberAdministrator::new(User::new(user_id, "Ada", false))
                .with_can_delete_messages(true),
        )
    }

    fn _creator_member(user_id: i64) -> TelegramChatMember {
        ChatMember::Creator(ChatMemberCreator::new(User::new(user_id, "Ada", false)))
    }
}
