//! Telegram callback data parsing and fetcher action routing helpers.

use std::collections::BTreeMap;

use carapax::types::{InlineKeyboardButton, InlineKeyboardMarkup};
use serde::Serialize;

use crate::{
    outbound::{
        CallbackAnswerRequest, build_callback_answer_method, build_inline_keyboard_button_data,
        build_inline_keyboard_markup, build_inline_keyboard_row,
    },
    transport::TelegramOutboundMethod,
};

pub type CallbackActionData = BTreeMap<String, String>;

/// Go delete-drawing callback action: initialize deletion.
pub const DELETE_DRAWING_ACTION_INIT: &str = "del_i";
/// Go delete-drawing callback action: confirm all deletion.
pub const DELETE_DRAWING_ACTION_CONFIRM: &str = "del_c";
/// Go delete-drawing callback action: pick one frame.
pub const DELETE_DRAWING_ACTION_FRAME_PICK: &str = "del_fp";
/// Go delete-drawing callback action: confirm one frame.
pub const DELETE_DRAWING_ACTION_FRAME_CONFIRM: &str = "del_fc";
/// Go delete-drawing callback action: close controls.
pub const DELETE_DRAWING_ACTION_CLOSE: &str = "del_x";

/// Go delete-lyrics callback action: initialize deletion.
pub const DELETE_LYRICS_ACTION_INIT: &str = "dl_i";
/// Go delete-lyrics callback action: confirm deletion.
pub const DELETE_LYRICS_ACTION_CONFIRM: &str = "dl_c";
/// Go delete-lyrics callback action generated in keyboards but not routed by Go.
pub const DELETE_LYRICS_ACTION_CLOSE: &str = "dl_x";

/// Result of parsing callback data while preserving Go's ack-routing split.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallbackActionParse {
    /// JSON callback data with an `action` or short `a` key.
    Action {
        /// Parsed string-only JSON data.
        data: CallbackActionData,
        /// Resolved action value, preferring `action` over `a`.
        action: String,
    },
    /// Valid string-only JSON callback data without an action key.
    Actionless(CallbackActionData),
    /// Non-JSON or non-string-map callback data.
    Invalid,
}

impl CallbackActionParse {
    /// Return parsed callback data for both action and actionless JSON cases.
    #[must_use]
    pub fn into_data(self) -> Option<CallbackActionData> {
        match self {
            Self::Action { data, .. } | Self::Actionless(data) => Some(data),
            Self::Invalid => None,
        }
    }
}

/// Fetcher callback handler group selected by callback action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallbackHandlerKind {
    /// Legacy delete callback for reply-targeted messages.
    Delete,
    /// VIP cancellation confirmation prompt.
    CancelVip,
    /// Confirm VIP subscription cancellation.
    ConfirmCancelVip,
    /// Return from cancellation prompt to the VIP status view.
    BackToVipStatus,
    /// Daily check-in theme selector.
    CheckinThemeSelect,
    /// Image-generation deletion controls.
    DeleteDrawing,
    /// Generated lyrics deletion controls.
    DeleteLyrics,
}

/// Go `processCallbackQuery` decision before concrete handler side effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallbackQueryRoute {
    /// Callback query had no associated message, so Go only acknowledges it.
    AckOrphan,
    /// Chat is rate-limited; Go skips callback handling without acknowledging.
    SkipRateLimited,
    /// Callback data was empty and should receive an empty acknowledgement.
    AckEmptyData,
    /// Settings callback data is handled by the settings callback path.
    Settings {
        /// Raw callback data beginning with `settings:`.
        data: String,
    },
    /// Callback data was not JSON.
    AckLegacyData,
    /// Callback data was JSON but had neither `action` nor `a`.
    AckActionlessJson {
        /// Parsed callback data.
        data: CallbackActionData,
    },
    /// Callback data had an action but there is no Go handler for it.
    AckUnknownAction {
        /// Unhandled action value.
        action: String,
    },
    /// Callback data should be dispatched to a concrete handler group.
    Handle {
        /// Go handler group.
        handler: CallbackHandlerKind,
        /// Resolved action value.
        action: String,
        /// Parsed callback data.
        data: CallbackActionData,
    },
}

/// Parse Go fetcher callback data, accepting long `action` and short `a` keys.
#[must_use]
pub fn parse_callback_action(raw: &str) -> CallbackActionParse {
    let Ok(data) = serde_json::from_str::<CallbackActionData>(raw) else {
        return CallbackActionParse::Invalid;
    };

    if let Some(action) = data.get("action").or_else(|| data.get("a")).cloned() {
        return CallbackActionParse::Action { data, action };
    }

    CallbackActionParse::Actionless(data)
}

/// Parse Go callback integer fields, returning zero for blank or invalid input.
#[must_use]
pub fn parse_callback_i64(raw: &str) -> i64 {
    let raw = raw.trim();
    if raw.is_empty() {
        return 0;
    }
    raw.parse().unwrap_or(0)
}

/// Encode Go delete-drawing callback data with stable field order.
#[must_use]
pub fn delete_drawing_callback_data(
    action: &str,
    user_id: i64,
    chat_id: i64,
    frame_num: i64,
) -> String {
    if frame_num > 0 {
        return serde_json::to_string(&DeleteDrawingCallbackDataWithFrame {
            action,
            user_id: user_id.to_string(),
            chat_id: chat_id.to_string(),
            frame_num: frame_num.to_string(),
        })
        .expect("delete drawing callback data serialization cannot fail");
    }
    serde_json::to_string(&DeleteCallbackData {
        action,
        user_id: user_id.to_string(),
        chat_id: chat_id.to_string(),
    })
    .expect("delete drawing callback data serialization cannot fail")
}

/// Encode Go delete-lyrics callback data with stable field order.
#[must_use]
pub fn delete_lyrics_callback_data(action: &str, user_id: i64, chat_id: i64) -> String {
    serde_json::to_string(&DeleteCallbackData {
        action,
        user_id: user_id.to_string(),
        chat_id: chat_id.to_string(),
    })
    .expect("delete lyrics callback data serialization cannot fail")
}

/// Build Go `buildDeleteDrawingInitialKeyboard` inline controls.
#[must_use]
pub fn build_delete_drawing_initial_keyboard(user_id: i64, chat_id: i64) -> InlineKeyboardMarkup {
    build_inline_keyboard_markup([build_inline_keyboard_row([
        build_inline_keyboard_button_data(
            "🗑️ Удалить",
            delete_drawing_callback_data(DELETE_DRAWING_ACTION_INIT, user_id, chat_id, 0),
        ),
        build_inline_keyboard_button_data(
            "✕",
            delete_drawing_callback_data(DELETE_DRAWING_ACTION_CLOSE, user_id, chat_id, 0),
        ),
    ])])
}

/// Build Go `buildDeleteDrawingConfirmKeyboard` inline controls.
#[must_use]
pub fn build_delete_drawing_confirm_keyboard(user_id: i64, chat_id: i64) -> InlineKeyboardMarkup {
    build_inline_keyboard_markup([build_inline_keyboard_row([
        build_inline_keyboard_button_data(
            "Да, удалить? ❌",
            delete_drawing_callback_data(DELETE_DRAWING_ACTION_CONFIRM, user_id, chat_id, 0),
        ),
        build_inline_keyboard_button_data(
            "✕",
            delete_drawing_callback_data(DELETE_DRAWING_ACTION_CLOSE, user_id, chat_id, 0),
        ),
    ])])
}

/// Build Go `buildDeleteDrawingFramePickerKeyboard` inline controls.
#[must_use]
pub fn build_delete_drawing_frame_picker_keyboard(
    user_id: i64,
    chat_id: i64,
    frame_count: i64,
) -> InlineKeyboardMarkup {
    let mut buttons = Vec::new();
    for frame_num in 1..=frame_count {
        buttons.push(build_inline_keyboard_button_data(
            format!("#{frame_num}"),
            delete_drawing_callback_data(
                DELETE_DRAWING_ACTION_FRAME_PICK,
                user_id,
                chat_id,
                frame_num,
            ),
        ));
    }
    buttons.push(build_inline_keyboard_button_data(
        "Всё",
        delete_drawing_callback_data(DELETE_DRAWING_ACTION_CONFIRM, user_id, chat_id, 0),
    ));
    buttons.push(build_inline_keyboard_button_data(
        "✕",
        delete_drawing_callback_data(DELETE_DRAWING_ACTION_CLOSE, user_id, chat_id, 0),
    ));
    build_inline_keyboard_markup(chunk_inline_keyboard_buttons(buttons, 5))
}

/// Build Go `buildDeleteDrawingFrameConfirmKeyboard` inline controls.
#[must_use]
pub fn build_delete_drawing_frame_confirm_keyboard(
    user_id: i64,
    chat_id: i64,
    frame_num: i64,
) -> InlineKeyboardMarkup {
    build_inline_keyboard_markup([build_inline_keyboard_row([
        build_inline_keyboard_button_data(
            format!("Удалить #{frame_num}? ❌"),
            delete_drawing_callback_data(
                DELETE_DRAWING_ACTION_FRAME_CONFIRM,
                user_id,
                chat_id,
                frame_num,
            ),
        ),
        build_inline_keyboard_button_data(
            "✕",
            delete_drawing_callback_data(DELETE_DRAWING_ACTION_CLOSE, user_id, chat_id, 0),
        ),
    ])])
}

/// Build Go `buildLyricsDeleteKeyboard` inline controls.
#[must_use]
pub fn build_lyrics_delete_keyboard(user_id: i64, chat_id: i64) -> InlineKeyboardMarkup {
    build_inline_keyboard_markup([build_inline_keyboard_row([
        build_inline_keyboard_button_data(
            "🗑 Удалить текст",
            delete_lyrics_callback_data(DELETE_LYRICS_ACTION_INIT, user_id, chat_id),
        ),
    ])])
}

/// Build Go `buildLyricsDeleteConfirmKeyboard` inline controls.
#[must_use]
pub fn build_lyrics_delete_confirm_keyboard(user_id: i64, chat_id: i64) -> InlineKeyboardMarkup {
    build_inline_keyboard_markup([build_inline_keyboard_row([
        build_inline_keyboard_button_data(
            "Да, удалить",
            delete_lyrics_callback_data(DELETE_LYRICS_ACTION_CONFIRM, user_id, chat_id),
        ),
        build_inline_keyboard_button_data(
            "✕",
            delete_lyrics_callback_data(DELETE_LYRICS_ACTION_CLOSE, user_id, chat_id),
        ),
    ])])
}

fn chunk_inline_keyboard_buttons(
    buttons: Vec<InlineKeyboardButton>,
    max_per_row: usize,
) -> Vec<Vec<InlineKeyboardButton>> {
    buttons
        .chunks(max_per_row)
        .map(<[InlineKeyboardButton]>::to_vec)
        .collect()
}

#[derive(Serialize)]
struct DeleteCallbackData<'a> {
    #[serde(rename = "a")]
    action: &'a str,
    #[serde(rename = "u")]
    user_id: String,
    #[serde(rename = "c")]
    chat_id: String,
}

#[derive(Serialize)]
struct DeleteDrawingCallbackDataWithFrame<'a> {
    #[serde(rename = "a")]
    action: &'a str,
    #[serde(rename = "u")]
    user_id: String,
    #[serde(rename = "c")]
    chat_id: String,
    #[serde(rename = "n")]
    frame_num: String,
}

/// Classify a callback query according to Go `processCallbackQuery` pre-handler order.
#[must_use]
pub fn callback_query_route(
    has_message: bool,
    is_rate_limited: bool,
    data: &str,
) -> CallbackQueryRoute {
    if !has_message {
        return CallbackQueryRoute::AckOrphan;
    }
    if is_rate_limited {
        return CallbackQueryRoute::SkipRateLimited;
    }
    if data.is_empty() {
        return CallbackQueryRoute::AckEmptyData;
    }
    if data.starts_with("settings:") {
        return CallbackQueryRoute::Settings {
            data: data.to_owned(),
        };
    }

    match parse_callback_action(data) {
        CallbackActionParse::Invalid => CallbackQueryRoute::AckLegacyData,
        CallbackActionParse::Actionless(data) => CallbackQueryRoute::AckActionlessJson { data },
        CallbackActionParse::Action { data, action } => {
            match callback_handler_for_action(&action) {
                Some(handler) => CallbackQueryRoute::Handle {
                    handler,
                    action,
                    data,
                },
                None => CallbackQueryRoute::AckUnknownAction { action },
            }
        }
    }
}

/// Build the direct callback acknowledgement used by Go for terminal ack routes.
#[must_use]
pub fn callback_query_ack_request(
    callback_query_id: impl Into<String>,
    route: &CallbackQueryRoute,
) -> Option<CallbackAnswerRequest> {
    match route {
        CallbackQueryRoute::AckOrphan
        | CallbackQueryRoute::AckEmptyData
        | CallbackQueryRoute::AckLegacyData
        | CallbackQueryRoute::AckActionlessJson { .. }
        | CallbackQueryRoute::AckUnknownAction { .. } => Some(CallbackAnswerRequest {
            callback_query_id: callback_query_id.into(),
            text: String::new(),
            show_alert: false,
            url: String::new(),
            cache_time: 0,
        }),
        CallbackQueryRoute::SkipRateLimited
        | CallbackQueryRoute::Settings { .. }
        | CallbackQueryRoute::Handle { .. } => None,
    }
}

/// Build the concrete direct `answerCallbackQuery` method for terminal ack routes.
#[must_use]
pub fn callback_query_ack_method(
    callback_query_id: impl Into<String>,
    route: &CallbackQueryRoute,
) -> Option<TelegramOutboundMethod> {
    callback_query_ack_request(callback_query_id, route)
        .map(|request| TelegramOutboundMethod::from(build_callback_answer_method(&request)))
}

/// Build Go's cached empty acknowledgement for settings callbacks.
#[must_use]
pub fn settings_callback_ack_method(
    callback_query_id: impl Into<String>,
) -> TelegramOutboundMethod {
    TelegramOutboundMethod::from(build_callback_answer_method(&CallbackAnswerRequest {
        callback_query_id: callback_query_id.into(),
        text: String::new(),
        show_alert: false,
        url: String::new(),
        cache_time: 10,
    }))
}

/// Return the Go callback handler group for an action.
#[must_use]
pub fn callback_handler_for_action(action: &str) -> Option<CallbackHandlerKind> {
    match action {
        "delete" => Some(CallbackHandlerKind::Delete),
        "cancel_vip" => Some(CallbackHandlerKind::CancelVip),
        "confirm_cancel_vip" => Some(CallbackHandlerKind::ConfirmCancelVip),
        "back_to_vip_status" => Some(CallbackHandlerKind::BackToVipStatus),
        "checkin_theme_select" | "cts" => Some(CallbackHandlerKind::CheckinThemeSelect),
        DELETE_DRAWING_ACTION_INIT
        | DELETE_DRAWING_ACTION_CONFIRM
        | DELETE_DRAWING_ACTION_FRAME_PICK
        | DELETE_DRAWING_ACTION_FRAME_CONFIRM
        | DELETE_DRAWING_ACTION_CLOSE => Some(CallbackHandlerKind::DeleteDrawing),
        DELETE_LYRICS_ACTION_INIT | DELETE_LYRICS_ACTION_CONFIRM => {
            Some(CallbackHandlerKind::DeleteLyrics)
        }
        _ => None,
    }
}

/// Resolve the check-in theme initiator from long `init` or short `i`.
#[must_use]
pub fn checkin_theme_callback_init(data: &CallbackActionData) -> Option<&str> {
    data.get("init")
        .or_else(|| data.get("i"))
        .map(String::as_str)
}

/// Resolve the check-in theme from long `theme` or short `t`.
#[must_use]
pub fn checkin_theme_callback_theme(data: &CallbackActionData) -> &str {
    if let Some(theme) = data.get("theme").filter(|theme| !theme.is_empty()) {
        return theme;
    }
    data.get("t").map_or("", String::as_str)
}

/// Return Go's check-in theme selection alert and whether selection is blocked.
#[must_use]
pub fn checkin_theme_selection_alert(
    user_id: i64,
    data: &CallbackActionData,
) -> (&'static str, bool) {
    let Some(init) = checkin_theme_callback_init(data).filter(|init| !init.is_empty()) else {
        return ("", true);
    };

    if init == user_id.to_string() {
        ("", false)
    } else {
        ("Только инициатор может выбрать тему", true)
    }
}

/// Build Go's check-in theme selection callback acknowledgement or alert.
#[must_use]
pub fn checkin_theme_selection_ack_method(
    callback_query_id: impl Into<String>,
    user_id: i64,
    data: &CallbackActionData,
) -> (TelegramOutboundMethod, bool) {
    let (text, blocked) = checkin_theme_selection_alert(user_id, data);
    let request = CallbackAnswerRequest {
        callback_query_id: callback_query_id.into(),
        text: text.to_owned(),
        show_alert: blocked,
        url: String::new(),
        cache_time: 0,
    };
    (
        TelegramOutboundMethod::from(build_callback_answer_method(&request)),
        blocked,
    )
}
