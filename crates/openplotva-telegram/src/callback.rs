//! Telegram callback data parsing and fetcher action routing helpers.

use std::collections::BTreeMap;

use crate::{
    outbound::{CallbackAnswerRequest, build_callback_answer_method},
    transport::TelegramOutboundMethod,
};

pub type CallbackActionData = BTreeMap<String, String>;

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

/// Return the Go callback handler group for an action.
#[must_use]
pub fn callback_handler_for_action(action: &str) -> Option<CallbackHandlerKind> {
    match action {
        "delete" => Some(CallbackHandlerKind::Delete),
        "cancel_vip" => Some(CallbackHandlerKind::CancelVip),
        "confirm_cancel_vip" => Some(CallbackHandlerKind::ConfirmCancelVip),
        "back_to_vip_status" => Some(CallbackHandlerKind::BackToVipStatus),
        "checkin_theme_select" | "cts" => Some(CallbackHandlerKind::CheckinThemeSelect),
        "del_i" | "del_c" | "del_fp" | "del_fc" | "del_x" => {
            Some(CallbackHandlerKind::DeleteDrawing)
        }
        "dl_i" | "dl_c" => Some(CallbackHandlerKind::DeleteLyrics),
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
