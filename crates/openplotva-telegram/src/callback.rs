//! Telegram callback data parsing and fetcher action routing helpers.

use std::collections::BTreeMap;

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
