//! Telegram update startup method builders.

use std::{collections::HashSet, time::Duration};

use carapax::types::{AllowedUpdate, DeleteWebhook, GetUpdates, SetWebhook};

/// Go `StartUpdateLoop` long-poll timeout.
pub const GO_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(60);

/// Go webhook route registered by `ListenForUpdates`.
pub const TELEGRAM_WEBHOOK_PATH: &str = "/telegram/webhook";

/// Minimal webhook setup inputs used by Go `StartWebhookServer`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebhookSetup {
    /// Public Telegram webhook URL.
    pub url: String,
    /// Optional `X-Telegram-Bot-Api-Secret-Token` value.
    pub secret_token: Option<String>,
}

impl WebhookSetup {
    /// Build setup inputs from a URL and optional secret token.
    pub fn new(url: impl Into<String>, secret_token: Option<String>) -> Self {
        Self {
            url: url.into(),
            secret_token,
        }
    }
}

/// Return the Go allowed-update list as a native `carapax` set.
pub fn go_allowed_update_set() -> HashSet<AllowedUpdate> {
    openplotva_updates::GO_ALLOWED_UPDATES
        .iter()
        .copied()
        .collect()
}

/// Build Go's long-polling `getUpdates` method.
pub fn build_get_updates_method() -> GetUpdates {
    GetUpdates::default()
        .with_offset(0)
        .with_timeout(GO_LONG_POLL_TIMEOUT)
        .with_allowed_updates(go_allowed_update_set())
}

/// Build Go's webhook deletion method used before long polling.
pub fn build_delete_webhook_method() -> DeleteWebhook {
    DeleteWebhook::default()
}

/// Build Go's webhook setup method.
pub fn build_set_webhook_method(setup: &WebhookSetup) -> SetWebhook {
    let mut method =
        SetWebhook::new(setup.url.clone()).with_allowed_updates(go_allowed_update_set());

    if let Some(secret_token) = setup
        .secret_token
        .as_deref()
        .filter(|secret_token| !secret_token.is_empty())
    {
        method = method.with_secret_token(secret_token);
    }

    method
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::{Value, json};

    use super::{
        GO_LONG_POLL_TIMEOUT, TELEGRAM_WEBHOOK_PATH, WebhookSetup, build_delete_webhook_method,
        build_get_updates_method, build_set_webhook_method,
    };

    #[test]
    fn get_updates_method_matches_go_long_poll_startup_contract() {
        let payload = serde_json::to_value(build_get_updates_method()).expect("getUpdates JSON");

        assert_eq!(payload.get("offset"), Some(&json!(0)));
        assert_eq!(payload.get("timeout"), Some(&json!(60)));
        assert_eq!(
            allowed_update_names(&payload),
            openplotva_updates::GO_ALLOWED_UPDATE_NAMES
                .iter()
                .copied()
                .collect()
        );
        assert!(
            payload.get("limit").is_none(),
            "Go NewUpdate(0) leaves limit unset"
        );
        assert_eq!(GO_LONG_POLL_TIMEOUT.as_secs(), 60);
    }

    #[test]
    fn delete_webhook_method_matches_go_long_poll_startup_contract() {
        let payload =
            serde_json::to_value(build_delete_webhook_method()).expect("deleteWebhook JSON");

        assert_eq!(payload.get("drop_pending_updates"), Some(&Value::Null));
    }

    #[test]
    fn set_webhook_method_matches_go_webhook_startup_contract() {
        let setup = WebhookSetup::new("https://plotva.example/tg", Some("secret-token".to_owned()));
        let payload =
            serde_json::to_value(build_set_webhook_method(&setup)).expect("setWebhook JSON");

        assert_eq!(
            payload.get("url"),
            Some(&json!("https://plotva.example/tg"))
        );
        assert_eq!(payload.get("secret_token"), Some(&json!("secret-token")));
        assert_eq!(
            allowed_update_names(&payload),
            openplotva_updates::GO_ALLOWED_UPDATE_NAMES
                .iter()
                .copied()
                .collect()
        );
        assert_eq!(TELEGRAM_WEBHOOK_PATH, "/telegram/webhook");
    }

    #[test]
    fn set_webhook_method_omits_empty_secret_like_go_add_non_empty() {
        let setup = WebhookSetup::new("https://plotva.example/tg", Some(String::new()));
        let payload =
            serde_json::to_value(build_set_webhook_method(&setup)).expect("setWebhook JSON");

        assert!(payload.get("secret_token").is_none());
    }

    fn allowed_update_names(payload: &Value) -> BTreeSet<&str> {
        payload
            .get("allowed_updates")
            .and_then(Value::as_array)
            .expect("allowed_updates array")
            .iter()
            .map(|value| value.as_str().expect("allowed update name"))
            .collect()
    }
}
