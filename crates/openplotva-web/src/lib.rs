//! Admin and settings WebApp backend helpers.

use sha2::{Digest, Sha224};

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "web";

/// Go WebApp signature salt from `internal/utils/security.go`.
pub const SETTINGS_SIGNATURE_SALT: &str = "plotva-signature-salt-2024";

/// Build Go's short settings signature for a chat/user ID.
pub fn settings_signature(chat_id: i64) -> String {
    let input = format!("{chat_id}:{SETTINGS_SIGNATURE_SALT}");
    let hash_hex = hex::encode(Sha224::digest(input.as_bytes()));
    hash_hex.chars().rev().take(8).collect()
}

/// Normalize the settings selection base URL like Go `settingsSelectionBaseURL`.
pub fn settings_selection_base_url(web_app_url: &str) -> Option<String> {
    if web_app_url.is_empty() {
        return None;
    }
    Some(ensure_trailing_slash(web_app_url))
}

/// Build the Go URL used by `Fetcher.CreateSettingsButton`.
pub fn settings_button_url(web_app_url: &str, chat_id: i64) -> Option<String> {
    let base = settings_selection_base_url(web_app_url)?;
    Some(format!(
        "{base}settings/?chat_id={chat_id}&signature={}",
        settings_signature(chat_id)
    ))
}

/// Build the personal settings WebApp URL in the settings selection keyboard.
pub fn settings_selection_personal_url(base_url: &str, user_id: i64) -> String {
    format!(
        "{base_url}settings/?chat_id={user_id}&user_id={user_id}&signature={}",
        settings_signature(user_id)
    )
}

/// Build a group settings WebApp URL in the settings selection keyboard.
pub fn settings_selection_chat_url(base_url: &str, chat_id: i64, user_id: i64) -> String {
    format!(
        "{base_url}settings/?chat_id={chat_id}&user_id={user_id}&signature={}",
        settings_signature(chat_id)
    )
}

/// Build the private `/settings` command WebApp URL like Go `settingsWebAppURL`.
pub fn private_settings_web_app_url(base_url: &str, user_id: i64) -> String {
    format!(
        "{base_url}/settings/index.html?signature={}",
        settings_signature(user_id)
    )
}

fn ensure_trailing_slash(value: &str) -> String {
    if value.ends_with('/') {
        value.to_owned()
    } else {
        format!("{value}/")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        private_settings_web_app_url, settings_button_url, settings_selection_base_url,
        settings_selection_chat_url, settings_selection_personal_url, settings_signature,
    };

    #[test]
    fn settings_signature_matches_go_sha224_reverse_prefix() {
        assert_eq!(settings_signature(42), "780e28cf");
        assert_eq!(settings_signature(-1001234567890), "8ebdb694");
    }

    #[test]
    fn settings_button_url_matches_go_create_settings_button_url() {
        assert_eq!(
            settings_button_url("https://plotva.example", 42).as_deref(),
            Some("https://plotva.example/settings/?chat_id=42&signature=780e28cf")
        );
        assert_eq!(
            settings_button_url("https://plotva.example/", 42).as_deref(),
            Some("https://plotva.example/settings/?chat_id=42&signature=780e28cf")
        );
        assert_eq!(settings_button_url("", 42), None);
    }

    #[test]
    fn settings_selection_urls_match_go_query_shapes() -> Result<(), Box<dyn std::error::Error>> {
        let base = settings_selection_base_url("https://plotva.example")
            .ok_or("settings base URL must be present")?;
        assert_eq!(base, "https://plotva.example/");
        assert_eq!(
            settings_selection_personal_url(&base, 42),
            "https://plotva.example/settings/?chat_id=42&user_id=42&signature=780e28cf"
        );
        assert_eq!(
            settings_selection_chat_url(&base, -1001234567890, 42),
            "https://plotva.example/settings/?chat_id=-1001234567890&user_id=42&signature=8ebdb694"
        );
        assert_eq!(settings_selection_base_url(""), None);
        Ok(())
    }

    #[test]
    fn private_settings_url_preserves_go_base_url_joining() {
        assert_eq!(
            private_settings_web_app_url("https://plotva.example", 42),
            "https://plotva.example/settings/index.html?signature=780e28cf"
        );
        assert_eq!(
            private_settings_web_app_url("https://plotva.example/", 42),
            "https://plotva.example//settings/index.html?signature=780e28cf"
        );
    }
}
