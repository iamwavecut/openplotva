//! Admin and settings WebApp backend helpers.

use sha2::{Digest, Sha224, Sha256};

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "web";

pub const SETTINGS_SIGNATURE_SALT: &str = "plotva-signature-salt-2024";

pub const ADMIN_SESSION_COOKIE_NAME: &str = "admin_session";

pub const ADMIN_SESSION_COOKIE_PATH: &str = "/admin/";

pub const ADMIN_SESSION_COOKIE_MAX_AGE_SECONDS: i64 = 86_400 * 7;

pub const CUSTOM_PERSONA_MAX_CHARS: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticAssetGroup {
    Admin,
    Settings,
}

/// Embedded static file metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StaticAsset {
    /// Path relative to the mounted asset group.
    pub path: &'static str,
    pub content_type: &'static str,
    pub bytes: &'static [u8],
    pub sha256: &'static str,
}

const ADMIN_ASSETS: &[StaticAsset] = &[
    StaticAsset {
        path: "admin.css",
        content_type: "text/css; charset=utf-8",
        bytes: include_bytes!("../../../web/admin/admin.css"),
        sha256: "fabe22a83932e1670f2174f64ee39ef82f0bea9d86d501c31ea92584881f8b7b",
    },
    StaticAsset {
        path: "favicon.svg",
        content_type: "image/svg+xml",
        bytes: include_bytes!("../../../web/admin/favicon.svg"),
        sha256: "bd3d0ea8437cbfbc7bf337cbae239c81b712ca8b51878ff75be7d8ea3405de5b",
    },
    StaticAsset {
        path: "index.html",
        content_type: "text/html; charset=utf-8",
        bytes: include_bytes!("../../../web/admin/index.html"),
        sha256: "30af2f0d04945e82a58b17a045c9dabb53ccd9b844420995b7d58aed01c1f64a",
    },
    StaticAsset {
        path: "login.html",
        content_type: "text/html; charset=utf-8",
        bytes: include_bytes!("../../../web/admin/login.html"),
        sha256: "5efc575dd4770d788b6d1efa1c920e36d319d64d32a608f4c85e9bffbd348661",
    },
    StaticAsset {
        path: "site.webmanifest",
        content_type: "application/manifest+json",
        bytes: include_bytes!("../../../web/admin/site.webmanifest"),
        sha256: "e9214f0d8f54006a47ccc1aa6b247d229cd0a0da78163635c76c3439fe7c999e",
    },
];

const SETTINGS_ASSETS: &[StaticAsset] = &[
    StaticAsset {
        path: "index.html",
        content_type: "text/html; charset=utf-8",
        bytes: include_bytes!("../../../web/settings/index.html"),
        sha256: "fbe0416b5e718a39269963d3a0fd9a6e7bac213d3c7951ff967287ff223453f8",
    },
    StaticAsset {
        path: "index.js",
        content_type: "text/javascript; charset=utf-8",
        bytes: include_bytes!("../../../web/settings/index.js"),
        sha256: "1e854fc52928bb34e0ec27891c842740611ffcbd4e2f71979a01dc17331070cb",
    },
    StaticAsset {
        path: "landing.html",
        content_type: "text/html; charset=utf-8",
        bytes: include_bytes!("../../../web/settings/landing.html"),
        sha256: "97a68748cd17ce7f87f44b9d9e1872b470eb7ca77feb3a1eae1caeeb3a5ca36b",
    },
];

#[must_use]
pub fn static_assets(group: StaticAssetGroup) -> &'static [StaticAsset] {
    match group {
        StaticAssetGroup::Admin => ADMIN_ASSETS,
        StaticAssetGroup::Settings => SETTINGS_ASSETS,
    }
}

#[must_use]
pub fn static_asset(group: StaticAssetGroup, path: &str) -> Option<StaticAsset> {
    let path = normalize_static_asset_path(path)?;
    static_assets(group)
        .iter()
        .copied()
        .find(|asset| asset.path == path)
}

#[must_use]
pub fn static_asset_sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[must_use]
pub fn telegram_auth_hash<'src, I>(pairs: I, bot_token: &str) -> String
where
    I: IntoIterator<Item = (&'src str, &'src str)>,
{
    let mut lines = pairs
        .into_iter()
        .filter(|(key, _)| *key != "hash")
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    lines.sort();
    let data_check = lines.join("\n");
    let secret = Sha256::digest(bot_token.as_bytes());
    hmac_sha256_hex(&secret, data_check.as_bytes())
}

#[must_use]
pub fn validate_telegram_auth<'src, I>(pairs: I, bot_token: &str, provided_hash: &str) -> bool
where
    I: IntoIterator<Item = (&'src str, &'src str)>,
{
    telegram_auth_hash(pairs, bot_token).eq_ignore_ascii_case(provided_hash)
}

#[must_use]
pub fn admin_session_cookie(user_id: i64) -> String {
    format!(
        "{ADMIN_SESSION_COOKIE_NAME}={user_id}; Path={ADMIN_SESSION_COOKIE_PATH}; Max-Age={ADMIN_SESSION_COOKIE_MAX_AGE_SECONDS}; HttpOnly; SameSite=Lax"
    )
}

pub fn settings_signature(chat_id: i64) -> String {
    let input = format!("{chat_id}:{SETTINGS_SIGNATURE_SALT}");
    let hash_hex = hex::encode(Sha224::digest(input.as_bytes()));
    hash_hex.chars().rev().take(8).collect()
}

#[must_use]
pub fn validate_settings_access_signature(chat_id: i64, user_id: i64, signature: &str) -> bool {
    if user_id != 0 && settings_signature(user_id) == signature {
        return true;
    }
    settings_signature(chat_id) == signature
}

#[must_use]
pub fn truncate_custom_persona(persona: Option<String>) -> Option<String> {
    persona.map(|value| {
        if value.chars().count() <= CUSTOM_PERSONA_MAX_CHARS {
            value
        } else {
            value.chars().take(CUSTOM_PERSONA_MAX_CHARS).collect()
        }
    })
}

pub fn settings_selection_base_url(web_app_url: &str) -> Option<String> {
    if web_app_url.is_empty() {
        return None;
    }
    Some(ensure_trailing_slash(web_app_url))
}

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

fn hmac_sha256_hex(key: &[u8], message: &[u8]) -> String {
    const SHA256_BLOCK_SIZE: usize = 64;
    let mut normalized_key = [0_u8; SHA256_BLOCK_SIZE];
    if key.len() > SHA256_BLOCK_SIZE {
        normalized_key[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        normalized_key[..key.len()].copy_from_slice(key);
    }

    let mut inner_pad = [0x36_u8; SHA256_BLOCK_SIZE];
    let mut outer_pad = [0x5c_u8; SHA256_BLOCK_SIZE];
    for index in 0..SHA256_BLOCK_SIZE {
        inner_pad[index] ^= normalized_key[index];
        outer_pad[index] ^= normalized_key[index];
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_hash);
    hex::encode(outer.finalize())
}

fn normalize_static_asset_path(path: &str) -> Option<&str> {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return Some("index.html");
    }
    if path.contains('\\')
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return None;
    }
    Some(path)
}

#[cfg(test)]
mod tests {
    use super::{
        StaticAssetGroup, admin_session_cookie, private_settings_web_app_url, settings_button_url,
        settings_selection_base_url, settings_selection_chat_url, settings_selection_personal_url,
        settings_signature, static_asset, static_asset_sha256_hex, static_assets,
        telegram_auth_hash, truncate_custom_persona, validate_settings_access_signature,
        validate_telegram_auth,
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
    fn settings_access_signature_accepts_user_or_chat_signature_like_go() {
        assert!(validate_settings_access_signature(
            -1001234567890,
            42,
            "780e28cf"
        ));
        assert!(validate_settings_access_signature(
            -1001234567890,
            42,
            "8ebdb694"
        ));
        assert!(!validate_settings_access_signature(
            -1001234567890,
            42,
            "bad"
        ));
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

    #[test]
    fn embedded_web_assets_match_expected_hashes() {
        for group in [StaticAssetGroup::Admin, StaticAssetGroup::Settings] {
            for asset in static_assets(group) {
                assert_eq!(
                    static_asset_sha256_hex(asset.bytes),
                    asset.sha256,
                    "asset hash drifted: {:?}/{}",
                    group,
                    asset.path
                );
            }
        }
    }

    #[test]
    fn static_asset_lookup_matches_go_file_server_index_rules() {
        assert_eq!(
            static_asset(StaticAssetGroup::Settings, "").map(|asset| asset.path),
            Some("index.html")
        );
        assert_eq!(
            static_asset(StaticAssetGroup::Admin, "/login.html").map(|asset| asset.path),
            Some("login.html")
        );
        assert_eq!(static_asset(StaticAssetGroup::Admin, "../index.html"), None);
        assert_eq!(static_asset(StaticAssetGroup::Settings, "missing.js"), None);
    }

    #[test]
    fn telegram_admin_auth_hash_matches_go_login_widget_hmac() {
        let pairs = [
            ("id", "7"),
            ("first_name", "Ada"),
            ("auth_date", "1700000000"),
            ("username", "ada"),
        ];
        let hash = telegram_auth_hash(pairs, "123:ABC");
        assert_eq!(
            hash,
            "c340b883eb8c3556a6f1c1b9086b792ffdd782f369b68777ca404488f89fcfec"
        );
        assert!(validate_telegram_auth(
            pairs,
            "123:ABC",
            "C340B883EB8C3556A6F1C1B9086B792FFDD782F369B68777CA404488F89FCFEC"
        ));
        assert!(!validate_telegram_auth(pairs, "wrong", &hash));
    }

    #[test]
    fn admin_session_cookie_matches_go_cookie_shape() {
        assert_eq!(
            admin_session_cookie(7),
            "admin_session=7; Path=/admin/; Max-Age=604800; HttpOnly; SameSite=Lax"
        );
    }

    #[test]
    fn custom_persona_truncates_by_runes_like_go_webapp() {
        assert_eq!(truncate_custom_persona(None), None);
        assert_eq!(
            truncate_custom_persona(Some("abc".to_owned())).as_deref(),
            Some("abc")
        );
        let long = "я".repeat(1_001);
        let truncated = truncate_custom_persona(Some(long)).expect("truncated persona");
        assert_eq!(truncated.chars().count(), 1_000);
    }
}
