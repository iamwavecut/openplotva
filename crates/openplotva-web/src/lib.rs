//! Admin and settings WebApp backend helpers.

use serde::Deserialize;
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
        path: "tokens.css",
        content_type: "text/css; charset=utf-8",
        bytes: include_bytes!("../../../web/admin/tokens.css"),
        sha256: "12042e8934419ae96b20aefe0f1727aed16332ca307a88ef94b2fc75681f978f",
    },
    StaticAsset {
        path: "admin.css",
        content_type: "text/css; charset=utf-8",
        bytes: include_bytes!("../../../web/admin/admin.css"),
        sha256: "6a1c3762eb7389a535cef64084c9e8cfe5ec8de8028d6a891e5b0bc24ac7cc96",
    },
    StaticAsset {
        path: "components.css",
        content_type: "text/css; charset=utf-8",
        bytes: include_bytes!("../../../web/admin/components.css"),
        sha256: "c61a8a304c1f8c60cd61c23a3b5024eb37e8e439c8700abaad12e508e18ba04f",
    },
    StaticAsset {
        path: "components.js",
        content_type: "text/javascript; charset=utf-8",
        bytes: include_bytes!("../../../web/admin/components.js"),
        sha256: "6fa51860405fb7fce5c86084fcea245917decfafb7d1f95b293253fb10a4a11c",
    },
    StaticAsset {
        path: "favicon.svg",
        content_type: "image/svg+xml",
        bytes: include_bytes!("../../../web/admin/favicon.svg"),
        sha256: "7c7a018a353f4547556dc748b4aef10e7ed92f8fb13743859fd57189c079e7cf",
    },
    StaticAsset {
        path: "index.html",
        content_type: "text/html; charset=utf-8",
        bytes: include_bytes!("../../../web/admin/index.html"),
        sha256: "4764c6011926721a6e9d232366fc726b3d0effb9b188e69bae68eb5df4b5737d",
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
        sha256: "8c798715212832b795e3c347714e734f0bfdc896dec9b15ed842c46f796661fd",
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

/// Constant-time byte-slice equality. Returns false on length mismatch, then compares
/// every remaining byte without early exit.
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[must_use]
pub fn validate_telegram_auth<'src, I>(pairs: I, bot_token: &str, provided_hash: &str) -> bool
where
    I: IntoIterator<Item = (&'src str, &'src str)>,
{
    let expected = telegram_auth_hash(pairs, bot_token); // lowercase hex
    let provided = provided_hash.to_ascii_lowercase();
    constant_time_eq(expected.as_bytes(), provided.as_bytes())
}

/// HMAC tag (hex) binding an admin user id to the server secret.
#[must_use]
pub fn admin_session_signature(user_id: i64, secret: &str) -> String {
    hmac_sha256_hex(secret.as_bytes(), user_id.to_string().as_bytes())
}

/// Verify a signed admin-session cookie value of the form `<user_id>.<hex_sig>`.
/// Returns the user id only when the signature verifies. Rejects unsigned legacy
/// values (no `.`), tamper, and malformed input.
#[must_use]
pub fn verify_admin_session_value(value: &str, secret: &str) -> Option<i64> {
    let (user_part, sig) = value.split_once('.')?;
    let user_id = user_part.parse::<i64>().ok()?;
    let expected = admin_session_signature(user_id, secret);
    if constant_time_eq(expected.as_bytes(), sig.as_bytes()) {
        Some(user_id)
    } else {
        None
    }
}

/// Telegram user identity extracted from a validated WebApp `initData` payload.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct TelegramWebAppUser {
    pub id: i64,
    #[serde(default)]
    pub first_name: String,
    #[serde(default)]
    pub username: Option<String>,
}

/// Validate a Telegram WebApp `initData` payload and return the caller identity.
///
/// The WebApp key derivation differs from the Login Widget (`validate_telegram_auth`):
/// `secret_key = HMAC_SHA256(key = "WebAppData", message = bot_token)`, then
/// `hash = HMAC_SHA256(key = secret_key, message = data_check_string)`.
///
/// `now_unix` is passed in (not read from a clock) so freshness can be exercised in tests.
/// Returns `None` when the payload is missing `hash`/`user`, the bot token is empty,
/// the signature does not match (constant-time compare), or `auth_date` is older than
/// `max_age_seconds`.
#[must_use]
pub fn validate_webapp_init_data(
    init_data: &str,
    bot_token: &str,
    max_age_seconds: u64,
    now_unix: i64,
) -> Option<TelegramWebAppUser> {
    if bot_token.is_empty() {
        return None;
    }

    let mut provided_hash: Option<String> = None;
    let mut auth_date: Option<i64> = None;
    let mut user_field: Option<String> = None;
    let mut check_pairs: Vec<(String, String)> = Vec::new();
    for (key, value) in url::form_urlencoded::parse(init_data.as_bytes()) {
        match key.as_ref() {
            "hash" => provided_hash = Some(value.into_owned()),
            other => {
                if other == "auth_date" {
                    auth_date = value.parse::<i64>().ok();
                }
                if other == "user" {
                    user_field = Some(value.to_string());
                }
                check_pairs.push((other.to_owned(), value.into_owned()));
            }
        }
    }

    let provided_hash = provided_hash?;
    let auth_date = auth_date?;
    if now_unix.saturating_sub(auth_date) > max_age_seconds as i64 {
        return None;
    }
    let user_field = user_field?;

    check_pairs.sort_by(|left, right| left.0.cmp(&right.0));
    let data_check_string = check_pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("\n");

    let secret_key_hex = hmac_sha256_hex(b"WebAppData", bot_token.as_bytes());
    let secret_key = hex::decode(secret_key_hex).ok()?;
    let computed = hmac_sha256_hex(&secret_key, data_check_string.as_bytes());
    if !constant_time_eq(computed.as_bytes(), provided_hash.as_bytes()) {
        return None;
    }

    serde_json::from_str::<TelegramWebAppUser>(&user_field).ok()
}

#[must_use]
pub fn admin_session_cookie(user_id: i64, secret: &str) -> String {
    let sig = admin_session_signature(user_id, secret);
    format!(
        "{ADMIN_SESSION_COOKIE_NAME}={user_id}.{sig}; Path={ADMIN_SESSION_COOKIE_PATH}; Max-Age={ADMIN_SESSION_COOKIE_MAX_AGE_SECONDS}; HttpOnly; SameSite=Lax"
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
        StaticAssetGroup, admin_session_cookie, constant_time_eq, hmac_sha256_hex,
        private_settings_web_app_url, settings_button_url, settings_selection_base_url,
        settings_selection_chat_url, settings_selection_personal_url, settings_signature,
        static_asset, static_asset_sha256_hex, static_assets, telegram_auth_hash,
        truncate_custom_persona, validate_settings_access_signature, validate_telegram_auth,
        validate_webapp_init_data, verify_admin_session_value,
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

    // --- Design-system enforcement ("no bypassing") ---
    // The admin UI must route every interactive element through the pl-* component
    // library (web/admin/components.js) and tokens (web/admin/tokens.css). These tests
    // fail the build if hand-rolled controls, inline handlers/styles, native dialogs,
    // or hardcoded colors creep back into the shipped admin assets.

    const ADMIN_INDEX_HTML: &str = include_str!("../../../web/admin/index.html");
    const ADMIN_COMPONENTS_CSS: &str = include_str!("../../../web/admin/components.css");
    const ADMIN_CSS: &str = include_str!("../../../web/admin/admin.css");

    #[test]
    fn admin_markup_routes_through_design_system() {
        let html = ADMIN_INDEX_HTML;
        // No inline event handlers or styles in the app markup.
        assert!(
            !html.contains("onclick="),
            "index.html must not use inline onclick= (wire actions via data-action)"
        );
        assert!(
            !html.contains("onsubmit="),
            "index.html must not use inline onsubmit= (use <form data-action>)"
        );
        assert!(
            !html.contains("style=\""),
            "index.html must not use inline style= (use token-backed classes)"
        );
        assert!(
            !html.contains(".onclick ="),
            "use addEventListener, not el.onclick ="
        );

        // Native dialogs are forbidden: every alert(/confirm( must be a PL.* call.
        assert_eq!(
            html.matches("alert(").count(),
            html.matches("PL.alert(").count(),
            "index.html must use PL.toast/PL.alert, not native alert()"
        );
        assert_eq!(
            html.matches("confirm(").count(),
            html.matches("PL.confirm(").count(),
            "index.html must use PL.confirm, not native confirm()"
        );

        // Interactive controls come from the pl-* library, not raw HTML tags.
        for tag in ["<button", "<input", "<select", "<textarea", "<table"] {
            assert!(
                !html.contains(tag),
                "index.html must not contain a raw {tag} — use the matching pl-* component"
            );
        }

        // The library + tokens must stay wired in.
        for asset in ["tokens.css", "components.css", "components.js"] {
            assert!(html.contains(asset), "index.html must link {asset}");
        }
    }

    #[test]
    fn admin_styles_keep_colors_in_tokens() {
        // components.css and admin.css reference tokens only — color literals live in tokens.css.
        for (name, css) in [
            ("components.css", ADMIN_COMPONENTS_CSS),
            ("admin.css", ADMIN_CSS),
        ] {
            assert!(
                !css.contains(": #") && !css.contains(":#"),
                "{name} must not hardcode hex color values — use var(--c-*) tokens from tokens.css"
            );
            assert!(
                !css.contains("rgba(") && !css.contains("rgb("),
                "{name} must not hardcode rgb/rgba colors — use tokens or color-mix(var(--c-*))"
            );
        }
        // The token layer was extracted out of admin.css into tokens.css.
        assert!(
            !ADMIN_CSS.contains("--c-primary:"),
            "design tokens must be defined in tokens.css, not admin.css"
        );
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
    fn admin_session_cookie_is_signed_and_round_trips() {
        let secret = "123:ABC";
        let cookie = admin_session_cookie(7, secret);
        // value is "7.<64 hex chars>"
        let value = cookie
            .split(';')
            .next()
            .expect("cookie pair")
            .strip_prefix("admin_session=")
            .expect("name prefix");
        assert_eq!(verify_admin_session_value(value, secret), Some(7));
        // tamper / wrong secret / legacy unsigned are rejected
        assert_eq!(verify_admin_session_value("7", secret), None);
        assert_eq!(verify_admin_session_value(value, "wrong"), None);
        assert!(cookie.contains("HttpOnly; SameSite=Lax"));
    }

    #[test]
    fn validate_telegram_auth_is_case_insensitive_and_constant_time() {
        let pairs = [("id", "7"), ("auth_date", "1700000000")];
        let hash = telegram_auth_hash(pairs, "123:ABC");
        assert!(validate_telegram_auth(pairs, "123:ABC", &hash)); // lowercase
        assert!(validate_telegram_auth(
            pairs,
            "123:ABC",
            &hash.to_uppercase()
        )); // uppercase
        let mut tampered = hash.clone();
        tampered.replace_range(0..1, if &hash[0..1] == "0" { "1" } else { "0" });
        assert!(!validate_telegram_auth(pairs, "123:ABC", &tampered)); // one char off
        assert!(!validate_telegram_auth(pairs, "123:ABC", "deadbeef")); // length mismatch
    }

    const WEBAPP_FIXTURE_TOKEN: &str = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11";

    fn webapp_init_data_fixture(auth_date: i64) -> String {
        let user = r#"{"id":42,"first_name":"Ada","username":"ada"}"#;
        let pairs = [
            ("auth_date", auth_date.to_string()),
            ("query_id", "AAEa".to_owned()),
            ("user", user.to_owned()),
        ];
        let data_check_string = pairs
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("\n");
        let secret_key_hex = hmac_sha256_hex(b"WebAppData", WEBAPP_FIXTURE_TOKEN.as_bytes());
        let secret_key = hex::decode(secret_key_hex).expect("hmac hex decodes to bytes");
        let hash = hmac_sha256_hex(&secret_key, data_check_string.as_bytes());
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in &pairs {
            serializer.append_pair(key, value);
        }
        serializer.append_pair("hash", &hash);
        serializer.finish()
    }

    #[test]
    fn webapp_init_data_validates_with_webappdata_key_derivation() {
        let init_data = webapp_init_data_fixture(1_700_000_000);
        let user =
            validate_webapp_init_data(&init_data, WEBAPP_FIXTURE_TOKEN, 3_600, 1_700_000_100)
                .expect("freshly signed initData validates");
        assert_eq!(user.id, 42);
        assert_eq!(user.first_name, "Ada");
        assert_eq!(user.username.as_deref(), Some("ada"));
    }

    #[test]
    fn webapp_init_data_rejects_tampered_hash() {
        let init_data = webapp_init_data_fixture(1_700_000_000);
        let tampered = init_data.replace("hash=", "hash=00");
        assert!(
            validate_webapp_init_data(&tampered, WEBAPP_FIXTURE_TOKEN, 3_600, 1_700_000_100)
                .is_none()
        );
        assert!(
            validate_webapp_init_data(&init_data, "999999:WRONG", 3_600, 1_700_000_100).is_none()
        );
    }

    #[test]
    fn webapp_init_data_rejects_stale_auth_date() {
        let init_data = webapp_init_data_fixture(1_700_000_000);
        assert!(
            validate_webapp_init_data(&init_data, WEBAPP_FIXTURE_TOKEN, 3_600, 1_700_003_601)
                .is_none()
        );
        assert!(
            validate_webapp_init_data(&init_data, WEBAPP_FIXTURE_TOKEN, 3_600, 1_700_003_600)
                .is_some()
        );
    }

    #[test]
    fn webapp_init_data_rejects_missing_hash_or_user() {
        let init_data = webapp_init_data_fixture(1_700_000_000);
        let without_hash = init_data
            .split('&')
            .filter(|pair| !pair.starts_with("hash="))
            .collect::<Vec<_>>()
            .join("&");
        assert!(
            validate_webapp_init_data(&without_hash, WEBAPP_FIXTURE_TOKEN, 3_600, 1_700_000_100)
                .is_none()
        );

        let no_user_pairs = [("auth_date", "1700000000"), ("query_id", "AAEa")];
        let data_check_string = no_user_pairs
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("\n");
        let secret_key_hex = hmac_sha256_hex(b"WebAppData", WEBAPP_FIXTURE_TOKEN.as_bytes());
        let secret_key = hex::decode(secret_key_hex).expect("hmac hex decodes to bytes");
        let hash = hmac_sha256_hex(&secret_key, data_check_string.as_bytes());
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in &no_user_pairs {
            serializer.append_pair(key, value);
        }
        serializer.append_pair("hash", &hash);
        let without_user = serializer.finish();
        assert!(
            validate_webapp_init_data(&without_user, WEBAPP_FIXTURE_TOKEN, 3_600, 1_700_000_100)
                .is_none()
        );
    }

    #[test]
    fn webapp_init_data_rejects_empty_bot_token() {
        let init_data = webapp_init_data_fixture(1_700_000_000);
        assert!(validate_webapp_init_data(&init_data, "", 3_600, 1_700_000_100).is_none());
    }

    #[test]
    fn constant_time_eq_matches_only_identical_slices() {
        assert!(constant_time_eq(b"abc123", b"abc123"));
        assert!(!constant_time_eq(b"abc123", b"abc124"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
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
