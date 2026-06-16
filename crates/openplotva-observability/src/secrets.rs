//! Secret masking for log output.
//!
//! Telegram bot tokens (and other registered secrets) must never reach stdout
//! or the runtime log buffer. Redaction happens at the logging boundary so it
//! covers every call site regardless of where the secret was formatted in.

use std::{
    borrow::Cow,
    sync::{OnceLock, RwLock},
};

use regex::Regex;

const MASK: &str = "<redacted>";

/// Telegram bot tokens as they appear in Bot API URLs (`bot<id>:<token>`).
/// The `bot<id>:` prefix is captured so it can be kept for debugging while the
/// secret tail is masked.
fn bot_token_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(bot[0-9]{6,}:)[A-Za-z0-9_-]{30,}").expect("static bot-token regex is valid")
    })
}

fn literals() -> &'static RwLock<Vec<String>> {
    static LITERALS: OnceLock<RwLock<Vec<String>>> = OnceLock::new();
    LITERALS.get_or_init(|| RwLock::new(Vec::new()))
}

/// Register a literal secret value so it is masked wherever it appears in logs.
///
/// Short values are ignored to avoid masking innocuous text.
pub fn register_secret(value: &str) {
    let value = value.trim();
    if value.len() < 8 {
        return;
    }
    let mut literals = literals()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !literals.iter().any(|existing| existing == value) {
        literals.push(value.to_owned());
    }
}

/// Redact known secrets from one log fragment.
///
/// Returns the input untouched (borrowed) when nothing matched.
pub fn redact(input: &str) -> Cow<'_, str> {
    let mut current: Cow<'_, str> = Cow::Borrowed(input);

    {
        let literals = literals()
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for secret in literals.iter() {
            if current.contains(secret.as_str()) {
                current = Cow::Owned(current.replace(secret.as_str(), MASK));
            }
        }
    }

    let re = bot_token_re();
    if re.is_match(&current) {
        current = Cow::Owned(re.replace_all(&current, "${1}<redacted>").into_owned());
    }

    current
}

#[cfg(test)]
mod tests {
    use super::{redact, register_secret};
    use std::borrow::Cow;

    #[test]
    fn redacts_telegram_bot_token_but_keeps_bot_id() {
        let input = "GET https://api.telegram.org/file/bot345381228:AAFfake_value_1234567890abcdefghij/photos/x.jpg";
        let out = redact(input);
        assert!(
            !out.contains("AAFfake_value_1234567890abcdefghij"),
            "token must be masked: {out}"
        );
        assert!(out.contains("bot345381228:"), "bot id prefix kept: {out}");
    }

    #[test]
    fn leaves_ordinary_text_unchanged_without_allocating() {
        let input = "shield retrieval matched chat_id=-100123 user_id=456 matches=1";
        assert!(matches!(redact(input), Cow::Borrowed(_)));
        assert_eq!(redact(input), input);
    }

    #[test]
    fn masks_registered_literal_secret() {
        register_secret("zzz-unique-provider-key-abcdef123456");
        let input = "provider error using zzz-unique-provider-key-abcdef123456 token";
        let out = redact(input);
        assert!(
            !out.contains("zzz-unique-provider-key-abcdef123456"),
            "literal secret masked: {out}"
        );
    }
}
