//! Gradius privacy-safe request primitives.

use openplotva_memory::{
    DiscoveryRedactor, DiscoveryRedactorConfig, DiscoveryRedactorError, RedactionReplacementMode,
};
use sha2::{Digest, Sha256};

const USER_ID_SCOPE: &str = "gradius:v1:user";
const CHAT_ID_SCOPE: &str = "gradius:v1:chat";
const GRADIUS_REDACTION_CATEGORIES: [&str; 8] = [
    "account_number",
    "private_address",
    "private_date",
    "private_email",
    "private_person",
    "private_phone",
    "private_url",
    "secret",
];

/// Stable one-way identifiers sent to Gradius instead of Telegram IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GradiusSyntheticIds {
    /// Synthetic dialogue identifier, including Telegram topic identity.
    pub chat_id: String,
    /// Synthetic user identifier, stable across dialogues.
    pub user_id: String,
}

impl GradiusSyntheticIds {
    /// Derive both required Gradius IDs, or return `None` for missing source IDs.
    #[must_use]
    pub fn derive(chat_id: i64, thread_id: Option<i32>, user_id: i64) -> Option<Self> {
        if chat_id == 0 || user_id == 0 {
            return None;
        }
        let thread_id = thread_id.filter(|value| *value > 0).unwrap_or_default();
        Some(Self {
            chat_id: synthetic_id(
                "chat",
                &format!("{CHAT_ID_SCOPE}:{chat_id}:thread:{thread_id}"),
            ),
            user_id: synthetic_id("user", &format!("{USER_ID_SCOPE}:{user_id}")),
        })
    }
}

fn synthetic_id(prefix: &str, source: &str) -> String {
    let digest = Sha256::digest(source.as_bytes());
    format!("{prefix}_{}", hex::encode(digest))
}

/// Privacy-filter client fixed to the complete PII set and readable placeholders.
#[derive(Debug)]
pub struct GradiusPrivacyRedactor {
    redactor: DiscoveryRedactor,
}

impl GradiusPrivacyRedactor {
    /// Build a Gradius redactor from the shared Discovery transport configuration.
    pub fn new(mut config: DiscoveryRedactorConfig) -> Result<Self, reqwest::Error> {
        config.categories = Self::categories()
            .into_iter()
            .map(ToOwned::to_owned)
            .collect();
        Ok(Self {
            redactor: DiscoveryRedactor::new(config)?,
        })
    }

    /// Redact outbound Gradius text. Errors are returned so callers can skip the ad request.
    pub async fn redact_text(&self, text: &str) -> Result<String, DiscoveryRedactorError> {
        self.redactor
            .redact_text_with_mode(text, Self::replacement_mode())
            .await
    }

    #[must_use]
    const fn categories() -> [&'static str; 8] {
        GRADIUS_REDACTION_CATEGORIES
    }

    #[must_use]
    const fn replacement_mode() -> RedactionReplacementMode {
        RedactionReplacementMode::TypedPlaceholders
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_ids_are_stable_and_domain_separated() {
        let ids = GradiusSyntheticIds::derive(-100, Some(9), 200).expect("valid ids");

        assert_eq!(
            ids.chat_id,
            "chat_316c49b9f77f2743bb1ccadfb76bc12c2bc84bdc7c35e297546e9839d09d2494"
        );
        assert_eq!(
            ids.user_id,
            "user_96513f4d8d56fa336569cc54cd4048e63fc74067069b5a55090e235c4d7c72c8"
        );
        assert_eq!(
            GradiusSyntheticIds::derive(-100, Some(9), 200),
            Some(ids.clone())
        );
        assert_ne!(
            GradiusSyntheticIds::derive(200, None, 200)
                .expect("private chat ids")
                .chat_id,
            ids.user_id
        );
    }

    #[test]
    fn synthetic_ids_change_with_identity_or_dialogue() {
        let base = GradiusSyntheticIds::derive(-100, Some(9), 200).expect("base ids");
        let other_user = GradiusSyntheticIds::derive(-100, Some(9), 201).expect("other user");
        let other_thread = GradiusSyntheticIds::derive(-100, Some(10), 200).expect("other thread");

        assert_ne!(base.user_id, other_user.user_id);
        assert_eq!(base.chat_id, other_user.chat_id);
        assert_ne!(base.chat_id, other_thread.chat_id);
        assert_eq!(base.user_id, other_thread.user_id);
    }

    #[test]
    fn synthetic_ids_reject_missing_real_identity() {
        assert_eq!(GradiusSyntheticIds::derive(0, None, 200), None);
        assert_eq!(GradiusSyntheticIds::derive(-100, None, 0), None);
    }

    #[test]
    fn privacy_redactor_uses_all_labels_and_typed_placeholders() {
        assert_eq!(
            GradiusPrivacyRedactor::categories(),
            [
                "account_number",
                "private_address",
                "private_date",
                "private_email",
                "private_person",
                "private_phone",
                "private_url",
                "secret",
            ]
        );
        assert_eq!(
            GradiusPrivacyRedactor::replacement_mode(),
            openplotva_memory::RedactionReplacementMode::TypedPlaceholders
        );
    }

    #[tokio::test]
    async fn privacy_redactor_returns_errors_instead_of_unredacted_text() {
        let config = openplotva_memory::DiscoveryRedactorConfig {
            base_url: "not a url".to_owned(),
            ..Default::default()
        };
        let redactor = GradiusPrivacyRedactor::new(config).expect("client");

        assert!(redactor.redact_text("alice@example.test").await.is_err());
    }
}
