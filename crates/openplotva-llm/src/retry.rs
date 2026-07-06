//! LLM provider retry classification.

use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureReason {
    /// Provider/service unavailable.
    ProviderUnavailable,
    /// Discovery or backend capacity unavailable.
    CapacityUnavailable,
    /// Provider overloaded or rate-limited.
    ProviderOverloaded,
    /// Provider timed out.
    ProviderTimeout,
    /// Provider returned a protocol-level invalid response.
    ProviderProtocolError,
}

impl FailureReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderUnavailable => "provider_unavailable",
            Self::CapacityUnavailable => "capacity_unavailable",
            Self::ProviderOverloaded => "provider_overloaded",
            Self::ProviderTimeout => "provider_timeout",
            Self::ProviderProtocolError => "provider_protocol_error",
        }
    }
}

impl fmt::Display for FailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub struct ProviderError {
    provider: String,
    reason: FailureReason,
    message: String,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl ProviderError {
    /// Construct a provider error from a message.
    #[must_use]
    pub fn new(
        provider: impl Into<String>,
        reason: FailureReason,
        message: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into().trim().to_owned(),
            reason,
            message: message.into(),
            source: None,
        }
    }

    /// Wrap a source error.
    pub fn wrap<E>(provider: impl Into<String>, reason: FailureReason, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        let message = source.to_string();
        Self {
            provider: provider.into().trim().to_owned(),
            reason,
            message,
            source: Some(Box::new(source)),
        }
    }

    /// Provider name.
    #[must_use]
    pub fn provider(&self) -> &str {
        self.provider.trim()
    }

    /// Failure reason.
    #[must_use]
    pub const fn reason(&self) -> FailureReason {
        self.reason
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let provider = if self.provider.trim().is_empty() {
            "llm"
        } else {
            self.provider.trim()
        };
        if self.message.trim().is_empty() {
            write!(f, "{provider} provider {}", self.reason)
        } else {
            write!(f, "{provider} provider {}: {}", self.reason, self.message)
        }
    }
}

impl Error for ProviderError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as &(dyn Error + 'static))
    }
}

#[must_use]
pub fn retryable_reason(err: &(dyn Error + 'static)) -> Option<FailureReason> {
    if let Some(provider_error) = find_provider_error(err) {
        return Some(provider_error.reason());
    }
    // reqwest transport failures (connect, TLS, mid-body disconnects) stringify
    // in ways the message rules can't enumerate; the typed variant is authoritative.
    if let Some(crate::aifarm::AifarmClientError::Transport(_)) =
        find_error::<crate::aifarm::AifarmClientError>(err)
    {
        return Some(FailureReason::ProviderUnavailable);
    }
    retryable_reason_from_message(&err.to_string())
}

/// Return whether this is a retryable provider failure.
#[must_use]
pub fn is_retryable_provider_error(err: &(dyn Error + 'static)) -> bool {
    retryable_reason(err).is_some()
}

#[must_use]
pub fn provider_name<'a>(err: &'a (dyn Error + 'static)) -> &'a str {
    find_provider_error(err)
        .map(ProviderError::provider)
        .unwrap_or_default()
}

/// Classify retryability from an error message.
#[must_use]
pub fn retryable_reason_from_message(message: &str) -> Option<FailureReason> {
    let message = message.trim();
    if message.is_empty() || contains_any_ascii_fold(message, RETRYABLE_REJECT_PHRASES) {
        return None;
    }
    for (reason, phrases) in RETRYABLE_MESSAGE_RULES {
        if contains_any_ascii_fold(message, phrases) {
            return Some(*reason);
        }
    }
    None
}

fn find_provider_error<'a>(err: &'a (dyn Error + 'static)) -> Option<&'a ProviderError> {
    find_error::<ProviderError>(err)
}

fn find_error<'a, T: Error + 'static>(err: &'a (dyn Error + 'static)) -> Option<&'a T> {
    if let Some(found) = err.downcast_ref::<T>() {
        return Some(found);
    }
    err.source().and_then(find_error::<T>)
}

const RETRYABLE_REJECT_PHRASES: &[&str] = &[
    "telegram api error",
    "validation failed",
    "user cancelled",
    "user canceled",
];

const RETRYABLE_MESSAGE_RULES: &[(FailureReason, &[&str])] = &[
    (
        FailureReason::CapacityUnavailable,
        &[
            "capacity unavailable",
            "service capacity unavailable",
            "no slots",
            "no slot available",
            "resource exhausted",
        ],
    ),
    (
        FailureReason::ProviderProtocolError,
        &[
            "tool protocol error",
            "empty final text",
            "no tool calls",
            "decode chat completion payload",
            "decode memory extractor response",
            "response body is empty",
        ],
    ),
    (
        FailureReason::ProviderTimeout,
        &[
            "client.timeout",
            "context deadline exceeded",
            "timeout",
            "timed out",
        ],
    ),
    (
        FailureReason::ProviderOverloaded,
        &["high demand", "overload", "overloaded", "rate limit", "429"],
    ),
    (
        FailureReason::ProviderUnavailable,
        &[
            "status: unavailable",
            "status unavailable",
            "service unavailable",
            "service disabled",
            "connection refused",
            "connection reset",
            "error sending request for url",
            "error decoding response body",
            "connection closed before message completed",
            "incomplete message",
            "bad gateway",
            "gateway timeout",
            "error 503",
            "status 503",
            " 503",
            " 502",
            " 504",
        ],
    ),
];

fn contains_any_ascii_fold(haystack: &str, needles: &[&str]) -> bool {
    needles
        .iter()
        .copied()
        .any(|needle| contains_ascii_fold(haystack, needle))
}

fn contains_ascii_fold(haystack: &str, needle: &str) -> bool {
    let haystack = haystack.as_bytes();
    let needle = needle.as_bytes();
    if needle.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_go_retry_reasons() {
        let cases = [
            (
                ProviderError::new(
                    "gemini",
                    FailureReason::ProviderOverloaded,
                    "503 high demand",
                )
                .to_string(),
                FailureReason::ProviderOverloaded,
            ),
            (
                "discovery service capacity unavailable: status 503".to_owned(),
                FailureReason::CapacityUnavailable,
            ),
            (
                "Error 503, Status: UNAVAILABLE".to_owned(),
                FailureReason::ProviderUnavailable,
            ),
            (
                "submit dialog job: status 409: {\"detail\":\"service disabled\"}".to_owned(),
                FailureReason::ProviderUnavailable,
            ),
            (
                "error sending request for url (http://aifarm.barb-gray.ts.net:50051/v1/jobs/blocking)".to_owned(),
                FailureReason::ProviderUnavailable,
            ),
            (
                "context deadline exceeded".to_owned(),
                FailureReason::ProviderTimeout,
            ),
            (
                "tool protocol error: empty final text on iteration 2".to_owned(),
                FailureReason::ProviderProtocolError,
            ),
            (
                "extract memory batch: decode memory extractor response".to_owned(),
                FailureReason::ProviderProtocolError,
            ),
        ];

        for (message, reason) in cases {
            assert_eq!(retryable_reason_from_message(&message), Some(reason));
        }
    }

    #[test]
    fn typed_provider_error_wins_over_message() {
        let err = ProviderError::new(" gemini ", FailureReason::ProviderOverloaded, "status 503");

        assert_eq!(
            retryable_reason(&err),
            Some(FailureReason::ProviderOverloaded)
        );
        assert!(is_retryable_provider_error(&err));
        assert_eq!(provider_name(&err), "gemini");
        assert_eq!(
            err.to_string(),
            "gemini provider provider_overloaded: status 503"
        );
    }

    #[test]
    fn transport_client_error_is_retryable_via_typed_source() {
        // Message deliberately matches no phrase rule: the typed variant alone
        // must classify reqwest transport failures.
        let err =
            crate::aifarm::AifarmClientError::Transport("tls handshake eof mid-frame".to_owned());

        assert_eq!(
            retryable_reason(&err),
            Some(FailureReason::ProviderUnavailable)
        );
    }

    #[test]
    fn transport_body_decode_messages_are_retryable() {
        for message in [
            "error decoding response body",
            "connection closed before message completed",
            "incomplete message",
        ] {
            assert_eq!(
                retryable_reason_from_message(message),
                Some(FailureReason::ProviderUnavailable),
                "{message}"
            );
        }
    }

    #[test]
    fn rejects_local_and_telegram_errors() {
        for message in [
            "telegram API error: Too Many Requests: retry after 2160",
            "validation failed: prompt is empty",
            "user cancelled the job",
            "user canceled the job",
            "",
        ] {
            assert_eq!(retryable_reason_from_message(message), None, "{message}");
        }
    }

    #[test]
    fn ascii_fold_phrase_matching_handles_mixed_case() {
        assert_eq!(
            retryable_reason_from_message("Error 503, Status: UNAVAILABLE"),
            Some(FailureReason::ProviderUnavailable)
        );
    }
}
