//! Plotva-owned LLM provider contracts.

use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use openplotva_dialog::{DialogInput, DialogOutput};

pub mod aifarm;
pub mod gemini;
pub mod retry;
pub mod trace;
pub mod whitecircle;

pub use trace::{
    LlmCallContext, LlmCallObserver, LlmCallRecord, LlmCallTags, LlmCallTrace, LlmCallTraceRegistry,
};

/// Boxed LLM provider error.
pub type ChatProviderError = Box<dyn Error + Send + Sync + 'static>;

/// Boxed async dialog provider future.
pub type ChatProviderFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DialogOutput, ChatProviderError>> + Send + 'a>>;

/// Plotva-owned dialog provider boundary.
pub trait ChatProvider: Send + Sync {
    /// Stable provider name for logs and diagnostics.
    fn provider_name(&self) -> &str;

    /// Run one provider-owned dialog request.
    fn run_dialog<'a>(&'a self, input: DialogInput) -> ChatProviderFuture<'a>;
}

/// Shared chat provider handle.
pub type ChatProviderHandle = Arc<dyn ChatProvider>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContentBlockedError {
    reason: String,
}

impl ContentBlockedError {
    /// Build a model-safety block error.
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    /// Raw block reason.
    #[must_use]
    pub fn reason(&self) -> &str {
        self.reason.trim()
    }
}

impl fmt::Display for ContentBlockedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = if self.reason().is_empty() {
            "blocked"
        } else {
            self.reason()
        };
        write!(f, "content blocked by model safety filters: {reason}")
    }
}

impl Error for ContentBlockedError {}

#[must_use]
pub fn is_content_blocked_error(err: &(dyn Error + 'static)) -> bool {
    if err.downcast_ref::<ContentBlockedError>().is_some() {
        return true;
    }
    err.source().is_some_and(is_content_blocked_error)
        || err
            .to_string()
            .trim_start()
            .starts_with("content blocked by model safety filters:")
}

pub struct FallbackChatProvider {
    primary: ChatProviderHandle,
    fallback: ChatProviderHandle,
    provider_name: String,
}

/// Combined error returned when both primary and fallback providers fail.
#[derive(Debug)]
pub struct FallbackChatProviderError {
    primary_provider: String,
    primary_error: String,
    fallback_provider: String,
    fallback_error: String,
    fallback_retryable_reason: Option<retry::FailureReason>,
}

impl FallbackChatProvider {
    /// Build a provider wrapper that falls back only after retryable primary errors.
    #[must_use]
    pub fn new(primary: ChatProviderHandle, fallback: ChatProviderHandle) -> Self {
        let provider_name = format!(
            "{}+fallback:{}",
            primary.provider_name(),
            fallback.provider_name()
        );
        Self {
            primary,
            fallback,
            provider_name,
        }
    }
}

impl FallbackChatProviderError {
    #[must_use]
    pub const fn retryable_reason(&self) -> Option<retry::FailureReason> {
        self.fallback_retryable_reason
    }
}

impl fmt::Display for FallbackChatProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "primary provider {}: {}; fallback provider {}: {}",
            self.primary_provider, self.primary_error, self.fallback_provider, self.fallback_error
        )
    }
}

impl Error for FallbackChatProviderError {}

impl ChatProvider for FallbackChatProvider {
    fn provider_name(&self) -> &str {
        &self.provider_name
    }

    fn run_dialog<'a>(&'a self, input: DialogInput) -> ChatProviderFuture<'a> {
        Box::pin(async move {
            let mut output = match self.primary.run_dialog(input.clone()).await {
                Ok(output) => output,
                Err(primary_error) => {
                    if retry::retryable_reason(primary_error.as_ref()).is_none() {
                        return Err(primary_error);
                    }
                    return self.run_fallback(input, primary_error).await;
                }
            };
            if output.provider.trim().is_empty() {
                output.provider = self.primary.provider_name().to_owned();
            }
            Ok(output)
        })
    }
}

impl FallbackChatProvider {
    async fn run_fallback(
        &self,
        input: DialogInput,
        primary_error: ChatProviderError,
    ) -> Result<DialogOutput, ChatProviderError> {
        match self.fallback.run_dialog(input).await {
            Ok(mut output) => {
                if output.provider.trim().is_empty() {
                    output.provider = self.fallback.provider_name().to_owned();
                }
                output.fallback_from = self.primary.provider_name().to_owned();
                output.fallback_error = primary_error.to_string();
                Ok(output)
            }
            Err(fallback_error) => {
                let fallback_retryable_reason = retry::retryable_reason(fallback_error.as_ref());
                Err(Box::new(FallbackChatProviderError {
                    primary_provider: self.primary.provider_name().to_owned(),
                    primary_error: primary_error.to_string(),
                    fallback_provider: self.fallback.provider_name().to_owned(),
                    fallback_error: fallback_error.to_string(),
                    fallback_retryable_reason,
                }))
            }
        }
    }
}

#[must_use]
pub fn with_fallback(
    primary: Option<ChatProviderHandle>,
    fallback: Option<ChatProviderHandle>,
) -> Option<ChatProviderHandle> {
    match (primary, fallback) {
        (None, fallback) => fallback,
        (primary, None) => primary,
        (Some(primary), Some(fallback)) => {
            Some(Arc::new(FallbackChatProvider::new(primary, fallback)))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct FakeProvider {
        name: &'static str,
        result: Mutex<Option<Result<DialogOutput, ChatProviderError>>>,
    }

    impl FakeProvider {
        fn ok(name: &'static str, output: DialogOutput) -> Arc<Self> {
            Arc::new(Self {
                name,
                result: Mutex::new(Some(Ok(output))),
            })
        }

        fn err(name: &'static str, err: ChatProviderError) -> Arc<Self> {
            Arc::new(Self {
                name,
                result: Mutex::new(Some(Err(err))),
            })
        }
    }

    impl ChatProvider for FakeProvider {
        fn provider_name(&self) -> &str {
            self.name
        }

        fn run_dialog<'a>(&'a self, _input: DialogInput) -> ChatProviderFuture<'a> {
            Box::pin(async move {
                self.result
                    .lock()
                    .expect("provider result lock")
                    .take()
                    .expect("provider result configured")
            })
        }
    }

    #[tokio::test]
    async fn fallback_provider_skips_fallback_on_primary_success() {
        let provider = FallbackChatProvider::new(
            FakeProvider::ok("aifarm", DialogOutput::default()),
            FakeProvider::ok("genkit", DialogOutput::default()),
        );

        let output = provider
            .run_dialog(DialogInput::default())
            .await
            .expect("run");

        assert_eq!(provider.provider_name(), "aifarm+fallback:genkit");
        assert_eq!(output.provider, "aifarm");
        assert_eq!(output.fallback_from, "");
        assert_eq!(output.fallback_error, "");
    }

    #[tokio::test]
    async fn fallback_provider_uses_fallback_on_retryable_primary_error() {
        let provider = FallbackChatProvider::new(
            FakeProvider::err(
                "aifarm",
                Box::new(retry::ProviderError::new(
                    "aifarm",
                    retry::FailureReason::ProviderUnavailable,
                    "status 503",
                )),
            ),
            FakeProvider::ok(
                "genkit",
                DialogOutput {
                    answer: "ok".to_owned(),
                    ..DialogOutput::default()
                },
            ),
        );

        let output = provider
            .run_dialog(DialogInput::default())
            .await
            .expect("run");

        assert_eq!(output.provider, "genkit");
        assert_eq!(output.fallback_from, "aifarm");
        assert!(output.fallback_error.contains("status 503"));
        assert_eq!(output.answer, "ok");
    }

    #[tokio::test]
    async fn fallback_provider_skips_fallback_on_local_primary_error() {
        let provider = FallbackChatProvider::new(
            FakeProvider::err("aifarm", local_error("validation failed")),
            FakeProvider::ok("genkit", DialogOutput::default()),
        );

        let err = provider
            .run_dialog(DialogInput::default())
            .await
            .expect_err("local primary error");

        assert_eq!(err.to_string(), "validation failed");
    }

    #[tokio::test]
    async fn fallback_provider_does_not_preserve_primary_retryability_when_fallback_error_is_local()
    {
        let provider = FallbackChatProvider::new(
            FakeProvider::err(
                "aifarm",
                Box::new(retry::ProviderError::new(
                    "aifarm",
                    retry::FailureReason::CapacityUnavailable,
                    "capacity unavailable",
                )),
            ),
            FakeProvider::err("genkit", local_error("validation failed")),
        );

        let err = provider
            .run_dialog(DialogInput::default())
            .await
            .expect_err("fallback error");

        assert!(err.to_string().contains("primary provider aifarm"));
        assert!(err.to_string().contains("fallback provider genkit"));
        assert_eq!(retry::retryable_reason(err.as_ref()), None);
    }

    #[test]
    fn content_blocked_error_matches_go_text_and_message_fallback() {
        let typed = ContentBlockedError::new("");

        assert_eq!(
            typed.to_string(),
            "content blocked by model safety filters: blocked"
        );
        assert!(is_content_blocked_error(&typed));

        let message_only = local_error("content blocked by model safety filters: safety");
        assert!(is_content_blocked_error(message_only.as_ref()));
        assert_eq!(retry::retryable_reason(message_only.as_ref()), None);
    }

    fn local_error(message: &'static str) -> ChatProviderError {
        Box::new(std::io::Error::other(message))
    }
}
