//! Plotva-owned LLM provider contracts.

use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use openplotva_dialog::{DialogInput, DialogOutput};

pub mod aifarm;
pub mod gemini;
pub mod model_listing;
pub mod provider_schema;
pub mod retry;
pub mod router;
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
