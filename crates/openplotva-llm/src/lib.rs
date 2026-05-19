
/// Minimal provider identity trait for future LLM adapters.
pub trait ChatProvider: Send + Sync {
    /// Stable provider name for logs and diagnostics.
    fn provider_name(&self) -> &str;
}
