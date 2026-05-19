//! Telegram Bot API boundary for OpenPlotva.

pub const INTEGRATION_CRATE: &str = "carapax";

/// Re-exported `carapax` context type used to anchor the Telegram boundary.
pub type CarapaxContext = carapax::Context;

/// Create an empty Telegram integration context.
pub fn empty_context() -> CarapaxContext {
    CarapaxContext::default()
}

#[cfg(test)]
mod tests {
    use super::INTEGRATION_CRATE;

    #[test]
    fn telegram_boundary_uses_carapax() {
        assert_eq!(INTEGRATION_CRATE, "carapax");
    }
}
