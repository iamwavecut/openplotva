//! Prompt loading and Handlebars rendering.

use handlebars::Handlebars;

/// Build an empty Handlebars registry.
pub fn registry() -> Handlebars<'static> {
    Handlebars::new()
}

#[cfg(test)]
mod tests {
    use super::registry;

    #[test]
    fn registry_starts_empty() {
        assert_eq!(registry().get_templates().len(), 0);
    }
}
