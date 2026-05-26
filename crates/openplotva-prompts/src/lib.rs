//! Prompt loading and Handlebars rendering.

use std::{
    collections::BTreeMap,
    env, fs, io,
    path::{Path, PathBuf},
};

use handlebars::{
    Context, Handlebars, Helper, HelperDef, HelperResult, Output, RenderContext, RenderErrorReason,
    Renderable,
};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

/// Prompt rendering error.
#[derive(Debug, Error)]
pub enum PromptError {
    /// Filesystem operation failed.
    #[error("{action} prompt path {path:?}: {source}")]
    Io {
        /// Action being performed.
        action: &'static str,
        /// Path involved.
        path: PathBuf,
        /// Source IO error.
        #[source]
        source: io::Error,
    },
    /// Template registration failed.
    #[error("register prompt {name:?}: {source}")]
    Register {
        /// Template name.
        name: String,
        /// Source error.
        #[source]
        source: handlebars::TemplateError,
    },
    /// Data conversion failed.
    #[error("convert prompt data {name:?}: {source}")]
    Data {
        /// Prompt name.
        name: String,
        /// Source error.
        #[source]
        source: serde_json::Error,
    },
    /// Rendering failed.
    #[error("render prompt {name:?}: {source}")]
    Render {
        /// Prompt name.
        name: String,
        /// Source error.
        #[source]
        source: handlebars::RenderError,
    },
}

/// One rendered role-scoped prompt message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptMessage {
    /// Message role.
    pub role: String,
    /// Trimmed message content.
    pub content: String,
}

/// Leading dotprompt YAML front matter as string key/value pairs.
pub type PromptMetadata = BTreeMap<String, String>;

const ROLE_MARKER_PREFIX: &str = "@@OPENPLOTVA_ROLE:";
const ROLE_MARKER_SUFFIX: &str = "@@";

/// Build a Handlebars registry with Plotva helpers but without loaded files.
#[must_use]
pub fn registry() -> Handlebars<'static> {
    registry_with_role(RoleRenderMode::Blank)
}

fn registry_with_role(role_mode: RoleRenderMode) -> Handlebars<'static> {
    let mut registry = Handlebars::new();
    registry.register_helper("ifEquals", Box::new(IfEqualsHelper));
    registry.register_helper("role", Box::new(RoleHelper { mode: role_mode }));
    registry
}

/// Return the prompt root.
#[must_use]
pub fn prompt_root() -> PathBuf {
    env::var_os("OPENPLOTVA_PROMPTS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(default_prompt_root)
}

/// Read a raw prompt by name, with or without `.prompt` suffix.
pub fn read(name: &str) -> Result<String, PromptError> {
    let path = prompt_root().join(prompt_filename(name));
    fs::read_to_string(&path).map_err(|source| PromptError::Io {
        action: "read",
        path,
        source,
    })
}

/// Read leading dotprompt YAML front matter for a prompt.
pub fn prompt_metadata(name: &str) -> Result<PromptMetadata, PromptError> {
    let source = read(name)?;
    Ok(split_front_matter(&source).0)
}

/// Load all `.prompt` files from `prompt_root`.
pub fn load_registry(prompt_root: &Path) -> Result<Handlebars<'static>, PromptError> {
    let mut registry = registry();
    register_prompt_tree(&mut registry, prompt_root)?;
    Ok(registry)
}

/// Render a prompt by name with JSON-like data.
pub fn render<T>(name: &str, data: &T) -> Result<String, PromptError>
where
    T: Serialize + ?Sized,
{
    let prompt_root = prompt_root();
    let mut registry = registry_with_role(RoleRenderMode::Marker);
    register_prompt_tree(&mut registry, &prompt_root)?;
    let name = prompt_name(name);
    let data = serde_json::to_value(data).map_err(|source| PromptError::Data {
        name: name.clone(),
        source,
    })?;
    let rendered = registry
        .render(&name, &data)
        .map_err(|source| PromptError::Render { name, source })?;
    Ok(render_text_parts(&rendered))
}

/// Render a dotprompt-like prompt into role-scoped messages.
pub fn render_messages<T>(name: &str, data: &T) -> Result<Vec<PromptMessage>, PromptError>
where
    T: Serialize + ?Sized,
{
    let prompt_root = prompt_root();
    let mut registry = registry_with_role(RoleRenderMode::Marker);
    register_prompt_tree(&mut registry, &prompt_root)?;
    let name = prompt_name(name);
    let data = serde_json::to_value(data).map_err(|source| PromptError::Data {
        name: name.clone(),
        source,
    })?;
    let rendered = registry
        .render(&name, &data)
        .map_err(|source| PromptError::Render { name, source })?;
    Ok(parse_role_messages(&rendered))
}

fn register_prompt_tree(
    registry: &mut Handlebars<'static>,
    prompt_root: &Path,
) -> Result<(), PromptError> {
    let entries = prompt_entries(prompt_root)?;
    for path in &entries {
        let source = fs::read_to_string(path).map_err(|source| PromptError::Io {
            action: "read",
            path: path.clone(),
            source,
        })?;
        let source = strip_front_matter(&source).to_owned();
        let name = prompt_entry_name(prompt_root, path);
        registry
            .register_template_string(&name, source.clone())
            .map_err(|source| PromptError::Register {
                name: name.clone(),
                source,
            })?;
        if let Some(partial) = partial_name(path) {
            registry
                .register_template_string(&partial, source)
                .map_err(|source| PromptError::Register {
                    name: partial,
                    source,
                })?;
        }
    }
    Ok(())
}

fn strip_front_matter(source: &str) -> &str {
    split_front_matter(source).1
}

fn split_front_matter(source: &str) -> (PromptMetadata, &str) {
    let Some(opening_len) = front_matter_opening_len(source) else {
        return (PromptMetadata::new(), source);
    };
    let mut cursor = opening_len;
    for line in source[opening_len..].split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == "---" {
            let metadata = parse_front_matter_metadata(&source[opening_len..cursor]);
            let body_start = cursor + line.len();
            return (metadata, &source[body_start..]);
        }
        cursor += line.len();
    }
    (PromptMetadata::new(), source)
}

fn front_matter_opening_len(source: &str) -> Option<usize> {
    if source.starts_with("---\n") {
        Some("---\n".len())
    } else if source.starts_with("---\r\n") {
        Some("---\r\n".len())
    } else {
        None
    }
}

fn parse_front_matter_metadata(source: &str) -> PromptMetadata {
    let mut metadata = PromptMetadata::new();
    for line in source.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        metadata.insert(key.to_owned(), unquote_yaml_scalar(value.trim()).to_owned());
    }
    metadata
}

fn unquote_yaml_scalar(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
}

fn prompt_entries(prompt_root: &Path) -> Result<Vec<PathBuf>, PromptError> {
    let mut out = Vec::new();
    collect_prompt_entries(prompt_root, prompt_root, &mut out)?;
    out.sort();
    Ok(out)
}

fn collect_prompt_entries(
    prompt_root: &Path,
    dir: &Path,
    out: &mut Vec<PathBuf>,
) -> Result<(), PromptError> {
    for entry in fs::read_dir(dir).map_err(|source| PromptError::Io {
        action: "read directory",
        path: dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| PromptError::Io {
            action: "read directory entry",
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_prompt_entries(prompt_root, &path, out)?;
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) == Some("prompt") {
            out.push(path);
        }
    }
    let _ = prompt_root;
    Ok(())
}

fn prompt_entry_name(prompt_root: &Path, path: &Path) -> String {
    path.strip_prefix(prompt_root)
        .unwrap_or(path)
        .with_extension("")
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn partial_name(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_string_lossy();
    stem.strip_prefix('_').map(str::to_owned)
}

fn prompt_filename(name: &str) -> PathBuf {
    let trimmed = name.trim();
    if trimmed.ends_with(".prompt") {
        return PathBuf::from(trimmed);
    }
    PathBuf::from(format!("{trimmed}.prompt"))
}

fn prompt_name(name: &str) -> String {
    name.trim()
        .strip_suffix(".prompt")
        .unwrap_or_else(|| name.trim())
        .to_owned()
}

fn default_prompt_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../prompts")
}

struct IfEqualsHelper;

impl HelperDef for IfEqualsHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        helper: &Helper<'rc>,
        registry: &'reg Handlebars<'reg>,
        context: &'rc Context,
        render_context: &mut RenderContext<'reg, 'rc>,
        output: &mut dyn Output,
    ) -> HelperResult {
        let left = helper
            .param(0)
            .ok_or(RenderErrorReason::ParamNotFoundForIndex("ifEquals", 0))?
            .value();
        let right = helper
            .param(1)
            .ok_or(RenderErrorReason::ParamNotFoundForIndex("ifEquals", 1))?
            .value();
        let template = if json_values_equal(left, right) {
            helper.template()
        } else {
            helper.inverse()
        };
        match template {
            Some(template) => template.render(registry, context, render_context, output),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Copy)]
enum RoleRenderMode {
    Blank,
    Marker,
}

struct RoleHelper {
    mode: RoleRenderMode,
}

impl HelperDef for RoleHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        helper: &Helper<'rc>,
        _registry: &'reg Handlebars<'reg>,
        _context: &'rc Context,
        _render_context: &mut RenderContext<'reg, 'rc>,
        output: &mut dyn Output,
    ) -> HelperResult {
        let role = helper
            .param(0)
            .ok_or(RenderErrorReason::ParamNotFoundForIndex("role", 0))?
            .value();
        let role = json_scalar_text(role);
        let role = role.trim();
        if role.is_empty() {
            return Ok(());
        }
        if matches!(self.mode, RoleRenderMode::Marker) {
            output.write("\n")?;
            output.write(ROLE_MARKER_PREFIX)?;
            output.write(role)?;
            output.write(ROLE_MARKER_SUFFIX)?;
            output.write("\n")?;
        }
        Ok(())
    }
}

fn json_values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::String(left), Value::String(right)) => left == right,
        (Value::String(left), other) => left == &json_scalar_text(other),
        (other, Value::String(right)) => &json_scalar_text(other) == right,
        _ => left == right,
    }
}

fn json_scalar_text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn parse_role_messages(rendered: &str) -> Vec<PromptMessage> {
    let mut messages = Vec::new();
    let mut current_role = String::new();
    let mut current_content = String::new();

    for line in rendered.lines() {
        if let Some(role) = role_marker(line.trim()) {
            push_prompt_message(&mut messages, &current_role, &current_content);
            current_role = role.to_owned();
            current_content.clear();
            continue;
        }
        current_content.push_str(line);
        current_content.push('\n');
    }
    push_prompt_message(&mut messages, &current_role, &current_content);
    messages
}

fn render_text_parts(rendered: &str) -> String {
    if !rendered.contains(ROLE_MARKER_PREFIX) {
        return rendered.to_owned();
    }
    let messages = parse_role_messages(rendered);
    if messages.is_empty() {
        return rendered.to_owned();
    }
    let mut out = String::new();
    for message in messages {
        out.push_str(&message.content);
    }
    out
}

fn role_marker(line: &str) -> Option<&str> {
    line.strip_prefix(ROLE_MARKER_PREFIX)?
        .strip_suffix(ROLE_MARKER_SUFFIX)
        .map(str::trim)
        .filter(|role| !role.is_empty())
}

fn push_prompt_message(messages: &mut Vec<PromptMessage>, role: &str, content: &str) {
    let role = role.trim();
    let content = content.trim();
    if role.is_empty() || content.is_empty() {
        return;
    }
    messages.push(PromptMessage {
        role: role.to_owned(),
        content: content.to_owned(),
    });
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        load_registry, prompt_entries, prompt_metadata, prompt_root, read, render, render_messages,
    };

    #[test]
    fn reads_prompt_with_or_without_suffix() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(read("aifarm/system")?, read("aifarm/system.prompt")?);
        Ok(())
    }

    #[test]
    fn render_supports_partials_raw_values_and_if_equals() -> Result<(), Box<dyn std::error::Error>>
    {
        let rendered = render(
            "aifarm/system",
            &serde_json::json!({
                "toolMode": "native",
                "hasTools": true,
                "guestMode": true,
                "toolCatalog": "<tools><tool name=\"draw_image\"></tool></tools>",
                "locale": "ru",
            }),
        )?;

        assert!(rendered.contains("<system_contract>"));
        assert!(rendered.contains("OpenAI tools доступны"));
        assert!(rendered.contains("<tools><tool name=\"draw_image\"></tool></tools>"));
        assert!(rendered.contains("<guest_mode>"));
        assert!(rendered.contains("<locale_policy>"));
        Ok(())
    }

    #[test]
    fn render_messages_preserves_dotprompt_roles() -> Result<(), Box<dyn std::error::Error>> {
        let messages = render_messages(
            "music/song_reprompt",
            &serde_json::json!({
                "topic": "ночной город",
                "vocalLanguage": "ru",
            }),
        )?;

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "system");
        assert!(
            messages[0]
                .content
                .contains("ACE-Step music prompt engineer")
        );
        assert_eq!(messages[1].role, "user");
        assert!(messages[1].content.contains("Topic: ночной город"));
        assert!(messages[1].content.contains("Vocal language: ru"));
        Ok(())
    }

    #[test]
    fn render_strips_dotprompt_front_matter() -> Result<(), Box<dyn std::error::Error>> {
        let rendered = render("youtube/summary", &serde_json::json!({}))?;

        assert!(!rendered.starts_with("---"));
        assert!(!rendered.contains("model: googleai/gemini-2.5-flash-lite"));
        assert!(rendered.starts_with("Ты AI-ассистент"));
        Ok(())
    }

    #[test]
    fn prompt_metadata_exposes_front_matter_model() -> Result<(), Box<dyn std::error::Error>> {
        let metadata = prompt_metadata("youtube/summary")?;

        assert_eq!(
            metadata.get("model").map(String::as_str),
            Some("googleai/gemini-2.5-flash-lite")
        );
        Ok(())
    }

    #[test]
    fn registry_loads_relative_prompt_names() -> Result<(), Box<dyn std::error::Error>> {
        let registry = load_registry(Path::new(&prompt_root()))?;
        assert!(registry.has_template("aifarm/system"));
        assert!(registry.has_template("shared_core"));
        Ok(())
    }
}
