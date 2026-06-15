//! Image, vision, music, and Telegram media integrations.

pub mod acestep;
pub mod pruna;
pub mod uploader;

use std::sync::LazyLock;

use regex::{Captures, Regex};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "media";

static IMAGE_PROMPT_ASPECT_RE: LazyLock<Regex> =
    LazyLock::new(|| compile_regex(r"(?i)(\d{1,2}:\d{1,2})"));
static IMAGE_PROMPT_SEED_RE: LazyLock<Regex> =
    LazyLock::new(|| compile_regex(r"(?i)seed[:\s]*(\d+)"));
static REPLACE_WORD_RE: LazyLock<Regex> =
    LazyLock::new(|| compile_regex(r"(^|\s|[.,:;!?()])([\p{L}]+)($|\s|[.,:;!?()])"));

pub const OPTIMIZE_PROMPT_TERMINATOR_TOOL_NAME: &str = "optimize_prompt_terminator";
pub const OPTIMIZE_EDIT_PROMPT_TERMINATOR_TOOL_NAME: &str = "optimize_edit_prompt_terminator";
pub const IMAGE_OPTIMIZER_PROMPT_NAME: &str = "image/optimizer";
pub const IMAGE_EDIT_OPTIMIZER_PROMPT_NAME: &str = "image/edit_optimizer";

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImageOptimize {
    /// Original optimizer input.
    pub input: String,
    /// Optimized prompt variants.
    pub outputs: Vec<String>,
    /// Optional aspect ratio.
    pub aspect_ratio: String,
    /// NSFW classifier result.
    pub nsfw_result: NsfwResult,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImageEditOptimize {
    /// Original optimizer input.
    pub input: String,
    /// Optimized edit prompt variants.
    pub outputs: Vec<String>,
    /// NSFW classifier result.
    pub nsfw_result: NsfwResult,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OptimizePromptOptions {
    /// Requested prompt variant count.
    pub variant_count: usize,
}

/// Parsed image prompt modifiers.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImagePromptParts {
    /// Core prompt with modifiers removed.
    pub prompt: String,
    /// Negative prompt after `|`, with modifiers removed.
    pub negative_prompt: String,
    /// First `N:M` aspect ratio found in prompt or negative prompt.
    pub aspect_ratio: String,
    /// First `seed` value found in prompt or negative prompt.
    pub seed: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NsfwResult {
    /// Safe image prompt.
    Safe,
    /// Adult image prompt.
    #[default]
    Adult,
    /// Forbidden image prompt.
    Forbidden,
}

impl Serialize for NsfwResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for NsfwResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(normalize_nsfw_result(&raw))
    }
}

impl NsfwResult {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Adult => "adult",
            Self::Forbidden => "forbidden",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizerTerminatorDefinition {
    /// Tool name.
    pub name: &'static str,
    /// Tool description.
    pub description: &'static str,
    /// JSON-schema-like input schema.
    pub input_schema: Value,
}

#[must_use]
pub fn part_image_prompt(text: &str) -> ImagePromptParts {
    let mut prompt = text.to_owned();
    let mut negative_prompt = String::new();
    let mut aspect_ratio = String::new();
    let mut seed = String::new();

    if let Some(pipe) = prompt.find('|') {
        negative_prompt = prompt[pipe + 1..].to_owned();
        if let Some(next_pipe) = negative_prompt.find('|') {
            negative_prompt.truncate(next_pipe);
        }
        prompt.truncate(pipe);
        prompt = prompt.trim().to_owned();
        negative_prompt = negative_prompt.trim().to_owned();
    }

    if let Some(matched) = IMAGE_PROMPT_ASPECT_RE.find(&prompt) {
        aspect_ratio = matched.as_str().to_owned();
        prompt = remove_match(&prompt, matched.start(), matched.end());
    } else if let Some(matched) = IMAGE_PROMPT_ASPECT_RE.find(&negative_prompt) {
        aspect_ratio = matched.as_str().to_owned();
        negative_prompt = remove_match(&negative_prompt, matched.start(), matched.end());
    }

    if let Some(caps) = IMAGE_PROMPT_SEED_RE.captures(&prompt) {
        if let (Some(full), Some(value)) = (caps.get(0), caps.get(1)) {
            seed = value.as_str().to_owned();
            prompt = remove_match(&prompt, full.start(), full.end());
        }
    } else if let Some(caps) = IMAGE_PROMPT_SEED_RE.captures(&negative_prompt)
        && let (Some(full), Some(value)) = (caps.get(0), caps.get(1))
    {
        seed = value.as_str().to_owned();
        negative_prompt = remove_match(&negative_prompt, full.start(), full.end());
    }

    ImagePromptParts {
        prompt: prompt.trim().to_owned(),
        negative_prompt: negative_prompt.trim().to_owned(),
        aspect_ratio,
        seed,
    }
}

#[must_use]
pub fn apply_word_replacements(mut optimize: ImageOptimize) -> ImageOptimize {
    for output in &mut optimize.outputs {
        *output = REPLACE_WORD_RE
            .replace_all(output, replace_image_word)
            .into_owned();
    }
    optimize
}

#[must_use]
pub fn normalize_nsfw_result(raw: &str) -> NsfwResult {
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case(NsfwResult::Safe.as_str()) {
        return NsfwResult::Safe;
    }
    if raw == "1" || raw.eq_ignore_ascii_case(NsfwResult::Adult.as_str()) {
        return NsfwResult::Adult;
    }
    if raw.eq_ignore_ascii_case(NsfwResult::Forbidden.as_str()) {
        return NsfwResult::Forbidden;
    }
    NsfwResult::Adult
}

#[must_use]
pub fn draw_nsfw_decision(raw: &str) -> (NsfwResult, bool, bool) {
    let result = normalize_nsfw_result(raw);
    (
        result,
        result == NsfwResult::Forbidden,
        result == NsfwResult::Adult,
    )
}

#[must_use]
pub const fn normalize_variant_count(count: usize) -> usize {
    if count < 1 { 1 } else { count }
}

pub fn render_image_optimizer_prompt(
    variant_count: usize,
) -> Result<String, openplotva_prompts::PromptError> {
    openplotva_prompts::render(
        IMAGE_OPTIMIZER_PROMPT_NAME,
        &json!({ "variant_count": normalize_variant_count(variant_count) }),
    )
}

pub fn render_image_edit_optimizer_prompt(
    variant_count: usize,
) -> Result<String, openplotva_prompts::PromptError> {
    openplotva_prompts::render(
        IMAGE_EDIT_OPTIMIZER_PROMPT_NAME,
        &json!({ "variant_count": normalize_variant_count(variant_count) }),
    )
}

#[must_use]
pub fn optimize_prompt_terminator_definition(
    variant_count: usize,
) -> OptimizerTerminatorDefinition {
    optimizer_terminator_definition(
        variant_count,
        OPTIMIZE_PROMPT_TERMINATOR_TOOL_NAME,
        "Finalize image prompt optimization and return the structured payload.",
        true,
    )
}

#[must_use]
pub fn optimize_edit_prompt_terminator_definition(
    variant_count: usize,
) -> OptimizerTerminatorDefinition {
    optimizer_terminator_definition(
        variant_count,
        OPTIMIZE_EDIT_PROMPT_TERMINATOR_TOOL_NAME,
        "Finalize Kontext edit prompt optimization and return the structured payload.",
        false,
    )
}

pub fn normalize_outputs<I, S>(outputs: I, fallback: &str, variant_count: usize) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let required = normalize_variant_count(variant_count);
    let mut normalized = Vec::new();
    for output in outputs {
        let trimmed = output.as_ref().trim();
        if !trimmed.is_empty() {
            normalized.push(trimmed.to_owned());
        }
    }
    let fallback = fallback.trim();
    if normalized.is_empty() {
        normalized.push(fallback.to_owned());
    }
    let mut last = normalized
        .last()
        .map_or_else(String::new, |value| value.trim().to_owned());
    if last.is_empty() {
        last = fallback.to_owned();
    }
    while normalized.len() < required {
        normalized.push(last.clone());
    }
    if normalized.len() > required {
        normalized.truncate(required);
    }
    normalized
}

#[must_use]
pub fn image_optimize_fallback(text: &str, variant_count: usize) -> ImageOptimize {
    ImageOptimize {
        input: text.trim().to_owned(),
        outputs: normalize_outputs(std::iter::empty::<String>(), text, variant_count),
        nsfw_result: NsfwResult::Adult,
        ..ImageOptimize::default()
    }
}

#[must_use]
pub fn image_edit_optimize_fallback(text: &str, variant_count: usize) -> ImageEditOptimize {
    ImageEditOptimize {
        input: text.trim().to_owned(),
        outputs: normalize_outputs(std::iter::empty::<String>(), text, variant_count),
        nsfw_result: NsfwResult::Adult,
    }
}

#[must_use]
pub fn normalize_image_optimize(
    mut optimized: ImageOptimize,
    fallback: &str,
    variant_count: usize,
) -> ImageOptimize {
    optimized.input = optimized.input.trim().to_owned();
    optimized.outputs = normalize_outputs(optimized.outputs, fallback, variant_count);
    optimized.aspect_ratio = optimized.aspect_ratio.trim().to_owned();
    if optimized.input.is_empty() {
        optimized.input = fallback.trim().to_owned();
    }
    optimized
}

#[must_use]
pub fn normalize_image_edit_optimize(
    mut optimized: ImageEditOptimize,
    fallback: &str,
    variant_count: usize,
) -> ImageEditOptimize {
    optimized.input = optimized.input.trim().to_owned();
    optimized.outputs = normalize_outputs(optimized.outputs, fallback, variant_count);
    if optimized.input.is_empty() {
        optimized.input = fallback.trim().to_owned();
    }
    optimized
}

/// Decode and normalize an AIFarm/Gemini image optimizer payload.
pub fn decode_image_optimize_payload(
    payload: &str,
    fallback: &str,
    variant_count: usize,
) -> Result<ImageOptimize, serde_json::Error> {
    serde_json::from_str(&unwrap_json_like_content(payload))
        .map(|optimized| normalize_image_optimize(optimized, fallback, variant_count))
}

/// Decode and normalize an AIFarm/Gemini image-edit optimizer payload.
pub fn decode_image_edit_optimize_payload(
    payload: &str,
    fallback: &str,
    variant_count: usize,
) -> Result<ImageEditOptimize, serde_json::Error> {
    serde_json::from_str(&unwrap_json_like_content(payload))
        .map(|optimized| normalize_image_edit_optimize(optimized, fallback, variant_count))
}

#[must_use]
pub fn unwrap_json_like_content(content: &str) -> String {
    let mut trimmed = content.trim().to_owned();
    if trimmed.is_empty() {
        return content.to_owned();
    }
    if let Some(stripped) = strip_markdown_fence(&trimmed) {
        trimmed = stripped.trim().to_owned();
    }
    if let Some(stripped) = unwrap_json_from_xml_envelope(&trimmed) {
        trimmed = stripped.trim().to_owned();
    }
    trimmed
}

fn strip_markdown_fence(value: &str) -> Option<String> {
    let rest = value.strip_prefix("```")?;
    let rest = rest.find('\n').map_or(rest, |newline| &rest[newline + 1..]);
    let rest = rest.rfind("```").map_or(rest, |close| &rest[..close]);
    Some(rest.to_owned())
}

fn unwrap_json_from_xml_envelope(value: &str) -> Option<String> {
    if !value.starts_with('<') {
        return None;
    }
    let open = value.find('{')?;
    let close = value.rfind('}')?;
    (close > open).then(|| value[open..=close].to_owned())
}

#[must_use]
pub fn song_tool_topic(input_topic: &str, message_text: &str) -> String {
    let topic = input_topic.trim();
    if topic.is_empty() {
        message_text.trim().to_owned()
    } else {
        topic.to_owned()
    }
}

#[must_use]
pub fn vision_describe_error_retryable(message: &str, is_cancelled: bool) -> bool {
    if is_cancelled {
        return false;
    }
    let message = message.trim();
    !contains_ascii_case_insensitive(message, "not found")
        && !contains_ascii_case_insensitive(message, "empty")
}

fn remove_match(text: &str, start: usize, end: usize) -> String {
    let mut result = String::with_capacity(text.len().saturating_sub(end - start));
    result.push_str(&text[..start]);
    result.push_str(&text[end..]);
    result
}

fn replace_image_word(caps: &Captures<'_>) -> String {
    let full = caps.get(0).map_or("", |m| m.as_str());
    let prefix = caps.get(1).map_or("", |m| m.as_str());
    let word = caps.get(2).map_or("", |m| m.as_str());
    let suffix = caps.get(3).map_or("", |m| m.as_str());
    if let Some(replacement) = image_replacement_word(word) {
        format!("{prefix}{replacement}{suffix}")
    } else {
        full.to_owned()
    }
}

fn optimizer_terminator_definition(
    variant_count: usize,
    name: &'static str,
    description: &'static str,
    include_aspect_ratio: bool,
) -> OptimizerTerminatorDefinition {
    let count = normalize_variant_count(variant_count);
    let mut properties = serde_json::Map::from_iter([
        (
            "input".to_owned(),
            json!({
                "type": "string",
            }),
        ),
        (
            "outputs".to_owned(),
            json!({
                "type": "array",
                "minItems": count,
                "maxItems": count,
                "items": {
                    "type": "string",
                },
            }),
        ),
        (
            "nsfw_result".to_owned(),
            json!({
                "type": "string",
                "enum": ["safe", "adult", "forbidden"],
                "description": "Image safety classification. Use forbidden for minor sexual content only: CSAM, sexual content involving minors, sexualized minors, underage nudity, or facilitation of that content. Use adult for adult-only sexual content, gore, violence, horror, war, generic unsafe themes, or ambiguous non-CSAM content.",
            }),
        ),
    ]);
    if include_aspect_ratio {
        properties.insert(
            "aspect_ratio".to_owned(),
            json!({
                "type": "string",
            }),
        );
    }

    OptimizerTerminatorDefinition {
        name,
        description,
        input_schema: json!({
            "type": "object",
            "properties": properties,
            "required": ["input", "outputs", "nsfw_result"],
        }),
    }
}

fn image_replacement_word(word: &str) -> Option<&'static str> {
    if word.eq_ignore_ascii_case("roach")
        || word.eq_ignore_ascii_case("plotva")
        || word.eq_ignore_ascii_case("plotka")
    {
        Some("roach-fish")
    } else {
        None
    }
}

fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
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
        .any(|window| ascii_eq_ignore_case(window, needle))
}

fn ascii_eq_ignore_case(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

fn compile_regex(pattern: &str) -> Regex {
    match Regex::new(pattern) {
        Ok(regex) => regex,
        Err(err) => panic!("invalid OpenPlotva media regex {pattern:?}: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_image_prompt_extracts_modifiers() {
        let parts = part_image_prompt("cat portrait seed:123 16:9 | blurry lowres");

        assert_eq!(
            parts,
            ImagePromptParts {
                prompt: "cat portrait".to_owned(),
                negative_prompt: "blurry lowres".to_owned(),
                aspect_ratio: "16:9".to_owned(),
                seed: "123".to_owned(),
            }
        );
    }

    #[test]
    fn part_image_prompt_extracts_modifiers_from_negative_prompt() {
        let parts = part_image_prompt("cat portrait | blurry seed 77 lowres 9:16 | ignored");

        assert_eq!(parts.prompt, "cat portrait");
        assert_eq!(parts.negative_prompt, "blurry  lowres");
        assert_eq!(parts.aspect_ratio, "9:16");
        assert_eq!(parts.seed, "77");
    }

    #[test]
    fn apply_word_replacements_keeps_go_word_boundary_behavior() {
        let got = apply_word_replacements(ImageOptimize {
            outputs: vec![
                "plotva swims near a roach, but cockroach stays".to_owned(),
                "(Plotka) and plotva!".to_owned(),
            ],
            ..ImageOptimize::default()
        });

        assert_eq!(
            got.outputs,
            vec![
                "roach-fish swims near a roach-fish, but cockroach stays",
                "(roach-fish) and plotva!",
            ]
        );
    }

    #[test]
    fn normalize_nsfw_result_and_draw_decision_match_go_defaults() {
        assert_eq!(normalize_nsfw_result(" SAFE "), NsfwResult::Safe);
        assert_eq!(normalize_nsfw_result("1"), NsfwResult::Adult);
        assert_eq!(normalize_nsfw_result("FORBIDDEN"), NsfwResult::Forbidden);
        assert_eq!(normalize_nsfw_result(""), NsfwResult::Adult);
        assert_eq!(draw_nsfw_decision("safe"), (NsfwResult::Safe, false, false));
        assert_eq!(
            draw_nsfw_decision("adult"),
            (NsfwResult::Adult, false, true)
        );
        assert_eq!(
            draw_nsfw_decision("forbidden"),
            (NsfwResult::Forbidden, true, false)
        );
    }

    #[test]
    fn normalize_outputs_trims_fills_and_truncates_like_go() {
        assert_eq!(
            normalize_outputs(["", " first ", " second ", "third"], "fallback", 2),
            vec!["first", "second"]
        );
        assert_eq!(
            normalize_outputs([""], " fallback ", 3),
            vec!["fallback", "fallback", "fallback"]
        );
        assert_eq!(normalize_variant_count(0), 1);
    }

    #[test]
    fn optimizer_prompt_rendering_and_tool_schemas_match_go_contract() {
        let prompt = render_image_optimizer_prompt(2).expect("render image optimizer prompt");
        assert!(prompt.contains("optimize_prompt_terminator"));
        assert!(prompt.contains("must contain exactly `2` optimized prompts"));
        assert!(prompt.contains("Closed-gate decision tree"));
        assert!(prompt.contains("Both gates must be present for `forbidden`"));
        assert!(prompt.contains("adult-only nudity"));
        assert!(prompt.contains("non-sexual children"));
        assert!(prompt.contains("minor sexual content"));

        let edit_prompt =
            render_image_edit_optimizer_prompt(0).expect("render image edit optimizer prompt");
        assert!(edit_prompt.contains("optimize_edit_prompt_terminator"));
        assert!(edit_prompt.contains("must contain exactly `1` final edit instructions"));
        assert!(edit_prompt.contains("Closed-gate decision tree"));
        assert!(edit_prompt.contains("Both gates must be present for `forbidden`"));
        assert!(edit_prompt.contains("adult-only nudity"));
        assert!(edit_prompt.contains("non-sexual children"));
        assert!(edit_prompt.contains("minor sexual content"));

        let tool = optimize_prompt_terminator_definition(2);
        assert_eq!(tool.name, OPTIMIZE_PROMPT_TERMINATOR_TOOL_NAME);
        assert_eq!(tool.input_schema["type"], "object");
        assert_eq!(tool.input_schema["properties"]["outputs"]["minItems"], 2);
        assert_eq!(tool.input_schema["properties"]["outputs"]["maxItems"], 2);
        assert_eq!(
            tool.input_schema["required"],
            json!(["input", "outputs", "nsfw_result"])
        );
        assert_eq!(
            tool.input_schema["properties"]["aspect_ratio"]["type"],
            "string"
        );
        assert!(
            tool.input_schema["properties"]["nsfw_result"]["description"]
                .as_str()
                .is_some_and(|description| description.contains("minor sexual content only"))
        );

        let edit_tool = optimize_edit_prompt_terminator_definition(0);
        assert_eq!(edit_tool.name, OPTIMIZE_EDIT_PROMPT_TERMINATOR_TOOL_NAME);
        assert_eq!(
            edit_tool.input_schema["properties"]["outputs"]["minItems"],
            1
        );
        assert!(edit_tool.input_schema["properties"]["aspect_ratio"].is_null());
        assert!(
            edit_tool.input_schema["properties"]["nsfw_result"]["description"]
                .as_str()
                .is_some_and(|description| description.contains("minor sexual content only"))
        );
    }

    #[test]
    fn optimizer_payloads_decode_normalize_and_fallback_like_go() {
        let got = decode_image_optimize_payload(
            r#"{
                "input": "  ",
                "outputs": [" plotva by river ", "", "ignored"],
                "aspect_ratio": " 16:9 ",
                "nsfw_result": "SAFE"
            }"#,
            " fallback prompt ",
            2,
        )
        .expect("decode image optimizer");

        assert_eq!(got.input, "fallback prompt");
        assert_eq!(got.outputs, vec!["plotva by river", "ignored"]);
        assert_eq!(got.aspect_ratio, "16:9");
        assert_eq!(got.nsfw_result, NsfwResult::Safe);

        let edit = decode_image_edit_optimize_payload(
            r#"<assistant_message><text>{
                "input": "",
                "outputs": [" make it day "],
                "nsfw_result": "weird"
            }</text></assistant_message>"#,
            " fallback edit ",
            2,
        )
        .expect("decode edit optimizer");

        assert_eq!(edit.input, "fallback edit");
        assert_eq!(edit.outputs, vec!["make it day", "make it day"]);
        assert_eq!(edit.nsfw_result, NsfwResult::Adult);

        assert_eq!(
            image_optimize_fallback(" subject ", 2),
            ImageOptimize {
                input: "subject".to_owned(),
                outputs: vec!["subject".to_owned(), "subject".to_owned()],
                nsfw_result: NsfwResult::Adult,
                ..ImageOptimize::default()
            }
        );
        assert_eq!(
            image_edit_optimize_fallback(" edit ", 1),
            ImageEditOptimize {
                input: "edit".to_owned(),
                outputs: vec!["edit".to_owned()],
                nsfw_result: NsfwResult::Adult,
            }
        );
    }

    #[test]
    fn song_tool_topic_prefers_explicit_topic_then_message_text() {
        assert_eq!(song_tool_topic("  jazz rain  ", "ignored"), "jazz rain");
        assert_eq!(song_tool_topic(" ", " make music "), "make music");
    }

    #[test]
    fn vision_describe_error_retryable_matches_go_fragments() {
        assert!(!vision_describe_error_retryable(
            "Telegram file NOT FOUND",
            false
        ));
        assert!(!vision_describe_error_retryable(
            "empty image payload",
            false
        ));
        assert!(!vision_describe_error_retryable("provider down", true));
        assert!(vision_describe_error_retryable(
            "provider temporarily unavailable",
            false
        ));
    }
}
