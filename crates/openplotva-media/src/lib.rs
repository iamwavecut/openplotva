//! Image, vision, music, and Telegram media integrations.

pub mod acestep;
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

/// Aspect ratios the image optimizer may return, each in both orientations.
pub const IMAGE_ASPECT_RATIOS: [&str; 9] = [
    "1:1", "2:3", "3:2", "3:4", "4:3", "9:16", "16:9", "1:2", "2:1",
];

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
    /// Target aspect ratio; empty when the optimizer omitted it.
    #[serde(default)]
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
    /// Image models the variants are written for, one per slot.
    pub targets: ImageTargets,
}

/// Image model a prompt variant is written for.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ImageModel {
    /// FLUX.2 [klein]: short prompts, subject first, a 512-token text encoder.
    #[default]
    Klein,
    /// Boogu-Image (Turbo and Edit-Turbo): concise, style first.
    Boogu,
    /// Qwen-Image 2.1: long observational descriptions read by a Qwen3-VL encoder.
    QwenImage,
}

impl ImageModel {
    #[must_use]
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Klein => "FLUX.2 [klein]",
            Self::Boogu => "Boogu-Image",
            Self::QwenImage => "Qwen-Image 2.1",
        }
    }

    /// Model named by the `prompt_target` value of a routed provider model.
    #[must_use]
    pub fn from_prompt_target(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "klein" => Some(Self::Klein),
            "boogu" => Some(Self::Boogu),
            "qwen_image" => Some(Self::QwenImage),
            _ => None,
        }
    }

    /// Upper bound for one prompt written for this model, in characters.
    #[must_use]
    pub const fn prompt_max_chars(self) -> usize {
        match self {
            Self::Klein | Self::Boogu => IMAGE_PROMPT_MAX_CHARS,
            Self::QwenImage => QWEN_IMAGE_PROMPT_MAX_CHARS,
        }
    }
}

/// Most output slots one optimizer call is written for.
pub const MAX_IMAGE_SLOTS: usize = 4;

/// Which model each output slot targets, in slot order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImageTargets {
    models: [ImageModel; MAX_IMAGE_SLOTS],
    len: usize,
}

impl Default for ImageTargets {
    fn default() -> Self {
        Self::KLEIN
    }
}

impl ImageTargets {
    pub const KLEIN: Self = Self::single(ImageModel::Klein);
    pub const BOOGU: Self = Self::single(ImageModel::Boogu);
    pub const QWEN_IMAGE: Self = Self::single(ImageModel::QwenImage);

    #[must_use]
    pub const fn single(model: ImageModel) -> Self {
        Self {
            models: [model; MAX_IMAGE_SLOTS],
            len: 1,
        }
    }

    #[must_use]
    pub fn models(&self) -> &[ImageModel] {
        &self.models[..self.len]
    }

    /// Model for output slot `index`; slots past the list reuse the last model.
    #[must_use]
    pub fn model_for_slot(self, index: usize) -> ImageModel {
        self.models[index.min(self.len - 1)]
    }

    /// Length bound for every optimized string: the most any slot's model allows.
    #[must_use]
    pub fn prompt_max_chars(&self) -> usize {
        self.models()
            .iter()
            .map(|model| model.prompt_max_chars())
            .max()
            .unwrap_or(IMAGE_PROMPT_MAX_CHARS)
    }

    /// Targets for two generators rendered side by side: `own_slots` slots
    /// written for these targets, then `next_slots` slots for `next`.
    #[must_use]
    pub fn followed_by(self, own_slots: usize, next: Self, next_slots: usize) -> Self {
        let own_slots = own_slots.clamp(1, MAX_IMAGE_SLOTS);
        let mut out = self;
        for index in 0..own_slots {
            out.models[index] = self.model_for_slot(index);
        }
        out.len = own_slots;
        for index in 0..next_slots {
            if out.len == MAX_IMAGE_SLOTS {
                break;
            }
            out.models[out.len] = next.model_for_slot(index);
            out.len += 1;
        }
        out
    }
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
    options: OptimizePromptOptions,
) -> Result<String, openplotva_prompts::PromptError> {
    openplotva_prompts::render(IMAGE_OPTIMIZER_PROMPT_NAME, &optimizer_prompt_data(options))
}

pub fn render_image_optimizer_prompt_with(
    prompts: &openplotva_prompts::PromptStore,
    options: OptimizePromptOptions,
) -> Result<String, openplotva_prompts::PromptError> {
    prompts.render(IMAGE_OPTIMIZER_PROMPT_NAME, &optimizer_prompt_data(options))
}

pub fn render_image_edit_optimizer_prompt(
    options: OptimizePromptOptions,
) -> Result<String, openplotva_prompts::PromptError> {
    openplotva_prompts::render(
        IMAGE_EDIT_OPTIMIZER_PROMPT_NAME,
        &optimizer_prompt_data(options),
    )
}

pub fn render_image_edit_optimizer_prompt_with(
    prompts: &openplotva_prompts::PromptStore,
    options: OptimizePromptOptions,
) -> Result<String, openplotva_prompts::PromptError> {
    prompts.render(
        IMAGE_EDIT_OPTIMIZER_PROMPT_NAME,
        &optimizer_prompt_data(options),
    )
}

/// Upper bound for one optimized image prompt or edit instruction, in
/// characters. The prompts ask for at most 150 words; the bound only makes a
/// looping model close its string and the JSON instead of running to the token cap.
pub const IMAGE_PROMPT_MAX_CHARS: usize = 1200;

/// Qwen-Image rules ask for up to 300 words of description.
pub const QWEN_IMAGE_PROMPT_MAX_CHARS: usize = 2800;

/// Add a `maxLength` bound to every string of the schema's `outputs` array.
#[must_use]
pub fn with_output_max_chars(mut schema: Value, max_chars: usize) -> Value {
    if let Some(items) = schema
        .pointer_mut("/properties/outputs/items")
        .and_then(Value::as_object_mut)
    {
        items.insert("maxLength".to_owned(), json!(max_chars));
    }
    schema
}

/// Template data for the image prompts: the slot count, which model each slot
/// targets, and flags for the per-model rule blocks.
fn optimizer_prompt_data(options: OptimizePromptOptions) -> Value {
    let variant_count = normalize_variant_count(options.variant_count);
    let models: Vec<ImageModel> = (0..variant_count)
        .map(|index| options.targets.model_for_slot(index))
        .collect();
    let slots: Vec<Value> = models
        .iter()
        .enumerate()
        .map(|(index, model)| json!({ "index": index, "model": model.display_name() }))
        .collect();
    json!({
        "variant_count": variant_count,
        "slots": slots,
        "multi": variant_count > 1,
        "klein": models.contains(&ImageModel::Klein),
        "boogu": models.contains(&ImageModel::Boogu),
        "qwen_image": models.contains(&ImageModel::QwenImage),
    })
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
    let mut required = vec!["input", "outputs", "nsfw_result"];
    if include_aspect_ratio {
        properties.insert(
            "aspect_ratio".to_owned(),
            json!({
                "type": "string",
                "enum": IMAGE_ASPECT_RATIOS,
                "description": "Target aspect ratio for the generated image. Use 1:1 when nothing in the request implies an orientation.",
            }),
        );
        required.push("aspect_ratio");
    }

    OptimizerTerminatorDefinition {
        name,
        description,
        input_schema: json!({
            "type": "object",
            "properties": properties,
            "required": required,
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
    use std::{fs, time::SystemTime};

    use super::*;

    fn prompt_store_with(files: &[(&str, &str)]) -> openplotva_prompts::PromptStore {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "openplotva-media-prompts-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create prompt root");
        for (name, source) in files {
            let path = root.join(name);
            fs::create_dir_all(path.parent().expect("prompt parent"))
                .expect("create prompt directory");
            fs::write(path, source).expect("write prompt");
        }
        let store =
            openplotva_prompts::PromptStore::from_root(&root).expect("compile prompt store");
        fs::remove_dir_all(root).expect("remove prompt root");
        store
    }

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
    fn optimizer_prompt_renders_rules_for_each_slot_target() {
        let pair = render_image_optimizer_prompt(OptimizePromptOptions {
            variant_count: 2,
            targets: ImageTargets::KLEIN.followed_by(1, ImageTargets::BOOGU, 1),
        })
        .expect("render pair");
        assert!(pair.contains("`outputs[0]` is rendered by FLUX.2 [klein]"));
        assert!(pair.contains("`outputs[1]` is rendered by Boogu-Image"));
        assert!(pair.contains("**FLUX.2 [klein]**"));
        assert!(pair.contains("**Boogu-Image**"));
        assert!(pair.contains("All prompts describe the same image idea"));
        assert!(!pair.contains("{{"));

        let boogu = render_image_optimizer_prompt(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::BOOGU,
        })
        .expect("render boogu");
        assert!(boogu.contains("`outputs[0]` is rendered by Boogu-Image"));
        assert!(boogu.contains("**Boogu-Image**"));
        assert!(!boogu.contains("**FLUX.2 [klein]**"));
        assert!(!boogu.contains("All prompts describe the same image idea"));

        let edit = render_image_edit_optimizer_prompt(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::KLEIN,
        })
        .expect("render edit");
        assert!(edit.contains("`outputs[0]` is carried out by FLUX.2 [klein]"));
        assert!(edit.contains("**FLUX.2 [klein]**"));
        assert!(!edit.contains("**Boogu-Image Edit**"));
    }

    #[test]
    fn qwen_image_slots_get_their_own_rules_and_examples() {
        let vip_pair = render_image_optimizer_prompt(OptimizePromptOptions {
            variant_count: 2,
            targets: ImageTargets::QWEN_IMAGE.followed_by(1, ImageTargets::BOOGU, 1),
        })
        .expect("render qwen pair");
        assert!(vip_pair.contains("`outputs[0]` is rendered by Qwen-Image 2.1"));
        assert!(vip_pair.contains("`outputs[1]` is rendered by Boogu-Image"));
        assert!(vip_pair.contains("**Qwen-Image 2.1**"));
        assert!(vip_pair.contains("150 to 300 words"));
        assert!(vip_pair.contains("single Qwen-Image 2.1 slot"));
        assert!(!vip_pair.contains("**FLUX.2 [klein]**"));
        assert!(!vip_pair.contains("{{"));

        let klein =
            render_image_optimizer_prompt(OptimizePromptOptions::default()).expect("render klein");
        assert!(!klein.contains("Qwen-Image"));

        let edit = render_image_edit_optimizer_prompt(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::QWEN_IMAGE,
        })
        .expect("render qwen edit");
        assert!(edit.contains("`outputs[0]` is carried out by Qwen-Image 2.1"));
        assert!(edit.contains("Call the picture being edited <image1>"));
        assert!(edit.contains("<image2>"));
        assert!(!edit.contains("**FLUX.2 [klein]**"));
        assert!(!edit.contains("{{"));
    }

    #[test]
    fn prompt_targets_and_length_bounds_follow_the_model() {
        assert_eq!(
            ImageModel::from_prompt_target(" Qwen_Image "),
            Some(ImageModel::QwenImage)
        );
        assert_eq!(
            ImageModel::from_prompt_target("klein"),
            Some(ImageModel::Klein)
        );
        assert_eq!(
            ImageModel::from_prompt_target("boogu"),
            Some(ImageModel::Boogu)
        );
        assert_eq!(ImageModel::from_prompt_target("dall-e"), None);

        assert_eq!(
            ImageTargets::KLEIN.prompt_max_chars(),
            IMAGE_PROMPT_MAX_CHARS
        );
        assert_eq!(
            ImageTargets::QWEN_IMAGE
                .followed_by(1, ImageTargets::BOOGU, 1)
                .prompt_max_chars(),
            QWEN_IMAGE_PROMPT_MAX_CHARS
        );
    }

    #[test]
    fn output_strings_get_a_length_bound() {
        let schema = with_output_max_chars(
            optimize_prompt_terminator_definition(1).input_schema,
            IMAGE_PROMPT_MAX_CHARS,
        );
        assert_eq!(
            schema["properties"]["outputs"]["items"]["maxLength"],
            IMAGE_PROMPT_MAX_CHARS
        );
        assert_eq!(schema["properties"]["outputs"]["maxItems"], 1);
    }

    #[test]
    fn image_targets_follow_each_side_slot_count() {
        let pair = ImageTargets::KLEIN.followed_by(1, ImageTargets::BOOGU, 1);
        assert_eq!(pair.models(), [ImageModel::Klein, ImageModel::Boogu]);

        let wide_first = ImageTargets::KLEIN.followed_by(2, ImageTargets::BOOGU, 1);
        assert_eq!(
            wide_first.models(),
            [ImageModel::Klein, ImageModel::Klein, ImageModel::Boogu]
        );
        assert_eq!(wide_first.model_for_slot(1), ImageModel::Klein);
        assert_eq!(wide_first.model_for_slot(2), ImageModel::Boogu);

        assert_eq!(ImageTargets::KLEIN.model_for_slot(3), ImageModel::Klein);
        assert_eq!(ImageTargets::default(), ImageTargets::KLEIN);
        assert_eq!(
            ImageTargets::BOOGU
                .followed_by(3, ImageTargets::KLEIN, 3)
                .models()
                .len(),
            MAX_IMAGE_SLOTS
        );
    }

    #[test]
    fn image_prompts_stay_within_the_word_budget() {
        for prompt in [
            include_str!("../../../prompts/image/optimizer.prompt"),
            include_str!("../../../prompts/image/edit_optimizer.prompt"),
        ] {
            assert!(prompt.split_whitespace().count() <= 3_500);
            assert!(!prompt.contains("random visual style"));
            assert!(!prompt.contains("90 to 180 words"));
            assert!(!prompt.contains("Kontext"));
        }
    }

    #[test]
    fn optimizer_prompt_rendering_and_tool_schemas_match_contract() {
        let prompt = render_image_optimizer_prompt(OptimizePromptOptions {
            variant_count: 2,
            ..OptimizePromptOptions::default()
        })
        .expect("render image optimizer prompt");
        assert!(prompt.contains("`outputs` holds exactly `2` prompts"));
        assert!(prompt.contains("### ASPECT RATIO"));
        assert!(prompt.contains("`9:16`, `16:9`, `1:2`, `2:1`"));
        assert!(prompt.contains("Closed-gate decision tree"));
        assert!(prompt.contains("Both gates must be present for `forbidden`"));
        assert!(prompt.contains("adult-only nudity"));
        assert!(prompt.contains("non-sexual children"));
        assert!(prompt.contains("minor sexual content"));

        let edit_prompt = render_image_edit_optimizer_prompt(OptimizePromptOptions::default())
            .expect("render image edit optimizer prompt");
        assert!(edit_prompt.contains("`outputs` holds exactly `1` instructions"));
        assert!(edit_prompt.contains("Preserve the image unchanged"));
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
            json!(["input", "outputs", "nsfw_result", "aspect_ratio"])
        );
        assert_eq!(
            tool.input_schema["properties"]["aspect_ratio"]["type"],
            "string"
        );
        assert_eq!(
            tool.input_schema["properties"]["aspect_ratio"]["enum"],
            json!(IMAGE_ASPECT_RATIOS)
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
        assert_eq!(
            edit_tool.input_schema["required"],
            json!(["input", "outputs", "nsfw_result"])
        );
        assert!(
            edit_tool.input_schema["properties"]["nsfw_result"]["description"]
                .as_str()
                .is_some_and(|description| description.contains("minor sexual content only"))
        );
    }

    #[test]
    fn optimizer_prompt_renderers_use_injected_store() {
        let store = prompt_store_with(&[
            (
                "image/optimizer.prompt",
                "custom image variants={{variant_count}}",
            ),
            (
                "image/edit_optimizer.prompt",
                "custom edit variants={{variant_count}}",
            ),
        ]);

        assert_eq!(
            render_image_optimizer_prompt_with(&store, OptimizePromptOptions::default())
                .expect("render image prompt"),
            "custom image variants=1"
        );
        assert_eq!(
            render_image_edit_optimizer_prompt_with(
                &store,
                OptimizePromptOptions {
                    variant_count: 3,
                    ..OptimizePromptOptions::default()
                }
            )
            .expect("render edit prompt"),
            "custom edit variants=3"
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

        let missing_ratio = decode_image_optimize_payload(
            r#"{
                "input": "subject",
                "outputs": ["prompt"],
                "nsfw_result": "safe"
            }"#,
            "fallback",
            1,
        )
        .expect("decode image optimizer without aspect_ratio");
        assert_eq!(missing_ratio.aspect_ratio, "");

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
