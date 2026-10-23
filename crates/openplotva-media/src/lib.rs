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

/// Side of the square whose area every MOSS-Image canvas keeps, as draw-api sizes it.
const MOSS_CANVAS_SIDE: f64 = 1024.0;
/// MOSS-Image canvas sides are multiples of its latent grid.
const MOSS_CANVAS_GRID: f64 = 16.0;

/// MOSS-Image canvas in pixels for an optimizer aspect ratio such as "3:4": the 1 MP
/// training area on the 16-pixel grid, the same arithmetic as draw-api's `moss_job`.
#[must_use]
pub fn moss_canvas(aspect_ratio: &str) -> Option<(u32, u32)> {
    let (width, height) = aspect_ratio.split_once(':')?;
    let (width, height) = (
        width.trim().parse::<f64>().ok()?,
        height.trim().parse::<f64>().ok()?,
    );
    if width <= 0.0 || height <= 0.0 {
        return None;
    }
    let ratio = (width / height).clamp(0.4, 2.5);
    let width = (MOSS_CANVAS_SIDE * MOSS_CANVAS_SIDE * ratio).sqrt();
    let height = width / ratio;
    let snap = |side: f64| {
        // Sides stay below 2048 px, so the cast cannot truncate.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let snapped = ((side / MOSS_CANVAS_GRID).round() * MOSS_CANVAS_GRID) as u32;
        snapped
    };
    Some((snap(width), snap(height)))
}

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
    /// Request-specific exclusions for the MOSS-Image negative prompt, comma-separated;
    /// empty when the request rules nothing out or no slot targets MOSS-Image.
    #[serde(default, rename = "slot_moss_negative")]
    pub moss_negative: String,
    /// Whether MOSS-Image renders the request from `moss_layout` instead of its prose
    /// prompt: the optimizer's `moss_kind` is one of [`MOSS_LAYOUT_KINDS`].
    #[serde(
        default,
        rename = "moss_kind",
        deserialize_with = "deserialize_moss_kind"
    )]
    pub moss_design: bool,
    /// MOSS-Image layout of a designed image: `canvas_width` and `canvas_height`, `summary`,
    /// and `items` with `[x_min, y_min, x_max, y_max]` pixel boxes, the draft format of the
    /// official prompt enhancer. draw-api compiles it to the model's centred format.
    #[serde(
        default,
        rename = "moss_layout",
        deserialize_with = "deserialize_moss_layout"
    )]
    pub moss_layout: Option<Value>,
    /// Fibo 2.0 caption of the picture: the VGL fields except `short_description`, which is
    /// the Fibo 2.0 slot's prompt. draw-api orders the fields as Fibo 2.0 was trained on.
    #[serde(
        default,
        rename = "fibo_caption",
        deserialize_with = "deserialize_fibo_caption"
    )]
    pub fibo_caption: Option<Value>,
}

/// Keep a Fibo 2.0 caption only when it is a non-empty object, so a malformed one degrades
/// to the prose prompt instead of failing the whole payload.
fn deserialize_fibo_caption<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    Ok(value
        .as_object()
        .is_some_and(|fields| !fields.is_empty())
        .then_some(value))
}

/// Request kinds MOSS-Image draws from a layout, as the official prompt enhancer
/// advises: placement, a poster, several panels, or a controlled arrangement.
pub const MOSS_LAYOUT_KINDS: [&str; 5] = ["poster", "comic", "meme", "infographic", "arrangement"];

/// Kinds of `moss_kind`: the model names the request, and code decides the route,
/// which a model follows better than a boolean rule with exceptions.
const MOSS_KINDS: [&str; 10] = [
    "poster",
    "comic",
    "meme",
    "infographic",
    "arrangement",
    "card",
    "cover",
    "menu",
    "other_design",
    "scene",
];

fn deserialize_moss_kind<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    Ok(value
        .as_str()
        .is_some_and(|kind| MOSS_LAYOUT_KINDS.contains(&kind.trim())))
}

/// Keep a layout only when it is an object with a non-empty `items` array, so a
/// malformed one degrades to the prose prompt instead of failing the whole payload.
fn deserialize_moss_layout<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let has_items = value
        .get("items")
        .and_then(Value::as_array)
        .is_some_and(|items| !items.is_empty());
    Ok((value.is_object() && has_items).then_some(value))
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImageEditOptimize {
    /// Original optimizer input.
    pub input: String,
    /// Optimized edit prompt variants.
    pub outputs: Vec<String>,
    /// NSFW classifier result.
    pub nsfw_result: NsfwResult,
    /// Fibo 2.0 caption of the edited picture, without `edit_instruction`, which is the
    /// slot's prompt.
    #[serde(
        default,
        rename = "fibo_caption",
        deserialize_with = "deserialize_fibo_caption"
    )]
    pub fibo_caption: Option<Value>,
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
    /// Krea 2 Turbo: one natural-prose paragraph, medium and style first.
    #[default]
    Krea2,
    /// Fibo 2.0: a structured VGL caption with a prose summary, or an edit instruction
    /// with the caption of the result, read by a Qwen3-VL encoder.
    Fibo,
    /// MOSS-Image SFT: a detailed caption in the form of its recaptioner, read by a
    /// Qwen3.5 encoder.
    Moss,
}

impl ImageModel {
    #[must_use]
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Krea2 => "Krea 2 Turbo",
            Self::Fibo => "Fibo 2.0",
            Self::Moss => "MOSS-Image",
        }
    }

    /// Model named by the `prompt_target` value of a routed provider model.
    #[must_use]
    pub fn from_prompt_target(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "krea2" => Some(Self::Krea2),
            "fibo" => Some(Self::Fibo),
            "moss" => Some(Self::Moss),
            _ => None,
        }
    }

    /// Upper bound for one prompt written for this model, in characters.
    #[must_use]
    pub const fn prompt_max_chars(self) -> usize {
        match self {
            Self::Krea2 => KREA2_PROMPT_MAX_CHARS,
            Self::Fibo => FIBO_PROMPT_MAX_CHARS,
            Self::Moss => MOSS_PROMPT_MAX_CHARS,
        }
    }
}

/// Most output slots one optimizer call is written for.
pub const MAX_IMAGE_SLOTS: usize = 4;

/// Which model each output slot targets, in slot order, and whether the optimizer
/// also writes a MOSS-Image layout for a designed picture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImageTargets {
    models: [ImageModel; MAX_IMAGE_SLOTS],
    len: usize,
    moss_layout: bool,
}

impl Default for ImageTargets {
    fn default() -> Self {
        Self::KREA2
    }
}

impl ImageTargets {
    pub const KREA2: Self = Self::single(ImageModel::Krea2);
    pub const FIBO: Self = Self::single(ImageModel::Fibo);
    pub const MOSS: Self = Self::single(ImageModel::Moss);

    #[must_use]
    pub const fn single(model: ImageModel) -> Self {
        Self {
            models: [model; MAX_IMAGE_SLOTS],
            len: 1,
            moss_layout: false,
        }
    }

    /// These targets with the MOSS-Image layout route on or off. Off, a MOSS-Image slot
    /// gets its prose prompt and negative prompt only.
    #[must_use]
    pub const fn with_moss_layout(mut self, enabled: bool) -> Self {
        self.moss_layout = enabled;
        self
    }

    /// Whether a MOSS-Image slot also gets a layout for a designed picture.
    #[must_use]
    pub fn moss_layout(&self) -> bool {
        self.moss_layout && self.models().contains(&ImageModel::Moss)
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
            .unwrap_or(KREA2_PROMPT_MAX_CHARS)
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
        out.moss_layout = self.moss_layout || next.moss_layout;
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
        *output = replace_image_words(output);
    }
    optimize.moss_negative = replace_image_words(&optimize.moss_negative);
    if let Some(caption) = optimize.fibo_caption.as_mut() {
        replace_caption_words(caption);
    }
    if let Some(layout) = optimize.moss_layout.as_mut() {
        // Captions describe the picture; `text` is lettering and stays verbatim.
        if let Some(caption) = layout.pointer_mut("/summary/caption") {
            replace_image_words_in(caption);
        }
        if let Some(items) = layout.get_mut("items").and_then(Value::as_array_mut) {
            for item in items {
                if let Some(caption) = item.get_mut("caption") {
                    replace_image_words_in(caption);
                }
            }
        }
    }
    optimize
}

fn replace_image_words(text: &str) -> String {
    REPLACE_WORD_RE
        .replace_all(text, replace_image_word)
        .into_owned()
}

fn replace_image_words_in(value: &mut Value) {
    if let Value::String(text) = value {
        *text = replace_image_words(text);
    }
}

/// Word replacements in every description of a Fibo 2.0 caption; the lettering in
/// `text_render` stays verbatim.
fn replace_caption_words(value: &mut Value) {
    match value {
        Value::String(_) => replace_image_words_in(value),
        Value::Array(items) => items.iter_mut().for_each(replace_caption_words),
        Value::Object(fields) => {
            for (key, field) in fields.iter_mut() {
                if key != "text" {
                    replace_caption_words(field);
                }
            }
        }
        _ => {}
    }
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

/// Line the edit optimizer reads between the attached pictures and the request: the
/// tokens Fibo 2.0 Edit names several pictures by.
pub const EDIT_REFERENCE_ORDER_LINE: &str =
    "The pictures above are <image_1>, <image_2>, ... in order.";

/// Most pictures one edit takes: Fibo 2.0 Edit reads up to four.
pub const MAX_EDIT_REFERENCE_IMAGES: usize = 4;

/// Text of the edit optimizer's user turn: the reference order line when pictures are
/// attached, then the request.
#[must_use]
pub fn image_edit_optimizer_text(request: &str, image_count: usize) -> String {
    if image_count == 0 {
        request.to_owned()
    } else {
        format!("{EDIT_REFERENCE_ORDER_LINE}\n{request}")
    }
}

const UPSAMPLER_REFERENCE_MULTIPLE: u32 = 32;
const UPSAMPLER_MAX_ASPECT_RATIO: u32 = 200;

/// Size the edit optimizer sees one of `count` reference pictures at, the FLUX.3 prompt
/// upsampler's policy: sides on a
/// multiple of 32 and at most 1024², 724², or 512² pixels for one, two, or more pictures.
/// `None` for an empty or overly elongated picture.
#[must_use]
pub fn upsampler_reference_size(width: u32, height: u32, count: usize) -> Option<(u32, u32)> {
    if width == 0 || height == 0 {
        return None;
    }
    let (long, short) = (width.max(height), width.min(height));
    if long > short.saturating_mul(UPSAMPLER_MAX_ASPECT_RATIO) {
        return None;
    }
    let multiple = f64::from(UPSAMPLER_REFERENCE_MULTIPLE);
    let cap = match count {
        0 | 1 => 1024.0 * 1024.0,
        2 => 724.0 * 724.0,
        _ => 512.0 * 512.0,
    };
    let floor = |value: f64| ((value / multiple).floor() * multiple).max(multiple);
    let (width, height) = (f64::from(width), f64::from(height));
    let mut w = ((width / multiple).round_ties_even() * multiple).max(multiple);
    let mut h = ((height / multiple).round_ties_even() * multiple).max(multiple);
    if w * h > cap {
        let beta = (width * height / cap).sqrt();
        w = floor(width / beta);
        h = floor(height / beta);
    }
    if w * h > cap {
        if w == multiple {
            h = floor(cap / w);
        } else {
            w = floor(cap / h);
        }
    }
    Some((w as u32, h as u32))
}

/// Krea 2 rules ask for up to 200 words. Every bound only makes a looping model close its
/// string and the JSON instead of running to the token cap.
pub const KREA2_PROMPT_MAX_CHARS: usize = 1600;

/// A Fibo 2.0 summary has at most 200 words, and an edit instruction is one command.
pub const FIBO_PROMPT_MAX_CHARS: usize = 1600;

/// MOSS-Image rules ask for up to about 350 words.
pub const MOSS_PROMPT_MAX_CHARS: usize = 4000;

/// Cut every optimized output to at most `max_chars` characters. The bound is applied
/// after decoding rather than as a schema `maxLength`: a bounded string makes the
/// structured-output grammar of large-vocabulary engines slow to compile and decode.
pub fn truncate_outputs_to_chars(outputs: &mut [String], max_chars: usize) {
    for output in outputs {
        if let Some((cut, _)) = output.char_indices().nth(max_chars) {
            output.truncate(cut);
            let kept = output.trim_end().len();
            output.truncate(kept);
        }
    }
}

/// Template data for the image prompts: the slot count, which model each slot
/// targets, and flags for the per-model rule blocks.
fn optimizer_prompt_data(options: OptimizePromptOptions) -> Value {
    let variant_count = normalize_variant_count(options.variant_count);
    let models = slot_models(options);
    let slots: Vec<Value> = models
        .iter()
        .enumerate()
        .map(|(index, model)| json!({ "index": index, "model": model.display_name() }))
        .collect();
    json!({
        "variant_count": variant_count,
        "slots": slots,
        "multi": variant_count > 1,
        "krea2": models.contains(&ImageModel::Krea2),
        "fibo": models.contains(&ImageModel::Fibo),
        "moss": models.contains(&ImageModel::Moss),
        "moss_layout": options.targets.moss_layout(),
        "moss_canvases": IMAGE_ASPECT_RATIOS
            .iter()
            .filter_map(|ratio| {
                moss_canvas(ratio)
                    .map(|(width, height)| json!({ "ratio": ratio, "width": width, "height": height }))
            })
            .collect::<Vec<_>>(),
    })
}

/// The model each output slot of one optimizer call is written for.
fn slot_models(options: OptimizePromptOptions) -> Vec<ImageModel> {
    (0..normalize_variant_count(options.variant_count))
        .map(|index| options.targets.model_for_slot(index))
        .collect()
}

/// The image optimizer's payload schema; a call with a MOSS-Image slot also offers
/// that model's negative prompt, and its layout when the targets enable it, and a call
/// with a Fibo 2.0 slot its caption.
#[must_use]
pub fn optimize_prompt_terminator_definition(
    options: OptimizePromptOptions,
) -> OptimizerTerminatorDefinition {
    let models = slot_models(options);
    let moss = if !models.contains(&ImageModel::Moss) {
        MossFields::None
    } else if options.targets.moss_layout() {
        MossFields::NegativeAndLayout
    } else {
        MossFields::Negative
    };
    optimizer_terminator_definition(
        options.variant_count,
        OPTIMIZE_PROMPT_TERMINATOR_TOOL_NAME,
        "Finalize image prompt optimization and return the structured payload.",
        true,
        moss,
        models.contains(&ImageModel::Fibo),
    )
}

/// The MOSS-Image fields an optimizer payload carries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MossFields {
    None,
    Negative,
    NegativeAndLayout,
}

/// The edit optimizer's payload schema; a Fibo 2.0 slot also gets the caption of the
/// edited picture.
#[must_use]
pub fn optimize_edit_prompt_terminator_definition(
    options: OptimizePromptOptions,
) -> OptimizerTerminatorDefinition {
    optimizer_terminator_definition(
        options.variant_count,
        OPTIMIZE_EDIT_PROMPT_TERMINATOR_TOOL_NAME,
        "Finalize image edit prompt optimization and return the structured payload.",
        false,
        MossFields::None,
        slot_models(options).contains(&ImageModel::Fibo),
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

/// Fewest words a real optimized prompt has; shorter slots are placeholders.
const MIN_PROMPT_WORDS: usize = 4;

/// A grammar-constrained model sometimes fills the later slots of a multi-slot call
/// with a field name such as "nsfw_result" or "outputs[1] = " instead of a prompt.
/// Each such slot takes the nearest real prompt, the one before it first, so every
/// slot keeps its position and renders the request.
fn replace_placeholder_outputs(outputs: &mut [String]) {
    let is_prompt = |output: &str| output.split_whitespace().count() >= MIN_PROMPT_WORDS;
    for index in 0..outputs.len() {
        if is_prompt(&outputs[index]) {
            continue;
        }
        let donor = (0..index)
            .rev()
            .chain(index + 1..outputs.len())
            .find(|&other| is_prompt(&outputs[other]));
        if let Some(donor) = donor {
            outputs[index] = outputs[donor].clone();
        }
    }
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
        fibo_caption: None,
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
    replace_placeholder_outputs(&mut optimized.outputs);
    for output in &mut optimized.outputs {
        // A model that escapes twice writes a literal backslash-n; the line break is meant.
        if output.contains("\\n") {
            *output = output.replace("\\n", "\n");
        }
    }
    optimized.aspect_ratio = optimized.aspect_ratio.trim().to_owned();
    optimized.moss_negative = optimized.moss_negative.trim().to_owned();
    if optimized.nsfw_result == NsfwResult::Forbidden {
        optimized.moss_negative.clear();
        optimized.moss_design = false;
        optimized.fibo_caption = None;
    }
    if !optimized.moss_design {
        optimized.moss_layout = None;
    }
    if let Some(items) = optimized
        .moss_layout
        .as_mut()
        .and_then(|layout| layout.get_mut("items"))
        .and_then(Value::as_array_mut)
    {
        // A model used to quoting text in prose also quotes a layout `text`, and
        // MOSS-Image then draws the quotation marks.
        for text in items.iter_mut().filter_map(|item| item.get_mut("text")) {
            if let Some(inner) = text.as_str().and_then(unquoted_lettering) {
                *text = Value::from(inner);
            }
        }
    }
    if optimized.input.is_empty() {
        optimized.input = fallback.trim().to_owned();
    }
    optimized
}

/// The lettering inside a pair of double quotes that wraps all of it; guillemets stay,
/// since Russian lettering uses them.
fn unquoted_lettering(text: &str) -> Option<&str> {
    let text = text.trim();
    [('"', '"'), ('“', '”'), ('„', '“')]
        .iter()
        .find_map(|&(open, close)| text.strip_prefix(open)?.strip_suffix(close))
        .map(str::trim)
        .filter(|inner| !inner.is_empty())
}

#[must_use]
pub fn normalize_image_edit_optimize(
    mut optimized: ImageEditOptimize,
    fallback: &str,
    variant_count: usize,
) -> ImageEditOptimize {
    optimized.input = optimized.input.trim().to_owned();
    optimized.outputs = normalize_outputs(optimized.outputs, fallback, variant_count);
    if optimized.nsfw_result == NsfwResult::Forbidden {
        optimized.fibo_caption = None;
    }
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
    moss: MossFields,
    fibo: bool,
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
    // Required, so the model decides each one: a grammar-constrained model skips
    // optional fields. Schema keys serialize in alphabetical order, which a
    // grammar-constrained model writes in. `moss_kind` and `moss_layout` sort before
    // `outputs`, so the layout is drafted before the prompts, as the official enhancer
    // drafts it: written after them, the MOSS-Image prompt of a three-slot call ran on
    // to the token limit in 5 of 25 calls (2026-10-21). `slot_moss_negative` stays after.
    if moss == MossFields::NegativeAndLayout {
        properties.insert(
            "moss_kind".to_owned(),
            json!({
                "type": "string",
                "enum": MOSS_KINDS,
                "description": "What the request is; scene for a photograph, painting, or illustration, also with a sign in it.",
            }),
        );
        properties.insert(
            "moss_layout".to_owned(),
            json!({
                "anyOf": [moss_layout_schema(), { "type": "null" }],
                "description": "MOSS-Image layout for a poster, comic, meme, infographic, or arrangement, otherwise null.",
            }),
        );
        required.extend(["moss_kind", "moss_layout"]);
    }
    // `fibo_caption` sorts before `outputs`, so the caption is drafted first and the
    // Fibo 2.0 slot's summary follows it.
    if fibo {
        properties.insert("fibo_caption".to_owned(), fibo_caption_schema());
        required.push("fibo_caption");
    }
    if moss != MossFields::None {
        properties.insert(
            "slot_moss_negative".to_owned(),
            json!({
                "type": "string",
                "description": "Up to three short comma-separated things the request rules out for the MOSS-Image picture; empty when it rules nothing out.",
            }),
        );
        required.push("slot_moss_negative");
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

/// Most objects one Fibo 2.0 caption describes; the rest belong to `background_setting`.
const FIBO_CAPTION_MAX_OBJECTS: usize = 5;

/// Fibo 2.0 caption in the VGL fields of its training captions, without the
/// `short_description` and `edit_instruction` that the slot's prompt carries. The fields
/// a VGL caption may omit are optional; a person's fields are optional on every object.
fn fibo_caption_schema() -> Value {
    let text = json!({ "type": "string" });
    let section = |fields: &[&str]| {
        json!({
            "type": "object",
            "properties": fields.iter().map(|field| ((*field).to_owned(), text.clone())).collect::<serde_json::Map<_, _>>(),
            "required": fields,
        })
    };
    let mut object_fields: serde_json::Map<String, Value> = [
        "description",
        "location",
        "relationship",
        "relative_size",
        "shape_and_color",
        "texture",
        "appearance_details",
        "orientation",
        "pose",
        "expression",
        "clothing",
        "action",
    ]
    .iter()
    .map(|field| ((*field).to_owned(), text.clone()))
    .collect();
    object_fields.insert(
        "number_of_objects".to_owned(),
        json!({ "type": "integer", "minimum": 1 }),
    );
    json!({
        "type": "object",
        "description": "Fibo 2.0 caption of the picture, in the VGL fields.",
        "properties": {
            "objects": {
                "type": "array",
                "maxItems": FIBO_CAPTION_MAX_OBJECTS,
                "items": {
                    "type": "object",
                    "properties": object_fields,
                    "required": ["description", "location", "relationship", "relative_size", "shape_and_color", "texture"],
                },
            },
            "background_setting": text,
            "lighting": section(&["conditions", "direction", "shadows"]),
            "aesthetics": section(&["composition", "color_scheme", "mood_atmosphere"]),
            "photographic_characteristics": section(&["depth_of_field", "focus", "camera_angle", "lens_focal_length"]),
            "style_medium": text,
            "text_render": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "text": text,
                        "location": text,
                        "size": text,
                        "color": text,
                        "font": text,
                        "appearance_details": text,
                    },
                    "required": ["text", "location", "size", "color", "font"],
                },
            },
            "context": text,
            "artistic_style": text,
        },
        "required": [
            "objects",
            "background_setting",
            "lighting",
            "aesthetics",
            "photographic_characteristics",
            "style_medium",
            "text_render",
            "context",
            "artistic_style",
        ],
    })
}

/// Most items one MOSS-Image layout carries; draw-api compiles at most this many.
const MOSS_LAYOUT_MAX_ITEMS: usize = 16;

/// MOSS-Image layout as the official prompt enhancer drafts it: the canvas in pixels,
/// `summary`, and flat `items` with `[x_min, y_min, x_max, y_max]` pixel boxes from the
/// top-left corner. The prompt lists the canvas of each aspect ratio.
fn moss_layout_schema() -> Value {
    let strings = json!({ "type": "array", "items": { "type": "string" } });
    let side = json!({ "type": "integer", "minimum": 16, "maximum": 2048 });
    let bbox = json!({
        "type": "array",
        "minItems": 4,
        "maxItems": 4,
        "items": { "type": "integer", "minimum": 0 },
    });
    // Two item shapes, so only a text item can carry wording: a model that may put
    // `text` on any item folds a caption into its panel, and the compiler drops it there.
    let text_item = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "label": { "type": "string", "enum": ["text"] },
            "caption": { "type": "string" },
            "text": { "type": "string" },
            "bbox": bbox.clone(),
        },
        "required": ["label", "caption", "text", "bbox"],
    });
    let shape_item = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "label": { "type": "string", "enum": ["object", "character"] },
            "caption": { "type": "string" },
            "bbox": bbox,
        },
        "required": ["label", "caption", "bbox"],
    });
    // `canvas_` names sort before `items`, so the model fixes the canvas before
    // the boxes; a plain `width` would come after them.
    json!({
        "type": "object",
        "properties": {
            "canvas_width": side.clone(),
            "canvas_height": side,
            "summary": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "caption": { "type": "string" },
                    "meta": strings,
                    "canvas": { "type": "string", "enum": ["single", "multi_panel", "collage"] },
                },
                "required": ["caption", "meta", "canvas"],
            },
            "items": {
                "type": "array",
                "minItems": 1,
                "maxItems": MOSS_LAYOUT_MAX_ITEMS,
                "items": { "anyOf": [text_item, shape_item] },
            },
        },
        "required": ["canvas_width", "canvas_height", "summary", "items"],
        "additionalProperties": false,
    })
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
            targets: ImageTargets::KREA2.followed_by(1, ImageTargets::FIBO, 1),
        })
        .expect("render pair");
        assert!(pair.contains("`outputs[0]` is rendered by Krea 2 Turbo"));
        assert!(pair.contains("`outputs[1]` is rendered by Fibo 2.0"));
        assert!(pair.contains("**Krea 2 Turbo**"));
        assert!(pair.contains("**Fibo 2.0**"));
        assert!(pair.contains("All prompts describe the same image idea"));
        assert!(!pair.contains("{{"));

        let fibo = render_image_optimizer_prompt(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::FIBO,
        })
        .expect("render fibo");
        assert!(fibo.contains("`outputs[0]` is rendered by Fibo 2.0"));
        assert!(fibo.contains("the fields `aspect_ratio`, `fibo_caption`, `input`, `nsfw_result`, and `outputs`, in that order"));
        assert!(fibo.contains("single Fibo 2.0 slot"));
        assert!(!fibo.contains("Krea 2"));
        assert!(!fibo.contains("FLUX"));
        assert!(!fibo.contains("All prompts describe the same image idea"));
        assert!(!fibo.contains("{{"));
    }

    #[test]
    fn edit_prompt_pairs_an_instruction_with_the_caption_of_the_result() {
        let edit = render_image_edit_optimizer_prompt(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::FIBO,
        })
        .expect("render edit");
        assert!(edit.contains("`outputs[0]` is carried out by Fibo 2.0"));
        assert!(edit.contains("**Fibo 2.0**"));
        assert!(edit.contains(
            "the fields `fibo_caption`, `input`, `nsfw_result`, and `outputs`, in that order"
        ));
        assert!(edit.contains("<image_2>"));
        assert!(!edit.contains("FLUX"));
        assert!(!edit.contains("{{"));
        let mut examples = 0;
        for line in edit.lines().filter(|line| line.starts_with("{\"")) {
            let example: Value = serde_json::from_str(line).expect("edit example parses");
            let keys: Vec<&String> = example.as_object().expect("example").keys().collect();
            assert_eq!(
                keys,
                ["fibo_caption", "input", "nsfw_result", "outputs"],
                "{line}"
            );
            let positions: Vec<usize> = keys
                .iter()
                .map(|key| line.find(&format!("\"{key}\": ")).expect("example key"))
                .collect();
            assert!(positions.windows(2).all(|pair| pair[0] < pair[1]), "{line}");
            assert!(
                !example["fibo_caption"].to_string().contains("<image_"),
                "{line}"
            );
            examples += 1;
        }
        assert!(examples > 0);
    }

    #[test]
    fn krea2_slots_get_their_own_rules_and_example() {
        let krea2 = render_image_optimizer_prompt(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::KREA2,
        })
        .expect("render krea2");
        assert!(krea2.contains("`outputs[0]` is rendered by Krea 2 Turbo"));
        assert!(krea2.contains("**Krea 2 Turbo**"));
        assert!(krea2.contains("60 to 200 words"));
        assert!(krea2.contains("single Krea 2 Turbo slot"));
        assert!(!krea2.contains("**Fibo 2.0**"));
        assert!(!krea2.contains("fibo_caption"));
        assert!(!krea2.contains("{{"));
    }

    #[test]
    fn prompt_targets_and_length_bounds_follow_the_model() {
        assert_eq!(
            ImageModel::from_prompt_target(" FIBO "),
            Some(ImageModel::Fibo)
        );
        assert_eq!(
            ImageModel::from_prompt_target("krea2"),
            Some(ImageModel::Krea2)
        );
        assert_eq!(
            ImageModel::from_prompt_target("moss"),
            Some(ImageModel::Moss)
        );
        for retired in ["flux3", "klein", "boogu", "qwen_image", "dall-e"] {
            assert_eq!(ImageModel::from_prompt_target(retired), None);
        }

        assert_eq!(ImageTargets::FIBO.prompt_max_chars(), FIBO_PROMPT_MAX_CHARS);
        assert_eq!(
            ImageTargets::KREA2
                .followed_by(1, ImageTargets::MOSS, 1)
                .prompt_max_chars(),
            MOSS_PROMPT_MAX_CHARS
        );
        assert_eq!(
            ImageTargets::KREA2.prompt_max_chars(),
            KREA2_PROMPT_MAX_CHARS
        );
        assert_eq!(ImageTargets::MOSS.prompt_max_chars(), MOSS_PROMPT_MAX_CHARS);
    }

    #[test]
    fn moss_slots_get_their_own_rules_and_example() {
        let moss = render_image_optimizer_prompt(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::MOSS,
        })
        .expect("render moss");
        assert!(moss.contains("`outputs[0]` is rendered by MOSS-Image"));
        assert!(moss.contains("**MOSS-Image**"));
        assert!(moss.contains("single MOSS-Image slot"));
        assert!(!moss.contains("**Fibo 2.0**"));
        assert!(!moss.contains("**Krea 2 Turbo**"));
        assert!(!moss.contains("{{"));

        let vip = render_image_optimizer_prompt(OptimizePromptOptions {
            variant_count: 3,
            targets: ImageTargets::KREA2
                .followed_by(1, ImageTargets::MOSS, 1)
                .followed_by(2, ImageTargets::FIBO, 1),
        })
        .expect("render vip");
        assert!(vip.contains("`outputs[0]` is rendered by Krea 2 Turbo"));
        assert!(vip.contains("`outputs[1]` is rendered by MOSS-Image"));
        assert!(vip.contains("`outputs[2]` is rendered by Fibo 2.0"));
        assert!(vip.contains("**Krea 2 Turbo**"));
        assert!(vip.contains("**MOSS-Image**"));
        assert!(vip.contains("**Fibo 2.0**"));
        assert!(vip.contains("`outputs` is one array of 3 complete prompts in slot order"));
        assert!(vip.contains("`aspect_ratio`, `fibo_caption`, `input`, `nsfw_result`, `outputs`, and `slot_moss_negative`, in that order"));
        assert!(vip.contains("written as `\\n` in the JSON string"));
        assert!(vip.contains("**MOSS-Image negative prompt**"));
        assert!(!vip.contains("moss_layout"));
        assert!(!vip.contains("moss_kind"));
        assert!(!vip.contains("{{"));

        let layout = render_image_optimizer_prompt(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::MOSS.with_moss_layout(true),
        })
        .expect("render moss layout");
        assert!(layout.contains(
                "`moss_kind`, `moss_layout`, `nsfw_result`, `outputs`, and `slot_moss_negative`, in that order"
            ));
        assert!(layout.contains("**MOSS-Image layout**"));
        assert!(layout.contains("Use the same breaks in `moss_layout`."));
        assert!(layout.contains("\"moss_kind\": \"poster\", \"moss_layout\": {"));
        assert!(!layout.contains("{{"));

        let krea2 =
            render_image_optimizer_prompt(OptimizePromptOptions::default()).expect("render krea2");
        assert!(!krea2.contains("slot_moss_"));
        assert!(!krea2.contains("one array of"));
    }

    #[test]
    fn moss_layouts_use_the_pixel_canvas_draw_api_draws() {
        // draw-api's area_dimensions(ratio, 1024, 16) for every optimizer aspect ratio.
        for (ratio, width, height) in [
            ("1:1", 1024, 1024),
            ("2:3", 832, 1248),
            ("3:2", 1248, 832),
            ("3:4", 880, 1184),
            ("4:3", 1184, 880),
            ("9:16", 768, 1360),
            ("16:9", 1360, 768),
            ("1:2", 720, 1456),
            ("2:1", 1456, 720),
        ] {
            assert_eq!(moss_canvas(ratio), Some((width, height)), "{ratio}");
        }
        assert_eq!(moss_canvas("square"), None);
        assert_eq!(moss_canvas("0:1"), None);

        let prompt = render_image_optimizer_prompt(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::MOSS.with_moss_layout(true),
        })
        .expect("render moss layout");
        assert!(prompt.contains("`aspect_ratio`: 1:1 is 1024 × 1024, 2:3 is 832 × 1248,"));
        assert!(prompt.contains("1:2 is 720 × 1456, 2:1 is 1456 × 720."));
        assert!(prompt.contains(
            "`bbox` is `[x_min, y_min, x_max, y_max]` in pixels from the top-left corner"
        ));
        assert!(prompt.contains(
            "\"canvas_height\": 1184, \"canvas_width\": 880, \"items\": [{\"bbox\": [106, 83, 774, 391]"
        ));

        let schema = optimize_prompt_terminator_definition(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::MOSS.with_moss_layout(true),
        })
        .input_schema;
        let layout = &schema["properties"]["moss_layout"]["anyOf"][0];
        assert_eq!(
            layout["required"],
            json!(["canvas_width", "canvas_height", "summary", "items"])
        );
        assert_eq!(
            layout["properties"]["items"]["maxItems"],
            MOSS_LAYOUT_MAX_ITEMS
        );
        assert_eq!(
            layout["properties"]["summary"]["required"],
            json!(["caption", "meta", "canvas"])
        );
    }

    #[test]
    fn moss_fields_join_the_schema_only_with_a_moss_slot() {
        let prose = optimize_prompt_terminator_definition(OptimizePromptOptions {
            variant_count: 2,
            targets: ImageTargets::KREA2.followed_by(1, ImageTargets::MOSS, 1),
        })
        .input_schema;
        assert_eq!(
            prose["required"],
            json!([
                "input",
                "outputs",
                "nsfw_result",
                "aspect_ratio",
                "slot_moss_negative"
            ])
        );
        assert!(prose["properties"]["moss_layout"].is_null());

        // The layout flag of either side survives joining two generators.
        let vip = optimize_prompt_terminator_definition(OptimizePromptOptions {
            variant_count: 2,
            targets: ImageTargets::KREA2.followed_by(
                1,
                ImageTargets::MOSS.with_moss_layout(true),
                1,
            ),
        })
        .input_schema;
        assert_eq!(
            vip["required"],
            json!([
                "input",
                "outputs",
                "nsfw_result",
                "aspect_ratio",
                "moss_kind",
                "moss_layout",
                "slot_moss_negative"
            ])
        );
        assert_eq!(vip["properties"]["moss_kind"]["enum"], json!(MOSS_KINDS));
        assert!(
            MOSS_LAYOUT_KINDS
                .iter()
                .all(|kind| MOSS_KINDS.contains(kind))
        );
        let layout = &vip["properties"]["moss_layout"]["anyOf"];
        assert_eq!(layout[1], json!({ "type": "null" }));
        assert_eq!(layout[0]["additionalProperties"], false);
        let shapes = &layout[0]["properties"]["items"]["items"]["anyOf"];
        let bbox = &shapes[0]["properties"]["bbox"];
        assert_eq!(
            (bbox["minItems"].clone(), bbox["maxItems"].clone()),
            (json!(4), json!(4))
        );
        // Only a text item carries wording.
        assert_eq!(shapes[0]["properties"]["label"]["enum"], json!(["text"]));
        assert_eq!(
            shapes[0]["required"],
            json!(["label", "caption", "text", "bbox"])
        );
        assert_eq!(
            shapes[1]["properties"]["label"]["enum"],
            json!(["object", "character"])
        );
        assert!(shapes[1]["properties"]["text"].is_null());
        assert_eq!(shapes[1]["additionalProperties"], false);
        // The grammar writes keys in serialized order: the prompts before the MOSS fields.
        let keys: Vec<&String> = vip["properties"]
            .as_object()
            .expect("properties")
            .keys()
            .collect();
        let position = |key: &str| keys.iter().position(|k| *k == key).expect("key");
        assert!(position("moss_kind") < position("moss_layout"));
        assert!(position("moss_layout") < position("outputs"));
        assert!(position("outputs") < position("slot_moss_negative"));

        // Without a MOSS-Image slot the layout flag adds nothing.
        let krea2 = optimize_prompt_terminator_definition(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::KREA2.with_moss_layout(true),
        });
        assert!(krea2.input_schema["properties"]["moss_layout"].is_null());
        assert!(krea2.input_schema["properties"]["slot_moss_negative"].is_null());
        let edit = optimize_edit_prompt_terminator_definition(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::FIBO,
        });
        assert!(edit.input_schema["properties"]["slot_moss_negative"].is_null());
    }

    #[test]
    fn examples_write_their_keys_in_the_order_the_grammar_does() {
        // A grammar-constrained model writes the keys in the schema's serialized order.
        // Examples in another order made it write a field name as the second prompt of a
        // multi-slot call, or loop in it until the token limit.
        for targets in [
            ImageTargets::KREA2,
            ImageTargets::FIBO,
            ImageTargets::MOSS,
            ImageTargets::MOSS.with_moss_layout(true),
        ] {
            let options = OptimizePromptOptions {
                variant_count: 1,
                targets,
            };
            let prompt = render_image_optimizer_prompt(options).expect("render");
            let schema = optimize_prompt_terminator_definition(options).input_schema;
            let keys: Vec<&String> = schema["properties"]
                .as_object()
                .expect("properties")
                .keys()
                .collect();
            let mut examples = 0;
            for line in prompt.lines().filter(|line| line.starts_with("{\"")) {
                let example: Value = serde_json::from_str(line).expect("example parses");
                assert_eq!(
                    example.as_object().map(serde_json::Map::len),
                    Some(keys.len()),
                    "{line}"
                );
                let positions: Vec<usize> = keys
                    .iter()
                    .map(|key| line.find(&format!("\"{key}\": ")).expect("example key"))
                    .collect();
                assert!(positions.windows(2).all(|pair| pair[0] < pair[1]), "{line}");
                examples += 1;
            }
            assert!(examples > 0);
        }
    }

    #[test]
    fn moss_layout_decodes_only_for_a_designed_image() {
        let layout = json!({
            "items": [{"bbox": [70, 120, 330, 880], "caption": "Bold cream letters.", "label": "text", "text": "Джаз\nу моря"}],
            "summary": {"caption": "A flat vector jazz poster."},
        });
        let payload = |kind: &str, layout: &Value| {
            json!({
                "input": "постер",
                "outputs": ["A flat vector illustration poster for a jazz festival."],
                "nsfw_result": "safe",
                "aspect_ratio": "3:4",
                "moss_kind": kind,
                "moss_layout": layout,
                "slot_moss_negative": " people ",
            })
            .to_string()
        };
        let designed = decode_image_optimize_payload(&payload("poster", &layout), "постер", 1)
            .expect("decode designed");
        assert!(designed.moss_design);
        assert_eq!(designed.moss_layout.as_ref(), Some(&layout));
        assert_eq!(designed.moss_negative, "people");
        assert_eq!(
            designed
                .moss_layout
                .as_ref()
                .and_then(|l| l.pointer("/items/0/text")),
            Some(&json!("Джаз\nу моря"))
        );

        let quoted = json!({
            "items": [
                {"bbox": [0, 0, 10, 10], "caption": "c", "label": "text", "text": "\"Кино на крыше\""},
                {"bbox": [0, 0, 10, 10], "caption": "c", "label": "text", "text": "фильм «Солярис»"},
                {"bbox": [0, 0, 10, 10], "caption": "c", "label": "text", "text": "\"\""},
            ],
            "summary": {"caption": "c"},
        });
        let unquoted = decode_image_optimize_payload(&payload("poster", &quoted), "постер", 1)
            .expect("decode quoted");
        let texts: Vec<&Value> = unquoted.moss_layout.as_ref().expect("layout")["items"]
            .as_array()
            .expect("items")
            .iter()
            .map(|item| &item["text"])
            .collect();
        assert_eq!(
            texts,
            [
                &json!("Кино на крыше"),
                &json!("фильм «Солярис»"),
                &json!("\"\"")
            ]
        );

        // Only a layout kind keeps the layout, whatever the model wrote.
        for kind in ["scene", "card", "menu", "unknown"] {
            let other = decode_image_optimize_payload(&payload(kind, &layout), "постер", 1)
                .expect("decode other kind");
            assert!(!other.moss_design, "{kind}");
            assert_eq!(other.moss_layout, None, "{kind}");
        }
        for kind in MOSS_LAYOUT_KINDS {
            let designed = decode_image_optimize_payload(&payload(kind, &layout), "постер", 1)
                .expect("decode layout kind");
            assert!(designed.moss_design, "{kind}");
        }

        // A malformed layout degrades to the prose prompt instead of failing the payload.
        for broken in [json!("layout"), json!({"items": []}), json!(null)] {
            let decoded = decode_image_optimize_payload(&payload("poster", &broken), "постер", 1)
                .expect("decode broken layout");
            assert_eq!(decoded.moss_layout, None);
            assert_eq!(decoded.outputs.len(), 1);
        }

        let legacy = decode_image_optimize_payload(
            r#"{"input": "cat", "outputs": ["A cat on a red sofa."], "nsfw_result": "safe"}"#,
            "cat",
            1,
        )
        .expect("decode legacy");
        assert!(!legacy.moss_design);
        assert_eq!(legacy.moss_layout, None);
        assert!(legacy.moss_negative.is_empty());
    }

    #[test]
    fn placeholder_slots_take_the_nearest_real_prompt() {
        let first = "A photograph of a cozy cafe on a rainy evening.";
        let third = "A photograph of a cafe sign reading Plotva at dusk.";
        let payload = json!({
            "input": "кафе",
            "outputs": [first, "nsfw_result", third],
            "nsfw_result": "safe",
        })
        .to_string();
        let decoded = decode_image_optimize_payload(&payload, "кафе", 3).expect("decode");
        assert_eq!(decoded.outputs, [first, first, third]);

        let payload = json!({
            "input": "кафе",
            "outputs": ["outputs[1] = ", "safe", third],
            "nsfw_result": "safe",
        })
        .to_string();
        let decoded = decode_image_optimize_payload(&payload, "кафе", 3).expect("decode");
        assert_eq!(decoded.outputs, [third, third, third]);

        let payload = json!({
            "input": "меню",
            "outputs": ["A chalkboard menu reading \"капучино 250\\nлатте 280\" in white chalk."],
            "nsfw_result": "safe",
        })
        .to_string();
        let decoded = decode_image_optimize_payload(&payload, "меню", 1).expect("decode");
        assert_eq!(
            decoded.outputs,
            ["A chalkboard menu reading \"капучино 250\nлатте 280\" in white chalk."]
        );

        // Nothing to borrow from: the slots stay as the model wrote them.
        let decoded = decode_image_optimize_payload(
            r#"{"input": "кот", "outputs": ["кот"], "nsfw_result": "safe"}"#,
            "кот",
            1,
        )
        .expect("decode");
        assert_eq!(decoded.outputs, ["кот"]);
    }

    #[test]
    fn forbidden_requests_drop_the_moss_fields() {
        let payload = json!({
            "input": "x",
            "outputs": ["Blocked: forbidden CSAM request."],
            "nsfw_result": "forbidden",
            "moss_kind": "poster",
            "moss_layout": {"items": [{"bbox": [0, 0, 10, 10], "caption": "c", "label": "object"}], "summary": {"caption": "c"}},
            "slot_moss_negative": "hat",
        })
        .to_string();
        let decoded = decode_image_optimize_payload(&payload, "x", 1).expect("decode");
        assert!(!decoded.moss_design);
        assert_eq!(decoded.moss_layout, None);
        assert!(decoded.moss_negative.is_empty());
    }

    #[test]
    fn word_replacements_reach_layout_captions_but_not_lettering() {
        let optimized = ImageOptimize {
            outputs: vec!["plotva swims.".to_owned()],
            moss_negative: "roach".to_owned(),
            moss_design: true,
            moss_layout: Some(json!({
                "items": [{"caption": "plotva on a plate.", "label": "text", "text": "plotva", "bbox": [0, 0, 10, 10]}],
                "summary": {"caption": "roach poster."},
            })),
            ..ImageOptimize::default()
        };
        let replaced = apply_word_replacements(optimized);
        assert_eq!(replaced.outputs, ["roach-fish swims."]);
        assert_eq!(replaced.moss_negative, "roach-fish");
        let layout = replaced.moss_layout.expect("layout");
        assert_eq!(
            layout.pointer("/summary/caption"),
            Some(&json!("roach-fish poster."))
        );
        assert_eq!(
            layout.pointer("/items/0/caption"),
            Some(&json!("roach-fish on a plate."))
        );
        assert_eq!(layout.pointer("/items/0/text"), Some(&json!("plotva")));
    }

    #[test]
    fn upsampler_reference_sizes_follow_the_official_policy() {
        assert_eq!(upsampler_reference_size(1000, 1000, 1), Some((992, 992)));
        assert_eq!(upsampler_reference_size(1280, 960, 1), Some((1152, 864)));
        assert_eq!(upsampler_reference_size(1000, 1000, 2), Some((704, 704)));
        assert_eq!(upsampler_reference_size(896, 1120, 3), Some((448, 544)));
        assert_eq!(upsampler_reference_size(500, 300, 1), Some((512, 288)));
        assert_eq!(upsampler_reference_size(20, 3000, 1), Some((32, 3008)));
        assert_eq!(upsampler_reference_size(1, 300, 1), None);
        assert_eq!(upsampler_reference_size(0, 300, 1), None);
    }

    #[test]
    fn edit_optimizer_text_names_the_reference_order_only_with_pictures() {
        assert_eq!(
            image_edit_optimizer_text("make it night", 0),
            "make it night"
        );
        assert_eq!(
            image_edit_optimizer_text("make it night", 2),
            "The pictures above are <image_1>, <image_2>, ... in order.\nmake it night"
        );
    }

    #[test]
    fn optimizer_schema_strings_carry_no_length_bound() {
        let schema = optimize_prompt_terminator_definition(OptimizePromptOptions {
            variant_count: 1,
            ..OptimizePromptOptions::default()
        })
        .input_schema;
        assert!(schema["properties"]["outputs"]["items"]["maxLength"].is_null());
        assert_eq!(schema["properties"]["outputs"]["maxItems"], 1);
    }

    #[test]
    fn outputs_are_cut_to_the_limit_in_characters() {
        let mut outputs = vec![
            "ж".repeat(10),
            "short".to_owned(),
            format!("{} {}", "a".repeat(4), "b".repeat(5)),
        ];
        truncate_outputs_to_chars(&mut outputs, 5);
        assert_eq!(outputs, vec!["жжжжж", "short", "aaaa"]);
    }

    #[test]
    fn image_targets_follow_each_side_slot_count() {
        let pair = ImageTargets::KREA2.followed_by(1, ImageTargets::FIBO, 1);
        assert_eq!(pair.models(), [ImageModel::Krea2, ImageModel::Fibo]);

        let wide_first = ImageTargets::KREA2.followed_by(2, ImageTargets::FIBO, 1);
        assert_eq!(
            wide_first.models(),
            [ImageModel::Krea2, ImageModel::Krea2, ImageModel::Fibo]
        );
        assert_eq!(wide_first.model_for_slot(1), ImageModel::Krea2);
        assert_eq!(wide_first.model_for_slot(2), ImageModel::Fibo);

        assert_eq!(ImageTargets::FIBO.model_for_slot(3), ImageModel::Fibo);
        assert_eq!(ImageTargets::default(), ImageTargets::KREA2);
        assert_eq!(
            ImageTargets::FIBO
                .followed_by(3, ImageTargets::KREA2, 3)
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
            assert!(prompt.split_whitespace().count() <= 6_000);
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

        let tool = optimize_prompt_terminator_definition(OptimizePromptOptions {
            variant_count: 2,
            ..OptimizePromptOptions::default()
        });
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

        let edit_tool =
            optimize_edit_prompt_terminator_definition(OptimizePromptOptions::default());
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
                fibo_caption: None,
            }
        );
    }

    #[test]
    fn fibo_slots_add_a_caption_drafted_before_the_prompts() {
        let vip = optimize_prompt_terminator_definition(OptimizePromptOptions {
            variant_count: 3,
            targets: ImageTargets::KREA2
                .followed_by(1, ImageTargets::MOSS, 1)
                .followed_by(2, ImageTargets::FIBO, 1),
        })
        .input_schema;
        assert_eq!(
            vip["required"],
            json!([
                "input",
                "outputs",
                "nsfw_result",
                "aspect_ratio",
                "fibo_caption",
                "slot_moss_negative"
            ])
        );
        let keys: Vec<&String> = vip["properties"]
            .as_object()
            .expect("properties")
            .keys()
            .collect();
        let position = |key: &str| keys.iter().position(|k| *k == key).expect("key");
        assert!(position("fibo_caption") < position("outputs"));
        let caption = &vip["properties"]["fibo_caption"];
        assert!(caption["properties"]["short_description"].is_null());
        assert!(caption["properties"]["edit_instruction"].is_null());
        assert_eq!(
            caption["properties"]["objects"]["maxItems"],
            FIBO_CAPTION_MAX_OBJECTS
        );
        assert_eq!(
            caption["properties"]["text_render"]["items"]["required"],
            json!(["text", "location", "size", "color", "font"])
        );
        assert_eq!(
            caption["properties"]["lighting"]["required"],
            json!(["conditions", "direction", "shadows"])
        );

        let edit = optimize_edit_prompt_terminator_definition(OptimizePromptOptions {
            variant_count: 1,
            targets: ImageTargets::FIBO,
        })
        .input_schema;
        assert_eq!(
            edit["required"],
            json!(["input", "outputs", "nsfw_result", "fibo_caption"])
        );
        let krea2 = optimize_prompt_terminator_definition(OptimizePromptOptions::default());
        assert!(krea2.input_schema["properties"]["fibo_caption"].is_null());
    }

    #[test]
    fn fibo_captions_decode_only_as_objects_and_never_for_forbidden_requests() {
        let caption = json!({
            "objects": [{"description": "Plotva on a plate."}],
            "text_render": [{"text": "plotva"}],
        });
        let payload = |nsfw: &str, caption: &Value| {
            json!({
                "aspect_ratio": "1:1",
                "fibo_caption": caption,
                "input": "рыба",
                "nsfw_result": nsfw,
                "outputs": ["A photograph of a fish on a plate."],
            })
            .to_string()
        };
        let decoded =
            decode_image_optimize_payload(&payload("safe", &caption), "рыба", 1).expect("decode");
        assert_eq!(decoded.fibo_caption.as_ref(), Some(&caption));
        let replaced = apply_word_replacements(decoded);
        let replaced = replaced.fibo_caption.expect("caption");
        assert_eq!(
            replaced.pointer("/objects/0/description"),
            Some(&json!("roach-fish on a plate."))
        );
        assert_eq!(
            replaced.pointer("/text_render/0/text"),
            Some(&json!("plotva"))
        );

        for broken in [json!("caption"), json!({}), json!(null), json!([1])] {
            let decoded = decode_image_optimize_payload(&payload("safe", &broken), "рыба", 1)
                .expect("decode broken caption");
            assert_eq!(decoded.fibo_caption, None);
        }
        let forbidden = decode_image_optimize_payload(&payload("forbidden", &caption), "x", 1)
            .expect("decode forbidden");
        assert_eq!(forbidden.fibo_caption, None);

        let edit = decode_image_edit_optimize_payload(
            &json!({
                "fibo_caption": caption,
                "input": "сделай ночь",
                "nsfw_result": "safe",
                "outputs": ["Change the time of day to night."],
            })
            .to_string(),
            "сделай ночь",
            1,
        )
        .expect("decode edit");
        assert_eq!(edit.fibo_caption.as_ref(), Some(&caption));
        let blocked = decode_image_edit_optimize_payload(
            &json!({
                "fibo_caption": caption,
                "input": "x",
                "nsfw_result": "forbidden",
                "outputs": ["Blocked: forbidden request."],
            })
            .to_string(),
            "x",
            1,
        )
        .expect("decode blocked edit");
        assert_eq!(blocked.fibo_caption, None);
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
