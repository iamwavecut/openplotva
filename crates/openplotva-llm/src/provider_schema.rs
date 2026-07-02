//! Typed views over the provider/model `config` JSONB columns.
//!
//! A provider's `protocol` names the wire payload shape the runtime sends;
//! discovery resolution stays orthogonal (presence of a discovery service
//! name). The optional `runtime_hint` names the serving engine behind the
//! endpoint and only drives which parameter fields the admin UI offers.
//!
//! Config structs are `#[serde(default)]` and flatten-tolerant, so any stored
//! row parses regardless of age and unknown keys survive round-trips; no
//! schema-version column is needed.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

/// Wire-payload protocol of a provider endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    /// OpenAI-compatible `/v1/chat/completions` (direct or discovery-resolved).
    OpenAiCompat,
    /// Google AI / genkit chat.
    Genkit,
    /// ACE-Step music API (`/release_task` + `/query_result` or completion mode).
    AceStep,
    /// Discovery blocking-jobs API (`/v1/jobs/blocking` + poll), e.g. embedder.
    DiscoveryJobs,
    /// Discovery draw API (image generation/editing job queue).
    DiscoveryDraw,
    /// Non-LLM privacy/redaction discovery service.
    PrivacyFilter,
}

impl Protocol {
    #[must_use]
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "openai_compat" => Some(Self::OpenAiCompat),
            "genkit" => Some(Self::Genkit),
            "acestep" => Some(Self::AceStep),
            "discovery_jobs" => Some(Self::DiscoveryJobs),
            "discovery_draw" => Some(Self::DiscoveryDraw),
            "privacy_filter" => Some(Self::PrivacyFilter),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenAiCompat => "openai_compat",
            Self::Genkit => "genkit",
            Self::AceStep => "acestep",
            Self::DiscoveryJobs => "discovery_jobs",
            Self::DiscoveryDraw => "discovery_draw",
            Self::PrivacyFilter => "privacy_filter",
        }
    }

    /// Whether the admin can fetch this protocol's model list from the endpoint.
    #[must_use]
    pub fn supports_model_listing(self) -> bool {
        matches!(self, Self::OpenAiCompat | Self::AceStep)
    }

    #[must_use]
    pub fn all() -> &'static [Self] {
        &[
            Self::OpenAiCompat,
            Self::Genkit,
            Self::AceStep,
            Self::DiscoveryJobs,
            Self::DiscoveryDraw,
            Self::PrivacyFilter,
        ]
    }
}

/// Serving engine behind an endpoint; keys the parameter schemas the UI offers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeHint {
    LlamaCpp,
    Vllm,
    Sglang,
    Ollama,
    Tgi,
}

impl RuntimeHint {
    #[must_use]
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "llama_cpp" => Some(Self::LlamaCpp),
            "vllm" => Some(Self::Vllm),
            "sglang" => Some(Self::Sglang),
            "ollama" => Some(Self::Ollama),
            "tgi" => Some(Self::Tgi),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LlamaCpp => "llama_cpp",
            Self::Vllm => "vllm",
            Self::Sglang => "sglang",
            Self::Ollama => "ollama",
            Self::Tgi => "tgi",
        }
    }

    #[must_use]
    pub fn all() -> &'static [Self] {
        &[
            Self::LlamaCpp,
            Self::Vllm,
            Self::Sglang,
            Self::Ollama,
            Self::Tgi,
        ]
    }
}

/// Common provider settings tolerated as a subset of the provider JSONB config.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct ProviderCommonConfig {
    pub timeout_ms: Option<u64>,
    pub connect_timeout_ms: Option<u64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Model inference parameters tolerated as a subset of the model JSONB config.
/// Unknown keys survive round-trips via `extra` (e.g. provider-specific
/// passthrough like `chat_template_kwargs` mirrored into `extra_body`).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct ModelParamsConfig {
    pub temperature: Option<f64>,
    pub max_tokens: Option<i64>,
    pub top_p: Option<f64>,
    pub context_length: Option<i64>,
    pub enable_thinking: Option<bool>,
    pub chat_template_kwargs: Option<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A config value failed schema validation.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ConfigSchemaError {
    #[error("config must be a JSON object")]
    NotAnObject,
    #[error("field `{field}`: {message}")]
    Field {
        field: &'static str,
        message: String,
    },
}

fn field_err(field: &'static str, message: impl Into<String>) -> ConfigSchemaError {
    ConfigSchemaError::Field {
        field,
        message: message.into(),
    }
}

/// Validate an admin-entered provider config against the common schema.
pub fn validate_provider_config(
    _protocol: Protocol,
    config: &Value,
) -> Result<(), ConfigSchemaError> {
    if !config.is_object() {
        return Err(ConfigSchemaError::NotAnObject);
    }
    let parsed: ProviderCommonConfig = serde_json::from_value(config.clone())
        .map_err(|error| field_err("config", error.to_string()))?;
    if parsed.timeout_ms == Some(0) {
        return Err(field_err("timeout_ms", "must be positive"));
    }
    if parsed.connect_timeout_ms == Some(0) {
        return Err(field_err("connect_timeout_ms", "must be positive"));
    }
    Ok(())
}

/// Validate an admin-entered model config against the params schema.
pub fn validate_model_config(config: &Value) -> Result<(), ConfigSchemaError> {
    if !config.is_object() {
        return Err(ConfigSchemaError::NotAnObject);
    }
    let parsed: ModelParamsConfig = serde_json::from_value(config.clone())
        .map_err(|error| field_err("config", error.to_string()))?;
    if let Some(temperature) = parsed.temperature
        && !(0.0..=2.0).contains(&temperature)
    {
        return Err(field_err("temperature", "must be within 0.0..=2.0"));
    }
    if let Some(top_p) = parsed.top_p
        && !(0.0..=1.0).contains(&top_p)
    {
        return Err(field_err("top_p", "must be within 0.0..=1.0"));
    }
    if let Some(max_tokens) = parsed.max_tokens
        && max_tokens <= 0
    {
        return Err(field_err("max_tokens", "must be positive"));
    }
    if let Some(context_length) = parsed.context_length
        && context_length <= 0
    {
        return Err(field_err("context_length", "must be positive"));
    }
    if let Some(kwargs) = &parsed.chat_template_kwargs
        && !kwargs.is_object()
    {
        return Err(field_err("chat_template_kwargs", "must be a JSON object"));
    }
    Ok(())
}

/// Field descriptor set the admin UI renders as a typed form for one
/// (protocol, runtime hint) pair. A pure data contract: `fields` list the
/// common knobs, `runtime_fields` the engine-specific extras.
#[must_use]
pub fn param_descriptor(protocol: Protocol, hint: Option<RuntimeHint>) -> Value {
    let provider_fields = match protocol {
        Protocol::OpenAiCompat | Protocol::Genkit | Protocol::AceStep => json!([
            { "key": "timeout_ms", "kind": "int", "label": "Request timeout (ms)" },
            { "key": "connect_timeout_ms", "kind": "int", "label": "Connect timeout (ms)" },
        ]),
        Protocol::DiscoveryJobs | Protocol::DiscoveryDraw | Protocol::PrivacyFilter => json!([
            { "key": "timeout_ms", "kind": "int", "label": "Job timeout (ms)" },
        ]),
    };
    let model_fields = match protocol {
        Protocol::OpenAiCompat | Protocol::Genkit => json!([
            { "key": "temperature", "kind": "float", "min": 0.0, "max": 2.0, "label": "Temperature" },
            { "key": "top_p", "kind": "float", "min": 0.0, "max": 1.0, "label": "Top-p" },
            { "key": "max_tokens", "kind": "int", "label": "Max output tokens" },
            { "key": "context_length", "kind": "int", "label": "Context length" },
        ]),
        Protocol::AceStep
        | Protocol::DiscoveryJobs
        | Protocol::DiscoveryDraw
        | Protocol::PrivacyFilter => json!([]),
    };
    let runtime_fields = match (protocol, hint) {
        (Protocol::OpenAiCompat, Some(RuntimeHint::LlamaCpp)) => json!([
            { "key": "repeat_penalty", "kind": "float", "label": "Repeat penalty" },
            { "key": "dry_multiplier", "kind": "float", "label": "DRY multiplier" },
        ]),
        (Protocol::OpenAiCompat, Some(RuntimeHint::Vllm | RuntimeHint::Sglang)) => json!([
            { "key": "enable_thinking", "kind": "bool", "label": "Enable thinking" },
            { "key": "chat_template_kwargs", "kind": "json", "label": "Chat template kwargs" },
        ]),
        (Protocol::OpenAiCompat, Some(RuntimeHint::Ollama | RuntimeHint::Tgi)) => json!([
            { "key": "repeat_penalty", "kind": "float", "label": "Repeat penalty" },
        ]),
        _ => json!([]),
    };
    json!({
        "protocol": protocol.as_str(),
        "runtime_hint": hint.map(RuntimeHint::as_str),
        "supports_model_listing": protocol.supports_model_listing(),
        "provider_fields": provider_fields,
        "model_fields": model_fields,
        "runtime_fields": runtime_fields,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_round_trips_through_db_strings() {
        for protocol in Protocol::all() {
            assert_eq!(Protocol::from_db(protocol.as_str()), Some(*protocol));
        }
        assert_eq!(Protocol::from_db("discovery"), None);
    }

    #[test]
    fn runtime_hint_round_trips_through_db_strings() {
        for hint in RuntimeHint::all() {
            assert_eq!(RuntimeHint::from_db(hint.as_str()), Some(*hint));
        }
        assert_eq!(RuntimeHint::from_db("llamacpp"), None);
    }

    #[test]
    fn model_config_unknown_keys_survive_round_trip() {
        let raw = json!({
            "temperature": 0.4,
            "custom_extra_body": { "thinking": false },
        });
        let parsed: ModelParamsConfig = serde_json::from_value(raw.clone()).expect("parse");
        assert_eq!(parsed.temperature, Some(0.4));
        assert!(parsed.extra.contains_key("custom_extra_body"));
        let back = serde_json::to_value(&parsed).expect("serialize");
        assert_eq!(back.get("custom_extra_body"), raw.get("custom_extra_body"));
    }

    #[test]
    fn model_config_validation_rejects_out_of_range_values() {
        assert!(validate_model_config(&json!({ "temperature": 0.7 })).is_ok());
        assert!(validate_model_config(&json!({ "temperature": 3.0 })).is_err());
        assert!(validate_model_config(&json!({ "top_p": 1.5 })).is_err());
        assert!(validate_model_config(&json!({ "max_tokens": 0 })).is_err());
        assert!(validate_model_config(&json!({ "chat_template_kwargs": 5 })).is_err());
        assert!(validate_model_config(&json!("nope")).is_err());
    }

    #[test]
    fn provider_config_validation_accepts_unknown_keys() {
        let config = json!({ "timeout_ms": 10_000, "vendor_specific": true });
        assert!(validate_provider_config(Protocol::OpenAiCompat, &config).is_ok());
        assert!(
            validate_provider_config(Protocol::OpenAiCompat, &json!({ "timeout_ms": 0 })).is_err()
        );
    }

    #[test]
    fn param_descriptor_marks_listing_support() {
        let descriptor = param_descriptor(Protocol::OpenAiCompat, Some(RuntimeHint::Sglang));
        assert_eq!(descriptor["supports_model_listing"], json!(true));
        assert_eq!(descriptor["runtime_hint"], json!("sglang"));
        let descriptor = param_descriptor(Protocol::PrivacyFilter, None);
        assert_eq!(descriptor["supports_model_listing"], json!(false));
    }
}
