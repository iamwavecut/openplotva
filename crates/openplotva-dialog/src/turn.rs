//! Pure classification of what a dialog turn actually produced.
//!
//! The turn engine feeds every provider round through [`classify_reply_material`]
//! so no output shape can complete a turn without an explicit, recorded class.

use serde_json::Value;

use crate::{
    DialogOutput, TOOL_RESULT_STATUS_FAILED, TOOL_RESULT_STATUS_NOOP, ToolResult, ToolSideEffect,
};

/// Side-effect kind emitted by the image generation scheduler.
pub const SIDE_EFFECT_KIND_IMAGE: &str = "image_generation_job";
/// Side-effect kind emitted by the song generation scheduler.
pub const SIDE_EFFECT_KIND_MUSIC: &str = "music_generation_job";

pub const SIDE_EFFECT_STATE_QUEUED: &str = "queued";

/// Anti-loop nudge appended to `reference_context` on duplicate regeneration.
pub const ANTI_LOOP_HINT: &str = "Твой прошлый ответ уже отправлен в чат. Не повторяй его дословно — ответь на текущее сообщение заново, другими словами и по сути.";

/// What the provider round actually produced, after outbound sanitization.
#[derive(Clone, Debug, PartialEq)]
pub enum ReplyMaterial {
    /// Sanitized non-empty text (may additionally carry queued side effects).
    Text {
        text: String,
        side_effects: Vec<QueuedSideEffect>,
    },
    /// No text, but generation job(s) were queued; the artifact is the reply.
    Delegated { side_effects: Vec<QueuedSideEffect> },
    /// A tool intentionally suppressed the reply; classified reason.
    IntentionalSilence { reason: NoReplyReason },
    /// Nothing usable came back — retryable.
    Empty { reason: EmptyReason },
}

/// One generation job queued by a tool during the turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuedSideEffect {
    /// `image_generation_job` or `music_generation_job`.
    pub kind: String,
    /// Taskman ticket ID, when the scheduler populated it.
    pub ticket_job_id: Option<i64>,
    /// Estimated completion text, when present.
    pub eta: String,
}

/// Why the turn produced no material at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmptyReason {
    /// Answer, response, and tool material were all empty.
    ProviderEmpty,
    /// Raw text existed but collapsed during outbound sanitization.
    SanitizedEmpty,
}

/// Classified reason a tool intentionally suppressed the reply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoReplyReason {
    DrawDisabled,
    ImageNotAllowed,
    AudioNotAllowed,
    VipOnly,
    ActiveJobLimit,
    EnqueueRateLimited,
    GenerationRateLimited,
    QueueFull,
    ServiceUnavailable,
    EmptyTopic,
    EditSourceUnavailable,
    /// Parseable `no_reply` without a recognizable reason code.
    Unclassified,
}

impl NoReplyReason {
    /// Map a rejection reason code from `ToolResult.data["reason"]`.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        match code.trim() {
            "draw_disabled" => Some(Self::DrawDisabled),
            "image_not_allowed" => Some(Self::ImageNotAllowed),
            "audio_not_allowed" => Some(Self::AudioNotAllowed),
            "vip_only" => Some(Self::VipOnly),
            "active_job_limit" => Some(Self::ActiveJobLimit),
            "enqueue_rate_limited" => Some(Self::EnqueueRateLimited),
            "generation_rate_limited" => Some(Self::GenerationRateLimited),
            "queue_full" => Some(Self::QueueFull),
            "service_unavailable" => Some(Self::ServiceUnavailable),
            "empty_topic" => Some(Self::EmptyTopic),
            "edit_source_unavailable" => Some(Self::EditSourceUnavailable),
            _ => None,
        }
    }

    /// Stable reason code recorded in ledgers and job events.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DrawDisabled => "draw_disabled",
            Self::ImageNotAllowed => "image_not_allowed",
            Self::AudioNotAllowed => "audio_not_allowed",
            Self::VipOnly => "vip_only",
            Self::ActiveJobLimit => "active_job_limit",
            Self::EnqueueRateLimited => "enqueue_rate_limited",
            Self::GenerationRateLimited => "generation_rate_limited",
            Self::QueueFull => "queue_full",
            Self::ServiceUnavailable => "service_unavailable",
            Self::EmptyTopic => "empty_topic",
            Self::EditSourceUnavailable => "edit_source_unavailable",
            Self::Unclassified => "no_reply_unclassified",
        }
    }
}

/// Classify one provider round. Precedence: queued side effects are collected
/// first; non-empty sanitized text wins; then delegation; then intentional
/// silence from `no_reply`/`noop`/`failed` tool results; then raw-but-sanitized-
/// empty; and only a turn with nothing at all is `ProviderEmpty`.
#[must_use]
pub fn classify_reply_material(
    output: &DialogOutput,
    sanitized_answer: &str,
    raw_answer: &str,
) -> ReplyMaterial {
    let results = parsed_tool_results(output);
    let side_effects = queued_side_effects(&results);

    if !sanitized_answer.trim().is_empty() {
        return ReplyMaterial::Text {
            text: sanitized_answer.to_owned(),
            side_effects,
        };
    }
    if !side_effects.is_empty() {
        return ReplyMaterial::Delegated { side_effects };
    }
    if let Some(reason) = intentional_silence_reason(&results) {
        return ReplyMaterial::IntentionalSilence { reason };
    }
    if !raw_answer.trim().is_empty() {
        return ReplyMaterial::Empty {
            reason: EmptyReason::SanitizedEmpty,
        };
    }
    ReplyMaterial::Empty {
        reason: EmptyReason::ProviderEmpty,
    }
}

fn parsed_tool_results(output: &DialogOutput) -> Vec<ToolResult> {
    output
        .tool_calls
        .iter()
        .filter_map(|call| call.output.as_ref())
        .filter_map(|value| serde_json::from_value::<ToolResult>(value.clone()).ok())
        .collect()
}

fn queued_side_effects(results: &[ToolResult]) -> Vec<QueuedSideEffect> {
    results
        .iter()
        .filter_map(|result| result.side_effect.as_ref())
        .filter(|side_effect| is_queued_generation_side_effect(side_effect))
        .map(|side_effect| QueuedSideEffect {
            kind: side_effect.kind.clone(),
            ticket_job_id: side_effect.ticket_id.trim().parse::<i64>().ok(),
            eta: side_effect.eta.clone(),
        })
        .collect()
}

fn is_queued_generation_side_effect(side_effect: &ToolSideEffect) -> bool {
    side_effect
        .state
        .eq_ignore_ascii_case(SIDE_EFFECT_STATE_QUEUED)
        && matches!(
            side_effect.kind.as_str(),
            SIDE_EFFECT_KIND_IMAGE | SIDE_EFFECT_KIND_MUSIC
        )
}

fn intentional_silence_reason(results: &[ToolResult]) -> Option<NoReplyReason> {
    let mut found_silencing_result = false;
    for result in results {
        let silences = result.no_reply
            || result.status.eq_ignore_ascii_case(TOOL_RESULT_STATUS_NOOP)
            || result
                .status
                .eq_ignore_ascii_case(TOOL_RESULT_STATUS_FAILED);
        if !silences {
            continue;
        }
        found_silencing_result = true;
        if let Some(reason) = result
            .data
            .as_ref()
            .and_then(|data| data.get("reason"))
            .and_then(Value::as_str)
            .and_then(NoReplyReason::from_code)
        {
            return Some(reason);
        }
    }
    found_silencing_result.then_some(NoReplyReason::Unclassified)
}

#[cfg(test)]
mod tests {
    use openplotva_core::ToolCall;
    use serde_json::json;

    use super::*;

    fn output_with_tool_outputs(outputs: Vec<Value>) -> DialogOutput {
        DialogOutput {
            tool_calls: outputs
                .into_iter()
                .map(|output| ToolCall {
                    name: "tool".to_owned(),
                    output: Some(output),
                    ..ToolCall::default()
                })
                .collect(),
            ..DialogOutput::default()
        }
    }

    fn queued_side_effect_output(kind: &str, ticket_id: &str) -> Value {
        json!({
            "status": "queued",
            "side_effect": {
                "kind": kind,
                "ticket_id": ticket_id,
                "eta": "~2 минуты",
                "state": "queued"
            }
        })
    }

    #[test]
    fn classifies_reply_material_by_exact_precedence() {
        let cases: Vec<(&str, DialogOutput, &str, &str, ReplyMaterial)> = vec![
            (
                "text only",
                DialogOutput::default(),
                "привет",
                "привет",
                ReplyMaterial::Text {
                    text: "привет".to_owned(),
                    side_effects: Vec::new(),
                },
            ),
            (
                "text with queued image side effect",
                output_with_tool_outputs(vec![queued_side_effect_output(
                    SIDE_EFFECT_KIND_IMAGE,
                    "123",
                )]),
                "рисую",
                "рисую",
                ReplyMaterial::Text {
                    text: "рисую".to_owned(),
                    side_effects: vec![QueuedSideEffect {
                        kind: SIDE_EFFECT_KIND_IMAGE.to_owned(),
                        ticket_job_id: Some(123),
                        eta: "~2 минуты".to_owned(),
                    }],
                },
            ),
            (
                "queued image without ticket delegates",
                output_with_tool_outputs(vec![queued_side_effect_output(
                    SIDE_EFFECT_KIND_IMAGE,
                    "",
                )]),
                "",
                "",
                ReplyMaterial::Delegated {
                    side_effects: vec![QueuedSideEffect {
                        kind: SIDE_EFFECT_KIND_IMAGE.to_owned(),
                        ticket_job_id: None,
                        eta: "~2 минуты".to_owned(),
                    }],
                },
            ),
            (
                "queued song delegates",
                output_with_tool_outputs(vec![queued_side_effect_output(
                    SIDE_EFFECT_KIND_MUSIC,
                    "456",
                )]),
                "",
                "",
                ReplyMaterial::Delegated {
                    side_effects: vec![QueuedSideEffect {
                        kind: SIDE_EFFECT_KIND_MUSIC.to_owned(),
                        ticket_job_id: Some(456),
                        eta: "~2 минуты".to_owned(),
                    }],
                },
            ),
            (
                "no_reply without reason code is unclassified",
                output_with_tool_outputs(vec![json!({"status": "noop", "no_reply": true})]),
                "",
                "",
                ReplyMaterial::IntentionalSilence {
                    reason: NoReplyReason::Unclassified,
                },
            ),
            (
                "failed status without no_reply still silences",
                output_with_tool_outputs(vec![
                    json!({"status": "failed", "data": {"reason": "queue_full"}}),
                ]),
                "",
                "",
                ReplyMaterial::IntentionalSilence {
                    reason: NoReplyReason::QueueFull,
                },
            ),
            (
                "raw text that sanitized to nothing",
                DialogOutput::default(),
                "",
                "<tool_calls><tool_call name=\"x\"/></tool_calls>",
                ReplyMaterial::Empty {
                    reason: EmptyReason::SanitizedEmpty,
                },
            ),
            (
                "nothing at all is provider empty",
                DialogOutput::default(),
                "",
                "",
                ReplyMaterial::Empty {
                    reason: EmptyReason::ProviderEmpty,
                },
            ),
            (
                "malformed tool result json is ignored",
                output_with_tool_outputs(vec![json!("not an object"), json!({"status": 5})]),
                "",
                "",
                ReplyMaterial::Empty {
                    reason: EmptyReason::ProviderEmpty,
                },
            ),
            (
                "non-generation side effect kinds are not delegation",
                output_with_tool_outputs(vec![json!({
                    "status": "queued",
                    "side_effect": {"kind": "other_job", "state": "queued"}
                })]),
                "",
                "",
                ReplyMaterial::Empty {
                    reason: EmptyReason::ProviderEmpty,
                },
            ),
        ];

        for (name, output, sanitized, raw, expected) in cases {
            assert_eq!(
                classify_reply_material(&output, sanitized, raw),
                expected,
                "case: {name}"
            );
        }
    }

    #[test]
    fn classifies_every_rejection_reason_code() {
        let codes = [
            ("draw_disabled", NoReplyReason::DrawDisabled),
            ("image_not_allowed", NoReplyReason::ImageNotAllowed),
            ("audio_not_allowed", NoReplyReason::AudioNotAllowed),
            ("vip_only", NoReplyReason::VipOnly),
            ("active_job_limit", NoReplyReason::ActiveJobLimit),
            ("enqueue_rate_limited", NoReplyReason::EnqueueRateLimited),
            (
                "generation_rate_limited",
                NoReplyReason::GenerationRateLimited,
            ),
            ("queue_full", NoReplyReason::QueueFull),
            ("service_unavailable", NoReplyReason::ServiceUnavailable),
            ("empty_topic", NoReplyReason::EmptyTopic),
            (
                "edit_source_unavailable",
                NoReplyReason::EditSourceUnavailable,
            ),
        ];
        for (code, expected) in codes {
            let output = output_with_tool_outputs(vec![json!({
                "status": "noop",
                "no_reply": true,
                "data": {"reason": code}
            })]);
            assert_eq!(
                classify_reply_material(&output, "", ""),
                ReplyMaterial::IntentionalSilence { reason: expected },
                "code: {code}"
            );
            assert_eq!(NoReplyReason::from_code(code), Some(expected));
            assert_eq!(expected.as_str(), code);
        }
        assert_eq!(NoReplyReason::from_code("unknown_code"), None);
        assert_eq!(
            NoReplyReason::Unclassified.as_str(),
            "no_reply_unclassified"
        );
    }
}
