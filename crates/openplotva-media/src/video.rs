//! Prompt optimizer for FastFrames text-to-video clips.

use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

use crate::NsfwResult;

pub const VIDEO_OPTIMIZER_PROMPT_NAME: &str = "video/optimizer";
pub const VIDEO_OPTIMIZER_TOOL_NAME: &str = "optimize_video_prompt";

/// One optimized clip prompt with the safety verdict for the request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VideoOptimize {
    /// English prompt for the video model.
    pub prompt: String,
    pub nsfw_result: NsfwResult,
}

#[derive(Debug, Error)]
pub enum VideoOptimizeError {
    #[error("decode video optimizer payload: {0}")]
    Json(#[from] serde_json::Error),
    #[error("video optimizer returned an empty prompt")]
    EmptyPrompt,
}

#[derive(Deserialize)]
struct VideoOptimizePayload {
    #[serde(default)]
    nsfw_result: NsfwResult,
    #[serde(default)]
    prompt: String,
}

pub fn render_video_optimizer_messages(
    request: &str,
    brief: &str,
) -> Result<Vec<openplotva_prompts::PromptMessage>, openplotva_prompts::PromptError> {
    openplotva_prompts::render_messages(
        VIDEO_OPTIMIZER_PROMPT_NAME,
        &video_optimizer_template_data(request, brief),
    )
}

pub fn render_video_optimizer_messages_with(
    prompts: &openplotva_prompts::PromptStore,
    request: &str,
    brief: &str,
) -> Result<Vec<openplotva_prompts::PromptMessage>, openplotva_prompts::PromptError> {
    prompts.render_messages(
        VIDEO_OPTIMIZER_PROMPT_NAME,
        &video_optimizer_template_data(request, brief),
    )
}

/// The brief is shown only when it adds something to the request.
fn video_optimizer_template_data(request: &str, brief: &str) -> Value {
    let request = request.trim();
    let brief = brief.trim();
    let (request, brief) = if request.is_empty() {
        (brief, "")
    } else if brief == request {
        (request, "")
    } else {
        (request, brief)
    };
    json!({ "request": request, "brief": brief })
}

#[must_use]
pub fn video_optimizer_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "input": { "type": "string", "description": "the request, copied" },
            "nsfw_result": { "type": "string", "enum": ["safe", "adult", "forbidden"] },
            "prompt": {
                "type": "string",
                "description": "one English paragraph of 80-120 words for the video model"
            }
        },
        "required": ["input", "nsfw_result", "prompt"],
        "additionalProperties": false
    })
}

pub fn decode_video_optimize_payload(content: &str) -> Result<VideoOptimize, VideoOptimizeError> {
    let payload: VideoOptimizePayload = serde_json::from_str(content.trim())?;
    let prompt = payload.prompt.trim().to_owned();
    if prompt.is_empty() && payload.nsfw_result != NsfwResult::Forbidden {
        return Err(VideoOptimizeError::EmptyPrompt);
    }
    Ok(VideoOptimize {
        prompt,
        nsfw_result: payload.nsfw_result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_the_request_and_a_distinct_brief() {
        let messages = render_video_optimizer_messages(
            "@plotva_bot сделай видео: корги на сёрфе",
            "A corgi surfing a wave at sunset",
        )
        .expect("render");

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "system");
        assert!(messages[0].content.contains("FastFrames"));
        assert!(messages[0].content.contains("\"First ... Then ...\""));
        assert_eq!(messages[1].role, "user");
        assert_eq!(
            messages[1].content,
            "Request: @plotva_bot сделай видео: корги на сёрфе\nBrief from the conversation: A corgi surfing a wave at sunset"
        );
    }

    #[test]
    fn a_brief_equal_to_the_request_or_alone_is_shown_once() {
        for (request, brief) in [("кот и снег", "кот и снег"), ("", "кот и снег")]
        {
            let messages = render_video_optimizer_messages(request, brief).expect("render");
            assert_eq!(messages[1].content, "Request: кот и снег");
        }
    }

    #[test]
    fn decodes_the_prompt_and_verdict() {
        let decoded = decode_video_optimize_payload(
            r#"{"input": "кот", "nsfw_result": "safe", "prompt": " A grey cat yawns. "}"#,
        )
        .expect("decode");
        assert_eq!(
            decoded,
            VideoOptimize {
                prompt: "A grey cat yawns.".to_owned(),
                nsfw_result: NsfwResult::Safe,
            }
        );

        let unknown = decode_video_optimize_payload(r#"{"prompt": "Rain on a window."}"#)
            .expect("decode without a verdict");
        assert_eq!(unknown.nsfw_result, NsfwResult::Adult);
    }

    #[test]
    fn an_empty_prompt_is_an_error_unless_the_request_is_forbidden() {
        assert!(matches!(
            decode_video_optimize_payload(r#"{"nsfw_result": "safe", "prompt": " "}"#),
            Err(VideoOptimizeError::EmptyPrompt)
        ));
        let blocked = decode_video_optimize_payload(r#"{"nsfw_result": "forbidden"}"#)
            .expect("forbidden decodes");
        assert_eq!(blocked.nsfw_result, NsfwResult::Forbidden);
        assert!(decode_video_optimize_payload("not json").is_err());
    }
}
