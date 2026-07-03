//! Provider-neutral dispatch of one parsed tool step to the dialog toolbox.
//!
//! Pure trait dispatch plus argument mapping: invalid or missing arguments
//! come back as failed tool results the model can react to; only toolbox
//! transport failures surface as errors.

use openplotva_core::ChatMessageMeta;

use crate::{
    DialogToolbox, DrawRequest, HistorySearchRequest, HistorySummaryRequest, RatesRequest,
    STEP_CANCEL_DRAWING, STEP_CHAT_HISTORY_SUMMARY, STEP_CRAWL_URL, STEP_CURRENCY_RATES,
    STEP_DRAW_IMAGE, STEP_GENERATE_SONG, STEP_HISTORY_SEARCH, STEP_QUEUE_STATUS,
    STEP_TRANSLATE_TEXT, STEP_VISION_IMAGE, STEP_WEB_SEARCH, STEP_YOUTUBE_SUMMARY, SongRequest,
    ToolContext, ToolResult, ToolStep, ToolboxError, VisionRequest,
};

/// Dispatch one tool step to the toolbox.
pub async fn dispatch_dialog_tool(
    toolbox: &dyn DialogToolbox,
    meta: &ToolContext,
    step: &ToolStep,
) -> Result<ToolResult, ToolboxError> {
    match step.step.as_str() {
        STEP_DRAW_IMAGE => {
            if step.prompt.trim().is_empty() {
                return Ok(ToolResult::failed(
                    "draw_image_prompt_empty",
                    "draw_image prompt is empty",
                ));
            }
            toolbox
                .draw_image(DrawRequest {
                    context: meta.clone(),
                    prompt: step.prompt.clone(),
                    negative_prompt: step.negative_prompt.clone(),
                    aspect_ratio: step.aspect_ratio.clone(),
                    seed: step.seed.clone(),
                })
                .await
        }
        STEP_GENERATE_SONG => {
            if step.topic.trim().is_empty() {
                return Ok(ToolResult::failed(
                    "generate_song_topic_empty",
                    "generate_song topic is empty",
                ));
            }
            toolbox
                .generate_song(SongRequest {
                    context: meta.clone(),
                    topic: step.topic.clone(),
                })
                .await
        }
        STEP_VISION_IMAGE => {
            let Some(file_id) = vision_tool_file_id(&step.file_id, meta) else {
                return Ok(ToolResult::failed(
                    "vision_image_file_empty",
                    "tool protocol error: vision_image file_id is empty",
                ));
            };
            toolbox
                .vision_image(VisionRequest {
                    context: meta.clone(),
                    file_id,
                })
                .await
        }
        STEP_CURRENCY_RATES => {
            toolbox
                .currency_rates(RatesRequest {
                    context: meta.clone(),
                    pairs: step.pairs.clone(),
                })
                .await
        }
        STEP_WEB_SEARCH => {
            let query = non_empty_or(&step.query, &meta.message_text);
            if query.trim().is_empty() {
                return Ok(ToolResult::failed(
                    "web_search_query_empty",
                    "web_search query is empty",
                ));
            }
            toolbox.web_search(query).await
        }
        STEP_CRAWL_URL => {
            if step.url.trim().is_empty() {
                return Ok(ToolResult::failed(
                    "crawl_url_url_empty",
                    "crawl_url url is empty",
                ));
            }
            toolbox.crawl_url(step.url.clone()).await
        }
        STEP_YOUTUBE_SUMMARY => {
            if step.video.trim().is_empty() {
                return Ok(ToolResult::failed(
                    "youtube_summary_video_empty",
                    "youtube_summary video is empty",
                ));
            }
            toolbox.youtube_summary(step.video.clone()).await
        }
        STEP_QUEUE_STATUS => toolbox.queue_status(meta.user_id).await,
        STEP_CANCEL_DRAWING => toolbox.cancel_drawing(meta.user_id, meta.chat_id).await,
        STEP_TRANSLATE_TEXT => {
            if step.text.trim().is_empty() {
                return Ok(ToolResult::failed(
                    "translate_text_empty",
                    "translate_text text is empty",
                ));
            }
            let target_lang = non_empty_or(&step.target_lang, "ru");
            toolbox.translate_text(step.text.clone(), target_lang).await
        }
        STEP_CHAT_HISTORY_SUMMARY => {
            toolbox
                .chat_history_summary(HistorySummaryRequest {
                    context: meta.clone(),
                    window: step.window.clone(),
                    hours: step.hours,
                    message_count: step.message_count,
                    since: step.since.clone(),
                    scope: step.scope.clone(),
                })
                .await
        }
        STEP_HISTORY_SEARCH => {
            if step.query.trim().is_empty() {
                return Ok(ToolResult::failed(
                    "history_search_query_empty",
                    "history_search query is empty",
                ));
            }
            toolbox
                .history_search(HistorySearchRequest {
                    context: meta.clone(),
                    query: step.query.clone(),
                })
                .await
        }
        other => Ok(ToolResult::failed(
            "unsupported_tool",
            format!("unsupported step {other:?}"),
        )),
    }
}

/// Explicit `file_id`, or the single image attached to the trigger message.
fn vision_tool_file_id(file_id: &str, meta: &ToolContext) -> Option<String> {
    let file_id = file_id.trim();
    if !file_id.is_empty() {
        return Some(file_id.to_owned());
    }
    single_current_image_file_id(&meta.message_meta)
}

fn single_current_image_file_id(meta: &ChatMessageMeta) -> Option<String> {
    let mut found = None;
    for attachment in &meta.attachments {
        if !attachment.kind.trim().eq_ignore_ascii_case("image") {
            continue;
        }
        let file_id = attachment.file_unique_id.trim();
        if file_id.is_empty() {
            continue;
        }
        if found.is_some() {
            return None;
        }
        found = Some(file_id.to_owned());
    }
    found
}

fn non_empty_or(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.trim().to_owned()
    } else {
        value.to_owned()
    }
}
