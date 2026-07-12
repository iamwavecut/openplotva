//! Dialog context materialization helpers shared by app-level dialog execution.

use openplotva_core::ChatMessageMeta;
use openplotva_dialog::{DialogInput, HistoryMessage, MultimodalImage};
use openplotva_memory::{RetrievalRequest, RetrievalScope};
use openplotva_taskman::DialogJobParams;

pub const DEFAULT_DIALOG_VISION_DIRECT_IMAGE_LIMIT: usize = 2;
pub const DEFAULT_SHIELD_QUERY_MAX_CHARS: usize = 4000;
pub const DIALOG_MEMORY_CARD_LIMIT: i32 = 12;
pub const DIALOG_MEMORY_EPISODE_LIMIT: i32 = 2;

const ATTACHMENT_KIND_IMAGE: &str = "image";
const ATTACHMENT_SOURCE_MESSAGE: &str = "message";
const ATTACHMENT_SOURCE_QUOTED: &str = "quoted";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogVisionCandidate {
    /// Index in `ChatMessageMeta.attachments`.
    pub attachment_index: usize,
    /// Normalized source, currently `message` or `quoted`.
    pub source: String,
    /// Telegram stable file unique ID.
    pub file_unique_id: String,
    /// Stable label injected into prompt/image context.
    pub label: String,
    /// Normalized attachment kind.
    pub media_kind: String,
}

#[must_use]
pub fn select_dialog_vision_candidates(
    message_id: i32,
    meta: &ChatMessageMeta,
) -> Vec<DialogVisionCandidate> {
    let mut candidates = Vec::with_capacity(meta.attachments.len());
    add_dialog_vision_candidates_for_source(
        &mut candidates,
        message_id,
        meta,
        ATTACHMENT_SOURCE_MESSAGE,
    );
    add_dialog_vision_candidates_for_source(
        &mut candidates,
        message_id,
        meta,
        ATTACHMENT_SOURCE_QUOTED,
    );
    candidates
}

#[must_use]
pub fn vision_attachment_file_id_candidates(
    input_file_id: &str,
    message_id: i32,
    meta: &ChatMessageMeta,
) -> Vec<String> {
    let input = input_file_id.trim();
    let images = indexed_vision_attachments(meta);
    if images.is_empty() {
        return Vec::new();
    }

    for image in &images {
        if matches_vision_attachment_alias(
            input,
            message_id,
            image.index,
            &image.source,
            image.source_index,
        ) {
            return vec![image.file_unique_id.clone()];
        }
    }
    if images.len() == 1 {
        return vec![images[0].file_unique_id.clone()];
    }
    Vec::new()
}

#[must_use]
pub fn dialog_vision_direct_image_limit(configured: Option<i32>) -> usize {
    let limit = configured.unwrap_or(DEFAULT_DIALOG_VISION_DIRECT_IMAGE_LIMIT as i32);
    usize::try_from(limit.max(0)).unwrap_or_default()
}

pub fn update_dialog_vision_attachment_caption(
    meta: &mut ChatMessageMeta,
    index: usize,
    file_unique_id: &str,
    caption: &str,
) -> bool {
    let Some(attachment) = meta.attachments.get_mut(index) else {
        return false;
    };
    if attachment.file_unique_id.trim() != file_unique_id.trim() {
        return false;
    }
    attachment.caption = caption.trim().to_owned();
    true
}

pub fn apply_materialized_dialog_vision_context(
    input: &mut DialogInput,
    meta: ChatMessageMeta,
    captions: &[String],
    direct_images: Vec<MultimodalImage>,
) {
    if !captions.is_empty() {
        let mut meta = meta;
        meta.vision_description = captions.join("\n");
        input.message.meta = meta;
    }
    input.multimodal_images = direct_images;
}

#[must_use]
pub fn dialog_memory_retrieval_request(
    params: &DialogJobParams,
    chat_type: impl Into<String>,
    username: impl Into<String>,
    active_usernames: Vec<String>,
) -> Option<RetrievalRequest> {
    if params.message_text.trim().is_empty() {
        return None;
    }
    Some(RetrievalRequest {
        scope: RetrievalScope {
            chat_id: params.chat_id,
            thread_id: params.thread_id.unwrap_or_default(),
            user_id: params.user_id,
            chat_type: chat_type.into().trim().to_owned(),
            username: username.into().trim().to_owned(),
            active_usernames,
        },
        query: params.message_text.clone(),
        card_limit: DIALOG_MEMORY_CARD_LIMIT,
        episode_limit: DIALOG_MEMORY_EPISODE_LIMIT,
    })
}

#[must_use]
pub fn dialog_reference_context_from_memory(memory_context: &str) -> Vec<String> {
    if memory_context.trim().is_empty() {
        Vec::new()
    } else {
        vec![memory_context.to_owned()]
    }
}

#[must_use]
pub fn build_dialog_shield_query_text(
    params: &DialogJobParams,
    history_messages: &[HistoryMessage],
    max_chars: usize,
    history_tail_messages: usize,
) -> String {
    let mut parts = Vec::new();
    append_shield_query_part(&mut parts, "current", &params.message_text);
    let original = params.original_text.trim();
    if !original.is_empty() && original != params.message_text.trim() {
        append_shield_query_part(&mut parts, "original", original);
    }
    append_dialog_shield_history_tail(
        &mut parts,
        params.message_id,
        history_messages,
        history_tail_messages,
    );

    truncate_dialog_shield_query_text(parts.join("\n").trim(), max_chars)
}

fn add_dialog_vision_candidates_for_source(
    candidates: &mut Vec<DialogVisionCandidate>,
    message_id: i32,
    meta: &ChatMessageMeta,
    source: &str,
) {
    let mut source_count = 0;
    for (idx, attachment) in meta.attachments.iter().enumerate() {
        let media_kind = attachment.kind.trim().to_lowercase();
        if !is_vision_attachment_kind(&media_kind) {
            continue;
        }
        if !attachment.source.trim().eq_ignore_ascii_case(source) {
            continue;
        }
        let file_unique_id = attachment.file_unique_id.trim();
        if file_unique_id.is_empty() {
            continue;
        }

        source_count += 1;
        let label = if source == ATTACHMENT_SOURCE_MESSAGE && message_id > 0 {
            format!("message_{message_id}_{media_kind}_{source_count}")
        } else {
            format!("{source}_{media_kind}_{source_count}")
        };
        candidates.push(DialogVisionCandidate {
            attachment_index: idx,
            source: source.to_owned(),
            file_unique_id: file_unique_id.to_owned(),
            label,
            media_kind,
        });
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IndexedVisionAttachment {
    index: usize,
    source_index: usize,
    source: String,
    file_unique_id: String,
}

fn indexed_vision_attachments(meta: &ChatMessageMeta) -> Vec<IndexedVisionAttachment> {
    let mut images = Vec::with_capacity(meta.attachments.len());
    let mut message_source_count = 0;
    let mut quoted_source_count = 0;
    for attachment in &meta.attachments {
        if !is_vision_attachment_kind(&attachment.kind.trim().to_lowercase()) {
            continue;
        }
        let file_unique_id = attachment.file_unique_id.trim();
        if file_unique_id.is_empty() {
            continue;
        }
        let source = normalize_vision_attachment_source(&attachment.source);
        let mut source_index = 0;
        match source.as_str() {
            ATTACHMENT_SOURCE_MESSAGE => {
                message_source_count += 1;
                source_index = message_source_count;
            }
            ATTACHMENT_SOURCE_QUOTED => {
                quoted_source_count += 1;
                source_index = quoted_source_count;
            }
            _ => {}
        }
        images.push(IndexedVisionAttachment {
            index: images.len() + 1,
            source_index,
            source,
            file_unique_id: file_unique_id.to_owned(),
        });
    }
    images
}

fn is_vision_attachment_kind(kind: &str) -> bool {
    matches!(
        kind,
        ATTACHMENT_KIND_IMAGE | "video" | "animation" | "video_note"
    )
}

fn normalize_vision_attachment_source(source: &str) -> String {
    let source = source.trim();
    if source.eq_ignore_ascii_case(ATTACHMENT_SOURCE_MESSAGE) {
        ATTACHMENT_SOURCE_MESSAGE.to_owned()
    } else if source.eq_ignore_ascii_case(ATTACHMENT_SOURCE_QUOTED) {
        ATTACHMENT_SOURCE_QUOTED.to_owned()
    } else {
        String::new()
    }
}

fn matches_vision_attachment_alias(
    input: &str,
    message_id: i32,
    image_index: usize,
    source: &str,
    source_index: usize,
) -> bool {
    matches_global_vision_attachment_alias(input, image_index)
        || matches_message_scoped_vision_attachment_alias(input, message_id, image_index)
        || matches_source_vision_attachment_alias(input, source, source_index)
}

fn matches_global_vision_attachment_alias(input: &str, image_index: usize) -> bool {
    matches_indexed_alias(input, "image_", image_index)
        || matches_indexed_alias(input, "attachment_", image_index)
        || image_index == 1
            && (input.eq_ignore_ascii_case("image") || input.eq_ignore_ascii_case("attachment"))
}

fn matches_message_scoped_vision_attachment_alias(
    input: &str,
    message_id: i32,
    image_index: usize,
) -> bool {
    message_id > 0
        && (matches_message_image_alias(input, "message_", message_id, image_index)
            || matches_message_image_alias(input, "msg_", message_id, image_index))
}

fn matches_source_vision_attachment_alias(input: &str, source: &str, source_index: usize) -> bool {
    match source {
        ATTACHMENT_SOURCE_MESSAGE => {
            matches_message_source_vision_attachment_alias(input, source_index)
        }
        ATTACHMENT_SOURCE_QUOTED => {
            matches_quoted_source_vision_attachment_alias(input, source_index)
        }
        _ => false,
    }
}

fn matches_message_source_vision_attachment_alias(input: &str, source_index: usize) -> bool {
    matches_indexed_alias(input, "message_image_", source_index)
        || matches_indexed_alias(input, "current_image_", source_index)
        || source_index == 1
            && (input.eq_ignore_ascii_case("message_image")
                || input.eq_ignore_ascii_case("current_image"))
}

fn matches_quoted_source_vision_attachment_alias(input: &str, source_index: usize) -> bool {
    matches_indexed_alias(input, "quoted_image_", source_index)
        || source_index == 1 && input.eq_ignore_ascii_case("quoted_image")
}

fn matches_indexed_alias(input: &str, prefix: &str, want: usize) -> bool {
    if want == 0 || !has_prefix_fold(input, prefix) {
        return false;
    }
    parse_positive_usize(&input[prefix.len()..]).is_some_and(|got| got == want)
}

fn matches_message_image_alias(
    input: &str,
    prefix: &str,
    message_id: i32,
    image_index: usize,
) -> bool {
    if !has_prefix_fold(input, prefix) {
        return false;
    }
    let rest = &input[prefix.len()..];
    let Some((message_part, image_part)) = rest.split_once("_image_") else {
        return false;
    };
    parse_positive_i32(message_part).is_some_and(|got| got == message_id)
        && parse_positive_usize(image_part).is_some_and(|got| got == image_index)
}

fn parse_positive_usize(value: &str) -> Option<usize> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse::<usize>().ok().filter(|value| *value > 0)
}

fn parse_positive_i32(value: &str) -> Option<i32> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse::<i32>().ok().filter(|value| *value > 0)
}

fn has_prefix_fold(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

fn append_dialog_shield_history_tail(
    parts: &mut Vec<String>,
    current_message_id: i32,
    history_messages: &[HistoryMessage],
    history_tail_messages: usize,
) {
    if history_tail_messages == 0 {
        return;
    }

    let mut tail = Vec::with_capacity(history_tail_messages);
    for item in history_messages.iter().rev() {
        if tail.len() >= history_tail_messages {
            break;
        }
        if item.text.trim().is_empty() || item.message_id == current_message_id {
            continue;
        }
        tail.push(item);
    }
    for item in tail.iter().rev() {
        append_shield_query_part(parts, &dialog_shield_history_label(item), &item.text);
    }
}

fn dialog_shield_history_label(item: &HistoryMessage) -> String {
    if item.name.is_empty() {
        return item.role.clone();
    }
    format!("{}:{}", item.role, item.name)
}

fn append_shield_query_part(parts: &mut Vec<String>, label: &str, text: &str) {
    let text = text.trim();
    if text.is_empty() {
        return;
    }
    parts.push(format!("{label}: {text}"));
}

fn truncate_dialog_shield_query_text(query: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return query.to_owned();
    }
    query.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use openplotva_core::ChatAttachment;

    use super::*;

    #[test]
    fn select_dialog_vision_candidates_prioritizes_current_then_quoted() {
        let meta = ChatMessageMeta {
            attachments: vec![
                ChatAttachment {
                    kind: "image".to_owned(),
                    source: "quoted".to_owned(),
                    file_unique_id: "quoted-1".to_owned(),
                    ..ChatAttachment::default()
                },
                ChatAttachment {
                    kind: "audio".to_owned(),
                    source: "message".to_owned(),
                    file_unique_id: "audio-1".to_owned(),
                    ..ChatAttachment::default()
                },
                ChatAttachment {
                    kind: " image ".to_owned(),
                    source: "MESSAGE".to_owned(),
                    file_unique_id: " current-1 ".to_owned(),
                    ..ChatAttachment::default()
                },
                ChatAttachment {
                    kind: "image".to_owned(),
                    source: "quoted".to_owned(),
                    file_unique_id: "quoted-2".to_owned(),
                    ..ChatAttachment::default()
                },
            ],
            ..ChatMessageMeta::default()
        };

        let got = select_dialog_vision_candidates(42, &meta);

        assert_eq!(
            got,
            vec![
                DialogVisionCandidate {
                    attachment_index: 2,
                    source: "message".to_owned(),
                    file_unique_id: "current-1".to_owned(),
                    label: "message_42_image_1".to_owned(),
                    media_kind: "image".to_owned(),
                },
                DialogVisionCandidate {
                    attachment_index: 0,
                    source: "quoted".to_owned(),
                    file_unique_id: "quoted-1".to_owned(),
                    label: "quoted_image_1".to_owned(),
                    media_kind: "image".to_owned(),
                },
                DialogVisionCandidate {
                    attachment_index: 3,
                    source: "quoted".to_owned(),
                    file_unique_id: "quoted-2".to_owned(),
                    label: "quoted_image_2".to_owned(),
                    media_kind: "image".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn vision_attachment_file_id_candidates_match_go_aliases() {
        let meta = ChatMessageMeta {
            attachments: vec![
                ChatAttachment {
                    kind: "image".to_owned(),
                    source: "message".to_owned(),
                    file_unique_id: "current-1".to_owned(),
                    ..ChatAttachment::default()
                },
                ChatAttachment {
                    kind: "image".to_owned(),
                    source: "quoted".to_owned(),
                    file_unique_id: "quoted-1".to_owned(),
                    ..ChatAttachment::default()
                },
                ChatAttachment {
                    kind: "image".to_owned(),
                    source: "message".to_owned(),
                    file_unique_id: "current-2".to_owned(),
                    ..ChatAttachment::default()
                },
                ChatAttachment {
                    kind: "audio".to_owned(),
                    source: "message".to_owned(),
                    file_unique_id: "audio-1".to_owned(),
                    ..ChatAttachment::default()
                },
            ],
            ..ChatMessageMeta::default()
        };

        assert_eq!(
            vision_attachment_file_id_candidates("image_2", 42, &meta),
            vec!["quoted-1"]
        );
        assert_eq!(
            vision_attachment_file_id_candidates("attachment_3", 42, &meta),
            vec!["current-2"]
        );
        assert_eq!(
            vision_attachment_file_id_candidates("message_42_image_1", 42, &meta),
            vec!["current-1"]
        );
        assert_eq!(
            vision_attachment_file_id_candidates("msg_42_image_3", 42, &meta),
            vec!["current-2"]
        );
        assert_eq!(
            vision_attachment_file_id_candidates("current_image_2", 42, &meta),
            vec!["current-2"]
        );
        assert_eq!(
            vision_attachment_file_id_candidates("quoted_image", 42, &meta),
            vec!["quoted-1"]
        );
        assert!(vision_attachment_file_id_candidates("unknown", 42, &meta).is_empty());
    }

    #[test]
    fn vision_attachment_file_id_candidates_fall_back_to_single_image_like_go() {
        let meta = ChatMessageMeta {
            attachments: vec![ChatAttachment {
                kind: "image".to_owned(),
                source: "message".to_owned(),
                file_unique_id: "only-image".to_owned(),
                ..ChatAttachment::default()
            }],
            ..ChatMessageMeta::default()
        };

        assert_eq!(
            vision_attachment_file_id_candidates("anything", 42, &meta),
            vec!["only-image"]
        );
        assert_eq!(
            vision_attachment_file_id_candidates("", 42, &meta),
            vec!["only-image"]
        );
    }

    #[test]
    fn dialog_vision_helpers_match_go_limit_caption_and_input_apply() {
        assert_eq!(dialog_vision_direct_image_limit(None), 2);
        assert_eq!(dialog_vision_direct_image_limit(Some(0)), 0);
        assert_eq!(dialog_vision_direct_image_limit(Some(-3)), 0);
        assert_eq!(dialog_vision_direct_image_limit(Some(4)), 4);

        let mut meta = ChatMessageMeta {
            attachments: vec![ChatAttachment {
                file_unique_id: "file-1".to_owned(),
                ..ChatAttachment::default()
            }],
            ..ChatMessageMeta::default()
        };
        assert!(update_dialog_vision_attachment_caption(
            &mut meta, 0, " file-1 ", " cat "
        ));
        assert_eq!(meta.attachments[0].caption, "cat");
        assert!(!update_dialog_vision_attachment_caption(
            &mut meta, 0, "other", "dog"
        ));

        let mut input = DialogInput::default();
        apply_materialized_dialog_vision_context(
            &mut input,
            meta,
            &[
                "message_1_image_1: cat".to_owned(),
                "quoted_image_1: dog".to_owned(),
            ],
            vec![MultimodalImage {
                file_unique_id: "file-1".to_owned(),
                source: "message".to_owned(),
                label: "message_1_image_1".to_owned(),
                caption: "cat".to_owned(),
                data_url: "data:image/jpeg;base64,abc".to_owned(),
            }],
        );
        assert_eq!(
            input.message.meta.vision_description,
            "message_1_image_1: cat\nquoted_image_1: dog"
        );
        assert_eq!(input.multimodal_images.len(), 1);
    }

    #[test]
    fn build_dialog_shield_query_text_omits_history_when_tail_disabled() {
        let params = DialogJobParams {
            message_id: 10,
            message_text: "Плотва".to_owned(),
            ..dialog_params()
        };
        let got = build_dialog_shield_query_text(
            &params,
            &[HistoryMessage {
                message_id: 8,
                role: "user".to_owned(),
                text: "я хочу умереть".to_owned(),
                ..HistoryMessage::default()
            }],
            DEFAULT_SHIELD_QUERY_MAX_CHARS,
            0,
        );

        assert!(got.contains("current: Плотва"));
        assert!(!got.contains("хочу умереть"));
    }

    #[test]
    fn dialog_memory_request_and_reference_context_match_go_shape() {
        let params = DialogJobParams {
            chat_id: -100,
            thread_id: Some(7),
            user_id: 42,
            message_text: " remember this ".to_owned(),
            ..dialog_params()
        };

        let request = dialog_memory_retrieval_request(
            &params,
            " supergroup ",
            " plotva_chat ",
            vec!["ada".to_owned(), "bob".to_owned()],
        )
        .expect("memory request");

        assert_eq!(request.scope.chat_id, -100);
        assert_eq!(request.scope.thread_id, 7);
        assert_eq!(request.scope.user_id, 42);
        assert_eq!(request.scope.chat_type, "supergroup");
        assert_eq!(request.scope.username, "plotva_chat");
        assert_eq!(request.scope.active_usernames, vec!["ada", "bob"]);
        assert_eq!(request.query, " remember this ");
        assert_eq!(request.card_limit, DIALOG_MEMORY_CARD_LIMIT);
        assert_eq!(request.episode_limit, DIALOG_MEMORY_EPISODE_LIMIT);
        assert!(
            dialog_memory_retrieval_request(
                &DialogJobParams {
                    message_text: "   ".to_owned(),
                    ..dialog_params()
                },
                "",
                "",
                Vec::new()
            )
            .is_none()
        );
        assert_eq!(
            dialog_reference_context_from_memory(" memory context "),
            vec![" memory context ".to_owned()]
        );
        assert!(dialog_reference_context_from_memory(" \t ").is_empty());
    }

    #[test]
    fn build_dialog_shield_query_text_can_include_configured_history_tail() {
        let params = DialogJobParams {
            message_id: 10,
            message_text: "да".to_owned(),
            original_text: " да ".to_owned(),
            ..dialog_params()
        };
        let got = build_dialog_shield_query_text(
            &params,
            &[
                HistoryMessage {
                    message_id: 7,
                    role: "user".to_owned(),
                    text: "старое".to_owned(),
                    ..HistoryMessage::default()
                },
                HistoryMessage {
                    message_id: 8,
                    role: "user".to_owned(),
                    name: "Ada".to_owned(),
                    text: "я хочу умереть".to_owned(),
                    ..HistoryMessage::default()
                },
                HistoryMessage {
                    message_id: 10,
                    role: "user".to_owned(),
                    text: "да".to_owned(),
                    ..HistoryMessage::default()
                },
            ],
            DEFAULT_SHIELD_QUERY_MAX_CHARS,
            1,
        );

        assert!(!got.contains("старое"));
        assert!(got.contains("user:Ada: я хочу умереть"));
        assert_eq!(got.matches("да").count(), 1);
    }

    #[test]
    fn build_dialog_shield_query_text_adds_distinct_original_and_truncates_by_chars() {
        let params = DialogJobParams {
            message_id: 10,
            message_text: "abc".to_owned(),
            original_text: "абв".to_owned(),
            ..dialog_params()
        };

        assert_eq!(
            build_dialog_shield_query_text(&params, &[], 200, 0),
            "current: abc\noriginal: абв"
        );
        assert_eq!(
            build_dialog_shield_query_text(&params, &[], 12, 0),
            "current: abc"
        );
    }

    fn dialog_params() -> DialogJobParams {
        DialogJobParams {
            chat_id: 0,
            message_id: 0,
            user_id: 0,
            user_full_name: String::new(),
            message_text: String::new(),
            original_text: String::new(),
            meta: serde_json::Value::Null,
            max_output_tokens: 0,
            thread_id: None,
        }
    }
}
