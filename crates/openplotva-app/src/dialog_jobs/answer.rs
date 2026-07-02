//! Dialog answer preparation: sanitization, deliverability validation, rich
//! detection, and the duplicate-reply guard.

use openplotva_dialog::{DialogOutput, HistoryMessage, ROLE_MODEL, conversation_projection};
use openplotva_telegram::{
    TELEGRAM_PARSE_MODE_HTML, clean_unicode_non_printables, decode_html_entities,
    ensure_telegram_safe_text, sanitize_rich_html, sanitize_telegram_html,
};

/// Answer text of one provider round. Queued generation results deliberately
/// contribute nothing here: the artifact is the reply, and
/// `classify_reply_material` resolves such turns as `Delegated`.
#[must_use]
pub fn dialog_job_answer(output: &DialogOutput) -> String {
    let answer = output.answer.trim();
    if !answer.is_empty() {
        return answer.to_owned();
    }
    output.response.trim().to_owned()
}

#[must_use]
pub fn prepare_dialog_chat_response(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let decoded = decode_html_entities(&strip_code_fences(trimmed));
    if dialog_response_requires_rich(&decoded) {
        let rich = sanitize_rich_html(&decoded).trim().to_owned();
        if dialog_response_requires_rich(&rich) {
            return rich;
        }
        // Rich sanitization can drop the only rich-only construct (e.g. a
        // stray `</li>`), flipping the answer onto the plain send path while
        // rich-only inline tags survive; re-sanitize for the narrower plain
        // tag set so Telegram accepts the send.
        return sanitize_telegram_html(&rich).trim().to_owned();
    }
    sanitize_telegram_html(&decoded).trim().to_owned()
}

/// Mirror of the outbound boundary checks: will `queue_text_message_parts` /
/// `queue_rich_message` accept this answer verbatim? The turn engine verifies
/// deliverability before queueing so rejected answers regenerate instead of
/// failing terminally at send time.
pub fn validate_dialog_answer_deliverable(
    answer: &str,
) -> Result<(), openplotva_telegram::OutboundBuildError> {
    if dialog_response_requires_rich(answer) {
        let html = openplotva_telegram::format_rich_html(answer);
        if html.is_empty() {
            return Err(openplotva_telegram::OutboundBuildError::EmptyText);
        }
        if !openplotva_telegram::rich_message_within_char_limit(&html) {
            return Err(openplotva_telegram::OutboundBuildError::RichMessageTooLong(
                html.chars().count(),
                openplotva_telegram::RICH_MESSAGE_MAX_CHARS,
            ));
        }
        Ok(())
    } else {
        openplotva_telegram::validate_text_message_text(answer, TELEGRAM_PARSE_MODE_HTML)
    }
}

#[must_use]
pub fn dialog_response_requires_rich(value: &str) -> bool {
    // Every tag the rich sanitizer accepts that the plain Telegram HTML
    // sanitizer would strip; answers using them must ride sendRichMessage or
    // the markup (subscripts, highlights, tables, media) is lost.
    const RICH_ONLY_TAGS: &[&str] = &[
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "p",
        "footer",
        "hr",
        "ul",
        "ol",
        "li",
        "table",
        "caption",
        "tr",
        "td",
        "th",
        "details",
        "summary",
        "figure",
        "figcaption",
        "img",
        "video",
        "audio",
        "sub",
        "sup",
        "mark",
        "cite",
        "aside",
        "input",
        "tg-time",
        "tg-math",
        "tg-math-block",
        "tg-reference",
        "tg-slideshow",
        "tg-collage",
        "tg-map",
    ];
    let lower = value.to_ascii_lowercase();
    RICH_ONLY_TAGS
        .iter()
        .any(|tag| html_contains_tag(&lower, tag))
}

fn html_contains_tag(lower_html: &str, tag: &str) -> bool {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    if lower_html.contains(&close) {
        return true;
    }
    let mut search_from = 0;
    while let Some(offset) = lower_html[search_from..].find(&open) {
        let after_tag = search_from + offset + open.len();
        match lower_html.as_bytes().get(after_tag) {
            None | Some(b'>') | Some(b'/') | Some(b' ') | Some(b'\t' | b'\n' | b'\r') => {
                return true;
            }
            Some(_) => {
                search_from = after_tag;
            }
        }
    }
    false
}

fn strip_code_fences(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(body) = strip_triple_code_fence(trimmed) {
        return body;
    }
    if trimmed.starts_with('`') && trimmed.ends_with('`') && trimmed.len() >= 2 {
        return trimmed.trim().trim_matches('`').to_owned();
    }
    trimmed.to_owned()
}

fn strip_triple_code_fence(value: &str) -> Option<String> {
    if !value.starts_with("```") {
        return None;
    }
    let end = value.rfind("```")?;
    if end <= 2 {
        return None;
    }

    let mut body = value[3..end].trim();
    if let Some((head, rest)) = body.split_once('\n')
        && head.len() < 16
        && is_code_fence_language(head)
    {
        body = rest.trim();
    }
    Some(body.trim().to_owned())
}

fn is_code_fence_language(head: &str) -> bool {
    head.chars().all(
        |ch| matches!(ch, ' ' | '\t' | '-' | '_' | '.' | '+' | '0'..='9' | 'A'..='Z' | 'a'..='z'),
    )
}

#[must_use]
pub fn should_suppress_duplicate_bot_reply(
    history: &[HistoryMessage],
    candidate: &str,
) -> (i32, bool) {
    let normalized_candidate = normalize_comparable_chat_text(candidate);
    if normalized_candidate.is_empty() {
        return (0, false);
    }

    for entry in conversation_projection(history) {
        if entry.role != ROLE_MODEL {
            continue;
        }
        let previous = comparable_history_entry_text(&entry);
        if previous.is_empty() {
            continue;
        }
        if previous == normalized_candidate {
            return (entry.message_id, true);
        }
        return (0, false);
    }

    (0, false)
}

#[must_use]
pub fn normalize_comparable_chat_text(text: &str) -> String {
    let text = text.trim();
    if text.is_empty() {
        return String::new();
    }

    collapse_comparable_whitespace(&clean_unicode_non_printables(&decode_html_entities(
        &strip_html_tags_to_spaces(&ensure_telegram_safe_text(text)),
    )))
}

fn comparable_history_entry_text(entry: &HistoryMessage) -> String {
    let original = normalize_comparable_chat_text(&entry.original_text);
    if !original.is_empty() {
        return original;
    }
    normalize_comparable_chat_text(&entry.text)
}

fn strip_html_tags_to_spaces(input: &str) -> String {
    if input.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(input.len());
    let mut idx = 0;
    while idx < input.len() {
        let ch = input[idx..].chars().next().expect("char boundary");
        if ch == '<'
            && let Some(end) = input[idx + 1..].find('>')
            && end > 0
        {
            out.push(' ');
            idx += end + 2;
            continue;
        }
        out.push(ch);
        idx += ch.len_utf8();
    }
    out
}

fn collapse_comparable_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}
