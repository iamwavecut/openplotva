//! Throwaway probe: score replayed replies with the production finalize path.
//!
//! Reads JSONL rows of {"messages": [...], "content": "..."} on stdin, rebuilds the reply
//! leak guard the way `openplotva_llm::aifarm::reply_leak_guard` does (system and
//! `<chat_context>` messages are protected text, rendered history entries are history),
//! and reports what the bot would have delivered.

use std::io::{self, BufRead, Write};

use openplotva_dialog::{DialogReplyOutcome, DialogReplySuppression, ReplyLeakGuard};

fn inner_text(block: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}");
    let start = block.find(&open)?;
    let after_open = block[start..].find('>')? + start + 1;
    let close = format!("</{tag}>");
    let end = block[after_open..].find(&close)? + after_open;
    Some(block[after_open..end].trim().to_owned())
}

/// Rendered history elements of one prompt message, as (sender, text).
fn history_entries(content: &str) -> Vec<(String, String)> {
    let mut entries = Vec::new();
    let mut offset = 0;
    while let Some(rel) = content[offset..].find("<message ") {
        let start = offset + rel;
        let end = content[start..]
            .find("</message>")
            .map_or(content.len(), |at| start + at);
        let block = &content[start..end];
        let sender = inner_text(block, "user").unwrap_or_default();
        if let Some(text) = inner_text(block, "text") {
            entries.push((sender, text));
        }
        offset = end.max(start + 1);
    }
    entries
}

fn guard_for(messages: &serde_json::Value) -> ReplyLeakGuard {
    let mut builder = ReplyLeakGuard::builder().sender("Плотва");
    for message in messages.as_array().into_iter().flatten() {
        let role = message.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let content = match message.get("content") {
            Some(serde_json::Value::String(text)) => text.clone(),
            Some(serde_json::Value::Array(parts)) => parts
                .iter()
                .filter_map(|part| part.get("text").and_then(|v| v.as_str()))
                .collect::<Vec<_>>()
                .join("\n"),
            _ => String::new(),
        };
        let is_runtime_context =
            role.eq_ignore_ascii_case("user") && content.trim_start().starts_with("<chat_context");
        if role.eq_ignore_ascii_case("system") || is_runtime_context {
            builder = builder.protected_text(&content);
            continue;
        }
        for (sender, text) in history_entries(&content) {
            builder = builder.history_entry(&sender, &text);
        }
    }
    builder.build()
}

fn suppression_name(reason: &DialogReplySuppression) -> String {
    match reason {
        DialogReplySuppression::Empty => "empty".to_owned(),
        DialogReplySuppression::ProtocolOnly => "protocol_only".to_owned(),
        DialogReplySuppression::ContextLeak => "context_leak".to_owned(),
        DialogReplySuppression::TranscriptLeak => "transcript_leak".to_owned(),
        DialogReplySuppression::PromptLeak => "prompt_leak".to_owned(),
        DialogReplySuppression::ReasoningLeak => "reasoning_leak".to_owned(),
        DialogReplySuppression::Pathological(reason) => format!("pathological:{reason}"),
    }
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    for line in stdin.lock().lines() {
        let line = line.expect("read line");
        if line.trim().is_empty() {
            continue;
        }
        let row: serde_json::Value = serde_json::from_str(&line).expect("parse row");
        let content = row.get("content").and_then(|v| v.as_str()).unwrap_or("");
        let guard = guard_for(row.get("messages").unwrap_or(&serde_json::Value::Null));

        let (outcome, delivered) =
            match openplotva_dialog::finalize_dialog_reply_with_guard(content, &guard) {
                DialogReplyOutcome::Reply(text) => ("reply".to_owned(), text),
                DialogReplyOutcome::Suppressed(reason) => (
                    format!("suppressed:{}", suppression_name(&reason)),
                    String::new(),
                ),
            };
        let steps = match openplotva_dialog::parse_assistant_content(content) {
            Ok(parsed) => parsed
                .tool_steps
                .iter()
                .map(|step| step.step.clone())
                .collect::<Vec<_>>(),
            Err(_) => Vec::new(),
        };

        let mut out = row.clone();
        let object = out.as_object_mut().expect("object");
        object.remove("messages");
        object.insert(
            "verdict".to_owned(),
            serde_json::json!({
                "outcome": outcome,
                "delivered_chars": delivered.chars().count(),
                "tool_steps": steps,
                "starts_with_tag": content.trim_start().starts_with('<'),
            }),
        );
        writeln!(stdout, "{out}").expect("write");
    }
}
