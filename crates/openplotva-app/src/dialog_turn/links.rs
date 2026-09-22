use std::collections::BTreeSet;

use openplotva_dialog::{DialogInput, ROLE_TOOL, ROLE_USER, ToolResult};
use quick_xml::{Reader, events::Event};
use serde_json::Value;

use crate::dialog_jobs::prepare_dialog_chat_response;

#[derive(Default)]
pub(super) struct DialogLinks {
    urls: BTreeSet<String>,
}

impl DialogLinks {
    pub(super) fn from_input(input: &DialogInput) -> Self {
        let mut links = Self::default();
        for text in [
            &input.message.text,
            &input.message.original_text,
            &input.message.normalized,
        ] {
            links.collect_text(text);
        }
        for attachment in &input.message.meta.attachments {
            links.collect_text(&attachment.content);
            links.collect_text(&attachment.caption);
        }
        for entry in &input.history {
            if entry.role == ROLE_USER {
                links.collect_text(&entry.text);
                links.collect_text(&entry.original_text);
                for attachment in &entry.meta.attachments {
                    links.collect_text(&attachment.content);
                    links.collect_text(&attachment.caption);
                }
            } else if entry.role == ROLE_TOOL
                && let Some(output) = entry
                    .tool_call
                    .as_ref()
                    .and_then(|call| call.output.as_ref())
                && let Ok(result) = serde_json::from_value::<ToolResult>(output.clone())
            {
                links.record_tool_result(&result);
            }
        }
        links
    }

    pub(super) fn record_tool_result(&mut self, result: &ToolResult) {
        if !matches!(result.status.as_str(), "ok" | "queued") {
            return;
        }
        self.collect_text(&result.message);
        if let Some(data) = &result.data {
            self.collect_value(data);
        }
    }

    fn collect_value(&mut self, value: &Value) {
        match value {
            Value::Object(fields) => {
                for (key, value) in fields {
                    // Search queries echo model input, not a returned source.
                    if key != "query" {
                        self.collect_value(value);
                    }
                }
            }
            Value::Array(items) => {
                for item in items {
                    self.collect_value(item);
                }
            }
            Value::String(text) => {
                let decoded = openplotva_telegram::decode_html_entities(text);
                let trimmed = decoded.trim();
                if self.collect_exact_url(trimmed) {
                    return;
                }
                if let Ok(nested) = serde_json::from_str::<Value>(text) {
                    self.collect_value(&nested);
                } else {
                    self.collect_text(text);
                }
            }
            _ => {}
        }
    }

    fn collect_exact_url(&mut self, value: &str) -> bool {
        if !value.chars().any(char::is_whitespace)
            && url::Url::parse(value)
                .is_ok_and(|url| matches!(url.scheme(), "http" | "https" | "tg" | "mailto" | "tel"))
        {
            self.urls.insert(value.to_owned());
            return true;
        }
        false
    }

    fn collect_text(&mut self, text: &str) {
        let decoded = openplotva_telegram::decode_html_entities(text);
        if self.collect_exact_url(decoded.trim()) {
            return;
        }
        let is_delimiter =
            |ch: char| ch.is_whitespace() || matches!(ch, '<' | '>' | '"' | '\'' | '`' | '\\');
        for part in decoded.split_inclusive(is_delimiter) {
            let quoted = part.ends_with(['"', '\'', '`', '>']);
            let part = part.trim_end_matches(is_delimiter);
            let lower = part.to_ascii_lowercase();
            let Some(start) = ["https://", "http://", "tg://", "mailto:", "tel:"]
                .iter()
                .filter_map(|scheme| lower.find(scheme))
                .min()
            else {
                continue;
            };
            let mut candidate = &part[start..];
            if !quoted {
                candidate = candidate.trim_end_matches(['.', ',', ';', ':', '!', '?']);
                for (open, close) in [('(', ')'), ('[', ']'), ('{', '}')] {
                    while candidate.ends_with(close)
                        && candidate.matches(close).count() > candidate.matches(open).count()
                    {
                        candidate = &candidate[..candidate.len() - close.len_utf8()];
                    }
                }
            }
            if url::Url::parse(candidate).is_ok() {
                self.urls.insert(candidate.to_owned());
            }
        }
    }

    pub(super) fn prepare_response(&self, raw: &str) -> String {
        let html = prepare_dialog_chat_response(raw);
        if !html.contains("<a ") {
            return html;
        }
        let mut reader = Reader::from_str(&html);
        let mut out = String::with_capacity(html.len());
        let mut anchors = Vec::new();
        loop {
            let start = reader.buffer_position() as usize;
            let keep = match reader.read_event() {
                Ok(Event::Eof) => break,
                Ok(Event::Start(tag) | Event::Empty(tag)) if tag.name().as_ref() == b"a" => {
                    let allowed = match tag.try_get_attribute("href") {
                        Ok(Some(attr)) => std::str::from_utf8(&attr.value).is_ok_and(|href| {
                            self.urls
                                .contains(&openplotva_telegram::decode_html_entities(href))
                        }),
                        Ok(None) => true,
                        Err(_) => false,
                    };
                    // Sanitization produces balanced anchors, including an explicit
                    // closing tag for a self-closing anchor.
                    anchors.push(allowed);
                    allowed
                }
                Ok(Event::End(tag)) if tag.name().as_ref() == b"a" => {
                    anchors.pop().unwrap_or(false)
                }
                Ok(_) => true,
                Err(_) => {
                    return openplotva_telegram::escape_telegram_html_text(
                        &openplotva_telegram::strip_telegram_html(&html),
                    );
                }
            };
            if keep {
                out.push_str(&html[start..reader.buffer_position() as usize]);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use openplotva_core::ToolCall;
    use openplotva_dialog::{HistoryMessage, ROLE_MODEL};
    use serde_json::json;

    use super::*;

    #[test]
    fn link_policy_preserves_user_and_tool_sources_but_not_model_history() {
        let mut input = DialogInput::default();
        input.message.original_text =
            "[page](https://source.test/Foo_(bar)), https://source.test/?x=1&y=2".to_owned();
        input.history = vec![
            HistoryMessage {
                role: ROLE_USER.to_owned(),
                text: r#"<a href="tg://user?id=42">person</a>"#.to_owned(),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                role: ROLE_MODEL.to_owned(),
                text: "https://invented.test".to_owned(),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                role: ROLE_TOOL.to_owned(),
                tool_call: Some(ToolCall {
                    output: Some(json!({"status":"ok","data":{"url":"https://tool.test"}})),
                    ..ToolCall::default()
                }),
                ..HistoryMessage::default()
            },
        ];
        let links = DialogLinks::from_input(&input);
        assert_eq!(
            links.prepare_response(r#"<a href="https://source.test/Foo_(bar)">page</a> <a href="https://source.test/?x=1&amp;y=2">query</a> <a href="tg://user?id=42">person</a> <a href="https://tool.test">tool</a> <a href="https://invented.test">fake</a>"#),
            r#"<a href="https://source.test/Foo_(bar)">page</a> <a href="https://source.test/?x=1&amp;y=2">query</a> <a href="tg://user?id=42">person</a> <a href="https://tool.test">tool</a> fake"#,
        );
    }

    #[test]
    fn link_policy_reads_nested_tool_payloads_and_does_not_trust_failed_calls() {
        let mut links = DialogLinks::default();
        links.record_tool_result(&ToolResult {
            status: "ok".to_owned(),
            message: "Details: https://tool.test/details".to_owned(),
            data: Some(json!({"query":"https://query.test", "results":json!({"organic":[{"link":"https://tool.test/page"}]}).to_string()})),
            ..ToolResult::default()
        });
        links.record_tool_result(&ToolResult::failed("unavailable", "https://failed.test"));
        assert_eq!(
            links.prepare_response(r#"<a href="https://tool.test/page">page</a> <a href="https://tool.test/details">details</a> <a href="https://query.test">query</a> <a href="https://failed.test">failed</a>"#),
            r#"<a href="https://tool.test/page">page</a> <a href="https://tool.test/details">details</a> query failed"#,
        );
    }

    #[test]
    fn link_policy_keeps_exact_structured_urls_without_authorizing_trimmed_variants() {
        let mut input = DialogInput::default();
        input.message.text = r#"See <a href="https://user.test/path?)">page</a>"#.to_owned();
        let mut links = DialogLinks::from_input(&input);
        links.record_tool_result(&ToolResult {
            status: "ok".to_owned(),
            data: Some(json!({"url":"https://tool.test/path?"})),
            ..ToolResult::default()
        });
        assert_eq!(
            links.prepare_response(r#"<a href="https://tool.test/path?">source</a> <a href="https://tool.test/path">variant</a>"#),
            r#"<a href="https://tool.test/path?">source</a> variant"#,
        );
        assert_eq!(
            links.prepare_response(r#"<a href="https://user.test/path?)">user</a> <a href="https://user.test/path">variant</a>"#),
            r#"<a href="https://user.test/path?)">user</a> variant"#,
        );
    }

    #[test]
    fn link_policy_unwraps_nested_anchors_without_changing_rich_markup_or_entities() {
        let mut input = DialogInput::default();
        input.message.text = "https://source.test".to_owned();
        let links = DialogLinks::from_input(&input);
        assert_eq!(
            links.prepare_response(r#"<p><a href="https://fake.test"><b>A &amp; B</b> <a href="https://source.test">source</a></a></p><hr/><blockquote expandable>quote</blockquote>"#),
            r#"<p><b>A &amp; B</b> <a href="https://source.test">source</a></p><hr/><blockquote>quote</blockquote>"#,
        );
        assert_eq!(
            links.prepare_response(r##"<a href="https://source.test.evil.test">host</a> <a href="https://source.test/extra">path</a> <a href="#invented">fragment</a>"##),
            "host path fragment",
        );
    }
}
