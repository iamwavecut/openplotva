//! Sanitizer for Telegram **Rich Message** HTML (`InputRichMessage.html`, Bot API 10.1).
//!
//! Rich messages use a much larger, closed tag set than classic `parse_mode=HTML`
//! (headings, tables, lists, details, slideshows, media, footnotes, formulas). This
//! module enforces that allowlist, the per-tag attribute policy, HTTPS-only media,
//! void-element serialization, balanced nesting, and the Bot API media/nesting limits.
//!
//! It deliberately stays separate from [`crate::html`] (classic Telegram HTML) and reuses
//! only the shared low-level escaping/stack primitives from it.

use std::cell::RefCell;

use html5ever::tendril::StrTendril;
use html5ever::tokenizer::{
    BufferQueue, Tag, TagKind, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts,
};
use url::Url;

use crate::html::{
    CleanAttr, attr_name_eq, escape_html_attr, escape_telegram_html_text, is_skipped_content_tag,
    last_stack_index,
};

/// Maximum rich-message length in characters (Bot API limit).
pub const RICH_MESSAGE_MAX_CHARS: usize = 32768;
/// Maximum number of media attachments per rich message (Bot API limit).
pub const RICH_MEDIA_MAX: usize = 50;
/// Maximum nesting depth of blocks/formatting (Bot API limit).
const RICH_MAX_NESTING: usize = 16;

const RICH_TAG_ALIASES: &[(&str, &str)] = &[
    ("b", "b"),
    ("strong", "b"),
    ("i", "i"),
    ("em", "i"),
    ("u", "u"),
    ("ins", "u"),
    ("s", "s"),
    ("strike", "s"),
    ("del", "s"),
    ("code", "code"),
    ("mark", "mark"),
    ("sub", "sub"),
    ("sup", "sup"),
    ("tg-spoiler", "tg-spoiler"),
    ("span", "span"),
    ("a", "a"),
    ("tg-emoji", "tg-emoji"),
    ("tg-time", "tg-time"),
    ("tg-math", "tg-math"),
    ("tg-reference", "tg-reference"),
    ("h1", "h1"),
    ("h2", "h2"),
    ("h3", "h3"),
    ("h4", "h4"),
    ("h5", "h5"),
    ("h6", "h6"),
    ("p", "p"),
    ("footer", "footer"),
    ("hr", "hr"),
    ("br", "br"),
    ("pre", "pre"),
    ("ul", "ul"),
    ("ol", "ol"),
    ("li", "li"),
    ("input", "input"),
    ("blockquote", "blockquote"),
    ("aside", "aside"),
    ("cite", "cite"),
    ("table", "table"),
    ("caption", "caption"),
    ("tr", "tr"),
    ("td", "td"),
    ("th", "th"),
    ("details", "details"),
    ("summary", "summary"),
    ("figure", "figure"),
    ("figcaption", "figcaption"),
    ("img", "img"),
    ("video", "video"),
    ("audio", "audio"),
    ("tg-slideshow", "tg-slideshow"),
    ("tg-collage", "tg-collage"),
    ("tg-math-block", "tg-math-block"),
    ("tg-map", "tg-map"),
];

/// Sanitize HTML for the Telegram rich-message `html` field.
pub fn sanitize_rich_html(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let sink = RichHtmlSink::default();
    let tokenizer = Tokenizer::new(sink, TokenizerOpts::default());
    let buffer = BufferQueue::default();
    buffer.push_back(StrTendril::from(trimmed));
    let _ = tokenizer.feed(&buffer);
    tokenizer.end();
    tokenizer.sink.finish()
}

/// Sanitize rich HTML and separate block-level rich elements with Telegram-visible blank lines.
pub fn format_rich_html(input: &str) -> String {
    let html = sanitize_rich_html(input);
    if html.is_empty() {
        return html;
    }
    add_rich_block_spacing(&html)
}

/// Return true when the input already conforms to the rich-message policy.
pub fn is_valid_rich_html(input: &str) -> bool {
    sanitize_rich_html(input) == input.trim()
}

/// Return true when the rendered rich HTML is within the Bot API character limit.
pub fn rich_message_within_char_limit(html: &str) -> bool {
    html.chars().count() <= RICH_MESSAGE_MAX_CHARS
}

#[derive(Default)]
struct RichHtmlSink {
    state: RefCell<RichHtmlSanitizer>,
}

impl RichHtmlSink {
    fn finish(self) -> String {
        self.state.into_inner().finish()
    }
}

impl TokenSink for RichHtmlSink {
    type Handle = ();

    fn process_token(&self, token: Token, _line_number: u64) -> TokenSinkResult<Self::Handle> {
        self.state.borrow_mut().handle_token(token);
        TokenSinkResult::Continue
    }
}

#[derive(Default)]
struct RichHtmlSanitizer {
    out: String,
    stack: Vec<String>,
    skip_depth: usize,
    media_count: usize,
}

impl RichHtmlSanitizer {
    fn finish(mut self) -> String {
        for tag in self.stack.iter().rev() {
            write_close_tag(&mut self.out, tag);
        }
        self.out.trim().to_owned()
    }

    fn handle_token(&mut self, token: Token) {
        match token {
            Token::TagToken(tag) if tag.kind == TagKind::StartTag => self.handle_start(tag),
            Token::TagToken(tag) if tag.kind == TagKind::EndTag => self.handle_end(&tag),
            Token::CharacterTokens(text) => self.handle_text(&text),
            Token::NullCharacterToken => self.handle_text("\0"),
            _ => {}
        }
    }

    fn handle_start(&mut self, token: Tag) {
        let raw = token.name.as_ref();
        if self.skip_depth > 0 {
            if !token.self_closing && is_skipped_content_tag(raw) {
                self.skip_depth += 1;
            }
            return;
        }
        if is_skipped_content_tag(raw) {
            if !token.self_closing {
                self.skip_depth = 1;
            }
            return;
        }
        let Some((tag, attrs, self_closed)) = sanitize_rich_tag(&token) else {
            return;
        };
        let is_media = matches!(tag, "img" | "video" | "audio");
        if is_media && self.media_count >= RICH_MEDIA_MAX {
            return;
        }
        if !self_closed && self.stack.len() >= RICH_MAX_NESTING {
            return;
        }
        if is_media {
            self.media_count += 1;
        }
        if self_closed {
            write_void_tag(&mut self.out, tag, &attrs);
        } else {
            write_open_tag(&mut self.out, tag, &attrs);
            self.stack.push(tag.to_owned());
        }
    }

    fn handle_text(&mut self, text: &str) {
        if self.skip_depth == 0 {
            self.out.push_str(&escape_telegram_html_text(text));
        }
    }

    fn handle_end(&mut self, token: &Tag) {
        let raw = token.name.as_ref();
        if self.skip_depth > 0 {
            if is_skipped_content_tag(raw) {
                self.skip_depth -= 1;
            }
            return;
        }
        let Some(tag) = normalize_rich_tag(raw) else {
            return;
        };
        let Some(idx) = last_stack_index(&self.stack, tag) else {
            return;
        };
        for open in self.stack[idx..].iter().rev() {
            write_close_tag(&mut self.out, open);
        }
        self.stack.truncate(idx);
    }
}

fn sanitize_rich_tag(token: &Tag) -> Option<(&'static str, Vec<CleanAttr>, bool)> {
    let tag = normalize_rich_tag(token.name.as_ref())?;
    let always_void = matches!(tag, "img" | "br" | "hr" | "input" | "tg-map");
    let self_closed = always_void || (matches!(tag, "video" | "audio") && token.self_closing);
    let attrs: Vec<CleanAttr> = match tag {
        "a" => {
            let mut attrs = Vec::new();
            if let Some(href) = cleaned(token, "href", clean_rich_href) {
                attrs.push(href);
            }
            if let Some(name) = cleaned(token, "name", clean_token) {
                attrs.push(name);
            }
            if attrs.is_empty() {
                return None;
            }
            attrs
        }
        "span" => vec![cleaned(token, "class", clean_spoiler_class)?],
        "code" => cleaned(token, "class", clean_code_class)
            .into_iter()
            .collect(),
        "tg-emoji" => vec![cleaned(token, "emoji-id", clean_emoji_id)?],
        "tg-time" => {
            let mut attrs = vec![cleaned(token, "unix", clean_int)?];
            if let Some(format) = cleaned(token, "format", clean_format) {
                attrs.push(format);
            }
            attrs
        }
        "tg-reference" => vec![cleaned(token, "name", clean_token)?],
        "tg-map" => {
            let mut attrs = vec![
                cleaned(token, "lat", clean_float)?,
                cleaned(token, "long", clean_float)?,
            ];
            if let Some(zoom) = cleaned(token, "zoom", clean_zoom) {
                attrs.push(zoom);
            }
            attrs
        }
        "img" => {
            let mut attrs = vec![cleaned(token, "src", clean_img_src)?];
            if let Some(alt) = cleaned(token, "alt", clean_alt) {
                attrs.push(alt);
            }
            if let Some(spoiler) = flag(token, "tg-spoiler") {
                attrs.push(spoiler);
            }
            attrs
        }
        "video" => {
            let mut attrs = vec![cleaned(token, "src", clean_media_src)?];
            if let Some(spoiler) = flag(token, "tg-spoiler") {
                attrs.push(spoiler);
            }
            attrs
        }
        "audio" => vec![cleaned(token, "src", clean_media_src)?],
        "ol" => {
            let mut attrs = Vec::new();
            if let Some(start) = cleaned(token, "start", clean_int) {
                attrs.push(start);
            }
            if let Some(kind) = cleaned(token, "type", clean_list_type) {
                attrs.push(kind);
            }
            if let Some(reversed) = flag(token, "reversed") {
                attrs.push(reversed);
            }
            attrs
        }
        "li" => {
            let mut attrs = Vec::new();
            if let Some(value) = cleaned(token, "value", clean_int) {
                attrs.push(value);
            }
            if let Some(kind) = cleaned(token, "type", clean_list_type) {
                attrs.push(kind);
            }
            attrs
        }
        "input" => {
            cleaned(token, "type", clean_checkbox_type)?;
            let mut attrs = vec![CleanAttr {
                key: "type",
                value: Some("checkbox".to_owned()),
            }];
            if let Some(checked) = flag(token, "checked") {
                attrs.push(checked);
            }
            attrs
        }
        "table" => {
            let mut attrs = Vec::new();
            if let Some(bordered) = flag(token, "bordered") {
                attrs.push(bordered);
            }
            if let Some(striped) = flag(token, "striped") {
                attrs.push(striped);
            }
            attrs
        }
        "td" | "th" => {
            let mut attrs = Vec::new();
            if let Some(colspan) = cleaned(token, "colspan", clean_int) {
                attrs.push(colspan);
            }
            if let Some(rowspan) = cleaned(token, "rowspan", clean_int) {
                attrs.push(rowspan);
            }
            if let Some(align) = cleaned(token, "align", clean_align) {
                attrs.push(align);
            }
            if let Some(valign) = cleaned(token, "valign", clean_valign) {
                attrs.push(valign);
            }
            attrs
        }
        "details" => flag(token, "open").into_iter().collect(),
        _ => Vec::new(),
    };
    Some((tag, attrs, self_closed))
}

fn cleaned(token: &Tag, key: &'static str, clean: fn(&str) -> Option<String>) -> Option<CleanAttr> {
    token
        .attrs
        .iter()
        .find(|attr| attr_name_eq(attr, key))
        .and_then(|attr| clean(attr.value.as_ref()))
        .map(|value| CleanAttr {
            key,
            value: Some(value),
        })
}

fn flag(token: &Tag, key: &'static str) -> Option<CleanAttr> {
    token
        .attrs
        .iter()
        .any(|attr| attr_name_eq(attr, key))
        .then(|| CleanAttr { key, value: None })
}

fn normalize_rich_tag(tag: &str) -> Option<&'static str> {
    let trimmed = tag.trim();
    RICH_TAG_ALIASES
        .iter()
        .find(|(alias, _)| trimmed.eq_ignore_ascii_case(alias))
        .map(|(_, normalized)| *normalized)
}

fn clean_rich_href(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(fragment) = trimmed.strip_prefix('#') {
        return (is_token(fragment)).then(|| trimmed.to_owned());
    }
    let parsed = Url::parse(trimmed).ok()?;
    matches!(parsed.scheme(), "http" | "https" | "tg" | "mailto" | "tel")
        .then(|| trimmed.to_owned())
}

fn clean_media_src(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let parsed = Url::parse(trimmed).ok()?;
    matches!(parsed.scheme(), "http" | "https").then(|| trimmed.to_owned())
}

fn clean_img_src(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let parsed = Url::parse(trimmed).ok()?;
    matches!(parsed.scheme(), "http" | "https" | "tg").then(|| trimmed.to_owned())
}

fn clean_spoiler_class(value: &str) -> Option<String> {
    (value.trim() == "tg-spoiler").then(|| "tg-spoiler".to_owned())
}

fn clean_code_class(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let lang = trimmed.strip_prefix("language-")?;
    (!lang.is_empty()
        && lang
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'))
    .then(|| trimmed.to_owned())
}

fn clean_emoji_id(value: &str) -> Option<String> {
    clean_int(value)
}

fn clean_int(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty() && trimmed.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| trimmed.to_owned())
}

fn clean_float(value: &str) -> Option<String> {
    let trimmed = value.trim();
    trimmed.parse::<f64>().ok().map(|_| trimmed.to_owned())
}

fn clean_zoom(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let zoom = trimmed.parse::<i64>().ok()?;
    (13..=20).contains(&zoom).then(|| trimmed.to_owned())
}

fn clean_align(value: &str) -> Option<String> {
    let trimmed = value.trim().to_ascii_lowercase();
    matches!(trimmed.as_str(), "left" | "center" | "right").then_some(trimmed)
}

fn clean_valign(value: &str) -> Option<String> {
    let trimmed = value.trim().to_ascii_lowercase();
    matches!(trimmed.as_str(), "top" | "middle" | "bottom").then_some(trimmed)
}

fn clean_list_type(value: &str) -> Option<String> {
    let trimmed = value.trim();
    matches!(trimmed, "1" | "a" | "A" | "i" | "I").then(|| trimmed.to_owned())
}

fn clean_format(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty() && trimmed.bytes().all(|byte| byte.is_ascii_alphabetic()))
        .then(|| trimmed.to_owned())
}

fn clean_checkbox_type(value: &str) -> Option<String> {
    value
        .trim()
        .eq_ignore_ascii_case("checkbox")
        .then(|| "checkbox".to_owned())
}

fn clean_alt(value: &str) -> Option<String> {
    Some(value.to_owned())
}

fn clean_token(value: &str) -> Option<String> {
    let trimmed = value.trim();
    is_token(trimmed).then(|| trimmed.to_owned())
}

fn is_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn write_open_tag(out: &mut String, tag: &str, attrs: &[CleanAttr]) {
    out.push('<');
    out.push_str(tag);
    write_attrs(out, attrs);
    out.push('>');
}

fn write_void_tag(out: &mut String, tag: &str, attrs: &[CleanAttr]) {
    out.push('<');
    out.push_str(tag);
    write_attrs(out, attrs);
    out.push_str("/>");
}

fn write_attrs(out: &mut String, attrs: &[CleanAttr]) {
    for attr in attrs {
        out.push(' ');
        out.push_str(attr.key);
        if let Some(value) = &attr.value {
            out.push_str("=\"");
            out.push_str(&escape_html_attr(value));
            out.push('"');
        }
    }
}

fn write_close_tag(out: &mut String, tag: &str) {
    out.push_str("</");
    out.push_str(tag);
    out.push('>');
}

const SPACED_RICH_BLOCK_TAGS: &[&str] = &[
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "p",
    "footer",
    "blockquote",
    "details",
    "figure",
    "table",
    "ul",
    "ol",
    "audio",
    "video",
    "tg-slideshow",
    "tg-collage",
    "tg-math-block",
    "tg-map",
];
const SPACED_RICH_VOID_BLOCK_TAGS: &[&str] = &["hr"];

fn add_rich_block_spacing(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut i = 0;
    while i < html.len() {
        let Some((tag_end, closes_spaced_block)) = rich_tag_boundary(html, i) else {
            let ch = html[i..].chars().next().expect("valid char boundary");
            out.push(ch);
            i += ch.len_utf8();
            continue;
        };

        out.push_str(&html[i..tag_end]);
        if closes_spaced_block {
            let next = skip_ascii_space(html, tag_end);
            if next < html.len() && !html[next..].starts_with("</") {
                out.push_str("\n\n");
                i = next;
                continue;
            }
        }
        i = tag_end;
    }
    out
}

fn rich_tag_boundary(html: &str, start: usize) -> Option<(usize, bool)> {
    let rest = html.get(start..)?;
    if !rest.starts_with('<') {
        return None;
    }
    let end = start + rest.find('>')? + 1;
    let body = html[start + 1..end - 1].trim();
    if let Some(closing) = body.strip_prefix('/') {
        let tag = closing.trim_start().split_whitespace().next()?;
        return Some((end, SPACED_RICH_BLOCK_TAGS.contains(&tag)));
    }
    let tag = body
        .trim_end_matches('/')
        .trim_end()
        .split_whitespace()
        .next()?;
    Some((
        end,
        body.ends_with('/') && SPACED_RICH_VOID_BLOCK_TAGS.contains(&tag),
    ))
}

fn skip_ascii_space(value: &str, mut index: usize) -> usize {
    while index < value.len() {
        let byte = value.as_bytes()[index];
        if !byte.is_ascii_whitespace() {
            break;
        }
        index += 1;
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_basic_inline_and_normalizes_synonyms() {
        assert_eq!(sanitize_rich_html("<b>x</b>"), "<b>x</b>");
        assert_eq!(sanitize_rich_html("<strong>x</strong>"), "<b>x</b>");
        assert_eq!(sanitize_rich_html("<em>x</em>"), "<i>x</i>");
        assert_eq!(sanitize_rich_html("<ins>x</ins>"), "<u>x</u>");
        assert_eq!(sanitize_rich_html("<del>x</del>"), "<s>x</s>");
        assert_eq!(sanitize_rich_html("<mark>x</mark>"), "<mark>x</mark>");
        assert_eq!(sanitize_rich_html("<sup>2</sup>"), "<sup>2</sup>");
        assert_eq!(
            sanitize_rich_html("<tg-spoiler>x</tg-spoiler>"),
            "<tg-spoiler>x</tg-spoiler>"
        );
    }

    #[test]
    fn drops_unknown_tags_keeps_text_and_strips_script() {
        assert_eq!(sanitize_rich_html("<div>x</div>"), "x");
        assert_eq!(sanitize_rich_html("<script>evil()</script>hi"), "hi");
        assert_eq!(sanitize_rich_html("<style>a{}</style>hi"), "hi");
    }

    #[test]
    fn anchor_policy() {
        assert_eq!(
            sanitize_rich_html(r#"<a href="https://t.me/">l</a>"#),
            r#"<a href="https://t.me/">l</a>"#
        );
        assert_eq!(sanitize_rich_html(r#"<a href="javascript:x()">l</a>"#), "l");
        assert_eq!(
            sanitize_rich_html(r#"<a name="c1"></a>"#),
            r#"<a name="c1"></a>"#
        );
        assert_eq!(
            sanitize_rich_html(r##"<a href="#c1">l</a>"##),
            r##"<a href="#c1">l</a>"##
        );
        assert_eq!(
            sanitize_rich_html(r#"<a href="mailto:a@b.co">m</a>"#),
            r#"<a href="mailto:a@b.co">m</a>"#
        );
    }

    #[test]
    fn media_is_https_only_and_void() {
        assert_eq!(
            sanitize_rich_html(r#"<img src="https://x.test/a.png"/>"#),
            r#"<img src="https://x.test/a.png"/>"#
        );
        // missing slash is normalized to void serialization
        assert_eq!(
            sanitize_rich_html(r#"<img src="https://x.test/a.png">"#),
            r#"<img src="https://x.test/a.png"/>"#
        );
        assert_eq!(sanitize_rich_html(r#"<img src="ftp://x.test/a.png"/>"#), "");
        assert_eq!(sanitize_rich_html("<img/>"), "");
        assert_eq!(
            sanitize_rich_html(r#"<audio src="https://x.test/a.mp3"></audio>"#),
            r#"<audio src="https://x.test/a.mp3"></audio>"#
        );
    }

    #[test]
    fn slideshow_and_figure() {
        assert_eq!(
            sanitize_rich_html(
                r#"<tg-slideshow><img src="https://x.test/a.png"/><img src="https://x.test/b.png"/></tg-slideshow>"#
            ),
            r#"<tg-slideshow><img src="https://x.test/a.png"/><img src="https://x.test/b.png"/></tg-slideshow>"#
        );
        assert_eq!(
            sanitize_rich_html(
                r#"<figure><img src="https://x.test/a.png"/><figcaption>cap<cite>cr</cite></figcaption></figure>"#
            ),
            r#"<figure><img src="https://x.test/a.png"/><figcaption>cap<cite>cr</cite></figcaption></figure>"#
        );
    }

    #[test]
    fn table_policy() {
        assert_eq!(
            sanitize_rich_html(
                r#"<table bordered striped><tr><th>H</th></tr><tr><td align="right" colspan="2">1</td></tr></table>"#
            ),
            r#"<table bordered striped><tr><th>H</th></tr><tr><td colspan="2" align="right">1</td></tr></table>"#
        );
        assert_eq!(
            sanitize_rich_html(r#"<table><tr><td align="bogus">1</td></tr></table>"#),
            "<table><tr><td>1</td></tr></table>"
        );
    }

    #[test]
    fn details_blockquote_lists() {
        assert_eq!(
            sanitize_rich_html("<details open><summary>S</summary>C</details>"),
            "<details open><summary>S</summary>C</details>"
        );
        assert_eq!(
            sanitize_rich_html("<blockquote>q<cite>A</cite></blockquote>"),
            "<blockquote>q<cite>A</cite></blockquote>"
        );
        assert_eq!(
            sanitize_rich_html(r#"<ol start="3" type="a" reversed><li value="7">x</li></ol>"#),
            r#"<ol start="3" type="a" reversed><li value="7">x</li></ol>"#
        );
        assert_eq!(
            sanitize_rich_html(r#"<ul><li><input type="checkbox" checked>done</li></ul>"#),
            r#"<ul><li><input type="checkbox" checked/>done</li></ul>"#
        );
    }

    #[test]
    fn headings_footer_rules_and_breaks() {
        assert_eq!(sanitize_rich_html("<h3>H</h3>"), "<h3>H</h3>");
        assert_eq!(
            sanitize_rich_html("<footer>f</footer>"),
            "<footer>f</footer>"
        );
        assert_eq!(sanitize_rich_html("a<hr/>b"), "a<hr/>b");
        assert_eq!(sanitize_rich_html("a<br>b"), "a<br/>b");
    }

    #[test]
    fn rich_formatter_separates_blocks_with_blank_lines() {
        assert_eq!(
            format_rich_html("<h3>Debug token</h3><p>body</p><footer>footer</footer>"),
            "<h3>Debug token</h3>\n\n<p>body</p>\n\n<footer>footer</footer>"
        );
        assert_eq!(
            format_rich_html("<h3>H</h3>\n<p>P</p><hr/>tail"),
            "<h3>H</h3>\n\n<p>P</p>\n\n<hr/>\n\ntail"
        );
        assert_eq!(
            format_rich_html(
                r#"<tg-collage><img src="https://x.test/a.png"/><img src="https://x.test/b.png"/></tg-collage><p>done</p>"#
            ),
            "<tg-collage><img src=\"https://x.test/a.png\"/><img src=\"https://x.test/b.png\"/></tg-collage>\n\n<p>done</p>"
        );
    }

    #[test]
    fn tg_emoji_requires_numeric_id() {
        assert_eq!(
            sanitize_rich_html(r#"<tg-emoji emoji-id="123">x</tg-emoji>"#),
            r#"<tg-emoji emoji-id="123">x</tg-emoji>"#
        );
        assert_eq!(
            sanitize_rich_html(r#"<tg-emoji emoji-id="bad">x</tg-emoji>"#),
            "x"
        );
    }

    #[test]
    fn autocloses_mismatched_nesting() {
        assert_eq!(sanitize_rich_html("<b><i>x</b>"), "<b><i>x</i></b>");
    }

    #[test]
    fn escapes_stray_text_entities() {
        assert_eq!(sanitize_rich_html("5 &lt; 6 &amp; 7"), "5 &lt; 6 &amp; 7");
    }

    #[test]
    fn caps_media_count() {
        let mut input = String::new();
        for i in 0..(RICH_MEDIA_MAX + 5) {
            input.push_str(&format!(r#"<img src="https://x.test/{i}.png"/>"#));
        }
        let output = sanitize_rich_html(&input);
        assert_eq!(output.matches("<img").count(), RICH_MEDIA_MAX);
    }

    #[test]
    fn validity_and_limit_helpers() {
        assert!(is_valid_rich_html("<b>x</b>"));
        assert!(!is_valid_rich_html("<div>x</div>"));
        assert!(rich_message_within_char_limit("short"));
        assert!(!rich_message_within_char_limit(
            &"a".repeat(RICH_MESSAGE_MAX_CHARS + 1)
        ));
    }
}
