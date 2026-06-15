use std::cell::RefCell;

use html5ever::Attribute;
use html5ever::tendril::StrTendril;
use html5ever::tokenizer::{
    BufferQueue, Tag, TagKind, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts,
};
use url::Url;

/// Telegram Bot API HTML parse mode string.
pub const TELEGRAM_PARSE_MODE_HTML: &str = "HTML";

const TAG_ALIASES: &[(&str, &str)] = &[
    ("b", "b"),
    ("strong", "b"),
    ("i", "i"),
    ("em", "i"),
    ("u", "u"),
    ("ins", "u"),
    ("s", "s"),
    ("strike", "s"),
    ("del", "s"),
    ("pre", "pre"),
    ("a", "a"),
    ("span", "span"),
    ("tg-spoiler", "tg-spoiler"),
    ("tg-emoji", "tg-emoji"),
    ("code", "code"),
    ("blockquote", "blockquote"),
];

const KNOWN_HTML_TAG_NAMES: &[&str] = &[
    "a",
    "b",
    "br",
    "blockquote",
    "code",
    "del",
    "em",
    "hr",
    "i",
    "ins",
    "pre",
    "s",
    "span",
    "strike",
    "strong",
    "tg-emoji",
    "tg-spoiler",
    "u",
];

/// Sanitize text for Telegram HTML parse mode.
pub fn sanitize_telegram_html(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let sink = TelegramHtmlSink::default();
    let tokenizer = Tokenizer::new(sink, TokenizerOpts::default());
    let buffer = BufferQueue::default();
    buffer.push_back(StrTendril::from(trimmed));
    let _ = tokenizer.feed(&buffer);
    tokenizer.end();
    tokenizer.sink.finish()
}

/// Return true when input is already valid under the sanitizer policy.
pub fn is_valid_telegram_html(input: &str) -> bool {
    sanitize_telegram_html(input) == input.trim()
}

/// Strip Telegram HTML while preserving decoded visible text.
pub fn strip_telegram_html(input: &str) -> String {
    let sanitized = sanitize_telegram_html(input);
    if sanitized.is_empty() {
        return String::new();
    }

    let sink = VisibleTextSink::default();
    let tokenizer = Tokenizer::new(sink, TokenizerOpts::default());
    let buffer = BufferQueue::default();
    buffer.push_back(StrTendril::from(sanitized.as_str()));
    let _ = tokenizer.feed(&buffer);
    tokenizer.end();
    tokenizer.sink.finish()
}

/// Decode HTML entities without applying Telegram tag sanitization.
pub fn decode_html_entities(input: &str) -> String {
    if input.is_empty() {
        return String::new();
    }

    html_escape::decode_html_entities(input).into_owned()
}

pub fn extract_visible_text(text: &str, parse_mode: &str) -> String {
    let cleaned = ensure_telegram_safe_text(text);
    if parse_mode == TELEGRAM_PARSE_MODE_HTML {
        clean_unicode_non_printables(&strip_telegram_html(&cleaned))
    } else {
        clean_unicode_non_printables(&cleaned)
    }
}

pub fn ensure_telegram_safe_text(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    clean_unicode_non_printables(text).replace(
        ['\0', '\u{8}', '\u{b}', '\u{c}', '\u{e}', '\u{f}', '\u{7f}'],
        "",
    )
}

pub fn clean_unicode_non_printables(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }

    let mut result = String::with_capacity(text.len());
    let mut pending_space = false;
    for ch in text.chars() {
        let Some(ch) = clean_unicode_char(ch) else {
            continue;
        };
        if ch == ' ' || ch == '\t' {
            pending_space = true;
            continue;
        }
        if pending_space {
            result.push(' ');
            pending_space = false;
        }
        result.push(ch);
    }
    result.trim().to_owned()
}

pub fn split_telegram_text(text: &str, parse_mode: &str, max_len: usize) -> Vec<String> {
    if max_len == 0 || text.len() <= max_len {
        return vec![text.to_owned()];
    }
    if parse_mode == TELEGRAM_PARSE_MODE_HTML {
        split_html(text, max_len)
    } else {
        split_plain(text, max_len)
    }
}

/// Escape visible text for Telegram HTML.
pub fn escape_telegram_html_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    out
}

#[derive(Default)]
struct TelegramHtmlSink {
    state: RefCell<TelegramHtmlSanitizer>,
}

impl TelegramHtmlSink {
    fn finish(self) -> String {
        self.state.into_inner().finish()
    }
}

impl TokenSink for TelegramHtmlSink {
    type Handle = ();

    fn process_token(&self, token: Token, _line_number: u64) -> TokenSinkResult<Self::Handle> {
        self.state.borrow_mut().handle_token(token);
        TokenSinkResult::Continue
    }
}

#[derive(Default)]
struct VisibleTextSink {
    text: RefCell<String>,
}

impl VisibleTextSink {
    fn finish(self) -> String {
        self.text.into_inner().trim().to_owned()
    }
}

impl TokenSink for VisibleTextSink {
    type Handle = ();

    fn process_token(&self, token: Token, _line_number: u64) -> TokenSinkResult<Self::Handle> {
        match token {
            Token::CharacterTokens(text) => self.text.borrow_mut().push_str(&text),
            Token::NullCharacterToken => self.text.borrow_mut().push('\0'),
            _ => {}
        }
        TokenSinkResult::Continue
    }
}

#[derive(Default)]
struct TelegramHtmlSanitizer {
    out: String,
    stack: Vec<String>,
    skip_depth: usize,
}

impl TelegramHtmlSanitizer {
    fn finish(mut self) -> String {
        close_html_tags(&mut self.out, &self.stack);
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
        let raw_tag = token.name.as_ref();
        if self.skip_depth > 0 {
            if !token.self_closing && is_skipped_content_tag(raw_tag) {
                self.skip_depth += 1;
            }
            return;
        }
        if is_skipped_content_tag(raw_tag) {
            if !token.self_closing {
                self.skip_depth = 1;
            }
            return;
        }
        let Some((tag, attrs)) = sanitize_tag(&token) else {
            return;
        };
        write_start_tag(&mut self.out, tag, &attrs);
        if token.self_closing {
            write_end_tag(&mut self.out, tag);
        } else {
            self.stack.push(tag.to_owned());
        }
    }

    fn handle_text(&mut self, text: &str) {
        if self.skip_depth == 0 {
            self.out.push_str(&escape_telegram_html_text(text));
        }
    }

    fn handle_end(&mut self, token: &Tag) {
        let raw_tag = token.name.as_ref();
        if self.skip_depth > 0 {
            if is_skipped_content_tag(raw_tag) {
                self.skip_depth -= 1;
            }
            return;
        }
        let Some(tag) = normalize_tag(raw_tag) else {
            return;
        };
        let Some(idx) = last_stack_index(&self.stack, tag) else {
            return;
        };
        for open in self.stack[idx..].iter().rev() {
            write_end_tag(&mut self.out, open);
        }
        self.stack.truncate(idx);
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CleanAttr {
    pub(crate) key: &'static str,
    pub(crate) value: Option<String>,
}

fn sanitize_tag(token: &Tag) -> Option<(&'static str, Vec<CleanAttr>)> {
    let tag = normalize_tag(token.name.as_ref())?;
    match tag {
        "a" => telegram_attr(token, "href", true, clean_href).map(|attrs| (tag, attrs)),
        "span" => {
            telegram_attr(token, "class", true, clean_spoiler_class).map(|attrs| (tag, attrs))
        }
        "tg-emoji" => {
            telegram_attr(token, "emoji-id", true, clean_emoji_id).map(|attrs| (tag, attrs))
        }
        "code" => Some((
            tag,
            telegram_attr(token, "class", false, clean_code_class).unwrap_or_default(),
        )),
        "blockquote" => Some((
            tag,
            telegram_flag_attr(token, "expandable").unwrap_or_default(),
        )),
        _ => Some((tag, Vec::new())),
    }
}

pub(crate) fn telegram_attr(
    token: &Tag,
    key: &'static str,
    required: bool,
    clean: fn(&str) -> Option<String>,
) -> Option<Vec<CleanAttr>> {
    for attr in &token.attrs {
        if !attr_name_eq(attr, key) {
            continue;
        }
        if let Some(value) = clean(attr.value.as_ref()) {
            return Some(vec![CleanAttr {
                key,
                value: Some(value),
            }]);
        }
    }
    if required { None } else { Some(Vec::new()) }
}

pub(crate) fn telegram_flag_attr(token: &Tag, key: &'static str) -> Option<Vec<CleanAttr>> {
    token
        .attrs
        .iter()
        .find(|attr| attr_name_eq(attr, key))
        .map(|_| vec![CleanAttr { key, value: None }])
}

fn clean_href(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let parsed = Url::parse(trimmed).ok()?;
    if is_allowed_scheme(parsed.scheme()) {
        Some(trimmed.to_owned())
    } else {
        None
    }
}

fn clean_spoiler_class(value: &str) -> Option<String> {
    if value.trim() == "tg-spoiler" {
        Some("tg-spoiler".to_owned())
    } else {
        None
    }
}

fn clean_emoji_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if !trimmed.is_empty() && trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        Some(trimmed.to_owned())
    } else {
        None
    }
}

fn clean_code_class(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let lang = trimmed.strip_prefix("language-")?;
    if !lang.is_empty()
        && lang
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        Some(trimmed.to_owned())
    } else {
        None
    }
}

pub(crate) fn attr_name_eq(attr: &Attribute, key: &str) -> bool {
    attr.name.local.as_ref().eq_ignore_ascii_case(key)
}

fn normalize_tag(tag: &str) -> Option<&'static str> {
    let trimmed = tag.trim();
    TAG_ALIASES
        .iter()
        .find(|(alias, _)| trimmed.eq_ignore_ascii_case(alias))
        .map(|(_, normalized)| *normalized)
}

pub(crate) fn is_skipped_content_tag(tag: &str) -> bool {
    let trimmed = tag.trim();
    trimmed.eq_ignore_ascii_case("script")
        || trimmed.eq_ignore_ascii_case("style")
        || trimmed.eq_ignore_ascii_case("template")
}

fn is_allowed_scheme(scheme: &str) -> bool {
    scheme.eq_ignore_ascii_case("http")
        || scheme.eq_ignore_ascii_case("https")
        || scheme.eq_ignore_ascii_case("tg")
}

fn write_start_tag(out: &mut String, tag: &str, attrs: &[CleanAttr]) {
    out.push('<');
    out.push_str(tag);
    for attr in attrs {
        out.push(' ');
        out.push_str(attr.key);
        if let Some(value) = &attr.value {
            out.push_str("=\"");
            out.push_str(&escape_html_attr(value));
            out.push('"');
        }
    }
    out.push('>');
}

fn write_end_tag(out: &mut String, tag: &str) {
    out.push_str("</");
    out.push_str(tag);
    out.push('>');
}

fn close_html_tags(out: &mut String, stack: &[String]) {
    for tag in stack.iter().rev() {
        write_end_tag(out, tag);
    }
}

pub(crate) fn last_stack_index(stack: &[String], tag: &str) -> Option<usize> {
    stack.iter().rposition(|open| open == tag)
}

pub(crate) fn escape_html_attr(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

fn split_plain(mut text: &str, max_len: usize) -> Vec<String> {
    let mut out = Vec::new();
    while !text.is_empty() {
        if text.len() <= max_len {
            out.push(text.to_owned());
            break;
        }
        let candidate = truncate_utf8(text, max_len);
        let idx = candidate.rfind([' ', '\n', '\t']);
        let Some(idx) = idx.filter(|idx| *idx > 0) else {
            out.push(candidate.to_owned());
            text = &text[candidate.len()..];
            continue;
        };
        let part = &candidate[..idx];
        out.push(part.to_owned());
        text = &text[part.len()..];
    }
    out
}

fn split_html(text: &str, max_len: usize) -> Vec<String> {
    let mut state = HtmlSplitState::new(max_len);
    let mut i = 0;
    while i < text.len() {
        if state.remaining() <= 0 {
            state.flush();
            continue;
        }
        if text.as_bytes()[i] == b'<' {
            i = state.append_tag_or_text_tail(text, i);
            continue;
        }
        i = state.append_text_run(text, i);
    }
    state.finish()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OpenTag {
    name: String,
    full: String,
}

#[derive(Debug)]
struct HtmlSplitState {
    out: Vec<String>,
    current: String,
    stack: Vec<OpenTag>,
    max_len: usize,
}

impl HtmlSplitState {
    fn new(max_len: usize) -> Self {
        Self {
            out: Vec::new(),
            current: String::new(),
            stack: Vec::new(),
            max_len,
        }
    }

    fn remaining(&self) -> isize {
        self.max_len as isize
            - self.current.len() as isize
            - closing_suffix_len(&self.stack) as isize
    }

    fn flush(&mut self) {
        self.current.push_str(&build_closing_suffix(&self.stack));
        self.out.push(std::mem::take(&mut self.current));
        let prefix = build_reopen_prefix(&self.stack);
        if prefix.len() < self.max_len {
            self.current.push_str(&prefix);
        }
    }

    fn finish(mut self) -> Vec<String> {
        if !self.current.is_empty() {
            self.current.push_str(&build_closing_suffix(&self.stack));
            self.out.push(self.current);
        }
        self.out
    }

    fn append_tag_or_text_tail(&mut self, text: &str, i: usize) -> usize {
        let Some(relative_end) = text[i..].find('>') else {
            return self.append_text_segment(&text[i..], i);
        };
        let token = &text[i..i + relative_end + 1];
        let (name, is_end, is_self) = parse_tag_token(token);
        if token.len() as isize <= self.remaining() {
            self.current.push_str(token);
            self.update_tag_stack(name, is_end, is_self, token);
            return i + token.len();
        }
        self.flush();
        i
    }

    fn append_text_run(&mut self, text: &str, i: usize) -> usize {
        let Some(relative_tag) = text[i..].find('<') else {
            return self.append_text_segment(&text[i..], i);
        };
        self.append_text_segment(&text[i..i + relative_tag], i)
    }

    fn append_text_segment(&mut self, segment: &str, i: usize) -> usize {
        let remaining = self.remaining();
        if segment.len() as isize <= remaining {
            self.current.push_str(segment);
            return i + segment.len();
        }
        let part = slice_text_prefer_boundary(segment, remaining.max(0) as usize);
        self.current.push_str(part);
        self.flush();
        i + part.len()
    }

    fn update_tag_stack(&mut self, name: String, is_end: bool, is_self: bool, token: &str) {
        if is_end {
            if self.stack.last().is_some_and(|open| open.name == name) {
                self.stack.pop();
            }
            return;
        }
        if !is_self {
            self.stack.push(OpenTag {
                name,
                full: token.to_owned(),
            });
        }
    }
}

fn parse_tag_token(token: &str) -> (String, bool, bool) {
    if token.len() < 3 {
        return (String::new(), false, false);
    }
    let mut inner = token[1..token.len() - 1].trim();
    let is_end = inner.starts_with('/');
    if is_end {
        inner = inner[1..].trim();
    }
    let name_end = inner
        .char_indices()
        .find_map(|(idx, ch)| matches!(ch, ' ' | '\t' | '\n' | '/').then_some(idx))
        .unwrap_or(inner.len());
    let name = lower_known_html_tag_name(&inner[..name_end]);
    let is_self = inner.ends_with('/') || is_self_closing_tag(&name);
    (name, is_end, is_self)
}

fn lower_known_html_tag_name(name: &str) -> String {
    KNOWN_HTML_TAG_NAMES
        .iter()
        .find(|known| name.eq_ignore_ascii_case(known))
        .copied()
        .map(str::to_owned)
        .unwrap_or_else(|| name.to_lowercase())
}

fn is_self_closing_tag(name: &str) -> bool {
    matches!(name, "br" | "hr")
}

fn closing_suffix_len(stack: &[OpenTag]) -> usize {
    stack.iter().rev().map(|open| 3 + open.name.len()).sum()
}

fn build_closing_suffix(stack: &[OpenTag]) -> String {
    let mut out = String::new();
    for open in stack.iter().rev() {
        write_end_tag(&mut out, &open.name);
    }
    out
}

fn build_reopen_prefix(stack: &[OpenTag]) -> String {
    let mut out = String::new();
    for open in stack {
        out.push_str(&open.full);
    }
    out
}

fn slice_text_prefer_boundary(value: &str, limit: usize) -> &str {
    if value.len() <= limit {
        return value;
    }
    let mut part = truncate_utf8(value, limit);
    if let Some(start) = entity_tail_start(part) {
        part = &part[..start];
    }
    if let Some(idx) = part.rfind([' ', '\n', '\t']).filter(|idx| *idx > 0) {
        return &part[..idx];
    }
    part
}

fn entity_tail_start(value: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let window_start = bytes.len().saturating_sub(11);
    let amp = bytes[window_start..]
        .iter()
        .rposition(|byte| *byte == b'&')
        .map(|idx| window_start + idx)?;
    let tail = &bytes[amp + 1..];
    if tail.len() <= 10
        && tail
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'#')
    {
        Some(amp)
    } else {
        None
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn clean_unicode_char(ch: char) -> Option<char> {
    match ch {
        '\u{00a0}' | '\u{2000}' | '\u{2001}' | '\u{2002}' | '\u{2003}' | '\u{2004}'
        | '\u{2005}' | '\u{2006}' | '\u{2007}' | '\u{2008}' | '\u{2009}' | '\u{200a}' => Some(' '),
        '\u{200b}' | '\u{200c}' | '\u{200d}' | '\u{200e}' | '\u{200f}' | '\u{feff}' => None,
        _ if ch.is_control() && !matches!(ch, '\t' | '\n' | '\r') => None,
        _ => Some(ch),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TELEGRAM_PARSE_MODE_HTML, clean_unicode_non_printables, decode_html_entities,
        ensure_telegram_safe_text, escape_telegram_html_text, extract_visible_text,
        is_allowed_scheme, is_valid_telegram_html, parse_tag_token, sanitize_telegram_html,
        split_html, split_telegram_text, strip_telegram_html, truncate_utf8,
    };

    #[test]
    fn sanitize_keeps_allowed_tags_and_fixes_nesting() {
        let got = sanitize_telegram_html("<b>bold <i>x</b> y");

        assert_eq!(got, "<b>bold <i>x</i></b> y");
    }

    #[test]
    fn sanitize_drops_unsafe_tags_and_attrs() {
        let got = sanitize_telegram_html(
            r#"<script>alert(1)</script><a href="javascript:bad">bad</a><a href="https://example.com?q=1&x=2" onclick="x">ok</a>"#,
        );

        assert_eq!(
            got,
            r#"bad<a href="https://example.com?q=1&amp;x=2">ok</a>"#
        );
    }

    #[test]
    fn sanitize_keeps_telegram_specific_tags() {
        let got = sanitize_telegram_html(
            r#"<span class="tg-spoiler" style="x">secret</span><tg-spoiler>hidden</tg-spoiler><tg-emoji emoji-id="5368324170671202286" bad="x">x</tg-emoji><pre><code class="language-go">fmt.Println("&")</code></pre>"#,
        );

        assert_eq!(
            got,
            r#"<span class="tg-spoiler">secret</span><tg-spoiler>hidden</tg-spoiler><tg-emoji emoji-id="5368324170671202286">x</tg-emoji><pre><code class="language-go">fmt.Println("&amp;")</code></pre>"#
        );
    }

    #[test]
    fn sanitize_drops_unsafe_code_class() {
        let got = sanitize_telegram_html(
            r#"<pre><code class="language-go bad">fmt.Println(1)</code></pre>"#,
        );

        assert_eq!(got, "<pre><code>fmt.Println(1)</code></pre>");
    }

    #[test]
    fn sanitize_keeps_expandable_blockquote() {
        let got =
            sanitize_telegram_html("<blockquote expandable>line 1\nline 2 & more</blockquote>");

        assert_eq!(
            got,
            "<blockquote expandable>line 1\nline 2 &amp; more</blockquote>"
        );
    }

    #[test]
    fn sanitize_normalizes_synonyms() {
        let got = sanitize_telegram_html(
            "<strong>bold</strong><em>italic</em><ins>under</ins><del>gone</del>",
        );

        assert_eq!(got, "<b>bold</b><i>italic</i><u>under</u><s>gone</s>");
    }

    #[test]
    fn sanitize_preserves_entity_meaning() {
        let got = sanitize_telegram_html("5 &lt; 6 &amp; ok");

        assert_eq!(got, "5 &lt; 6 &amp; ok");
    }

    #[test]
    fn strip_html_preserves_visible_text() {
        let got = strip_telegram_html("<b>hello</b> <a href=\"https://example.com\">world</a>");

        assert_eq!(got, "hello world");
    }

    #[test]
    fn decode_html_entities_matches_go_unescape_step() {
        assert_eq!(
            decode_html_entities("<b>Tom &amp; Jerry</b> &#39;ok&#39; &#x1f44b;"),
            "<b>Tom & Jerry</b> 'ok' 👋"
        );
    }

    #[test]
    fn valid_html_requires_sanitizer_stability() {
        assert!(is_valid_telegram_html("<b>ok</b>"));
        assert!(!is_valid_telegram_html("<strong>ok</strong>"));
    }

    #[test]
    fn escape_html_text_matches_go_text_escaper() {
        assert_eq!(
            escape_telegram_html_text("Tom & Jerry < Spike > Tyke"),
            "Tom &amp; Jerry &lt; Spike &gt; Tyke"
        );
    }

    #[test]
    fn clean_unicode_non_printables_matches_go_cleanup() {
        assert_eq!(
            clean_unicode_non_printables("alpha\u{00a0}\u{2000} beta\t\tgamma\u{200b}\nline"),
            "alpha beta gamma\nline"
        );
    }

    #[test]
    fn ensure_telegram_safe_text_removes_control_characters() {
        assert_eq!(
            ensure_telegram_safe_text("hello\0\u{8}\u{b}\u{c}\u{e}\u{f}\u{7f}world"),
            "helloworld"
        );
    }

    #[test]
    fn extract_visible_text_matches_html_mode() {
        assert_eq!(
            extract_visible_text("<b>hello</b>", TELEGRAM_PARSE_MODE_HTML),
            "hello"
        );
        assert_eq!(
            extract_visible_text("<b></b>", TELEGRAM_PARSE_MODE_HTML),
            ""
        );
    }

    #[test]
    fn split_html_keeps_chunks_valid_and_preserves_visible_text() {
        let parts = split_html("<b>hello brave new world</b>", 16);

        assert!(parts.len() > 1);
        for part in &parts {
            assert!(part.len() <= 16, "oversized chunk: {part:?}");
            assert!(is_valid_telegram_html(part), "invalid chunk: {part:?}");
        }
        assert_eq!(
            strip_telegram_html(&parts.join("")),
            "hello brave new world"
        );
    }

    #[test]
    fn split_telegram_text_uses_plain_and_html_paths() {
        assert_eq!(
            split_telegram_text("hello brave new world", "", 12),
            vec!["hello brave".to_owned(), " new world".to_owned()]
        );
        assert!(
            split_telegram_text("<b>hello brave new world</b>", TELEGRAM_PARSE_MODE_HTML, 16).len()
                > 1
        );
    }

    #[test]
    fn parse_tag_token_normalizes_known_tags() {
        assert_eq!(
            parse_tag_token("<BLOCKQUOTE expandable>"),
            ("blockquote".to_owned(), false, false)
        );
        assert_eq!(
            parse_tag_token("</TG-SPOILER>"),
            ("tg-spoiler".to_owned(), true, false)
        );
        assert_eq!(parse_tag_token("<BR/>"), ("br".to_owned(), false, true));
    }

    #[test]
    fn truncate_utf8_matches_go_byte_boundary_behavior() {
        assert_eq!(truncate_utf8("Привет мир", 12), "Привет");
        assert_eq!(truncate_utf8("Hello 👋 World 🌍", 10), "Hello 👋");
    }

    #[test]
    fn scheme_policy_matches_go_allowlist() {
        assert!(is_allowed_scheme("HTTPS"));
        assert!(is_allowed_scheme("tg"));
        assert!(!is_allowed_scheme("javascript"));
    }
}
