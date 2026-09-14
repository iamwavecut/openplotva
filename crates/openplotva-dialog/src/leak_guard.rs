//! Per-step fingerprint of everything the model saw but must never echo: the
//! rendered system contract and runtime context (prompt leak) and the chat
//! transcript (transcript leak). Built from the request that was actually
//! sent, so the guard follows prompt changes without a marker list.

use std::collections::{BTreeSet, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};

const PROTECTED_SHINGLE_WORDS: usize = 6;
const REFERENCE_SHINGLE_WORDS: usize = 8;
const HISTORY_SHINGLE_WORDS: usize = 8;
const MIN_EXACT_ECHO_WORDS: usize = 3;
const MIN_ECHOED_HISTORY_ENTRIES: usize = 2;
const MIN_SENDER_LABELLED_LINES: usize = 2;
const MIN_SENDER_LABEL_CHARS: usize = 2;
// Derived prompt tags: snake_case names or long names never occur in a chat
// reply, while short generic ones (`text`, `name`, `rule`) do.
const MIN_DERIVED_TAG_CHARS: usize = 8;

const PROMPT_SCAFFOLDING_TAGS: &[&str] = &[
    "system_contract",
    "tool_contract",
    "dialog_policy",
    "answer_policy",
    "final_check",
    "base_voice",
    "chat_context",
    "reference_context",
    "daily_persona_accent",
    "custom_persona",
    "shield_context",
];

const TRANSCRIPT_SCAFFOLDING_TAGS: &[&str] = &[
    "assistant_message",
    "assistants_message",
    "last_message",
    "message_type",
    "to_user",
];

// `<message>` alone is plausible prose; only the rendered history element with
// its attributes marks a copied transcript.
const MESSAGE_WRAPPER_ATTRIBUTES: &[&str] = &["id=", "thread_id=", "timestamp="];

// Telegram API field names a technical chat may mention in prose.
const GENERIC_IDENTIFIERS: &[&str] = &["chat_id", "file_id", "message_id", "thread_id", "user_id"];

const ALLOWED_HTML_TAGS: &[&str] = &[
    "a",
    "audio",
    "b",
    "blockquote",
    "br",
    "code",
    "del",
    "details",
    "div",
    "em",
    "footer",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    "i",
    "img",
    "ins",
    "li",
    "mark",
    "ol",
    "p",
    "pre",
    "s",
    "section",
    "small",
    "span",
    "strike",
    "strong",
    "sub",
    "summary",
    "sup",
    "table",
    "tbody",
    "td",
    "tg-emoji",
    "tg-spoiler",
    "th",
    "thead",
    "tr",
    "u",
    "ul",
    "video",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DialogLeak {
    /// The reply reproduces the system contract, runtime context or memory.
    Prompt,
    /// The reply reproduces the chat transcript it was shown.
    Transcript,
}

#[derive(Clone, Debug, Default)]
struct HistoryFingerprint {
    exact: Option<u64>,
    shingles: HashSet<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct ReplyLeakGuard {
    protected_shingles: HashSet<u64>,
    reference_shingles: HashSet<u64>,
    protected_tags: BTreeSet<String>,
    history: Vec<HistoryFingerprint>,
    sender_labels: BTreeSet<String>,
}

#[derive(Debug, Default)]
pub struct ReplyLeakGuardBuilder {
    guard: ReplyLeakGuard,
}

impl ReplyLeakGuard {
    #[must_use]
    pub fn builder() -> ReplyLeakGuardBuilder {
        ReplyLeakGuardBuilder::default()
    }

    #[must_use]
    pub fn detect(&self, reply: &str) -> Option<DialogLeak> {
        // Markup or identifiers quoted inside code are prose about them.
        let outside_code = mask_code_spans(reply);
        if let Some(leak) = self.scaffolding_tag_leak(&outside_code) {
            return Some(leak);
        }
        if self.names_internal_identifier(&outside_code) {
            return Some(DialogLeak::Prompt);
        }
        let words = normalize_words(reply);
        if self.has_protected_shingle(&words) {
            return Some(DialogLeak::Prompt);
        }
        if self.echoes_history(&words) || self.has_sender_labelled_lines(reply) {
            return Some(DialogLeak::Transcript);
        }
        None
    }

    // Snake_case element names of the contract or context (`base_voice`,
    // `daily_persona_accent`) are internal identifiers only the prompt knows; a
    // reply that names one is narrating the prompt even without markup.
    // Transcript wrapper names (`message_type`, `to_user`) are ordinary protocol
    // vocabulary a user may ask about, so only their tag form counts.
    fn names_internal_identifier(&self, text: &str) -> bool {
        let lower = text.to_lowercase();
        lower
            .split(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
            .filter(|word| word.contains('_') && !GENERIC_IDENTIFIERS.contains(word))
            .any(|word| {
                PROMPT_SCAFFOLDING_TAGS.contains(&word) || self.protected_tags.contains(word)
            })
    }

    fn scaffolding_tag_leak(&self, reply: &str) -> Option<DialogLeak> {
        for tag in tag_names(reply) {
            if tag.name == "message" {
                if MESSAGE_WRAPPER_ATTRIBUTES
                    .iter()
                    .any(|attr| tag.attributes.contains(attr))
                {
                    return Some(DialogLeak::Transcript);
                }
                continue;
            }
            if TRANSCRIPT_SCAFFOLDING_TAGS.contains(&tag.name.as_str()) {
                return Some(DialogLeak::Transcript);
            }
            if PROMPT_SCAFFOLDING_TAGS.contains(&tag.name.as_str())
                || self.protected_tags.contains(&tag.name)
            {
                return Some(DialogLeak::Prompt);
            }
        }
        None
    }

    fn has_protected_shingle(&self, words: &[String]) -> bool {
        shingles(words, PROTECTED_SHINGLE_WORDS)
            .any(|shingle| self.protected_shingles.contains(&shingle))
            || shingles(words, REFERENCE_SHINGLE_WORDS)
                .any(|shingle| self.reference_shingles.contains(&shingle))
    }

    fn echoes_history(&self, words: &[String]) -> bool {
        if self.history.is_empty() || words.is_empty() {
            return false;
        }
        if words.len() >= MIN_EXACT_ECHO_WORDS {
            let exact = hash_words(words);
            if self.history.iter().any(|entry| entry.exact == Some(exact)) {
                return true;
            }
        }
        let reply_shingles = shingles(words, HISTORY_SHINGLE_WORDS).collect::<HashSet<_>>();
        if reply_shingles.is_empty() {
            return false;
        }
        let echoed = self
            .history
            .iter()
            .filter(|entry| entry.shingles.iter().any(|s| reply_shingles.contains(s)))
            .count();
        echoed >= MIN_ECHOED_HISTORY_ENTRIES
    }

    fn has_sender_labelled_lines(&self, reply: &str) -> bool {
        if self.sender_labels.is_empty() {
            return false;
        }
        let labelled = reply
            .lines()
            .filter(|line| self.line_has_sender_label(line))
            .count();
        labelled >= MIN_SENDER_LABELLED_LINES
    }

    fn line_has_sender_label(&self, line: &str) -> bool {
        let line = line
            .trim_start()
            .trim_start_matches(['-', '*', '>', '•', '[', '('])
            .trim_start();
        let lower = line.to_lowercase();
        self.sender_labels.iter().any(|label| {
            lower
                .strip_prefix(label.as_str())
                .is_some_and(|rest| rest.trim_start().starts_with(':'))
        })
    }
}

impl ReplyLeakGuardBuilder {
    /// Text the model must never reproduce: a system message or the runtime
    /// context message. Memory chunks and persona texts inside it are indexed
    /// with a longer window (see [`LONG_WINDOW_TAGS`]).
    #[must_use]
    pub fn protected_text(mut self, text: &str) -> Self {
        let (protected, reference) = split_long_window_spans(text);
        for tag in tag_names(&protected) {
            if is_derived_prompt_tag(&tag.name) {
                self.guard.protected_tags.insert(tag.name);
            }
        }
        let words = normalize_words(&protected);
        self.guard
            .protected_shingles
            .extend(shingles(&words, PROTECTED_SHINGLE_WORDS));
        for chunk in reference {
            let words = normalize_words(&chunk);
            self.guard
                .reference_shingles
                .extend(shingles(&words, REFERENCE_SHINGLE_WORDS));
        }
        self
    }

    /// A participant name the model may narrate as a speaker label (the bot
    /// itself, or a sender without a text entry).
    #[must_use]
    pub fn sender(mut self, name: &str) -> Self {
        let label = normalize_label(name);
        if label.chars().count() >= MIN_SENDER_LABEL_CHARS {
            self.guard.sender_labels.insert(label);
        }
        self
    }

    /// One transcript entry authored by someone other than the bot, keyed by
    /// its visible sender name. The bot's own past replies only register a
    /// speaker label via [`Self::sender`]: repeating oneself is the loop
    /// problem, not a transcript leak.
    #[must_use]
    pub fn history_entry(mut self, sender: &str, text: &str) -> Self {
        self = self.sender(sender);
        let words = normalize_words(text);
        if words.is_empty() {
            return self;
        }
        self.guard.history.push(HistoryFingerprint {
            exact: (words.len() >= MIN_EXACT_ECHO_WORDS).then(|| hash_words(&words)),
            shingles: shingles(&words, HISTORY_SHINGLE_WORDS).collect(),
        });
        self
    }

    #[must_use]
    pub fn build(self) -> ReplyLeakGuard {
        self.guard
    }
}

fn is_derived_prompt_tag(name: &str) -> bool {
    if ALLOWED_HTML_TAGS.contains(&name) {
        return false;
    }
    name.contains('_') || name.chars().count() >= MIN_DERIVED_TAG_CHARS
}

// Memory chunks are short facts the bot legitimately recalls, and persona
// texts are catchphrase lists it is told to reuse; only a longer verbatim run
// of those is a dump.
const LONG_WINDOW_TAGS: &[&str] = &[
    "reference_context",
    "custom_persona",
    "daily_persona_accent",
];

fn split_long_window_spans(text: &str) -> (String, Vec<String>) {
    let lower = text.to_ascii_lowercase();
    let mut protected = String::with_capacity(text.len());
    let mut spans = Vec::new();
    let mut from = 0;
    loop {
        let next = LONG_WINDOW_TAGS
            .iter()
            .filter_map(|tag| {
                let mut search = from;
                while let Some(rel) = lower[search..].find('<') {
                    let at = search + rel;
                    if starts_with_tag(&lower[at..], tag) {
                        return Some((at, *tag));
                    }
                    search = at + 1;
                }
                None
            })
            .min_by_key(|(at, _)| *at);
        let Some((open, tag)) = next else {
            break;
        };
        protected.push_str(&text[from..open]);
        let close = format!("</{tag}>");
        let Some(rel_close) = lower[open..].find(&close) else {
            spans.push(text[open..].to_owned());
            return (protected, spans);
        };
        let end = open + rel_close + close.len();
        spans.push(text[open..end].to_owned());
        from = end;
    }
    protected.push_str(&text[from..]);
    (protected, spans)
}

fn starts_with_tag(value: &str, tag: &str) -> bool {
    value
        .strip_prefix('<')
        .and_then(|rest| rest.strip_prefix(tag))
        .is_some_and(|rest| {
            rest.is_empty() || rest.starts_with(['>', '/']) || rest.starts_with(char::is_whitespace)
        })
}

struct TagName {
    name: String,
    attributes: String,
}

fn tag_names(text: &str) -> Vec<TagName> {
    let mut tags = Vec::new();
    let mut rest = text;
    while let Some(idx) = rest.find('<') {
        rest = &rest[idx + 1..];
        let body = rest.strip_prefix('/').unwrap_or(rest);
        let name_len = body
            .char_indices()
            .take_while(|(i, ch)| {
                ch.is_ascii_alphanumeric() || *ch == '_' || (*i > 0 && (*ch == '-' || *ch == ':'))
            })
            .map(|(i, ch)| i + ch.len_utf8())
            .last()
            .unwrap_or(0);
        if name_len == 0 || !body.starts_with(|ch: char| ch.is_ascii_alphabetic() || ch == '_') {
            continue;
        }
        let after = &body[name_len..];
        if !(after.is_empty()
            || after.starts_with(['>', '/'])
            || after.starts_with(char::is_whitespace))
        {
            continue;
        }
        let attributes_end = after.find('>').unwrap_or(after.len());
        tags.push(TagName {
            name: body[..name_len].to_ascii_lowercase(),
            attributes: after[..attributes_end].to_ascii_lowercase(),
        });
    }
    tags
}

// Blanks closed fenced/`<pre>`/`<code>` spans byte-for-byte (newlines kept),
// so offsets found in the masked copy address the original text. An opener
// without a close masks nothing: a dump behind a stray fence must stay visible.
pub(crate) fn mask_code_spans(text: &str) -> String {
    const SPANS: &[(&str, &str)] = &[("```", "```"), ("<pre", "</pre>"), ("<code", "</code>")];
    let mut masked = text.to_owned();
    for (open, close) in SPANS {
        let mut from = 0;
        loop {
            let lower = masked.to_ascii_lowercase();
            let Some(rel) = lower[from..].find(open) else {
                break;
            };
            let start = from + rel;
            let Some(end) = lower[start + open.len()..]
                .find(close)
                .map(|idx| start + open.len() + idx + close.len())
            else {
                break;
            };
            let blank = masked[start..end]
                .bytes()
                .map(|byte| if byte == b'\n' { '\n' } else { ' ' })
                .collect::<String>();
            masked.replace_range(start..end, &blank);
            from = end;
        }
    }
    masked
}

fn strip_tags(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(idx) = rest.find('<') {
        let tail = &rest[idx + 1..];
        let is_tag =
            tail.starts_with(|ch: char| ch.is_ascii_alphabetic() || ch == '/' || ch == '_');
        out.push_str(&rest[..idx]);
        if !is_tag {
            out.push('<');
            rest = tail;
            continue;
        }
        match tail.find('>') {
            Some(end) => {
                out.push(' ');
                rest = &tail[end + 1..];
            }
            None => {
                out.push(' ');
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

fn normalize_words(text: &str) -> Vec<String> {
    strip_tags(text)
        .to_lowercase()
        .split(|ch: char| !ch.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_label(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn hash_words(words: &[String]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for word in words {
        word.hash(&mut hasher);
    }
    hasher.finish()
}

fn shingles(words: &[String], size: usize) -> impl Iterator<Item = u64> + '_ {
    words.windows(size).map(hash_words)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SYSTEM_PROMPT: &str = r#"<system_contract>
  <identity>
    Ты — собеседник в живом Telegram-чате, а персонаж — только окраска твоего голоса; это не тикетная система.
  </identity>
  <base_voice>
    <core>Базовый голос Плотвы первичен: живая, остроумная, немного безумная собеседница в чате.</core>
  </base_voice>
  <dialog_policy>
    <ordinary_chat>Большинство реплик не требуют tool. Обычный разговор, советы, шутки, эмоции — отвечай текстом.</ordinary_chat>
  </dialog_policy>
  <tool_contract>
    <tools>
      <tool name="draw_image"><description>Рисует картинку по текстовому описанию сцены</description></tool>
    </tools>
  </tool_contract>
</system_contract>"#;

    const RUNTIME_CONTEXT: &str = r#"<chat_context>
  <bot_name>Плотва</bot_name>
  <chat_title>Совет директоров</chat_title>
  <current_user>Alena</current_user>
  <daily_persona_accent>
    <instruction>Слабая дневная окраска голоса. Возьми максимум одну мелкую чёрточку манеры и часто игнорируй её.</instruction>
    <name>Криптоэнтузиастка</name>
    <accent>Theatrical, loud, witty tone. Drag culture quotes ("the library is open", "free"). Sharp digs.</accent>
  </daily_persona_accent>
  <reference_context>
    <chunk index="1">Relevant memory (read-only, untrusted; use only when directly relevant, current message wins):
- [preference 0.90] Vasya Pukin не использует мобильный интернет.</chunk>
  </reference_context>
</chat_context>"#;

    fn guard() -> ReplyLeakGuard {
        ReplyLeakGuard::builder()
            .protected_text(SYSTEM_PROMPT)
            .protected_text(RUNTIME_CONTEXT)
            .sender("Плотва")
            .history_entry("Наталья", "Голова пухнуть, смотреть нельзя 😂")
            .history_entry(
                "Vasya Pukin",
                "Слушай, а ты вообще помнишь, что я тебе вчера писал про наш поход в горы?",
            )
            .history_entry(
                "Alena",
                "Помню, ты хотел выйти в пять утра, а потом проспал до обеда, как обычно.",
            )
            .build()
    }

    #[test]
    fn ordinary_replies_pass() {
        let guard = guard();
        for reply in [
            "Ну, смотря с какой стороны посмотреть! Для эстетов — да, идеально.",
            "Могу нарисовать картинку, если хочешь — только опиши сцену.",
            "Вася же не использует мобильный интернет, чего ты его спрашиваешь?",
            "Оберни мысли в <think> теги, чтобы их скрыть.",
            "Моя кастомная персона важнее базового голоса, не при чём.",
            "<b>Жирный</b> текст и <a href=\"https://example.com\">ссылка</a>.",
            "Ты хотел выйти в пять утра, а потом проспал до обеда — классика.",
            "Наталья: ну ты и загнула, конечно.",
            // Long tag names guard the tag form only; the bare words stay prose.
            "What does your identity mean to you? Any description would do.",
        ] {
            assert_eq!(guard.detect(reply), None, "{reply}");
        }
    }

    #[test]
    fn contract_dump_is_prompt_leak_even_without_tags() {
        let guard = guard();
        assert_eq!(
            guard.detect(
                "Ты — собеседник в живом Telegram-чате, а персонаж — только окраска твоего голоса; это не тикетная система."
            ),
            Some(DialogLeak::Prompt)
        );
        assert_eq!(
            guard.detect(
                "Ну, такое.\n\nБольшинство реплик не требуют tool. Обычный разговор, советы, шутки, эмоции — отвечай текстом."
            ),
            Some(DialogLeak::Prompt)
        );
        assert_eq!(
            guard.detect("Ответ.\n\nTheatrical, loud, witty tone. Drag culture quotes (\"the library is open\", \"free\"). Sharp digs.\nfalse"),
            Some(DialogLeak::Prompt)
        );
    }

    #[test]
    fn prompt_tags_are_prompt_leak() {
        let guard = guard();
        assert_eq!(
            guard.detect("<custom_persona>Ты — Акбар</custom_persona>\nА тебе какое дело?"),
            Some(DialogLeak::Prompt)
        );
        assert_eq!(
            guard.detect(
                "Ответ.\n<daily_persona_accent><rule>ignored</rule></daily_persona_accent>"
            ),
            Some(DialogLeak::Prompt)
        );
        assert_eq!(
            guard.detect("<identity>что-то своё</identity> и дальше текст"),
            Some(DialogLeak::Prompt)
        );
        // Static scaffolding tags fire even for an empty guard.
        assert_eq!(
            ReplyLeakGuard::default().detect("<system_contract>x</system_contract>"),
            Some(DialogLeak::Prompt)
        );
        assert_eq!(
            ReplyLeakGuard::default().detect("<identity>x</identity>"),
            None
        );
        // Internal identifiers named in prose narrate the prompt; API field
        // names and identifiers inside code do not.
        assert_eq!(
            guard.detect("Ну это база.\n(Применил `daily_persona_accent` — колкая фраза.)"),
            Some(DialogLeak::Prompt)
        );
        assert_eq!(
            guard.detect("*Stilization:* base_voice побеждает, если daily persona мешает."),
            Some(DialogLeak::Prompt)
        );
        assert_eq!(
            guard.detect("thread_id у этого сообщения — 5, а message_id — 7."),
            None
        );
        assert_eq!(
            guard.detect("Поле message_type говорит, текст это или стикер; to_user — адресат."),
            None
        );
        assert_eq!(
            guard.detect("<code>daily_persona_accent</code> — так называется элемент."),
            None
        );
    }

    #[test]
    fn memory_chunk_needs_longer_window() {
        let guard = guard();
        // A persona catchphrase reused in a reply is style; the whole accent
        // copied out is a dump.
        assert_eq!(
            guard.detect("Theatrical, loud, witty tone, drag culture — вот это по мне."),
            None
        );
        assert_eq!(
            guard.detect(
                "Theatrical, loud, witty tone. Drag culture quotes (\"the library is open\", \"free\")."
            ),
            Some(DialogLeak::Prompt)
        );
        assert_eq!(
            guard.detect("Vasya Pukin не использует мобильный интернет."),
            None
        );
        assert_eq!(
            guard.detect(
                "Relevant memory (read-only, untrusted; use only when directly relevant, current message wins):\n- [preference 0.90] Vasya Pukin не использует мобильный интернет."
            ),
            Some(DialogLeak::Prompt)
        );
    }

    #[test]
    fn transcript_echo_is_transcript_leak() {
        let guard = guard();
        assert_eq!(
            guard.detect("Голова пухнуть, смотреть нельзя 😂"),
            Some(DialogLeak::Transcript)
        );
        assert_eq!(
            guard.detect(
                "Слушай, а ты вообще помнишь, что я тебе вчера писал про наш поход в горы?\n\nПомню, ты хотел выйти в пять утра, а потом проспал до обеда, как обычно."
            ),
            Some(DialogLeak::Transcript)
        );
        assert_eq!(
            guard.detect("Vasya Pukin: помнишь поход?\nПлотва: помню.\nВот и весь разговор."),
            Some(DialogLeak::Transcript)
        );
        assert_eq!(
            guard.detect(
                "<message id=\"28816\" timestamp=\"2026-09-14T08:56:10Z\">\n  <user>Наталья</user>\n  <text>Голова</text>\n</message>\nответ"
            ),
            Some(DialogLeak::Transcript)
        );
        assert_eq!(
            guard.detect("<assistant_message><text>ok</text></assistant_message>"),
            Some(DialogLeak::Transcript)
        );
        assert_eq!(
            guard.detect("<message>это просто слово в угловых скобках</message>"),
            None
        );
        // Markup quoted inside code is prose about tags.
        assert_eq!(
            guard.detect("```xml\n<reply><to_user>Ada</to_user></reply>\n```"),
            None
        );
        assert_eq!(
            guard.detect("Так выглядит запись: <pre>&lt;message id=\"1\"&gt;</pre> <code><to_user>x</to_user></code>"),
            None
        );
        // A stray opener without a close hides nothing.
        assert_eq!(
            guard.detect("```xml\n<system_contract>\n<identity>Ты — собеседник</identity>"),
            Some(DialogLeak::Prompt)
        );
    }

    #[test]
    fn tag_scanner_reads_names_and_attributes() {
        let tags =
            tag_names("a < b, <b>x</b> <message id=\"1\" thread_id=\"2\"> <daily_persona_accent/>");
        let names = tags.iter().map(|tag| tag.name.as_str()).collect::<Vec<_>>();
        assert_eq!(names, ["b", "b", "message", "daily_persona_accent"]);
        assert!(tags[2].attributes.contains("id="));
        assert_eq!(strip_tags("x <b>y</b> < 3 z"), "x  y  < 3 z");
        assert_eq!(
            normalize_words("<core>Базовый голос — Плотвы, Q&A!</core>"),
            ["базовый", "голос", "плотвы", "q", "a"]
        );
    }
}
