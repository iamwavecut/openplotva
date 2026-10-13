//! Markdown a model writes into an HTML reply. Telegram shows its asterisks, hashes and
//! backticks verbatim, so the unambiguous constructs become the matching Telegram tags.
//! Tag markup and the inside of `<code>`/`<pre>` stay as they are; emphasis markers count
//! only at word edges, so a censored «бл**ь» or `2*3*4` is not emphasis.

use crate::escape_telegram_html_text;

const MAX_ITALIC_CHARS: usize = 200;
const MAX_FENCE_LANGUAGE_CHARS: usize = 16;
const RULE_LINE: &str = "———";

/// Convert the Markdown left in an HTML reply into Telegram HTML.
#[must_use]
pub fn telegram_html_from_stray_markdown(text: &str) -> String {
    if !text.contains(['*', '_', '`', '#', '[']) {
        return text.to_owned();
    }
    let fenced = convert_fenced_blocks(text);
    let mut out = String::with_capacity(fenced.len() + 16);
    let mut code_depth = 0_usize;
    let mut at_line_start = true;
    let mut rest = fenced.as_str();
    while !rest.is_empty() {
        if let Some((tag, after)) = split_tag(rest) {
            match tag_name(tag) {
                Some((name, closing)) if is_literal_element(&name) => {
                    if closing {
                        code_depth = code_depth.saturating_sub(1);
                    } else {
                        code_depth += 1;
                    }
                }
                _ => {}
            }
            out.push_str(tag);
            rest = after;
            continue;
        }
        let end = next_tag_start(rest);
        let (run, after) = rest.split_at(end);
        if code_depth == 0 {
            out.push_str(&convert_run(run, at_line_start));
        } else {
            out.push_str(run);
        }
        if let Some(last) = run.chars().last() {
            at_line_start = last == '\n';
        }
        rest = after;
    }
    out
}

/// ```` ```lang\n…\n``` ```` pairs become `<pre>` with the body escaped; an unpaired fence
/// is left as text.
fn convert_fenced_blocks(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find("```") {
        let body_start = open + 3;
        let Some(close) = rest[body_start..].find("```") else {
            break;
        };
        let mut body = &rest[body_start..body_start + close];
        if let Some((head, tail)) = body.split_once('\n')
            && is_fence_language(head)
        {
            body = tail;
        }
        out.push_str(&rest[..open]);
        out.push_str("<pre>");
        out.push_str(&escape_telegram_html_text(body.trim_matches('\n')));
        out.push_str("</pre>");
        rest = &rest[body_start + close + 3..];
    }
    out.push_str(rest);
    out
}

fn is_fence_language(head: &str) -> bool {
    let head = head.trim();
    head.chars().count() < MAX_FENCE_LANGUAGE_CHARS
        && head
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '#' | '.'))
}

/// A run of text between tags, line by line: headings and bullets at the start of a line,
/// then inline emphasis, code and links.
fn convert_run(run: &str, mut at_line_start: bool) -> String {
    let mut out = String::with_capacity(run.len() + 8);
    for line in run.split_inclusive('\n') {
        let (body, newline) = line
            .strip_suffix('\n')
            .map_or((line, ""), |body| (body, "\n"));
        if at_line_start && is_rule_line(body) {
            out.push_str(RULE_LINE);
        } else if at_line_start && let Some(heading) = heading_text(body) {
            out.push_str("<b>");
            out.push_str(&convert_inline(heading));
            out.push_str("</b>");
        } else if at_line_start && let Some(item) = bullet_text(body) {
            out.push_str("• ");
            out.push_str(&convert_inline(item.trim_start()));
        } else {
            out.push_str(&convert_inline(body));
        }
        out.push_str(newline);
        at_line_start = !newline.is_empty();
    }
    out
}

/// `***` or `___` alone on a line: a Markdown thematic break.
fn is_rule_line(line: &str) -> bool {
    let marks: String = line.chars().filter(|ch| !ch.is_whitespace()).collect();
    marks.chars().count() >= 3
        && (marks.chars().all(|ch| ch == '*') || marks.chars().all(|ch| ch == '_'))
}

fn heading_text(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    let hashes = trimmed.chars().take_while(|ch| *ch == '#').count();
    if !(1..=6).contains(&hashes) {
        return None;
    }
    let text = trimmed[hashes..].strip_prefix(' ')?.trim();
    (!text.is_empty()).then_some(text.trim_end_matches('#').trim_end())
}

fn bullet_text(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    trimmed
        .strip_prefix("* ")
        .or_else(|| trimmed.strip_prefix("+ "))
        .filter(|item| !item.trim().is_empty())
}

fn convert_inline(line: &str) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::with_capacity(line.len() + 8);
    let mut index = 0;
    while index < chars.len() {
        if let Some((html, next)) = inline_code(&chars, index)
            .or_else(|| link(&chars, index))
            .or_else(|| bold(&chars, index))
            .or_else(|| italic(&chars, index))
        {
            out.push_str(&html);
            index = next;
        } else {
            out.push(chars[index]);
            index += 1;
        }
    }
    out
}

fn inline_code(chars: &[char], at: usize) -> Option<(String, usize)> {
    if chars[at] != '`' {
        return None;
    }
    let close = (at + 1..chars.len()).find(|&index| chars[index] == '`')?;
    if close == at + 1 {
        return None;
    }
    let body: String = chars[at + 1..close].iter().collect();
    Some((
        format!("<code>{}</code>", escape_telegram_html_text(&body)),
        close + 1,
    ))
}

fn link(chars: &[char], at: usize) -> Option<(String, usize)> {
    if chars[at] != '[' {
        return None;
    }
    let label_end = (at + 1..chars.len()).find(|&index| chars[index] == ']')?;
    if chars.get(label_end + 1) != Some(&'(') || label_end == at + 1 {
        return None;
    }
    let url_start = label_end + 2;
    let url_end = (url_start..chars.len()).find(|&index| chars[index] == ')')?;
    let url: String = chars[url_start..url_end].iter().collect();
    if !(url.starts_with("https://") || url.starts_with("http://"))
        || url.chars().any(char::is_whitespace)
    {
        return None;
    }
    let label: String = chars[at + 1..label_end].iter().collect();
    Some((
        format!(
            "<a href=\"{}\">{}</a>",
            escape_attribute(&url),
            convert_inline(&label)
        ),
        url_end + 1,
    ))
}

/// `**x**` hugging a word: the opener follows no letter and precedes no space, the closer
/// follows no space and precedes no letter.
fn bold(chars: &[char], at: usize) -> Option<(String, usize)> {
    const MARKER: [char; 2] = ['*', '*'];
    if !chars[at..].starts_with(&MARKER) || is_word_char_before(chars, at) {
        return None;
    }
    let body_start = at + MARKER.len();
    if chars.get(body_start).is_none_or(|ch| ch.is_whitespace()) {
        return None;
    }
    let close = (body_start + 1..chars.len().saturating_sub(1)).find(|&index| {
        chars[index..].starts_with(&MARKER)
            && !chars[index - 1].is_whitespace()
            && !is_word_char_at(chars, index + MARKER.len())
    })?;
    let body: String = chars[body_start..close].iter().collect();
    Some((
        format!("<b>{}</b>", convert_inline(&body)),
        close + MARKER.len(),
    ))
}

fn italic(chars: &[char], at: usize) -> Option<(String, usize)> {
    if chars[at] != '*'
        || chars.get(at + 1) == Some(&'*')
        || (at > 0 && chars[at - 1] == '*')
        || is_word_char_before(chars, at)
    {
        return None;
    }
    let body_start = at + 1;
    if chars.get(body_start).is_none_or(|ch| ch.is_whitespace()) {
        return None;
    }
    let close = (body_start + 1..chars.len()).find(|&index| chars[index] == '*')?;
    if chars[close - 1].is_whitespace()
        || chars.get(close + 1) == Some(&'*')
        || is_word_char_at(chars, close + 1)
        || close - body_start > MAX_ITALIC_CHARS
    {
        return None;
    }
    let body: String = chars[body_start..close].iter().collect();
    Some((format!("<i>{body}</i>"), close + 1))
}

fn is_word_char_before(chars: &[char], at: usize) -> bool {
    at > 0 && chars[at - 1].is_alphanumeric()
}

fn is_word_char_at(chars: &[char], at: usize) -> bool {
    chars.get(at).is_some_and(|ch| ch.is_alphanumeric())
}

fn escape_attribute(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// A tag at the start of `text`: `<` followed by a letter, `/` or `!`, up to `>`.
fn split_tag(text: &str) -> Option<(&str, &str)> {
    let mut chars = text.chars();
    if chars.next() != Some('<') {
        return None;
    }
    let next = chars.next()?;
    if !(next.is_ascii_alphabetic() || next == '/' || next == '!') {
        return None;
    }
    let end = text.find('>')?;
    Some(text.split_at(end + 1))
}

fn next_tag_start(text: &str) -> usize {
    text.char_indices()
        .skip(1)
        .find(|&(index, ch)| ch == '<' && split_tag(&text[index..]).is_some())
        .map_or(text.len(), |(index, _)| index)
}

fn tag_name(tag: &str) -> Option<(String, bool)> {
    let inner = tag.strip_prefix('<')?.trim_end_matches('>');
    let (closing, inner) = inner
        .strip_prefix('/')
        .map_or((false, inner), |rest| (true, rest));
    let name: String = inner
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '-')
        .collect::<String>()
        .to_ascii_lowercase();
    (!name.is_empty()).then_some((name, closing))
}

fn is_literal_element(name: &str) -> bool {
    matches!(name, "code" | "pre")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emphasis_code_links_and_headings_become_telegram_tags() {
        assert_eq!(
            telegram_html_from_stray_markdown("1. **Если хочешь кокетства:**\n— «Чтобы понимать…»"),
            "1. <b>Если хочешь кокетства:</b>\n— «Чтобы понимать…»"
        );
        assert_eq!(
            telegram_html_from_stray_markdown("*заговорщицки шепчет* ну всё"),
            "<i>заговорщицки шепчет</i> ну всё"
        );
        assert_eq!(
            telegram_html_from_stray_markdown("### Анализ\nЗапусти `ls -la` и смотри"),
            "<b>Анализ</b>\nЗапусти <code>ls -la</code> и смотри"
        );
        assert_eq!(
            telegram_html_from_stray_markdown("Смотри [доку](https://example.test/a?b=1&c=2)"),
            "Смотри <a href=\"https://example.test/a?b=1&amp;c=2\">доку</a>"
        );
        assert_eq!(
            telegram_html_from_stray_markdown("Список:\n* раз\n*   **два**"),
            "Список:\n• раз\n• <b>два</b>"
        );
        assert_eq!(
            telegram_html_from_stray_markdown("Вот текст:\n\n***\n\n<b>Глава</b>"),
            "Вот текст:\n\n———\n\n<b>Глава</b>"
        );
        assert_eq!(
            telegram_html_from_stray_markdown("Код:\n```rust\nlet a = b < c;\n```\nготово"),
            "Код:\n<pre>let a = b &lt; c;</pre>\nготово"
        );
    }

    #[test]
    fn markers_inside_words_code_and_tags_stay_literal() {
        for text in [
            "Ну бл**ь, опять х**ня",
            "2*3*4 = 24",
            "snake_case_name и __init__.py",
            "Оценка 5* из 5*",
            "<code>**literal**</code> и <pre>*x*</pre>",
            "<a href=\"https://example.test/a_b_c\">ссылка</a>",
            "Хэштег #вечер без пробела",
            "[не ссылка](ftp://example.test)",
            "Звёздочка * одна",
        ] {
            assert_eq!(telegram_html_from_stray_markdown(text), text, "{text}");
        }
    }

    #[test]
    fn html_already_in_the_reply_is_kept() {
        assert_eq!(
            telegram_html_from_stray_markdown("<b>Готово</b>: **ещё** и <i>так</i>"),
            "<b>Готово</b>: <b>ещё</b> и <i>так</i>"
        );
    }
}
