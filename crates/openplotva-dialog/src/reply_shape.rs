//! Shape review of a finished reply against what the bot already said. The model copies
//! the frame of its own earlier replies and announces drawings it never started; such a
//! reply asks for one more sample, and the model rewrites it after a note on why.

/// How many of the bot's latest replies a new opening is compared with.
pub const RECENT_REPLY_WINDOW: usize = 8;

const OPENING_FRAME_WORDS: usize = 3;
const OPENING_SCAN_CHARS: usize = 240;

/// Laughter and exclamations that only precede the opening; skipping them makes
/// «Ха-ха, Катя, ну ты и…» and «Катя, ну ты и…» the same frame.
const LEADING_INTERJECTIONS: &[&str] = &[
    "а",
    "ага",
    "ах",
    "ахах",
    "ахаха",
    "ахахах",
    "вау",
    "ха",
    "ха-ха",
    "ха-ха-ха",
    "хаха",
    "хе-хе",
    "хех",
    "хм",
    "лол",
    "слушай",
    "мда",
    "о",
    "ого",
    "ой",
    "омг",
    "оу",
    "ох",
    "угу",
    "упс",
    "ура",
    "ух",
    "эх",
];

const GENERATION_VERBS: &[&str] = &[
    "рисую",
    "нарисую",
    "дорисую",
    "перерисую",
    "запускаю",
    "запущу",
    "генерирую",
    "сгенерирую",
];

/// What the review sees besides the reply itself.
pub struct ReplyContext<'a> {
    /// The bot's latest replies in the chat, newest first.
    pub recent_replies: &'a [&'a str],
    /// A generation tool was offered and no call of this turn started one.
    pub generation_pending: bool,
    /// The turn may still ask for another sample.
    pub resample_allowed: bool,
}

/// Outcome of the review.
#[derive(Debug, Eq, PartialEq)]
pub enum ReplyShape {
    /// Deliver the reply as it is.
    Keep,
    /// Ask the model for another sample.
    Resample(ShapeFault),
}

/// Why a reply asks for another sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShapeFault {
    /// The reply opens the way one of the bot's latest replies opened.
    RepeatedOpening,
    /// The reply announces a generation no call of this turn started.
    UnexecutedAction,
}

/// Review a finished reply: ask for one more sample when it announces a generation no
/// call started or opens the way one of the bot's latest replies opened.
#[must_use]
pub fn review_reply(reply: &str, context: &ReplyContext<'_>) -> ReplyShape {
    if !context.resample_allowed {
        return ReplyShape::Keep;
    }
    if context.generation_pending && announces_generation(reply) {
        return ReplyShape::Resample(ShapeFault::UnexecutedAction);
    }
    if repeats_recent_opening(reply, context.recent_replies.iter().copied()) {
        return ReplyShape::Resample(ShapeFault::RepeatedOpening);
    }
    ReplyShape::Keep
}

/// The first words of a reply with markup, a vocative name («Катя, …») and leading
/// interjections removed, lowercased; `None` when the reply is too short to have one.
#[must_use]
pub fn opening_frame(text: &str) -> Option<String> {
    let visible = strip_tags(text);
    let head: String = visible.chars().take(OPENING_SCAN_CHARS).collect();
    let kept: Vec<String> = significant_words(&head).take(OPENING_FRAME_WORDS).collect();
    (kept.len() == OPENING_FRAME_WORDS).then(|| kept.join(" "))
}

/// Whether the reply opens with the same frame as one of `recent` replies.
#[must_use]
pub fn repeats_recent_opening<'a>(reply: &str, recent: impl IntoIterator<Item = &'a str>) -> bool {
    let Some(frame) = opening_frame(reply) else {
        return false;
    };
    recent
        .into_iter()
        .filter_map(opening_frame)
        .any(|previous| previous == frame)
}

/// Whether a sentence of the reply states, in the first person, that a drawing or a
/// generation is happening: «Рисую…», «Запускаю генерацию», «Сейчас нарисую». Questions,
/// conditionals and negations are offers or refusals, not announcements.
#[must_use]
pub fn announces_generation(reply: &str) -> bool {
    let visible = strip_tags(reply);
    split_sentences(&visible).iter().any(|sentence| {
        if sentence.trim_end().ends_with('?') {
            return false;
        }
        let tokens: Vec<String> = words(sentence).map(|word| normalize(word.text)).collect();
        if tokens.iter().any(|token| token == "если") {
            return false;
        }
        tokens.iter().enumerate().any(|(index, token)| {
            let negated = index > 0 && tokens[index - 1] == "не";
            if negated {
                return false;
            }
            let next = tokens.get(index + 1).map(String::as_str);
            GENERATION_VERBS.contains(&token.as_str())
                || (token == "начинаю"
                    && matches!(next, Some("рисовать" | "генерацию" | "генерировать")))
                || (token == "иду" && next == Some("рисовать"))
        })
    })
}

/// Words after leading interjections, with vocative names («Катя, …») dropped anywhere.
fn significant_words(text: &str) -> impl Iterator<Item = String> + '_ {
    let mut leading = true;
    words(text).filter_map(move |word| {
        let normalized = normalize(word.text);
        if word.before_comma && starts_uppercase(word.text) && !is_interjection(&normalized) {
            return None;
        }
        if leading && is_interjection(&normalized) {
            return None;
        }
        leading = false;
        Some(normalized)
    })
}

/// Sentences of `text` as consecutive slices that join back into it. A sentence ends at
/// `.`, `!`, `?`, `…` before a non-word character or at a line break, and keeps the
/// emoji and spaces that follow it. Tags and «quotes» are never split.
fn split_sentences(text: &str) -> Vec<&str> {
    let mut sentences = Vec::new();
    let mut start = 0;
    let mut quote_depth = 0_usize;
    let mut chars = text.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if ch == '<'
            && chars
                .peek()
                .is_some_and(|(_, next)| next.is_ascii_alphabetic() || *next == '/')
        {
            for (_, inner) in chars.by_ref() {
                if inner == '>' {
                    break;
                }
            }
            continue;
        }
        match ch {
            '«' | '“' => quote_depth += 1,
            '»' | '”' => quote_depth = quote_depth.saturating_sub(1),
            _ => {}
        }
        let terminator = matches!(ch, '.' | '!' | '?' | '…');
        if !(ch == '\n' || (terminator && quote_depth == 0)) {
            continue;
        }
        let mut end = index + ch.len_utf8();
        while let Some(&(next_index, next)) = chars.peek() {
            if matches!(next, '.' | '!' | '?' | '…') {
                end = next_index + next.len_utf8();
                chars.next();
            } else {
                break;
            }
        }
        if terminator && chars.peek().is_some_and(|(_, next)| next.is_alphanumeric()) {
            continue;
        }
        while let Some(&(next_index, next)) = chars.peek() {
            if next.is_whitespace() || is_trailing_decoration(next) {
                end = next_index + next.len_utf8();
                chars.next();
            } else {
                break;
            }
        }
        sentences.push(&text[start..end]);
        start = end;
    }
    if start < text.len() {
        sentences.push(&text[start..]);
    }
    sentences
        .into_iter()
        .filter(|sentence| !sentence.trim().is_empty())
        .collect()
}

fn is_trailing_decoration(ch: char) -> bool {
    !ch.is_alphanumeric()
        && !matches!(
            ch,
            '<' | '«' | '“' | '"' | '\'' | '(' | '[' | '*' | '-' | '—' | '•'
        )
}

struct Word<'a> {
    text: &'a str,
    before_comma: bool,
}

/// Letter and digit runs, joined across inner hyphens («ха-ха», «во-первых»).
fn words(text: &str) -> impl Iterator<Item = Word<'_>> {
    let mut rest = text;
    std::iter::from_fn(move || {
        let start = rest.find(char::is_alphanumeric)?;
        let tail = &rest[start..];
        let mut end = 0;
        let mut chars = tail.char_indices().peekable();
        while let Some((index, ch)) = chars.next() {
            if ch.is_alphanumeric() {
                end = index + ch.len_utf8();
            } else if ch == '-' && chars.peek().is_some_and(|(_, next)| next.is_alphanumeric()) {
                continue;
            } else {
                break;
            }
        }
        let word = &tail[..end];
        rest = &tail[end..];
        Some(Word {
            text: word,
            before_comma: rest.trim_start_matches('.').starts_with(','),
        })
    })
}

fn strip_tags(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        let opens_tag = ch == '<'
            && chars
                .peek()
                .is_some_and(|next| next.is_ascii_alphabetic() || *next == '/');
        if opens_tag {
            for inner in chars.by_ref() {
                if inner == '>' {
                    break;
                }
            }
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    out
}

fn normalize(word: &str) -> String {
    word.to_lowercase().replace('ё', "е")
}

fn starts_uppercase(word: &str) -> bool {
    word.chars().next().is_some_and(char::is_uppercase)
}

fn is_interjection(normalized: &str) -> bool {
    LEADING_INTERJECTIONS.contains(&normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context<'a>(recent: &'a [&'a str]) -> ReplyContext<'a> {
        ReplyContext {
            recent_replies: recent,
            generation_pending: false,
            resample_allowed: true,
        }
    }

    #[test]
    fn opening_frame_drops_markup_names_and_leading_interjections() {
        assert_eq!(
            opening_frame("Моська, ну ты и мастер успокаивать! 😂").as_deref(),
            Some("ну ты и")
        );
        assert_eq!(
            opening_frame("Ха-ха, Моська, ну ты и выдала!").as_deref(),
            Some("ну ты и")
        );
        assert_eq!(
            opening_frame("😂 Евгений, ну ты и выдал!").as_deref(),
            Some("ну ты и")
        );
        assert_eq!(
            opening_frame("<b>Ой</b>, ну всё, Гидра, не надо так").as_deref(),
            Some("ну все не")
        );
        assert_eq!(
            opening_frame("Ого, да ты сегодня просто мастер").as_deref(),
            Some("да ты сегодня")
        );
        assert_eq!(opening_frame("Кто? 🤨"), None);
        assert_eq!(opening_frame("Да, верно."), None);
    }

    #[test]
    fn sentences_keep_quotes_tags_and_trailing_emoji_together() {
        assert_eq!(
            split_sentences("Ну ты и выдал! 😂 «Не гадай... крути» — это ты так? Ок."),
            vec![
                "Ну ты и выдал! 😂 ",
                "«Не гадай... крути» — это ты так? ",
                "Ок."
            ]
        );
        assert_eq!(
            split_sentences("Версия 1.5 вышла.\n\n<b>Итог:</b> всё."),
            vec!["Версия 1.5 вышла.\n\n", "<b>Итог:</b> всё."]
        );
    }

    #[test]
    fn a_plain_answer_or_a_fresh_opening_is_kept() {
        let recent = ["Держись там, это пройдёт.", "Ну ты и выдал!"];
        for reply in [
            "Поезд в шесть, успеешь.",
            "Да, поезд в шесть, успеешь.",
            "Кто? 🤨",
        ] {
            assert_eq!(
                review_reply(reply, &context(&recent)),
                ReplyShape::Keep,
                "{reply}"
            );
        }
    }

    #[test]
    fn repeated_opening_asks_for_one_sample() {
        let recent = ["Держись там, это пройдёт.", "Я чувствую, как всё меняется."];
        let reply = "Я чувствую, как дождь стучит по крыше.";
        assert_eq!(
            review_reply(reply, &context(&recent)),
            ReplyShape::Resample(ShapeFault::RepeatedOpening)
        );
        let resampled = ReplyContext {
            resample_allowed: false,
            ..context(&recent)
        };
        assert_eq!(review_reply(reply, &resampled), ReplyShape::Keep);
    }

    #[test]
    fn unstarted_generation_promise_asks_for_one_sample() {
        let pending = ReplyContext {
            generation_pending: true,
            ..context(&[])
        };
        assert_eq!(
            review_reply(
                "Пещера с радужными стенами — сильная идея. Рисую твою пещеру! 🎨",
                &pending
            ),
            ReplyShape::Resample(ShapeFault::UnexecutedAction)
        );
        assert_eq!(
            review_reply("Рисую твою пещеру! 🎨", &context(&[])),
            ReplyShape::Keep
        );
    }

    #[test]
    fn announced_generation_is_a_first_person_statement() {
        assert!(announces_generation(
            "Твой сценарий принят. Рисую твою радужную карстовую пещеру! 🎨"
        ));
        assert!(announces_generation("Всё, не томи, запускаю."));
        assert!(announces_generation(
            "Сейчас нарисую новую версию с нуля, где палочки будут на месте."
        ));
        assert!(announces_generation("Уже начинаю рисовать, жди."));
        assert!(!announces_generation(
            "Нарисовать? Могу, если скажешь, в каком стиле."
        ));
        assert!(!announces_generation("Нарисую, если пришлёшь референс."));
        assert!(!announces_generation("Я такое не рисую, это за гранью."));
        assert!(!announces_generation("Картинка уже в чате, смотри выше."));
        assert!(!announces_generation("Запущу генерацию?"));
    }
}
