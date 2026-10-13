//! Shape checks of a finished reply against what the bot already said: a reply that
//! opens the way one of its latest replies opened, or one that announces a generation
//! the turn never started.

/// How many of the bot's latest replies a new opening is compared with.
pub const RECENT_REPLY_WINDOW: usize = 3;

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

/// The first words of a reply with markup, a vocative name («Катя, …») and leading
/// interjections removed, lowercased; `None` when the reply is too short to have one.
#[must_use]
pub fn opening_frame(text: &str) -> Option<String> {
    let visible = strip_tags(text);
    let head: String = visible.chars().take(OPENING_SCAN_CHARS).collect();
    let mut kept = Vec::with_capacity(OPENING_FRAME_WORDS);
    let mut leading = true;
    for word in words(&head) {
        let normalized = normalize(word.text);
        if word.before_comma && starts_uppercase(word.text) && !is_interjection(&normalized) {
            continue;
        }
        if leading && is_interjection(&normalized) {
            continue;
        }
        leading = false;
        kept.push(normalized);
        if kept.len() == OPENING_FRAME_WORDS {
            return Some(kept.join(" "));
        }
    }
    None
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
    sentences(&visible).any(|sentence| {
        let trimmed = sentence.trim_end();
        if trimmed.ends_with('?') {
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
            before_comma: rest.starts_with(','),
        })
    })
}

fn sentences(text: &str) -> impl Iterator<Item = &str> {
    text.split_inclusive(['.', '!', '?', '…', '\n'])
        .filter(|sentence| !sentence.trim().is_empty())
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
    fn repeated_opening_matches_any_recent_reply() {
        let recent = [
            "Держись там, это пройдёт.",
            "Моська, ну ты и мастер подколоть! 😂",
        ];
        assert!(repeats_recent_opening(
            "Ха-ха, Моська, ну ты и философ!",
            recent
        ));
        assert!(!repeats_recent_opening(
            "Песок там твёрдый, верблюдам нормально.",
            recent
        ));
        assert!(!repeats_recent_opening("Ага.", recent));
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
