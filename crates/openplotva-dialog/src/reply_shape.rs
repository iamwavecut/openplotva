//! Shape review of a finished reply. The model copies the frame of its own earlier
//! replies: a reaction that praises the person, their message quoted back with a guess
//! at what they meant, a promise of a drawing that never started. Those parts are cut
//! from the reply, so the chat and the history keep only what was said. An opening
//! copied from a recent reply that cannot be cut asks for one more sample.

/// How many of the bot's latest replies a new opening is compared with.
pub const RECENT_REPLY_WINDOW: usize = 8;

const OPENING_FRAME_WORDS: usize = 3;
const OPENING_SCAN_CHARS: usize = 240;
const MIN_KEPT_WORDS: usize = 3;
const MIN_ECHO_WORDS: usize = 2;
const REACTION_SCAN_WORDS: usize = 6;
const MAX_REACTION_SENTENCES: usize = 2;

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

/// Words that read a quoted message back to its author instead of answering it.
const READ_BACK_MARKERS: &[&str] = &["намекаешь", "намекает", "звучит", "типа", "значит"];

/// Openers of a sentence that keeps reading back the quote before it.
const READ_BACK_CONTINUATIONS: &[&str] = &["типа", "значит", "получается", "мол"];

const LAUGH_EMOJI: &[char] = &['😂', '🤣', '😆'];

/// What the review sees besides the reply itself.
pub struct ReplyContext<'a> {
    /// The bot's latest replies in the chat, newest first.
    pub recent_replies: &'a [&'a str],
    /// The message the reply answers.
    pub user_text: &'a str,
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
    /// Deliver the reply with its frame removed.
    Trimmed(String),
    /// Ask the model for another sample.
    Resample(ShapeFault),
}

/// Why a reply asks for another sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShapeFault {
    /// The reply opens the way one of the bot's latest replies opened.
    RepeatedOpening,
    /// The whole reply announces a generation no call started.
    UnexecutedAction,
}

/// Review a finished reply: cut the reaction opener, read-back quotes and an unstarted
/// generation promise when something else remains, then ask for one more sample when
/// the opening still repeats a recent reply or the reply is only a promise.
#[must_use]
pub fn review_reply(reply: &str, context: &ReplyContext<'_>) -> ReplyShape {
    let sentences = split_sentences(reply);
    let user_words = echo_words(&strip_tags(context.user_text));
    let mut removed = vec![false; sentences.len()];
    for (index, sentence) in sentences.iter().enumerate().take(MAX_REACTION_SENTENCES) {
        if !is_reaction_opener(sentence) {
            break;
        }
        removed[index] = true;
    }
    let mut after_echo = false;
    for (index, sentence) in sentences.iter().enumerate() {
        if echoes_user_quote(sentence, &user_words) || (after_echo && continues_read_back(sentence))
        {
            removed[index] = true;
            after_echo = true;
        } else {
            after_echo = false;
        }
    }
    let promise_only =
        context.generation_pending && sentences.len() == 1 && announces_generation(sentences[0]);
    if context.generation_pending
        && sentences.len() > 1
        && let Some(last) = sentences.last()
        && announces_generation(last)
    {
        removed[sentences.len() - 1] = true;
    }

    let kept: Vec<&str> = sentences
        .iter()
        .zip(&removed)
        .filter(|(_, removed)| !**removed)
        .map(|(sentence, _)| *sentence)
        .collect();
    let trimmed = removed.iter().any(|removed| *removed)
        && kept
            .iter()
            .map(|sentence| word_count(sentence))
            .sum::<usize>()
            >= MIN_KEPT_WORDS;
    let text = if trimmed {
        capitalize_first(kept.concat().trim())
    } else {
        reply.to_owned()
    };

    if context.resample_allowed {
        if promise_only {
            return ReplyShape::Resample(ShapeFault::UnexecutedAction);
        }
        if repeats_recent_opening(&text, context.recent_replies.iter().copied()) {
            return ReplyShape::Resample(ShapeFault::RepeatedOpening);
        }
    }
    if trimmed {
        ReplyShape::Trimmed(text)
    } else {
        ReplyShape::Keep
    }
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

/// A first sentence that only reacts to the message or rates the person: laughter and
/// interjections alone, or an exclaimed «ну ты и…», «да ты…», «ты просто…», possibly
/// after a name or a «Слушай,» set off by a comma.
fn is_reaction_opener(sentence: &str) -> bool {
    let visible = strip_tags(sentence);
    let trimmed = visible.trim_end();
    if trimmed.ends_with('?') {
        return false;
    }
    let tokens: Vec<(String, bool)> = words(&visible)
        .map(|word| (normalize(word.text), word.before_comma))
        .collect();
    let laughing = visible.contains(LAUGH_EMOJI);
    if tokens.iter().all(|(token, _)| is_interjection(token)) {
        let structural = visible.contains(['[', ']', '{', '}', '`', '"', ':']);
        let pictographic = visible.chars().any(is_pictographic);
        return !structural && (laughing || pictographic || !tokens.is_empty());
    }
    let exclaimed = trimmed.contains('!') || laughing;
    (0..tokens.len().min(REACTION_SCAN_WORDS)).any(|start| {
        let after_pause = start == 0 || tokens[start - 1].1;
        let leading_interjections = tokens[..start]
            .iter()
            .all(|(token, _)| is_interjection(token));
        if exclaimed
            && start > 0
            && leading_interjections
            && tokens[start].0 == "все"
            && tokens[start].1
        {
            return true;
        }
        (after_pause || leading_interjections)
            && match rating(&tokens[start..]) {
                Rating::Plain => true,
                Rating::Exclaimed => exclaimed,
                Rating::None => false,
            }
    })
}

enum Rating {
    /// Rates the person on its own: «ну ты и стратег».
    Plain,
    /// Rates the person only when exclaimed: «ты просто мастер!», «да ты сегодня…!».
    Exclaimed,
    None,
}

/// Words after «ну ты и» that continue a thought instead of rating the person.
const NOT_A_RATING: &[&str] = &["сам", "сама", "сами", "так", "тогда", "это", "не"];

fn rating(tokens: &[(String, bool)]) -> Rating {
    let word = |index: usize| tokens.get(index).map(|(token, _)| token.as_str());
    match (word(0), word(1), word(2)) {
        (Some("ну"), Some("ты" | "вы"), Some("и")) => {
            if word(3).is_some_and(|next| NOT_A_RATING.contains(&next)) {
                Rating::None
            } else {
                Rating::Plain
            }
        }
        (Some("ну"), Some("ты" | "вы"), Some("даешь" | "даете")) => Rating::Plain,
        (Some("ну"), Some("ты" | "вы" | "все"), _)
        | (Some("да"), Some("ты" | "вы"), _)
        | (Some("ты" | "вы"), Some("просто" | "прямо" | "сегодня" | "и"), _)
        | (Some("какой" | "какая" | "какие"), Some("ты" | "вы"), _)
        | (Some("вот"), Some("это"), Some("ты" | "вы")) => Rating::Exclaimed,
        _ => Rating::None,
    }
}

fn is_pictographic(ch: char) -> bool {
    let code = u32::from(ch);
    code >= 0x1F000 || (0x2600..=0x27BF).contains(&code)
}

/// A sentence quoting a run of the current message and reading it back: a question, or
/// a «это ты…» / «намекаешь» / «звучит как» gloss.
fn echoes_user_quote(sentence: &str, user_words: &[String]) -> bool {
    if user_words.len() < MIN_ECHO_WORDS {
        return false;
    }
    let visible = strip_tags(sentence);
    let quoted = quoted_runs(&visible).into_iter().any(|quote| {
        let quote = echo_words(quote);
        quote.len() >= MIN_ECHO_WORDS
            && user_words
                .windows(quote.len())
                .any(|window| window == quote.as_slice())
    });
    if !quoted {
        return false;
    }
    let tokens: Vec<String> = words(&visible).map(|word| normalize(word.text)).collect();
    visible.trim_end().ends_with('?')
        || tokens
            .windows(2)
            .any(|pair| pair[0] == "это" && matches!(pair[1].as_str(), "ты" | "вы"))
        || tokens
            .iter()
            .any(|token| READ_BACK_MARKERS.contains(&token.as_str()))
}

/// Words for matching a quote against the message, with hyphenated words split so
/// «гадай-всё» and «гадай — всё» match.
fn echo_words(text: &str) -> Vec<String> {
    words(text)
        .flat_map(|word| {
            normalize(word.text)
                .split('-')
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn continues_read_back(sentence: &str) -> bool {
    let visible = strip_tags(sentence);
    let mut tokens = words(&visible).map(|word| normalize(word.text));
    let first = tokens.next();
    let second = tokens.next();
    first
        .as_deref()
        .is_some_and(|first| READ_BACK_CONTINUATIONS.contains(&first))
        || (first.as_deref() == Some("то") && second.as_deref() == Some("есть"))
}

fn quoted_runs(text: &str) -> Vec<&str> {
    let mut runs = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find(['«', '“']) {
        let after = &rest[open..];
        let body_start = after.chars().next().map_or(1, char::len_utf8);
        let body = &after[body_start..];
        let Some(close) = body.find(['»', '”']) else {
            break;
        };
        runs.push(&body[..close]);
        rest = &body[close..];
    }
    runs
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

fn word_count(text: &str) -> usize {
    words(&strip_tags(text)).count()
}

fn capitalize_first(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut done = false;
    let mut in_tag = false;
    for ch in text.chars() {
        if !done && !in_tag && ch.is_alphabetic() {
            out.extend(ch.to_uppercase());
            done = true;
            continue;
        }
        if ch == '<' {
            in_tag = true;
        } else if ch == '>' {
            in_tag = false;
        }
        out.push(ch);
    }
    out
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

    fn context<'a>(recent: &'a [&'a str], user_text: &'a str) -> ReplyContext<'a> {
        ReplyContext {
            recent_replies: recent,
            user_text,
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
    fn reaction_opener_and_read_back_quote_are_cut() {
        let user = "Не гадай-всё в порядке. Делай уверенно свои дела и без задней тени ...крути";
        let reply = "Моська, ну ты и мастер успокаивать! 😂 «Не гадай — всё в порядке» — это ты так даёшь мне понять, что можно не ждать подвоха? Типа, всё под контролем!\n\nПринято. Буду работать спокойно, без лишних раздумий.";
        assert_eq!(
            review_reply(reply, &context(&[], user)),
            ReplyShape::Trimmed("Принято. Буду работать спокойно, без лишних раздумий.".to_owned())
        );
    }

    #[test]
    fn a_lone_reaction_or_plain_answer_is_kept() {
        assert_eq!(
            review_reply("Ну ты и выдал!", &context(&[], "смотри")),
            ReplyShape::Keep
        );
        assert_eq!(
            review_reply(
                "Да ты что, серьёзно? Тогда бери зонт.",
                &context(&[], "дождь")
            ),
            ReplyShape::Keep
        );
        assert_eq!(
            review_reply("[\n  {\"id\": 1}\n]", &context(&[], "дай json")),
            ReplyShape::Keep
        );
        assert_eq!(
            review_reply(
                "Всё, договорились! Завтра в десять у входа.",
                &context(&[], "давай завтра")
            ),
            ReplyShape::Keep
        );
        assert_eq!(
            review_reply(
                "😂\n\nну всё, Moonshine, ты меня смешишь! Пока Том ищет пиццу, мы тут сидим.",
                &context(&[], "ахах")
            ),
            ReplyShape::Trimmed("Пока Том ищет пиццу, мы тут сидим.".to_owned())
        );
        assert_eq!(
            review_reply(
                "миша попов, ну ты и выдал! Скейт в лицо — это перебор.",
                &context(&[], "лови")
            ),
            ReplyShape::Trimmed("Скейт в лицо — это перебор.".to_owned())
        );
        assert_eq!(
            review_reply(
                "Хасн, ну ты и философ! Честность — это база.",
                &context(&[], "я честный")
            ),
            ReplyShape::Trimmed("Честность — это база.".to_owned())
        );
    }

    #[test]
    fn repeated_opening_asks_for_one_sample_when_nothing_is_cut() {
        let recent = ["Держись там, это пройдёт.", "Я чувствую, как всё меняется."];
        let reply = "Я чувствую, как дождь стучит по крыше.";
        assert_eq!(
            review_reply(reply, &context(&recent, "дождь")),
            ReplyShape::Resample(ShapeFault::RepeatedOpening)
        );
        let resampled = ReplyContext {
            resample_allowed: false,
            ..context(&recent, "дождь")
        };
        assert_eq!(review_reply(reply, &resampled), ReplyShape::Keep);
    }

    #[test]
    fn unstarted_generation_promise_is_cut_or_resampled() {
        let pending = ReplyContext {
            generation_pending: true,
            ..context(&[], "нарисуй пещеру")
        };
        assert_eq!(
            review_reply(
                "Пещера с радужными стенами — сильная идея. Рисую твою пещеру! 🎨",
                &pending
            ),
            ReplyShape::Trimmed("Пещера с радужными стенами — сильная идея.".to_owned())
        );
        assert_eq!(
            review_reply("Рисую твою пещеру! 🎨", &pending),
            ReplyShape::Resample(ShapeFault::UnexecutedAction)
        );
        assert_eq!(
            review_reply("Рисую твою пещеру! 🎨", &context(&[], "нарисуй")),
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
