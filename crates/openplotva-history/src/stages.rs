//! Two-stage chat summary: events are extracted from every chunk of the
//! window, merged in code, and the recap is written from the merged events.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

use crate::{
    HistorySummaryDecodeError, SummaryActor, SummaryContent, SummaryEvent, SummaryInput,
    SummaryInputItem, deserialize_f64_loose, estimate_summary_text_tokens, go_zero_time,
};

/// Estimated tokens (a quarter of the characters) of one stage-one chunk.
/// Cyrillic chats run about twice that in real tokens; chunks twice this size
/// lost a quarter of the planted threads in the eval.
pub const EVENTS_CHUNK_MAX_TOKENS: i32 = 3_000;
/// Share of a chunk's trailing items repeated at the start of the next chunk.
pub const EVENTS_CHUNK_OVERLAP_PERCENT: usize = 10;
/// A chunk with fewer substantive messages is quiet without a model call.
pub const EVENTS_MIN_SUBSTANTIVE_MESSAGES: usize = 8;
/// Most events kept from one chunk.
pub const EVENTS_MAX_PER_CHUNK: usize = 12;
/// Shortest one-word message that can carry an event.
pub const MIN_ONE_WORD_EVENT_MESSAGE_CHARS: usize = 12;
/// Closing line of the stage-one user message.
pub const EVENTS_TASK_LINE: &str = "По окну выше верни JSON с событиями, как описано в инструкции.";
/// Closing line of the stage-two user message.
pub const RECAP_TASK_LINE: &str =
    "По событиям выше верни JSON с пересказом, как описано в инструкции.";
/// Recap of a window without notable events; no model writes it.
pub const QUIET_RECAP: &str = "В чате было тихо: заметных событий за этот период нет.";

/// The two model calls of a summary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryStage {
    /// Events of one chunk of the window.
    Events,
    /// Recap written from the merged events.
    Recap,
}

impl HistoryStage {
    #[must_use]
    pub const fn prompt_name(self) -> &'static str {
        match self {
            Self::Events => "history/events",
            Self::Recap => "history/recap",
        }
    }

    #[must_use]
    pub const fn max_output_tokens(self) -> i32 {
        match self {
            Self::Events => 3072,
            Self::Recap => 4096,
        }
    }

    /// Whether the request carries the answer schema. Stage one asks for its
    /// JSON in the prompt only: on a server that decodes schemas without
    /// whitespace (vLLM's `disable_any_whitespace`), a long, mostly idle chunk
    /// made the model close the events list at once, and the prompt alone read
    /// every planted thread with or without that setting.
    #[must_use]
    pub const fn sends_response_schema(self) -> bool {
        matches!(self, Self::Recap)
    }

    #[must_use]
    pub const fn schema_name(self) -> &'static str {
        match self {
            Self::Events => "chat_history_events",
            Self::Recap => "chat_history_recap",
        }
    }

    /// JSON schema of the stage's answer.
    #[must_use]
    pub fn response_schema(self) -> Value {
        match self {
            Self::Events => json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["events", "nothing_notable"],
                "properties": {
                    "events": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["source_ids", "title", "description", "actors", "confidence"],
                            "properties": {
                                "source_ids": {"type": "array", "items": {"type": "string"}},
                                "title": {"type": "string"},
                                "description": {"type": "string"},
                                "actors": {"type": "array", "items": {"type": "string"}},
                                "confidence": {"type": "number"},
                            },
                        },
                    },
                    "nothing_notable": {"type": "boolean"},
                },
            }),
            Self::Recap => json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["recap", "actors", "open_questions", "source_style", "quality_score", "quality_notes"],
                "properties": {
                    "recap": {"type": "string"},
                    "actors": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["name", "description"],
                            "properties": {
                                "name": {"type": "string"},
                                "description": {"type": "string"},
                            },
                        },
                    },
                    "open_questions": {"type": "array", "items": {"type": "string"}},
                    "source_style": {"type": "string"},
                    "quality_score": {"type": "number"},
                    "quality_notes": {"type": "string"},
                },
            }),
        }
    }
}

fn iso(value: OffsetDateTime) -> Option<String> {
    (value != go_zero_time())
        .then(|| value.format(&Rfc3339).ok())
        .flatten()
}

fn utc(value: OffsetDateTime) -> Option<OffsetDateTime> {
    (value != go_zero_time()).then(|| value.to_offset(UtcOffset::UTC))
}

fn escape_text(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace('<', "&lt;")
}

fn escape_attr(text: &str) -> String {
    escape_text(text).replace('"', "&quot;")
}

fn is_summary_item(item: &SummaryInputItem) -> bool {
    item.kind == "summary"
}

/// Text a model reads for an item: the message (with an image description when
/// there is one), or the recap of a reused summary.
fn item_text(item: &SummaryInputItem) -> String {
    if is_summary_item(item) {
        let recap = item.summary_json.recap.trim();
        return if recap.is_empty() {
            item.summary_json.events.join("; ")
        } else {
            recap.to_owned()
        };
    }
    let text = if item.text.trim().is_empty() {
        item.original_text.trim()
    } else {
        item.text.trim()
    };
    let vision = item.vision_description.trim();
    match (text.is_empty(), vision.is_empty()) {
        (_, true) => text.to_owned(),
        (true, false) => format!("[изображение: {vision}]"),
        (false, false) => format!("{text} [изображение: {vision}]"),
    }
}

/// The id merging and dating use for an item; the model sees a short
/// position instead.
fn item_source_id(item: &SummaryInputItem) -> String {
    if is_summary_item(item) {
        format!("s{}", item.summary_id)
    } else if item.message_id != 0 {
        item.message_id.to_string()
    } else {
        item.entry_id.trim().to_owned()
    }
}

fn item_time(item: &SummaryInputItem) -> OffsetDateTime {
    if is_summary_item(item) && item.range_start_at != go_zero_time() {
        item.range_start_at
    } else {
        item.at
    }
}

fn item_author(item: &SummaryInputItem) -> String {
    let name = item.sender_name.trim();
    if !name.is_empty() {
        return name.to_owned();
    }
    let username = item.sender_username.trim().trim_start_matches('@');
    if username.is_empty() {
        item.role.trim().to_owned()
    } else {
        format!("@{username}")
    }
}

/// One prompt line per item.
#[must_use]
fn render_item(item: &SummaryInputItem, id: usize) -> String {
    let text = escape_text(&item_text(item));
    if is_summary_item(item) {
        let from = iso(item.range_start_at).unwrap_or_default();
        let to = iso(item.range_end_at).unwrap_or_default();
        return format!("<summary id=\"{id}\" from=\"{from}\" to=\"{to}\">{text}</summary>");
    }
    let at = utc(item.at)
        .map(|at| format!("{:02}:{:02}", at.hour(), at.minute()))
        .unwrap_or_default();
    let from = escape_attr(&item_author(item));
    format!("<msg id=\"{id}\" at=\"{at}\" from=\"{from}\">{text}</msg>")
}

fn item_cost(item: &SummaryInputItem) -> i32 {
    estimate_summary_text_tokens(&render_item(item, 100)) + 1
}

/// Whether an item can carry an event: summaries always; messages need a letter
/// or digit, must not be a bot command, and a one-word message needs
/// `MIN_ONE_WORD_EVENT_MESSAGE_CHARS` characters.
#[must_use]
pub fn item_carries_event(item: &SummaryInputItem) -> bool {
    if is_summary_item(item) {
        return true;
    }
    let text = item_text(item);
    let text = text.trim();
    let mut chars = text.chars();
    if chars.next() == Some('/') && chars.next().is_some_and(|ch| ch.is_ascii_alphabetic()) {
        return false;
    }
    if !text.chars().any(char::is_alphanumeric) {
        return false;
    }
    text.chars().count() >= MIN_ONE_WORD_EVENT_MESSAGE_CHARS || text.split_whitespace().count() >= 2
}

fn window_header(input: &SummaryInput, items: usize) -> String {
    let mut header = serde_json::Map::new();
    header.insert("scope".to_owned(), json!(input.scope.as_str()));
    if let Some(start) = iso(input.range_start_at) {
        header.insert("range_start".to_owned(), json!(start));
    }
    if let Some(end) = iso(input.range_end_at) {
        header.insert("range_end".to_owned(), json!(end));
    }
    header.insert("items".to_owned(), json!(items));
    Value::Object(header).to_string()
}

/// Stage-one user message for one chunk: the window header, one line per item,
/// then the task. Items are numbered from 1 within the chunk and messages show
/// the UTC time of day under a `<day>` line for every new date: short ids and
/// times cost a third fewer tokens than message ids and full timestamps.
#[must_use]
pub fn events_payload(input: &SummaryInput, chunk: &[SummaryInputItem]) -> String {
    let mut out = format!(
        "<window>\n{}\n</window>\n<items>\n",
        window_header(input, chunk.len())
    );
    let mut day = None;
    for (index, item) in chunk.iter().enumerate() {
        if let Some(at) = utc(item.at).filter(|_| !is_summary_item(item))
            && day != Some(at.date())
        {
            day = Some(at.date());
            out.push_str(&format!("<day date=\"{}\"/>\n", at.date()));
        }
        out.push_str(&render_item(item, index + 1));
        out.push('\n');
    }
    out.push_str("</items>\n");
    out.push_str(EVENTS_TASK_LINE);
    out
}

/// Split the items into chunks of at most `max_tokens` estimated tokens, the
/// next chunk repeating the last `EVENTS_CHUNK_OVERLAP_PERCENT` of the previous
/// one so an event on a boundary is seen whole at least once.
#[must_use]
pub fn chunk_items(items: &[SummaryInputItem], max_tokens: i32) -> Vec<Vec<SummaryInputItem>> {
    let costs: Vec<i32> = items.iter().map(item_cost).collect();
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < items.len() {
        let mut end = start;
        let mut used = 0;
        while end < items.len() && (end == start || used + costs[end] <= max_tokens) {
            used += costs[end];
            end += 1;
        }
        chunks.push(items[start..end].to_vec());
        if end == items.len() {
            break;
        }
        let taken = end - start;
        start = if taken > 1 {
            end - (taken * EVENTS_CHUNK_OVERLAP_PERCENT / 100).clamp(1, taken - 1)
        } else {
            end
        };
    }
    chunks
}

/// Messages of a chunk that can carry an event.
#[must_use]
pub fn substantive_message_count(chunk: &[SummaryInputItem]) -> usize {
    chunk
        .iter()
        .filter(|item| !is_summary_item(item) && item_carries_event(item))
        .count()
}

/// A stage-one event as the model wrote it.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct StageEvent {
    #[serde(default)]
    pub source_ids: Vec<Value>,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub actors: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_f64_loose")]
    pub confidence: f64,
}

/// Stage-one answer for one chunk.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct StageEvents {
    #[serde(default)]
    pub events: Vec<StageEvent>,
    #[serde(default)]
    pub nothing_notable: bool,
}

/// Stage-two answer: the parts of the summary written from the events.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct StageRecap {
    #[serde(default)]
    pub recap: String,
    #[serde(default)]
    pub actors: Vec<SummaryActor>,
    #[serde(default)]
    pub open_questions: Vec<String>,
    #[serde(default)]
    pub source_style: String,
    #[serde(default, deserialize_with = "deserialize_f64_loose")]
    pub quality_score: f64,
    #[serde(default)]
    pub quality_notes: String,
}

/// An event checked against its chunk.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryEvent {
    /// Ids of chunk items the event rests on.
    pub source_ids: Vec<String>,
    /// Time of the earliest source; never taken from the model.
    pub occurred_at: Option<OffsetDateTime>,
    pub title: String,
    pub description: String,
    pub actors: Vec<String>,
    pub confidence: f64,
}

fn json_object(raw: &str) -> Result<&str, HistorySummaryDecodeError> {
    let trimmed = raw.trim();
    match (trimmed.find('{'), trimmed.rfind('}')) {
        (Some(start), Some(end)) if end > start => Ok(&trimmed[start..=end]),
        _ => Err(HistorySummaryDecodeError::EmptySummaryJson),
    }
}

/// Decode a stage-one answer.
pub fn decode_stage_events(raw: &str) -> Result<StageEvents, HistorySummaryDecodeError> {
    serde_json::from_str(json_object(raw)?).map_err(HistorySummaryDecodeError::Json)
}

/// Decode a stage-two answer.
pub fn decode_stage_recap(raw: &str) -> Result<StageRecap, HistorySummaryDecodeError> {
    let recap: StageRecap =
        serde_json::from_str(json_object(raw)?).map_err(HistorySummaryDecodeError::Json)?;
    if recap.recap.trim().is_empty() {
        return Err(HistorySummaryDecodeError::EmptySummaryJson);
    }
    Ok(recap)
}

fn source_id_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.trim().to_owned()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
    .filter(|id| !id.is_empty())
}

/// Keep the events whose sources are positions in the chunk, at most
/// `EVENTS_MAX_PER_CHUNK`, each citing the items' own ids and dated by its
/// earliest source.
#[must_use]
pub fn validated_events(chunk: &[SummaryInputItem], answer: StageEvents) -> Vec<HistoryEvent> {
    let positions: HashMap<String, &SummaryInputItem> = chunk
        .iter()
        .enumerate()
        .map(|(index, item)| ((index + 1).to_string(), item))
        .collect();
    answer
        .events
        .into_iter()
        .filter_map(|event| {
            let mut sources: Vec<&SummaryInputItem> = Vec::new();
            for item in event
                .source_ids
                .iter()
                .filter_map(source_id_text)
                .filter_map(|id| positions.get(&id).copied())
            {
                if !sources.iter().any(|kept| std::ptr::eq(*kept, item)) {
                    sources.push(item);
                }
            }
            let title = event.title.trim().to_owned();
            if sources.is_empty() || title.is_empty() {
                return None;
            }
            let occurred_at = sources
                .iter()
                .map(|item| item_time(item))
                .filter(|at| *at != go_zero_time())
                .min();
            let source_ids = sources.iter().map(|item| item_source_id(item)).collect();
            Some(HistoryEvent {
                source_ids,
                occurred_at,
                title,
                description: event.description.trim().to_owned(),
                actors: event
                    .actors
                    .into_iter()
                    .map(|actor| actor.trim().to_owned())
                    .filter(|actor| !actor.is_empty())
                    .collect(),
                confidence: event.confidence.clamp(0.0, 1.0),
            })
        })
        .take(EVENTS_MAX_PER_CHUNK)
        .collect()
}

/// Merge the events of all chunks: an event sharing a source with a kept one
/// replaces it only when more confident; the result is in time order.
#[must_use]
pub fn merge_events(chunks: Vec<Vec<HistoryEvent>>) -> Vec<HistoryEvent> {
    let mut merged: Vec<HistoryEvent> = Vec::new();
    for event in chunks.into_iter().flatten() {
        match merged.iter_mut().find(|kept| {
            kept.source_ids
                .iter()
                .any(|id| event.source_ids.contains(id))
        }) {
            Some(kept) if event.confidence > kept.confidence => *kept = event,
            Some(_) => {}
            None => merged.push(event),
        }
    }
    merged.sort_by_key(|event| event.occurred_at.unwrap_or(OffsetDateTime::UNIX_EPOCH));
    merged
}

fn ru_plural(count: i64, one: &str, few: &str, many: &str) -> String {
    let tail = count % 100;
    let word = if (11..=14).contains(&tail) {
        many
    } else {
        match count % 10 {
            1 => one,
            2..=4 => few,
            _ => many,
        }
    };
    format!("{count} {word}")
}

/// Relative time next to the ISO time, so the model does not do date arithmetic.
#[must_use]
pub fn relative_time_label(at: OffsetDateTime, now: OffsetDateTime) -> String {
    let elapsed = now - at;
    let minutes = elapsed.whole_minutes();
    let hours = elapsed.whole_hours();
    let days = elapsed.whole_days();
    if minutes < 1 {
        "только что".to_owned()
    } else if minutes < 60 {
        format!("{} назад", ru_plural(minutes, "минуту", "минуты", "минут"))
    } else if hours < 24 {
        format!("{} назад", ru_plural(hours, "час", "часа", "часов"))
    } else if days < 2 {
        "вчера".to_owned()
    } else if days < 30 {
        format!("{} назад", ru_plural(days, "день", "дня", "дней"))
    } else {
        format!(
            "{} назад",
            ru_plural(days / 30, "месяц", "месяца", "месяцев")
        )
    }
}

/// Stage-two user message: the window header, one line per merged event with
/// its time and a relative label, then the task.
#[must_use]
pub fn recap_payload(input: &SummaryInput, events: &[HistoryEvent], now: OffsetDateTime) -> String {
    let mut out = format!(
        "<window>\n{}\n</window>\n<events>\n",
        window_header(input, events.len())
    );
    for (index, event) in events.iter().enumerate() {
        out.push_str(&format!("<event id=\"E{}\"", index + 1));
        if let Some(at) = event.occurred_at {
            out.push_str(&format!(
                " at=\"{}\" when=\"{}\"",
                iso(at).unwrap_or_default(),
                relative_time_label(at, now)
            ));
        }
        if !event.actors.is_empty() {
            out.push_str(&format!(
                " actors=\"{}\"",
                escape_attr(&event.actors.join(", "))
            ));
        }
        out.push('>');
        out.push_str(&escape_text(&event.title));
        if !event.description.is_empty() {
            out.push_str(" — ");
            out.push_str(&escape_text(&event.description));
        }
        out.push_str("</event>\n");
    }
    out.push_str("</events>\n");
    out.push_str(RECAP_TASK_LINE);
    out
}

/// The stored summary: events and their details come from stage one only, the
/// rest from stage two, so the recap step cannot add an event. Without events
/// the summary says the chat was quiet.
#[must_use]
pub fn summary_content(events: &[HistoryEvent], recap: Option<StageRecap>) -> SummaryContent {
    let recap = match recap {
        Some(recap) if !events.is_empty() => recap,
        _ => StageRecap {
            recap: QUIET_RECAP.to_owned(),
            ..StageRecap::default()
        },
    };
    SummaryContent {
        events: events.iter().map(|event| event.title.clone()).collect(),
        event_details: events
            .iter()
            .map(|event| SummaryEvent {
                title: event.title.clone(),
                description: event.description.clone(),
                actors: event.actors.clone(),
                occurred_at: event.occurred_at.and_then(iso).unwrap_or_default(),
                confidence: event.confidence,
            })
            .collect(),
        actors: recap.actors,
        recap: recap.recap.trim().to_owned(),
        open_questions: recap.open_questions,
        source_style: recap.source_style.trim().to_owned(),
        quality_score: recap.quality_score.clamp(0.0, 1.0),
        quality_notes: recap.quality_notes.trim().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(minutes: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_758_000_000).expect("time")
            + time::Duration::minutes(minutes)
    }

    fn message(id: i32, from: &str, text: &str, minutes: i64) -> SummaryInputItem {
        SummaryInputItem {
            kind: "message".to_owned(),
            at: at(minutes),
            message_id: id,
            sender_name: from.to_owned(),
            text: text.to_owned(),
            ..SummaryInputItem::default()
        }
    }

    fn window() -> SummaryInput {
        SummaryInput {
            range_start_at: at(0),
            range_end_at: at(600),
            ..SummaryInput::default()
        }
    }

    #[test]
    fn summary_prompt_payload_numbers_items_under_day_lines() {
        let items = vec![
            message(
                10,
                "Аня \"А\"",
                "Решили:\nвстречаемся по пятницам <в 19:00>",
                5,
            ),
            message(11, "Дима", "Поддерживаю", 1200),
            SummaryInputItem {
                kind: "summary".to_owned(),
                summary_id: 45,
                range_start_at: at(-600),
                range_end_at: at(-10),
                summary_json: SummaryContent {
                    recap: "Вчера обсуждали поход".to_owned(),
                    ..SummaryContent::default()
                },
                ..SummaryInputItem::default()
            },
        ];
        let payload = events_payload(&window(), &items);
        assert!(
            payload.contains(concat!(
                "<items>\n<day date=\"2025-09-16\"/>\n",
                r#"<msg id="1" at="05:25" from="Аня &quot;А&quot;">Решили: встречаемся по пятницам &lt;в 19:00></msg>"#,
                "\n<day date=\"2025-09-17\"/>\n",
                r#"<msg id="2" at="01:20" from="Дима">Поддерживаю</msg>"#,
                "\n",
                r#"<summary id="3" from="2025-09-15T19:20:00Z" to="2025-09-16T05:10:00Z">Вчера обсуждали поход</summary>"#,
            )),
            "{payload}"
        );
        assert!(payload.ends_with(EVENTS_TASK_LINE));
        assert!(!payload.contains("[2025,"), "{payload}");
        assert!(payload.find("<window>") < payload.find("<items>"));
    }

    #[test]
    fn long_window_is_chunked_with_overlap() {
        let items: Vec<SummaryInputItem> = (0..2000)
            .map(|index| {
                message(
                    index,
                    "Участник",
                    "сообщение средней длины о планах на выходные и работе",
                    i64::from(index),
                )
            })
            .collect();
        let chunks = chunk_items(&items, EVENTS_CHUNK_MAX_TOKENS);
        assert!(chunks.len() > 1);
        let covered: std::collections::HashSet<i32> = chunks
            .iter()
            .flatten()
            .map(|item| item.message_id)
            .collect();
        assert_eq!(covered.len(), 2000, "every message is in some chunk");
        for pair in chunks.windows(2) {
            let last = pair[0].last().expect("chunk").message_id;
            assert!(
                pair[1].iter().any(|item| item.message_id == last),
                "chunks overlap"
            );
        }
        for chunk in &chunks {
            let tokens: i32 = chunk.iter().map(item_cost).sum();
            assert!(tokens <= EVENTS_CHUNK_MAX_TOKENS);
        }
    }

    #[test]
    fn oversized_items_get_a_chunk_each() {
        let long = "длинное сообщение ".repeat(400);
        let items: Vec<SummaryInputItem> = (0..3)
            .map(|index| message(index, "A", &long, i64::from(index)))
            .collect();
        let chunks = chunk_items(&items, 100);
        let ids: Vec<Vec<i32>> = chunks
            .iter()
            .map(|chunk| chunk.iter().map(|item| item.message_id).collect())
            .collect();
        assert_eq!(ids, vec![vec![0], vec![1], vec![2]]);
    }

    #[test]
    fn triage_skips_commands_reactions_and_one_word_replies() {
        let carries = |text: &str| item_carries_event(&message(1, "A", text, 0));
        assert!(!carries("/summary за день"));
        assert!(!carries("😂😂"));
        assert!(!carries("ок"));
        assert!(!carries("спасибо!"));
        assert!(carries("мне 30"));
        assert!(carries("Переезжаем в субботу"));
        let image = SummaryInputItem {
            vision_description: "кот спит на ноутбуке".to_owned(),
            ..message(2, "A", "", 0)
        };
        assert!(item_carries_event(&image));
    }

    #[test]
    fn stage_one_positions_map_back_to_message_ids() {
        let chunk = vec![
            message(10, "Аня", "Решили встречаться по пятницам", 5),
            message(11, "Дима", "Поддерживаю, в 19:00", 7),
        ];
        let answer = decode_stage_events(
            r#"```json
{"events":[{"source_ids":["2",1,"2"],"title":"Встречи по пятницам","description":"Решили встречаться по пятницам в 19:00","actors":["Аня","Дима"],"confidence":0.9},{"source_ids":["3","10"],"title":"Выдумка","confidence":0.9},{"source_ids":["1"],"title":"  "}],"nothing_notable":false}
```"#,
        )
        .expect("decode");
        let events = validated_events(&chunk, answer);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source_ids, vec!["11", "10"]);
        assert_eq!(events[0].occurred_at, Some(at(5)));
    }

    #[test]
    fn merge_keeps_the_more_confident_copy_and_orders_by_time() {
        let event = |ids: &[&str], title: &str, confidence: f64, minutes: i64| HistoryEvent {
            source_ids: ids.iter().map(|id| (*id).to_owned()).collect(),
            occurred_at: Some(at(minutes)),
            title: title.to_owned(),
            description: String::new(),
            actors: Vec::new(),
            confidence,
        };
        let merged = merge_events(vec![
            vec![
                event(&["5", "6"], "поздняя тема", 0.5, 50),
                event(&["1"], "ранняя тема", 0.9, 1),
            ],
            vec![event(&["6", "7"], "поздняя тема, точнее", 0.8, 50)],
        ]);
        let titles: Vec<&str> = merged.iter().map(|event| event.title.as_str()).collect();
        assert_eq!(titles, vec!["ранняя тема", "поздняя тема, точнее"]);
    }

    #[test]
    fn stage_two_never_invents_events() {
        let events = vec![HistoryEvent {
            source_ids: vec!["10".to_owned()],
            occurred_at: Some(at(5)),
            title: "Встречи по пятницам".to_owned(),
            description: "Решили встречаться по пятницам".to_owned(),
            actors: vec!["Аня".to_owned()],
            confidence: 0.9,
        }];
        let recap = decode_stage_recap(
            r#"{"recap":"Хроника: группа договорилась о пятницах.","actors":[{"name":"Аня","description":"предложила"}],"open_questions":[],"source_style":"хроника","quality_score":0.8,"quality_notes":"","events":["лишнее событие"],"event_details":[{"title":"лишнее событие"}]}"#,
        )
        .expect("decode");
        let content = summary_content(&events, Some(recap));
        assert_eq!(content.events, vec!["Встречи по пятницам"]);
        assert_eq!(content.event_details.len(), 1);
        assert_eq!(content.event_details[0].occurred_at, "2025-09-16T05:25:00Z");
        assert_eq!(content.source_style, "хроника");
    }

    #[test]
    fn quiet_window_yields_empty_summary() {
        let content = summary_content(&[], None);
        assert!(content.events.is_empty());
        assert!(content.event_details.is_empty());
        assert_eq!(content.recap, QUIET_RECAP);
        let chunk = vec![message(1, "A", "ок", 0), message(2, "B", "+", 1)];
        assert_eq!(substantive_message_count(&chunk), 0);
    }

    #[test]
    fn eval_harness_stage_schemas_match_the_requests() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tools/prompt-eval/schemas");
        for (file, stage) in [
            ("history_events.json", HistoryStage::Events),
            ("history_recap.json", HistoryStage::Recap),
        ] {
            let text = std::fs::read_to_string(dir.join(file)).expect("harness schema file");
            let harness: Value = serde_json::from_str(&text).expect("harness schema json");
            assert_eq!(
                harness,
                stage.response_schema(),
                "{file} drifted from the request schema"
            );
        }
    }

    #[test]
    fn relative_labels_use_russian_plurals() {
        assert_eq!(relative_time_label(at(0), at(0)), "только что");
        assert_eq!(relative_time_label(at(0), at(21)), "21 минуту назад");
        assert_eq!(relative_time_label(at(0), at(180)), "3 часа назад");
        assert_eq!(relative_time_label(at(0), at(60 * 30)), "вчера");
        assert_eq!(relative_time_label(at(0), at(60 * 24 * 5)), "5 дней назад");
        let payload = recap_payload(
            &window(),
            &[HistoryEvent {
                source_ids: vec!["1".to_owned()],
                occurred_at: Some(at(0)),
                title: "Тема".to_owned(),
                description: "подробности".to_owned(),
                actors: vec!["Аня".to_owned()],
                confidence: 0.5,
            }],
            at(180),
        );
        assert!(
            payload.contains(r#"<event id="E1" at="2025-09-16T05:20:00Z" when="3 часа назад" actors="Аня">Тема — подробности</event>"#),
            "{payload}"
        );
        assert!(payload.ends_with(RECAP_TASK_LINE));
    }
}
