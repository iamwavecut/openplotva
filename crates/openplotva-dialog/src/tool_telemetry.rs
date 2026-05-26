use std::{
    collections::BTreeMap,
    sync::{LazyLock, Mutex},
};

use serde::Serialize;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const DEFAULT_MAX_EVENTS: usize = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolTelemetryEvent {
    /// Event time.
    pub at: OffsetDateTime,
    /// Provider name.
    pub provider: String,
    /// Model name.
    pub model: String,
    /// Tool name.
    pub tool: String,
    /// Parser form.
    pub form: String,
    /// Parser outcome.
    pub outcome: String,
    /// Reason for ignored/error outcomes.
    pub reason: String,
    /// Dialog iteration.
    pub iteration: i32,
}

impl Default for ToolTelemetryEvent {
    fn default() -> Self {
        Self {
            at: OffsetDateTime::UNIX_EPOCH,
            provider: String::new(),
            model: String::new(),
            tool: String::new(),
            form: String::new(),
            outcome: String::new(),
            reason: String::new(),
            iteration: 0,
        }
    }
}

impl ToolTelemetryEvent {
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let mut root = serde_json::Map::new();
        root.insert("at".to_owned(), serde_json::json!(format_time(self.at)));
        insert_non_empty(&mut root, "provider", &self.provider);
        insert_non_empty(&mut root, "model", &self.model);
        insert_non_empty(&mut root, "tool", &self.tool);
        insert_non_empty(&mut root, "form", &self.form);
        insert_non_empty(&mut root, "outcome", &self.outcome);
        insert_non_empty(&mut root, "reason", &self.reason);
        if self.iteration != 0 {
            root.insert("iteration".to_owned(), serde_json::json!(self.iteration));
        }
        serde_json::Value::Object(root)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolTelemetryCounter {
    /// Counter key.
    pub key: String,
    /// Count.
    pub count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolTelemetrySnapshot {
    /// Since timestamp.
    pub since: String,
    /// Total filtered events.
    pub total: usize,
    /// Outcome counters.
    pub by_outcome: Vec<ToolTelemetryCounter>,
    /// Parser-form counters.
    pub by_form: Vec<ToolTelemetryCounter>,
    /// Tool counters.
    pub by_tool: Vec<ToolTelemetryCounter>,
    /// Recent events in chronological order.
    pub recent: Vec<ToolTelemetryEvent>,
}

impl ToolTelemetrySnapshot {
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "since": self.since,
            "total": self.total,
            "by_outcome": self.by_outcome,
            "by_form": self.by_form,
            "by_tool": self.by_tool,
            "recent": self.recent.iter().map(ToolTelemetryEvent::to_json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug)]
struct ToolTelemetryRecorder {
    max_events: usize,
    events: Vec<ToolTelemetryEvent>,
    next: usize,
    filled: bool,
}

impl ToolTelemetryRecorder {
    fn new(max_events: usize) -> Self {
        let max_events = max_events.max(1);
        Self {
            max_events,
            events: Vec::with_capacity(max_events),
            next: 0,
            filled: false,
        }
    }

    fn record(&mut self, mut event: ToolTelemetryEvent) {
        if event.at == OffsetDateTime::UNIX_EPOCH {
            event.at = OffsetDateTime::now_utc();
        }
        event.provider = event.provider.trim().to_owned();
        event.model = event.model.trim().to_owned();
        event.tool = event.tool.trim().to_owned();
        event.form = event.form.trim().to_owned();
        event.outcome = event.outcome.trim().to_owned();
        event.reason = event.reason.trim().to_owned();
        if self.events.len() < self.max_events {
            self.events.push(event);
            return;
        }
        self.events[self.next] = event;
        self.next = (self.next + 1) % self.max_events;
        self.filled = true;
    }

    fn snapshot_since(&self, since: OffsetDateTime, recent_limit: usize) -> ToolTelemetrySnapshot {
        let recent_limit = if recent_limit == 0 { 20 } else { recent_limit };
        let mut filtered = Vec::with_capacity(self.events.len());
        let mut by_outcome = BTreeMap::new();
        let mut by_form = BTreeMap::new();
        let mut by_tool = BTreeMap::new();
        for event in self.ordered_events() {
            if event.at < since {
                continue;
            }
            increment_counter(&mut by_outcome, &event.outcome);
            if !event.form.trim().is_empty() {
                increment_counter(&mut by_form, &event.form);
            }
            if !event.tool.trim().is_empty() {
                increment_counter(&mut by_tool, &event.tool);
            }
            filtered.push(event);
        }
        let recent_start = filtered.len().saturating_sub(recent_limit);
        ToolTelemetrySnapshot {
            since: format_time(since),
            total: filtered.len(),
            by_outcome: sorted_counters(by_outcome),
            by_form: sorted_counters(by_form),
            by_tool: sorted_counters(by_tool),
            recent: filtered[recent_start..].to_vec(),
        }
    }

    fn ordered_events(&self) -> Vec<ToolTelemetryEvent> {
        if self.events.is_empty() {
            return Vec::new();
        }
        if !self.filled {
            return self.events.clone();
        }
        let mut out = Vec::with_capacity(self.events.len());
        out.extend_from_slice(&self.events[self.next..]);
        out.extend_from_slice(&self.events[..self.next]);
        out
    }
}

static DEFAULT_RECORDER: LazyLock<Mutex<ToolTelemetryRecorder>> =
    LazyLock::new(|| Mutex::new(ToolTelemetryRecorder::new(DEFAULT_MAX_EVENTS)));

pub fn record(event: ToolTelemetryEvent) {
    if let Ok(mut recorder) = DEFAULT_RECORDER.lock() {
        recorder.record(event);
    }
}

#[must_use]
pub fn snapshot_since(since: OffsetDateTime, recent_limit: usize) -> ToolTelemetrySnapshot {
    if let Ok(recorder) = DEFAULT_RECORDER.lock() {
        return recorder.snapshot_since(since, recent_limit);
    }
    ToolTelemetryRecorder::new(DEFAULT_MAX_EVENTS).snapshot_since(since, recent_limit)
}

fn increment_counter(counters: &mut BTreeMap<String, i64>, key: &str) {
    let key = key.trim();
    let key = if key.is_empty() { "unknown" } else { key };
    *counters.entry(key.to_owned()).or_default() += 1;
}

fn sorted_counters(counters: BTreeMap<String, i64>) -> Vec<ToolTelemetryCounter> {
    let mut out = counters
        .into_iter()
        .map(|(key, count)| ToolTelemetryCounter { key, count })
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.key.cmp(&right.key))
    });
    out
}

fn insert_non_empty(root: &mut serde_json::Map<String, serde_json::Value>, key: &str, value: &str) {
    if !value.trim().is_empty() {
        root.insert(key.to_owned(), serde_json::json!(value));
    }
}

fn format_time(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).unwrap_or_else(|_| value.to_string())
}

#[doc(hidden)]
pub fn clear_for_tests() {
    if let Ok(mut recorder) = DEFAULT_RECORDER.lock() {
        *recorder = ToolTelemetryRecorder::new(DEFAULT_MAX_EVENTS);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Duration;

    #[test]
    fn snapshot_since_aggregates_recent_events_like_go() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid unix timestamp");
        let mut recorder = ToolTelemetryRecorder::new(10);
        recorder.record(ToolTelemetryEvent {
            at: now - Duration::minutes(2),
            outcome: "detected".to_owned(),
            form: "fenced".to_owned(),
            tool: "draw_image".to_owned(),
            ..ToolTelemetryEvent::default()
        });
        recorder.record(ToolTelemetryEvent {
            at: now - Duration::minutes(1),
            outcome: "executed".to_owned(),
            form: "fenced".to_owned(),
            tool: "draw_image".to_owned(),
            ..ToolTelemetryEvent::default()
        });
        recorder.record(ToolTelemetryEvent {
            at: now - Duration::hours(1),
            outcome: "ignored".to_owned(),
            form: "json".to_owned(),
            tool: "do_magic".to_owned(),
            ..ToolTelemetryEvent::default()
        });

        let snapshot = recorder.snapshot_since(now - Duration::minutes(5), 10);

        assert_eq!(snapshot.total, 2);
        assert_eq!(snapshot.by_outcome[0].key, "detected");
        assert_eq!(snapshot.by_tool[0].key, "draw_image");
        assert_eq!(snapshot.by_tool[0].count, 2);
        assert_eq!(snapshot.recent.len(), 2);
    }

    #[test]
    fn recorder_keeps_ring_order_like_go() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid unix timestamp");
        let mut recorder = ToolTelemetryRecorder::new(2);
        recorder.record(ToolTelemetryEvent {
            at: now,
            outcome: "first".to_owned(),
            ..ToolTelemetryEvent::default()
        });
        recorder.record(ToolTelemetryEvent {
            at: now + Duration::seconds(1),
            outcome: "second".to_owned(),
            ..ToolTelemetryEvent::default()
        });
        recorder.record(ToolTelemetryEvent {
            at: now + Duration::seconds(2),
            outcome: "third".to_owned(),
            ..ToolTelemetryEvent::default()
        });

        let snapshot = recorder.snapshot_since(now - Duration::seconds(1), 10);

        assert_eq!(snapshot.recent[0].outcome, "second");
        assert_eq!(snapshot.recent[1].outcome, "third");
    }

    #[test]
    fn snapshot_since_limits_recent_events_like_go() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid unix timestamp");
        let mut recorder = ToolTelemetryRecorder::new(3);
        for (idx, outcome) in ["first", "second", "third"].into_iter().enumerate() {
            recorder.record(ToolTelemetryEvent {
                at: now + Duration::seconds(idx as i64),
                outcome: outcome.to_owned(),
                ..ToolTelemetryEvent::default()
            });
        }

        let snapshot = recorder.snapshot_since(now - Duration::seconds(1), 1);

        assert_eq!(snapshot.total, 3);
        assert_eq!(snapshot.recent.len(), 1);
        assert_eq!(snapshot.recent[0].outcome, "third");
    }
}
