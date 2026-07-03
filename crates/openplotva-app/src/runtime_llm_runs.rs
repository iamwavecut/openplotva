//! In-memory agent-run records for the admin "LLM Dialogs" section.
//!
//! One [`RunRecord`] groups every LLM round-trip of an agent run — a dialog
//! session, a song/image prompt-optimizer run, or a console turn — plus its
//! circumstances and outcome. Skeletons live here; full raw payloads resolve
//! from the trace ring (or the DB fallback) at detail time.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

/// Default capacity of the closed-run ring (`LLM_RUN_BUFFER_CAPACITY`).
pub const DEFAULT_LLM_RUN_BUFFER_CAPACITY: usize = 512;

/// Per-round response text kept on the skeleton.
const RUN_ROUND_RESPONSE_TEXT_MAX_CHARS: usize = 8_000;

/// List-level preview length of the final response.
const RUN_PREVIEW_MAX_CHARS: usize = 200;

/// Open runs older than this are swept into the ring as `Abandoned` (crashed
/// callers, lost finishes).
const RUN_OPEN_MAX_AGE: Duration = Duration::minutes(30);

/// Where the run originated: the chat/user/message that triggered it plus the
/// queue that carried it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RunOrigin {
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    pub chat_title: Option<String>,
    pub user_id: i64,
    pub user_full_name: Option<String>,
    pub trigger_message_id: i32,
    pub trigger_preview: Option<String>,
    pub queue_name: Option<String>,
    pub job_id: Option<i64>,
}

/// Lifecycle state of a run record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Abandoned,
}

impl RunStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

/// One executed tool call attached to a round.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RunToolCall {
    pub name: String,
    pub status: String,
    pub duration_ms: Option<i64>,
}

/// Whether a round's text reached the chat.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RunRoundSent {
    #[default]
    No,
    Intermediate,
    Final,
}

impl RunRoundSent {
    #[must_use]
    pub fn as_str(self) -> Option<&'static str> {
        match self {
            Self::No => None,
            Self::Intermediate => Some("intermediate"),
            Self::Final => Some("final"),
        }
    }
}

/// One LLM round-trip inside a run.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RunRound {
    /// 1-based ordinal within the run; mirrors `llm_request_events.run_seq`.
    pub seq: i32,
    /// Trace-ring id of the underlying request (raw payload lookup).
    pub trace_id: i64,
    pub at: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub flow: Option<String>,
    /// Nested auxiliary call (e.g. vision materialization inside a dialog).
    pub is_aux: bool,
    pub iteration: i32,
    pub duration_ms: i32,
    pub input_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
    pub total_tokens: Option<i32>,
    pub error: Option<String>,
    /// Response text capped at [`RUN_ROUND_RESPONSE_TEXT_MAX_CHARS`].
    pub response_text: Option<String>,
    pub sent: RunRoundSent,
    pub tool_calls: Vec<RunToolCall>,
}

/// Aggregates over the run's rounds.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RunTotals {
    pub rounds: i32,
    pub aux_rounds: i32,
    pub tool_calls: i32,
    pub errors: i32,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub wall_ms: Option<i64>,
}

/// Ledger enrichment for dialog runs.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RunOutcome {
    pub outcome: String,
    pub reason: Option<String>,
    pub user_signal: Option<String>,
    pub sent_message_parts: Option<i32>,
    pub side_effect_ticket_id: Option<i64>,
    pub detail: serde_json::Value,
}

/// One agent run: skeleton metadata plus its rounds.
#[derive(Clone, Debug, PartialEq)]
pub struct RunRecord {
    /// Buffer-local id (resets on restart) — the admin detail lookup key.
    pub id: i64,
    pub run_id: String,
    pub kind: String,
    pub origin: RunOrigin,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub status: RunStatus,
    pub error: Option<String>,
    pub rounds: Vec<RunRound>,
    pub totals: RunTotals,
    pub outcome: Option<RunOutcome>,
}

impl RunRecord {
    /// Final-response preview for the list skeleton.
    #[must_use]
    pub fn preview(&self) -> Option<String> {
        self.rounds
            .iter()
            .rev()
            .find_map(|round| round.response_text.as_deref())
            .map(|text| cap_chars(text, RUN_PREVIEW_MAX_CHARS))
    }
}

/// Server-side list filter (the admin UI filters client-side in v1; these are
/// honored for API consumers and future deep search).
#[derive(Clone, Debug, Default)]
pub struct RunListFilter {
    pub kind: Option<String>,
    pub chat_id: Option<i64>,
    pub errors_only: bool,
    pub q: String,
    pub limit: usize,
}

struct Inner {
    open: BTreeMap<String, RunRecord>,
    ring: Vec<Option<RunRecord>>,
    write: usize,
    count: usize,
}

/// Ring of agent-run records fed by the LLM observer and the run lifecycle
/// call sites. All methods take the mutex briefly and never hold it across an
/// await (there are none).
#[derive(Clone)]
pub struct RuntimeLlmRunBuffer {
    inner: Arc<Mutex<Inner>>,
    next_id: Arc<AtomicI64>,
}

impl RuntimeLlmRunBuffer {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                open: BTreeMap::new(),
                ring: vec![None; capacity],
                write: 0,
                count: 0,
            })),
            next_id: Arc::new(AtomicI64::new(0)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn next_id(&self) -> i64 {
        self.next_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Open a run. Re-opening an id that is still open (taskman re-delivery)
    /// force-closes the stale record as `Abandoned` first.
    pub fn begin_run(&self, run_id: String, kind: &str, origin: RunOrigin, now: OffsetDateTime) {
        let record = RunRecord {
            id: self.next_id(),
            run_id: run_id.clone(),
            kind: kind.to_owned(),
            origin,
            started_at: format_ts(now),
            ended_at: None,
            status: RunStatus::Running,
            error: None,
            rounds: Vec::new(),
            totals: RunTotals::default(),
            outcome: None,
        };
        let mut inner = self.lock();
        sweep_stale_locked(&mut inner, now);
        if let Some(mut stale) = inner.open.remove(&run_id) {
            stale.status = RunStatus::Abandoned;
            stale.ended_at = Some(format_ts(now));
            push_closed_locked(&mut inner, stale);
        }
        inner.open.insert(run_id, record);
    }

    /// Attach one LLM round to an open run; returns the 1-based round seq, or
    /// `None` when no such run is open (the caller records a one-off instead).
    pub fn record_round(&self, run_id: &str, mut round: RunRound) -> Option<i32> {
        let mut inner = self.lock();
        let record = inner.open.get_mut(run_id)?;
        let seq = i32::try_from(record.rounds.len()).unwrap_or(i32::MAX - 1) + 1;
        round.seq = seq;
        round.response_text = round
            .response_text
            .map(|text| cap_chars(&text, RUN_ROUND_RESPONSE_TEXT_MAX_CHARS));
        if record.origin.chat_title.is_none() {
            record.origin.chat_title = round.flow.as_ref().and(None);
        }
        record.totals.rounds += 1;
        if round.is_aux {
            record.totals.aux_rounds += 1;
        }
        if round.error.is_some() {
            record.totals.errors += 1;
        }
        record.totals.input_tokens += i64::from(round.input_tokens.unwrap_or(0));
        record.totals.output_tokens += i64::from(round.output_tokens.unwrap_or(0));
        record.totals.total_tokens += i64::from(round.total_tokens.unwrap_or(0));
        record.rounds.push(round);
        Some(seq)
    }

    /// Attach an executed tool result to the run's latest round.
    pub fn record_tool_result(&self, run_id: &str, tool: RunToolCall) {
        let mut inner = self.lock();
        let Some(record) = inner.open.get_mut(run_id) else {
            return;
        };
        record.totals.tool_calls += 1;
        if let Some(round) = record.rounds.last_mut() {
            round.tool_calls.push(tool);
        }
    }

    /// Mark the latest round's text as delivered to the chat.
    pub fn mark_round_sent(&self, run_id: &str, sent: RunRoundSent) {
        let mut inner = self.lock();
        if let Some(round) = inner
            .open
            .get_mut(run_id)
            .and_then(|record| record.rounds.last_mut())
        {
            round.sent = sent;
        }
    }

    /// Record an unscoped LLM call as a closed single-round run (kind = flow).
    pub fn record_one_off(
        &self,
        kind: &str,
        origin: RunOrigin,
        round: RunRound,
        now: OffsetDateTime,
    ) -> i64 {
        let id = self.next_id();
        let failed = round.error.is_some();
        let mut round = round;
        round.seq = 1;
        round.response_text = round
            .response_text
            .map(|text| cap_chars(&text, RUN_ROUND_RESPONSE_TEXT_MAX_CHARS));
        let record = RunRecord {
            id,
            run_id: format!("call-{}", round.trace_id),
            kind: kind.to_owned(),
            origin,
            started_at: round.at.clone(),
            ended_at: Some(format_ts(now)),
            status: if failed {
                RunStatus::Failed
            } else {
                RunStatus::Completed
            },
            error: round.error.clone(),
            totals: RunTotals {
                rounds: 1,
                aux_rounds: 0,
                tool_calls: 0,
                errors: i32::from(failed),
                input_tokens: i64::from(round.input_tokens.unwrap_or(0)),
                output_tokens: i64::from(round.output_tokens.unwrap_or(0)),
                total_tokens: i64::from(round.total_tokens.unwrap_or(0)),
                wall_ms: Some(i64::from(round.duration_ms)),
            },
            outcome: None,
            rounds: vec![round],
        };
        let mut inner = self.lock();
        push_closed_locked(&mut inner, record);
        id
    }

    /// Close an open run. No-op for unknown ids so merged/parked dialog turns
    /// never create phantom records.
    pub fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        outcome: Option<RunOutcome>,
        error: Option<String>,
        now: OffsetDateTime,
    ) {
        let mut inner = self.lock();
        let Some(mut record) = inner.open.remove(run_id) else {
            return;
        };
        record.status = status;
        record.outcome = outcome;
        if record.error.is_none() {
            record.error = error;
        }
        record.ended_at = Some(format_ts(now));
        if let Ok(started) = OffsetDateTime::parse(&record.started_at, &Rfc3339) {
            record.totals.wall_ms =
                Some(((now - started).whole_milliseconds()).clamp(0, i64::MAX as i128) as i64);
        }
        push_closed_locked(&mut inner, record);
    }

    /// Open (running) runs newest-first, then closed runs newest-first.
    #[must_use]
    pub fn list(&self, filter: &RunListFilter, now: OffsetDateTime) -> Vec<RunRecord> {
        let mut inner = self.lock();
        sweep_stale_locked(&mut inner, now);
        let limit = if filter.limit == 0 { 200 } else { filter.limit };
        let mut out: Vec<RunRecord> = Vec::new();
        let mut open: Vec<&RunRecord> = inner.open.values().collect();
        open.sort_by(|a, b| b.id.cmp(&a.id));
        let ring_len = inner.ring.len().max(1);
        let closed = (0..inner.count.min(inner.ring.len())).filter_map(|offset| {
            let idx = (inner.write + ring_len - 1 - offset) % ring_len;
            inner.ring[idx].as_ref()
        });
        for record in open.into_iter().chain(closed) {
            if !matches_filter(record, filter) {
                continue;
            }
            out.push(record.clone());
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Fetch one run (open or closed) by its buffer id.
    #[must_use]
    pub fn get(&self, id: i64) -> Option<RunRecord> {
        let inner = self.lock();
        inner
            .open
            .values()
            .find(|record| record.id == id)
            .or_else(|| inner.ring.iter().flatten().find(|record| record.id == id))
            .cloned()
    }

    /// Drop every record (admin clear).
    pub fn clear(&self) {
        let mut inner = self.lock();
        inner.open.clear();
        for slot in &mut inner.ring {
            *slot = None;
        }
        inner.write = 0;
        inner.count = 0;
    }

    /// Drop every record belonging to one chat (virtual-console cleanup).
    pub fn prune_chat(&self, chat_id: i64) -> i32 {
        let mut inner = self.lock();
        let before = inner.open.len();
        inner
            .open
            .retain(|_, record| record.origin.chat_id != chat_id);
        let mut pruned = (before - inner.open.len()) as i32;
        for slot in &mut inner.ring {
            if slot
                .as_ref()
                .is_some_and(|record| record.origin.chat_id == chat_id)
            {
                *slot = None;
                pruned += 1;
            }
        }
        pruned
    }
}

fn push_closed_locked(inner: &mut Inner, record: RunRecord) {
    if inner.ring.is_empty() {
        return;
    }
    let write = inner.write;
    inner.ring[write] = Some(record);
    inner.write = (write + 1) % inner.ring.len();
    inner.count = inner.count.saturating_add(1).min(inner.ring.len());
}

fn sweep_stale_locked(inner: &mut Inner, now: OffsetDateTime) {
    let stale: Vec<String> = inner
        .open
        .iter()
        .filter(|(_, record)| {
            OffsetDateTime::parse(&record.started_at, &Rfc3339)
                .map(|started| now - started > RUN_OPEN_MAX_AGE)
                .unwrap_or(false)
        })
        .map(|(run_id, _)| run_id.clone())
        .collect();
    for run_id in stale {
        if let Some(mut record) = inner.open.remove(&run_id) {
            record.status = RunStatus::Abandoned;
            record.ended_at = Some(format_ts(now));
            push_closed_locked(inner, record);
        }
    }
}

fn matches_filter(record: &RunRecord, filter: &RunListFilter) -> bool {
    if let Some(kind) = &filter.kind
        && !record.kind.eq_ignore_ascii_case(kind)
    {
        return false;
    }
    if let Some(chat_id) = filter.chat_id
        && record.origin.chat_id != chat_id
    {
        return false;
    }
    if filter.errors_only && record.status != RunStatus::Failed && record.error.is_none() {
        return false;
    }
    if !filter.q.is_empty() {
        let q = filter.q.to_lowercase();
        let hay = format!(
            "{} {} {} {} {} {} {}",
            record.run_id,
            record.kind,
            record.origin.chat_title.as_deref().unwrap_or(""),
            record.origin.chat_id,
            record.origin.user_id,
            record.error.as_deref().unwrap_or(""),
            record.preview().unwrap_or_default(),
        )
        .to_lowercase();
        if !hay.contains(&q) {
            return false;
        }
    }
    true
}

fn cap_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    let mut out: String = text.chars().take(max).collect();
    out.push('…');
    out
}

fn format_ts(at: OffsetDateTime) -> String {
    at.format(&Rfc3339)
        .unwrap_or_else(|_| at.unix_timestamp().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(offset_secs: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_779_193_800 + offset_secs).expect("ts")
    }

    fn round(text: &str) -> RunRound {
        RunRound {
            trace_id: 1,
            at: format_ts(ts(0)),
            provider: Some("aifarm".to_owned()),
            model: Some("gemma".to_owned()),
            flow: Some("dialog".to_owned()),
            duration_ms: 1200,
            input_tokens: Some(100),
            output_tokens: Some(20),
            total_tokens: Some(120),
            response_text: Some(text.to_owned()),
            ..RunRound::default()
        }
    }

    #[test]
    fn run_lifecycle_rounds_tools_sent_and_finish() {
        let buffer = RuntimeLlmRunBuffer::new(4);
        buffer.begin_run(
            "job-1".to_owned(),
            "dialog",
            RunOrigin {
                chat_id: -100,
                user_id: 42,
                trigger_message_id: 20,
                ..RunOrigin::default()
            },
            ts(0),
        );
        assert_eq!(buffer.record_round("job-1", round("щас гляну")), Some(1));
        buffer.record_tool_result(
            "job-1",
            RunToolCall {
                name: "web_search".to_owned(),
                status: "ok".to_owned(),
                duration_ms: Some(300),
            },
        );
        assert_eq!(buffer.record_round("job-1", round("готово")), Some(2));
        buffer.mark_round_sent("job-1", RunRoundSent::Final);
        buffer.finish_run(
            "job-1",
            RunStatus::Completed,
            Some(RunOutcome {
                outcome: "sent".to_owned(),
                sent_message_parts: Some(1),
                ..RunOutcome::default()
            }),
            None,
            ts(5),
        );

        let runs = buffer.list(&RunListFilter::default(), ts(6));
        assert_eq!(runs.len(), 1);
        let run = &runs[0];
        assert_eq!(run.status, RunStatus::Completed);
        assert_eq!(run.totals.rounds, 2);
        assert_eq!(run.totals.tool_calls, 1);
        assert_eq!(run.totals.total_tokens, 240);
        assert_eq!(run.totals.wall_ms, Some(5000));
        assert_eq!(run.rounds[0].tool_calls.len(), 1);
        assert_eq!(run.rounds[1].sent, RunRoundSent::Final);
        assert_eq!(run.preview().as_deref(), Some("готово"));
        assert_eq!(
            buffer.get(run.id).map(|r| r.run_id),
            Some("job-1".to_owned())
        );
    }

    #[test]
    fn unknown_run_ids_are_no_ops_and_one_offs_close_immediately() {
        let buffer = RuntimeLlmRunBuffer::new(4);
        assert_eq!(buffer.record_round("job-x", round("ignored")), None);
        buffer.finish_run("job-x", RunStatus::Completed, None, None, ts(0));
        assert!(buffer.list(&RunListFilter::default(), ts(1)).is_empty());

        let mut one_off = round("extracted");
        one_off.error = Some("boom".to_owned());
        buffer.record_one_off(
            "memory_extraction",
            RunOrigin {
                chat_id: -5,
                ..RunOrigin::default()
            },
            one_off,
            ts(2),
        );
        let runs = buffer.list(&RunListFilter::default(), ts(3));
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].kind, "memory_extraction");
        assert_eq!(runs[0].status, RunStatus::Failed);
        assert_eq!(runs[0].totals.rounds, 1);
    }

    #[test]
    fn re_begin_abandons_the_stale_run_and_watchdog_sweeps_old_ones() {
        let buffer = RuntimeLlmRunBuffer::new(4);
        buffer.begin_run("job-1".to_owned(), "dialog", RunOrigin::default(), ts(0));
        buffer.begin_run("job-1".to_owned(), "dialog", RunOrigin::default(), ts(1));
        let runs = buffer.list(&RunListFilter::default(), ts(2));
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].status, RunStatus::Running);
        assert_eq!(runs[1].status, RunStatus::Abandoned);

        // The still-open run ages past the watchdog window.
        let runs = buffer.list(&RunListFilter::default(), ts(31 * 60));
        assert!(runs.iter().all(|run| run.status == RunStatus::Abandoned));
    }

    #[test]
    fn ring_evicts_oldest_and_filters_apply() {
        let buffer = RuntimeLlmRunBuffer::new(2);
        for i in 0..3 {
            buffer.record_one_off(
                if i == 2 {
                    "vision"
                } else {
                    "memory_extraction"
                },
                RunOrigin {
                    chat_id: -1 - i,
                    ..RunOrigin::default()
                },
                round(&format!("r{i}")),
                ts(i),
            );
        }
        let runs = buffer.list(&RunListFilter::default(), ts(10));
        assert_eq!(runs.len(), 2, "capacity 2 keeps the newest two");
        assert_eq!(runs[0].kind, "vision");

        let only_vision = buffer.list(
            &RunListFilter {
                kind: Some("vision".to_owned()),
                ..RunListFilter::default()
            },
            ts(10),
        );
        assert_eq!(only_vision.len(), 1);

        assert_eq!(buffer.prune_chat(-3), 1);
        assert_eq!(buffer.list(&RunListFilter::default(), ts(10)).len(), 1);

        buffer.clear();
        assert!(buffer.list(&RunListFilter::default(), ts(10)).is_empty());
    }
}
