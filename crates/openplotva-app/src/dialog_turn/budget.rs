//! Turn budget anchored at the job's first processing start.
//!
//! The anchor rides a `turn_started` job event (events survive requeues), so
//! the budget spans all attempts of one turn while queue wait before the first
//! dequeue is governed separately by the queue-age gate.

use std::time::Instant;

use openplotva_taskman::TaskQueueJobEvent;
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

/// Job event stage marking when a job first began processing.
pub const TURN_STARTED_STAGE: &str = "turn_started";

/// Job event stage recorded for each executed session tool.
pub const SESSION_TOOL_STAGE: &str = "session_tool";

/// Durable tool-event flag indicating that this call extended the session budget.
pub const SESSION_TOOL_BUDGET_EXTENSION_GRANTED_KEY: &str = "budget_extension_granted";

tokio::task_local! {
    /// Wall-clock deadline for the current turn's provider work, set by the
    /// engine around each chat step and read by `RouterChatProvider`
    /// when building the routed walker context.
    pub static TURN_DEADLINE: Option<Instant>;
}

/// Deadline for the current turn, when the engine set one on this task.
#[must_use]
pub fn current_turn_deadline() -> Option<Instant> {
    TURN_DEADLINE.try_with(|deadline| *deadline).ok().flatten()
}

/// Earliest `turn_started` event time, the durable processing-start anchor.
#[must_use]
pub fn turn_started_at(events: &[TaskQueueJobEvent]) -> Option<OffsetDateTime> {
    events
        .iter()
        .filter(|event| event.stage == TURN_STARTED_STAGE)
        .filter_map(|event| OffsetDateTime::parse(&event.at, &Rfc3339).ok())
        .min()
}

/// Wall-clock budget for one dialog turn across all of its attempts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TurnBudget {
    pub anchor: OffsetDateTime,
    pub limit: TimeDuration,
}

impl TurnBudget {
    /// Anchor at the earliest recorded `turn_started` event, or at `now` for
    /// a first attempt that has none yet.
    #[must_use]
    pub fn from_events(
        events: &[TaskQueueJobEvent],
        budget_secs: i32,
        now: OffsetDateTime,
    ) -> Self {
        Self {
            anchor: turn_started_at(events).unwrap_or(now),
            limit: TimeDuration::seconds(i64::from(budget_secs.max(1))),
        }
    }

    /// Restore the extension earned by successful toolbox calls recorded by
    /// earlier attempts of the same session.
    #[must_use]
    pub fn from_session_events(
        events: &[TaskQueueJobEvent],
        budget_secs: i32,
        per_tool_extension_secs: i32,
        hard_cap_secs: i32,
        now: OffsetDateTime,
    ) -> Self {
        let mut budget = Self::from_events(events, budget_secs, now);
        let successful_tools = events
            .iter()
            .filter(|event| event.stage == SESSION_TOOL_STAGE)
            .filter(|event| {
                event.data.get("status").is_some_and(|status| {
                    status.eq_ignore_ascii_case(openplotva_dialog::TOOL_RESULT_STATUS_OK)
                }) && event
                    .data
                    .get(SESSION_TOOL_BUDGET_EXTENSION_GRANTED_KEY)
                    .is_some_and(|granted| granted.eq_ignore_ascii_case("true"))
            })
            .count() as i64;
        let extension = TimeDuration::seconds(
            i64::from(per_tool_extension_secs.max(0)).saturating_mul(successful_tools),
        );
        let hard_cap = TimeDuration::seconds(i64::from(hard_cap_secs.max(1))).max(budget.limit);
        budget.limit = (budget.limit + extension).min(hard_cap);
        budget
    }

    #[must_use]
    pub fn deadline(&self) -> OffsetDateTime {
        self.anchor + self.limit
    }

    /// Remaining budget, saturating at zero.
    #[must_use]
    pub fn remaining(&self, now: OffsetDateTime) -> TimeDuration {
        (self.deadline() - now).max(TimeDuration::ZERO)
    }

    /// Time spent since the anchor, saturating at zero.
    #[must_use]
    pub fn elapsed(&self, now: OffsetDateTime) -> TimeDuration {
        (now - self.anchor).max(TimeDuration::ZERO)
    }
}

/// Session budget: the restored turn budget plus a bounded extension per
/// started toolbox call, hard-capped so a session can never outlive
/// `DIALOG_SESSION_HARD_CAP_SECS`. Successful calls are recorded as durable
/// events and restored by [`TurnBudget::from_session_events`] on a later
/// attempt; failed or interrupted calls keep only their in-memory extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionBudget {
    base: TurnBudget,
    extension: TimeDuration,
    per_tool_extension: TimeDuration,
    hard_cap: TimeDuration,
}

impl SessionBudget {
    #[must_use]
    pub fn new(base: TurnBudget, per_tool_extension_secs: i32, hard_cap_secs: i32) -> Self {
        Self {
            base,
            extension: TimeDuration::ZERO,
            per_tool_extension: TimeDuration::seconds(i64::from(per_tool_extension_secs.max(0))),
            hard_cap: TimeDuration::seconds(i64::from(hard_cap_secs.max(1))).max(base.limit),
        }
    }

    /// Grant the extension for one STARTED toolbox call. Tool-less turns never
    /// extend, so they keep exactly the legacy budget behavior.
    pub fn extend_for_tool_start(&mut self) {
        self.extension =
            (self.extension + self.per_tool_extension).min(self.hard_cap - self.base.limit);
    }

    #[must_use]
    pub fn limit(&self) -> TimeDuration {
        (self.base.limit + self.extension).min(self.hard_cap)
    }

    #[must_use]
    pub fn deadline(&self) -> OffsetDateTime {
        self.base.anchor + self.limit()
    }

    #[must_use]
    pub fn remaining(&self, now: OffsetDateTime) -> TimeDuration {
        (self.deadline() - now).max(TimeDuration::ZERO)
    }

    #[must_use]
    pub fn elapsed(&self, now: OffsetDateTime) -> TimeDuration {
        self.base.elapsed(now)
    }

    /// Milliseconds of granted extension, for the outcome ledger.
    #[must_use]
    pub fn extended_ms(&self) -> i64 {
        self.extension.whole_milliseconds().max(0) as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(stage: &str, at: &str) -> TaskQueueJobEvent {
        TaskQueueJobEvent {
            at: at.to_owned(),
            stage: stage.to_owned(),
            ..TaskQueueJobEvent::default()
        }
    }

    #[test]
    fn budget_anchors_at_earliest_turn_started_event() {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("timestamp");
        let events = vec![
            event("llm_job_retry", "2026-05-19T12:25:00Z"),
            event(TURN_STARTED_STAGE, "2026-05-19T12:28:30Z"),
            event(TURN_STARTED_STAGE, "2026-05-19T12:29:00Z"),
            event(TURN_STARTED_STAGE, "not a timestamp"),
        ];

        let budget = TurnBudget::from_events(&events, 120, now);

        assert_eq!(
            budget.anchor,
            OffsetDateTime::parse("2026-05-19T12:28:30Z", &Rfc3339).expect("anchor")
        );
        assert_eq!(budget.elapsed(now), TimeDuration::seconds(90));
        assert_eq!(budget.remaining(now), TimeDuration::seconds(30));
    }

    #[test]
    fn budget_without_anchor_event_starts_now_and_saturates() {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("timestamp");

        let budget = TurnBudget::from_events(&[], 120, now);

        assert_eq!(budget.anchor, now);
        assert_eq!(budget.remaining(now), TimeDuration::seconds(120));
        assert_eq!(
            budget.remaining(now + TimeDuration::seconds(500)),
            TimeDuration::ZERO
        );
        assert_eq!(
            budget.elapsed(now - TimeDuration::seconds(5)),
            TimeDuration::ZERO
        );
    }

    #[test]
    fn turn_budget_restores_successful_tool_extensions_across_attempts() {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("timestamp");
        let mut successful = event("session_tool", "2026-05-19T12:28:45Z");
        successful.data.insert("status".to_owned(), "ok".to_owned());
        successful
            .data
            .insert("budget_extension_granted".to_owned(), "true".to_owned());
        let mut cached = event("session_tool", "2026-05-19T12:29:00Z");
        cached.data.insert("status".to_owned(), "ok".to_owned());
        cached
            .data
            .insert("budget_extension_granted".to_owned(), "false".to_owned());
        let mut failed = event("session_tool", "2026-05-19T12:29:15Z");
        failed.data.insert("status".to_owned(), "failed".to_owned());
        failed
            .data
            .insert("budget_extension_granted".to_owned(), "true".to_owned());
        let events = vec![
            event(TURN_STARTED_STAGE, "2026-05-19T12:28:30Z"),
            successful.clone(),
            cached,
            failed,
            successful,
        ];

        let budget = TurnBudget::from_session_events(&events, 120, 60, 300, now);

        assert_eq!(budget.limit, TimeDuration::seconds(240));
        assert_eq!(budget.remaining(now), TimeDuration::seconds(150));
    }

    #[test]
    fn session_budget_extends_per_tool_and_hard_caps() {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("timestamp");
        let base = TurnBudget::from_events(&[], 120, now);
        let mut budget = SessionBudget::new(base, 60, 300);

        // Tool-less: exactly the legacy 120s.
        assert_eq!(budget.limit(), TimeDuration::seconds(120));
        assert_eq!(budget.remaining(now), TimeDuration::seconds(120));

        budget.extend_for_tool_start();
        assert_eq!(budget.limit(), TimeDuration::seconds(180));
        budget.extend_for_tool_start();
        assert_eq!(budget.limit(), TimeDuration::seconds(240));
        budget.extend_for_tool_start();
        assert_eq!(budget.limit(), TimeDuration::seconds(300));
        // The hard cap holds no matter how many tools start.
        budget.extend_for_tool_start();
        budget.extend_for_tool_start();
        assert_eq!(budget.limit(), TimeDuration::seconds(300));
        assert_eq!(budget.extended_ms(), 180_000);
        assert_eq!(budget.deadline(), now + TimeDuration::seconds(300));
    }

    #[test]
    fn session_budget_hard_cap_never_shrinks_the_base() {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("timestamp");
        let base = TurnBudget::from_events(&[], 120, now);
        // A misconfigured cap below the base clamps to the base.
        let budget = SessionBudget::new(base, 60, 30);
        assert_eq!(budget.limit(), TimeDuration::seconds(120));
    }

    #[tokio::test]
    async fn turn_deadline_task_local_is_scoped() {
        assert_eq!(current_turn_deadline(), None);
        let deadline = Instant::now();
        let observed = TURN_DEADLINE
            .scope(Some(deadline), async { current_turn_deadline() })
            .await;
        assert_eq!(observed, Some(deadline));
        assert_eq!(current_turn_deadline(), None);
    }
}
