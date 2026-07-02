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

tokio::task_local! {
    /// Wall-clock deadline for the current turn's provider work, set by the
    /// engine around `provider.run_dialog` and read by `RouterChatProvider`
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
