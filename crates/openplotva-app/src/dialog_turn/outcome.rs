//! App-side resolution types for one dialog turn: every dequeued turn returns
//! exactly one [`TurnResolution`], and only `finalize_turn` acts on it.

use super::ledger::{
    TURN_OUTCOME_NO_REPLY_INTENTIONAL, TURN_OUTCOME_RETRY_SCHEDULED, TURN_OUTCOME_SENT,
    TURN_OUTCOME_SIDE_EFFECT_DELEGATED, TURN_OUTCOME_SKIPPED, TURN_OUTCOME_TERMINAL_FAILED,
};

/// Ledger reason recorded when a never-processed job outlived the queue-age gate.
pub const REASON_QUEUE_BACKLOG_EXPIRED: &str = "queue_backlog_expired";

/// The single mandatory result of one dialog turn. `#[must_use]` so the
/// compiler forbids resolving a turn without finalizing it.
#[derive(Clone, Debug, PartialEq)]
#[must_use]
pub struct TurnResolution {
    pub outcome: TurnOutcome,
    pub disposition: JobDisposition,
}

/// Classified per-attempt outcome of a dialog turn.
#[derive(Clone, Debug, PartialEq)]
pub enum TurnOutcome {
    /// A text answer was accepted by the outbound queue.
    Sent {
        parts: usize,
        side_effect_tickets: Vec<i64>,
    },
    /// Generation job(s) are the reply; no text was produced.
    SideEffectDelegated {
        tickets: Vec<i64>,
        kinds: Vec<String>,
    },
    /// The turn deliberately produced no reply.
    NoReplyIntentional { reason: &'static str },
    /// Non-final attempt outcome: the job was requeued for another try.
    RetryScheduled {
        reason: &'static str,
        attempt: i32,
        max_attempts: i32,
        target_queue: String,
    },
    /// The turn gave up. `user_signal` is the Phase 3 reaction plan; the
    /// ledger records "none" until reactions ship.
    TerminalFailed {
        reason: &'static str,
        error: String,
        user_signal: UserSignalPlan,
    },
    /// Job params failed to decode; the chat is unknown by definition.
    SkippedDecodeError { error: String },
    /// Nothing to answer: the trigger carried no payload.
    SkippedEmptyPayload,
    /// Never-processed job outlived `DIALOG_TURN_MAX_QUEUE_AGE_SECS`.
    SkippedQueueBacklog,
}

/// Taskman status transition finalize applies for a resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobDisposition {
    Complete,
    Fail(String),
    /// Requeue into the named queue for another attempt.
    Requeue(String),
}

/// How a terminal failure should be signalled to the user (Phase 3).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UserSignalPlan {
    React,
    SkipTooOld,
    SkipUnaddressable,
}

impl TurnOutcome {
    /// Ledger `outcome` column value.
    #[must_use]
    pub(crate) fn ledger_outcome(&self) -> &'static str {
        match self {
            Self::Sent { .. } => TURN_OUTCOME_SENT,
            Self::SideEffectDelegated { .. } => TURN_OUTCOME_SIDE_EFFECT_DELEGATED,
            Self::NoReplyIntentional { .. } => TURN_OUTCOME_NO_REPLY_INTENTIONAL,
            Self::RetryScheduled { .. } => TURN_OUTCOME_RETRY_SCHEDULED,
            Self::TerminalFailed { .. } => TURN_OUTCOME_TERMINAL_FAILED,
            Self::SkippedDecodeError { .. }
            | Self::SkippedEmptyPayload
            | Self::SkippedQueueBacklog => TURN_OUTCOME_SKIPPED,
        }
    }

    /// Ledger `reason` column value.
    #[must_use]
    pub(crate) fn ledger_reason(&self) -> Option<&'static str> {
        match self {
            Self::Sent { .. } | Self::SideEffectDelegated { .. } => None,
            Self::NoReplyIntentional { reason }
            | Self::RetryScheduled { reason, .. }
            | Self::TerminalFailed { reason, .. } => Some(reason),
            Self::SkippedDecodeError { .. } => Some("decode_error"),
            Self::SkippedEmptyPayload => Some("empty_payload"),
            Self::SkippedQueueBacklog => Some(REASON_QUEUE_BACKLOG_EXPIRED),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_strings_cover_every_outcome_without_silent() {
        let cases: Vec<(TurnOutcome, &str, Option<&str>)> = vec![
            (
                TurnOutcome::Sent {
                    parts: 1,
                    side_effect_tickets: vec![7],
                },
                "sent",
                None,
            ),
            (
                TurnOutcome::SideEffectDelegated {
                    tickets: vec![7],
                    kinds: vec!["image_generation_job".to_owned()],
                },
                "side_effect_delegated",
                None,
            ),
            (
                TurnOutcome::NoReplyIntentional {
                    reason: "duplicate_suppressed",
                },
                "no_reply_intentional",
                Some("duplicate_suppressed"),
            ),
            (
                TurnOutcome::RetryScheduled {
                    reason: "provider_empty",
                    attempt: 1,
                    max_attempts: 5,
                    target_queue: "dialog-aifarm".to_owned(),
                },
                "retry_scheduled",
                Some("provider_empty"),
            ),
            (
                TurnOutcome::TerminalFailed {
                    reason: "budget_exhausted",
                    error: "boom".to_owned(),
                    user_signal: UserSignalPlan::React,
                },
                "terminal_failed",
                Some("budget_exhausted"),
            ),
            (
                TurnOutcome::SkippedDecodeError {
                    error: "bad".to_owned(),
                },
                "skipped",
                Some("decode_error"),
            ),
            (
                TurnOutcome::SkippedEmptyPayload,
                "skipped",
                Some("empty_payload"),
            ),
            (
                TurnOutcome::SkippedQueueBacklog,
                "skipped",
                Some("queue_backlog_expired"),
            ),
        ];
        for (outcome, expected_outcome, expected_reason) in cases {
            assert_eq!(outcome.ledger_outcome(), expected_outcome, "{outcome:?}");
            assert_eq!(outcome.ledger_reason(), expected_reason, "{outcome:?}");
            assert_ne!(outcome.ledger_outcome(), "silent");
        }
    }
}
