//! Dialog turn engine surfaces.
//!
//! Phase 1 shipped the reply-outcome ledger; Phase 2 makes the invariant
//! structural: every dequeued dialog turn resolves to exactly one
//! [`TurnResolution`] and `finalize_turn` is the only writer of job status,
//! the `turn_outcome` job event, and the ledger row. Fully-silent completions
//! are impossible — empty provider output is retryable.

mod budget;
mod engine;
mod ledger;
mod outcome;

pub use budget::{
    TURN_DEADLINE, TURN_STARTED_STAGE, TurnBudget, current_turn_deadline, turn_started_at,
};
pub use engine::TURN_OUTCOME_STAGE;
pub(crate) use engine::{TurnContext, execute_dialog_turn, finalize_turn};
pub use ledger::{
    DialogTurnObserver, DialogTurnOutcomeRecord, PostgresDialogTurnOutcomeRecorder,
    RuntimeTurnOutcomeBuffer, TURN_OUTCOME_NO_REPLY_INTENTIONAL, TURN_OUTCOME_RETRY_SCHEDULED,
    TURN_OUTCOME_SENT, TURN_OUTCOME_SIDE_EFFECT_DELEGATED, TURN_OUTCOME_SKIPPED,
    TURN_OUTCOME_TERMINAL_FAILED, delete_old_turn_outcomes_batch,
};
pub use outcome::{
    JobDisposition, REASON_QUEUE_BACKLOG_EXPIRED, TurnOutcome, TurnResolution, UserSignalPlan,
};
