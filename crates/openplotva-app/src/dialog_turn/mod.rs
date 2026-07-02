//! Dialog turn engine surfaces.
//!
//! Phase 1 ships the reply-outcome ledger: every dialog worker tick that
//! handled a job is classified into exactly one recorded outcome so silent
//! completions become measurable before the engine makes them impossible.

mod ledger;

pub use ledger::{
    DialogTurnObserver, DialogTurnOutcomeRecord, PostgresDialogTurnOutcomeRecorder,
    RuntimeTurnOutcomeBuffer, TURN_OUTCOME_NO_REPLY_INTENTIONAL, TURN_OUTCOME_RETRY_SCHEDULED,
    TURN_OUTCOME_SENT, TURN_OUTCOME_SIDE_EFFECT_DELEGATED, TURN_OUTCOME_SILENT,
    TURN_OUTCOME_SKIPPED, TURN_OUTCOME_TERMINAL_FAILED, delete_old_turn_outcomes_batch,
    outcome_from_report,
};
