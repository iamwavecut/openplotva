//! Dialog turn engine surfaces.
//!
//! Phase 1 shipped the reply-outcome ledger; Phase 2 makes the invariant
//! structural: every dequeued dialog turn resolves to exactly one
//! [`TurnResolution`] and `finalize_turn` is the only writer of job status,
//! the `turn_outcome` job event, and the ledger row. Fully-silent completions
//! are impossible — empty provider output is retryable. Phase 3 adds the
//! terminal user signal (reaction with text fallback) and in-process
//! duplicate-answer regeneration.
//!
//! # At-least-once turn semantics
//!
//! A turn's externally visible steps (tool side effects, user reaction, sent
//! answer) happen before `finalize_turn` writes the terminal job status, so a
//! crash — or a failed status write — re-runs the whole turn. The crash
//! windows and their bounds:
//!
//! - **After a side-effect ticket is assigned / delivery obligation
//!   inserted:** the re-run re-executes generation and its tools, so a second
//!   generation job for the same trigger is possible. Duplicate inserts of
//!   the *same* ticket are blocked by the obligation unique index; two
//!   distinct tickets are accepted, pre-existing behavior that obligations
//!   make visible.
//! - **After the terminal user reaction:** the re-run may re-react —
//!   `setMessageReaction` is replace-idempotent, so this is a visual no-op.
//!   The `terminal_user_signal` job event marker gates only the
//!   non-idempotent text fallback, which is sent at most once.
//! - **After a sent answer, before the status write:** without a marker the
//!   re-run would send a duplicate message. The `answer_sent` job event
//!   marker (appended right after a successful send; append failure
//!   non-fatal) makes the re-run resolve `Sent` with `resent_skipped`
//!   instead of re-sending; a crash inside the marker window itself is
//!   further bounded by the reply-scoped outbound debounce key.

mod budget;
mod engine;
mod ledger;
mod obligations;
mod outcome;
mod signal;

pub use budget::{
    TURN_DEADLINE, TURN_STARTED_STAGE, TurnBudget, current_turn_deadline, turn_started_at,
};
pub use engine::{ANSWER_SENT_STAGE, DIALOG_TURN_REGENERATE_STAGE, TURN_OUTCOME_STAGE};
pub(crate) use engine::{TurnContext, execute_dialog_turn, finalize_turn};
pub use ledger::{
    DialogTurnObserver, DialogTurnOutcomeRecord, PostgresDialogTurnOutcomeRecorder,
    RuntimeTurnOutcomeBuffer, TURN_OUTCOME_NO_REPLY_INTENTIONAL, TURN_OUTCOME_RETRY_SCHEDULED,
    TURN_OUTCOME_SENT, TURN_OUTCOME_SIDE_EFFECT_DELEGATED, TURN_OUTCOME_SKIPPED,
    TURN_OUTCOME_TERMINAL_FAILED, delete_old_turn_outcomes_batch,
};
pub use obligations::{
    DEFAULT_DIALOG_IMAGE_DELIVERY_TIMEOUT_SECS, DEFAULT_DIALOG_MUSIC_DELIVERY_TIMEOUT_SECS,
    DEFAULT_DIALOG_OBLIGATION_WATCH_INTERVAL_SECS, DeliveryObligationAnnotator,
    DeliveryObligationNotifier, DeliveryObligationRecorder, DeliveryObligationStore,
    DeliveryObligationTimeouts, DispatchFailureSignalReport, DispatchFailureSignalScan,
    DispatcherDeliveryObligationNotifier, FallbackTicketRecordSource, OBLIGATION_EXTENDED_NOTICE,
    OBLIGATION_FAILURE_NOTICE, ObligationError, ObligationFuture, ObligationNoticeResult,
    ObligationNoticeTarget, ObligationWatchTickReport, TicketRecordSource,
    process_delivery_obligations_once, process_dispatch_failure_signals_once,
    run_delivery_obligation_watcher,
};
pub use outcome::{
    JobDisposition, REASON_QUEUE_BACKLOG_EXPIRED, TurnOutcome, TurnResolution, UserSignalPlan,
};
pub use signal::{
    DEFAULT_DIALOG_TERMINAL_REACTION_EMOJI, DEFAULT_DIALOG_TERMINAL_SIGNAL_MAX_AGE_SECS,
    DispatcherTerminalUserSignal, SignalTarget, TERMINAL_REACTION_TIMEOUT,
    TERMINAL_USER_SIGNAL_STAGE, TerminalUserSignal, TurnSignalPolicy, UserSignalFuture,
    UserSignalResult,
};
