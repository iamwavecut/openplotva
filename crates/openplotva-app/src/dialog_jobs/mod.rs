//! Dialog-owned taskman worker: queue boundary, side effects, input
//! materialization, answer preparation, cross-tick retry policy, and the
//! worker loops. This facade re-exports every submodule item so
//! `crate::dialog_jobs::X` paths stay stable.

use std::{future::Future, pin::Pin, time::Duration};

use openplotva_dialog::DialogInput;
use openplotva_taskman::{DIALOG_AIFARM_QUEUE_NAME, TEXT_QUEUE_NAME};
use thiserror::Error;
use time::Duration as TimeDuration;

mod answer;
mod effects;
mod input;
mod queue;
mod report;
mod retry;
mod worker;

#[cfg(test)]
mod tests;

pub use answer::*;
pub use effects::*;
pub use input::*;
pub use queue::*;
pub use report::*;
pub(crate) use retry::*;
pub use worker::*;

const DIALOG_JOB_WORKER_ID: &str = "dialog-job";
pub const DIALOG_JOB_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DIALOG_HISTORY_FETCH_LIMIT: i32 = 100;
const DIALOG_HISTORY_TTL: TimeDuration = TimeDuration::days(7);

/// Default per-turn wall-clock budget (`DIALOG_TURN_BUDGET_SECS`).
pub const DEFAULT_DIALOG_TURN_BUDGET_SECS: i32 = 120;
/// Default pending age beyond which never-processed dialog jobs are dropped
/// (`DIALOG_TURN_MAX_QUEUE_AGE_SECS`).
pub const DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS: i32 = 600;
/// Default in-process duplicate-answer regenerations per turn
/// (`DIALOG_TURN_MAX_REGENERATIONS`).
pub const DEFAULT_DIALOG_TURN_MAX_REGENERATIONS: i32 = 2;

pub const DIALOG_JOB_WORKER_QUEUES: [&str; 2] = [DIALOG_AIFARM_QUEUE_NAME, TEXT_QUEUE_NAME];

/// Boxed future returned by dialog taskman queue calls.
pub type DialogJobWorkerFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by dialog side effects.
pub type DialogJobEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned after a final dialog answer has been durably queued.
pub type DialogJobReceiptFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<QueuedBatchReceipt, E>> + Send + 'a>>;

/// Boxed future used to detect a previously committed final-answer batch.
pub type DialogJobReceiptLookupFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Option<QueuedBatchReceipt>, E>> + Send + 'a>>;

/// Boxed future returned by dialog tool-call history storage.
pub type DialogToolCallHistoryFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<bool, E>> + Send + 'a>>;

/// Required dialog input could not be materialized safely.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DialogInputMaterializationError {
    #[error("load canonical chat history: {message}")]
    History { message: String },
}

/// Boxed future returned by dialog input materializers.
pub type DialogInputMaterializerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DialogInput, DialogInputMaterializationError>> + Send + 'a>>;
