//! Queue/status boundary of the dialog worker: the taskman work item, the
//! queue trait, and its in-memory implementation.

use std::fmt;

use openplotva_taskman::{
    InMemoryTaskQueue, JobType, Priority, StatelessJobItem, TaskQueueError, TaskQueueJobEvent,
    TaskQueueWorkItem,
};
use time::OffsetDateTime;

use super::{DIALOG_JOB_WORKER_ID, DialogJobWorkerFuture};

/// Concrete taskman row ready for the dialog worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogJobWorkItem {
    /// Taskman job ID used for completion/failure writes.
    pub id: i64,
    pub job: StatelessJobItem,
    pub events: Vec<TaskQueueJobEvent>,
}

/// Queue/status boundary for the dialog-owned taskman worker.
pub trait DialogJobWorkerQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Dequeue the next pending dialog job from a named taskman queue.
    fn dequeue_dialog_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> DialogJobWorkerFuture<'a, Option<DialogJobWorkItem>, Self::Error>;

    /// Count pending dialog jobs at this priority or higher.
    fn pending_dialog_job_depth<'a>(
        &'a self,
        queue_name: &'static str,
        priority: Priority,
    ) -> DialogJobWorkerFuture<'a, usize, Self::Error>;

    /// Mark one dialog job completed.
    fn complete_dialog_job<'a>(&'a self, job_id: i64)
    -> DialogJobWorkerFuture<'a, (), Self::Error>;

    /// Mark one dialog job failed.
    fn fail_dialog_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error>;

    fn append_dialog_job_event<'a>(
        &'a self,
        job_id: i64,
        event: TaskQueueJobEvent,
        at: OffsetDateTime,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error>;

    /// Move one retryable dialog job back to pending in the chosen queue.
    fn requeue_retryable_dialog_job<'a>(
        &'a self,
        job_id: i64,
        target_queue: &'a str,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error>;
}

impl DialogJobWorkerQueue for InMemoryTaskQueue {
    type Error = TaskQueueError;

    fn dequeue_dialog_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> DialogJobWorkerFuture<'a, Option<DialogJobWorkItem>, Self::Error> {
        Box::pin(async move {
            Ok(self
                .dequeue_matching(
                    queue_name,
                    DIALOG_JOB_WORKER_ID,
                    OffsetDateTime::now_utc(),
                    is_dialog_job,
                )
                .map(dialog_work_item_from_taskman))
        })
    }

    fn pending_dialog_job_depth<'a>(
        &'a self,
        queue_name: &'static str,
        priority: Priority,
    ) -> DialogJobWorkerFuture<'a, usize, Self::Error> {
        Box::pin(async move { Ok(self.queue_depth_for_priority_or_higher(queue_name, priority)) })
    }

    fn complete_dialog_job<'a>(
        &'a self,
        job_id: i64,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move { self.complete(job_id, OffsetDateTime::now_utc()) })
    }

    fn fail_dialog_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move { self.fail(job_id, error, OffsetDateTime::now_utc()) })
    }

    fn append_dialog_job_event<'a>(
        &'a self,
        job_id: i64,
        event: TaskQueueJobEvent,
        at: OffsetDateTime,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move { self.append_job_event(job_id, event, at) })
    }

    fn requeue_retryable_dialog_job<'a>(
        &'a self,
        job_id: i64,
        target_queue: &'a str,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move { self.requeue_job_to_queue(job_id, target_queue) })
    }
}

fn dialog_work_item_from_taskman(item: TaskQueueWorkItem) -> DialogJobWorkItem {
    DialogJobWorkItem {
        id: item.id,
        job: item.job,
        events: item.events,
    }
}

fn is_dialog_job(job: &StatelessJobItem) -> bool {
    job.data.job_type == JobType::Dialog
}
