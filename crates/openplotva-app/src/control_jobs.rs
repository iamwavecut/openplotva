use std::{fmt, future::Future, pin::Pin, time::Duration};

use openplotva_taskman::{
    CONTROL_QUEUE_NAME, ControlJobParams, ControlKind, JobType, StatelessJobItem,
    control_job_params_from_stateless_job,
};
use openplotva_telegram::DispatcherQueue;
use time::OffsetDateTime;

use crate::{
    checkin::{CheckinControlJobEffects, CheckinControlJobOutcome},
    members::{MemberStateControlJobEffects, MemberStateControlJobExecution},
    payments::{
        PaymentControlJobReport, SharedControlJobWorkerQueue, SuccessfulPaymentStore,
        execute_payment_control_job_at, payment_control_job_failure_message,
    },
    settings::{
        GroupSettingsControlJobEffects, NewMembersFollowupControlJobEffects,
        SettingsControlJobExecution, execute_group_settings_control_job_at,
        execute_new_members_followup_control_job_at, settings_control_job_failure_message,
    },
    translate::{
        TextTranslator, TranslateControlJobOutcome, TranslateEffects, execute_translate_control_job,
    },
};

pub const CONTROL_JOB_WORKER_ID: &str = "control-job-dispatcher";

pub const CONTROL_JOB_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Boxed future returned by unified control-job executor registries.
pub type ControlJobExecutorFuture<'a> =
    Pin<Box<dyn Future<Output = ControlJobExecutionResult> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlJobExecution {
    /// Payment-owned executor result.
    Payment(PaymentControlJobReport),
    /// Settings/new-member executor result.
    Settings(SettingsControlJobExecution),
    /// Translation executor result.
    Translate(TranslateControlJobOutcome),
    /// Chat-admin/member sync executor result.
    MemberState(MemberStateControlJobExecution),
    /// Check-in game executor result.
    Checkin(CheckinControlJobOutcome),
}

/// Result returned by one control-kind executor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlJobExecutionResult {
    /// Executor completed and the queue row should be marked completed.
    Completed(ControlJobExecution),
    /// Executor failed and the queue row should be marked failed.
    Failed {
        /// Optional partial execution details.
        execution: Option<ControlJobExecution>,
        /// Queue failure message.
        error: String,
    },
}

/// Result of one unified control-job worker tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ControlJobWorkerReport {
    /// Whether a job was dequeued.
    pub dequeued: bool,
    /// Dequeued taskman job ID.
    pub job_id: Option<i64>,
    /// Decoded control kind.
    pub kind: Option<ControlKind>,
    /// Executor result, when the payload decoded and execution returned details.
    pub execution: Option<ControlJobExecution>,
    /// The job was finalized as completed.
    pub completed: bool,
    /// The job was finalized as failed.
    pub failed: bool,
    /// Queue dequeue failed.
    pub dequeue_error: Option<String>,
    pub decode_error: Option<String>,
    /// Executor failed or timed out.
    pub execution_error: Option<String>,
    /// Completion/failure status write failed.
    pub status_error: Option<String>,
}

/// Aggregate report for a long-running unified control-job worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ControlJobWorkerRunReport {
    /// Number of poll ticks.
    pub ticks: u64,
    /// Number of ticks that dequeued a job.
    pub dequeued: u64,
    /// Number of jobs completed.
    pub completed: u64,
    /// Number of jobs failed.
    pub failed: u64,
    /// Number of queue dequeue errors.
    pub dequeue_errors: u64,
    /// Number of completion/failure write errors.
    pub status_errors: u64,
}

pub trait ControlJobExecutorRegistry {
    /// Execute one already-decoded control job.
    fn execute_control_job<'a>(
        &'a self,
        params: &'a ControlJobParams,
        now: OffsetDateTime,
    ) -> ControlJobExecutorFuture<'a>;
}

pub struct AppControlJobExecutors<
    'a,
    PaymentStore,
    PaymentEffects,
    GroupEffects,
    NewMembersEffects,
    Translator,
    TranslationEffects,
    MemberEffects,
    CheckinEffects,
    NextId,
> {
    /// Payment storage.
    pub payment_store: &'a PaymentStore,
    /// Payment Telegram/dispatcher effects.
    pub payment_effects: &'a PaymentEffects,
    /// Outbound dispatcher queue.
    pub dispatcher_queue: &'a DispatcherQueue,
    /// Group settings side effects.
    pub group_settings_effects: &'a GroupEffects,
    /// New-members follow-up side effects.
    pub new_members_effects: &'a NewMembersEffects,
    /// Bot username for deep links.
    pub bot_username: &'a str,
    /// Virtual ID factory.
    pub next_virtual_id: &'a NextId,
    /// Translation provider.
    pub translator: &'a Translator,
    /// Translation side effects.
    pub translation_effects: &'a TranslationEffects,
    /// Chat-admin/member sync effects.
    pub member_effects: &'a MemberEffects,
    /// Check-in game effects.
    pub checkin_effects: &'a CheckinEffects,
}

impl<
    PaymentStore,
    PaymentEffects,
    GroupEffects,
    NewMembersEffects,
    Translator,
    TranslationEffects,
    MemberEffects,
    CheckinEffects,
    NextId,
> ControlJobExecutorRegistry
    for AppControlJobExecutors<
        '_,
        PaymentStore,
        PaymentEffects,
        GroupEffects,
        NewMembersEffects,
        Translator,
        TranslationEffects,
        MemberEffects,
        CheckinEffects,
        NextId,
    >
where
    PaymentStore: SuccessfulPaymentStore + Sync,
    PaymentEffects:
        crate::payments::PaymentInvoiceEffects + crate::payments::SuccessfulPaymentEffects + Sync,
    GroupEffects: GroupSettingsControlJobEffects + Sync,
    NewMembersEffects: NewMembersFollowupControlJobEffects + Sync,
    Translator: TextTranslator + Sync,
    TranslationEffects: TranslateEffects + Sync,
    MemberEffects: MemberStateControlJobEffects + Sync,
    CheckinEffects: CheckinControlJobEffects + Sync,
    NextId: Fn() -> String + Sync,
{
    fn execute_control_job<'a>(
        &'a self,
        params: &'a ControlJobParams,
        now: OffsetDateTime,
    ) -> ControlJobExecutorFuture<'a> {
        Box::pin(async move {
            match params.data.kind {
                ControlKind::VipInvoice
                | ControlKind::DonateInvoice
                | ControlKind::SuccessfulPayment => {
                    let report = execute_payment_control_job_at(
                        self.payment_store,
                        self.payment_effects,
                        params,
                        now,
                    )
                    .await;
                    let execution = ControlJobExecution::Payment(report.clone());
                    if let Some(error) = payment_control_job_failure_message(&report) {
                        ControlJobExecutionResult::Failed {
                            execution: Some(execution),
                            error,
                        }
                    } else {
                        ControlJobExecutionResult::Completed(execution)
                    }
                }
                ControlKind::GroupSettings => {
                    let mut next_virtual_id = || (self.next_virtual_id)();
                    match execute_group_settings_control_job_at(
                        self.dispatcher_queue,
                        self.group_settings_effects,
                        params,
                        self.bot_username,
                        &mut next_virtual_id,
                    )
                    .await
                    {
                        Ok(outcome) => {
                            let execution = SettingsControlJobExecution::GroupSettings(outcome);
                            complete_or_fail_settings_execution(execution)
                        }
                        Err(error) => ControlJobExecutionResult::Failed {
                            execution: None,
                            error: error.to_string(),
                        },
                    }
                }
                ControlKind::NewMembersFollowup => {
                    let mut next_virtual_id = || (self.next_virtual_id)();
                    match execute_new_members_followup_control_job_at(
                        self.dispatcher_queue,
                        self.new_members_effects,
                        params,
                        self.bot_username,
                        &mut next_virtual_id,
                    )
                    .await
                    {
                        Ok(outcome) => {
                            let execution =
                                SettingsControlJobExecution::NewMembersFollowup(outcome);
                            complete_or_fail_settings_execution(execution)
                        }
                        Err(error) => ControlJobExecutionResult::Failed {
                            execution: None,
                            error: error.to_string(),
                        },
                    }
                }
                ControlKind::Translate => {
                    match execute_translate_control_job(
                        self.translator,
                        self.translation_effects,
                        params,
                    )
                    .await
                    {
                        Ok(outcome) => ControlJobExecutionResult::Completed(
                            ControlJobExecution::Translate(outcome),
                        ),
                        Err(error) => ControlJobExecutionResult::Failed {
                            execution: None,
                            error: error.to_string(),
                        },
                    }
                }
                ControlKind::ChatAdminsSync => {
                    self.member_effects.sync_chat_admins(params.chat_id).await;
                    ControlJobExecutionResult::Completed(ControlJobExecution::MemberState(
                        MemberStateControlJobExecution::ChatAdminsSync,
                    ))
                }
                ControlKind::ChatMemberSync => {
                    self.member_effects
                        .sync_chat_member(params.chat_id, params.user_id)
                        .await;
                    ControlJobExecutionResult::Completed(ControlJobExecution::MemberState(
                        MemberStateControlJobExecution::ChatMemberSync,
                    ))
                }
                ControlKind::Checkin => match self.checkin_effects.run_checkin_game(params).await {
                    Ok(()) => ControlJobExecutionResult::Completed(ControlJobExecution::Checkin(
                        CheckinControlJobOutcome::Completed,
                    )),
                    Err(error) => ControlJobExecutionResult::Failed {
                        execution: None,
                        error: error.to_string(),
                    },
                },
                kind => unsupported_control_job_kind(kind),
            }
        })
    }
}

#[must_use]
pub const fn is_supported_control_job_kind(kind: ControlKind) -> bool {
    matches!(
        kind,
        ControlKind::Translate
            | ControlKind::GroupSettings
            | ControlKind::VipInvoice
            | ControlKind::DonateInvoice
            | ControlKind::SuccessfulPayment
            | ControlKind::Checkin
            | ControlKind::ChatAdminsSync
            | ControlKind::ChatMemberSync
            | ControlKind::NewMembersFollowup
    )
}

#[must_use]
pub const fn control_job_timeout(kind: ControlKind) -> Duration {
    match kind {
        ControlKind::GroupSettings | ControlKind::ChatAdminsSync | ControlKind::ChatMemberSync => {
            Duration::from_secs(15)
        }
        ControlKind::Translate => Duration::from_secs(45),
        _ => Duration::from_secs(30),
    }
}

pub async fn process_control_job_once_at<Queue, Registry>(
    queue: &Queue,
    registry: &Registry,
    now: OffsetDateTime,
) -> ControlJobWorkerReport
where
    Queue: SharedControlJobWorkerQueue + Sync,
    Registry: ControlJobExecutorRegistry + Sync,
{
    let mut report = ControlJobWorkerReport::default();
    let item = match queue
        .dequeue_shared_control_job_matching(
            CONTROL_QUEUE_NAME,
            CONTROL_JOB_WORKER_ID,
            is_control_job,
        )
        .await
    {
        Ok(item) => item,
        Err(error) => {
            report.dequeue_error = Some(error.to_string());
            return report;
        }
    };

    let Some(item) = item else {
        return report;
    };
    report.dequeued = true;
    report.job_id = Some(item.id);

    let params = match control_job_params_from_stateless_job(&item.job) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            report.decode_error = Some(error.clone());
            mark_control_job_failed(queue, item.id, &error, &mut report).await;
            return report;
        }
    };
    report.kind = Some(params.data.kind);

    let result = match tokio::time::timeout(
        control_job_timeout(params.data.kind),
        registry.execute_control_job(&params, now),
    )
    .await
    {
        Ok(result) => result,
        Err(_elapsed) => ControlJobExecutionResult::Failed {
            execution: None,
            error: format!(
                "control job {:?} timed out after {:?}",
                params.data.kind,
                control_job_timeout(params.data.kind)
            ),
        },
    };

    match result {
        ControlJobExecutionResult::Completed(execution) => {
            report.execution = Some(execution);
            match queue.complete_shared_control_job(item.id).await {
                Ok(()) => report.completed = true,
                Err(error) => report.status_error = Some(error.to_string()),
            }
        }
        ControlJobExecutionResult::Failed { execution, error } => {
            report.execution = execution;
            report.execution_error = Some(error.clone());
            mark_control_job_failed(queue, item.id, &error, &mut report).await;
        }
    }

    report
}

/// Run the unified taskman control-job worker until the stop future resolves.
pub async fn run_control_job_worker_until<Queue, Registry, Stop>(
    queue: &Queue,
    registry: &Registry,
    stop: Stop,
) -> ControlJobWorkerRunReport
where
    Queue: SharedControlJobWorkerQueue + Sync,
    Registry: ControlJobExecutorRegistry + Sync,
    Stop: Future<Output = ()>,
{
    run_control_job_worker_every_until(queue, registry, CONTROL_JOB_POLL_INTERVAL, stop).await
}

/// Run the unified taskman control-job worker with an injected interval.
pub async fn run_control_job_worker_every_until<Queue, Registry, Stop>(
    queue: &Queue,
    registry: &Registry,
    interval: Duration,
    stop: Stop,
) -> ControlJobWorkerRunReport
where
    Queue: SharedControlJobWorkerQueue + Sync,
    Registry: ControlJobExecutorRegistry + Sync,
    Stop: Future<Output = ()>,
{
    let mut report = ControlJobWorkerRunReport::default();
    let mut stop = std::pin::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                let tick = process_control_job_once_at(queue, registry, OffsetDateTime::now_utc()).await;
                tracing::debug!(?tick, "control-job worker tick");
                report.record_tick(&tick);
            }
        }
    }

    report
}

impl ControlJobWorkerRunReport {
    fn record_tick(&mut self, tick: &ControlJobWorkerReport) {
        self.ticks += 1;
        if tick.dequeued {
            self.dequeued += 1;
        }
        if tick.completed {
            self.completed += 1;
        }
        if tick.failed {
            self.failed += 1;
        }
        if tick.dequeue_error.is_some() {
            self.dequeue_errors += 1;
        }
        if tick.status_error.is_some() {
            self.status_errors += 1;
        }
    }
}

fn complete_or_fail_settings_execution(
    execution: SettingsControlJobExecution,
) -> ControlJobExecutionResult {
    if let Some(error) = settings_control_job_failure_message(&execution) {
        ControlJobExecutionResult::Failed {
            execution: Some(ControlJobExecution::Settings(execution)),
            error,
        }
    } else {
        ControlJobExecutionResult::Completed(ControlJobExecution::Settings(execution))
    }
}

fn unsupported_control_job_kind(kind: ControlKind) -> ControlJobExecutionResult {
    ControlJobExecutionResult::Failed {
        execution: None,
        error: format!("unsupported control job kind: {kind:?}"),
    }
}

async fn mark_control_job_failed<Queue>(
    queue: &Queue,
    job_id: i64,
    error: &str,
    report: &mut ControlJobWorkerReport,
) where
    Queue: SharedControlJobWorkerQueue + Sync,
{
    match queue.fail_shared_control_job(job_id, error).await {
        Ok(()) => report.failed = true,
        Err(error) => report.status_error = Some(error.to_string()),
    }
}

fn is_control_job(job: &StatelessJobItem) -> bool {
    job.data.job_type == JobType::Control
}

impl<F> ControlJobExecutorRegistry for F
where
    F: for<'a> Fn(&'a ControlJobParams, OffsetDateTime) -> ControlJobExecutorFuture<'a> + Sync,
{
    fn execute_control_job<'a>(
        &'a self,
        params: &'a ControlJobParams,
        now: OffsetDateTime,
    ) -> ControlJobExecutorFuture<'a> {
        self(params, now)
    }
}

impl fmt::Display for ControlJobExecutionResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed(_) => formatter.write_str("completed"),
            Self::Failed { error, .. } => error.fmt(formatter),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use openplotva_taskman::{
        ControlJobData, ControlJobParams, HIGH_PRIORITY, TRANSACTION_PRIORITY, new_control_job_at,
    };

    use super::*;
    use crate::payments::{InMemoryPaymentControlJobStatus, PaymentControlJobQueue};

    #[tokio::test]
    async fn unified_control_worker_dequeues_next_pending_control_job_by_go_order()
    -> Result<(), Box<dyn Error>> {
        let queue = crate::payments::InMemoryPaymentControlJobQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        queue
            .assign_payment_control_job(
                CONTROL_QUEUE_NAME,
                control_job(ControlKind::Translate, now, 0),
            )
            .await?;
        let low_id = queue.snapshot().last().expect("low job").id;
        queue
            .assign_payment_control_job(
                CONTROL_QUEUE_NAME,
                control_job(
                    ControlKind::Checkin,
                    now + Duration::from_secs(1),
                    HIGH_PRIORITY,
                ),
            )
            .await?;
        let high_id = queue.snapshot().last().expect("high job").id;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let registry = RecordingRegistry {
            seen: Arc::clone(&seen),
        };

        let report = process_control_job_once_at(&queue, &registry, now).await;

        assert_eq!(report.job_id, Some(high_id));
        assert_eq!(report.kind, Some(ControlKind::Checkin));
        assert_eq!(
            report.execution,
            Some(ControlJobExecution::Checkin(
                CheckinControlJobOutcome::Completed
            ))
        );
        assert!(report.completed);
        assert_eq!(*seen.lock().expect("seen"), vec![ControlKind::Checkin]);
        let snapshot = queue.snapshot();
        assert_eq!(
            snapshot_record(&snapshot, high_id).status,
            InMemoryPaymentControlJobStatus::Completed
        );
        assert_eq!(
            snapshot_record(&snapshot, low_id).status,
            InMemoryPaymentControlJobStatus::Pending
        );
        Ok(())
    }

    #[tokio::test]
    async fn transactional_control_job_overtakes_older_membership_sync()
    -> Result<(), Box<dyn Error>> {
        let queue = crate::payments::InMemoryPaymentControlJobQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        queue
            .assign_payment_control_job(
                CONTROL_QUEUE_NAME,
                control_job(ControlKind::ChatMemberSync, now, HIGH_PRIORITY),
            )
            .await?;
        queue
            .assign_payment_control_job(
                CONTROL_QUEUE_NAME,
                control_job(
                    ControlKind::SuccessfulPayment,
                    now + Duration::from_secs(1),
                    TRANSACTION_PRIORITY,
                ),
            )
            .await?;

        let item = queue
            .dequeue_shared_control_job_matching(
                CONTROL_QUEUE_NAME,
                "priority-test",
                is_control_job,
            )
            .await?
            .expect("transactional job");
        let params = control_job_params_from_stateless_job(&item.job)?;

        assert_eq!(params.data.kind, ControlKind::SuccessfulPayment);
        assert_eq!(item.job.priority, TRANSACTION_PRIORITY);
        Ok(())
    }

    #[tokio::test]
    async fn unified_control_worker_fails_unknown_control_kind() -> Result<(), Box<dyn Error>> {
        let queue = crate::payments::InMemoryPaymentControlJobQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        queue
            .assign_payment_control_job(
                CONTROL_QUEUE_NAME,
                control_job(ControlKind::Unknown, now, HIGH_PRIORITY),
            )
            .await?;
        let job_id = queue.snapshot().last().expect("unknown job").id;
        let registry = RecordingRegistry::default();

        let report = process_control_job_once_at(&queue, &registry, now).await;

        assert_eq!(report.job_id, Some(job_id));
        assert_eq!(report.kind, Some(ControlKind::Unknown));
        assert!(report.failed);
        assert_eq!(
            report.execution_error,
            Some("unsupported control job kind: Unknown".to_owned())
        );
        let snapshot = queue.snapshot();
        let record = snapshot_record(&snapshot, job_id);
        assert_eq!(record.status, InMemoryPaymentControlJobStatus::Failed);
        assert_eq!(
            record.error.as_deref(),
            Some("unsupported control job kind: Unknown")
        );
        Ok(())
    }

    #[test]
    fn control_job_registry_covers_go_known_kinds() {
        for kind in [
            ControlKind::Translate,
            ControlKind::GroupSettings,
            ControlKind::VipInvoice,
            ControlKind::DonateInvoice,
            ControlKind::SuccessfulPayment,
            ControlKind::Checkin,
            ControlKind::ChatAdminsSync,
            ControlKind::ChatMemberSync,
            ControlKind::NewMembersFollowup,
        ] {
            assert!(is_supported_control_job_kind(kind), "missing {kind:?}");
        }

        assert!(!is_supported_control_job_kind(ControlKind::Unknown));
    }

    #[test]
    fn control_job_timeouts_match_go_fetcher() {
        assert_eq!(
            control_job_timeout(ControlKind::GroupSettings),
            Duration::from_secs(15)
        );
        assert_eq!(
            control_job_timeout(ControlKind::ChatAdminsSync),
            Duration::from_secs(15)
        );
        assert_eq!(
            control_job_timeout(ControlKind::ChatMemberSync),
            Duration::from_secs(15)
        );
        assert_eq!(
            control_job_timeout(ControlKind::Translate),
            Duration::from_secs(45)
        );
        assert_eq!(
            control_job_timeout(ControlKind::Checkin),
            Duration::from_secs(30)
        );
    }

    #[derive(Clone, Default)]
    struct RecordingRegistry {
        seen: Arc<Mutex<Vec<ControlKind>>>,
    }

    impl ControlJobExecutorRegistry for RecordingRegistry {
        fn execute_control_job<'a>(
            &'a self,
            params: &'a ControlJobParams,
            _now: OffsetDateTime,
        ) -> ControlJobExecutorFuture<'a> {
            Box::pin(async move {
                self.seen.lock().expect("seen").push(params.data.kind);
                match params.data.kind {
                    ControlKind::Translate => ControlJobExecutionResult::Completed(
                        ControlJobExecution::Translate(TranslateControlJobOutcome::Sent),
                    ),
                    ControlKind::Checkin => ControlJobExecutionResult::Completed(
                        ControlJobExecution::Checkin(CheckinControlJobOutcome::Completed),
                    ),
                    kind => unsupported_control_job_kind(kind),
                }
            })
        }
    }

    fn control_job(kind: ControlKind, created: OffsetDateTime, priority: i32) -> StatelessJobItem {
        new_control_job_at(
            ControlJobParams {
                chat_id: -10042,
                message_id: 12,
                user_id: 42,
                user_full_name: "Tester".to_owned(),
                thread_id: None,
                data: ControlJobData {
                    kind,
                    text: "hello".to_owned(),
                    target_lang: "ru".to_owned(),
                    theme: "classic".to_owned(),
                    ..ControlJobData::default()
                },
            },
            created,
        )
        .with_priority(priority)
    }

    fn snapshot_record(
        snapshot: &[crate::payments::InMemoryPaymentControlJobRecord],
        id: i64,
    ) -> &crate::payments::InMemoryPaymentControlJobRecord {
        snapshot
            .iter()
            .find(|record| record.id == id)
            .expect("snapshot record")
    }
}
