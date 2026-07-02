//! Silent-but-guaranteed side effects: schedulers record a delivery obligation
//! for every assigned generation ticket, and this watcher resolves each
//! obligation against the ticket — delivered artifacts stay silent, failures
//! and orphans notify the user, and a still-running ticket past its deadline
//! gets ONE "taking longer" notice with a single deadline extension before it
//! expires. Transitions are idempotent winner-notifies updates in Postgres, so
//! the guarantee survives restarts and concurrent ticks.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use openplotva_dialog::turn::SIDE_EFFECT_KIND_MUSIC;
use openplotva_storage::{
    DELIVERY_OBLIGATION_STATE_EXPIRED_NOTIFIED, DELIVERY_OBLIGATION_STATE_EXTENDED_ONCE,
    DELIVERY_OBLIGATION_STATE_FAILED_NOTIFIED, DELIVERY_OBLIGATION_STATE_ORPHANED_NOTIFIED,
    DELIVERY_OBLIGATION_STATE_PENDING, DeliveryObligationRecord, NewDeliveryObligation,
    PostgresDeliveryObligationStore, PostgresTaskQueueStore,
};
use openplotva_taskman::{InMemoryTaskQueue, JobStatus, TaskQueueRecord};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, ReplyMessageRef, TELEGRAM_PARSE_MODE_HTML, TextMessageRequest,
};
use time::{Duration as TimeDuration, OffsetDateTime};

use super::signal::{DispatcherTerminalUserSignal, SignalTarget, TerminalUserSignal};
use crate::virtual_messages::{
    QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory, queue_text_message_parts,
};

/// Visible in-character text when a promised generation failed, vanished, or
/// expired. Sent as a protected reply to the trigger message.
pub const OBLIGATION_FAILURE_NOTICE: &str = "Не получилось сгенерировать 😿 Попробуй ещё раз.";

/// One-time notice when a generation runs past its deadline but is still alive
/// (punch #8: extend once, never error-then-image).
pub const OBLIGATION_EXTENDED_NOTICE: &str = "Дольше обычного, не бросила 🐟";

/// Default watcher poll interval (`DIALOG_OBLIGATION_WATCH_INTERVAL_SECS`).
pub const DEFAULT_DIALOG_OBLIGATION_WATCH_INTERVAL_SECS: i32 = 15;
/// Default image delivery deadline (`DIALOG_IMAGE_DELIVERY_TIMEOUT_SECS`).
pub const DEFAULT_DIALOG_IMAGE_DELIVERY_TIMEOUT_SECS: i32 = 1200;
/// Default music delivery deadline (`DIALOG_MUSIC_DELIVERY_TIMEOUT_SECS`).
pub const DEFAULT_DIALOG_MUSIC_DELIVERY_TIMEOUT_SECS: i32 = 1800;

/// Obligations scanned per watcher tick; the table is low-volume by design.
const OBLIGATION_SCAN_LIMIT: i64 = 500;

/// Boxed error shared by the obligation traits so fakes stay cheap.
pub type ObligationError = Box<dyn std::error::Error + Send + Sync>;
/// Boxed future returned by the obligation traits.
pub type ObligationFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ObligationError>> + Send + 'a>>;

/// Records one obligation at schedule time (idempotent: ON CONFLICT on the
/// ticket id). Returns whether a new row was inserted.
pub trait DeliveryObligationRecorder: Send + Sync {
    fn record_delivery_obligation<'a>(
        &'a self,
        obligation: NewDeliveryObligation,
    ) -> ObligationFuture<'a, bool>;
}

/// Watcher-side store surface: list open obligations and apply idempotent
/// winner-notifies transitions (each returns true only for the winner).
pub trait DeliveryObligationStore: Send + Sync {
    fn list_open<'a>(&'a self, limit: i64) -> ObligationFuture<'a, Vec<DeliveryObligationRecord>>;

    fn mark_delivered<'a>(
        &'a self,
        id: i64,
        result_message_id: Option<i32>,
        detail: &'a str,
        now: OffsetDateTime,
    ) -> ObligationFuture<'a, bool>;

    fn mark_notified<'a>(
        &'a self,
        id: i64,
        state: &'static str,
        detail: &'a str,
        now: OffsetDateTime,
    ) -> ObligationFuture<'a, bool>;

    fn extend_once<'a>(
        &'a self,
        id: i64,
        new_deadline: OffsetDateTime,
        now: OffsetDateTime,
    ) -> ObligationFuture<'a, bool>;
}

/// Backfills the dialog taskman job id after turn finalization
/// (update-if-placeholder; annotation only, never creates).
pub trait DeliveryObligationAnnotator: Send + Sync {
    fn annotate_dialog_job<'a>(
        &'a self,
        ticket_job_id: i64,
        dialog_job_id: i64,
    ) -> ObligationFuture<'a, bool>;
}

impl DeliveryObligationRecorder for PostgresDeliveryObligationStore {
    fn record_delivery_obligation<'a>(
        &'a self,
        obligation: NewDeliveryObligation,
    ) -> ObligationFuture<'a, bool> {
        Box::pin(async move {
            self.insert_delivery_obligation(&obligation)
                .await
                .map_err(|error| Box::new(error) as ObligationError)
        })
    }
}

impl DeliveryObligationStore for PostgresDeliveryObligationStore {
    fn list_open<'a>(&'a self, limit: i64) -> ObligationFuture<'a, Vec<DeliveryObligationRecord>> {
        Box::pin(async move {
            self.list_open_delivery_obligations(limit)
                .await
                .map_err(|error| Box::new(error) as ObligationError)
        })
    }

    fn mark_delivered<'a>(
        &'a self,
        id: i64,
        result_message_id: Option<i32>,
        detail: &'a str,
        now: OffsetDateTime,
    ) -> ObligationFuture<'a, bool> {
        Box::pin(async move {
            self.mark_delivery_obligation_delivered(id, result_message_id, detail, now)
                .await
                .map_err(|error| Box::new(error) as ObligationError)
        })
    }

    fn mark_notified<'a>(
        &'a self,
        id: i64,
        state: &'static str,
        detail: &'a str,
        now: OffsetDateTime,
    ) -> ObligationFuture<'a, bool> {
        Box::pin(async move {
            self.mark_delivery_obligation_notified(id, state, detail, now)
                .await
                .map_err(|error| Box::new(error) as ObligationError)
        })
    }

    fn extend_once<'a>(
        &'a self,
        id: i64,
        new_deadline: OffsetDateTime,
        now: OffsetDateTime,
    ) -> ObligationFuture<'a, bool> {
        Box::pin(async move {
            self.extend_delivery_obligation_once(id, new_deadline, now)
                .await
                .map_err(|error| Box::new(error) as ObligationError)
        })
    }
}

impl DeliveryObligationAnnotator for PostgresDeliveryObligationStore {
    fn annotate_dialog_job<'a>(
        &'a self,
        ticket_job_id: i64,
        dialog_job_id: i64,
    ) -> ObligationFuture<'a, bool> {
        Box::pin(async move {
            self.annotate_delivery_obligation_dialog_job(ticket_job_id, dialog_job_id)
                .await
                .map_err(|error| Box::new(error) as ObligationError)
        })
    }
}

/// Resolves a generation ticket record by id.
pub trait TicketRecordSource: Send + Sync {
    fn ticket_record<'a>(
        &'a self,
        ticket_job_id: i64,
    ) -> ObligationFuture<'a, Option<TaskQueueRecord>>;
}

impl<T: TicketRecordSource + ?Sized> TicketRecordSource for Arc<T> {
    fn ticket_record<'a>(
        &'a self,
        ticket_job_id: i64,
    ) -> ObligationFuture<'a, Option<TaskQueueRecord>> {
        self.as_ref().ticket_record(ticket_job_id)
    }
}

impl TicketRecordSource for InMemoryTaskQueue {
    fn ticket_record<'a>(
        &'a self,
        ticket_job_id: i64,
    ) -> ObligationFuture<'a, Option<TaskQueueRecord>> {
        let record = self.record(ticket_job_id);
        Box::pin(async move { Ok(record) })
    }
}

impl TicketRecordSource for PostgresTaskQueueStore {
    fn ticket_record<'a>(
        &'a self,
        ticket_job_id: i64,
    ) -> ObligationFuture<'a, Option<TaskQueueRecord>> {
        Box::pin(async move {
            self.load_task_queue_record(ticket_job_id)
                .await
                .map_err(|error| Box::new(error) as ObligationError)
        })
    }
}

/// In-memory queue first, Postgres `taskman_jobs` second: restarted processes
/// lose the in-memory record but the durable row still resolves the obligation.
pub struct FallbackTicketRecordSource<Primary, Secondary> {
    primary: Primary,
    secondary: Option<Secondary>,
}

impl<Primary, Secondary> FallbackTicketRecordSource<Primary, Secondary> {
    #[must_use]
    pub fn new(primary: Primary, secondary: Option<Secondary>) -> Self {
        Self { primary, secondary }
    }
}

impl<Primary, Secondary> TicketRecordSource for FallbackTicketRecordSource<Primary, Secondary>
where
    Primary: TicketRecordSource,
    Secondary: TicketRecordSource,
{
    fn ticket_record<'a>(
        &'a self,
        ticket_job_id: i64,
    ) -> ObligationFuture<'a, Option<TaskQueueRecord>> {
        Box::pin(async move {
            match self.primary.ticket_record(ticket_job_id).await {
                Ok(Some(record)) => return Ok(Some(record)),
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!(ticket_job_id, %error, "primary ticket record source failed");
                }
            }
            let Some(secondary) = &self.secondary else {
                return Ok(None);
            };
            secondary.ticket_record(ticket_job_id).await
        })
    }
}

/// Where and what to tell the user about one obligation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObligationNoticeTarget<'a> {
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    /// Trigger message the notice replies to (also scopes the debounce key).
    pub trigger_message_id: i32,
    pub text: &'a str,
}

/// Observable result of one notice attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObligationNoticeResult {
    TextQueued,
    ReactionSent,
    Failed(String),
}

/// Sends obligation notices to the user.
pub trait DeliveryObligationNotifier: Send + Sync {
    fn notify<'a>(
        &'a self,
        target: ObligationNoticeTarget<'a>,
    ) -> Pin<Box<dyn Future<Output = ObligationNoticeResult> + Send + 'a>>;
}

/// Production notifier: a protected, reply-scoped-debounced text through the
/// dispatcher (punch #11 — the message that guarantees non-silence must never
/// be trimmed or deduped away), with the terminal-signal reaction as fallback
/// when the text cannot even be queued.
pub struct DispatcherDeliveryObligationNotifier {
    queue: Arc<DispatcherQueue>,
    signal: DispatcherTerminalUserSignal,
    next_virtual_id: VirtualIdFactory,
}

impl DispatcherDeliveryObligationNotifier {
    #[must_use]
    pub fn new(queue: Arc<DispatcherQueue>, signal: DispatcherTerminalUserSignal) -> Self {
        Self {
            queue,
            signal,
            next_virtual_id: monotonic_virtual_id_factory("dialog-obligation"),
        }
    }

    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl DeliveryObligationNotifier for DispatcherDeliveryObligationNotifier {
    fn notify<'a>(
        &'a self,
        target: ObligationNoticeTarget<'a>,
    ) -> Pin<Box<dyn Future<Output = ObligationNoticeResult> + Send + 'a>> {
        Box::pin(async move {
            let chat = ChatRef {
                id: target.chat_id,
                is_forum: target.thread_id.is_some(),
            };
            let reply_to = ReplyMessageRef {
                message_id: i64::from(target.trigger_message_id),
                chat,
                is_topic_message: target.thread_id.is_some(),
                message_thread_id: target.thread_id.map(i64::from).unwrap_or_default(),
            };
            let request = TextMessageRequest {
                chat: Some(chat),
                message_thread_id: target.thread_id.map(i64::from).unwrap_or_default(),
                disable_notification: false,
                allow_sending_without_reply: None,
                text: target.text.to_owned(),
                render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                reply_markup: None,
            };
            let debounce_key = format!("r{}", target.trigger_message_id);
            let queue_error = match queue_text_message_parts(
                &self.queue,
                QueueTextRequest {
                    message: &request,
                    reply_to: Some(&reply_to),
                    immediate_first: true,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: None,
                    protected: true,
                    debounce_key: Some(&debounce_key),
                },
                || (self.next_virtual_id)(),
            )
            .await
            {
                Ok(_) => return ObligationNoticeResult::TextQueued,
                Err(error) => error.to_string(),
            };
            tracing::warn!(
                chat_id = target.chat_id,
                trigger_message_id = target.trigger_message_id,
                error = %queue_error,
                "obligation notice text failed to queue; falling back to reaction"
            );
            match self
                .signal
                .signal_turn_failure(SignalTarget {
                    chat_id: target.chat_id,
                    thread_id: target.thread_id,
                    message_id: target.trigger_message_id,
                    emoji: super::signal::DEFAULT_DIALOG_TERMINAL_REACTION_EMOJI.to_owned(),
                    text_fallback_allowed: false,
                })
                .await
            {
                super::signal::UserSignalResult::ReactionSent => {
                    ObligationNoticeResult::ReactionSent
                }
                other => ObligationNoticeResult::Failed(format!(
                    "notice text failed ({queue_error}); reaction fallback: {other:?}"
                )),
            }
        })
    }
}

/// Kind-specific delivery deadlines; the one-time extension reuses the same
/// timeout from the moment the "taking longer" notice is sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryObligationTimeouts {
    pub image: TimeDuration,
    pub music: TimeDuration,
}

impl DeliveryObligationTimeouts {
    #[must_use]
    pub fn from_secs(image_secs: i32, music_secs: i32) -> Self {
        Self {
            image: TimeDuration::seconds(i64::from(image_secs.max(1))),
            music: TimeDuration::seconds(i64::from(music_secs.max(1))),
        }
    }

    fn for_kind(&self, kind: &str) -> TimeDuration {
        if kind == SIDE_EFFECT_KIND_MUSIC {
            self.music
        } else {
            self.image
        }
    }
}

impl Default for DeliveryObligationTimeouts {
    fn default() -> Self {
        Self::from_secs(
            DEFAULT_DIALOG_IMAGE_DELIVERY_TIMEOUT_SECS,
            DEFAULT_DIALOG_MUSIC_DELIVERY_TIMEOUT_SECS,
        )
    }
}

/// Counters from one watcher tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObligationWatchTickReport {
    pub scanned: usize,
    pub delivered: usize,
    pub failed_notified: usize,
    pub orphaned_notified: usize,
    pub extended: usize,
    pub expired_notified: usize,
    pub notices_sent: usize,
    pub notice_failures: usize,
    pub errors: usize,
}

impl ObligationWatchTickReport {
    fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Resolve every open obligation once. Only the transition winner notifies, so
/// concurrent ticks or a watcher restart cannot double-send a notice.
pub async fn process_delivery_obligations_once(
    store: &dyn DeliveryObligationStore,
    tickets: &dyn TicketRecordSource,
    notifier: &dyn DeliveryObligationNotifier,
    timeouts: DeliveryObligationTimeouts,
    now: OffsetDateTime,
) -> ObligationWatchTickReport {
    let mut report = ObligationWatchTickReport::default();
    let obligations = match store.list_open(OBLIGATION_SCAN_LIMIT).await {
        Ok(obligations) => obligations,
        Err(error) => {
            tracing::warn!(%error, "failed to list open delivery obligations");
            report.errors += 1;
            return report;
        }
    };
    for obligation in obligations {
        report.scanned += 1;
        if let Err(error) = resolve_obligation(
            store,
            tickets,
            notifier,
            timeouts,
            now,
            &obligation,
            &mut report,
        )
        .await
        {
            tracing::warn!(
                obligation_id = obligation.id,
                ticket_job_id = obligation.ticket_job_id,
                %error,
                "failed to resolve delivery obligation"
            );
            report.errors += 1;
        }
    }
    report
}

async fn resolve_obligation(
    store: &dyn DeliveryObligationStore,
    tickets: &dyn TicketRecordSource,
    notifier: &dyn DeliveryObligationNotifier,
    timeouts: DeliveryObligationTimeouts,
    now: OffsetDateTime,
    obligation: &DeliveryObligationRecord,
    report: &mut ObligationWatchTickReport,
) -> Result<(), ObligationError> {
    let ticket = tickets.ticket_record(obligation.ticket_job_id).await?;
    match ticket {
        Some(record) => match record.status {
            // Failed tickets keep their placeholder in result_message_id — a
            // Failed status is failed regardless of that field.
            JobStatus::Failed => {
                if store
                    .mark_notified(
                        obligation.id,
                        DELIVERY_OBLIGATION_STATE_FAILED_NOTIFIED,
                        "ticket_failed",
                        now,
                    )
                    .await?
                {
                    report.failed_notified += 1;
                    send_notice(notifier, obligation, OBLIGATION_FAILURE_NOTICE, report).await;
                }
            }
            JobStatus::Completed => {
                if let Some(result_message_id) = record.result_message_id {
                    if store
                        .mark_delivered(obligation.id, Some(result_message_id), "delivered", now)
                        .await?
                    {
                        report.delivered += 1;
                    }
                } else if store
                    .mark_notified(
                        obligation.id,
                        DELIVERY_OBLIGATION_STATE_ORPHANED_NOTIFIED,
                        "completed_without_result_message",
                        now,
                    )
                    .await?
                {
                    report.orphaned_notified += 1;
                    send_notice(notifier, obligation, OBLIGATION_FAILURE_NOTICE, report).await;
                }
            }
            // The user cancelled their own job; an error notice would be wrong.
            JobStatus::Cancelled => {
                if store
                    .mark_delivered(obligation.id, None, "ticket_cancelled", now)
                    .await?
                {
                    report.delivered += 1;
                }
            }
            JobStatus::Pending | JobStatus::Processing => {
                resolve_in_flight_deadline(store, notifier, timeouts, now, obligation, report)
                    .await?;
            }
        },
        None => {
            if store
                .mark_notified(
                    obligation.id,
                    DELIVERY_OBLIGATION_STATE_ORPHANED_NOTIFIED,
                    "ticket_record_missing",
                    now,
                )
                .await?
            {
                report.orphaned_notified += 1;
                send_notice(notifier, obligation, OBLIGATION_FAILURE_NOTICE, report).await;
            }
        }
    }
    Ok(())
}

/// Punch #8: past-deadline tickets that are still alive get ONE "taking
/// longer" notice with a single deadline extension; only a ticket that also
/// outlives the extension expires with the error notice. The ticket itself is
/// left alone — a late artifact after `extended_once` still resolves as
/// delivered, never error-then-image.
async fn resolve_in_flight_deadline(
    store: &dyn DeliveryObligationStore,
    notifier: &dyn DeliveryObligationNotifier,
    timeouts: DeliveryObligationTimeouts,
    now: OffsetDateTime,
    obligation: &DeliveryObligationRecord,
    report: &mut ObligationWatchTickReport,
) -> Result<(), ObligationError> {
    if now <= obligation.deadline_at {
        return Ok(());
    }
    if obligation.state == DELIVERY_OBLIGATION_STATE_PENDING {
        let new_deadline = now + timeouts.for_kind(&obligation.kind);
        if store.extend_once(obligation.id, new_deadline, now).await? {
            report.extended += 1;
            send_notice(notifier, obligation, OBLIGATION_EXTENDED_NOTICE, report).await;
        }
        return Ok(());
    }
    if obligation.state == DELIVERY_OBLIGATION_STATE_EXTENDED_ONCE
        && store
            .mark_notified(
                obligation.id,
                DELIVERY_OBLIGATION_STATE_EXPIRED_NOTIFIED,
                "deadline_expired_after_extension",
                now,
            )
            .await?
    {
        report.expired_notified += 1;
        send_notice(notifier, obligation, OBLIGATION_FAILURE_NOTICE, report).await;
    }
    Ok(())
}

async fn send_notice(
    notifier: &dyn DeliveryObligationNotifier,
    obligation: &DeliveryObligationRecord,
    text: &str,
    report: &mut ObligationWatchTickReport,
) {
    let result = notifier
        .notify(ObligationNoticeTarget {
            chat_id: obligation.chat_id,
            thread_id: obligation.thread_id,
            trigger_message_id: obligation.trigger_message_id,
            text,
        })
        .await;
    match result {
        ObligationNoticeResult::TextQueued | ObligationNoticeResult::ReactionSent => {
            report.notices_sent += 1;
        }
        ObligationNoticeResult::Failed(error) => {
            report.notice_failures += 1;
            tracing::warn!(
                obligation_id = obligation.id,
                chat_id = obligation.chat_id,
                trigger_message_id = obligation.trigger_message_id,
                %error,
                "delivery obligation notice failed"
            );
        }
    }
}

/// Periodic watcher loop (poller pattern, like `run_router_trigger_poller`).
pub async fn run_delivery_obligation_watcher<Stop>(
    store: Arc<dyn DeliveryObligationStore>,
    tickets: Arc<dyn TicketRecordSource>,
    notifier: Arc<dyn DeliveryObligationNotifier>,
    timeouts: DeliveryObligationTimeouts,
    interval: Duration,
    stop: Stop,
) where
    Stop: Future<Output = ()>,
{
    let interval = if interval.is_zero() {
        Duration::from_secs(
            u64::try_from(DEFAULT_DIALOG_OBLIGATION_WATCH_INTERVAL_SECS).unwrap_or(15),
        )
    } else {
        interval
    };
    let mut stop = std::pin::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                let report = process_delivery_obligations_once(
                    store.as_ref(),
                    tickets.as_ref(),
                    notifier.as_ref(),
                    timeouts,
                    OffsetDateTime::now_utc(),
                )
                .await;
                if !report.is_empty() {
                    tracing::debug!(?report, "processed delivery obligation tick");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use openplotva_storage::{
        DELIVERY_OBLIGATION_DIALOG_JOB_UNKNOWN, DELIVERY_OBLIGATION_STATE_DELIVERED,
    };
    use openplotva_taskman::{ImageGenJobParams, new_image_gen_job_at};
    use serde_json::json;

    use super::*;

    fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    struct FakeObligationStore {
        rows: Mutex<Vec<DeliveryObligationRecord>>,
    }

    impl FakeObligationStore {
        fn new(rows: Vec<DeliveryObligationRecord>) -> Self {
            Self {
                rows: Mutex::new(rows),
            }
        }

        fn state_of(&self, id: i64) -> String {
            lock(&self.rows)
                .iter()
                .find(|row| row.id == id)
                .map(|row| row.state.clone())
                .expect("obligation exists")
        }

        fn row(&self, id: i64) -> DeliveryObligationRecord {
            lock(&self.rows)
                .iter()
                .find(|row| row.id == id)
                .cloned()
                .expect("obligation exists")
        }
    }

    const OPEN_STATES: [&str; 2] = [
        DELIVERY_OBLIGATION_STATE_PENDING,
        DELIVERY_OBLIGATION_STATE_EXTENDED_ONCE,
    ];

    impl DeliveryObligationStore for FakeObligationStore {
        fn list_open<'a>(
            &'a self,
            _limit: i64,
        ) -> ObligationFuture<'a, Vec<DeliveryObligationRecord>> {
            let rows = lock(&self.rows)
                .iter()
                .filter(|row| OPEN_STATES.contains(&row.state.as_str()))
                .cloned()
                .collect();
            Box::pin(async move { Ok(rows) })
        }

        fn mark_delivered<'a>(
            &'a self,
            id: i64,
            result_message_id: Option<i32>,
            _detail: &'a str,
            now: OffsetDateTime,
        ) -> ObligationFuture<'a, bool> {
            let mut rows = lock(&self.rows);
            let won = rows
                .iter_mut()
                .find(|row| row.id == id && OPEN_STATES.contains(&row.state.as_str()))
                .map(|row| {
                    row.state = DELIVERY_OBLIGATION_STATE_DELIVERED.to_owned();
                    row.result_message_id = result_message_id;
                    row.resolved_at = Some(now);
                })
                .is_some();
            Box::pin(async move { Ok(won) })
        }

        fn mark_notified<'a>(
            &'a self,
            id: i64,
            state: &'static str,
            _detail: &'a str,
            now: OffsetDateTime,
        ) -> ObligationFuture<'a, bool> {
            let mut rows = lock(&self.rows);
            let won = rows
                .iter_mut()
                .find(|row| row.id == id && OPEN_STATES.contains(&row.state.as_str()))
                .map(|row| {
                    row.state = state.to_owned();
                    row.resolved_at = Some(now);
                })
                .is_some();
            Box::pin(async move { Ok(won) })
        }

        fn extend_once<'a>(
            &'a self,
            id: i64,
            new_deadline: OffsetDateTime,
            _now: OffsetDateTime,
        ) -> ObligationFuture<'a, bool> {
            let mut rows = lock(&self.rows);
            let won = rows
                .iter_mut()
                .find(|row| row.id == id && row.state == DELIVERY_OBLIGATION_STATE_PENDING)
                .map(|row| {
                    row.state = DELIVERY_OBLIGATION_STATE_EXTENDED_ONCE.to_owned();
                    row.deadline_at = new_deadline;
                })
                .is_some();
            Box::pin(async move { Ok(won) })
        }
    }

    #[derive(Default)]
    struct FakeTicketSource {
        records: HashMap<i64, TaskQueueRecord>,
    }

    impl FakeTicketSource {
        fn with_records(records: Vec<TaskQueueRecord>) -> Self {
            Self {
                records: records
                    .into_iter()
                    .map(|record| (record.id, record))
                    .collect(),
            }
        }
    }

    impl TicketRecordSource for FakeTicketSource {
        fn ticket_record<'a>(
            &'a self,
            ticket_job_id: i64,
        ) -> ObligationFuture<'a, Option<TaskQueueRecord>> {
            let record = self.records.get(&ticket_job_id).cloned();
            Box::pin(async move { Ok(record) })
        }
    }

    #[derive(Default)]
    struct FakeNotifier {
        notices: Mutex<Vec<(i64, i32, String)>>,
    }

    impl FakeNotifier {
        fn notices(&self) -> Vec<(i64, i32, String)> {
            lock(&self.notices).clone()
        }
    }

    impl DeliveryObligationNotifier for FakeNotifier {
        fn notify<'a>(
            &'a self,
            target: ObligationNoticeTarget<'a>,
        ) -> Pin<Box<dyn Future<Output = ObligationNoticeResult> + Send + 'a>> {
            lock(&self.notices).push((
                target.chat_id,
                target.trigger_message_id,
                target.text.to_owned(),
            ));
            Box::pin(async move { ObligationNoticeResult::TextQueued })
        }
    }

    fn base_time() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("timestamp")
    }

    fn obligation(
        id: i64,
        ticket_job_id: i64,
        deadline_at: OffsetDateTime,
    ) -> DeliveryObligationRecord {
        DeliveryObligationRecord {
            id,
            created_at: base_time(),
            chat_id: 42,
            thread_id: None,
            user_id: 7,
            trigger_message_id: 100,
            dialog_job_id: DELIVERY_OBLIGATION_DIALOG_JOB_UNKNOWN,
            kind: openplotva_dialog::turn::SIDE_EFFECT_KIND_IMAGE.to_owned(),
            ticket_job_id,
            deadline_at,
            state: DELIVERY_OBLIGATION_STATE_PENDING.to_owned(),
            result_message_id: None,
            resolved_at: None,
            detail: json!({}),
        }
    }

    fn ticket(id: i64, status: JobStatus, result_message_id: Option<i32>) -> TaskQueueRecord {
        TaskQueueRecord {
            id,
            queue_name: "image".to_owned(),
            status,
            job: new_image_gen_job_at(ImageGenJobParams::default(), base_time()),
            worker_id: None,
            started_at: None,
            execution_started_at: None,
            completed_at: None,
            error: None,
            progress_message_id: None,
            queue_position_message_id: None,
            queue_position_message_pending: false,
            result_message_id,
            messages: Vec::new(),
            events: Vec::new(),
            agent_state: None,
        }
    }

    fn timeouts() -> DeliveryObligationTimeouts {
        DeliveryObligationTimeouts::from_secs(1200, 1800)
    }

    #[tokio::test]
    async fn watcher_resolves_tickets_by_state_table() {
        let now = base_time();
        let future_deadline = now + TimeDuration::minutes(20);
        struct Case {
            name: &'static str,
            ticket: Option<TaskQueueRecord>,
            expected_state: &'static str,
            expected_notice: Option<&'static str>,
        }
        let cases = vec![
            Case {
                name: "completed with result message is delivered silently",
                ticket: Some(ticket(500, JobStatus::Completed, Some(9000))),
                expected_state: DELIVERY_OBLIGATION_STATE_DELIVERED,
                expected_notice: None,
            },
            Case {
                name: "failed ticket notifies even with placeholder result_message_id",
                ticket: Some(ticket(500, JobStatus::Failed, Some(9000))),
                expected_state: DELIVERY_OBLIGATION_STATE_FAILED_NOTIFIED,
                expected_notice: Some(OBLIGATION_FAILURE_NOTICE),
            },
            Case {
                name: "completed without result message is orphaned",
                ticket: Some(ticket(500, JobStatus::Completed, None)),
                expected_state: DELIVERY_OBLIGATION_STATE_ORPHANED_NOTIFIED,
                expected_notice: Some(OBLIGATION_FAILURE_NOTICE),
            },
            Case {
                name: "missing ticket record is orphaned",
                ticket: None,
                expected_state: DELIVERY_OBLIGATION_STATE_ORPHANED_NOTIFIED,
                expected_notice: Some(OBLIGATION_FAILURE_NOTICE),
            },
            Case {
                name: "cancelled ticket resolves silently",
                ticket: Some(ticket(500, JobStatus::Cancelled, None)),
                expected_state: DELIVERY_OBLIGATION_STATE_DELIVERED,
                expected_notice: None,
            },
            Case {
                name: "in-flight ticket before deadline is left open",
                ticket: Some(ticket(500, JobStatus::Processing, None)),
                expected_state: DELIVERY_OBLIGATION_STATE_PENDING,
                expected_notice: None,
            },
        ];
        for case in cases {
            let store = FakeObligationStore::new(vec![obligation(1, 500, future_deadline)]);
            let tickets = FakeTicketSource::with_records(case.ticket.into_iter().collect());
            let notifier = FakeNotifier::default();

            let report =
                process_delivery_obligations_once(&store, &tickets, &notifier, timeouts(), now)
                    .await;

            assert_eq!(report.scanned, 1, "case: {}", case.name);
            assert_eq!(report.errors, 0, "case: {}", case.name);
            assert_eq!(
                store.state_of(1),
                case.expected_state,
                "case: {}",
                case.name
            );
            match case.expected_notice {
                Some(text) => {
                    assert_eq!(
                        notifier.notices(),
                        vec![(42, 100, text.to_owned())],
                        "case: {}",
                        case.name
                    );
                }
                None => assert!(notifier.notices().is_empty(), "case: {}", case.name),
            }
        }
    }

    #[tokio::test]
    async fn deadline_extends_once_with_single_notice_then_expires() {
        let now = base_time();
        let past_deadline = now - TimeDuration::minutes(1);
        let store = FakeObligationStore::new(vec![obligation(1, 500, past_deadline)]);
        let tickets =
            FakeTicketSource::with_records(vec![ticket(500, JobStatus::Processing, None)]);
        let notifier = FakeNotifier::default();

        // First tick past deadline: extend once + one "taking longer" notice.
        let first =
            process_delivery_obligations_once(&store, &tickets, &notifier, timeouts(), now).await;
        assert_eq!(first.extended, 1);
        assert_eq!(store.state_of(1), DELIVERY_OBLIGATION_STATE_EXTENDED_ONCE);
        assert_eq!(
            store.row(1).deadline_at,
            now + TimeDuration::seconds(1200),
            "deadline pushed once by the kind timeout"
        );
        assert_eq!(
            notifier.notices(),
            vec![(42, 100, OBLIGATION_EXTENDED_NOTICE.to_owned())]
        );

        // Second tick before the new deadline: nothing happens (single notice).
        let second =
            process_delivery_obligations_once(&store, &tickets, &notifier, timeouts(), now).await;
        assert_eq!(second.extended, 0);
        assert_eq!(second.expired_notified, 0);
        assert_eq!(notifier.notices().len(), 1, "no duplicate notice");

        // Past the extended deadline too: expire with the error notice; the
        // ticket itself is left alone.
        let later = now + TimeDuration::seconds(1201);
        let third =
            process_delivery_obligations_once(&store, &tickets, &notifier, timeouts(), later).await;
        assert_eq!(third.expired_notified, 1);
        assert_eq!(
            store.state_of(1),
            DELIVERY_OBLIGATION_STATE_EXPIRED_NOTIFIED
        );
        assert_eq!(notifier.notices().len(), 2);
        assert_eq!(notifier.notices()[1].2, OBLIGATION_FAILURE_NOTICE);
    }

    #[tokio::test]
    async fn double_tick_on_failed_ticket_notifies_once() {
        let now = base_time();
        let store =
            FakeObligationStore::new(vec![obligation(1, 500, now + TimeDuration::minutes(20))]);
        let tickets = FakeTicketSource::with_records(vec![ticket(500, JobStatus::Failed, None)]);
        let notifier = FakeNotifier::default();

        let first =
            process_delivery_obligations_once(&store, &tickets, &notifier, timeouts(), now).await;
        let second =
            process_delivery_obligations_once(&store, &tickets, &notifier, timeouts(), now).await;

        assert_eq!(first.failed_notified, 1);
        assert_eq!(second.scanned, 0, "resolved obligation is no longer open");
        assert_eq!(second.failed_notified, 0);
        assert_eq!(notifier.notices().len(), 1, "winner notifies exactly once");
    }

    #[tokio::test]
    async fn fallback_ticket_source_resolves_from_secondary_after_restart() {
        let now = base_time();
        let store =
            FakeObligationStore::new(vec![obligation(1, 500, now + TimeDuration::minutes(20))]);
        // Restarted process: the in-memory primary lost the record; the durable
        // Postgres row still resolves the obligation.
        let source = FallbackTicketRecordSource::new(
            FakeTicketSource::default(),
            Some(FakeTicketSource::with_records(vec![ticket(
                500,
                JobStatus::Completed,
                Some(9000),
            )])),
        );
        let notifier = FakeNotifier::default();

        let report =
            process_delivery_obligations_once(&store, &source, &notifier, timeouts(), now).await;

        assert_eq!(report.delivered, 1);
        assert_eq!(store.state_of(1), DELIVERY_OBLIGATION_STATE_DELIVERED);
        assert_eq!(store.row(1).result_message_id, Some(9000));
        assert!(notifier.notices().is_empty());
    }

    #[tokio::test]
    async fn dispatcher_notifier_queues_protected_reply_with_reply_scoped_debounce_key() {
        let queue = Arc::new(DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        ));
        // Nothing listens on the discard port, so the reaction fallback (never
        // reached here) would fail fast in tests.
        let client =
            openplotva_telegram::telegram_client_with_base_url("TEST-TOKEN", "http://127.0.0.1:9/")
                .expect("test telegram client");
        let signal = DispatcherTerminalUserSignal::new(client, Arc::clone(&queue));
        let notifier = DispatcherDeliveryObligationNotifier::new(Arc::clone(&queue), signal)
            .with_virtual_id_factory(Arc::new(|| "obligation-v1".to_owned()));

        let result = notifier
            .notify(ObligationNoticeTarget {
                chat_id: 42,
                thread_id: None,
                trigger_message_id: 100,
                text: OBLIGATION_FAILURE_NOTICE,
            })
            .await;

        assert_eq!(result, ObligationNoticeResult::TextQueued);
        let item = queue.dequeue_immediate().expect("queued notice");
        assert!(item.metadata().protected, "notice must survive queue trims");
        assert!(
            item.metadata().fingerprint_key.ends_with(":r100"),
            "debounce key is reply-scoped, got {}",
            item.metadata().fingerprint_key
        );
        let (_, method) = item.into_parts();
        let Some(openplotva_telegram::TelegramOutboundMethod::SendMessage(method)) = method else {
            panic!("expected sendMessage notice");
        };
        let value = serde_json::to_value(method.as_ref()).expect("method json");
        assert_eq!(value["chat_id"], 42);
        assert_eq!(value["text"], OBLIGATION_FAILURE_NOTICE);
        assert_eq!(value["reply_parameters"]["message_id"], 100);
    }
}
