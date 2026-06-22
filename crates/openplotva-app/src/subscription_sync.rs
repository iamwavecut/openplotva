//! Telegram Stars subscription reconciliation service.

use std::{fmt, future::Future, pin::Pin, time::Duration};

use openplotva_core::UserState;
use openplotva_storage::{VipEventRecord, VipSummaryRecord};
use time::OffsetDateTime;

use crate::payments::{
    PaymentEffectsFuture, PaymentStoreFuture, PaymentTextMessage, SuccessfulPayment,
    SuccessfulPaymentEffects, SuccessfulPaymentMessage, SuccessfulPaymentOutcome,
    SuccessfulPaymentStore, process_successful_payment_at,
};

pub const DEFAULT_SUBSCRIPTION_SYNC_ENABLED: bool = true;
pub const DEFAULT_SUBSCRIPTION_SYNC_INTERVAL: Duration = Duration::from_secs(3_600);
pub const DEFAULT_SUBSCRIPTION_SYNC_PAGE_LIMIT: i64 = 100;
pub const SUBSCRIPTION_SYNC_COMPENSATION_MULTIPLIER: i64 = 2;

pub type SubscriptionSyncSourceFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

pub type SubscriptionSyncStoreFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionSyncConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub dry_run: bool,
    pub page_limit: i64,
}

impl Default for SubscriptionSyncConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_SUBSCRIPTION_SYNC_ENABLED,
            interval: DEFAULT_SUBSCRIPTION_SYNC_INTERVAL,
            dry_run: false,
            page_limit: DEFAULT_SUBSCRIPTION_SYNC_PAGE_LIMIT,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarSubscriptionPayment {
    pub telegram_payment_charge_id: String,
    pub user: UserState,
    pub invoice_payload: String,
    pub amount_stars: i64,
    pub paid_at: OffsetDateTime,
    pub subscription_period_seconds: i64,
}

impl From<openplotva_telegram::StarSubscriptionPayment> for StarSubscriptionPayment {
    fn from(value: openplotva_telegram::StarSubscriptionPayment) -> Self {
        Self {
            telegram_payment_charge_id: value.telegram_payment_charge_id,
            user: UserState::new(
                value.user_id,
                value.first_name,
                value.last_name,
                value.username,
                value.language_code,
                value.is_premium,
            ),
            invoice_payload: value.invoice_payload,
            amount_stars: value.amount_stars,
            paid_at: OffsetDateTime::from_unix_timestamp(value.paid_at_unix)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
            subscription_period_seconds: value.subscription_period_seconds,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VipCoverageInterval {
    pub started_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionSyncExpiry {
    pub paid_expires_at: OffsetDateTime,
    pub vip_expires_at: OffsetDateTime,
    pub uncovered_seconds: i64,
    pub compensation_seconds: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubscriptionSyncItemStatus {
    AlreadySynced,
    MissingSubscription,
    MissingVipEvent,
    Repaired,
    RepairedCandidate,
    ExpiredMissedPayment,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionSyncItemReport {
    pub status: SubscriptionSyncItemStatus,
    pub user_id: i64,
    pub telegram_payment_charge_id: String,
    pub paid_at: OffsetDateTime,
    pub target_expires_at: OffsetDateTime,
    pub uncovered_seconds: i64,
    pub compensation_seconds: i64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubscriptionSyncReport {
    pub scanned: usize,
    pub subscription_payments: usize,
    pub already_synced: usize,
    pub repaired: usize,
    pub repaired_candidates: usize,
    pub expired_missed_payments: usize,
    pub errors: usize,
    pub items: Vec<SubscriptionSyncItemReport>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubscriptionSyncWorkerReport {
    pub runs: usize,
    pub last_report: Option<SubscriptionSyncReport>,
}

pub trait StarSubscriptionSyncSource {
    type Error: fmt::Display + Send + Sync + 'static;

    fn list_subscription_payments<'a>(
        &'a self,
        page_limit: i64,
    ) -> SubscriptionSyncSourceFuture<'a, Vec<StarSubscriptionPayment>, Self::Error>;
}

pub trait SubscriptionSyncLedgerStore: SuccessfulPaymentStore {
    fn list_vip_coverage_intervals<'a>(
        &'a self,
        user_id: i64,
    ) -> SubscriptionSyncStoreFuture<'a, Vec<VipCoverageInterval>, Self::Error>;

    fn delete_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> SubscriptionSyncStoreFuture<'a, (), Self::Error>;
}

#[derive(Clone, Debug)]
pub struct TelegramStarSubscriptionSyncSource {
    client: openplotva_telegram::StarTransactionsClient,
}

impl TelegramStarSubscriptionSyncSource {
    #[must_use]
    pub fn new(client: openplotva_telegram::StarTransactionsClient) -> Self {
        Self { client }
    }
}

impl StarSubscriptionSyncSource for TelegramStarSubscriptionSyncSource {
    type Error = openplotva_telegram::StarTransactionsError;

    fn list_subscription_payments<'a>(
        &'a self,
        page_limit: i64,
    ) -> SubscriptionSyncSourceFuture<'a, Vec<StarSubscriptionPayment>, Self::Error> {
        Box::pin(async move {
            let mut offset = 0;
            let mut payments = Vec::new();
            loop {
                let page = self
                    .client
                    .get_star_transactions_page(offset, page_limit)
                    .await?;
                let transaction_count = page.transaction_count;
                payments.extend(page.payments.into_iter().map(StarSubscriptionPayment::from));
                if transaction_count < page_limit as usize {
                    break;
                }
                offset += page_limit;
            }
            payments.sort_by(|left, right| {
                (left.paid_at, &left.telegram_payment_charge_id)
                    .cmp(&(right.paid_at, &right.telegram_payment_charge_id))
            });
            Ok(payments)
        })
    }
}

pub async fn run_subscription_sync_once<Source, Store>(
    source: &Source,
    store: &Store,
    now: OffsetDateTime,
    config: SubscriptionSyncConfig,
) -> SubscriptionSyncReport
where
    Source: StarSubscriptionSyncSource + Sync,
    Store: SubscriptionSyncLedgerStore + Sync,
{
    if !config.enabled {
        return SubscriptionSyncReport::default();
    }
    let payments = match source.list_subscription_payments(config.page_limit).await {
        Ok(payments) => payments,
        Err(error) => {
            return SubscriptionSyncReport {
                errors: 1,
                items: vec![SubscriptionSyncItemReport {
                    status: SubscriptionSyncItemStatus::Error,
                    user_id: 0,
                    telegram_payment_charge_id: String::new(),
                    paid_at: now,
                    target_expires_at: now,
                    uncovered_seconds: 0,
                    compensation_seconds: 0,
                    error: Some(error.to_string()),
                }],
                ..SubscriptionSyncReport::default()
            };
        }
    };
    let mut report = SubscriptionSyncReport {
        scanned: payments.len(),
        subscription_payments: payments.len(),
        ..SubscriptionSyncReport::default()
    };
    for payment in payments {
        let effects = SubscriptionSyncEffects { store };
        let item =
            sync_one_subscription_payment(store, &effects, payment, now, config.dry_run).await;
        match item.status {
            SubscriptionSyncItemStatus::AlreadySynced => report.already_synced += 1,
            SubscriptionSyncItemStatus::Repaired => report.repaired += 1,
            SubscriptionSyncItemStatus::RepairedCandidate
            | SubscriptionSyncItemStatus::MissingSubscription
            | SubscriptionSyncItemStatus::MissingVipEvent => report.repaired_candidates += 1,
            SubscriptionSyncItemStatus::ExpiredMissedPayment => {
                report.expired_missed_payments += 1;
            }
            SubscriptionSyncItemStatus::Error => report.errors += 1,
        }
        report.items.push(item);
    }

    report
}

pub async fn run_subscription_sync_worker_until<Source, Store, Stop>(
    source: &Source,
    store: &Store,
    config: SubscriptionSyncConfig,
    stop: Stop,
) -> SubscriptionSyncWorkerReport
where
    Source: StarSubscriptionSyncSource + Sync,
    Store: SubscriptionSyncLedgerStore + Sync,
    Stop: Future<Output = ()>,
{
    tokio::pin!(stop);
    let mut report = SubscriptionSyncWorkerReport::default();
    if !config.enabled {
        return report;
    }

    loop {
        let now = OffsetDateTime::now_utc();
        let current = run_subscription_sync_once(source, store, now, config).await;
        tracing::info!(
            scanned = current.scanned,
            subscription_payments = current.subscription_payments,
            already_synced = current.already_synced,
            repaired = current.repaired,
            repaired_candidates = current.repaired_candidates,
            expired_missed_payments = current.expired_missed_payments,
            errors = current.errors,
            dry_run = config.dry_run,
            "processed subscription sync run"
        );
        report.runs += 1;
        report.last_report = Some(current);

        tokio::select! {
            () = &mut stop => return report,
            () = tokio::time::sleep(config.interval) => {}
        }
    }
}

async fn sync_one_subscription_payment<Store>(
    store: &Store,
    effects: &SubscriptionSyncEffects<'_, Store>,
    payment: StarSubscriptionPayment,
    now: OffsetDateTime,
    dry_run: bool,
) -> SubscriptionSyncItemReport
where
    Store: SubscriptionSyncLedgerStore + Sync,
{
    let intervals = match store.list_vip_coverage_intervals(payment.user.id).await {
        Ok(intervals) => intervals,
        Err(error) => {
            return sync_error_report(&payment, now, error);
        }
    };
    let expiry = compensated_subscription_expiry(&payment, &intervals, now);

    let existing_subscription = match store
        .get_subscription_by_telegram_payment_charge_id(&payment.telegram_payment_charge_id)
        .await
    {
        Ok(subscription) => subscription,
        Err(error) => {
            return sync_error_report(&payment, expiry.vip_expires_at, error);
        }
    };
    if let Some(subscription) = existing_subscription.as_ref() {
        match store
            .get_vip_event_by_subscription_id(subscription.id)
            .await
        {
            Ok(Some(_)) => {
                return sync_item_report(
                    SubscriptionSyncItemStatus::AlreadySynced,
                    &payment,
                    expiry.vip_expires_at,
                    expiry.uncovered_seconds,
                    expiry.compensation_seconds,
                    None,
                );
            }
            Ok(None) => {}
            Err(error) => {
                return sync_error_report(&payment, expiry.vip_expires_at, error);
            }
        }
    }

    if expiry.vip_expires_at <= now {
        return sync_item_report(
            SubscriptionSyncItemStatus::ExpiredMissedPayment,
            &payment,
            expiry.vip_expires_at,
            expiry.uncovered_seconds,
            expiry.compensation_seconds,
            None,
        );
    }

    let candidate_status = if existing_subscription.is_some() {
        SubscriptionSyncItemStatus::MissingVipEvent
    } else {
        SubscriptionSyncItemStatus::MissingSubscription
    };
    if dry_run {
        return sync_item_report(
            candidate_status,
            &payment,
            expiry.vip_expires_at,
            expiry.uncovered_seconds,
            expiry.compensation_seconds,
            None,
        );
    }

    let message = SuccessfulPaymentMessage {
        chat_id: payment.user.id,
        message_id: 0,
        user: payment.user.clone(),
        payment: SuccessfulPayment {
            currency: openplotva_telegram::TELEGRAM_STARS_CURRENCY.to_owned(),
            total_amount: payment.amount_stars,
            invoice_payload: payment.invoice_payload.clone(),
            telegram_payment_charge_id: payment.telegram_payment_charge_id.clone(),
            provider_payment_charge_id: String::new(),
            paid_at: Some(payment.paid_at),
            subscription_period_seconds: Some(payment.subscription_period_seconds),
            subscription_expiration_date: Some(expiry.paid_expires_at),
            vip_target_expires_at: Some(expiry.vip_expires_at),
            is_recurring: Some(true),
            is_first_recurring: None,
        },
    };
    let activation = process_successful_payment_at(store, effects, &message, now).await;
    let status = match activation.outcome {
        SuccessfulPaymentOutcome::SubscriptionProcessed
        | SuccessfulPaymentOutcome::SubscriptionDuplicateProcessed => {
            SubscriptionSyncItemStatus::Repaired
        }
        _ => SubscriptionSyncItemStatus::Error,
    };
    sync_item_report(
        status,
        &payment,
        expiry.vip_expires_at,
        expiry.uncovered_seconds,
        expiry.compensation_seconds,
        activation.error.map(|error| error.to_string()),
    )
}

#[must_use]
pub fn compensated_subscription_expiry(
    payment: &StarSubscriptionPayment,
    intervals: &[VipCoverageInterval],
    now: OffsetDateTime,
) -> SubscriptionSyncExpiry {
    let expected_start = expected_subscription_start(payment.paid_at, intervals);
    let expected_end =
        expected_start + time::Duration::seconds(payment.subscription_period_seconds);
    let observed_end = now.min(expected_end);
    let uncovered_seconds = uncovered_seconds(expected_start, observed_end, intervals);
    let compensation_seconds = uncovered_seconds * SUBSCRIPTION_SYNC_COMPENSATION_MULTIPLIER;
    SubscriptionSyncExpiry {
        paid_expires_at: expected_end,
        vip_expires_at: expected_end + time::Duration::seconds(compensation_seconds),
        uncovered_seconds,
        compensation_seconds,
    }
}

#[must_use]
pub fn compensated_target_expires_at(
    payment: &StarSubscriptionPayment,
    intervals: &[VipCoverageInterval],
    now: OffsetDateTime,
) -> (OffsetDateTime, i64, i64) {
    let expiry = compensated_subscription_expiry(payment, intervals, now);
    (
        expiry.vip_expires_at,
        expiry.uncovered_seconds,
        expiry.compensation_seconds,
    )
}

fn expected_subscription_start(
    paid_at: OffsetDateTime,
    intervals: &[VipCoverageInterval],
) -> OffsetDateTime {
    let mut end = paid_at;
    let mut changed = true;
    while changed {
        changed = false;
        for interval in intervals {
            if interval.started_at <= end && interval.expires_at > end {
                end = interval.expires_at;
                changed = true;
            }
        }
    }
    end
}

fn uncovered_seconds(
    start: OffsetDateTime,
    end: OffsetDateTime,
    intervals: &[VipCoverageInterval],
) -> i64 {
    if end <= start {
        return 0;
    }
    let mut covered = intervals
        .iter()
        .filter_map(|interval| {
            let clipped_start = interval.started_at.max(start);
            let clipped_end = interval.expires_at.min(end);
            (clipped_end > clipped_start).then_some((clipped_start, clipped_end))
        })
        .collect::<Vec<_>>();
    covered.sort_by_key(|(covered_start, _)| *covered_start);

    let mut covered_seconds = 0;
    let mut cursor: Option<(OffsetDateTime, OffsetDateTime)> = None;
    for (covered_start, covered_end) in covered {
        match cursor {
            None => cursor = Some((covered_start, covered_end)),
            Some((start_cursor, end_cursor)) if covered_start <= end_cursor => {
                cursor = Some((start_cursor, end_cursor.max(covered_end)));
            }
            Some((start_cursor, end_cursor)) => {
                covered_seconds += (end_cursor - start_cursor).whole_seconds();
                cursor = Some((covered_start, covered_end));
            }
        }
    }
    if let Some((start_cursor, end_cursor)) = cursor {
        covered_seconds += (end_cursor - start_cursor).whole_seconds();
    }
    ((end - start).whole_seconds() - covered_seconds).max(0)
}

fn sync_error_report<E>(
    payment: &StarSubscriptionPayment,
    target_expires_at: OffsetDateTime,
    error: E,
) -> SubscriptionSyncItemReport
where
    E: fmt::Display,
{
    sync_item_report(
        SubscriptionSyncItemStatus::Error,
        payment,
        target_expires_at,
        0,
        0,
        Some(error.to_string()),
    )
}

fn sync_item_report(
    status: SubscriptionSyncItemStatus,
    payment: &StarSubscriptionPayment,
    target_expires_at: OffsetDateTime,
    uncovered_seconds: i64,
    compensation_seconds: i64,
    error: Option<String>,
) -> SubscriptionSyncItemReport {
    SubscriptionSyncItemReport {
        status,
        user_id: payment.user.id,
        telegram_payment_charge_id: payment.telegram_payment_charge_id.clone(),
        paid_at: payment.paid_at,
        target_expires_at,
        uncovered_seconds,
        compensation_seconds,
        error,
    }
}

#[derive(Clone, Copy, Debug)]
struct SubscriptionSyncEffects<'a, Store> {
    store: &'a Store,
}

impl<Store> SuccessfulPaymentEffects for SubscriptionSyncEffects<'_, Store>
where
    Store: SubscriptionSyncLedgerStore + Sync,
{
    type Error = Store::Error;

    fn send_text<'a>(
        &'a self,
        _message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn invalidate_vip_cache<'a>(&'a self, user_id: i64) -> PaymentEffectsFuture<'a, Self::Error> {
        self.store.delete_vip_cache(user_id)
    }
}

pub struct PostgresSubscriptionSyncStore {
    inner: crate::payments::PostgresSuccessfulPaymentStore,
    vip: openplotva_storage::PostgresVipStore,
}

impl PostgresSubscriptionSyncStore {
    #[must_use]
    pub fn new(
        inner: crate::payments::PostgresSuccessfulPaymentStore,
        vip: openplotva_storage::PostgresVipStore,
    ) -> Self {
        Self { inner, vip }
    }
}

impl SuccessfulPaymentStore for PostgresSubscriptionSyncStore {
    type Error = openplotva_storage::StorageError;

    fn upsert_payment_user<'a>(
        &'a self,
        user: &'a UserState,
    ) -> PaymentStoreFuture<'a, (), Self::Error> {
        self.inner.upsert_payment_user(user)
    }

    fn create_subscription<'a>(
        &'a self,
        subscription: openplotva_storage::SubscriptionCreate<'a>,
    ) -> PaymentStoreFuture<'a, openplotva_storage::SubscriptionRecord, Self::Error> {
        self.inner.create_subscription(subscription)
    }

    fn get_subscription_by_telegram_payment_charge_id<'a>(
        &'a self,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<openplotva_storage::SubscriptionRecord>, Self::Error> {
        self.inner
            .get_subscription_by_telegram_payment_charge_id(telegram_payment_charge_id)
    }

    fn get_vip_summary_by_user<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error> {
        self.inner.get_vip_summary_by_user(user_id)
    }

    fn get_vip_event_by_subscription_id<'a>(
        &'a self,
        subscription_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipEventRecord>, Self::Error> {
        self.inner.get_vip_event_by_subscription_id(subscription_id)
    }

    fn create_donation<'a>(
        &'a self,
        donation: openplotva_storage::DonationCreate<'a>,
    ) -> PaymentStoreFuture<'a, openplotva_storage::DonationRecord, Self::Error> {
        self.inner.create_donation(donation)
    }

    fn get_donation_by_telegram_payment_charge_id<'a>(
        &'a self,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<openplotva_storage::DonationRecord>, Self::Error> {
        self.inner
            .get_donation_by_telegram_payment_charge_id(telegram_payment_charge_id)
    }

    fn create_vip_event<'a>(
        &'a self,
        event: openplotva_storage::VipEventCreate<'a>,
    ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error> {
        self.inner.create_vip_event(event)
    }

    fn is_duplicate_insert_no_row(error: &Self::Error) -> bool {
        crate::payments::PostgresSuccessfulPaymentStore::is_duplicate_insert_no_row(error)
    }
}

impl SubscriptionSyncLedgerStore for PostgresSubscriptionSyncStore {
    fn list_vip_coverage_intervals<'a>(
        &'a self,
        user_id: i64,
    ) -> SubscriptionSyncStoreFuture<'a, Vec<VipCoverageInterval>, Self::Error> {
        Box::pin(async move {
            let events = self.vip.list_vip_events_by_user(user_id).await?;
            Ok(events
                .into_iter()
                .filter_map(|event| {
                    (event.effective_expires_at > event.created_at).then_some(VipCoverageInterval {
                        started_at: event.created_at,
                        expires_at: event.effective_expires_at,
                    })
                })
                .collect())
        })
    }

    fn delete_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> SubscriptionSyncStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.vip.delete_vip_cache(user_id).await })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex, MutexGuard},
    };

    use openplotva_core::VIP_EVENT_TYPE_PAYMENT;
    use openplotva_storage::{
        DonationCreate, DonationRecord, SubscriptionCreate, SubscriptionRecord, VipEventCreate,
    };
    use time::Duration as TimeDuration;

    use super::*;

    #[test]
    fn compensation_doubles_uncovered_subscription_time() {
        let paid_at = OffsetDateTime::from_unix_timestamp(1_779_193_800).unwrap();
        let now = paid_at + TimeDuration::days(2);
        let payment = sample_payment(paid_at);

        let (target, uncovered_seconds, compensation_seconds) =
            compensated_target_expires_at(&payment, &[], now);

        assert_eq!(uncovered_seconds, TimeDuration::days(2).whole_seconds());
        assert_eq!(compensation_seconds, TimeDuration::days(4).whole_seconds());
        assert_eq!(
            target,
            paid_at + TimeDuration::days(30) + TimeDuration::days(4)
        );
    }

    #[tokio::test]
    async fn subscription_sync_dry_run_reports_repair_without_writing() {
        let paid_at = OffsetDateTime::from_unix_timestamp(1_779_193_800).unwrap();
        let now = paid_at + TimeDuration::days(2);
        let source = SourceStub::new(vec![sample_payment(paid_at)]);
        let store = StoreStub::new();

        let report = run_subscription_sync_once(
            &source,
            &store,
            now,
            SubscriptionSyncConfig {
                dry_run: true,
                ..SubscriptionSyncConfig::default()
            },
        )
        .await;

        assert_eq!(report.scanned, 1);
        assert_eq!(report.repaired_candidates, 1);
        assert_eq!(
            report.items[0].status,
            SubscriptionSyncItemStatus::MissingSubscription
        );
        assert_eq!(store.subscriptions().len(), 0);
        assert_eq!(store.vip_events().len(), 0);
        assert_eq!(
            report.items[0].target_expires_at,
            paid_at + TimeDuration::days(30) + TimeDuration::days(4)
        );
    }

    #[tokio::test]
    async fn subscription_sync_repairs_missing_subscription_with_compensation() {
        let paid_at = OffsetDateTime::from_unix_timestamp(1_779_193_800).unwrap();
        let now = paid_at + TimeDuration::days(2);
        let source = SourceStub::new(vec![sample_payment(paid_at)]);
        let paid_expires_at = paid_at + TimeDuration::days(30);
        let target = paid_at + TimeDuration::days(30) + TimeDuration::days(4);
        let store = StoreStub::new().with_next_vip_event(VipEventRecord {
            id: 10,
            user_id: 42,
            event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
            delta_seconds: (target - now).whole_seconds(),
            effective_expires_at: target,
            subscription_id: Some(1),
            actor_user_id: None,
            reason: "payment stx-charge".to_owned(),
            created_at: now,
        });

        let report =
            run_subscription_sync_once(&source, &store, now, SubscriptionSyncConfig::default())
                .await;

        assert_eq!(report.repaired, 1);
        assert_eq!(store.subscriptions().len(), 1);
        assert_eq!(store.subscriptions()[0].expires_at, paid_expires_at);
        assert_eq!(store.vip_events().len(), 1);
        assert_eq!(
            store.vip_events()[0].delta_seconds,
            (target - now).whole_seconds()
        );
        assert_eq!(store.deleted_vip_cache_users(), vec![42]);
    }

    #[tokio::test]
    async fn subscription_sync_skips_already_synced_charge() {
        let paid_at = OffsetDateTime::from_unix_timestamp(1_779_193_800).unwrap();
        let source = SourceStub::new(vec![sample_payment(paid_at)]);
        let subscription = sample_subscription(paid_at + TimeDuration::days(30));
        let event = VipEventRecord {
            id: 10,
            user_id: 42,
            event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
            delta_seconds: TimeDuration::days(30).whole_seconds(),
            effective_expires_at: paid_at + TimeDuration::days(30),
            subscription_id: Some(subscription.id),
            actor_user_id: None,
            reason: "payment stx-charge".to_owned(),
            created_at: paid_at,
        };
        let store = StoreStub::new()
            .with_existing_subscription(subscription)
            .with_existing_event(event);

        let report = run_subscription_sync_once(
            &source,
            &store,
            paid_at + TimeDuration::days(2),
            SubscriptionSyncConfig::default(),
        )
        .await;

        assert_eq!(report.already_synced, 1);
        assert!(store.subscriptions().is_empty());
        assert!(store.vip_events().is_empty());
    }

    fn sample_payment(paid_at: OffsetDateTime) -> StarSubscriptionPayment {
        StarSubscriptionPayment {
            telegram_payment_charge_id: "stx-charge".to_owned(),
            user: UserState::new(
                42,
                "Alice",
                Some("Smith".to_owned()),
                Some("alice".to_owned()),
                Some("en".to_owned()),
                Some(true),
            ),
            invoice_payload: "subscription_42".to_owned(),
            amount_stars: 300,
            paid_at,
            subscription_period_seconds: TimeDuration::days(30).whole_seconds(),
        }
    }

    fn sample_subscription(expires_at: OffsetDateTime) -> SubscriptionRecord {
        SubscriptionRecord {
            id: 1,
            user_id: 42,
            telegram_payment_charge_id: "stx-charge".to_owned(),
            provider_payment_charge_id: String::new(),
            expires_at,
            created_at: expires_at,
            updated_at: expires_at,
            canceled_at: None,
            refunded_at: None,
        }
    }

    #[derive(Clone)]
    struct SourceStub {
        payments: Vec<StarSubscriptionPayment>,
    }

    impl SourceStub {
        fn new(payments: Vec<StarSubscriptionPayment>) -> Self {
            Self { payments }
        }
    }

    impl StarSubscriptionSyncSource for SourceStub {
        type Error = StubError;

        fn list_subscription_payments<'a>(
            &'a self,
            _page_limit: i64,
        ) -> SubscriptionSyncSourceFuture<'a, Vec<StarSubscriptionPayment>, Self::Error> {
            Box::pin(async move { Ok(self.payments.clone()) })
        }
    }

    #[derive(Clone, Default)]
    struct StoreStub {
        state: Arc<Mutex<StoreState>>,
    }

    #[derive(Default)]
    struct StoreState {
        subscriptions: Vec<SubscriptionCreate<'static>>,
        vip_events: Vec<VipEventCreate<'static>>,
        existing_subscription: Option<SubscriptionRecord>,
        existing_event: Option<VipEventRecord>,
        next_vip_events: VecDeque<VipEventRecord>,
        coverage: Vec<VipCoverageInterval>,
        deleted_vip_cache_users: Vec<i64>,
    }

    impl StoreStub {
        fn new() -> Self {
            Self::default()
        }

        fn with_existing_subscription(self, subscription: SubscriptionRecord) -> Self {
            self.lock().existing_subscription = Some(subscription);
            self
        }

        fn with_existing_event(self, event: VipEventRecord) -> Self {
            self.lock().existing_event = Some(event);
            self
        }

        fn with_next_vip_event(self, event: VipEventRecord) -> Self {
            self.lock().next_vip_events.push_back(event);
            self
        }

        fn subscriptions(&self) -> Vec<SubscriptionCreate<'static>> {
            self.lock().subscriptions.clone()
        }

        fn vip_events(&self) -> Vec<VipEventCreate<'static>> {
            self.lock().vip_events.clone()
        }

        fn deleted_vip_cache_users(&self) -> Vec<i64> {
            self.lock().deleted_vip_cache_users.clone()
        }

        fn lock(&self) -> MutexGuard<'_, StoreState> {
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    impl SuccessfulPaymentStore for StoreStub {
        type Error = StubError;

        fn upsert_payment_user<'a>(
            &'a self,
            _user: &'a UserState,
        ) -> PaymentStoreFuture<'a, (), Self::Error> {
            Box::pin(async { Ok(()) })
        }

        fn create_subscription<'a>(
            &'a self,
            subscription: SubscriptionCreate<'a>,
        ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state
                    .subscriptions
                    .push(subscription_create_into_owned(subscription));
                Ok(sample_subscription(subscription.expires_at))
            })
        }

        fn get_subscription_by_telegram_payment_charge_id<'a>(
            &'a self,
            _telegram_payment_charge_id: &'a str,
        ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
            Box::pin(async move { Ok(self.lock().existing_subscription.clone()) })
        }

        fn get_vip_summary_by_user<'a>(
            &'a self,
            _user_id: i64,
        ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error> {
            Box::pin(async { Ok(None) })
        }

        fn get_vip_event_by_subscription_id<'a>(
            &'a self,
            _subscription_id: i64,
        ) -> PaymentStoreFuture<'a, Option<VipEventRecord>, Self::Error> {
            Box::pin(async move { Ok(self.lock().existing_event.clone()) })
        }

        fn create_donation<'a>(
            &'a self,
            _donation: DonationCreate<'a>,
        ) -> PaymentStoreFuture<'a, DonationRecord, Self::Error> {
            Box::pin(async { Err(StubError) })
        }

        fn get_donation_by_telegram_payment_charge_id<'a>(
            &'a self,
            _telegram_payment_charge_id: &'a str,
        ) -> PaymentStoreFuture<'a, Option<DonationRecord>, Self::Error> {
            Box::pin(async { Ok(None) })
        }

        fn create_vip_event<'a>(
            &'a self,
            event: VipEventCreate<'a>,
        ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.vip_events.push(vip_event_create_into_owned(event));
                Ok(state.next_vip_events.pop_front().unwrap())
            })
        }
    }

    impl SubscriptionSyncLedgerStore for StoreStub {
        fn list_vip_coverage_intervals<'a>(
            &'a self,
            _user_id: i64,
        ) -> SubscriptionSyncStoreFuture<'a, Vec<VipCoverageInterval>, Self::Error> {
            Box::pin(async move { Ok(self.lock().coverage.clone()) })
        }

        fn delete_vip_cache<'a>(
            &'a self,
            user_id: i64,
        ) -> SubscriptionSyncStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.lock().deleted_vip_cache_users.push(user_id);
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug)]
    struct StubError;

    impl fmt::Display for StubError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "stub error")
        }
    }

    fn subscription_create_into_owned(
        value: SubscriptionCreate<'_>,
    ) -> SubscriptionCreate<'static> {
        SubscriptionCreate {
            user_id: value.user_id,
            telegram_payment_charge_id: Box::leak(
                value.telegram_payment_charge_id.to_owned().into_boxed_str(),
            ),
            provider_payment_charge_id: Box::leak(
                value.provider_payment_charge_id.to_owned().into_boxed_str(),
            ),
            expires_at: value.expires_at,
        }
    }

    fn vip_event_create_into_owned(value: VipEventCreate<'_>) -> VipEventCreate<'static> {
        VipEventCreate {
            user_id: value.user_id,
            event_type: Box::leak(value.event_type.to_owned().into_boxed_str()),
            delta_seconds: value.delta_seconds,
            subscription_id: value.subscription_id,
            actor_user_id: value.actor_user_id,
            reason: value
                .reason
                .map(|reason| Box::leak(reason.to_owned().into_boxed_str()) as &'static str),
        }
    }
}
