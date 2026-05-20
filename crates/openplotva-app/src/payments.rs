use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, MessageData as TelegramMessageData,
    PreCheckoutQuery as TelegramPreCheckoutQuery, SuccessfulPayment as TelegramSuccessfulPayment,
    TextEntity as TelegramTextEntity, Update as TelegramUpdate, UpdateType as TelegramUpdateType,
    User as TelegramUser,
};
use openplotva_core::{UserState, VIP_EVENT_TYPE_PAYMENT, vip_days_to_seconds};
use openplotva_storage::{
    DonationCreate, DonationRecord, PostgresPaymentStore, PostgresVipStore,
    PostgresVirtualMessageStore, StorageError, SubscriptionCreate, SubscriptionRecord,
    VipCacheRecord, VipEventCreate, VipEventRecord, VipSummaryRecord,
};
use openplotva_taskman::{
    CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, ControlPayment,
    HIGH_PRIORITY, StatelessJobItem, control_job_params_from_stateless_job, new_control_job_at,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;

/// Boxed future returned by payment storage calls.
pub type PaymentStoreFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by payment side-effect calls.
pub type PaymentEffectsFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by VIP cache invalidation calls.
pub type VipCacheInvalidationFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by direct pre-checkout acknowledgement calls.
pub type PreCheckoutFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by direct invoice-link creation calls.
pub type PaymentInvoiceLinkFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<String, E>> + Send + 'a>>;

/// Boxed future returned by taskman control-job assignment calls.
pub type PaymentControlJobQueueFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by payment taskman worker queue calls.
pub type PaymentControlJobWorkerFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by VIP status checks.
pub type VipStatusCheckFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// Telegram successful-payment payload captured from a message/control job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SuccessfulPayment {
    /// Telegram currency. Go only processes Stars payments (`XTR`).
    pub currency: String,
    /// Telegram total amount in Stars.
    pub total_amount: i64,
    /// Invoice payload string.
    pub invoice_payload: String,
    /// Telegram charge ID.
    pub telegram_payment_charge_id: String,
    /// Provider-side charge ID.
    pub provider_payment_charge_id: String,
}

/// Minimal message context needed to process Go successful-payment side effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SuccessfulPaymentMessage {
    /// Telegram chat ID used for the response message.
    pub chat_id: i64,
    /// Telegram message ID used as the response anchor.
    pub message_id: i32,
    /// Paying Telegram user.
    pub user: UserState,
    /// Payment payload reported by Telegram.
    pub payment: SuccessfulPayment,
}

/// User-visible text sent by the Go successful-payment path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaymentTextMessage<'value> {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram message ID used as the response anchor.
    pub reply_to_message_id: i32,
    /// Message text.
    pub text: &'value str,
    /// Go parse mode string.
    pub parse_mode: &'value str,
}

/// Payment processing result class.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SuccessfulPaymentOutcome {
    /// Payment was ignored because the currency is not Telegram Stars.
    UnsupportedCurrency,
    /// Payment was ignored because the invoice payload prefix is unknown.
    UnknownPayload,
    /// VIP subscription payment was persisted and acknowledged.
    SubscriptionProcessed,
    /// VIP subscription duplicate was loaded and acknowledged.
    SubscriptionDuplicateProcessed,
    /// Subscription row persistence failed.
    SubscriptionStorageError,
    /// VIP ledger event persistence failed.
    SubscriptionLedgerError,
    /// Donation payment was persisted and acknowledged.
    DonationProcessed,
    /// Donation duplicate was loaded and acknowledged.
    DonationDuplicateProcessed,
    /// Donation row persistence failed.
    DonationStorageError,
}

/// Payment processing report. Go logs most failures and returns nil, so Rust reports them explicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SuccessfulPaymentReport {
    /// Result class.
    pub outcome: SuccessfulPaymentOutcome,
    /// Displayable storage/side-effect error, if the outcome represents a failure.
    pub error: Option<String>,
}

/// Routing result for a decoded Telegram update passed through the payment-aware wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuccessfulPaymentUpdateRoute {
    /// A Go successful-payment service message was processed.
    Processed(SuccessfulPaymentOutcome),
    /// The update was not a successful-payment service message and was delegated.
    Delegated,
}

/// Routing result for queueing Go successful-payment control jobs from decoded updates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuccessfulPaymentControlJobUpdateRoute {
    /// A successful-payment control job was assigned to the Go control queue.
    Queued,
    /// Control-job assignment failed. Go logs and keeps the update handled.
    QueueError,
    /// The update was not a successful-payment service message and was delegated.
    Delegated,
}

/// Result of building the Go `/vip` or `/donate` invoice control job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PaymentInvoiceControlJobBuild {
    /// A high-priority payment invoice control job was built.
    Job(Box<StatelessJobItem>),
    /// The update was not a payment invoice command.
    NotPaymentCommand,
    /// The command was sent outside a private chat; Go sends a deep-link redirect instead.
    NonPrivateChat,
    /// Go cannot identify a valid non-bot VIP subscriber.
    MissingUser,
    /// `/donate` carried an amount outside the Go-supported Stars range or not an integer.
    InvalidDonationAmount,
}

/// Routing result for queueing Go payment invoice command control jobs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PaymentInvoiceCommandUpdateRoute {
    /// A VIP/donation invoice control job was assigned to the Go control queue.
    Queued,
    /// Control-job assignment failed. Go logs and keeps the command handled.
    QueueError,
    /// The command was sent outside a private chat; caller should send the Go redirect message.
    NonPrivateChat,
    /// VIP invoice command did not have a valid non-bot user.
    MissingUser,
    /// Private `/vip` found an active VIP status and sent Go's status message instead of an invoice.
    ExistingVipStatusSent,
    /// Private `/vip` found an active VIP status but failed to send Go's status message.
    ExistingVipStatusSendError,
    /// Private `/vip` was VIP-positive, but the status lookup failed; Go sends an error instead of queueing.
    ExistingVipStatusLookupError,
    /// Private `/vip` was VIP-positive, but no displayable status view was found.
    ExistingVipStatusUnavailable,
    /// Donation command had an invalid explicit amount.
    InvalidDonationAmount,
    /// The update was not a payment invoice command and was delegated.
    Delegated,
}

/// Pre-checkout acknowledgement result class.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreCheckoutOutcome {
    /// Telegram accepted the successful pre-checkout answer.
    Acknowledged,
    /// Telegram rejected the answer. Go logs this and still treats the update as handled.
    AcknowledgementError,
}

/// Routing result for a decoded Telegram pre-checkout update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreCheckoutUpdateRoute {
    /// A Go pre-checkout query was answered.
    Processed(PreCheckoutOutcome),
    /// The update was not a pre-checkout query and was delegated.
    Delegated,
}

/// Routing result for the payment-aware decoded-update wrapper used before fetcher routing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PaymentUpdateRoute {
    /// A Go pre-checkout query was answered directly.
    PreCheckout(PreCheckoutOutcome),
    /// A successful-payment service message was queued as a taskman control job.
    SuccessfulPayment(SuccessfulPaymentControlJobUpdateRoute),
    /// A `/vip` or `/donate` command was queued or classified for Go side effects.
    InvoiceCommand(PaymentInvoiceCommandUpdateRoute),
    /// The update was not payment-owned and was delegated.
    Delegated,
}

/// HTML message with one invoice URL button, matching Go direct chattable sends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvoiceButtonMessage {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// HTML message text.
    pub text: String,
    /// Inline URL button text.
    pub button_text: String,
    /// Telegram invoice URL returned by `createInvoiceLink`.
    pub url: String,
    /// Telegram parse mode. Go uses HTML.
    pub parse_mode: String,
}

/// Direct group-chat redirect for payment commands that must be completed in private chat.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaymentRedirectMessage {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Message ID to reply to.
    pub reply_to_message_id: i32,
    /// Forum topic ID when the source message belongs to a topic.
    pub message_thread_id: Option<i32>,
    /// Plain-text message body.
    pub text: String,
    /// Inline URL button text.
    pub button_text: String,
    /// Telegram deep link URL.
    pub url: String,
}

/// Direct private-chat status reply for an already-active VIP user.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VipStatusMessage {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Message ID to reply to.
    pub reply_to_message_id: i32,
    /// Forum topic ID when the source message belongs to a topic.
    pub message_thread_id: Option<i32>,
    /// HTML message body.
    pub text: String,
    /// Whether to attach Go's VIP cancellation callback button.
    pub show_cancel_button: bool,
}

/// Control-job invoice execution result class.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum InvoiceControlJobOutcome {
    /// Invoice link was created and the button message was sent.
    InvoiceSent,
    /// VIP invoice jobs cannot determine the paying user.
    MissingUser,
    /// Telegram `createInvoiceLink` returned an error.
    InvoiceLinkRequestError,
    /// Telegram returned a blank invoice link.
    EmptyInvoiceLink,
    /// The final invoice button message failed to send.
    SendError,
}

/// Invoice control-job report. Go returns errors for request/parse/send failures; Rust reports them.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvoiceControlJobReport {
    /// Result class.
    pub outcome: InvoiceControlJobOutcome,
    /// Displayable side-effect error, if the outcome represents a failure.
    pub error: Option<String>,
}

/// Payment-specific taskman control-job routing result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PaymentControlJobOutcome {
    /// `vip_invoice` was routed to the VIP invoice executor.
    VipInvoice(InvoiceControlJobOutcome),
    /// `donate_invoice` was routed to the donation invoice executor.
    DonateInvoice(InvoiceControlJobOutcome),
    /// `successful_payment` was routed to the successful-payment processor.
    SuccessfulPayment(SuccessfulPaymentOutcome),
    /// `successful_payment` did not carry a Telegram payment payload.
    MissingSuccessfulPayment,
    /// `successful_payment` could not reconstruct a paying user.
    MissingSuccessfulPaymentUser,
    /// The control-job kind belongs to another app/fetcher executor.
    NotPaymentControlJob,
}

/// Payment-specific taskman control-job execution report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PaymentControlJobReport {
    /// Result class.
    pub outcome: PaymentControlJobOutcome,
    /// Displayable storage/side-effect error, if the routed executor reported one.
    pub error: Option<String>,
}

/// A concrete taskman row ready for the payment-owned control-job worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaymentControlJobWorkItem {
    /// Taskman job ID used for completion/failure writes.
    pub id: i64,
    /// Stateless Go-shaped task payload.
    pub job: StatelessJobItem,
}

/// Result of one payment-owned taskman worker tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PaymentControlJobWorkerReport {
    /// Whether a job was dequeued.
    pub dequeued: bool,
    /// Dequeued taskman job ID.
    pub job_id: Option<i64>,
    /// Payment dispatcher execution report, when payload decoding succeeded.
    pub execution: Option<PaymentControlJobReport>,
    /// The job was finalized as completed.
    pub completed: bool,
    /// The job was finalized as failed.
    pub failed: bool,
    /// Queue dequeue failed.
    pub dequeue_error: Option<String>,
    /// Queued task payload could not be decoded as Go control-job executor input.
    pub decode_error: Option<String>,
    /// Completion/failure status write failed.
    pub status_error: Option<String>,
}

/// Aggregate report for a long-running payment-owned taskman worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PaymentControlJobWorkerRunReport {
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

/// Concrete Telegram invoice-effect failure.
#[derive(Debug, Error)]
pub enum TelegramPaymentInvoiceEffectError {
    /// A trusted app-level invoice message could not be converted into a Telegram method.
    #[error("failed to build Telegram invoice message: {0}")]
    Build(#[from] openplotva_telegram::OutboundBuildError),
    /// Telegram rejected or failed a direct Bot API request.
    #[error("failed to execute Telegram invoice request: {0}")]
    Execute(#[from] carapax::api::ExecuteError),
}

/// Concrete successful-payment dispatch failure.
#[derive(Debug, Error)]
pub enum SuccessfulPaymentDispatchEffectError {
    /// A payment text message could not be converted into dispatcher work.
    #[error("failed to queue successful-payment text: {0}")]
    Queue(#[from] openplotva_telegram::OutboundBuildError),
    /// VIP cache invalidation failed.
    #[error("failed to invalidate VIP cache: {0}")]
    VipCache(String),
}

impl SuccessfulPaymentReport {
    fn new(outcome: SuccessfulPaymentOutcome) -> Self {
        Self {
            outcome,
            error: None,
        }
    }

    fn with_error(outcome: SuccessfulPaymentOutcome, error: impl fmt::Display) -> Self {
        Self {
            outcome,
            error: Some(error.to_string()),
        }
    }
}

impl InvoiceControlJobReport {
    fn new(outcome: InvoiceControlJobOutcome) -> Self {
        Self {
            outcome,
            error: None,
        }
    }

    fn with_error(outcome: InvoiceControlJobOutcome, error: impl fmt::Display) -> Self {
        Self {
            outcome,
            error: Some(error.to_string()),
        }
    }
}

impl PaymentControlJobReport {
    fn new(outcome: PaymentControlJobOutcome) -> Self {
        Self {
            outcome,
            error: None,
        }
    }

    fn with_error(outcome: PaymentControlJobOutcome, error: impl fmt::Display) -> Self {
        Self {
            outcome,
            error: Some(error.to_string()),
        }
    }

    fn from_invoice(
        outcome: impl FnOnce(InvoiceControlJobOutcome) -> PaymentControlJobOutcome,
        report: InvoiceControlJobReport,
    ) -> Self {
        Self {
            outcome: outcome(report.outcome),
            error: report.error,
        }
    }

    fn from_successful_payment(report: SuccessfulPaymentReport) -> Self {
        Self {
            outcome: PaymentControlJobOutcome::SuccessfulPayment(report.outcome),
            error: report.error,
        }
    }
}

impl PaymentControlJobWorkerRunReport {
    fn record_tick(&mut self, tick: &PaymentControlJobWorkerReport) {
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

/// Storage boundary needed by Go successful-payment processing.
pub trait SuccessfulPaymentStore {
    /// Error returned by the concrete storage implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Persist the paying user. Errors are non-fatal, matching Go `ensureUserPersistence`.
    fn upsert_payment_user<'a>(
        &'a self,
        user: &'a UserState,
    ) -> PaymentStoreFuture<'a, (), Self::Error>;

    /// Create a subscription row.
    fn create_subscription<'a>(
        &'a self,
        subscription: SubscriptionCreate<'a>,
    ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error>;

    /// Load an existing subscription by Telegram charge ID after a duplicate insert result.
    fn get_subscription_by_telegram_payment_charge_id<'a>(
        &'a self,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error>;

    /// Create a donation row.
    fn create_donation<'a>(
        &'a self,
        donation: DonationCreate<'a>,
    ) -> PaymentStoreFuture<'a, DonationRecord, Self::Error>;

    /// Load an existing donation by Telegram charge ID after a duplicate insert result.
    fn get_donation_by_telegram_payment_charge_id<'a>(
        &'a self,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<DonationRecord>, Self::Error>;

    /// Create a VIP ledger event.
    fn create_vip_event<'a>(
        &'a self,
        event: VipEventCreate<'a>,
    ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error>;

    /// Whether an insert error means SQLC's duplicate/no-row fallback should run.
    fn is_duplicate_insert_no_row(error: &Self::Error) -> bool {
        let _ = error;
        false
    }
}

/// Storage boundary needed by Go `buildVIPStatusView`.
pub trait VipStatusStore {
    /// Error returned by the concrete storage implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load the latest VIP ledger summary for a user.
    fn get_vip_summary_by_user<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error>;

    /// Load the current active, non-canceled, non-refunded paid subscription for a user.
    fn get_active_subscription<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error>;

    /// Load the external VIP cache row for a user.
    fn get_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipCacheRecord>, Self::Error>;
}

/// VIP predicate boundary matching Go `Fetcher.IsVIP` before existing-status rendering.
pub trait VipStatusChecker {
    /// Return whether this user should see existing VIP status instead of a new invoice.
    fn is_vip_at<'a>(&'a self, user_id: i64, now: OffsetDateTime) -> VipStatusCheckFuture<'a>;
}

/// Side-effect boundary for Go successful-payment processing.
pub trait SuccessfulPaymentEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Send a user-facing text response. Go ignores send failures here.
    fn send_text<'a>(
        &'a self,
        message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Invalidate cached VIP status after subscription activation.
    fn invalidate_vip_cache<'a>(&'a self, user_id: i64) -> PaymentEffectsFuture<'a, Self::Error>;
}

/// Side-effect boundary for the Go in-memory VIP cache.
pub trait VipCacheInvalidator {
    /// Error returned by the concrete cache implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Invalidate one user-specific Go VIP cache key.
    fn invalidate_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> VipCacheInvalidationFuture<'a, Self::Error>;
}

/// Placeholder invalidator for runtime slices that do not yet carry Go's server main cache.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopVipCacheInvalidator;

impl VipCacheInvalidator for NoopVipCacheInvalidator {
    type Error = std::convert::Infallible;

    fn invalidate_vip_cache<'a>(
        &'a self,
        _user_id: i64,
    ) -> VipCacheInvalidationFuture<'a, Self::Error> {
        Box::pin(async { Ok(()) })
    }
}

/// Side-effect boundary for Go `processPreCheckoutQuery`.
pub trait PreCheckoutPaymentEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Answer a Telegram pre-checkout query with `ok=true`.
    fn answer_pre_checkout_query<'a>(
        &'a self,
        pre_checkout_query_id: &'a str,
    ) -> PreCheckoutFuture<'a, Self::Error>;
}

/// Side-effect boundary for Go existing-VIP status replies.
pub trait VipStatusEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Send Go's existing-VIP status reply.
    fn send_vip_status_message<'a>(
        &'a self,
        message: VipStatusMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Send a Go `sendPaymentErrorMessage` equivalent for VIP status failures.
    fn send_vip_status_error_text<'a>(
        &'a self,
        chat_id: i64,
        reply_to_message_id: i32,
        text: &'a str,
        parse_mode: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;
}

/// Side-effect boundary for Go VIP/donation invoice control jobs.
pub trait PaymentInvoiceEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Create a Telegram invoice link for a VIP subscription.
    fn create_subscription_invoice_link<'a>(
        &'a self,
        request: &'a openplotva_telegram::SubscriptionInvoiceLinkRequest,
    ) -> PaymentInvoiceLinkFuture<'a, Self::Error>;

    /// Create a Telegram invoice link for a donation.
    fn create_donation_invoice_link<'a>(
        &'a self,
        request: &'a openplotva_telegram::DonationInvoiceLinkRequest,
    ) -> PaymentInvoiceLinkFuture<'a, Self::Error>;

    /// Send the final direct invoice button message.
    fn send_invoice_button_message<'a>(
        &'a self,
        message: InvoiceButtonMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Send a Go `sendPaymentErrorMessage` equivalent.
    fn send_invoice_error_text<'a>(
        &'a self,
        chat_id: i64,
        reply_to_message_id: i32,
        text: &'a str,
        parse_mode: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;
}

/// Queue boundary for Go payment-owned taskman control jobs.
pub trait PaymentControlJobQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Assign a control job to a named taskman queue.
    fn assign_payment_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> PaymentControlJobQueueFuture<'a, Self::Error>;
}

/// Queue/status boundary for the payment-owned taskman worker.
pub trait PaymentControlJobWorkerQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Dequeue the next pending payment control job from a named taskman queue.
    fn dequeue_payment_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> PaymentControlJobWorkerFuture<'a, Option<PaymentControlJobWorkItem>, Self::Error>;

    /// Finalize a payment control job as completed.
    fn complete_payment_control_job<'a>(
        &'a self,
        job_id: i64,
        report: &'a PaymentControlJobReport,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error>;

    /// Finalize a payment control job as failed.
    fn fail_payment_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error>;
}

/// Rust-native in-memory control-job queue for the current payment vertical slice.
///
/// Go's current taskman repository is memory/WAL-backed, while the converted SQL corpus drops the
/// old `job_queue` tables. This adapter gives runtime wiring a concrete queue with the same
/// priority ordering for payment-owned control jobs; WAL/snapshot persistence remains a later
/// taskman slice.
#[derive(Clone, Debug)]
pub struct InMemoryPaymentControlJobQueue {
    state: Arc<Mutex<InMemoryPaymentControlJobQueueState>>,
}

/// Version marker for the approved Rust-native payment control-job snapshot codec.
pub const PAYMENT_CONTROL_JOB_QUEUE_SNAPSHOT_FORMAT: &str =
    "openplotva.payment-control-job-queue.v1+json";

#[derive(Clone, Debug, Default)]
struct InMemoryPaymentControlJobQueueState {
    next_id: i64,
    records: Vec<InMemoryPaymentControlJobRecord>,
}

/// In-memory taskman status used by the current payment queue adapter.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum InMemoryPaymentControlJobStatus {
    /// Job is pending worker execution.
    Pending,
    /// Job has been dequeued by a worker.
    Processing,
    /// Job completed without a payment-worker failure.
    Completed,
    /// Job failed and carries an error string.
    Failed,
}

/// Snapshot row for the in-memory payment taskman adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InMemoryPaymentControlJobRecord {
    /// Taskman job ID.
    pub id: i64,
    /// Queue name, usually Go's `control` queue.
    pub queue_name: String,
    /// Current in-memory status.
    pub status: InMemoryPaymentControlJobStatus,
    /// Go-shaped stateless job payload.
    pub job: StatelessJobItem,
    /// Completion report recorded by the payment worker.
    pub completed_report: Option<PaymentControlJobReport>,
    /// Failure string recorded by the payment worker.
    pub error: Option<String>,
}

/// Versioned Rust-native snapshot of the in-memory payment control-job queue.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InMemoryPaymentControlJobQueueSnapshot {
    /// Snapshot codec marker.
    pub format: String,
    /// Next taskman job ID to assign after restore.
    pub next_id: i64,
    /// ID-ordered queue records.
    pub records: Vec<InMemoryPaymentControlJobRecord>,
}

/// Error returned by the in-memory payment taskman adapter.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum InMemoryPaymentControlJobQueueError {
    /// A status update targeted a job ID this queue has never assigned.
    #[error("payment control job {0} not found")]
    JobNotFound(i64),
}

/// Error returned while decoding the Rust-native payment control-job snapshot.
#[derive(Debug, Error)]
pub enum PaymentControlJobQueueSnapshotError {
    /// JSON decoding failed.
    #[error("decode payment control-job queue snapshot: {0}")]
    Json(#[from] serde_json::Error),
    /// Snapshot uses another codec family.
    #[error("unsupported payment control-job queue snapshot format: {0}")]
    UnsupportedFormat(String),
}

impl InMemoryPaymentControlJobQueue {
    /// Build an empty in-memory payment control-job queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryPaymentControlJobQueueState {
                next_id: 1,
                records: Vec::new(),
            })),
        }
    }

    /// Return a stable ID-ordered snapshot of the queue state.
    #[must_use]
    pub fn snapshot(&self) -> Vec<InMemoryPaymentControlJobRecord> {
        self.lock().records.clone()
    }

    /// Return a versioned Rust-native snapshot for durable queue persistence.
    #[must_use]
    pub fn persistence_snapshot(&self) -> InMemoryPaymentControlJobQueueSnapshot {
        let state = self.lock();
        InMemoryPaymentControlJobQueueSnapshot {
            format: PAYMENT_CONTROL_JOB_QUEUE_SNAPSHOT_FORMAT.to_owned(),
            next_id: state.next_id,
            records: state.records.clone(),
        }
    }

    /// Restore an in-memory queue from a decoded Rust-native persistence snapshot.
    #[must_use]
    pub fn from_persistence_snapshot(snapshot: InMemoryPaymentControlJobQueueSnapshot) -> Self {
        let next_id_from_records = snapshot
            .records
            .iter()
            .map(|record| record.id.saturating_add(1))
            .max()
            .unwrap_or(1);
        Self {
            state: Arc::new(Mutex::new(InMemoryPaymentControlJobQueueState {
                next_id: snapshot.next_id.max(next_id_from_records).max(1),
                records: snapshot.records,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, InMemoryPaymentControlJobQueueState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Encode a payment control-job queue snapshot as Rust-native JSON bytes.
pub fn encode_payment_control_job_queue_snapshot(
    snapshot: &InMemoryPaymentControlJobQueueSnapshot,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(snapshot)
}

/// Decode a payment control-job queue snapshot from Rust-native JSON bytes.
pub fn decode_payment_control_job_queue_snapshot(
    bytes: &[u8],
) -> Result<InMemoryPaymentControlJobQueueSnapshot, PaymentControlJobQueueSnapshotError> {
    let snapshot: InMemoryPaymentControlJobQueueSnapshot = serde_json::from_slice(bytes)?;
    if snapshot.format != PAYMENT_CONTROL_JOB_QUEUE_SNAPSHOT_FORMAT {
        return Err(PaymentControlJobQueueSnapshotError::UnsupportedFormat(
            snapshot.format,
        ));
    }
    Ok(snapshot)
}

impl Default for InMemoryPaymentControlJobQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl PaymentControlJobQueue for InMemoryPaymentControlJobQueue {
    type Error = InMemoryPaymentControlJobQueueError;

    fn assign_payment_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> PaymentControlJobQueueFuture<'a, Self::Error> {
        Box::pin(async move {
            let mut state = self.lock();
            let id = state.next_id;
            state.next_id += 1;
            state.records.push(InMemoryPaymentControlJobRecord {
                id,
                queue_name: queue_name.to_owned(),
                status: InMemoryPaymentControlJobStatus::Pending,
                job,
                completed_report: None,
                error: None,
            });
            Ok(())
        })
    }
}

impl PaymentControlJobWorkerQueue for InMemoryPaymentControlJobQueue {
    type Error = InMemoryPaymentControlJobQueueError;

    fn dequeue_payment_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> PaymentControlJobWorkerFuture<'a, Option<PaymentControlJobWorkItem>, Self::Error> {
        Box::pin(async move {
            let mut state = self.lock();
            let Some(index) = next_in_memory_payment_control_job_index(&state.records, queue_name)
            else {
                return Ok(None);
            };
            let record = &mut state.records[index];
            record.status = InMemoryPaymentControlJobStatus::Processing;
            Ok(Some(PaymentControlJobWorkItem {
                id: record.id,
                job: record.job.clone(),
            }))
        })
    }

    fn complete_payment_control_job<'a>(
        &'a self,
        job_id: i64,
        report: &'a PaymentControlJobReport,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move {
            let mut state = self.lock();
            let record = state
                .records
                .iter_mut()
                .find(|record| record.id == job_id)
                .ok_or(InMemoryPaymentControlJobQueueError::JobNotFound(job_id))?;
            record.status = InMemoryPaymentControlJobStatus::Completed;
            record.completed_report = Some(report.clone());
            record.error = None;
            Ok(())
        })
    }

    fn fail_payment_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move {
            let mut state = self.lock();
            let record = state
                .records
                .iter_mut()
                .find(|record| record.id == job_id)
                .ok_or(InMemoryPaymentControlJobQueueError::JobNotFound(job_id))?;
            record.status = InMemoryPaymentControlJobStatus::Failed;
            record.completed_report = None;
            record.error = Some(error.to_owned());
            Ok(())
        })
    }
}

fn next_in_memory_payment_control_job_index(
    records: &[InMemoryPaymentControlJobRecord],
    queue_name: &str,
) -> Option<usize> {
    records
        .iter()
        .enumerate()
        .filter(|(_index, record)| {
            record.queue_name == queue_name
                && record.status == InMemoryPaymentControlJobStatus::Pending
        })
        .min_by(|(_left_index, left), (_right_index, right)| {
            right
                .job
                .priority
                .cmp(&left.job.priority)
                .then_with(|| left.job.created.cmp(&right.job.created))
                .then_with(|| left.id.cmp(&right.id))
        })
        .map(|(index, _record)| index)
}

impl PreCheckoutPaymentEffects for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn answer_pre_checkout_query<'a>(
        &'a self,
        pre_checkout_query_id: &'a str,
    ) -> PreCheckoutFuture<'a, Self::Error> {
        Box::pin(async move {
            self.execute(openplotva_telegram::build_pre_checkout_ok_method(
                pre_checkout_query_id.to_owned(),
            ))
            .await
            .map(|_: bool| ())
        })
    }
}

impl PaymentInvoiceEffects for openplotva_telegram::TelegramClient {
    type Error = TelegramPaymentInvoiceEffectError;

    fn create_subscription_invoice_link<'a>(
        &'a self,
        request: &'a openplotva_telegram::SubscriptionInvoiceLinkRequest,
    ) -> PaymentInvoiceLinkFuture<'a, Self::Error> {
        Box::pin(async move {
            self.execute(openplotva_telegram::build_subscription_invoice_link_method(
                request,
            ))
            .await
            .map_err(TelegramPaymentInvoiceEffectError::from)
        })
    }

    fn create_donation_invoice_link<'a>(
        &'a self,
        request: &'a openplotva_telegram::DonationInvoiceLinkRequest,
    ) -> PaymentInvoiceLinkFuture<'a, Self::Error> {
        Box::pin(async move {
            self.execute(openplotva_telegram::build_donation_invoice_link_method(
                request,
            ))
            .await
            .map_err(TelegramPaymentInvoiceEffectError::from)
        })
    }

    fn send_invoice_button_message<'a>(
        &'a self,
        message: InvoiceButtonMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            let method = invoice_button_send_message_method(&message)?;
            let _message: carapax::types::Message = self.execute(method).await?;
            Ok(())
        })
    }

    fn send_invoice_error_text<'a>(
        &'a self,
        chat_id: i64,
        reply_to_message_id: i32,
        text: &'a str,
        parse_mode: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            let methods = invoice_error_text_send_message_methods(
                chat_id,
                reply_to_message_id,
                text,
                parse_mode,
            )?;
            for method in methods {
                let _message: carapax::types::Message = self.execute(method).await?;
            }
            Ok(())
        })
    }
}

impl VipStatusEffects for openplotva_telegram::TelegramClient {
    type Error = TelegramPaymentInvoiceEffectError;

    fn send_vip_status_message<'a>(
        &'a self,
        message: VipStatusMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            let method = vip_status_send_message_method(&message)?;
            let _message: carapax::types::Message = self.execute(method).await?;
            Ok(())
        })
    }

    fn send_vip_status_error_text<'a>(
        &'a self,
        chat_id: i64,
        reply_to_message_id: i32,
        text: &'a str,
        parse_mode: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            let methods = invoice_error_text_send_message_methods(
                chat_id,
                reply_to_message_id,
                text,
                parse_mode,
            )?;
            for method in methods {
                let _message: carapax::types::Message = self.execute(method).await?;
            }
            Ok(())
        })
    }
}

/// Generator for virtual-message IDs used by successful-payment text replies.
pub type PaymentVirtualIdFactory = Arc<dyn Fn() -> String + Send + Sync>;

/// Dispatcher-backed side effects for Go successful-payment processing.
#[derive(Clone)]
pub struct SuccessfulPaymentDispatcherEffects<Store, Invalidator = NoopVipCacheInvalidator> {
    store: Store,
    queue: Arc<openplotva_telegram::DispatcherQueue>,
    invalidator: Invalidator,
    next_virtual_id: PaymentVirtualIdFactory,
}

impl<Store, Invalidator> SuccessfulPaymentDispatcherEffects<Store, Invalidator> {
    /// Build successful-payment effects backed by the normal outbound dispatcher.
    pub fn new(
        store: Store,
        queue: Arc<openplotva_telegram::DispatcherQueue>,
        invalidator: Invalidator,
    ) -> Self {
        Self {
            store,
            queue,
            invalidator,
            next_virtual_id: monotonic_payment_virtual_id_factory(),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: PaymentVirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl<Store, Invalidator> SuccessfulPaymentEffects
    for SuccessfulPaymentDispatcherEffects<Store, Invalidator>
where
    Store: crate::virtual_messages::VirtualMessageStore + Send + Sync,
    Invalidator: VipCacheInvalidator + Send + Sync,
{
    type Error = SuccessfulPaymentDispatchEffectError;

    fn send_text<'a>(
        &'a self,
        message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            let chat = openplotva_telegram::ChatRef {
                id: message.chat_id,
                is_forum: false,
            };
            let request = openplotva_telegram::TextMessageRequest {
                chat: Some(chat),
                message_thread_id: 0,
                disable_notification: false,
                allow_sending_without_reply: None,
                text: message.text.to_owned(),
                render_as: message.parse_mode.to_owned(),
                reply_markup: None,
            };
            let reply_to = openplotva_telegram::ReplyMessageRef {
                message_id: i64::from(message.reply_to_message_id),
                chat,
                is_topic_message: false,
                message_thread_id: 0,
            };
            crate::virtual_messages::queue_text_message_parts(
                &self.store,
                &self.queue,
                crate::virtual_messages::QueueTextRequest {
                    message: &request,
                    reply_to: Some(&reply_to),
                    immediate_first: false,
                    bypass_chat_restrictions: false,
                },
                || (self.next_virtual_id)(),
            )
            .await?;
            Ok(())
        })
    }

    fn invalidate_vip_cache<'a>(&'a self, user_id: i64) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            self.invalidator
                .invalidate_vip_cache(user_id)
                .await
                .map_err(|error| SuccessfulPaymentDispatchEffectError::VipCache(error.to_string()))
        })
    }
}

fn monotonic_payment_virtual_id_factory() -> PaymentVirtualIdFactory {
    let next_id = Arc::new(AtomicU64::new(1));
    let process_id = std::process::id();
    let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    Arc::new(move || {
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        format!("payment-vmsg-{process_id:x}-{started_at:x}-{id:x}")
    })
}

/// Build the direct `sendMessage` used for Go invoice button messages.
pub fn invoice_button_send_message_method(
    message: &InvoiceButtonMessage,
) -> Result<openplotva_telegram::SendMessage, openplotva_telegram::OutboundBuildError> {
    openplotva_telegram::validate_text_message_text(&message.text, &message.parse_mode)?;
    let chat = openplotva_telegram::ChatRef {
        id: message.chat_id,
        is_forum: false,
    };
    let button =
        openplotva_telegram::build_inline_keyboard_button_url(&message.button_text, &message.url);
    let reply_markup = openplotva_telegram::ReplyMarkup::from([[button]]);
    let req = openplotva_telegram::TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: message.text.clone(),
        render_as: message.parse_mode.clone(),
        reply_markup: Some(reply_markup),
    };
    openplotva_telegram::build_text_message_method(&req, chat, None, message.text.clone(), true)
}

fn invoice_error_text_send_message_methods(
    chat_id: i64,
    reply_to_message_id: i32,
    text: &str,
    parse_mode: &str,
) -> Result<Vec<openplotva_telegram::SendMessage>, openplotva_telegram::OutboundBuildError> {
    let chat = openplotva_telegram::ChatRef {
        id: chat_id,
        is_forum: false,
    };
    let req = openplotva_telegram::TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: text.to_owned(),
        render_as: parse_mode.to_owned(),
        reply_markup: None,
    };
    let reply_to = openplotva_telegram::ReplyMessageRef {
        message_id: i64::from(reply_to_message_id),
        chat,
        is_topic_message: false,
        message_thread_id: 0,
    };
    openplotva_telegram::build_text_message_methods(&req, Some(&reply_to))
}

/// Build Go `createVIPRedirectMessage` payload.
#[must_use]
pub fn vip_redirect_message(
    bot_username: &str,
    chat_id: i64,
    reply_to_message_id: i32,
    message_thread_id: Option<i32>,
) -> PaymentRedirectMessage {
    let url = if bot_username.is_empty() {
        String::new()
    } else {
        format!("https://t.me/{bot_username}?start=vip")
    };
    PaymentRedirectMessage {
        chat_id,
        reply_to_message_id,
        message_thread_id,
        text: format!(
            "✨ Для оформления VIP подписки на 30 дней, пожалуйста, перейдите в личные сообщения с ботом.\n\nVIP даёт вам:\n{VIP_BENEFITS_TEXT}"
        ),
        button_text: "💎 Оформить VIP подписку".to_owned(),
        url,
    }
}

/// Build Go `createDonationRedirectMessage` payload.
#[must_use]
pub fn donation_redirect_message(
    bot_username: &str,
    chat_id: i64,
    reply_to_message_id: i32,
    message_thread_id: Option<i32>,
    command_arguments: &str,
) -> PaymentRedirectMessage {
    let start_param = donation_redirect_start_param(command_arguments);
    PaymentRedirectMessage {
        chat_id,
        reply_to_message_id,
        message_thread_id,
        text: "💖 Для отправки доната, пожалуйста, перейдите в личные сообщения с ботом.\n\n\
              Ваша поддержка:\n\
              ❤️ Согреет сердце разработчика\n\
              🚀 Поможет развивать бота\n\
              ✨ Добавит мотивацию на создание новых функций"
            .to_owned(),
        button_text: "💝 Сделать донат".to_owned(),
        url: format!("https://t.me/{bot_username}?start={start_param}"),
    }
}

/// Build a Go payment redirect message from a decoded non-private `/vip` or `/donate` update.
#[must_use]
pub fn payment_redirect_message_from_update(
    update: &TelegramUpdate,
    bot_username: &str,
) -> Option<PaymentRedirectMessage> {
    let TelegramUpdateType::Message(message) = &update.update_type else {
        return None;
    };
    payment_redirect_message_from_message(message, bot_username)
}

fn payment_redirect_message_from_message(
    message: &TelegramMessage,
    bot_username: &str,
) -> Option<PaymentRedirectMessage> {
    if matches!(message.chat, TelegramChat::Private(_)) {
        return None;
    }
    let command = payment_command_from_message(message)?;
    let chat_id = message.chat.get_id().into();
    let reply_to_message_id = i32::try_from(message.id).ok()?;
    let message_thread_id = message
        .message_thread_id
        .and_then(|thread_id| i32::try_from(thread_id).ok())
        .filter(|thread_id| *thread_id != 0);
    match command.name {
        "vip" => Some(vip_redirect_message(
            bot_username,
            chat_id,
            reply_to_message_id,
            message_thread_id,
        )),
        "donate" => Some(donation_redirect_message(
            bot_username,
            chat_id,
            reply_to_message_id,
            message_thread_id,
            command.arguments,
        )),
        _ => None,
    }
}

fn donation_redirect_start_param(command_arguments: &str) -> String {
    if command_arguments.is_empty() {
        return "donate".to_owned();
    }
    match command_arguments.parse::<i64>() {
        Ok(amount) if (MIN_DONATION_STARS..=MAX_DONATION_STARS).contains(&amount) => {
            format!("donate_{amount}")
        }
        _ => "donate".to_owned(),
    }
}

/// Build the direct `sendMessage` used by Go payment group redirect messages.
pub fn payment_redirect_send_message_method(
    message: &PaymentRedirectMessage,
) -> Result<openplotva_telegram::SendMessage, openplotva_telegram::OutboundBuildError> {
    openplotva_telegram::validate_text_message_text(&message.text, "")?;
    let chat = openplotva_telegram::ChatRef {
        id: message.chat_id,
        is_forum: message.message_thread_id.is_some(),
    };
    let button =
        openplotva_telegram::build_inline_keyboard_button_url(&message.button_text, &message.url);
    let reply_markup = openplotva_telegram::ReplyMarkup::from([[button]]);
    let req = openplotva_telegram::TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: Some(false),
        text: message.text.clone(),
        render_as: String::new(),
        reply_markup: Some(reply_markup),
    };
    let reply_to = openplotva_telegram::ReplyMessageRef {
        message_id: i64::from(message.reply_to_message_id),
        chat,
        is_topic_message: message.message_thread_id.is_some(),
        message_thread_id: message.message_thread_id.map(i64::from).unwrap_or_default(),
    };
    openplotva_telegram::build_text_message_method(
        &req,
        chat,
        Some(&reply_to),
        message.text.clone(),
        true,
    )
}

/// Build Go `activeVIPStatusText` plus optional cancel keyboard metadata.
#[must_use]
pub fn active_vip_status_message_at(
    summary: &VipSummaryRecord,
    active_subscription: Option<&SubscriptionRecord>,
    chat_id: i64,
    reply_to_message_id: i32,
    message_thread_id: Option<i32>,
    now: OffsetDateTime,
) -> VipStatusMessage {
    let days_left = vip_display_days_left_at(summary.effective_expires_at, now);
    VipStatusMessage {
        chat_id,
        reply_to_message_id,
        message_thread_id,
        text: format!(
            "✅ У вас уже есть активный VIP статус!\n\n\
             Он действует до: <b>{}</b> ({} дней осталось)\n\n\
             Вы получаете:\n{}",
            go_date(summary.effective_expires_at),
            days_left,
            VIP_BENEFITS_TEXT
        ),
        show_cancel_button: active_subscription.is_some_and(|subscription| subscription.id > 0),
    }
}

/// Build Go `externalVIPStatusText` when the cached external VIP status is active.
#[must_use]
pub fn external_vip_status_message_if_active_at(
    vip_cache: Option<&VipCacheRecord>,
    chat_id: i64,
    reply_to_message_id: i32,
    message_thread_id: Option<i32>,
    now: OffsetDateTime,
) -> Option<VipStatusMessage> {
    let vip_cache = vip_cache?;
    if !vip_cache.is_vip || now >= vip_cache.expires_at {
        return None;
    }
    Some(VipStatusMessage {
        chat_id,
        reply_to_message_id,
        message_thread_id,
        text: format!(
            "✅ У вас активен VIP статус.\n\n\
             Доступ подтверждён через внешний источник.\n\n\
             Вы получаете:\n{}",
            VIP_BENEFITS_TEXT
        ),
        show_cancel_button: false,
    })
}

/// Build Go `buildVIPStatusView` from storage, preserving lookup order and active checks.
pub async fn build_vip_status_message_at<Store>(
    store: &Store,
    user_id: i64,
    chat_id: i64,
    reply_to_message_id: i32,
    message_thread_id: Option<i32>,
    now: OffsetDateTime,
) -> Result<Option<VipStatusMessage>, Store::Error>
where
    Store: VipStatusStore + Sync,
{
    if let Some(summary) = store.get_vip_summary_by_user(user_id).await?
        && summary.is_active
        && now < summary.effective_expires_at
    {
        let active_subscription = store.get_active_subscription(user_id).await?;
        return Ok(Some(active_vip_status_message_at(
            &summary,
            active_subscription.as_ref(),
            chat_id,
            reply_to_message_id,
            message_thread_id,
            now,
        )));
    }

    let vip_cache = store.get_vip_cache(user_id).await?;
    Ok(external_vip_status_message_if_active_at(
        vip_cache.as_ref(),
        chat_id,
        reply_to_message_id,
        message_thread_id,
        now,
    ))
}

/// Build the direct HTML `sendMessage` used by Go existing-VIP status replies.
pub fn vip_status_send_message_method(
    message: &VipStatusMessage,
) -> Result<openplotva_telegram::SendMessage, openplotva_telegram::OutboundBuildError> {
    openplotva_telegram::validate_text_message_text(
        &message.text,
        openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
    )?;
    let chat = openplotva_telegram::ChatRef {
        id: message.chat_id,
        is_forum: message.message_thread_id.is_some(),
    };
    let reply_markup = message.show_cancel_button.then(|| {
        let button = openplotva_telegram::build_inline_keyboard_button_data(
            "Отменить подписку",
            "{\"action\":\"cancel_vip\"}",
        );
        openplotva_telegram::ReplyMarkup::from([[button]])
    });
    let req = openplotva_telegram::TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: message.text.clone(),
        render_as: openplotva_telegram::TELEGRAM_PARSE_MODE_HTML.to_owned(),
        reply_markup,
    };
    let reply_to = openplotva_telegram::ReplyMessageRef {
        message_id: i64::from(message.reply_to_message_id),
        chat,
        is_topic_message: message.message_thread_id.is_some(),
        message_thread_id: message.message_thread_id.map(i64::from).unwrap_or_default(),
    };
    openplotva_telegram::build_text_message_method(
        &req,
        chat,
        Some(&reply_to),
        message.text.clone(),
        true,
    )
}

/// Execute Go `executeVIPInvoiceControlJob`.
pub async fn execute_vip_invoice_control_job<Effects>(
    effects: &Effects,
    params: &ControlJobParams,
) -> InvoiceControlJobReport
where
    Effects: PaymentInvoiceEffects + Sync,
{
    if params.user_id <= 0 {
        send_invoice_error_text(effects, params, USER_IDENTIFICATION_ERROR_TEXT).await;
        return InvoiceControlJobReport::new(InvoiceControlJobOutcome::MissingUser);
    }

    let request = openplotva_telegram::SubscriptionInvoiceLinkRequest {
        user_id: params.user_id,
        user_name: params.data.user_name.clone(),
        amount_stars: params.data.amount,
    };
    let invoice_url = match effects.create_subscription_invoice_link(&request).await {
        Ok(invoice_url) => invoice_url,
        Err(error) => {
            send_invoice_error_text(effects, params, SUBSCRIPTION_INVOICE_CREATE_ERROR_TEXT).await;
            return InvoiceControlJobReport::with_error(
                InvoiceControlJobOutcome::InvoiceLinkRequestError,
                error,
            );
        }
    };
    if invoice_url.trim().is_empty() {
        send_invoice_error_text(effects, params, SUBSCRIPTION_INVOICE_CREATE_ERROR_TEXT).await;
        return InvoiceControlJobReport::with_error(
            InvoiceControlJobOutcome::EmptyInvoiceLink,
            "failed to parse vip invoice link",
        );
    }

    let message = InvoiceButtonMessage {
        chat_id: params.chat_id,
        text: subscription_invoice_message_text(),
        button_text: SUBSCRIPTION_INVOICE_BUTTON_TEXT.to_owned(),
        url: invoice_url,
        parse_mode: openplotva_telegram::TELEGRAM_PARSE_MODE_HTML.to_owned(),
    };
    match effects.send_invoice_button_message(message).await {
        Ok(()) => InvoiceControlJobReport::new(InvoiceControlJobOutcome::InvoiceSent),
        Err(error) => {
            InvoiceControlJobReport::with_error(InvoiceControlJobOutcome::SendError, error)
        }
    }
}

/// Execute Go `executeDonateInvoiceControlJob`.
pub async fn execute_donate_invoice_control_job<Effects>(
    effects: &Effects,
    params: &ControlJobParams,
) -> InvoiceControlJobReport
where
    Effects: PaymentInvoiceEffects + Sync,
{
    let request = openplotva_telegram::DonationInvoiceLinkRequest {
        user_id: params.user_id,
        amount_stars: params.data.amount,
    };
    let invoice_url = match effects.create_donation_invoice_link(&request).await {
        Ok(invoice_url) => invoice_url,
        Err(error) => {
            send_invoice_error_text(effects, params, DONATION_INVOICE_CREATE_ERROR_TEXT).await;
            return InvoiceControlJobReport::with_error(
                InvoiceControlJobOutcome::InvoiceLinkRequestError,
                error,
            );
        }
    };
    if invoice_url.trim().is_empty() {
        send_invoice_error_text(effects, params, DONATION_INVOICE_CREATE_ERROR_TEXT).await;
        return InvoiceControlJobReport::with_error(
            InvoiceControlJobOutcome::EmptyInvoiceLink,
            "failed to parse donation invoice link",
        );
    }

    let message = InvoiceButtonMessage {
        chat_id: params.chat_id,
        text: donation_invoice_message_text(),
        button_text: donation_invoice_button_text(params.data.amount),
        url: invoice_url,
        parse_mode: openplotva_telegram::TELEGRAM_PARSE_MODE_HTML.to_owned(),
    };
    match effects.send_invoice_button_message(message).await {
        Ok(()) => InvoiceControlJobReport::new(InvoiceControlJobOutcome::InvoiceSent),
        Err(error) => {
            InvoiceControlJobReport::with_error(InvoiceControlJobOutcome::SendError, error)
        }
    }
}

async fn send_invoice_error_text<Effects>(effects: &Effects, params: &ControlJobParams, text: &str)
where
    Effects: PaymentInvoiceEffects + Sync,
{
    let _ = effects
        .send_invoice_error_text(
            params.chat_id,
            params.message_id,
            text,
            openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
        )
        .await;
}

/// Concrete storage bundle for successful-payment processing.
#[derive(Clone, Debug)]
pub struct PostgresSuccessfulPaymentStore {
    users: PostgresVirtualMessageStore,
    payments: PostgresPaymentStore,
    vip: PostgresVipStore,
}

impl PostgresSuccessfulPaymentStore {
    /// Build a payment-processing store from concrete Postgres stores.
    pub fn new(
        users: PostgresVirtualMessageStore,
        payments: PostgresPaymentStore,
        vip: PostgresVipStore,
    ) -> Self {
        Self {
            users,
            payments,
            vip,
        }
    }
}

impl SuccessfulPaymentStore for PostgresSuccessfulPaymentStore {
    type Error = StorageError;

    fn upsert_payment_user<'a>(
        &'a self,
        user: &'a UserState,
    ) -> PaymentStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.users.upsert_user_state(user).await })
    }

    fn create_subscription<'a>(
        &'a self,
        subscription: SubscriptionCreate<'a>,
    ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error> {
        Box::pin(async move { self.payments.create_subscription(subscription).await })
    }

    fn get_subscription_by_telegram_payment_charge_id<'a>(
        &'a self,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
        Box::pin(async move {
            self.payments
                .get_subscription_by_telegram_payment_charge_id(telegram_payment_charge_id)
                .await
        })
    }

    fn create_donation<'a>(
        &'a self,
        donation: DonationCreate<'a>,
    ) -> PaymentStoreFuture<'a, DonationRecord, Self::Error> {
        Box::pin(async move { self.payments.create_donation(donation).await })
    }

    fn get_donation_by_telegram_payment_charge_id<'a>(
        &'a self,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<DonationRecord>, Self::Error> {
        Box::pin(async move {
            self.payments
                .get_donation_by_telegram_payment_charge_id(telegram_payment_charge_id)
                .await
        })
    }

    fn create_vip_event<'a>(
        &'a self,
        event: VipEventCreate<'a>,
    ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error> {
        Box::pin(async move { self.vip.create_vip_event(event).await })
    }

    fn is_duplicate_insert_no_row(error: &Self::Error) -> bool {
        error.is_row_not_found()
    }
}

impl VipStatusStore for PostgresSuccessfulPaymentStore {
    type Error = StorageError;

    fn get_vip_summary_by_user<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error> {
        Box::pin(async move { self.vip.get_vip_summary_by_user(user_id).await })
    }

    fn get_active_subscription<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
        Box::pin(async move { self.payments.get_active_subscription(user_id).await })
    }

    fn get_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipCacheRecord>, Self::Error> {
        Box::pin(async move { self.vip.get_vip_cache(user_id).await })
    }
}

impl VipStatusChecker for PostgresSuccessfulPaymentStore {
    fn is_vip_at<'a>(&'a self, user_id: i64, now: OffsetDateTime) -> VipStatusCheckFuture<'a> {
        Box::pin(async move {
            if user_id <= 0 {
                return false;
            }
            if let Ok(Some(summary)) = self.vip.get_vip_summary_by_user(user_id).await
                && summary.is_active
                && now < summary.effective_expires_at
            {
                return true;
            }
            match self.vip.get_vip_cache(user_id).await {
                Ok(Some(cache)) => cache.is_vip && now < cache.expires_at,
                _ => false,
            }
        })
    }
}

/// Process one Telegram successful-payment message with Go `processSuccessfulPaymentNow` semantics.
pub async fn process_successful_payment_at<Store, Effects>(
    store: &Store,
    effects: &Effects,
    message: &SuccessfulPaymentMessage,
    now: OffsetDateTime,
) -> SuccessfulPaymentReport
where
    Store: SuccessfulPaymentStore + Sync,
    Effects: SuccessfulPaymentEffects + Sync,
{
    if message.payment.currency != openplotva_telegram::TELEGRAM_STARS_CURRENCY {
        return SuccessfulPaymentReport::new(SuccessfulPaymentOutcome::UnsupportedCurrency);
    }

    match openplotva_telegram::classify_payment_payload(&message.payment.invoice_payload) {
        openplotva_telegram::PaymentPayloadKind::Subscription => {
            process_subscription_payment(store, effects, message, now).await
        }
        openplotva_telegram::PaymentPayloadKind::Donation => {
            process_donation_payment(store, effects, message).await
        }
        openplotva_telegram::PaymentPayloadKind::Unknown => {
            SuccessfulPaymentReport::new(SuccessfulPaymentOutcome::UnknownPayload)
        }
    }
}

/// Build the Go taskman control job produced by `processSuccessfulPayment`.
#[must_use]
pub fn successful_payment_control_job_from_update_at(
    update: &TelegramUpdate,
    created: OffsetDateTime,
) -> Option<StatelessJobItem> {
    let TelegramUpdateType::Message(message) = &update.update_type else {
        return None;
    };
    successful_payment_control_job_from_message_at(message, created)
}

fn successful_payment_control_job_from_message_at(
    message: &TelegramMessage,
    created: OffsetDateTime,
) -> Option<StatelessJobItem> {
    let TelegramMessageData::SuccessfulPayment(payment) = &message.data else {
        return None;
    };

    let mut data = ControlJobData {
        kind: ControlKind::SuccessfulPayment,
        chat_type: chat_type_name(&message.chat).to_owned(),
        payment: Some(ControlPayment {
            currency: payment.currency.clone(),
            total_amount: i32::try_from(payment.total_amount).ok()?,
            invoice_payload: payment.invoice_payload.clone(),
            telegram_payment_charge_id: payment.telegram_payment_charge_id.clone(),
            provider_payment_charge_id: payment.provider_payment_charge_id.clone(),
        }),
        ..ControlJobData::default()
    };
    let user = message.sender.get_user();
    if let Some(user) = user {
        data.user_name = user
            .username
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        data.first_name = user.first_name.clone();
        data.last_name = user.last_name.clone().unwrap_or_default();
        data.language_code = user.language_code.clone().unwrap_or_default();
        data.is_premium = user.is_premium.unwrap_or_default();
    }

    let params = ControlJobParams {
        chat_id: message.chat.get_id().into(),
        message_id: i32::try_from(message.id).ok()?,
        user_id: user.map(|user| user.id.into()).unwrap_or_default(),
        user_full_name: user
            .map(user_full_name)
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_default(),
        thread_id: message
            .message_thread_id
            .and_then(|thread_id| i32::try_from(thread_id).ok())
            .filter(|thread_id| *thread_id != 0),
        data,
    };
    Some(
        new_control_job_at(params, created)
            .with_name("successful payment")
            .with_priority(HIGH_PRIORITY),
    )
}

/// Queue successful-payment updates as Go taskman control jobs, delegating all other updates.
pub async fn enqueue_successful_payment_update_or_else_at<
    Queue,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    queue: &Queue,
    update: TelegramUpdate,
    created: OffsetDateTime,
    handle_other: HandleFn,
) -> Result<SuccessfulPaymentControlJobUpdateRoute, HandleError>
where
    Queue: PaymentControlJobQueue + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let Some(job) = successful_payment_control_job_from_update_at(&update, created) else {
        handle_other(update).await?;
        return Ok(SuccessfulPaymentControlJobUpdateRoute::Delegated);
    };

    match queue
        .assign_payment_control_job(CONTROL_QUEUE_NAME, job)
        .await
    {
        Ok(()) => Ok(SuccessfulPaymentControlJobUpdateRoute::Queued),
        Err(error) => {
            tracing::warn!(%error, "failed to assign successful-payment control job");
            Ok(SuccessfulPaymentControlJobUpdateRoute::QueueError)
        }
    }
}

/// Go default VIP subscription price in Telegram Stars.
pub const SUBSCRIPTION_PRICE_STARS: i64 = 300;
/// Go default donation amount in Telegram Stars.
pub const DEFAULT_DONATION_STARS: i64 = 600;
/// Minimum explicit donation amount accepted by Go.
pub const MIN_DONATION_STARS: i64 = 10;
/// Maximum explicit donation amount accepted by Go.
pub const MAX_DONATION_STARS: i64 = 10_000;

/// Build the Go taskman control job produced by private `/vip` and `/donate` commands.
#[must_use]
pub fn payment_invoice_control_job_from_update_at(
    update: &TelegramUpdate,
    created: OffsetDateTime,
) -> PaymentInvoiceControlJobBuild {
    let TelegramUpdateType::Message(message) = &update.update_type else {
        return PaymentInvoiceControlJobBuild::NotPaymentCommand;
    };
    payment_invoice_control_job_from_message_at(message, created)
}

fn payment_invoice_control_job_from_message_at(
    message: &TelegramMessage,
    created: OffsetDateTime,
) -> PaymentInvoiceControlJobBuild {
    let Some(command) = payment_command_from_message(message) else {
        return PaymentInvoiceControlJobBuild::NotPaymentCommand;
    };
    if !matches!(message.chat, TelegramChat::Private(_)) {
        return PaymentInvoiceControlJobBuild::NonPrivateChat;
    }

    match command.name {
        "vip" => vip_invoice_control_job_from_message_at(message, created),
        "donate" => {
            donation_invoice_control_job_from_message_at(message, command.arguments, created)
        }
        _ => PaymentInvoiceControlJobBuild::NotPaymentCommand,
    }
}

fn vip_invoice_control_job_from_message_at(
    message: &TelegramMessage,
    created: OffsetDateTime,
) -> PaymentInvoiceControlJobBuild {
    let Some(user) = message.sender.get_user() else {
        return PaymentInvoiceControlJobBuild::MissingUser;
    };
    let user_id: i64 = user.id.into();
    if user_id <= 0 || user.is_bot {
        return PaymentInvoiceControlJobBuild::MissingUser;
    }

    let data = payment_control_data_from_message(
        message,
        ControlKind::VipInvoice,
        subscription_price_stars_for_user(user),
    );
    control_job_from_message_at(message, data, "vip invoice", created)
        .map(Box::new)
        .map(PaymentInvoiceControlJobBuild::Job)
        .unwrap_or(PaymentInvoiceControlJobBuild::MissingUser)
}

fn donation_invoice_control_job_from_message_at(
    message: &TelegramMessage,
    command_arguments: &str,
    created: OffsetDateTime,
) -> PaymentInvoiceControlJobBuild {
    let Ok(amount) = donation_amount_stars_from_command_arguments(command_arguments) else {
        return PaymentInvoiceControlJobBuild::InvalidDonationAmount;
    };

    let data = payment_control_data_from_message(message, ControlKind::DonateInvoice, amount);
    control_job_from_message_at(message, data, "donate invoice", created)
        .map(Box::new)
        .map(PaymentInvoiceControlJobBuild::Job)
        .unwrap_or(PaymentInvoiceControlJobBuild::NotPaymentCommand)
}

/// Queue private `/vip` and `/donate` updates as Go taskman control jobs, delegating other updates.
pub async fn enqueue_payment_invoice_command_update_or_else_at<
    Queue,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    queue: &Queue,
    update: TelegramUpdate,
    created: OffsetDateTime,
    handle_other: HandleFn,
) -> Result<PaymentInvoiceCommandUpdateRoute, HandleError>
where
    Queue: PaymentControlJobQueue + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    match payment_invoice_control_job_from_update_at(&update, created) {
        PaymentInvoiceControlJobBuild::Job(job) => match queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, *job)
            .await
        {
            Ok(()) => Ok(PaymentInvoiceCommandUpdateRoute::Queued),
            Err(error) => {
                tracing::warn!(%error, "failed to assign payment invoice control job");
                Ok(PaymentInvoiceCommandUpdateRoute::QueueError)
            }
        },
        PaymentInvoiceControlJobBuild::NotPaymentCommand => {
            handle_other(update).await?;
            Ok(PaymentInvoiceCommandUpdateRoute::Delegated)
        }
        PaymentInvoiceControlJobBuild::NonPrivateChat => {
            Ok(PaymentInvoiceCommandUpdateRoute::NonPrivateChat)
        }
        PaymentInvoiceControlJobBuild::MissingUser => {
            Ok(PaymentInvoiceCommandUpdateRoute::MissingUser)
        }
        PaymentInvoiceControlJobBuild::InvalidDonationAmount => {
            Ok(PaymentInvoiceCommandUpdateRoute::InvalidDonationAmount)
        }
    }
}

/// Queue private `/vip` and `/donate` updates, but send Go existing-VIP status before `/vip` invoices.
pub async fn enqueue_payment_invoice_command_update_with_vip_status_or_else_at<
    Queue,
    Vip,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    queue: &Queue,
    vip: &Vip,
    effects: &Effects,
    update: TelegramUpdate,
    created: OffsetDateTime,
    now: OffsetDateTime,
    handle_other: HandleFn,
) -> Result<PaymentInvoiceCommandUpdateRoute, HandleError>
where
    Queue: PaymentControlJobQueue + Sync,
    Vip: VipStatusChecker + VipStatusStore + Sync,
    Effects: VipStatusEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update).await?;
        return Ok(PaymentInvoiceCommandUpdateRoute::Delegated);
    };
    let Some(command) = payment_command_from_message(message) else {
        handle_other(update).await?;
        return Ok(PaymentInvoiceCommandUpdateRoute::Delegated);
    };
    if !matches!(message.chat, TelegramChat::Private(_)) {
        return Ok(PaymentInvoiceCommandUpdateRoute::NonPrivateChat);
    }

    if command.name == "vip" {
        let Some(user) = message.sender.get_user() else {
            return Ok(PaymentInvoiceCommandUpdateRoute::MissingUser);
        };
        let user_id: i64 = user.id.into();
        if user_id <= 0 || user.is_bot {
            return Ok(PaymentInvoiceCommandUpdateRoute::MissingUser);
        }
        if vip.is_vip_at(user_id, now).await {
            let chat_id = message.chat.get_id().into();
            let reply_to_message_id = match i32::try_from(message.id) {
                Ok(message_id) => message_id,
                Err(_) => return Ok(PaymentInvoiceCommandUpdateRoute::MissingUser),
            };
            let message_thread_id = message
                .message_thread_id
                .and_then(|thread_id| i32::try_from(thread_id).ok())
                .filter(|thread_id| *thread_id != 0);
            return Ok(send_existing_vip_status_or_error(
                vip,
                effects,
                user_id,
                chat_id,
                reply_to_message_id,
                message_thread_id,
                now,
            )
            .await);
        }
    }

    match payment_invoice_control_job_from_message_at(message, created) {
        PaymentInvoiceControlJobBuild::Job(job) => match queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, *job)
            .await
        {
            Ok(()) => Ok(PaymentInvoiceCommandUpdateRoute::Queued),
            Err(error) => {
                tracing::warn!(%error, "failed to assign payment invoice control job");
                Ok(PaymentInvoiceCommandUpdateRoute::QueueError)
            }
        },
        PaymentInvoiceControlJobBuild::MissingUser => {
            Ok(PaymentInvoiceCommandUpdateRoute::MissingUser)
        }
        PaymentInvoiceControlJobBuild::InvalidDonationAmount => {
            Ok(PaymentInvoiceCommandUpdateRoute::InvalidDonationAmount)
        }
        PaymentInvoiceControlJobBuild::NonPrivateChat => {
            Ok(PaymentInvoiceCommandUpdateRoute::NonPrivateChat)
        }
        PaymentInvoiceControlJobBuild::NotPaymentCommand => {
            handle_other(update).await?;
            Ok(PaymentInvoiceCommandUpdateRoute::Delegated)
        }
    }
}

async fn send_existing_vip_status_or_error<Store, Effects>(
    store: &Store,
    effects: &Effects,
    user_id: i64,
    chat_id: i64,
    reply_to_message_id: i32,
    message_thread_id: Option<i32>,
    now: OffsetDateTime,
) -> PaymentInvoiceCommandUpdateRoute
where
    Store: VipStatusStore + Sync,
    Effects: VipStatusEffects + Sync,
{
    let status = match build_vip_status_message_at(
        store,
        user_id,
        chat_id,
        reply_to_message_id,
        message_thread_id,
        now,
    )
    .await
    {
        Ok(Some(message)) => message,
        Ok(None) => {
            send_vip_status_error_text(
                effects,
                chat_id,
                reply_to_message_id,
                VIP_STATUS_DETAILS_ERROR_TEXT,
            )
            .await;
            return PaymentInvoiceCommandUpdateRoute::ExistingVipStatusUnavailable;
        }
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to build VIP status view");
            send_vip_status_error_text(
                effects,
                chat_id,
                reply_to_message_id,
                VIP_SUBSCRIPTION_DETAILS_ERROR_TEXT,
            )
            .await;
            return PaymentInvoiceCommandUpdateRoute::ExistingVipStatusLookupError;
        }
    };

    match effects.send_vip_status_message(status).await {
        Ok(()) => PaymentInvoiceCommandUpdateRoute::ExistingVipStatusSent,
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to send VIP status message");
            PaymentInvoiceCommandUpdateRoute::ExistingVipStatusSendError
        }
    }
}

async fn send_vip_status_error_text<Effects>(
    effects: &Effects,
    chat_id: i64,
    reply_to_message_id: i32,
    text: &str,
) where
    Effects: VipStatusEffects + Sync,
{
    let _ = effects
        .send_vip_status_error_text(
            chat_id,
            reply_to_message_id,
            text,
            openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
        )
        .await;
}

/// `UpdateHandler` adapter that handles Go payment-owned updates before fetcher routing.
#[derive(Clone, Debug)]
pub struct PaymentUpdateHandler<Queue, Vip, Effects, Next> {
    queue: Arc<Queue>,
    vip: Arc<Vip>,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Queue, Vip, Effects, Next> PaymentUpdateHandler<Queue, Vip, Effects, Next> {
    /// Build a payment-aware handler around the real downstream update handler.
    pub fn new(queue: Arc<Queue>, vip: Arc<Vip>, effects: Arc<Effects>, next: Arc<Next>) -> Self {
        Self {
            queue,
            vip,
            effects,
            next,
        }
    }
}

impl<Queue, Vip, Effects, Next> UpdateHandler for PaymentUpdateHandler<Queue, Vip, Effects, Next>
where
    Queue: PaymentControlJobQueue + Send + Sync,
    Vip: VipStatusChecker + VipStatusStore + Send + Sync,
    Effects: PreCheckoutPaymentEffects + VipStatusEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            let now = OffsetDateTime::now_utc();
            handle_payment_update_or_else_at(
                self.queue.as_ref(),
                self.vip.as_ref(),
                self.effects.as_ref(),
                update,
                now,
                now,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

/// Handle the payment-owned subset of decoded updates, delegating everything else.
pub async fn handle_payment_update_or_else_at<
    Queue,
    Vip,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    queue: &Queue,
    vip: &Vip,
    effects: &Effects,
    update: TelegramUpdate,
    created: OffsetDateTime,
    now: OffsetDateTime,
    handle_other: HandleFn,
) -> Result<PaymentUpdateRoute, HandleError>
where
    Queue: PaymentControlJobQueue + Sync,
    Vip: VipStatusChecker + VipStatusStore + Sync,
    Effects: PreCheckoutPaymentEffects + VipStatusEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    if let Some(pre_checkout_query_id) = pre_checkout_query_id_from_update(&update) {
        let outcome = match effects
            .answer_pre_checkout_query(pre_checkout_query_id)
            .await
        {
            Ok(()) => PreCheckoutOutcome::Acknowledged,
            Err(error) => {
                tracing::warn!(
                    %error,
                    pre_checkout_query_id,
                    "failed to answer pre-checkout query"
                );
                PreCheckoutOutcome::AcknowledgementError
            }
        };
        return Ok(PaymentUpdateRoute::PreCheckout(outcome));
    }

    if let Some(job) = successful_payment_control_job_from_update_at(&update, created) {
        let route = match queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, job)
            .await
        {
            Ok(()) => SuccessfulPaymentControlJobUpdateRoute::Queued,
            Err(error) => {
                tracing::warn!(%error, "failed to assign successful-payment control job");
                SuccessfulPaymentControlJobUpdateRoute::QueueError
            }
        };
        return Ok(PaymentUpdateRoute::SuccessfulPayment(route));
    }

    let route = enqueue_payment_invoice_command_update_with_vip_status_or_else_at(
        queue,
        vip,
        effects,
        update,
        created,
        now,
        handle_other,
    )
    .await?;
    if route == PaymentInvoiceCommandUpdateRoute::Delegated {
        Ok(PaymentUpdateRoute::Delegated)
    } else {
        Ok(PaymentUpdateRoute::InvoiceCommand(route))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PaymentCommand<'a> {
    name: &'a str,
    arguments: &'a str,
}

fn payment_command_from_message(message: &TelegramMessage) -> Option<PaymentCommand<'_>> {
    let TelegramMessageData::Text(text) = &message.data else {
        return None;
    };
    let mut entities = text.entities.as_ref()?.into_iter();
    let TelegramTextEntity::BotCommand(position) = entities.next()? else {
        return None;
    };
    if position.offset != 0 {
        return None;
    }

    let command_end = utf16_index_to_byte_index(&text.data, position.length)?;
    let command_with_slash = text.data.get(..command_end)?;
    let command_with_at = command_with_slash.strip_prefix('/')?;
    let name = command_with_at
        .split_once('@')
        .map_or(command_with_at, |(name, _)| name);
    let arguments = command_arguments_after_command(&text.data, command_end)?;
    Some(PaymentCommand { name, arguments })
}

fn command_arguments_after_command(text: &str, command_end: usize) -> Option<&str> {
    let after_command = text.get(command_end..)?;
    let Some(separator) = after_command.chars().next() else {
        return Some("");
    };
    after_command.get(separator.len_utf8()..)
}

fn utf16_index_to_byte_index(text: &str, utf16_units: u32) -> Option<usize> {
    let mut consumed = 0_u32;
    for (byte_index, ch) in text.char_indices() {
        if consumed == utf16_units {
            return Some(byte_index);
        }
        consumed = consumed.checked_add(u32::try_from(ch.len_utf16()).ok()?)?;
        if consumed > utf16_units {
            return None;
        }
    }
    if consumed == utf16_units {
        Some(text.len())
    } else {
        None
    }
}

fn subscription_price_stars_for_user(user: &TelegramUser) -> i64 {
    if user
        .username
        .as_ref()
        .is_some_and(|username| username == "WaveCut")
    {
        1
    } else {
        SUBSCRIPTION_PRICE_STARS
    }
}

fn donation_amount_stars_from_command_arguments(arguments: &str) -> Result<i64, ()> {
    if arguments.is_empty() {
        return Ok(DEFAULT_DONATION_STARS);
    }
    let amount = arguments.parse::<i64>().map_err(|_| ())?;
    if (MIN_DONATION_STARS..=MAX_DONATION_STARS).contains(&amount) {
        Ok(amount)
    } else {
        Err(())
    }
}

fn payment_control_data_from_message(
    message: &TelegramMessage,
    kind: ControlKind,
    amount: i64,
) -> ControlJobData {
    let mut data = ControlJobData {
        kind,
        amount,
        chat_type: chat_type_name(&message.chat).to_owned(),
        ..ControlJobData::default()
    };
    if let Some(user) = message.sender.get_user() {
        data.user_name = user
            .username
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        data.first_name = user.first_name.clone();
        data.last_name = user.last_name.clone().unwrap_or_default();
        data.language_code = user.language_code.clone().unwrap_or_default();
        data.is_premium = user.is_premium.unwrap_or_default();
    }
    data
}

fn control_job_from_message_at(
    message: &TelegramMessage,
    data: ControlJobData,
    title: &str,
    created: OffsetDateTime,
) -> Option<StatelessJobItem> {
    let user = message.sender.get_user();
    let params = ControlJobParams {
        chat_id: message.chat.get_id().into(),
        message_id: i32::try_from(message.id).ok()?,
        user_id: user.map(|user| user.id.into()).unwrap_or_default(),
        user_full_name: user
            .map(user_full_name)
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| "Telegram".to_owned()),
        thread_id: message
            .message_thread_id
            .and_then(|thread_id| i32::try_from(thread_id).ok())
            .filter(|thread_id| *thread_id != 0),
        data,
    };
    Some(
        new_control_job_at(params, created)
            .with_name(title)
            .with_priority(HIGH_PRIORITY),
    )
}

fn chat_type_name(chat: &TelegramChat) -> &'static str {
    match chat {
        TelegramChat::Channel(_) => "channel",
        TelegramChat::Group(_) => "group",
        TelegramChat::Private(_) => "private",
        TelegramChat::Supergroup(_) => "supergroup",
    }
}

fn user_full_name(user: &TelegramUser) -> String {
    format!(
        "{} {}",
        user.first_name,
        user.last_name.as_deref().unwrap_or_default()
    )
    .trim()
    .to_owned()
}

/// Execute the payment-owned subset of Go taskman control jobs.
pub async fn execute_payment_control_job_at<Store, Effects>(
    store: &Store,
    effects: &Effects,
    params: &ControlJobParams,
    now: OffsetDateTime,
) -> PaymentControlJobReport
where
    Store: SuccessfulPaymentStore + Sync,
    Effects: PaymentInvoiceEffects + SuccessfulPaymentEffects + Sync,
{
    match params.data.kind {
        ControlKind::VipInvoice => {
            let report = execute_vip_invoice_control_job(effects, params).await;
            PaymentControlJobReport::from_invoice(PaymentControlJobOutcome::VipInvoice, report)
        }
        ControlKind::DonateInvoice => {
            let report = execute_donate_invoice_control_job(effects, params).await;
            PaymentControlJobReport::from_invoice(PaymentControlJobOutcome::DonateInvoice, report)
        }
        ControlKind::SuccessfulPayment => {
            if params.data.payment.is_none() {
                return PaymentControlJobReport::with_error(
                    PaymentControlJobOutcome::MissingSuccessfulPayment,
                    "successful payment control job payment is required",
                );
            }
            if params.user_id <= 0 {
                return PaymentControlJobReport::with_error(
                    PaymentControlJobOutcome::MissingSuccessfulPaymentUser,
                    "successful payment control job user is required",
                );
            }
            let Some(message) = successful_payment_message_from_control_job(params) else {
                return PaymentControlJobReport::with_error(
                    PaymentControlJobOutcome::MissingSuccessfulPayment,
                    "successful payment control job could not be reconstructed",
                );
            };
            let report = process_successful_payment_at(store, effects, &message, now).await;
            PaymentControlJobReport::from_successful_payment(report)
        }
        _ => PaymentControlJobReport::new(PaymentControlJobOutcome::NotPaymentControlJob),
    }
}

/// Go taskman control queue poll interval used by the payment-owned worker loop.
pub const PAYMENT_CONTROL_JOB_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Process one payment-owned taskman control job, if available.
pub async fn process_payment_control_job_once_at<Queue, Store, Effects>(
    queue: &Queue,
    store: &Store,
    effects: &Effects,
    now: OffsetDateTime,
) -> PaymentControlJobWorkerReport
where
    Queue: PaymentControlJobWorkerQueue + Sync,
    Store: SuccessfulPaymentStore + Sync,
    Effects: PaymentInvoiceEffects + SuccessfulPaymentEffects + Sync,
{
    let mut report = PaymentControlJobWorkerReport::default();
    let item = match queue.dequeue_payment_control_job(CONTROL_QUEUE_NAME).await {
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
            mark_payment_control_job_failed(queue, item.id, &error, &mut report).await;
            return report;
        }
    };

    let execution = execute_payment_control_job_at(store, effects, &params, now).await;
    if let Some(error) = payment_control_job_failure_message(&execution) {
        mark_payment_control_job_failed(queue, item.id, &error, &mut report).await;
    } else {
        match queue
            .complete_payment_control_job(item.id, &execution)
            .await
        {
            Ok(()) => report.completed = true,
            Err(error) => report.status_error = Some(error.to_string()),
        }
    }
    report.execution = Some(execution);
    report
}

/// Run the payment-owned taskman control-job worker until the stop future resolves.
pub async fn run_payment_control_job_worker_until<Queue, Store, Effects, Stop>(
    queue: &Queue,
    store: &Store,
    effects: &Effects,
    stop: Stop,
) -> PaymentControlJobWorkerRunReport
where
    Queue: PaymentControlJobWorkerQueue + Sync,
    Store: SuccessfulPaymentStore + Sync,
    Effects: PaymentInvoiceEffects + SuccessfulPaymentEffects + Sync,
    Stop: Future<Output = ()>,
{
    run_payment_control_job_worker_every_until(
        queue,
        store,
        effects,
        PAYMENT_CONTROL_JOB_POLL_INTERVAL,
        stop,
    )
    .await
}

/// Run the payment-owned taskman control-job worker with an injected interval.
pub async fn run_payment_control_job_worker_every_until<Queue, Store, Effects, Stop>(
    queue: &Queue,
    store: &Store,
    effects: &Effects,
    interval: Duration,
    stop: Stop,
) -> PaymentControlJobWorkerRunReport
where
    Queue: PaymentControlJobWorkerQueue + Sync,
    Store: SuccessfulPaymentStore + Sync,
    Effects: PaymentInvoiceEffects + SuccessfulPaymentEffects + Sync,
    Stop: Future<Output = ()>,
{
    let mut report = PaymentControlJobWorkerRunReport::default();
    let mut stop = std::pin::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                let tick = process_payment_control_job_once_at(queue, store, effects, OffsetDateTime::now_utc()).await;
                trace_payment_control_job_tick(&tick);
                report.record_tick(&tick);
            }
        }
    }

    report
}

async fn mark_payment_control_job_failed<Queue>(
    queue: &Queue,
    job_id: i64,
    error: &str,
    report: &mut PaymentControlJobWorkerReport,
) where
    Queue: PaymentControlJobWorkerQueue + Sync,
{
    match queue.fail_payment_control_job(job_id, error).await {
        Ok(()) => report.failed = true,
        Err(error) => report.status_error = Some(error.to_string()),
    }
}

fn payment_control_job_failure_message(report: &PaymentControlJobReport) -> Option<String> {
    match &report.outcome {
        PaymentControlJobOutcome::VipInvoice(
            InvoiceControlJobOutcome::InvoiceLinkRequestError
            | InvoiceControlJobOutcome::EmptyInvoiceLink
            | InvoiceControlJobOutcome::SendError,
        )
        | PaymentControlJobOutcome::DonateInvoice(
            InvoiceControlJobOutcome::InvoiceLinkRequestError
            | InvoiceControlJobOutcome::EmptyInvoiceLink
            | InvoiceControlJobOutcome::SendError,
        ) => Some(
            report
                .error
                .clone()
                .unwrap_or_else(|| "payment invoice control job failed".to_owned()),
        ),
        PaymentControlJobOutcome::MissingSuccessfulPayment => Some(
            report
                .error
                .clone()
                .unwrap_or_else(|| "successful payment control job payment is required".to_owned()),
        ),
        PaymentControlJobOutcome::MissingSuccessfulPaymentUser => Some(
            report
                .error
                .clone()
                .unwrap_or_else(|| "successful payment control job user is required".to_owned()),
        ),
        PaymentControlJobOutcome::NotPaymentControlJob => {
            Some("unsupported payment control job kind".to_owned())
        }
        _ => None,
    }
}

fn trace_payment_control_job_tick(tick: &PaymentControlJobWorkerReport) {
    if !tick.dequeued && tick.dequeue_error.is_none() {
        return;
    }

    tracing::debug!(
        dequeued = tick.dequeued,
        job_id = tick.job_id,
        completed = tick.completed,
        failed = tick.failed,
        outcome = ?tick.execution.as_ref().map(|report| &report.outcome),
        dequeue_error = tick.dequeue_error.as_deref(),
        decode_error = tick.decode_error.as_deref(),
        status_error = tick.status_error.as_deref(),
        "processed payment control job"
    );
}

#[must_use]
pub fn successful_payment_message_from_control_job(
    params: &ControlJobParams,
) -> Option<SuccessfulPaymentMessage> {
    if params.user_id <= 0 {
        return None;
    }
    let payment = params.data.payment.as_ref()?;
    Some(SuccessfulPaymentMessage {
        chat_id: params.chat_id,
        message_id: params.message_id,
        user: user_state_from_control_job(params),
        payment: SuccessfulPayment {
            currency: payment.currency.clone(),
            total_amount: i64::from(payment.total_amount),
            invoice_payload: payment.invoice_payload.clone(),
            telegram_payment_charge_id: payment.telegram_payment_charge_id.clone(),
            provider_payment_charge_id: payment.provider_payment_charge_id.clone(),
        },
    })
}

fn user_state_from_control_job(params: &ControlJobParams) -> UserState {
    let first_name = if params.data.first_name.trim().is_empty() {
        params.user_full_name.clone()
    } else {
        params.data.first_name.clone()
    };
    UserState::new(
        params.user_id,
        first_name,
        Some(params.data.last_name.clone()),
        Some(params.data.user_name.clone()),
        Some(params.data.language_code.clone()),
        Some(params.data.is_premium),
    )
}

/// Extract Go successful-payment message context from a decoded Telegram update.
#[must_use]
pub fn successful_payment_message_from_update(
    update: &TelegramUpdate,
) -> Option<SuccessfulPaymentMessage> {
    let TelegramUpdateType::Message(message) = &update.update_type else {
        return None;
    };
    successful_payment_message_from_message(message)
}

fn successful_payment_message_from_message(
    message: &TelegramMessage,
) -> Option<SuccessfulPaymentMessage> {
    let TelegramMessageData::SuccessfulPayment(payment) = &message.data else {
        return None;
    };
    let user = message.sender.get_user()?;
    Some(SuccessfulPaymentMessage {
        chat_id: message.chat.get_id().into(),
        message_id: i32::try_from(message.id).ok()?,
        user: user_state_from_telegram_user(user),
        payment: successful_payment_from_telegram(payment),
    })
}

fn user_state_from_telegram_user(user: &TelegramUser) -> UserState {
    UserState::new(
        user.id.into(),
        user.first_name.clone(),
        user.last_name.clone(),
        user.username.as_ref().map(ToString::to_string),
        user.language_code.clone(),
        user.is_premium,
    )
}

fn successful_payment_from_telegram(payment: &TelegramSuccessfulPayment) -> SuccessfulPayment {
    SuccessfulPayment {
        currency: payment.currency.clone(),
        total_amount: payment.total_amount,
        invoice_payload: payment.invoice_payload.clone(),
        telegram_payment_charge_id: payment.telegram_payment_charge_id.clone(),
        provider_payment_charge_id: payment.provider_payment_charge_id.clone(),
    }
}

/// Extract Go pre-checkout query ID from a decoded Telegram update.
#[must_use]
pub fn pre_checkout_query_id_from_update(update: &TelegramUpdate) -> Option<&str> {
    let TelegramUpdateType::PreCheckoutQuery(query) = &update.update_type else {
        return None;
    };
    Some(pre_checkout_query_id(query))
}

fn pre_checkout_query_id(query: &TelegramPreCheckoutQuery) -> &str {
    &query.id
}

/// Handle only pre-checkout query updates and delegate all other updates to the caller.
pub async fn handle_pre_checkout_update_or_else<Effects, HandleFn, HandleFuture, HandleError>(
    effects: &Effects,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<PreCheckoutUpdateRoute, HandleError>
where
    Effects: PreCheckoutPaymentEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let Some(pre_checkout_query_id) = pre_checkout_query_id_from_update(&update) else {
        handle_other(update).await?;
        return Ok(PreCheckoutUpdateRoute::Delegated);
    };

    match effects
        .answer_pre_checkout_query(pre_checkout_query_id)
        .await
    {
        Ok(()) => Ok(PreCheckoutUpdateRoute::Processed(
            PreCheckoutOutcome::Acknowledged,
        )),
        Err(error) => {
            tracing::warn!(
                %error,
                pre_checkout_query_id,
                "failed to answer pre-checkout query"
            );
            Ok(PreCheckoutUpdateRoute::Processed(
                PreCheckoutOutcome::AcknowledgementError,
            ))
        }
    }
}

/// Handle only successful-payment updates and delegate all other updates to the caller.
pub async fn handle_successful_payment_update_or_else_at<
    Store,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    store: &Store,
    effects: &Effects,
    update: TelegramUpdate,
    now: OffsetDateTime,
    handle_other: HandleFn,
) -> Result<SuccessfulPaymentUpdateRoute, HandleError>
where
    Store: SuccessfulPaymentStore + Sync,
    Effects: SuccessfulPaymentEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let Some(message) = successful_payment_message_from_update(&update) else {
        handle_other(update).await?;
        return Ok(SuccessfulPaymentUpdateRoute::Delegated);
    };

    let report = process_successful_payment_at(store, effects, &message, now).await;
    Ok(SuccessfulPaymentUpdateRoute::Processed(report.outcome))
}

async fn process_subscription_payment<Store, Effects>(
    store: &Store,
    effects: &Effects,
    message: &SuccessfulPaymentMessage,
    now: OffsetDateTime,
) -> SuccessfulPaymentReport
where
    Store: SuccessfulPaymentStore + Sync,
    Effects: SuccessfulPaymentEffects + Sync,
{
    persist_payment_user(store, &message.user).await;
    let expires_at = now + time::Duration::days(openplotva_telegram::SUBSCRIPTION_DURATION_DAYS);
    let mut duplicate_subscription = false;
    let subscription = match store
        .create_subscription(subscription_create(
            &message.user,
            &message.payment,
            expires_at,
        ))
        .await
    {
        Ok(subscription) => subscription,
        Err(error) if Store::is_duplicate_insert_no_row(&error) => {
            duplicate_subscription = true;
            match store
                .get_subscription_by_telegram_payment_charge_id(
                    &message.payment.telegram_payment_charge_id,
                )
                .await
            {
                Ok(Some(subscription)) => subscription,
                Ok(None) => {
                    let error = "duplicate subscription was not found by Telegram charge ID";
                    send_payment_text(
                        effects,
                        message,
                        SUBSCRIPTION_DETAILS_ERROR_TEXT,
                        openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
                    )
                    .await;
                    return SuccessfulPaymentReport::with_error(
                        SuccessfulPaymentOutcome::SubscriptionStorageError,
                        error,
                    );
                }
                Err(error) => {
                    send_payment_text(
                        effects,
                        message,
                        SUBSCRIPTION_DETAILS_ERROR_TEXT,
                        openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
                    )
                    .await;
                    return SuccessfulPaymentReport::with_error(
                        SuccessfulPaymentOutcome::SubscriptionStorageError,
                        error,
                    );
                }
            }
        }
        Err(error) => {
            send_payment_text(
                effects,
                message,
                SUBSCRIPTION_SAVE_ERROR_TEXT,
                openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
            )
            .await;
            return SuccessfulPaymentReport::with_error(
                SuccessfulPaymentOutcome::SubscriptionStorageError,
                error,
            );
        }
    };

    let reason = subscription_payment_reason(&message.payment.telegram_payment_charge_id);
    let event = match store
        .create_vip_event(VipEventCreate {
            user_id: message.user.id,
            event_type: VIP_EVENT_TYPE_PAYMENT,
            delta_seconds: vip_days_to_seconds(openplotva_telegram::SUBSCRIPTION_DURATION_DAYS),
            subscription_id: Some(subscription.id),
            actor_user_id: None,
            reason: Some(&reason),
        })
        .await
    {
        Ok(event) => event,
        Err(error) => {
            send_payment_text(
                effects,
                message,
                SUBSCRIPTION_LEDGER_ERROR_TEXT,
                openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
            )
            .await;
            return SuccessfulPaymentReport::with_error(
                SuccessfulPaymentOutcome::SubscriptionLedgerError,
                error,
            );
        }
    };

    send_payment_text(
        effects,
        message,
        &subscription_success_text(event.effective_expires_at),
        "",
    )
    .await;
    let _ = effects.invalidate_vip_cache(message.user.id).await;

    let outcome = if duplicate_subscription {
        SuccessfulPaymentOutcome::SubscriptionDuplicateProcessed
    } else {
        SuccessfulPaymentOutcome::SubscriptionProcessed
    };
    SuccessfulPaymentReport::new(outcome)
}

async fn process_donation_payment<Store, Effects>(
    store: &Store,
    effects: &Effects,
    message: &SuccessfulPaymentMessage,
) -> SuccessfulPaymentReport
where
    Store: SuccessfulPaymentStore + Sync,
    Effects: SuccessfulPaymentEffects + Sync,
{
    persist_payment_user(store, &message.user).await;
    let mut duplicate_donation = false;
    let donation = match store.create_donation(donation_create(message)).await {
        Ok(donation) => donation,
        Err(error) if Store::is_duplicate_insert_no_row(&error) => {
            duplicate_donation = true;
            match store
                .get_donation_by_telegram_payment_charge_id(
                    &message.payment.telegram_payment_charge_id,
                )
                .await
            {
                Ok(Some(donation)) => donation,
                Ok(None) => {
                    let error = "duplicate donation was not found by Telegram charge ID";
                    send_payment_text(
                        effects,
                        message,
                        DONATION_DETAILS_ERROR_TEXT,
                        openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
                    )
                    .await;
                    return SuccessfulPaymentReport::with_error(
                        SuccessfulPaymentOutcome::DonationStorageError,
                        error,
                    );
                }
                Err(error) => {
                    send_payment_text(
                        effects,
                        message,
                        DONATION_DETAILS_ERROR_TEXT,
                        openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
                    )
                    .await;
                    return SuccessfulPaymentReport::with_error(
                        SuccessfulPaymentOutcome::DonationStorageError,
                        error,
                    );
                }
            }
        }
        Err(error) => {
            send_payment_text(
                effects,
                message,
                DONATION_SAVE_ERROR_TEXT,
                openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
            )
            .await;
            return SuccessfulPaymentReport::with_error(
                SuccessfulPaymentOutcome::DonationStorageError,
                error,
            );
        }
    };

    send_payment_text(
        effects,
        message,
        &donation_success_text(donation.amount_stars),
        "",
    )
    .await;
    if duplicate_donation {
        SuccessfulPaymentReport::new(SuccessfulPaymentOutcome::DonationDuplicateProcessed)
    } else {
        SuccessfulPaymentReport::new(SuccessfulPaymentOutcome::DonationProcessed)
    }
}

async fn persist_payment_user<Store>(store: &Store, user: &UserState)
where
    Store: SuccessfulPaymentStore + Sync,
{
    let _ = store.upsert_payment_user(user).await;
}

async fn send_payment_text<Effects>(
    effects: &Effects,
    message: &SuccessfulPaymentMessage,
    text: &str,
    parse_mode: &str,
) where
    Effects: SuccessfulPaymentEffects + Sync,
{
    let _ = effects
        .send_text(PaymentTextMessage {
            chat_id: message.chat_id,
            reply_to_message_id: message.message_id,
            text,
            parse_mode,
        })
        .await;
}

fn subscription_create<'a>(
    user: &UserState,
    payment: &'a SuccessfulPayment,
    expires_at: OffsetDateTime,
) -> SubscriptionCreate<'a> {
    SubscriptionCreate {
        user_id: user.id,
        telegram_payment_charge_id: &payment.telegram_payment_charge_id,
        provider_payment_charge_id: &payment.provider_payment_charge_id,
        expires_at,
    }
}

fn donation_create(message: &SuccessfulPaymentMessage) -> DonationCreate<'_> {
    DonationCreate {
        user_id: message.user.id,
        telegram_payment_charge_id: &message.payment.telegram_payment_charge_id,
        provider_payment_charge_id: &message.payment.provider_payment_charge_id,
        amount_stars: message.payment.total_amount,
    }
}

fn subscription_payment_reason(telegram_payment_charge_id: &str) -> String {
    format!("payment {telegram_payment_charge_id}")
        .trim()
        .to_owned()
}

/// Go `subscriptionSuccessText`.
#[must_use]
pub fn subscription_success_text(expires_at: OffsetDateTime) -> String {
    format!(
        "✅ Спасибо за подписку! Ваш VIP статус активирован до {}.\n\n",
        go_date(expires_at)
    )
}

/// Go donation thank-you text from `processDonationPayment`.
#[must_use]
pub fn donation_success_text(amount_stars: i64) -> String {
    format!(
        "❤️ Большое спасибо за ваш донат в размере {amount_stars} звезд!\n\n\
         Ваша поддержка безумно согревает моё сердце и вдохновляет работать над ботом дальше! \
         Благодаря вам Плотва становится лучше каждый день."
    )
}

/// Go VIP invoice button message text.
#[must_use]
pub fn subscription_invoice_message_text() -> String {
    format!(
        "✨ Нажмите на кнопку ниже, чтобы оформить VIP подписку на <b>30 дней</b> и получить:\n\n{}",
        VIP_BENEFITS_EXTENDED
    )
}

fn donation_invoice_message_text() -> String {
    "💖 Нажмите на кнопку ниже, чтобы поддержать разработчика!\n\n\
     Ваша поддержка:\n\
     ❤️ Согревает сердце разработчика\n\
     🚀 Помогает развивать бота\n\
     ✨ Вдохновляет на создание новых функций\n\n\
     <i>Чтобы установить другую сумму - используйте формат <code>/donate XXX</code>, где <code>XXX</code> - сумма в звездах.</i>"
        .to_owned()
}

fn donation_invoice_button_text(amount_stars: i64) -> String {
    format!("🌟 Отправить донат {amount_stars} звезд?")
}

fn go_date(value: OffsetDateTime) -> String {
    format!(
        "{:02}.{:02}.{:04}",
        value.day(),
        u8::from(value.month()),
        value.year()
    )
}

fn vip_display_days_left_at(expires_at: OffsetDateTime, now: OffsetDateTime) -> i64 {
    let remaining = expires_at - now;
    if remaining <= time::Duration::ZERO {
        return 0;
    }
    ((remaining.whole_seconds() as f64) / 86_400.0)
        .round()
        .max(0.0) as i64
}

const SUBSCRIPTION_SAVE_ERROR_TEXT: &str = "❌ Платеж прошел успешно, но возникла ошибка при активации подписки. Пожалуйста, обратитесь в [поддержку](https://t.me/WaveCut)";
const SUBSCRIPTION_DETAILS_ERROR_TEXT: &str = "✅ Платеж успешно обработан, но возникла ошибка при получении деталей подписки. Пожалуйста, обратитесь в [поддержку](https://t.me/WaveCut)";
const SUBSCRIPTION_LEDGER_ERROR_TEXT: &str = "❌ Платеж прошел успешно, но возникла ошибка при фиксации VIP периода. Пожалуйста, обратитесь в [поддержку](https://t.me/WaveCut)";
const VIP_SUBSCRIPTION_DETAILS_ERROR_TEXT: &str =
    "❌ Не удалось получить информацию о вашей подписке.";
const VIP_STATUS_DETAILS_ERROR_TEXT: &str =
    "❌ Не удалось получить информацию о вашем VIP статусе.";
const DONATION_SAVE_ERROR_TEXT: &str = "❤️ Спасибо за донат! К сожалению, возникла ошибка при сохранении информации о платеже. Можете сообщить об этом в [поддержку](https://t.me/WaveCut)";
const DONATION_DETAILS_ERROR_TEXT: &str = "❤️ Спасибо за ваш донат! Платеж успешно обработан, но возникла ошибка при получении деталей. Можете сообщить об этом в [поддержку](https://t.me/WaveCut)";
const USER_IDENTIFICATION_ERROR_TEXT: &str = "❌ Не удалось определить пользователя.";
const SUBSCRIPTION_INVOICE_CREATE_ERROR_TEXT: &str =
    "❌ Не удалось создать счет для подписки. Пожалуйста, попробуйте позже.";
const DONATION_INVOICE_CREATE_ERROR_TEXT: &str =
    "❌ Не удалось создать счет для доната. Пожалуйста, попробуйте позже.";
const SUBSCRIPTION_INVOICE_BUTTON_TEXT: &str = "💎 Оформить VIP подписку";
const VIP_BENEFITS_TEXT: &str = "⚡ Приоритетное выполнение запросов вне очереди\n\
                                🔄 Вдвое больше рисунков за то же время\n\
                                🖼️ Изображения лучшего качества с меньшей цензурой\n\
                                🎵 Генерация песен по теме через /song (с поддержкой audio reference)\n\
                                🚀 Доступ к новым функциям и фичам (в разработке)";
const VIP_BENEFITS_EXTENDED: &str = "⚡ Приоритетное выполнение запросов на рисование вне очереди\n\
                                    🔄 Вдвое увеличенные лимиты на рисование\n\
                                    🖼️ Изображения лучшего качества с меньшей цензурой\n\
                                    🎵 Генерация песен по теме через /song (с поддержкой audio reference)\n\
                                    🚀 Ранний доступ к будущим улучшениям и фичам";

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        error::Error,
        fmt,
        future::Future,
        io,
        pin::Pin,
        sync::atomic::{AtomicU64, Ordering},
        sync::{Arc, Mutex, MutexGuard},
    };

    use crate::updates::{UpdateHandler, UpdateHandlerFuture};
    use carapax::types::Update as TelegramUpdate;
    use openplotva_core::{UserState, VIP_EVENT_TYPE_PAYMENT, vip_days_to_seconds};
    use openplotva_storage::{
        DonationCreate, DonationRecord, SubscriptionCreate, SubscriptionRecord, VipCacheRecord,
        VipEventCreate, VipEventRecord, VipSummaryRecord,
    };
    use openplotva_taskman::{
        CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, ControlPayment,
        DEFAULT_PRIORITY, HIGH_PRIORITY, JobPayload, JobType, StatelessJobItem, new_control_job_at,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::{
        InvoiceButtonMessage, InvoiceControlJobOutcome, PaymentControlJobOutcome,
        PaymentControlJobQueue, PaymentControlJobQueueFuture, PaymentControlJobReport,
        PaymentControlJobWorkItem, PaymentControlJobWorkerFuture, PaymentControlJobWorkerQueue,
        PaymentEffectsFuture, PaymentInvoiceCommandUpdateRoute, PaymentInvoiceControlJobBuild,
        PaymentInvoiceEffects, PaymentInvoiceLinkFuture, PaymentStoreFuture, PaymentTextMessage,
        PaymentUpdateHandler, PaymentUpdateRoute, PreCheckoutOutcome, PreCheckoutPaymentEffects,
        PreCheckoutUpdateRoute, SuccessfulPayment, SuccessfulPaymentControlJobUpdateRoute,
        SuccessfulPaymentDispatcherEffects, SuccessfulPaymentEffects, SuccessfulPaymentMessage,
        SuccessfulPaymentOutcome, SuccessfulPaymentStore, SuccessfulPaymentUpdateRoute,
        encode_payment_control_job_queue_snapshot,
        enqueue_payment_invoice_command_update_or_else_at,
        enqueue_payment_invoice_command_update_with_vip_status_or_else_at,
        enqueue_successful_payment_update_or_else_at, execute_donate_invoice_control_job,
        execute_payment_control_job_at, execute_vip_invoice_control_job,
        handle_payment_update_or_else_at, handle_pre_checkout_update_or_else,
        handle_successful_payment_update_or_else_at, invoice_button_send_message_method,
        payment_invoice_control_job_from_update_at, process_payment_control_job_once_at,
        process_successful_payment_at, subscription_invoice_message_text,
        subscription_success_text, successful_payment_control_job_from_update_at,
        successful_payment_message_from_control_job, successful_payment_message_from_update,
    };

    #[tokio::test]
    async fn subscription_payment_writes_user_subscription_vip_event_and_success_side_effects()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let expires_at = now + time::Duration::days(30);
        let store = StoreStub::new().with_next_vip_event(VipEventRecord {
            id: 77,
            user_id: 42,
            event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
            delta_seconds: vip_days_to_seconds(30),
            effective_expires_at: expires_at,
            subscription_id: Some(10),
            actor_user_id: None,
            reason: "payment telegram-charge".to_owned(),
            created_at: now,
        });
        let effects = EffectsStub::default();

        let report = process_successful_payment_at(
            &store,
            &effects,
            &sample_message("subscription_42", 300),
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            SuccessfulPaymentOutcome::SubscriptionProcessed
        );
        assert_eq!(
            store.users(),
            vec![UserState::new(
                42,
                "Alice",
                Some("Smith".to_owned()),
                Some("alice".to_owned()),
                Some("en".to_owned()),
                Some(true),
            )]
        );
        assert_eq!(
            store.subscriptions(),
            vec![SubscriptionCreate {
                user_id: 42,
                telegram_payment_charge_id: "telegram-charge",
                provider_payment_charge_id: "provider-charge",
                expires_at,
            }]
        );
        assert_eq!(
            store.vip_events(),
            vec![VipEventCreate {
                user_id: 42,
                event_type: VIP_EVENT_TYPE_PAYMENT,
                delta_seconds: vip_days_to_seconds(30),
                subscription_id: Some(10),
                actor_user_id: None,
                reason: Some("payment telegram-charge"),
            }]
        );
        assert_eq!(
            effects.sent_texts(),
            vec![subscription_success_text(expires_at)]
        );
        assert_eq!(effects.invalidated_vip_users(), vec![42]);
        Ok(())
    }

    #[tokio::test]
    async fn donation_payment_writes_user_donation_and_go_thank_you_text()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let store = StoreStub::new().with_next_donation(DonationRecord {
            id: 20,
            user_id: 42,
            telegram_payment_charge_id: "telegram-charge".to_owned(),
            provider_payment_charge_id: "provider-charge".to_owned(),
            amount_stars: 600,
            created_at: now,
        });
        let effects = EffectsStub::default();

        let report = process_successful_payment_at(
            &store,
            &effects,
            &sample_message("donation_42_600", 600),
            now,
        )
        .await;

        assert_eq!(report.outcome, SuccessfulPaymentOutcome::DonationProcessed);
        assert_eq!(
            store.donations(),
            vec![DonationCreate {
                user_id: 42,
                telegram_payment_charge_id: "telegram-charge",
                provider_payment_charge_id: "provider-charge",
                amount_stars: 600,
            }]
        );
        let sent_texts = effects.sent_texts();
        assert_eq!(sent_texts.len(), 1);
        assert!(sent_texts[0].contains("Большое спасибо за ваш донат в размере 600 звезд"));
        assert!(effects.invalidated_vip_users().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_subscription_payment_loads_existing_record_before_vip_ledger()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let expires_at = now + time::Duration::days(30);
        let existing = SubscriptionRecord {
            id: 55,
            user_id: 42,
            telegram_payment_charge_id: "telegram-charge".to_owned(),
            provider_payment_charge_id: "provider-charge".to_owned(),
            expires_at,
            created_at: now,
            updated_at: now,
            canceled_at: None,
            refunded_at: None,
        };
        let store = StoreStub::new()
            .with_next_subscription_error(StubError::Duplicate)
            .with_existing_subscription(existing)
            .with_next_vip_event(VipEventRecord {
                id: 77,
                user_id: 42,
                event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
                delta_seconds: vip_days_to_seconds(30),
                effective_expires_at: expires_at,
                subscription_id: Some(55),
                actor_user_id: None,
                reason: "payment telegram-charge".to_owned(),
                created_at: now,
            });
        let effects = EffectsStub::default();

        let report = process_successful_payment_at(
            &store,
            &effects,
            &sample_message("subscription_42", 300),
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            SuccessfulPaymentOutcome::SubscriptionDuplicateProcessed
        );
        assert_eq!(
            store.vip_events(),
            vec![VipEventCreate {
                user_id: 42,
                event_type: VIP_EVENT_TYPE_PAYMENT,
                delta_seconds: vip_days_to_seconds(30),
                subscription_id: Some(55),
                actor_user_id: None,
                reason: Some("payment telegram-charge"),
            }]
        );
        assert_eq!(
            effects.sent_texts(),
            vec![subscription_success_text(expires_at)]
        );
        assert_eq!(effects.invalidated_vip_users(), vec![42]);
        Ok(())
    }

    #[tokio::test]
    async fn subscription_payment_save_error_sends_go_html_error_text() -> Result<(), Box<dyn Error>>
    {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let store = StoreStub::new().with_next_subscription_error(StubError::Request);
        let effects = EffectsStub::default();

        let report = process_successful_payment_at(
            &store,
            &effects,
            &sample_message("subscription_42", 300),
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            SuccessfulPaymentOutcome::SubscriptionStorageError
        );
        assert_eq!(
            effects.sent_payment_texts(),
            vec![SentPaymentText {
                chat_id: 42,
                reply_to_message_id: 100,
                text: super::SUBSCRIPTION_SAVE_ERROR_TEXT.to_owned(),
                parse_mode: openplotva_telegram::TELEGRAM_PARSE_MODE_HTML.to_owned(),
            }]
        );
        assert!(store.vip_events().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_processor_skips_unsupported_currency_and_unknown_payload()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let store = StoreStub::new();
        let effects = EffectsStub::default();

        let mut unsupported = sample_message("subscription_42", 300);
        unsupported.payment.currency = "USD".to_owned();
        let unsupported_report =
            process_successful_payment_at(&store, &effects, &unsupported, now).await;
        let unknown_report = process_successful_payment_at(
            &store,
            &effects,
            &sample_message("surprise_42", 300),
            now,
        )
        .await;

        assert_eq!(
            unsupported_report.outcome,
            SuccessfulPaymentOutcome::UnsupportedCurrency
        );
        assert_eq!(
            unknown_report.outcome,
            SuccessfulPaymentOutcome::UnknownPayload
        );
        assert!(store.users().is_empty());
        assert!(store.subscriptions().is_empty());
        assert!(store.donations().is_empty());
        assert!(store.vip_events().is_empty());
        assert!(effects.sent_texts().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        Ok(())
    }

    #[test]
    fn successful_payment_message_from_update_extracts_go_message_context()
    -> Result<(), Box<dyn Error>> {
        let message = successful_payment_message_from_update(&sample_successful_payment_update(
            "subscription_42",
            300,
        )?)
        .expect("successful payment should be extracted");

        assert_eq!(message.chat_id, 42);
        assert_eq!(message.message_id, 100);
        assert_eq!(
            message.user,
            UserState::new(
                42,
                "Alice",
                Some("Smith".to_owned()),
                Some("alice".to_owned()),
                Some("en".to_owned()),
                Some(true),
            )
        );
        assert_eq!(message.payment.currency, "XTR");
        assert_eq!(message.payment.total_amount, 300);
        assert_eq!(message.payment.invoice_payload, "subscription_42");
        assert_eq!(
            message.payment.telegram_payment_charge_id,
            "telegram-charge"
        );
        assert_eq!(
            message.payment.provider_payment_charge_id,
            "provider-charge"
        );
        Ok(())
    }

    #[tokio::test]
    async fn successful_payment_update_wrapper_processes_payment_without_fallback()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let expires_at = now + time::Duration::days(30);
        let store = StoreStub::new().with_next_vip_event(VipEventRecord {
            id: 77,
            user_id: 42,
            event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
            delta_seconds: vip_days_to_seconds(30),
            effective_expires_at: expires_at,
            subscription_id: Some(10),
            actor_user_id: None,
            reason: "payment telegram-charge".to_owned(),
            created_at: now,
        });
        let effects = EffectsStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = handle_successful_payment_update_or_else_at(
            &store,
            &effects,
            sample_successful_payment_update("subscription_42", 300)?,
            now,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(
            route,
            SuccessfulPaymentUpdateRoute::Processed(
                SuccessfulPaymentOutcome::SubscriptionProcessed
            )
        );
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        assert_eq!(
            effects.sent_texts(),
            vec![subscription_success_text(expires_at)]
        );
        assert_eq!(effects.invalidated_vip_users(), vec![42]);
        Ok(())
    }

    #[test]
    fn successful_payment_update_builds_go_taskman_control_job() -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let update = sample_successful_payment_update_with_thread("subscription_42", 300, 77)?;

        let job = successful_payment_control_job_from_update_at(&update, created)
            .expect("successful payment should build a control job");

        assert_eq!(CONTROL_QUEUE_NAME, "control");
        assert_eq!(job.title, "successful payment");
        assert_eq!(job.created, created);
        assert_eq!(job.priority, HIGH_PRIORITY);
        assert_eq!(job.data.job_type, JobType::Control);
        let telegram_data = job.data.telegram_data.as_ref().expect("Telegram data");
        assert_eq!(telegram_data.chat_id, 42);
        assert_eq!(telegram_data.user_id, 42);
        assert_eq!(telegram_data.message_id, 100);
        assert_eq!(telegram_data.thread_message_id, Some(77));
        assert_eq!(telegram_data.user_full_name, "Alice Smith");
        let control_data = job.data.control_data.as_ref().expect("control data");
        assert_eq!(control_data.kind, ControlKind::SuccessfulPayment);
        assert_eq!(control_data.chat_type, "private");
        assert_eq!(control_data.user_name, "alice");
        assert_eq!(control_data.first_name, "Alice");
        assert_eq!(control_data.last_name, "Smith");
        assert_eq!(control_data.language_code, "en");
        assert!(control_data.is_premium);
        assert_eq!(
            control_data.payment,
            Some(ControlPayment {
                currency: "XTR".to_owned(),
                total_amount: 300,
                invoice_payload: "subscription_42".to_owned(),
                telegram_payment_charge_id: "telegram-charge".to_owned(),
                provider_payment_charge_id: "provider-charge".to_owned(),
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn successful_payment_queue_wrapper_assigns_control_job_without_fallback()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = PaymentControlJobQueueStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = enqueue_successful_payment_update_or_else_at(
            &queue,
            sample_successful_payment_update("subscription_42", 300)?,
            created,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(route, SuccessfulPaymentControlJobUpdateRoute::Queued);
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "successful payment");
        assert_eq!(assigned[0].1.priority, HIGH_PRIORITY);
        assert_eq!(
            assigned[0]
                .1
                .data
                .control_data
                .as_ref()
                .expect("control data")
                .kind,
            ControlKind::SuccessfulPayment
        );
        Ok(())
    }

    #[tokio::test]
    async fn successful_payment_queue_wrapper_delegates_non_payment_updates()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = PaymentControlJobQueueStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = enqueue_successful_payment_update_or_else_at(
            &queue,
            sample_text_update()?,
            created,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(route, SuccessfulPaymentControlJobUpdateRoute::Delegated);
        assert_eq!(
            fallback_calls.lock().expect("fallback calls").as_slice(),
            &[999]
        );
        assert!(queue.assigned().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn successful_payment_queue_wrapper_keeps_assign_errors_nonfatal()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = PaymentControlJobQueueStub::failing(StubError::Request);

        let route = enqueue_successful_payment_update_or_else_at(
            &queue,
            sample_successful_payment_update("subscription_42", 300)?,
            created,
            |_update| async { Ok::<(), std::io::Error>(()) },
        )
        .await?;

        assert_eq!(route, SuccessfulPaymentControlJobUpdateRoute::QueueError);
        assert_eq!(queue.assigned().len(), 1);
        Ok(())
    }

    #[test]
    fn vip_invoice_command_builds_go_taskman_control_job() -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let PaymentInvoiceControlJobBuild::Job(job) = payment_invoice_control_job_from_update_at(
            &sample_payment_command_update("/vip")?,
            created,
        ) else {
            panic!("VIP command should build a control job");
        };

        assert_eq!(job.title, "vip invoice");
        assert_eq!(job.created, created);
        assert_eq!(job.priority, HIGH_PRIORITY);
        assert_eq!(job.data.job_type, JobType::Control);
        let telegram_data = job.data.telegram_data.as_ref().expect("Telegram data");
        assert_eq!(telegram_data.chat_id, 42);
        assert_eq!(telegram_data.user_id, 42);
        assert_eq!(telegram_data.message_id, 100);
        assert_eq!(telegram_data.thread_message_id, Some(77));
        assert_eq!(telegram_data.user_full_name, "Alice Smith");
        let control_data = job.data.control_data.as_ref().expect("control data");
        assert_eq!(control_data.kind, ControlKind::VipInvoice);
        assert_eq!(control_data.amount, 300);
        assert_eq!(control_data.chat_type, "private");
        assert_eq!(control_data.user_name, "alice");
        assert_eq!(control_data.first_name, "Alice");
        assert_eq!(control_data.last_name, "Smith");
        assert_eq!(control_data.language_code, "en");
        assert!(control_data.is_premium);
        Ok(())
    }

    #[test]
    fn vip_invoice_command_preserves_wavecut_price_override() -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let PaymentInvoiceControlJobBuild::Job(job) = payment_invoice_control_job_from_update_at(
            &sample_payment_command_update_with_username("/vip", "WaveCut")?,
            created,
        ) else {
            panic!("VIP command should build a control job");
        };

        let control_data = job.data.control_data.as_ref().expect("control data");
        assert_eq!(control_data.kind, ControlKind::VipInvoice);
        assert_eq!(control_data.amount, 1);
        assert_eq!(control_data.user_name, "WaveCut");
        Ok(())
    }

    #[test]
    fn donation_invoice_command_defaults_and_parses_go_amounts() -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let PaymentInvoiceControlJobBuild::Job(default_job) =
            payment_invoice_control_job_from_update_at(
                &sample_payment_command_update("/donate")?,
                created,
            )
        else {
            panic!("donation command should build a default invoice job");
        };
        let default_data = default_job
            .data
            .control_data
            .as_ref()
            .expect("control data");
        assert_eq!(default_job.title, "donate invoice");
        assert_eq!(default_data.kind, ControlKind::DonateInvoice);
        assert_eq!(default_data.amount, 600);

        let PaymentInvoiceControlJobBuild::Job(custom_job) =
            payment_invoice_control_job_from_update_at(
                &sample_payment_command_update("/donate 777")?,
                created,
            )
        else {
            panic!("donation command should build a custom invoice job");
        };
        let custom_data = custom_job.data.control_data.as_ref().expect("control data");
        assert_eq!(custom_data.kind, ControlKind::DonateInvoice);
        assert_eq!(custom_data.amount, 777);
        Ok(())
    }

    #[test]
    fn donation_invoice_command_rejects_go_invalid_amounts() -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        assert_eq!(
            payment_invoice_control_job_from_update_at(
                &sample_payment_command_update("/donate 9")?,
                created
            ),
            PaymentInvoiceControlJobBuild::InvalidDonationAmount
        );
        assert_eq!(
            payment_invoice_control_job_from_update_at(
                &sample_payment_command_update("/donate 10001")?,
                created
            ),
            PaymentInvoiceControlJobBuild::InvalidDonationAmount
        );
        assert_eq!(
            payment_invoice_control_job_from_update_at(
                &sample_payment_command_update("/donate abc")?,
                created
            ),
            PaymentInvoiceControlJobBuild::InvalidDonationAmount
        );
        Ok(())
    }

    #[tokio::test]
    async fn payment_invoice_command_queue_wrapper_assigns_control_job_without_fallback()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = PaymentControlJobQueueStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = enqueue_payment_invoice_command_update_or_else_at(
            &queue,
            sample_payment_command_update("/donate 777")?,
            created,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(route, PaymentInvoiceCommandUpdateRoute::Queued);
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "donate invoice");
        assert_eq!(assigned[0].1.priority, HIGH_PRIORITY);
        let control_data = assigned[0]
            .1
            .data
            .control_data
            .as_ref()
            .expect("control data");
        assert_eq!(control_data.kind, ControlKind::DonateInvoice);
        assert_eq!(control_data.amount, 777);
        Ok(())
    }

    #[tokio::test]
    async fn payment_invoice_command_queue_wrapper_reports_invalid_amount_without_queue_or_fallback()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = PaymentControlJobQueueStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = enqueue_payment_invoice_command_update_or_else_at(
            &queue,
            sample_payment_command_update("/donate 10001")?,
            created,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(
            route,
            PaymentInvoiceCommandUpdateRoute::InvalidDonationAmount
        );
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        assert!(queue.assigned().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_invoice_command_with_vip_status_sends_existing_status_without_queue()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let queue = PaymentControlJobQueueStub::default();
        let store = VipStatusStoreStub::new()
            .with_is_vip(true)
            .with_summary(active_vip_summary(now, now + time::Duration::days(3)))
            .with_active_subscription(active_subscription(now, now + time::Duration::days(3)));
        let effects = EffectsStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = enqueue_payment_invoice_command_update_with_vip_status_or_else_at(
            &queue,
            &store,
            &effects,
            sample_payment_command_update("/vip")?,
            created,
            now,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(
            route,
            PaymentInvoiceCommandUpdateRoute::ExistingVipStatusSent
        );
        assert!(queue.assigned().is_empty());
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        let status_messages = effects.vip_status_messages();
        assert_eq!(status_messages.len(), 1);
        assert!(
            status_messages[0]
                .text
                .contains("У вас уже есть активный VIP статус")
        );
        assert!(status_messages[0].show_cancel_button);
        assert_eq!(
            store.calls(),
            vec![
                "is_vip_at",
                "get_vip_summary_by_user",
                "get_active_subscription"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn payment_invoice_command_with_vip_status_queues_invoice_when_user_is_not_vip()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let queue = PaymentControlJobQueueStub::default();
        let store = VipStatusStoreStub::new()
            .with_is_vip(false)
            .with_cache(active_vip_cache(now, now + time::Duration::days(5)));
        let effects = EffectsStub::default();

        let route = enqueue_payment_invoice_command_update_with_vip_status_or_else_at(
            &queue,
            &store,
            &effects,
            sample_payment_command_update("/vip")?,
            created,
            now,
            |_update| async { Ok::<(), std::io::Error>(()) },
        )
        .await?;

        assert_eq!(route, PaymentInvoiceCommandUpdateRoute::Queued);
        assert!(effects.vip_status_messages().is_empty());
        assert_eq!(queue.assigned().len(), 1);
        assert_eq!(queue.assigned()[0].1.title, "vip invoice");
        assert_eq!(store.calls(), vec!["is_vip_at"]);
        Ok(())
    }

    #[tokio::test]
    async fn payment_update_wrapper_acknowledges_pre_checkout_before_fallback()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let queue = PaymentControlJobQueueStub::default();
        let store = VipStatusStoreStub::new().with_is_vip(true);
        let effects = EffectsStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = handle_payment_update_or_else_at(
            &queue,
            &store,
            &effects,
            sample_pre_checkout_update()?,
            created,
            now,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(
            route,
            PaymentUpdateRoute::PreCheckout(PreCheckoutOutcome::Acknowledged)
        );
        assert_eq!(
            effects.answered_pre_checkout_queries(),
            vec!["pre-checkout-id".to_owned()]
        );
        assert!(queue.assigned().is_empty());
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_update_wrapper_queues_successful_payment_before_fallback()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let queue = PaymentControlJobQueueStub::default();
        let store = VipStatusStoreStub::new().with_is_vip(true);
        let effects = EffectsStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = handle_payment_update_or_else_at(
            &queue,
            &store,
            &effects,
            sample_successful_payment_update("subscription_42", 300)?,
            created,
            now,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(
            route,
            PaymentUpdateRoute::SuccessfulPayment(SuccessfulPaymentControlJobUpdateRoute::Queued)
        );
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "successful payment");
        assert_eq!(
            assigned[0]
                .1
                .data
                .control_data
                .as_ref()
                .expect("control data")
                .kind,
            ControlKind::SuccessfulPayment
        );
        assert!(effects.answered_pre_checkout_queries().is_empty());
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_update_wrapper_queues_invoice_command_before_fallback()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let queue = PaymentControlJobQueueStub::default();
        let store = VipStatusStoreStub::new().with_is_vip(false);
        let effects = EffectsStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = handle_payment_update_or_else_at(
            &queue,
            &store,
            &effects,
            sample_payment_command_update("/vip")?,
            created,
            now,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(
            route,
            PaymentUpdateRoute::InvoiceCommand(PaymentInvoiceCommandUpdateRoute::Queued)
        );
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].1.title, "vip invoice");
        assert_eq!(
            assigned[0]
                .1
                .data
                .control_data
                .as_ref()
                .expect("control data")
                .kind,
            ControlKind::VipInvoice
        );
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_update_wrapper_sends_existing_vip_status_before_invoice_queueing()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let queue = PaymentControlJobQueueStub::default();
        let store = VipStatusStoreStub::new()
            .with_is_vip(true)
            .with_summary(active_vip_summary(now, now + time::Duration::days(3)))
            .with_active_subscription(active_subscription(now, now + time::Duration::days(3)));
        let effects = EffectsStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = handle_payment_update_or_else_at(
            &queue,
            &store,
            &effects,
            sample_payment_command_update("/vip")?,
            created,
            now,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(
            route,
            PaymentUpdateRoute::InvoiceCommand(
                PaymentInvoiceCommandUpdateRoute::ExistingVipStatusSent
            )
        );
        assert!(queue.assigned().is_empty());
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        let status_messages = effects.vip_status_messages();
        assert_eq!(status_messages.len(), 1);
        assert!(
            status_messages[0]
                .text
                .contains("У вас уже есть активный VIP статус")
        );
        assert!(status_messages[0].show_cancel_button);
        Ok(())
    }

    #[tokio::test]
    async fn payment_update_wrapper_delegates_non_payment_updates_once()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let queue = PaymentControlJobQueueStub::default();
        let store = VipStatusStoreStub::new().with_is_vip(true);
        let effects = EffectsStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = handle_payment_update_or_else_at(
            &queue,
            &store,
            &effects,
            sample_text_update()?,
            created,
            now,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(route, PaymentUpdateRoute::Delegated);
        assert_eq!(
            fallback_calls.lock().expect("fallback calls").as_slice(),
            &[999]
        );
        assert!(queue.assigned().is_empty());
        assert!(effects.answered_pre_checkout_queries().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_update_handler_delegates_non_payment_update_to_wrapped_handler()
    -> Result<(), Box<dyn Error>> {
        let queue = Arc::new(PaymentControlJobQueueStub::default());
        let store = Arc::new(VipStatusStoreStub::new().with_is_vip(true));
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = PaymentUpdateHandler::new(
            Arc::clone(&queue),
            Arc::clone(&store),
            Arc::clone(&effects),
            Arc::clone(&next),
        );

        handler.handle_update(sample_text_update()?).await?;

        assert_eq!(next.calls(), vec![999]);
        assert!(queue.assigned().is_empty());
        assert!(effects.answered_pre_checkout_queries().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_update_handler_intercepts_invoice_command_before_wrapped_handler()
    -> Result<(), Box<dyn Error>> {
        let queue = Arc::new(PaymentControlJobQueueStub::default());
        let store = Arc::new(VipStatusStoreStub::new().with_is_vip(false));
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = PaymentUpdateHandler::new(
            Arc::clone(&queue),
            Arc::clone(&store),
            Arc::clone(&effects),
            Arc::clone(&next),
        );

        handler
            .handle_update(sample_payment_command_update("/vip")?)
            .await?;

        assert!(next.calls().is_empty());
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "vip invoice");
        assert!(effects.answered_pre_checkout_queries().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn successful_payment_dispatcher_effects_queue_text_reply_and_invalidate_vip_cache()
    -> Result<(), Box<dyn Error>> {
        let store = VirtualMessageStoreStub::default();
        let queue = Arc::new(openplotva_telegram::DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        ));
        let invalidator = VipCacheInvalidatorStub::default();
        let next_id = Arc::new(AtomicU64::new(1));
        let effects = SuccessfulPaymentDispatcherEffects::new(
            store.clone(),
            Arc::clone(&queue),
            invalidator.clone(),
        )
        .with_virtual_id_factory({
            let next_id = Arc::clone(&next_id);
            Arc::new(move || format!("payment-vmsg-{}", next_id.fetch_add(1, Ordering::Relaxed)))
        });

        effects
            .send_text(PaymentTextMessage {
                chat_id: 42,
                reply_to_message_id: 100,
                text: "hello <b>VIP</b>",
                parse_mode: openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
            })
            .await?;
        effects.invalidate_vip_cache(42).await?;

        assert_eq!(
            store.inserted(),
            vec![("payment-vmsg-1".to_owned(), 42, None)]
        );
        assert_eq!(invalidator.invalidated_users(), vec![42]);

        let snapshot = queue.snapshot();
        assert!(snapshot.immediate.is_empty());
        assert_eq!(snapshot.regular.len(), 1);
        assert_eq!(snapshot.regular[0].chat_id, 42);
        assert_eq!(snapshot.regular[0].virtual_id, "payment-vmsg-1");

        let item = queue.dequeue_regular().expect("queued payment text");
        assert_eq!(
            item.method_kind(),
            Some(openplotva_telegram::TelegramOutboundMethodKind::SendMessage)
        );
        let (_metadata, method) = item.into_parts();
        let method = match method.expect("queued Telegram method") {
            openplotva_telegram::TelegramOutboundMethod::SendMessage(method) => method,
            other => panic!("unexpected payment text method: {other:?}"),
        };
        let payload = serde_json::to_value(method)?;
        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["text"], json!("hello <b>VIP</b>"));
        assert_eq!(
            payload["parse_mode"],
            json!(openplotva_telegram::TELEGRAM_PARSE_MODE_HTML)
        );
        assert_eq!(payload["reply_parameters"]["message_id"], json!(100));
        assert_eq!(payload["reply_parameters"]["chat_id"], json!(42));
        assert_eq!(
            payload["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        Ok(())
    }

    #[test]
    fn vip_redirect_message_method_matches_go_group_command_payload() -> Result<(), Box<dyn Error>>
    {
        let message = super::vip_redirect_message("PlotvaBot", -10042, 555, Some(77));

        assert_eq!(message.chat_id, -10042);
        assert_eq!(message.reply_to_message_id, 555);
        assert_eq!(
            message.text,
            "✨ Для оформления VIP подписки на 30 дней, пожалуйста, перейдите в личные сообщения с ботом.\n\n\
             VIP даёт вам:\n\
             ⚡ Приоритетное выполнение запросов вне очереди\n\
             🔄 Вдвое больше рисунков за то же время\n\
             🖼️ Изображения лучшего качества с меньшей цензурой\n\
             🎵 Генерация песен по теме через /song (с поддержкой audio reference)\n\
             🚀 Доступ к новым функциям и фичам (в разработке)"
        );
        assert_eq!(message.button_text, "💎 Оформить VIP подписку");
        assert_eq!(message.url, "https://t.me/PlotvaBot?start=vip");

        let payload = serde_json::to_value(super::payment_redirect_send_message_method(&message)?)?;
        assert_eq!(payload["chat_id"], json!(-10042));
        assert_eq!(payload["message_thread_id"], json!(77));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(555));
        assert_eq!(payload["reply_parameters"]["chat_id"], json!(-10042));
        assert!(
            payload["reply_parameters"]
                .get("allow_sending_without_reply")
                .is_none()
        );
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!("💎 Оформить VIP подписку")
        );
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["url"],
            json!("https://t.me/PlotvaBot?start=vip")
        );
        assert!(payload.get("parse_mode").is_none());
        Ok(())
    }

    #[test]
    fn donation_redirect_message_preserves_valid_go_amount_start_param()
    -> Result<(), Box<dyn Error>> {
        let message = super::donation_redirect_message("PlotvaBot", -10042, 555, None, "777");

        assert_eq!(message.button_text, "💝 Сделать донат");
        assert_eq!(message.url, "https://t.me/PlotvaBot?start=donate_777");
        assert_eq!(
            message.text,
            "💖 Для отправки доната, пожалуйста, перейдите в личные сообщения с ботом.\n\n\
             Ваша поддержка:\n\
             ❤️ Согреет сердце разработчика\n\
             🚀 Поможет развивать бота\n\
             ✨ Добавит мотивацию на создание новых функций"
        );

        let payload = serde_json::to_value(super::payment_redirect_send_message_method(&message)?)?;
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["url"],
            json!("https://t.me/PlotvaBot?start=donate_777")
        );
        Ok(())
    }

    #[test]
    fn donation_redirect_message_ignores_invalid_amount_like_go_group_redirect() {
        let message = super::donation_redirect_message("", -10042, 555, None, "10001");

        assert_eq!(message.url, "https://t.me/?start=donate");
    }

    #[test]
    fn payment_redirect_message_from_update_builds_non_private_command_redirect()
    -> Result<(), Box<dyn Error>> {
        let message = super::payment_redirect_message_from_update(
            &sample_group_payment_command_update("/donate 777")?,
            "PlotvaBot",
        )
        .expect("group donate command should build a redirect");

        assert_eq!(message.chat_id, -10042);
        assert_eq!(message.reply_to_message_id, 555);
        assert_eq!(message.message_thread_id, Some(77));
        assert_eq!(message.url, "https://t.me/PlotvaBot?start=donate_777");
        Ok(())
    }

    #[test]
    fn active_vip_status_message_method_matches_go_existing_vip_reply() -> Result<(), Box<dyn Error>>
    {
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let expires_at = time::Date::from_calendar_date(2026, time::Month::June, 19)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let summary = VipSummaryRecord {
            latest_event_id: 77,
            user_id: 42,
            latest_event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
            latest_delta_seconds: vip_days_to_seconds(30),
            effective_expires_at: expires_at,
            is_active: true,
            remaining_seconds: 259_200,
            latest_subscription_id: Some(10),
            latest_actor_user_id: None,
            latest_reason: "payment telegram-charge".to_owned(),
            latest_created_at: now,
        };
        let subscription = SubscriptionRecord {
            id: 10,
            user_id: 42,
            telegram_payment_charge_id: "telegram-charge".to_owned(),
            provider_payment_charge_id: String::new(),
            expires_at,
            created_at: now,
            updated_at: now,
            canceled_at: None,
            refunded_at: None,
        };

        let message = super::active_vip_status_message_at(
            &summary,
            Some(&subscription),
            -10042,
            555,
            Some(77),
            now,
        );

        assert_eq!(
            message.text,
            "✅ У вас уже есть активный VIP статус!\n\n\
             Он действует до: <b>19.06.2026</b> (3 дней осталось)\n\n\
             Вы получаете:\n\
             ⚡ Приоритетное выполнение запросов вне очереди\n\
             🔄 Вдвое больше рисунков за то же время\n\
             🖼️ Изображения лучшего качества с меньшей цензурой\n\
             🎵 Генерация песен по теме через /song (с поддержкой audio reference)\n\
             🚀 Доступ к новым функциям и фичам (в разработке)"
        );
        assert!(message.show_cancel_button);

        let payload = serde_json::to_value(super::vip_status_send_message_method(&message)?)?;
        assert_eq!(payload["chat_id"], json!(-10042));
        assert_eq!(payload["message_thread_id"], json!(77));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(555));
        assert_eq!(payload["reply_parameters"]["chat_id"], json!(-10042));
        assert_eq!(
            payload["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        assert_eq!(payload["parse_mode"], json!("HTML"));
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!("Отменить подписку")
        );
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            json!("{\"action\":\"cancel_vip\"}")
        );
        Ok(())
    }

    #[test]
    fn external_vip_cache_status_message_matches_go_without_cancel_button()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let vip_cache = VipCacheRecord {
            user_id: 42,
            is_vip: true,
            expires_at: now + time::Duration::days(1),
            created_at: now,
            updated_at: now,
        };

        let message = super::external_vip_status_message_if_active_at(
            Some(&vip_cache),
            -10042,
            555,
            None,
            now,
        )
        .expect("active external VIP cache should build a status message");

        assert_eq!(
            message.text,
            "✅ У вас активен VIP статус.\n\n\
             Доступ подтверждён через внешний источник.\n\n\
             Вы получаете:\n\
             ⚡ Приоритетное выполнение запросов вне очереди\n\
             🔄 Вдвое больше рисунков за то же время\n\
             🖼️ Изображения лучшего качества с меньшей цензурой\n\
             🎵 Генерация песен по теме через /song (с поддержкой audio reference)\n\
             🚀 Доступ к новым функциям и фичам (в разработке)"
        );
        assert!(!message.show_cancel_button);

        let payload = serde_json::to_value(super::vip_status_send_message_method(&message)?)?;
        assert!(payload.get("reply_markup").is_none());
        assert_eq!(payload["parse_mode"], json!("HTML"));
        Ok(())
    }

    #[tokio::test]
    async fn build_vip_status_message_uses_active_summary_before_external_cache()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let store = VipStatusStoreStub::new()
            .with_summary(active_vip_summary(now, now + time::Duration::days(3)))
            .with_active_subscription(active_subscription(now, now + time::Duration::days(3)))
            .with_cache(active_vip_cache(now, now + time::Duration::days(5)));

        let message = super::build_vip_status_message_at(&store, 42, -10042, 555, Some(77), now)
            .await?
            .expect("active summary should build a status message");

        assert!(message.text.contains("У вас уже есть активный VIP статус"));
        assert!(message.show_cancel_button);
        assert_eq!(
            store.calls(),
            vec!["get_vip_summary_by_user", "get_active_subscription"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn build_vip_status_message_falls_back_to_external_cache_after_inactive_summary()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let mut inactive_summary = active_vip_summary(now, now + time::Duration::days(3));
        inactive_summary.is_active = false;
        let store = VipStatusStoreStub::new()
            .with_summary(inactive_summary)
            .with_active_subscription(active_subscription(now, now + time::Duration::days(3)))
            .with_cache(active_vip_cache(now, now + time::Duration::days(5)));

        let message = super::build_vip_status_message_at(&store, 42, -10042, 555, None, now)
            .await?
            .expect("active external cache should build a status message");

        assert!(
            message
                .text
                .contains("Доступ подтверждён через внешний источник")
        );
        assert!(!message.show_cancel_button);
        assert_eq!(
            store.calls(),
            vec!["get_vip_summary_by_user", "get_vip_cache"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn successful_payment_update_wrapper_delegates_non_payment_messages()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let store = StoreStub::new();
        let effects = EffectsStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = handle_successful_payment_update_or_else_at(
            &store,
            &effects,
            sample_text_update()?,
            now,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(route, SuccessfulPaymentUpdateRoute::Delegated);
        assert_eq!(
            fallback_calls.lock().expect("fallback calls").as_slice(),
            &[999]
        );
        assert!(store.users().is_empty());
        assert!(effects.sent_texts().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn pre_checkout_update_wrapper_acknowledges_query_without_fallback()
    -> Result<(), Box<dyn Error>> {
        let effects = EffectsStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = handle_pre_checkout_update_or_else(
            &effects,
            sample_pre_checkout_update()?,
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(
            route,
            PreCheckoutUpdateRoute::Processed(PreCheckoutOutcome::Acknowledged)
        );
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        assert_eq!(
            effects.answered_pre_checkout_queries(),
            vec!["pre-checkout-id".to_owned()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn pre_checkout_update_wrapper_keeps_ack_errors_nonfatal_like_go()
    -> Result<(), Box<dyn Error>> {
        let effects = EffectsStub::default().with_pre_checkout_error(StubError::Request);

        let route = handle_pre_checkout_update_or_else(
            &effects,
            sample_pre_checkout_update()?,
            |_update| async { Ok::<(), std::io::Error>(()) },
        )
        .await?;

        assert_eq!(
            route,
            PreCheckoutUpdateRoute::Processed(PreCheckoutOutcome::AcknowledgementError)
        );
        assert_eq!(
            effects.answered_pre_checkout_queries(),
            vec!["pre-checkout-id".to_owned()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn pre_checkout_update_wrapper_delegates_non_pre_checkout_updates()
    -> Result<(), Box<dyn Error>> {
        let effects = EffectsStub::default();
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route =
            handle_pre_checkout_update_or_else(&effects, sample_text_update()?, move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| std::io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), std::io::Error>(())
                }
            })
            .await?;

        assert_eq!(route, PreCheckoutUpdateRoute::Delegated);
        assert_eq!(
            fallback_calls.lock().expect("fallback calls").as_slice(),
            &[999]
        );
        assert!(effects.answered_pre_checkout_queries().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn vip_invoice_control_job_creates_link_and_sends_go_button_message()
    -> Result<(), Box<dyn Error>> {
        let effects = EffectsStub::default().with_next_invoice_url("https://t.me/invoice-vip");

        let report =
            execute_vip_invoice_control_job(&effects, &sample_invoice_job(ControlKind::VipInvoice))
                .await;

        assert_eq!(report.outcome, InvoiceControlJobOutcome::InvoiceSent);
        assert_eq!(
            effects.subscription_invoice_requests(),
            vec![openplotva_telegram::SubscriptionInvoiceLinkRequest {
                user_id: 42,
                user_name: "alice".to_owned(),
                amount_stars: 300,
            }]
        );
        assert!(effects.donation_invoice_requests().is_empty());
        assert_eq!(
            effects.invoice_messages(),
            vec![InvoiceButtonMessage {
                chat_id: 1000,
                text: expected_subscription_invoice_message_text(),
                button_text: "💎 Оформить VIP подписку".to_owned(),
                url: "https://t.me/invoice-vip".to_owned(),
                parse_mode: "HTML".to_owned(),
            }]
        );
        assert_eq!(
            subscription_invoice_message_text(),
            expected_subscription_invoice_message_text()
        );
        assert!(effects.invoice_error_texts().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn vip_invoice_control_job_reports_missing_user_without_invoice_request()
    -> Result<(), Box<dyn Error>> {
        let effects = EffectsStub::default().with_next_invoice_url("https://t.me/unused");
        let mut params = sample_invoice_job(ControlKind::VipInvoice);
        params.user_id = 0;

        let report = execute_vip_invoice_control_job(&effects, &params).await;

        assert_eq!(report.outcome, InvoiceControlJobOutcome::MissingUser);
        assert!(effects.subscription_invoice_requests().is_empty());
        assert_eq!(
            effects.invoice_error_texts(),
            vec![(
                1000,
                555,
                "❌ Не удалось определить пользователя.".to_owned(),
                "HTML".to_owned()
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn vip_invoice_control_job_sends_go_error_on_empty_invoice_link()
    -> Result<(), Box<dyn Error>> {
        let effects = EffectsStub::default().with_next_invoice_url("   ");

        let report =
            execute_vip_invoice_control_job(&effects, &sample_invoice_job(ControlKind::VipInvoice))
                .await;

        assert_eq!(report.outcome, InvoiceControlJobOutcome::EmptyInvoiceLink);
        assert_eq!(
            effects.invoice_error_texts(),
            vec![(
                1000,
                555,
                "❌ Не удалось создать счет для подписки. Пожалуйста, попробуйте позже.".to_owned(),
                "HTML".to_owned()
            )]
        );
        assert!(effects.invoice_messages().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn donation_invoice_control_job_creates_link_and_sends_go_button_message()
    -> Result<(), Box<dyn Error>> {
        let effects = EffectsStub::default().with_next_invoice_url("https://t.me/invoice-donation");
        let mut params = sample_invoice_job(ControlKind::DonateInvoice);
        params.data.amount = 777;

        let report = execute_donate_invoice_control_job(&effects, &params).await;

        assert_eq!(report.outcome, InvoiceControlJobOutcome::InvoiceSent);
        assert_eq!(
            effects.donation_invoice_requests(),
            vec![openplotva_telegram::DonationInvoiceLinkRequest {
                user_id: 42,
                amount_stars: 777,
            }]
        );
        assert_eq!(
            effects.invoice_messages(),
            vec![InvoiceButtonMessage {
                chat_id: 1000,
                text: "💖 Нажмите на кнопку ниже, чтобы поддержать разработчика!\n\n\
                      Ваша поддержка:\n\
                      ❤️ Согревает сердце разработчика\n\
                      🚀 Помогает развивать бота\n\
                      ✨ Вдохновляет на создание новых функций\n\n\
                      <i>Чтобы установить другую сумму - используйте формат <code>/donate XXX</code>, где <code>XXX</code> - сумма в звездах.</i>"
                    .to_owned(),
                button_text: "🌟 Отправить донат 777 звезд?".to_owned(),
                url: "https://t.me/invoice-donation".to_owned(),
                parse_mode: "HTML".to_owned(),
            }]
        );
        assert!(effects.invoice_error_texts().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_control_job_dispatches_vip_invoice_to_invoice_executor()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let store = StoreStub::new();
        let effects = EffectsStub::default().with_next_invoice_url("https://t.me/invoice-vip");

        let report = execute_payment_control_job_at(
            &store,
            &effects,
            &sample_invoice_job(ControlKind::VipInvoice),
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            PaymentControlJobOutcome::VipInvoice(InvoiceControlJobOutcome::InvoiceSent)
        );
        assert_eq!(
            effects.subscription_invoice_requests(),
            vec![openplotva_telegram::SubscriptionInvoiceLinkRequest {
                user_id: 42,
                user_name: "alice".to_owned(),
                amount_stars: 300,
            }]
        );
        assert!(effects.donation_invoice_requests().is_empty());
        assert!(store.users().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_control_job_dispatches_successful_payment_to_existing_processor()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let expires_at = now + time::Duration::days(30);
        let store = StoreStub::new().with_next_vip_event(VipEventRecord {
            id: 77,
            user_id: 42,
            event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
            delta_seconds: vip_days_to_seconds(30),
            effective_expires_at: expires_at,
            subscription_id: Some(10),
            actor_user_id: None,
            reason: "payment telegram-charge".to_owned(),
            created_at: now,
        });
        let effects = EffectsStub::default();

        let report = execute_payment_control_job_at(
            &store,
            &effects,
            &sample_successful_payment_control_job(),
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            PaymentControlJobOutcome::SuccessfulPayment(
                SuccessfulPaymentOutcome::SubscriptionProcessed
            )
        );
        assert_eq!(
            store.users(),
            vec![UserState::new(
                42,
                "Alice",
                Some("Smith".to_owned()),
                Some("alice".to_owned()),
                Some("en".to_owned()),
                Some(true),
            )]
        );
        assert_eq!(
            store.subscriptions(),
            vec![SubscriptionCreate {
                user_id: 42,
                telegram_payment_charge_id: "telegram-charge",
                provider_payment_charge_id: "provider-charge",
                expires_at,
            }]
        );
        assert_eq!(
            effects.sent_texts(),
            vec![subscription_success_text(expires_at)]
        );
        assert_eq!(effects.invalidated_vip_users(), vec![42]);
        Ok(())
    }

    #[test]
    fn successful_payment_control_job_message_uses_full_name_when_first_name_missing() {
        let mut params = sample_successful_payment_control_job();
        params.user_full_name = "Alice Smith".to_owned();
        params.data.first_name.clear();

        let message = successful_payment_message_from_control_job(&params)
            .expect("payment control job should build a message");

        assert_eq!(message.user.first_name, "Alice Smith");
        assert_eq!(message.payment.invoice_payload, "subscription_42");
    }

    #[tokio::test]
    async fn payment_control_job_skips_non_payment_kind() -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let store = StoreStub::new();
        let effects = EffectsStub::default().with_next_invoice_url("https://t.me/unused");

        let report = execute_payment_control_job_at(
            &store,
            &effects,
            &sample_invoice_job(ControlKind::Translate),
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            PaymentControlJobOutcome::NotPaymentControlJob
        );
        assert!(effects.subscription_invoice_requests().is_empty());
        assert!(effects.donation_invoice_requests().is_empty());
        assert!(effects.invoice_messages().is_empty());
        assert!(store.users().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_control_job_worker_dequeues_executes_and_completes_invoice_job()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = PaymentControlJobWorkerQueueStub::default().with_next_job(sample_work_item(
            7,
            sample_invoice_job(ControlKind::VipInvoice),
            now,
        ));
        let store = StoreStub::new();
        let effects = EffectsStub::default().with_next_invoice_url("https://t.me/invoice-vip");

        let report = process_payment_control_job_once_at(&queue, &store, &effects, now).await;

        assert!(report.dequeued);
        assert_eq!(report.job_id, Some(7));
        assert!(report.completed);
        assert!(!report.failed);
        assert_eq!(
            report.execution.as_ref().map(|report| &report.outcome),
            Some(&PaymentControlJobOutcome::VipInvoice(
                InvoiceControlJobOutcome::InvoiceSent
            ))
        );
        assert_eq!(queue.completed_ids(), vec![7]);
        assert!(queue.failed().is_empty());
        assert_eq!(
            effects.subscription_invoice_requests(),
            vec![openplotva_telegram::SubscriptionInvoiceLinkRequest {
                user_id: 42,
                user_name: "alice".to_owned(),
                amount_stars: 300,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn payment_control_job_worker_marks_invoice_executor_errors_failed()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = PaymentControlJobWorkerQueueStub::default().with_next_job(sample_work_item(
            7,
            sample_invoice_job(ControlKind::DonateInvoice),
            now,
        ));
        let store = StoreStub::new();
        let effects = EffectsStub::default().with_next_invoice_url("   ");

        let report = process_payment_control_job_once_at(&queue, &store, &effects, now).await;

        assert!(report.failed);
        assert!(!report.completed);
        assert_eq!(
            report.execution.as_ref().map(|report| &report.outcome),
            Some(&PaymentControlJobOutcome::DonateInvoice(
                InvoiceControlJobOutcome::EmptyInvoiceLink
            ))
        );
        assert_eq!(
            queue.failed(),
            vec![(7, "failed to parse donation invoice link".to_owned())]
        );
        assert!(queue.completed_ids().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_control_job_worker_fails_unsupported_control_kind()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = PaymentControlJobWorkerQueueStub::default().with_next_job(sample_work_item(
            7,
            sample_invoice_job(ControlKind::Translate),
            now,
        ));
        let store = StoreStub::new();
        let effects = EffectsStub::default().with_next_invoice_url("https://t.me/unused");

        let report = process_payment_control_job_once_at(&queue, &store, &effects, now).await;

        assert!(report.failed);
        assert!(!report.completed);
        assert_eq!(
            report.execution.as_ref().map(|report| &report.outcome),
            Some(&PaymentControlJobOutcome::NotPaymentControlJob)
        );
        assert_eq!(
            queue.failed(),
            vec![(7, "unsupported payment control job kind".to_owned())]
        );
        assert!(effects.subscription_invoice_requests().is_empty());
        assert!(effects.donation_invoice_requests().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn payment_control_job_worker_fails_malformed_control_payload_without_execution()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let mut item = sample_work_item(7, sample_invoice_job(ControlKind::VipInvoice), now);
        item.job.data = JobPayload {
            job_type: JobType::Control,
            telegram_data: item.job.data.telegram_data.take(),
            control_data: None,
        };
        let queue = PaymentControlJobWorkerQueueStub::default().with_next_job(item);
        let store = StoreStub::new();
        let effects = EffectsStub::default().with_next_invoice_url("https://t.me/unused");

        let report = process_payment_control_job_once_at(&queue, &store, &effects, now).await;

        assert!(report.failed);
        assert!(report.execution.is_none());
        assert_eq!(
            report.decode_error.as_deref(),
            Some("missing control data for control job")
        );
        assert_eq!(
            queue.failed(),
            vec![(7, "missing control data for control job".to_owned())]
        );
        assert!(effects.subscription_invoice_requests().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn in_memory_payment_control_job_queue_dequeues_go_priority_then_fifo_order()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = super::InMemoryPaymentControlJobQueue::new();
        let low_old = new_control_job_at(sample_invoice_job(ControlKind::DonateInvoice), created)
            .with_name("low old")
            .with_priority(DEFAULT_PRIORITY);
        let high_new = new_control_job_at(
            sample_invoice_job(ControlKind::SuccessfulPayment),
            created + time::Duration::seconds(2),
        )
        .with_name("high new")
        .with_priority(HIGH_PRIORITY);
        let high_old = new_control_job_at(
            sample_invoice_job(ControlKind::VipInvoice),
            created + time::Duration::seconds(1),
        )
        .with_name("high old")
        .with_priority(HIGH_PRIORITY);

        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, low_old)
            .await?;
        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, high_new)
            .await?;
        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, high_old)
            .await?;

        let first = queue
            .dequeue_payment_control_job(CONTROL_QUEUE_NAME)
            .await?
            .expect("first job");
        let second = queue
            .dequeue_payment_control_job(CONTROL_QUEUE_NAME)
            .await?
            .expect("second job");
        let third = queue
            .dequeue_payment_control_job(CONTROL_QUEUE_NAME)
            .await?
            .expect("third job");

        assert_eq!(first.job.title, "high old");
        assert_eq!(second.job.title, "high new");
        assert_eq!(third.job.title, "low old");
        assert!(
            queue
                .dequeue_payment_control_job(CONTROL_QUEUE_NAME)
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn in_memory_payment_control_job_queue_records_completion_and_failure_status()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = super::InMemoryPaymentControlJobQueue::new();
        let complete_job = new_control_job_at(sample_invoice_job(ControlKind::VipInvoice), created)
            .with_name("complete")
            .with_priority(HIGH_PRIORITY);
        let fail_job = new_control_job_at(
            sample_invoice_job(ControlKind::DonateInvoice),
            created + time::Duration::seconds(1),
        )
        .with_name("fail")
        .with_priority(HIGH_PRIORITY);

        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, complete_job)
            .await?;
        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, fail_job)
            .await?;

        let complete_item = queue
            .dequeue_payment_control_job(CONTROL_QUEUE_NAME)
            .await?
            .expect("complete item");
        let fail_item = queue
            .dequeue_payment_control_job(CONTROL_QUEUE_NAME)
            .await?
            .expect("fail item");
        let report = PaymentControlJobReport::new(PaymentControlJobOutcome::VipInvoice(
            InvoiceControlJobOutcome::InvoiceSent,
        ));

        queue
            .complete_payment_control_job(complete_item.id, &report)
            .await?;
        queue
            .fail_payment_control_job(fail_item.id, "invoice failed")
            .await?;

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(
            snapshot[0].status,
            super::InMemoryPaymentControlJobStatus::Completed
        );
        assert_eq!(snapshot[0].completed_report, Some(report));
        assert_eq!(snapshot[0].error, None);
        assert_eq!(
            snapshot[1].status,
            super::InMemoryPaymentControlJobStatus::Failed
        );
        assert_eq!(snapshot[1].completed_report, None);
        assert_eq!(snapshot[1].error.as_deref(), Some("invoice failed"));
        Ok(())
    }

    #[tokio::test]
    async fn in_memory_payment_control_job_queue_snapshot_uses_rust_native_json_and_restores_state()
    -> Result<(), Box<dyn Error>> {
        let queue = super::InMemoryPaymentControlJobQueue::new();
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let low_old = new_control_job_at(sample_invoice_job(ControlKind::DonateInvoice), created)
            .with_name("donate invoice");
        let high_new = new_control_job_at(
            sample_invoice_job(ControlKind::SuccessfulPayment),
            created + time::Duration::seconds(1),
        )
        .with_name("successful payment")
        .with_priority(HIGH_PRIORITY);

        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, low_old)
            .await?;
        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, high_new)
            .await?;
        let processing = queue
            .dequeue_payment_control_job(CONTROL_QUEUE_NAME)
            .await?
            .expect("high-priority job");
        assert_eq!(processing.id, 2);
        let report =
            PaymentControlJobReport::new(super::PaymentControlJobOutcome::SuccessfulPayment(
                SuccessfulPaymentOutcome::SubscriptionProcessed,
            ));
        queue
            .complete_payment_control_job(processing.id, &report)
            .await?;

        let snapshot = queue.persistence_snapshot();
        let encoded = encode_payment_control_job_queue_snapshot(&snapshot)?;
        let encoded_text = std::str::from_utf8(&encoded)?;
        assert!(encoded_text.starts_with('{'));
        assert!(encoded_text.contains(super::PAYMENT_CONTROL_JOB_QUEUE_SNAPSHOT_FORMAT));

        let decoded = super::decode_payment_control_job_queue_snapshot(&encoded)?;
        let restored = super::InMemoryPaymentControlJobQueue::from_persistence_snapshot(decoded);
        assert_eq!(restored.snapshot(), queue.snapshot());

        let next = new_control_job_at(sample_invoice_job(ControlKind::VipInvoice), created)
            .with_name("vip invoice");
        restored
            .assign_payment_control_job(CONTROL_QUEUE_NAME, next)
            .await?;
        assert_eq!(
            restored
                .snapshot()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let next_pending = restored
            .dequeue_payment_control_job(CONTROL_QUEUE_NAME)
            .await?
            .expect("old pending job survives restore");
        assert_eq!(next_pending.id, 1);

        let wrong_format = serde_json::to_vec(&json!({
            "format": "go-gob",
            "next_id": 1,
            "records": []
        }))?;
        assert!(matches!(
            super::decode_payment_control_job_queue_snapshot(&wrong_format),
            Err(super::PaymentControlJobQueueSnapshotError::UnsupportedFormat(format))
                if format == "go-gob"
        ));
        Ok(())
    }

    #[test]
    fn invoice_button_send_message_method_matches_go_direct_chattable_payload()
    -> Result<(), Box<dyn Error>> {
        let method = invoice_button_send_message_method(&InvoiceButtonMessage {
            chat_id: 1000,
            text: expected_subscription_invoice_message_text(),
            button_text: "💎 Оформить VIP подписку".to_owned(),
            url: "https://t.me/invoice-vip".to_owned(),
            parse_mode: "HTML".to_owned(),
        })?;

        let payload = serde_json::to_value(method)?;
        assert_eq!(payload["chat_id"], json!(1000));
        assert_eq!(
            payload["text"],
            json!(expected_subscription_invoice_message_text())
        );
        assert_eq!(payload["parse_mode"], json!("HTML"));
        assert!(payload.get("reply_parameters").is_none());
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!("💎 Оформить VIP подписку")
        );
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["url"],
            json!("https://t.me/invoice-vip")
        );
        Ok(())
    }

    #[test]
    fn telegram_client_implements_payment_invoice_effects() {
        fn assert_impl<T: PaymentInvoiceEffects>() {}

        assert_impl::<openplotva_telegram::TelegramClient>();
    }

    fn sample_message(payload: &str, total_amount: i64) -> SuccessfulPaymentMessage {
        SuccessfulPaymentMessage {
            chat_id: 42,
            message_id: 100,
            user: UserState::new(
                42,
                "Alice",
                Some("Smith".to_owned()),
                Some("alice".to_owned()),
                Some("en".to_owned()),
                Some(true),
            ),
            payment: SuccessfulPayment {
                currency: "XTR".to_owned(),
                total_amount,
                invoice_payload: payload.to_owned(),
                telegram_payment_charge_id: "telegram-charge".to_owned(),
                provider_payment_charge_id: "provider-charge".to_owned(),
            },
        }
    }

    fn sample_invoice_job(kind: ControlKind) -> ControlJobParams {
        ControlJobParams {
            chat_id: 1000,
            message_id: 555,
            user_id: 42,
            user_full_name: "Alice Smith".to_owned(),
            thread_id: None,
            data: ControlJobData {
                kind,
                amount: 300,
                user_name: "alice".to_owned(),
                first_name: "Alice".to_owned(),
                last_name: "Smith".to_owned(),
                language_code: "en".to_owned(),
                is_premium: true,
                chat_type: "private".to_owned(),
                ..ControlJobData::default()
            },
        }
    }

    fn sample_successful_payment_control_job() -> ControlJobParams {
        let mut params = sample_invoice_job(ControlKind::SuccessfulPayment);
        params.chat_id = 42;
        params.message_id = 100;
        params.data.payment = Some(ControlPayment {
            currency: "XTR".to_owned(),
            total_amount: 300,
            invoice_payload: "subscription_42".to_owned(),
            telegram_payment_charge_id: "telegram-charge".to_owned(),
            provider_payment_charge_id: "provider-charge".to_owned(),
        });
        params
    }

    fn sample_work_item(
        id: i64,
        params: ControlJobParams,
        created: OffsetDateTime,
    ) -> PaymentControlJobWorkItem {
        PaymentControlJobWorkItem {
            id,
            job: new_control_job_at(params, created),
        }
    }

    fn expected_subscription_invoice_message_text() -> String {
        "✨ Нажмите на кнопку ниже, чтобы оформить VIP подписку на <b>30 дней</b> и получить:\n\n\
         ⚡ Приоритетное выполнение запросов на рисование вне очереди\n\
         🔄 Вдвое увеличенные лимиты на рисование\n\
         🖼️ Изображения лучшего качества с меньшей цензурой\n\
         🎵 Генерация песен по теме через /song (с поддержкой audio reference)\n\
         🚀 Ранний доступ к будущим улучшениям и фичам"
            .to_owned()
    }

    fn sample_successful_payment_update(
        payload: &str,
        total_amount: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        sample_successful_payment_update_json(payload, total_amount, None)
    }

    fn sample_successful_payment_update_with_thread(
        payload: &str,
        total_amount: i64,
        thread_id: i32,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        sample_successful_payment_update_json(payload, total_amount, Some(thread_id))
    }

    fn sample_successful_payment_update_json(
        payload: &str,
        total_amount: i64,
        thread_id: Option<i32>,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let mut message = json!({
            "message_id": 100,
            "date": 1_710_000_000,
            "chat": {
                "id": 42,
                "type": "private",
                "first_name": "Alice",
                "username": "alice"
            },
            "from": {
                "id": 42,
                "is_bot": false,
                "first_name": "Alice",
                "last_name": "Smith",
                "username": "alice",
                "language_code": "en",
                "is_premium": true
            },
            "successful_payment": {
                "currency": "XTR",
                "total_amount": total_amount,
                "invoice_payload": payload,
                "telegram_payment_charge_id": "telegram-charge",
                "provider_payment_charge_id": "provider-charge"
            }
        });
        if let Some(thread_id) = thread_id {
            message["message_thread_id"] = json!(thread_id);
        }
        serde_json::from_value(json!({
            "update_id": 999,
            "message": message
        }))
    }

    fn sample_text_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 999,
            "message": {
                "message_id": 100,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Alice",
                    "username": "alice"
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Alice",
                    "username": "alice"
                },
                "text": "hello"
            }
        }))
    }

    fn sample_payment_command_update(text: &str) -> Result<TelegramUpdate, serde_json::Error> {
        sample_payment_command_update_with_username(text, "alice")
    }

    fn sample_payment_command_update_with_username(
        text: &str,
        username: &str,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let command_len = text.split_whitespace().next().map_or(text.len(), str::len);
        serde_json::from_value(json!({
            "update_id": 999,
            "message": {
                "message_id": 100,
                "message_thread_id": 77,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Alice",
                    "username": username
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Alice",
                    "last_name": "Smith",
                    "username": username,
                    "language_code": "en",
                    "is_premium": true
                },
                "text": text,
                "entities": [{
                    "offset": 0,
                    "length": command_len,
                    "type": "bot_command"
                }]
            }
        }))
    }

    fn sample_group_payment_command_update(
        text: &str,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let command_len = text.split_whitespace().next().map_or(text.len(), str::len);
        serde_json::from_value(json!({
            "update_id": 999,
            "message": {
                "message_id": 555,
                "message_thread_id": 77,
                "date": 1_710_000_000,
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Group"
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Alice",
                    "last_name": "Smith",
                    "username": "alice",
                    "language_code": "en",
                    "is_premium": true
                },
                "text": text,
                "entities": [{
                    "offset": 0,
                    "length": command_len,
                    "type": "bot_command"
                }]
            }
        }))
    }

    fn sample_pre_checkout_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 999,
            "pre_checkout_query": {
                "id": "pre-checkout-id",
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Alice",
                    "username": "alice"
                },
                "currency": "XTR",
                "total_amount": 300,
                "invoice_payload": "subscription_42"
            }
        }))
    }

    fn active_vip_summary(
        created_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> VipSummaryRecord {
        VipSummaryRecord {
            latest_event_id: 77,
            user_id: 42,
            latest_event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
            latest_delta_seconds: vip_days_to_seconds(30),
            effective_expires_at: expires_at,
            is_active: true,
            remaining_seconds: (expires_at - created_at).whole_seconds(),
            latest_subscription_id: Some(10),
            latest_actor_user_id: None,
            latest_reason: "payment telegram-charge".to_owned(),
            latest_created_at: created_at,
        }
    }

    fn active_subscription(
        created_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> SubscriptionRecord {
        SubscriptionRecord {
            id: 10,
            user_id: 42,
            telegram_payment_charge_id: "telegram-charge".to_owned(),
            provider_payment_charge_id: String::new(),
            expires_at,
            created_at,
            updated_at: created_at,
            canceled_at: None,
            refunded_at: None,
        }
    }

    fn active_vip_cache(created_at: OffsetDateTime, expires_at: OffsetDateTime) -> VipCacheRecord {
        VipCacheRecord {
            user_id: 42,
            is_vip: true,
            expires_at,
            created_at,
            updated_at: created_at,
        }
    }

    #[derive(Clone, Default)]
    struct VipStatusStoreStub {
        state: Arc<Mutex<VipStatusStoreState>>,
    }

    #[derive(Default)]
    struct VipStatusStoreState {
        is_vip: bool,
        summary: Option<VipSummaryRecord>,
        active_subscription: Option<SubscriptionRecord>,
        cache: Option<VipCacheRecord>,
        calls: Vec<&'static str>,
    }

    impl VipStatusStoreStub {
        fn new() -> Self {
            Self::default()
        }

        fn with_is_vip(self, is_vip: bool) -> Self {
            self.lock().is_vip = is_vip;
            self
        }

        fn with_summary(self, summary: VipSummaryRecord) -> Self {
            self.lock().summary = Some(summary);
            self
        }

        fn with_active_subscription(self, subscription: SubscriptionRecord) -> Self {
            self.lock().active_subscription = Some(subscription);
            self
        }

        fn with_cache(self, cache: VipCacheRecord) -> Self {
            self.lock().cache = Some(cache);
            self
        }

        fn calls(&self) -> Vec<&'static str> {
            self.lock().calls.clone()
        }

        fn lock(&self) -> MutexGuard<'_, VipStatusStoreState> {
            self.state.lock().expect("VIP status stub lock poisoned")
        }
    }

    impl super::VipStatusChecker for VipStatusStoreStub {
        fn is_vip_at<'a>(
            &'a self,
            _user_id: i64,
            _now: OffsetDateTime,
        ) -> super::VipStatusCheckFuture<'a> {
            Box::pin(async move {
                let mut state = self.lock();
                state.calls.push("is_vip_at");
                state.is_vip
            })
        }
    }

    impl super::VipStatusStore for VipStatusStoreStub {
        type Error = StubError;

        fn get_vip_summary_by_user<'a>(
            &'a self,
            _user_id: i64,
        ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.calls.push("get_vip_summary_by_user");
                Ok(state.summary.clone())
            })
        }

        fn get_active_subscription<'a>(
            &'a self,
            _user_id: i64,
        ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.calls.push("get_active_subscription");
                Ok(state.active_subscription.clone())
            })
        }

        fn get_vip_cache<'a>(
            &'a self,
            _user_id: i64,
        ) -> PaymentStoreFuture<'a, Option<VipCacheRecord>, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.calls.push("get_vip_cache");
                Ok(state.cache.clone())
            })
        }
    }

    #[derive(Clone, Default)]
    struct StoreStub {
        state: Arc<Mutex<StoreState>>,
    }

    #[derive(Default)]
    struct StoreState {
        users: Vec<UserState>,
        subscriptions: Vec<SubscriptionCreate<'static>>,
        donations: Vec<DonationCreate<'static>>,
        vip_events: Vec<VipEventCreate<'static>>,
        next_subscription_id: i64,
        next_subscription_error: Option<StubError>,
        existing_subscription: Option<SubscriptionRecord>,
        next_donations: VecDeque<DonationRecord>,
        next_vip_events: VecDeque<VipEventRecord>,
    }

    impl StoreStub {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(StoreState {
                    next_subscription_id: 10,
                    ..StoreState::default()
                })),
            }
        }

        fn with_next_donation(self, donation: DonationRecord) -> Self {
            self.lock().next_donations.push_back(donation);
            self
        }

        fn with_next_vip_event(self, event: VipEventRecord) -> Self {
            self.lock().next_vip_events.push_back(event);
            self
        }

        fn with_next_subscription_error(self, error: StubError) -> Self {
            self.lock().next_subscription_error = Some(error);
            self
        }

        fn with_existing_subscription(self, subscription: SubscriptionRecord) -> Self {
            self.lock().existing_subscription = Some(subscription);
            self
        }

        fn users(&self) -> Vec<UserState> {
            self.lock().users.clone()
        }

        fn subscriptions(&self) -> Vec<SubscriptionCreate<'static>> {
            self.lock().subscriptions.clone()
        }

        fn donations(&self) -> Vec<DonationCreate<'static>> {
            self.lock().donations.clone()
        }

        fn vip_events(&self) -> Vec<VipEventCreate<'static>> {
            self.lock().vip_events.clone()
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
            user: &'a UserState,
        ) -> PaymentStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.lock().users.push(user.clone());
                Ok(())
            })
        }

        fn create_subscription<'a>(
            &'a self,
            subscription: SubscriptionCreate<'a>,
        ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.subscriptions.push(subscription.into_owned());
                if let Some(error) = state.next_subscription_error.take() {
                    return Err(error);
                }
                let id = state.next_subscription_id;
                state.next_subscription_id += 1;
                Ok(SubscriptionRecord {
                    id,
                    user_id: subscription.user_id,
                    telegram_payment_charge_id: subscription.telegram_payment_charge_id.to_owned(),
                    provider_payment_charge_id: subscription.provider_payment_charge_id.to_owned(),
                    expires_at: subscription.expires_at,
                    created_at: subscription.expires_at,
                    updated_at: subscription.expires_at,
                    canceled_at: None,
                    refunded_at: None,
                })
            })
        }

        fn get_subscription_by_telegram_payment_charge_id<'a>(
            &'a self,
            _telegram_payment_charge_id: &'a str,
        ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
            Box::pin(async move { Ok(self.lock().existing_subscription.clone()) })
        }

        fn create_donation<'a>(
            &'a self,
            donation: DonationCreate<'a>,
        ) -> PaymentStoreFuture<'a, DonationRecord, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.donations.push(donation.into_owned());
                if let Some(record) = state.next_donations.pop_front() {
                    return Ok(record);
                }
                Ok(DonationRecord {
                    id: 20,
                    user_id: donation.user_id,
                    telegram_payment_charge_id: donation.telegram_payment_charge_id.to_owned(),
                    provider_payment_charge_id: donation.provider_payment_charge_id.to_owned(),
                    amount_stars: donation.amount_stars,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                })
            })
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
                state.vip_events.push(event.into_owned());
                Ok(state.next_vip_events.pop_front().unwrap_or(VipEventRecord {
                    id: 77,
                    user_id: event.user_id,
                    event_type: event.event_type.to_owned(),
                    delta_seconds: event.delta_seconds,
                    effective_expires_at: OffsetDateTime::UNIX_EPOCH,
                    subscription_id: event.subscription_id,
                    actor_user_id: event.actor_user_id,
                    reason: event.reason.unwrap_or_default().to_owned(),
                    created_at: OffsetDateTime::UNIX_EPOCH,
                }))
            })
        }

        fn is_duplicate_insert_no_row(error: &Self::Error) -> bool {
            matches!(error, StubError::Duplicate)
        }
    }

    #[derive(Clone, Default)]
    struct VirtualMessageStoreStub {
        state: Arc<Mutex<VirtualMessageStoreState>>,
    }

    #[derive(Default)]
    struct VirtualMessageStoreState {
        inserted: Vec<(String, i64, Option<i32>)>,
    }

    impl VirtualMessageStoreStub {
        fn inserted(&self) -> Vec<(String, i64, Option<i32>)> {
            self.state
                .lock()
                .expect("virtual message store state")
                .inserted
                .clone()
        }
    }

    impl crate::virtual_messages::VirtualMessageStore for VirtualMessageStoreStub {
        type Error = StubError;

        fn get_mapping_by_virtual<'a>(
            &'a self,
            _vmsg_id: String,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<Option<openplotva_core::MessageIdMapping>, Self::Error>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async { Ok(None) })
        }

        fn insert_virtual_message<'a>(
            &'a self,
            vmsg_id: String,
            chat_id: i64,
            thread_id: Option<i32>,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("virtual message store state")
                    .inserted
                    .push((vmsg_id, chat_id, thread_id));
                Ok(())
            })
        }

        fn resolve_virtual_message<'a>(
            &'a self,
            _vmsg_id: String,
            _real_message_id: i32,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn enqueue_message_op<'a>(
            &'a self,
            _vmsg_id: String,
            _chat_id: i64,
            _op: &'static str,
            _payload_json: Option<String>,
        ) -> Pin<Box<dyn Future<Output = Result<i64, Self::Error>> + Send + 'a>> {
            Box::pin(async { Ok(1) })
        }

        fn delete_mapping_by_virtual<'a>(
            &'a self,
            _vmsg_id: String,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Clone, Default)]
    struct VipCacheInvalidatorStub {
        invalidated_users: Arc<Mutex<Vec<i64>>>,
    }

    impl VipCacheInvalidatorStub {
        fn invalidated_users(&self) -> Vec<i64> {
            self.invalidated_users
                .lock()
                .expect("invalidated users")
                .clone()
        }
    }

    impl super::VipCacheInvalidator for VipCacheInvalidatorStub {
        type Error = StubError;

        fn invalidate_vip_cache<'a>(
            &'a self,
            user_id: i64,
        ) -> super::VipCacheInvalidationFuture<'a, Self::Error> {
            Box::pin(async move {
                self.invalidated_users
                    .lock()
                    .expect("invalidated users")
                    .push(user_id);
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct SentPaymentText {
        chat_id: i64,
        reply_to_message_id: i32,
        text: String,
        parse_mode: String,
    }

    #[derive(Clone, Default)]
    struct EffectsStub {
        state: Arc<Mutex<EffectsState>>,
    }

    #[derive(Default)]
    struct EffectsState {
        sent_texts: Vec<SentPaymentText>,
        invalidated_vip_users: Vec<i64>,
        answered_pre_checkout_queries: Vec<String>,
        next_pre_checkout_error: Option<StubError>,
        subscription_invoice_requests: Vec<openplotva_telegram::SubscriptionInvoiceLinkRequest>,
        donation_invoice_requests: Vec<openplotva_telegram::DonationInvoiceLinkRequest>,
        next_invoice_urls: VecDeque<String>,
        invoice_messages: Vec<InvoiceButtonMessage>,
        invoice_error_texts: Vec<(i64, i32, String, String)>,
        vip_status_messages: Vec<super::VipStatusMessage>,
        vip_status_error_texts: Vec<(i64, i32, String, String)>,
    }

    impl EffectsStub {
        fn with_pre_checkout_error(self, error: StubError) -> Self {
            self.lock().next_pre_checkout_error = Some(error);
            self
        }

        fn with_next_invoice_url(self, invoice_url: &str) -> Self {
            self.lock()
                .next_invoice_urls
                .push_back(invoice_url.to_owned());
            self
        }

        fn sent_texts(&self) -> Vec<String> {
            self.lock()
                .sent_texts
                .iter()
                .map(|message| message.text.clone())
                .collect()
        }

        fn sent_payment_texts(&self) -> Vec<SentPaymentText> {
            self.lock().sent_texts.clone()
        }

        fn invalidated_vip_users(&self) -> Vec<i64> {
            self.lock().invalidated_vip_users.clone()
        }

        fn answered_pre_checkout_queries(&self) -> Vec<String> {
            self.lock().answered_pre_checkout_queries.clone()
        }

        fn subscription_invoice_requests(
            &self,
        ) -> Vec<openplotva_telegram::SubscriptionInvoiceLinkRequest> {
            self.lock().subscription_invoice_requests.clone()
        }

        fn donation_invoice_requests(
            &self,
        ) -> Vec<openplotva_telegram::DonationInvoiceLinkRequest> {
            self.lock().donation_invoice_requests.clone()
        }

        fn invoice_messages(&self) -> Vec<InvoiceButtonMessage> {
            self.lock().invoice_messages.clone()
        }

        fn invoice_error_texts(&self) -> Vec<(i64, i32, String, String)> {
            self.lock().invoice_error_texts.clone()
        }

        fn vip_status_messages(&self) -> Vec<super::VipStatusMessage> {
            self.lock().vip_status_messages.clone()
        }

        fn lock(&self) -> MutexGuard<'_, EffectsState> {
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    impl SuccessfulPaymentEffects for EffectsStub {
        type Error = StubError;

        fn send_text<'a>(
            &'a self,
            message: PaymentTextMessage<'a>,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().sent_texts.push(SentPaymentText {
                    chat_id: message.chat_id,
                    reply_to_message_id: message.reply_to_message_id,
                    text: message.text.to_owned(),
                    parse_mode: message.parse_mode.to_owned(),
                });
                Ok(())
            })
        }

        fn invalidate_vip_cache<'a>(
            &'a self,
            user_id: i64,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().invalidated_vip_users.push(user_id);
                Ok(())
            })
        }
    }

    impl super::VipStatusEffects for EffectsStub {
        type Error = StubError;

        fn send_vip_status_message<'a>(
            &'a self,
            message: super::VipStatusMessage,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().vip_status_messages.push(message);
                Ok(())
            })
        }

        fn send_vip_status_error_text<'a>(
            &'a self,
            chat_id: i64,
            reply_to_message_id: i32,
            text: &'a str,
            parse_mode: &'a str,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().vip_status_error_texts.push((
                    chat_id,
                    reply_to_message_id,
                    text.to_owned(),
                    parse_mode.to_owned(),
                ));
                Ok(())
            })
        }
    }

    impl PaymentInvoiceEffects for EffectsStub {
        type Error = StubError;

        fn create_subscription_invoice_link<'a>(
            &'a self,
            request: &'a openplotva_telegram::SubscriptionInvoiceLinkRequest,
        ) -> PaymentInvoiceLinkFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.subscription_invoice_requests.push(request.clone());
                Ok(state
                    .next_invoice_urls
                    .pop_front()
                    .unwrap_or_else(|| "https://t.me/default-subscription-invoice".to_owned()))
            })
        }

        fn create_donation_invoice_link<'a>(
            &'a self,
            request: &'a openplotva_telegram::DonationInvoiceLinkRequest,
        ) -> PaymentInvoiceLinkFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.donation_invoice_requests.push(request.clone());
                Ok(state
                    .next_invoice_urls
                    .pop_front()
                    .unwrap_or_else(|| "https://t.me/default-donation-invoice".to_owned()))
            })
        }

        fn send_invoice_button_message<'a>(
            &'a self,
            message: InvoiceButtonMessage,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().invoice_messages.push(message);
                Ok(())
            })
        }

        fn send_invoice_error_text<'a>(
            &'a self,
            chat_id: i64,
            reply_to_message_id: i32,
            text: &'a str,
            parse_mode: &'a str,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().invoice_error_texts.push((
                    chat_id,
                    reply_to_message_id,
                    text.to_owned(),
                    parse_mode.to_owned(),
                ));
                Ok(())
            })
        }
    }

    impl PreCheckoutPaymentEffects for EffectsStub {
        type Error = StubError;

        fn answer_pre_checkout_query<'a>(
            &'a self,
            pre_checkout_query_id: &'a str,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state
                    .answered_pre_checkout_queries
                    .push(pre_checkout_query_id.to_owned());
                match state.next_pre_checkout_error.take() {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            })
        }
    }

    #[derive(Clone, Default)]
    struct PaymentControlJobQueueStub {
        state: Arc<Mutex<PaymentControlJobQueueState>>,
    }

    #[derive(Default)]
    struct PaymentControlJobQueueState {
        assigned: Vec<(&'static str, StatelessJobItem)>,
        error: Option<StubError>,
    }

    impl PaymentControlJobQueueStub {
        fn failing(error: StubError) -> Self {
            Self {
                state: Arc::new(Mutex::new(PaymentControlJobQueueState {
                    error: Some(error),
                    ..PaymentControlJobQueueState::default()
                })),
            }
        }

        fn assigned(&self) -> Vec<(&'static str, StatelessJobItem)> {
            self.lock().assigned.clone()
        }

        fn lock(&self) -> MutexGuard<'_, PaymentControlJobQueueState> {
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    impl PaymentControlJobQueue for PaymentControlJobQueueStub {
        type Error = StubError;

        fn assign_payment_control_job<'a>(
            &'a self,
            queue_name: &'static str,
            job: StatelessJobItem,
        ) -> PaymentControlJobQueueFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.assigned.push((queue_name, job));
                match state.error.take() {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            })
        }
    }

    #[derive(Clone, Default)]
    struct UpdateHandlerStub {
        calls: Arc<Mutex<Vec<i64>>>,
    }

    impl UpdateHandlerStub {
        fn calls(&self) -> Vec<i64> {
            self.calls.lock().expect("update handler calls").clone()
        }
    }

    impl UpdateHandler for UpdateHandlerStub {
        type Error = io::Error;

        fn handle_update<'a>(
            &'a self,
            update: TelegramUpdate,
        ) -> UpdateHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(update.id);
                Ok(())
            })
        }
    }

    #[derive(Clone, Default)]
    struct PaymentControlJobWorkerQueueStub {
        state: Arc<Mutex<PaymentControlJobWorkerQueueState>>,
    }

    #[derive(Default)]
    struct PaymentControlJobWorkerQueueState {
        jobs: VecDeque<PaymentControlJobWorkItem>,
        completed: Vec<(i64, PaymentControlJobReport)>,
        failed: Vec<(i64, String)>,
        dequeue_error: Option<StubError>,
        status_error: Option<StubError>,
    }

    impl PaymentControlJobWorkerQueueStub {
        fn with_next_job(self, job: PaymentControlJobWorkItem) -> Self {
            self.lock().jobs.push_back(job);
            self
        }

        fn completed_ids(&self) -> Vec<i64> {
            self.lock()
                .completed
                .iter()
                .map(|(id, _report)| *id)
                .collect()
        }

        fn failed(&self) -> Vec<(i64, String)> {
            self.lock().failed.clone()
        }

        fn lock(&self) -> MutexGuard<'_, PaymentControlJobWorkerQueueState> {
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    impl PaymentControlJobWorkerQueue for PaymentControlJobWorkerQueueStub {
        type Error = StubError;

        fn dequeue_payment_control_job<'a>(
            &'a self,
            _queue_name: &'static str,
        ) -> PaymentControlJobWorkerFuture<'a, Option<PaymentControlJobWorkItem>, Self::Error>
        {
            Box::pin(async move {
                let mut state = self.lock();
                if let Some(error) = state.dequeue_error.take() {
                    return Err(error);
                }
                Ok(state.jobs.pop_front())
            })
        }

        fn complete_payment_control_job<'a>(
            &'a self,
            job_id: i64,
            report: &'a PaymentControlJobReport,
        ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                if let Some(error) = state.status_error.take() {
                    return Err(error);
                }
                state.completed.push((job_id, report.clone()));
                Ok(())
            })
        }

        fn fail_payment_control_job<'a>(
            &'a self,
            job_id: i64,
            error: &'a str,
        ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                if let Some(status_error) = state.status_error.take() {
                    return Err(status_error);
                }
                state.failed.push((job_id, error.to_owned()));
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug)]
    enum StubError {
        Duplicate,
        Request,
    }

    impl fmt::Display for StubError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Duplicate => f.write_str("duplicate"),
                Self::Request => f.write_str("request"),
            }
        }
    }

    impl Error for StubError {}

    trait OwnedSubscriptionCreate {
        fn into_owned(self) -> SubscriptionCreate<'static>;
    }

    impl OwnedSubscriptionCreate for SubscriptionCreate<'_> {
        fn into_owned(self) -> SubscriptionCreate<'static> {
            SubscriptionCreate {
                user_id: self.user_id,
                telegram_payment_charge_id: Box::leak(
                    self.telegram_payment_charge_id.to_owned().into_boxed_str(),
                ),
                provider_payment_charge_id: Box::leak(
                    self.provider_payment_charge_id.to_owned().into_boxed_str(),
                ),
                expires_at: self.expires_at,
            }
        }
    }

    trait OwnedDonationCreate {
        fn into_owned(self) -> DonationCreate<'static>;
    }

    impl OwnedDonationCreate for DonationCreate<'_> {
        fn into_owned(self) -> DonationCreate<'static> {
            DonationCreate {
                user_id: self.user_id,
                telegram_payment_charge_id: Box::leak(
                    self.telegram_payment_charge_id.to_owned().into_boxed_str(),
                ),
                provider_payment_charge_id: Box::leak(
                    self.provider_payment_charge_id.to_owned().into_boxed_str(),
                ),
                amount_stars: self.amount_stars,
            }
        }
    }

    trait OwnedVipEventCreate {
        fn into_owned(self) -> VipEventCreate<'static>;
    }

    impl OwnedVipEventCreate for VipEventCreate<'_> {
        fn into_owned(self) -> VipEventCreate<'static> {
            VipEventCreate {
                user_id: self.user_id,
                event_type: Box::leak(self.event_type.to_owned().into_boxed_str()),
                delta_seconds: self.delta_seconds,
                subscription_id: self.subscription_id,
                actor_user_id: self.actor_user_id,
                reason: self
                    .reason
                    .map(|reason| Box::leak(reason.to_owned().into_boxed_str()) as &'static str),
            }
        }
    }
}
