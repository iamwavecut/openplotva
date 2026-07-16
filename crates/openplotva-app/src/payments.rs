use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    rate_limits::TaskEnqueueRateLimit,
    updates::{UpdateHandler, UpdateHandlerFuture},
    virtual_messages::{
        QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory, queue_text_message_parts,
    },
};
use carapax::types::{
    CallbackQuery as TelegramCallbackQuery, Chat as TelegramChat, ChatMember as TelegramChatMember,
    MaybeInaccessibleMessage, Message as TelegramMessage, MessageData as TelegramMessageData,
    PreCheckoutQuery as TelegramPreCheckoutQuery, ReplyTo as TelegramReplyTo,
    SuccessfulPayment as TelegramSuccessfulPayment, TextEntity as TelegramTextEntity,
    Update as TelegramUpdate, UpdateType as TelegramUpdateType, User as TelegramUser,
};
use openplotva_core::{
    UserState, VIP_EVENT_TYPE_ADMIN_ADJUSTMENT, VIP_EVENT_TYPE_ADMIN_REVOKE,
    VIP_EVENT_TYPE_PAYMENT, VIP_EVENT_TYPE_REFUND_REVERSAL, is_synthetic_subscription_charge_id,
    subscription_delta_seconds, vip_days_to_seconds,
};
use openplotva_storage::{
    DonationCreate, DonationRecord, PostgresPaymentStore, PostgresVipStore,
    PostgresVirtualMessageStore, StorageError, SubscriptionCreate, SubscriptionRecord,
    VipCacheRecord, VipCacheUpsert, VipEventCreate, VipEventRecord, VipSummaryRecord,
};
use openplotva_taskman::{
    CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, ControlPayment,
    InMemoryTaskQueue, JobStatus, JobType, StatelessJobItem, TRANSACTION_PRIORITY,
    TaskQueueIdAllocator, TaskQueueRecord, TaskQueueSchedule,
    control_job_params_from_stateless_job, new_control_job_at,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

/// Fixed cryptocurrency donation destinations shared by every donation-help surface.
pub const CRYPTO_DONATION_ADDRESSES: [(&str, &str); 3] = [
    ("BTC", "bc1qyv0lvyr4g5gnulqszyapuqrdycppt49p26re0v"),
    ("Ethereum EVM", "0x4fc711f4aa1744012b31911c1d5c451026ec2191"),
    ("Solana", "GJdwsdhp8pxEUc5g3mjigcp1BpCAnPuThTmyD3FFSTz1"),
];

/// Render cryptocurrency destinations for Telegram Rich Messages.
#[must_use]
pub fn crypto_donation_rich_html() -> String {
    let rows = CRYPTO_DONATION_ADDRESSES
        .iter()
        .map(|(network, address)| {
            format!("<tr><td>{network}</td><td><code>{address}</code></td></tr>")
        })
        .collect::<String>();
    format!(
        "<h3>Донат в криптовалюте</h3>\n\n\
         <table bordered><tr><th>Сеть</th><th>Адрес</th></tr>{rows}</table>"
    )
}

fn crypto_donation_html() -> String {
    let addresses = CRYPTO_DONATION_ADDRESSES
        .iter()
        .map(|(network, address)| format!("{network}: <code>{address}</code>"))
        .collect::<Vec<_>>()
        .join("\n");
    format!("<b>Донат в криптовалюте:</b>\n{addresses}")
}

fn crypto_donation_plain_text() -> String {
    let addresses = CRYPTO_DONATION_ADDRESSES
        .iter()
        .map(|(network, address)| format!("{network}: {address}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!("Донат в криптовалюте:\n{addresses}")
}

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

/// Boxed future returned by external VIP membership checks.
pub type ExternalVipMembershipFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<bool, E>> + Send + 'a>>;

/// Boxed future returned by VIP cancellation side effects.
pub type VipCancellationEffectFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Telegram successful-payment payload captured from a message/control job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SuccessfulPayment {
    pub currency: String,
    /// Telegram total amount in Stars.
    pub total_amount: i64,
    /// Invoice payload string.
    pub invoice_payload: String,
    /// Telegram charge ID.
    pub telegram_payment_charge_id: String,
    /// Provider-side charge ID.
    pub provider_payment_charge_id: String,
    /// Telegram message/transaction time.
    pub paid_at: Option<OffsetDateTime>,
    /// Paid subscription period in seconds, when Telegram reports it.
    pub subscription_period_seconds: Option<i64>,
    /// Telegram subscription expiry timestamp, when Telegram reports it.
    pub subscription_expiration_date: Option<OffsetDateTime>,
    /// Internal VIP ledger target. Used for database-only compensation on reconciled payments.
    pub vip_target_expires_at: Option<OffsetDateTime>,
    /// Whether this payment is a recurring subscription payment.
    pub is_recurring: Option<bool>,
    /// Whether this payment is the first recurring subscription payment.
    pub is_first_recurring: Option<bool>,
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaymentTextMessage<'value> {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram message ID used as the response anchor.
    pub reply_to_message_id: i32,
    /// Message text.
    pub text: &'value str,
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
    Processed(SuccessfulPaymentOutcome),
    /// The update was not a successful-payment service message and was delegated.
    Delegated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuccessfulPaymentControlJobUpdateRoute {
    Queued,
    QueueError,
    /// The update was not a successful-payment service message and was delegated.
    Delegated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PaymentInvoiceControlJobBuild {
    /// A high-priority payment invoice control job was built.
    Job(Box<StatelessJobItem>),
    /// The update was not a payment invoice command.
    NotPaymentCommand,
    NonPrivateChat,
    MissingUser,
    InvalidDonationAmount,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PaymentInvoiceCommandUpdateRoute {
    Queued,
    QueueError,
    NonPrivateChat,
    /// VIP invoice command did not have a valid non-bot user.
    MissingUser,
    ExistingVipStatusSent,
    ExistingVipStatusSendError,
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
    AcknowledgementError,
}

/// Routing result for a decoded Telegram pre-checkout update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreCheckoutUpdateRoute {
    Processed(PreCheckoutOutcome),
    /// The update was not a pre-checkout query and was delegated.
    Delegated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VipCancellationCallbackOutcome {
    /// Update did not belong to this slice and was delegated.
    Delegated,
    /// Callback matched a VIP cancellation action but did not carry a message to edit.
    MissingMessage,
    /// `cancel_vip` edited the status message into the confirmation prompt.
    ConfirmationShown,
    /// `confirm_cancel_vip` found no active paid subscription or failed to load it.
    MissingSubscription,
    /// Telegram rejected the Stars subscription cancellation request.
    TelegramCancelError,
    Canceled,
    /// `back_to_vip_status` found no active status view and edited the inactive text.
    BackInactive,
    /// `back_to_vip_status` restored the active VIP status view.
    BackStatusShown,
}

/// Fatal errors from the VIP cancellation callback wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum VipCancellationCallbackError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

/// Routing result for the payment-aware decoded-update wrapper used before fetcher routing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PaymentUpdateRoute {
    PreCheckout(PreCheckoutOutcome),
    /// A successful-payment service message was queued as a taskman control job.
    SuccessfulPayment(SuccessfulPaymentControlJobUpdateRoute),
    InvoiceCommand(PaymentInvoiceCommandUpdateRoute),
    /// The update was not payment-owned and was delegated.
    Delegated,
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminGrantVipCommandOutcome {
    /// A VIP ledger admin-adjustment event was created and a success reply was queued.
    Granted,
    GrantedWithDefaultDuration,
    /// Neither an explicit target nor a replied-to user was available.
    MissingTarget,
    /// The explicit target argument could not be parsed or resolved.
    TargetError,
    /// The VIP ledger event could not be written.
    StorageError,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminGrantVipCommandReport {
    /// Observable route/outcome.
    pub outcome: AdminGrantVipCommandOutcome,
    /// Target user affected by the command, when resolved.
    pub target_user_id: Option<i64>,
    /// Created VIP event ID, when an event was written.
    pub event_id: Option<i64>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminCancelVipCommandOutcome {
    /// VIP was revoked through an admin ledger event.
    Revoked,
    /// Telegram Stars payment was refunded and a refund-reversal event was created.
    Refunded,
    /// The target argument was missing or invalid.
    TargetError,
    /// Active VIP summary lookup failed.
    StatusLookupError,
    /// The target user does not have an active VIP status.
    NoActiveVip,
    /// The target user has active VIP, but not an active paid subscription to refund.
    NoPaidSubscription,
    /// Active paid subscription lookup failed.
    PaidSubscriptionLookupError,
    /// Telegram refund, subscription marking, or refund-reversal event creation failed.
    RefundFailed,
    /// The admin-revoke ledger event could not be written.
    RevokeStorageError,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminCancelVipCommandReport {
    /// Observable route/outcome.
    pub outcome: AdminCancelVipCommandOutcome,
    /// Target user affected by the command, when resolved.
    pub target_user_id: Option<i64>,
    /// Created VIP event ID, when an event was written.
    pub event_id: Option<i64>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminRefundCommandOutcome {
    /// No Telegram payment charge ID argument was provided.
    MissingChargeId,
    /// Subscription lookup failed before donation fallback.
    SubscriptionLookupError,
    /// Donation lookup failed after subscription fallback.
    DonationLookupError,
    /// Neither a subscription nor a donation matched the charge ID.
    NotFound,
    /// The stored row did not contain a usable user ID or amount.
    InvalidTarget,
    /// The target subscription is a legacy admin grant, not a Telegram payment.
    SyntheticSubscription,
    /// Telegram refund request failed or returned an unsuccessful result.
    RefundApiError,
    /// A subscription was refunded and post-refund storage effects were attempted.
    RefundedSubscription,
    /// A donation was refunded and its donation row deletion was attempted.
    RefundedDonation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminRefundCommandReport {
    /// Observable route/outcome.
    pub outcome: AdminRefundCommandOutcome,
    /// Telegram charge ID supplied to the command, when present.
    pub telegram_payment_charge_id: Option<String>,
    /// Refunded user ID, when resolved.
    pub target_user_id: Option<i64>,
    /// Refunded amount in Telegram Stars, when resolved.
    pub amount_stars: Option<i64>,
    pub error: Option<String>,
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
    #[error("{0}")]
    RefundRequest(String),
    /// Telegram returned a false refund result.
    #[error("telegram API сообщил, что возврат не был успешен")]
    RefundUnsuccessful,
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

/// Runtime admin-cancel effect failure preserving which side failed.
#[derive(Debug)]
pub enum PaymentRuntimeAdminCancelEffectError<Direct, Successful> {
    /// Direct Telegram payment side failed.
    Direct(Direct),
    /// Dispatcher/cache side failed.
    Successful(Successful),
}

impl<Direct, Successful> fmt::Display for PaymentRuntimeAdminCancelEffectError<Direct, Successful>
where
    Direct: fmt::Display,
    Successful: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct(error) => error.fmt(formatter),
            Self::Successful(error) => error.fmt(formatter),
        }
    }
}

/// Runtime VIP-cancellation effect failure preserving which side failed.
#[derive(Debug)]
pub enum PaymentRuntimeVipCancellationEffectError<Direct, Successful> {
    /// Direct Telegram payment side failed.
    Direct(Direct),
    /// Dispatcher/cache side failed.
    Successful(Successful),
}

impl<Direct, Successful> fmt::Display
    for PaymentRuntimeVipCancellationEffectError<Direct, Successful>
where
    Direct: fmt::Display,
    Successful: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct(error) => error.fmt(formatter),
            Self::Successful(error) => error.fmt(formatter),
        }
    }
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

pub trait SuccessfulPaymentStore {
    /// Error returned by the concrete storage implementation.
    type Error: fmt::Display + Send + Sync + 'static;

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

    /// Load the latest VIP ledger summary for payment activation math.
    fn get_vip_summary_by_user<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error>;

    /// Load an already-created VIP payment event for a subscription row.
    fn get_vip_event_by_subscription_id<'a>(
        &'a self,
        subscription_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipEventRecord>, Self::Error>;

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

pub trait VipCancellationStore: VipStatusStore {
    /// Mark a subscription canceled after Telegram accepts cancellation.
    fn mark_subscription_canceled<'a>(
        &'a self,
        id: i64,
    ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error>;
}

pub trait ExternalVipCacheStore {
    /// Error returned by the concrete storage implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Store the latest external VIP membership result.
    fn upsert_vip_cache<'a>(
        &'a self,
        cache: VipCacheUpsert,
    ) -> PaymentStoreFuture<'a, (), Self::Error>;
}

pub trait ExternalVipMembershipChecker {
    /// Error returned by the concrete membership provider.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Return whether the user is an active member of the configured VIP chat.
    fn is_external_vip_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> ExternalVipMembershipFuture<'a, Self::Error>;
}

pub trait AdminGrantVipStore {
    /// Error returned by the concrete storage implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn get_user_id_by_username<'a>(
        &'a self,
        username: &'a str,
    ) -> PaymentStoreFuture<'a, Option<i64>, Self::Error>;

    /// Create a VIP ledger event.
    fn create_admin_vip_event<'a>(
        &'a self,
        event: VipEventCreate<'a>,
    ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error>;
}

pub trait AdminCancelVipStore {
    /// Error returned by the concrete storage implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn get_user_id_by_username<'a>(
        &'a self,
        username: &'a str,
    ) -> PaymentStoreFuture<'a, Option<i64>, Self::Error>;

    /// Load the latest VIP ledger summary for active-status checks.
    fn get_vip_summary_by_user<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error>;

    /// Create a VIP ledger event.
    fn create_admin_cancel_vip_event<'a>(
        &'a self,
        event: VipEventCreate<'a>,
    ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error>;

    /// Load the active paid subscription that can be refunded through Telegram Stars.
    fn get_active_subscription<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error>;

    /// Mark a subscription refunded after Telegram accepts the refund.
    fn mark_subscription_refunded<'a>(
        &'a self,
        id: i64,
    ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error>;
}

pub trait AdminRefundStore {
    /// Error returned by the concrete storage implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load a subscription by Telegram Stars charge ID.
    fn get_admin_refund_subscription_by_charge_id<'a>(
        &'a self,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error>;

    /// Load a donation by Telegram Stars charge ID.
    fn get_admin_refund_donation_by_charge_id<'a>(
        &'a self,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<DonationRecord>, Self::Error>;

    /// Mark a subscription refunded after Telegram accepts the refund.
    fn mark_admin_refund_subscription_refunded<'a>(
        &'a self,
        id: i64,
    ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error>;

    /// Write the subscription refund-reversal ledger event.
    fn create_admin_refund_vip_event<'a>(
        &'a self,
        event: VipEventCreate<'a>,
    ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error>;

    fn delete_admin_refund_donation<'a>(
        &'a self,
        id: i64,
    ) -> PaymentStoreFuture<'a, DonationRecord, Self::Error>;
}

pub trait VipStatusChecker {
    /// Return whether this user should see existing VIP status instead of a new invoice.
    fn is_vip_at<'a>(&'a self, user_id: i64, now: OffsetDateTime) -> VipStatusCheckFuture<'a>;
}

pub trait SuccessfulPaymentEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_text<'a>(
        &'a self,
        message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Invalidate cached VIP status after subscription activation.
    fn invalidate_vip_cache<'a>(&'a self, user_id: i64) -> PaymentEffectsFuture<'a, Self::Error>;
}

pub trait VipCacheInvalidator {
    /// Error returned by the concrete cache implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn invalidate_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> VipCacheInvalidationFuture<'a, Self::Error>;
}

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

pub trait AdminGrantVipEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_admin_grant_vip_text<'a>(
        &'a self,
        message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Invalidate cached VIP status after a successful admin adjustment.
    fn invalidate_admin_grant_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentEffectsFuture<'a, Self::Error>;
}

impl<T> AdminGrantVipEffects for T
where
    T: SuccessfulPaymentEffects,
{
    type Error = T::Error;

    fn send_admin_grant_vip_text<'a>(
        &'a self,
        message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        self.send_text(message)
    }

    fn invalidate_admin_grant_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        <T as SuccessfulPaymentEffects>::invalidate_vip_cache(self, user_id)
    }
}

pub trait AdminCancelVipEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_admin_cancel_vip_text<'a>(
        &'a self,
        message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Invalidate cached VIP status after a successful cancel/revoke operation.
    fn invalidate_admin_cancel_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Refund the Telegram Stars payment for an active subscription.
    fn refund_admin_cancel_vip_payment<'a>(
        &'a self,
        user_id: i64,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    fn send_admin_cancel_vip_user_text<'a>(
        &'a self,
        user_id: i64,
        text: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;
}

pub trait AdminRefundEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_admin_refund_text<'a>(
        &'a self,
        message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Refund the Telegram Stars payment.
    fn refund_admin_refund_payment<'a>(
        &'a self,
        user_id: i64,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    fn send_admin_refund_user_text<'a>(
        &'a self,
        user_id: i64,
        text: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Invalidate cached VIP status after a subscription refund.
    fn invalidate_admin_refund_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentEffectsFuture<'a, Self::Error>;
}

impl<T> AdminRefundEffects for T
where
    T: AdminCancelVipEffects,
{
    type Error = T::Error;

    fn send_admin_refund_text<'a>(
        &'a self,
        message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        self.send_admin_cancel_vip_text(message)
    }

    fn refund_admin_refund_payment<'a>(
        &'a self,
        user_id: i64,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        self.refund_admin_cancel_vip_payment(user_id, telegram_payment_charge_id)
    }

    fn send_admin_refund_user_text<'a>(
        &'a self,
        user_id: i64,
        text: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        self.send_admin_cancel_vip_user_text(user_id, text)
    }

    fn invalidate_admin_refund_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        self.invalidate_admin_cancel_vip_cache(user_id)
    }
}

/// Direct Telegram payment effects needed by `/admin_cancel_vip` refunds.
pub trait AdminCancelVipDirectEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Refund a Telegram Stars payment.
    fn refund_star_payment<'a>(
        &'a self,
        user_id: i64,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Send a direct text message to the refunded user.
    fn send_direct_payment_text<'a>(
        &'a self,
        user_id: i64,
        text: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;
}

pub trait PreCheckoutPaymentEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Answer a Telegram pre-checkout query with `ok=true`.
    fn answer_pre_checkout_query<'a>(
        &'a self,
        pre_checkout_query_id: &'a str,
    ) -> PreCheckoutFuture<'a, Self::Error>;
}

pub trait VipStatusEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_vip_status_message<'a>(
        &'a self,
        message: VipStatusMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    fn send_vip_status_error_text<'a>(
        &'a self,
        chat_id: i64,
        reply_to_message_id: i32,
        text: &'a str,
        parse_mode: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;
}

pub trait PaymentRedirectEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_payment_redirect_message<'a>(
        &'a self,
        message: PaymentRedirectMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error>;
}

/// Direct Telegram side effects needed by VIP subscription callbacks.
pub trait VipCancellationDirectEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn execute_vip_cancellation_method<'a>(
        &'a self,
        method: openplotva_telegram::TelegramOutboundMethod,
    ) -> VipCancellationEffectFuture<'a, Self::Error>;
}

pub trait VipCancellationEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn execute_vip_cancellation_method<'a>(
        &'a self,
        method: openplotva_telegram::TelegramOutboundMethod,
    ) -> VipCancellationEffectFuture<'a, Self::Error>;

    /// Invalidate cached VIP status after successful cancellation.
    fn invalidate_vip_cancellation_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> VipCancellationEffectFuture<'a, Self::Error>;
}

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

    fn send_invoice_error_text<'a>(
        &'a self,
        chat_id: i64,
        reply_to_message_id: i32,
        text: &'a str,
        parse_mode: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;
}

pub trait PaymentControlJobQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Assign a control job to a named taskman queue.
    fn assign_payment_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> PaymentControlJobQueueFuture<'a, Self::Error>;

    /// Assign or replace a pending control job under a stable debounce key.
    fn schedule_payment_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
        schedule: TaskQueueSchedule,
    ) -> PaymentControlJobQueueFuture<'a, Self::Error> {
        drop(schedule);
        self.assign_payment_control_job(queue_name, job)
    }
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

pub trait SharedControlJobWorkerQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Dequeue the next pending control job owned by one executor family.
    fn dequeue_shared_control_job_matching<'a>(
        &'a self,
        queue_name: &'static str,
        worker_id: &'static str,
        predicate: fn(&StatelessJobItem) -> bool,
    ) -> PaymentControlJobWorkerFuture<'a, Option<PaymentControlJobWorkItem>, Self::Error>;

    /// Mark one shared control job completed.
    fn complete_shared_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error>;

    /// Mark one shared control job failed.
    fn fail_shared_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error>;
}

/// Rust-native in-memory control-job queue for the current payment vertical slice.
///
/// old `job_queue` tables. This adapter gives runtime wiring a concrete queue with the same
/// priority ordering for payment-owned control jobs.
#[derive(Clone, Debug)]
pub struct InMemoryPaymentControlJobQueue {
    queue: InMemoryTaskQueue,
    completed_reports: Arc<Mutex<HashMap<i64, PaymentControlJobReport>>>,
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
    pub queue_name: String,
    /// Current in-memory status.
    pub status: InMemoryPaymentControlJobStatus,
    pub job: StatelessJobItem,
    /// Completion report recorded by the payment worker.
    pub completed_report: Option<PaymentControlJobReport>,
    /// Failure string recorded by the payment worker.
    pub error: Option<String>,
}

/// Error returned by the in-memory payment taskman adapter.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum InMemoryPaymentControlJobQueueError {
    /// A status update targeted a job ID this queue has never assigned.
    #[error("payment control job {0} not found")]
    JobNotFound(i64),
    /// The shared taskman queue core rejected an operation unexpectedly.
    #[error("payment control job queue: {0}")]
    Taskman(String),
}

impl InMemoryPaymentControlJobQueue {
    /// Build an empty in-memory payment control-job queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            queue: InMemoryTaskQueue::new(),
            completed_reports: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build an empty in-memory payment control-job queue with shared taskman IDs.
    #[must_use]
    pub fn new_with_id_allocator(ids: TaskQueueIdAllocator) -> Self {
        Self {
            queue: InMemoryTaskQueue::new_with_id_allocator(ids),
            completed_reports: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[must_use]
    pub fn from_task_queue(queue: InMemoryTaskQueue) -> Self {
        Self {
            queue,
            completed_reports: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Return a stable ID-ordered snapshot of the queue state.
    #[must_use]
    pub fn snapshot(&self) -> Vec<InMemoryPaymentControlJobRecord> {
        let records = self.queue.records();
        let completed_reports = self.completed_reports();
        payment_records_from_taskman(&records, &completed_reports)
    }

    /// Return raw taskman records with worker IDs and timestamps for runtime diagnostics.
    #[must_use]
    pub fn taskman_records(&self) -> Vec<TaskQueueRecord> {
        self.queue.records()
    }

    /// Cancel one raw taskman control job for admin/runtime control surfaces.
    pub fn cancel_taskman_job(
        &self,
        job_id: i64,
    ) -> Result<(), InMemoryPaymentControlJobQueueError> {
        self.queue
            .cancel(job_id, "cancelled by admin", OffsetDateTime::now_utc())
            .map_err(in_memory_payment_queue_error)?;
        self.completed_reports().remove(&job_id);
        Ok(())
    }

    /// Delete one raw taskman control job for admin/runtime control surfaces.
    pub fn delete_taskman_job(
        &self,
        job_id: i64,
    ) -> Result<TaskQueueRecord, InMemoryPaymentControlJobQueueError> {
        let record = self
            .queue
            .delete(job_id)
            .map_err(in_memory_payment_queue_error)?;
        self.completed_reports().remove(&job_id);
        Ok(record)
    }

    /// Restart one raw taskman control job for admin/runtime control surfaces.
    pub fn restart_taskman_job(
        &self,
        job_id: i64,
    ) -> Result<i64, InMemoryPaymentControlJobQueueError> {
        self.queue
            .restart(job_id, OffsetDateTime::now_utc())
            .map_err(in_memory_payment_queue_error)
    }

    fn completed_reports(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<i64, PaymentControlJobReport>> {
        self.completed_reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Error returned by the persistent payment taskman adapter.
#[derive(Debug, Error)]
pub enum PersistentPaymentControlJobQueueError {
    /// The inner queue operation failed.
    #[error(transparent)]
    Queue(#[from] InMemoryPaymentControlJobQueueError),
    /// User/chat-originated control job creation was rejected by the task enqueue limiter.
    #[error("task enqueue rate limited")]
    TaskEnqueueRateLimited,
}

/// Payment control-job queue backed by the shared taskman queue.
#[derive(Clone)]
pub struct PersistentPaymentControlJobQueue {
    queue: InMemoryPaymentControlJobQueue,
    task_enqueue_rate_limit: Option<Arc<dyn TaskEnqueueRateLimit>>,
    db_journal: Option<
        crate::task_queue::BufferedTaskQueueJournal<openplotva_storage::PostgresTaskQueueStore>,
    >,
}

impl PersistentPaymentControlJobQueue {
    #[must_use]
    pub fn from_task_queue(queue: InMemoryTaskQueue) -> Self {
        Self {
            queue: InMemoryPaymentControlJobQueue::from_task_queue(queue),
            task_enqueue_rate_limit: None,
            db_journal: None,
        }
    }

    #[must_use]
    pub fn with_task_enqueue_rate_limit(
        mut self,
        rate_limit: Arc<dyn TaskEnqueueRateLimit>,
    ) -> Self {
        self.task_enqueue_rate_limit = Some(rate_limit);
        self
    }

    /// Attach the shared Postgres journal so control/payment enqueues are flushed
    /// to Postgres synchronously. Payment jobs (e.g. SuccessfulPayment) must be
    /// durable before the update is acked: a lost enqueue would silently drop money.
    #[must_use]
    pub fn with_db_journal(
        mut self,
        journal: crate::task_queue::BufferedTaskQueueJournal<
            openplotva_storage::PostgresTaskQueueStore,
        >,
    ) -> Self {
        self.db_journal = Some(journal);
        self
    }

    /// Return a stable ID-ordered snapshot of the queue state.
    #[must_use]
    pub fn snapshot(&self) -> Vec<InMemoryPaymentControlJobRecord> {
        self.queue.snapshot()
    }

    /// Return raw taskman records with worker IDs and timestamps for runtime diagnostics.
    #[must_use]
    pub fn taskman_records(&self) -> Vec<TaskQueueRecord> {
        self.queue.taskman_records()
    }

    /// Cancel one raw taskman control job and persist the queue snapshot.
    pub fn cancel_taskman_job(
        &self,
        job_id: i64,
    ) -> Result<(), PersistentPaymentControlJobQueueError> {
        self.queue.cancel_taskman_job(job_id)?;
        Ok(())
    }

    /// Delete one raw taskman control job and persist the queue snapshot.
    pub fn delete_taskman_job(
        &self,
        job_id: i64,
    ) -> Result<TaskQueueRecord, PersistentPaymentControlJobQueueError> {
        let record = self.queue.delete_taskman_job(job_id)?;
        Ok(record)
    }

    /// Restart one raw taskman control job and persist the queue snapshot.
    pub fn restart_taskman_job(
        &self,
        job_id: i64,
    ) -> Result<i64, PersistentPaymentControlJobQueueError> {
        let new_id = self.queue.restart_taskman_job(job_id)?;
        Ok(new_id)
    }
}

/// Synchronous-flush retry budget for payment-flow control jobs before falling back
/// to the background sync worker.
const PAYMENT_CONTROL_FLUSH_ATTEMPTS: u32 = 3;
const PAYMENT_CONTROL_FLUSH_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);

/// Control kinds that must never be dropped by enqueue rate limiting.
const fn control_kind_skips_task_enqueue_rate_limit(kind: ControlKind) -> bool {
    matches!(
        kind,
        ControlKind::SuccessfulPayment
            | ControlKind::VipInvoice
            | ControlKind::DonateInvoice
            | ControlKind::ChatAdminsSync
            | ControlKind::ChatMemberSync
    )
}

impl PersistentPaymentControlJobQueue {
    async fn assign_payment_control_job_with_schedule(
        &self,
        queue_name: &'static str,
        job: StatelessJobItem,
        schedule: Option<TaskQueueSchedule>,
    ) -> Result<(), PersistentPaymentControlJobQueueError> {
        let params = control_job_params_from_stateless_job(&job)
            .map_err(|error| InMemoryPaymentControlJobQueueError::Taskman(error.to_string()))?;
        let chat_id = params.chat_id;
        let user_id = params.user_id;
        let control_kind = params.data.kind;
        // Payment and member-state control jobs are exempt from enqueue throttling:
        // dropping them would silently lose durable payment intent or Telegram state.
        // They are rare control-plane events, not the dialog burst the limiter targets.
        let rate_limit = if control_kind_skips_task_enqueue_rate_limit(control_kind) {
            None
        } else {
            self.task_enqueue_rate_limit.as_ref()
        };
        if let Some(rate_limit) = rate_limit {
            let now = OffsetDateTime::now_utc();
            let report = rate_limit
                .check_task_enqueue_rate_limit(chat_id, user_id, now)
                .await;
            if report.limited {
                tracing::warn!(
                    queue_name,
                    chat_id,
                    user_id,
                    control_kind = ?control_kind,
                    rate_limit_scope = ?report.scope,
                    rate_limit_error = ?report.error,
                    "task enqueue rate limit rejected control job"
                );
                return Err(PersistentPaymentControlJobQueueError::TaskEnqueueRateLimited);
            }
            if let Some(error) = report.error {
                tracing::warn!(
                    queue_name,
                    chat_id,
                    user_id,
                    control_kind = ?control_kind,
                    error,
                    "task enqueue rate limit store check failed before control job assignment"
                );
            }
        }
        if let Some(schedule) = schedule {
            self.queue
                .schedule_payment_control_job(queue_name, job, schedule)
                .await?;
        } else {
            self.queue
                .assign_payment_control_job(queue_name, job)
                .await?;
        }
        // Best-effort synchronous durability for the payment-flow job: retry the
        // flush a few times to ride out a transient Postgres blip so the job is
        // durable before the update is acked. If every attempt fails, the 1s
        // background sync worker remains the durability backstop (a crash inside
        // that residual window would re-deliver the update, which is idempotent).
        if let Some(journal) = &self.db_journal {
            let mut last_error = None;
            for attempt in 0..PAYMENT_CONTROL_FLUSH_ATTEMPTS {
                match journal.flush_dirty().await {
                    Ok(_) => {
                        last_error = None;
                        break;
                    }
                    Err(error) => {
                        last_error = Some(error);
                        if attempt + 1 < PAYMENT_CONTROL_FLUSH_ATTEMPTS {
                            tokio::time::sleep(PAYMENT_CONTROL_FLUSH_RETRY_DELAY).await;
                        }
                    }
                }
            }
            if let Some(error) = last_error {
                tracing::error!(
                    queue_name,
                    control_kind = ?control_kind,
                    %error,
                    "control job not durably flushed after retries; relying on background sync worker"
                );
            }
        }
        if let Some(rate_limit) = rate_limit {
            let report = rate_limit
                .grow_task_enqueue_rate_limit(chat_id, user_id, OffsetDateTime::now_utc())
                .await;
            for error in report.save_errors {
                tracing::warn!(
                    queue_name,
                    chat_id,
                    user_id,
                    control_kind = ?control_kind,
                    error,
                    "task enqueue rate limit store save failed after control job assignment"
                );
            }
        }
        Ok(())
    }
}

impl PaymentControlJobQueue for PersistentPaymentControlJobQueue {
    type Error = PersistentPaymentControlJobQueueError;

    fn assign_payment_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> PaymentControlJobQueueFuture<'a, Self::Error> {
        Box::pin(self.assign_payment_control_job_with_schedule(queue_name, job, None))
    }

    fn schedule_payment_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
        schedule: TaskQueueSchedule,
    ) -> PaymentControlJobQueueFuture<'a, Self::Error> {
        Box::pin(self.assign_payment_control_job_with_schedule(queue_name, job, Some(schedule)))
    }
}

impl PaymentControlJobWorkerQueue for PersistentPaymentControlJobQueue {
    type Error = PersistentPaymentControlJobQueueError;

    fn dequeue_payment_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> PaymentControlJobWorkerFuture<'a, Option<PaymentControlJobWorkItem>, Self::Error> {
        self.dequeue_shared_control_job_matching(
            queue_name,
            "payment-control",
            is_payment_control_job,
        )
    }

    fn complete_payment_control_job<'a>(
        &'a self,
        job_id: i64,
        report: &'a PaymentControlJobReport,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.queue
                .complete_payment_control_job(job_id, report)
                .await?;
            Ok(())
        })
    }

    fn fail_payment_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
        self.fail_shared_control_job(job_id, error)
    }
}

impl SharedControlJobWorkerQueue for PersistentPaymentControlJobQueue {
    type Error = PersistentPaymentControlJobQueueError;

    fn dequeue_shared_control_job_matching<'a>(
        &'a self,
        queue_name: &'static str,
        worker_id: &'static str,
        predicate: fn(&StatelessJobItem) -> bool,
    ) -> PaymentControlJobWorkerFuture<'a, Option<PaymentControlJobWorkItem>, Self::Error> {
        Box::pin(async move {
            self.queue
                .dequeue_shared_control_job_matching(queue_name, worker_id, predicate)
                .await
                .map_err(Into::into)
        })
    }

    fn complete_shared_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.queue.complete_shared_control_job(job_id).await?;
            Ok(())
        })
    }

    fn fail_shared_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.queue.fail_shared_control_job(job_id, error).await?;
            Ok(())
        })
    }
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
            self.queue.assign(queue_name, job);
            Ok(())
        })
    }

    fn schedule_payment_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
        schedule: TaskQueueSchedule,
    ) -> PaymentControlJobQueueFuture<'a, Self::Error> {
        Box::pin(async move {
            self.queue.schedule_or_replace(queue_name, job, schedule);
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
        self.dequeue_shared_control_job_matching(
            queue_name,
            "payment-control",
            is_payment_control_job,
        )
    }

    fn complete_payment_control_job<'a>(
        &'a self,
        job_id: i64,
        report: &'a PaymentControlJobReport,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.queue
                .complete(job_id, OffsetDateTime::now_utc())
                .map_err(in_memory_payment_queue_error)?;
            self.completed_reports().insert(job_id, report.clone());
            Ok(())
        })
    }

    fn fail_payment_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.queue
                .fail(job_id, error, OffsetDateTime::now_utc())
                .map_err(in_memory_payment_queue_error)?;
            self.completed_reports().remove(&job_id);
            Ok(())
        })
    }
}

impl SharedControlJobWorkerQueue for InMemoryPaymentControlJobQueue {
    type Error = InMemoryPaymentControlJobQueueError;

    fn dequeue_shared_control_job_matching<'a>(
        &'a self,
        queue_name: &'static str,
        worker_id: &'static str,
        predicate: fn(&StatelessJobItem) -> bool,
    ) -> PaymentControlJobWorkerFuture<'a, Option<PaymentControlJobWorkItem>, Self::Error> {
        Box::pin(async move {
            Ok(self
                .queue
                .dequeue_matching(queue_name, worker_id, OffsetDateTime::now_utc(), predicate)
                .map(|item| PaymentControlJobWorkItem {
                    id: item.id,
                    job: item.job,
                }))
        })
    }

    fn complete_shared_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.queue
                .complete(job_id, OffsetDateTime::now_utc())
                .map_err(in_memory_payment_queue_error)?;
            self.completed_reports().remove(&job_id);
            Ok(())
        })
    }

    fn fail_shared_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> PaymentControlJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.queue
                .fail(job_id, error, OffsetDateTime::now_utc())
                .map_err(in_memory_payment_queue_error)?;
            self.completed_reports().remove(&job_id);
            Ok(())
        })
    }
}

fn in_memory_payment_queue_error(
    error: openplotva_taskman::TaskQueueError,
) -> InMemoryPaymentControlJobQueueError {
    match error {
        openplotva_taskman::TaskQueueError::JobNotFound(job_id) => {
            InMemoryPaymentControlJobQueueError::JobNotFound(job_id)
        }
        error => InMemoryPaymentControlJobQueueError::Taskman(error.to_string()),
    }
}

fn is_payment_control_job(job: &StatelessJobItem) -> bool {
    if job.data.job_type != JobType::Control {
        return false;
    }
    matches!(
        job.data.control_data.as_ref().map(|data| data.kind),
        Some(ControlKind::VipInvoice | ControlKind::DonateInvoice | ControlKind::SuccessfulPayment)
    )
}

fn payment_records_from_taskman(
    records: &[TaskQueueRecord],
    completed_reports: &HashMap<i64, PaymentControlJobReport>,
) -> Vec<InMemoryPaymentControlJobRecord> {
    records
        .iter()
        .map(|record| InMemoryPaymentControlJobRecord {
            id: record.id,
            queue_name: record.queue_name.clone(),
            status: payment_status_from_taskman(record.status),
            job: record.job.clone(),
            completed_report: completed_reports.get(&record.id).cloned(),
            error: record.error.clone(),
        })
        .collect()
}

fn payment_status_from_taskman(status: JobStatus) -> InMemoryPaymentControlJobStatus {
    match status {
        JobStatus::Pending => InMemoryPaymentControlJobStatus::Pending,
        JobStatus::Processing | JobStatus::WaitingDelivery => {
            InMemoryPaymentControlJobStatus::Processing
        }
        JobStatus::Completed => InMemoryPaymentControlJobStatus::Completed,
        JobStatus::Failed | JobStatus::Cancelled => InMemoryPaymentControlJobStatus::Failed,
    }
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

impl AdminCancelVipDirectEffects for openplotva_telegram::TelegramClient {
    type Error = TelegramPaymentInvoiceEffectError;

    fn refund_star_payment<'a>(
        &'a self,
        user_id: i64,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            let refunded = self
                .execute(openplotva_telegram::build_refund_star_payment_method(
                    user_id,
                    telegram_payment_charge_id.to_owned(),
                ))
                .await
                .map_err(|error| {
                    TelegramPaymentInvoiceEffectError::RefundRequest(format!(
                        "Ошибка при возврате средств: {error}"
                    ))
                })?;
            if refunded {
                Ok(())
            } else {
                Err(TelegramPaymentInvoiceEffectError::RefundUnsuccessful)
            }
        })
    }

    fn send_direct_payment_text<'a>(
        &'a self,
        user_id: i64,
        text: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            let _message: carapax::types::Message = self
                .execute(openplotva_telegram::SendMessage::new(
                    user_id,
                    text.to_owned(),
                ))
                .await?;
            Ok(())
        })
    }
}

impl VipCancellationDirectEffects for openplotva_telegram::TelegramClient {
    type Error = TelegramPaymentInvoiceEffectError;

    fn execute_vip_cancellation_method<'a>(
        &'a self,
        method: openplotva_telegram::TelegramOutboundMethod,
    ) -> VipCancellationEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            method.execute_with(self).await?;
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

impl PaymentRedirectEffects for openplotva_telegram::TelegramClient {
    type Error = TelegramPaymentInvoiceEffectError;

    fn send_payment_redirect_message<'a>(
        &'a self,
        message: PaymentRedirectMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            let method = payment_redirect_send_message_method(&message)?;
            let _message: carapax::types::Message = self.execute(method).await?;
            Ok(())
        })
    }
}

/// Generator for virtual-message IDs used by successful-payment text replies.
pub type PaymentVirtualIdFactory = VirtualIdFactory;

#[derive(Clone)]
pub struct SuccessfulPaymentDispatcherEffects<Invalidator = NoopVipCacheInvalidator> {
    queue: Arc<openplotva_telegram::DispatcherQueue>,
    invalidator: Invalidator,
    next_virtual_id: PaymentVirtualIdFactory,
}

impl<Invalidator> SuccessfulPaymentDispatcherEffects<Invalidator> {
    /// Build successful-payment effects backed by the normal outbound dispatcher.
    pub fn new(queue: Arc<openplotva_telegram::DispatcherQueue>, invalidator: Invalidator) -> Self {
        Self {
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

impl<Invalidator> SuccessfulPaymentEffects for SuccessfulPaymentDispatcherEffects<Invalidator>
where
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
                &self.queue,
                crate::virtual_messages::QueueTextRequest {
                    protected: false,
                    debounce_key: None,
                    message: &request,
                    reply_to: Some(&reply_to),
                    immediate_first: false,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: None,
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

/// Runtime payment side effects composed from direct Telegram invoice effects and dispatcher-backed
/// successful-payment effects.
#[derive(Clone)]
pub struct PaymentRuntimeEffects<Invoice, Successful> {
    invoice: Invoice,
    successful: Successful,
}

impl<Invoice, Successful> PaymentRuntimeEffects<Invoice, Successful> {
    /// Build a payment runtime effects adapter from the two concrete effect boundaries.
    #[must_use]
    pub fn new(invoice: Invoice, successful: Successful) -> Self {
        Self {
            invoice,
            successful,
        }
    }
}

impl<Invoice, Successful> PaymentInvoiceEffects for PaymentRuntimeEffects<Invoice, Successful>
where
    Invoice: PaymentInvoiceEffects + Sync,
    Successful: Sync,
{
    type Error = Invoice::Error;

    fn create_subscription_invoice_link<'a>(
        &'a self,
        request: &'a openplotva_telegram::SubscriptionInvoiceLinkRequest,
    ) -> PaymentInvoiceLinkFuture<'a, Self::Error> {
        self.invoice.create_subscription_invoice_link(request)
    }

    fn create_donation_invoice_link<'a>(
        &'a self,
        request: &'a openplotva_telegram::DonationInvoiceLinkRequest,
    ) -> PaymentInvoiceLinkFuture<'a, Self::Error> {
        self.invoice.create_donation_invoice_link(request)
    }

    fn send_invoice_button_message<'a>(
        &'a self,
        message: InvoiceButtonMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        self.invoice.send_invoice_button_message(message)
    }

    fn send_invoice_error_text<'a>(
        &'a self,
        chat_id: i64,
        reply_to_message_id: i32,
        text: &'a str,
        parse_mode: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        self.invoice
            .send_invoice_error_text(chat_id, reply_to_message_id, text, parse_mode)
    }
}

impl<Invoice, Successful> SuccessfulPaymentEffects for PaymentRuntimeEffects<Invoice, Successful>
where
    Invoice: Sync,
    Successful: SuccessfulPaymentEffects + Sync,
{
    type Error = Successful::Error;

    fn send_text<'a>(
        &'a self,
        message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        self.successful.send_text(message)
    }

    fn invalidate_vip_cache<'a>(&'a self, user_id: i64) -> PaymentEffectsFuture<'a, Self::Error> {
        self.successful.invalidate_vip_cache(user_id)
    }
}

impl<Direct, Successful> AdminCancelVipEffects for PaymentRuntimeEffects<Direct, Successful>
where
    Direct: AdminCancelVipDirectEffects + Sync,
    Successful: SuccessfulPaymentEffects + Sync,
{
    type Error = PaymentRuntimeAdminCancelEffectError<Direct::Error, Successful::Error>;

    fn send_admin_cancel_vip_text<'a>(
        &'a self,
        message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            self.successful
                .send_text(message)
                .await
                .map_err(PaymentRuntimeAdminCancelEffectError::Successful)
        })
    }

    fn invalidate_admin_cancel_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            self.successful
                .invalidate_vip_cache(user_id)
                .await
                .map_err(PaymentRuntimeAdminCancelEffectError::Successful)
        })
    }

    fn refund_admin_cancel_vip_payment<'a>(
        &'a self,
        user_id: i64,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            self.invoice
                .refund_star_payment(user_id, telegram_payment_charge_id)
                .await
                .map_err(PaymentRuntimeAdminCancelEffectError::Direct)
        })
    }

    fn send_admin_cancel_vip_user_text<'a>(
        &'a self,
        user_id: i64,
        text: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            self.invoice
                .send_direct_payment_text(user_id, text)
                .await
                .map_err(PaymentRuntimeAdminCancelEffectError::Direct)
        })
    }
}

impl<Direct, Successful> VipCancellationEffects for PaymentRuntimeEffects<Direct, Successful>
where
    Direct: VipCancellationDirectEffects + Sync,
    Successful: SuccessfulPaymentEffects + Sync,
{
    type Error = PaymentRuntimeVipCancellationEffectError<Direct::Error, Successful::Error>;

    fn execute_vip_cancellation_method<'a>(
        &'a self,
        method: openplotva_telegram::TelegramOutboundMethod,
    ) -> VipCancellationEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            self.invoice
                .execute_vip_cancellation_method(method)
                .await
                .map_err(PaymentRuntimeVipCancellationEffectError::Direct)
        })
    }

    fn invalidate_vip_cancellation_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> VipCancellationEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            self.successful
                .invalidate_vip_cache(user_id)
                .await
                .map_err(PaymentRuntimeVipCancellationEffectError::Successful)
        })
    }
}

fn monotonic_payment_virtual_id_factory() -> PaymentVirtualIdFactory {
    monotonic_virtual_id_factory("payment-vmsg")
}

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
        text: format!(
            "💖 Для отправки доната, пожалуйста, перейдите в личные сообщения с ботом.\n\n\
             Ваша поддержка:\n\
             ❤️ Согреет сердце разработчика\n\
             🚀 Поможет развивать бота\n\
             ✨ Добавит мотивацию на создание новых функций\n\n{}",
            crypto_donation_plain_text()
        ),
        button_text: "💝 Сделать донат".to_owned(),
        url: format!("https://t.me/{bot_username}?start={start_param}"),
    }
}

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
    if bot_username.is_empty() || command.target != Some(bot_username) {
        return None;
    }
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
        allow_sending_without_reply: Some(true),
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

fn non_private_payment_command_targets_other_bot(
    update: &TelegramUpdate,
    bot_username: &str,
) -> bool {
    let TelegramUpdateType::Message(message) = &update.update_type else {
        return false;
    };
    if matches!(message.chat, TelegramChat::Private(_)) {
        return false;
    }
    let Some(command) = payment_command_from_message(message) else {
        return false;
    };
    matches!(command.name, "vip" | "donate")
        && (bot_username.is_empty() || command.target != Some(bot_username))
}

#[must_use]
/// Payment effects that send the VIP status reply as a rich message while delegating
/// pre-checkout answers and payment-redirect sends to the classic Telegram client.
pub struct RichPaymentEffects {
    inner: openplotva_telegram::TelegramClient,
    rich: Arc<dyn crate::rich::RichSender>,
}

impl RichPaymentEffects {
    pub fn new(
        inner: openplotva_telegram::TelegramClient,
        rich: Arc<dyn crate::rich::RichSender>,
    ) -> Self {
        Self { inner, rich }
    }
}

impl PreCheckoutPaymentEffects for RichPaymentEffects {
    type Error = String;

    fn answer_pre_checkout_query<'a>(
        &'a self,
        pre_checkout_query_id: &'a str,
    ) -> PreCheckoutFuture<'a, Self::Error> {
        Box::pin(async move {
            self.inner
                .answer_pre_checkout_query(pre_checkout_query_id)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

impl PaymentRedirectEffects for RichPaymentEffects {
    type Error = String;

    fn send_payment_redirect_message<'a>(
        &'a self,
        message: PaymentRedirectMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            self.inner
                .send_payment_redirect_message(message)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

impl VipStatusEffects for RichPaymentEffects {
    type Error = String;

    fn send_vip_status_message<'a>(
        &'a self,
        message: VipStatusMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        Box::pin(async move {
            let reply_markup = message.show_cancel_button.then(|| {
                serde_json::json!({
                    "inline_keyboard": [[{
                        "text": "Отменить подписку",
                        "callback_data": "{\"action\":\"cancel_vip\"}"
                    }]]
                })
            });
            let options = openplotva_telegram::RichSendOptions {
                message_thread_id: message.message_thread_id.map(i64::from),
                reply_to_message_id: Some(i64::from(message.reply_to_message_id)),
                allow_sending_without_reply: true,
                disable_notification: false,
                reply_markup,
            };
            let html = vip_status_rich_html(&message.text);
            self.rich
                .send_rich(message.chat_id, &html, &options)
                .await
                .map_err(|error| error.to_string())?;
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
            self.inner
                .send_vip_status_error_text(chat_id, reply_to_message_id, text, parse_mode)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

fn vip_status_rich_html(text: &str) -> String {
    let mut html = String::new();
    for (index, block) in text
        .split("\n\n")
        .map(str::trim)
        .filter(|block| !block.is_empty())
        .enumerate()
    {
        if index > 0 {
            html.push_str("\n\n");
        }
        if index == 0 {
            html.push_str("<h3>");
            html.push_str(block);
            html.push_str("</h3>");
        } else if let Some(items) = block.strip_prefix("Вы получаете:") {
            html.push_str("<h3>Вы получаете</h3>\n\n<ul>");
            for item in items.lines().map(str::trim).filter(|item| !item.is_empty()) {
                html.push_str("<li>");
                html.push_str(item);
                html.push_str("</li>");
            }
            html.push_str("</ul>");
        } else {
            html.push_str("<p>");
            html.push_str(&block.replace('\n', "<br/>"));
            html.push_str("</p>");
        }
    }
    html
}

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

#[derive(Clone, Debug)]
pub struct VipCancellationCallbackUpdateHandler<Store, Effects, Next> {
    store: Arc<Store>,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Store, Effects, Next> VipCancellationCallbackUpdateHandler<Store, Effects, Next> {
    /// Build a VIP callback handler around the real downstream update handler.
    #[must_use]
    pub fn new(store: Arc<Store>, effects: Arc<Effects>, next: Arc<Next>) -> Self {
        Self {
            store,
            effects,
            next,
        }
    }
}

impl<Store, Effects, Next> UpdateHandler
    for VipCancellationCallbackUpdateHandler<Store, Effects, Next>
where
    Store: VipCancellationStore + Send + Sync,
    Effects: VipCancellationEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = VipCancellationCallbackError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_vip_cancellation_callback_update_or_else_at(
                self.store.as_ref(),
                self.effects.as_ref(),
                update,
                OffsetDateTime::now_utc(),
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_vip_cancellation_callback_update_or_else_at<
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
) -> Result<VipCancellationCallbackOutcome, VipCancellationCallbackError>
where
    Store: VipCancellationStore + Sync,
    Effects: VipCancellationEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let TelegramUpdateType::CallbackQuery(query) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| VipCancellationCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(VipCancellationCallbackOutcome::Delegated);
    };

    let Some(action) = vip_cancellation_callback_action(query) else {
        handle_other(update)
            .await
            .map_err(|error| VipCancellationCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(VipCancellationCallbackOutcome::Delegated);
    };

    Ok(handle_vip_cancellation_callback_at(store, effects, query, action, now).await)
}

pub async fn handle_vip_cancellation_callback_at<Store, Effects>(
    store: &Store,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    action: &str,
    now: OffsetDateTime,
) -> VipCancellationCallbackOutcome
where
    Store: VipCancellationStore + Sync,
    Effects: VipCancellationEffects + Sync,
{
    let Some(message) = vip_callback_message(query) else {
        return VipCancellationCallbackOutcome::MissingMessage;
    };

    match action {
        "cancel_vip" => handle_cancel_vip_callback(effects, query, message).await,
        "confirm_cancel_vip" => {
            handle_confirm_cancel_vip_callback_at(store, effects, query, message, now).await
        }
        "back_to_vip_status" => {
            handle_back_to_vip_status_callback_at(store, effects, query, message, now).await
        }
        _ => VipCancellationCallbackOutcome::Delegated,
    }
}

async fn handle_cancel_vip_callback<Effects>(
    effects: &Effects,
    query: &TelegramCallbackQuery,
    message: VipCallbackMessage,
) -> VipCancellationCallbackOutcome
where
    Effects: VipCancellationEffects + Sync,
{
    let keyboard = openplotva_telegram::build_inline_keyboard_markup([
        openplotva_telegram::build_inline_keyboard_row([
            openplotva_telegram::build_inline_keyboard_button_data(
                "Да, отменить",
                "{\"action\":\"confirm_cancel_vip\"}",
            ),
            openplotva_telegram::build_inline_keyboard_button_data(
                "Нет, вернуться",
                "{\"action\":\"back_to_vip_status\"}",
            ),
        ]),
    ]);
    try_edit_vip_callback_text(
        effects,
        message,
        "Вы уверены, что хотите отменить вашу VIP подписку? 😢",
        openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
        Some(keyboard),
        "VIP cancel confirmation edit",
    )
    .await;
    try_answer_vip_callback(
        effects,
        &query.id,
        "",
        false,
        0,
        "VIP cancel confirmation ack",
    )
    .await;
    VipCancellationCallbackOutcome::ConfirmationShown
}

async fn handle_confirm_cancel_vip_callback_at<Store, Effects>(
    store: &Store,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    message: VipCallbackMessage,
    now: OffsetDateTime,
) -> VipCancellationCallbackOutcome
where
    Store: VipCancellationStore + Sync,
    Effects: VipCancellationEffects + Sync,
{
    let user_id = i64::from(query.from.id);
    let subscription = match store.get_active_subscription(user_id).await {
        Ok(Some(subscription)) => subscription,
        Ok(None) => {
            try_answer_vip_callback(
                effects,
                &query.id,
                "Не удалось отменить подписку. Не найдена активная подписка.",
                true,
                0,
                "VIP missing subscription ack",
            )
            .await;
            return VipCancellationCallbackOutcome::MissingSubscription;
        }
        Err(error) => {
            tracing::warn!(
                message = %error,
                user_id,
                "failed to load active subscription for VIP cancellation"
            );
            try_answer_vip_callback(
                effects,
                &query.id,
                "Не удалось отменить подписку. Не найдена активная подписка.",
                true,
                0,
                "VIP missing subscription ack",
            )
            .await;
            return VipCancellationCallbackOutcome::MissingSubscription;
        }
    };

    let cancel = openplotva_telegram::build_cancel_star_subscription_method(
        user_id,
        subscription.telegram_payment_charge_id.clone(),
    );
    if let Err(error) = effects.execute_vip_cancellation_method(cancel.into()).await {
        tracing::warn!(
            message = %error,
            user_id,
            subscription_id = subscription.id,
            telegram_payment_charge_id = %subscription.telegram_payment_charge_id,
            "failed to cancel VIP subscription on Telegram side"
        );
        try_answer_vip_callback(
            effects,
            &query.id,
            "Не удалось отменить подписку на стороне Telegram. Пожалуйста, попробуйте позже или обратитесь в поддержку.",
            true,
            0,
            "VIP Telegram cancellation failure ack",
        )
        .await;
        return VipCancellationCallbackOutcome::TelegramCancelError;
    }

    if let Err(error) = store.mark_subscription_canceled(subscription.id).await {
        tracing::warn!(
            message = %error,
            user_id,
            subscription_id = subscription.id,
            "failed to mark VIP subscription canceled after Telegram cancellation"
        );
    }
    if let Err(error) = effects.invalidate_vip_cancellation_cache(user_id).await {
        tracing::warn!(message = %error, user_id, "failed to invalidate VIP cache after cancellation");
    }

    let text = vip_cancellation_success_text_at(store, user_id, now).await;
    try_edit_vip_callback_text(
        effects,
        message,
        &text,
        openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
        Some(openplotva_telegram::InlineKeyboardMarkup::default()),
        "VIP cancellation success edit",
    )
    .await;
    try_answer_vip_callback(
        effects,
        &query.id,
        "Подписка отменена.",
        false,
        0,
        "VIP cancellation success ack",
    )
    .await;
    VipCancellationCallbackOutcome::Canceled
}

async fn handle_back_to_vip_status_callback_at<Store, Effects>(
    store: &Store,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    message: VipCallbackMessage,
    now: OffsetDateTime,
) -> VipCancellationCallbackOutcome
where
    Store: VipCancellationStore + Sync,
    Effects: VipCancellationEffects + Sync,
{
    let user_id = i64::from(query.from.id);
    let status = match build_vip_status_message_at(
        store,
        user_id,
        message.chat.id,
        message.message_id as i32,
        None,
        now,
    )
    .await
    {
        Ok(Some(status)) => status,
        Ok(None) => {
            edit_inactive_vip_status(effects, query, message).await;
            return VipCancellationCallbackOutcome::BackInactive;
        }
        Err(error) => {
            tracing::warn!(
                message = %error,
                user_id,
                "failed to build VIP status for back callback"
            );
            edit_inactive_vip_status(effects, query, message).await;
            return VipCancellationCallbackOutcome::BackInactive;
        }
    };

    try_edit_vip_callback_text(
        effects,
        message,
        &status.text,
        openplotva_telegram::TELEGRAM_PARSE_MODE_HTML,
        status.show_cancel_button.then(vip_cancel_status_keyboard),
        "VIP status back edit",
    )
    .await;
    try_answer_vip_callback(effects, &query.id, "", false, 0, "VIP status back ack").await;
    VipCancellationCallbackOutcome::BackStatusShown
}

async fn edit_inactive_vip_status<Effects>(
    effects: &Effects,
    query: &TelegramCallbackQuery,
    message: VipCallbackMessage,
) where
    Effects: VipCancellationEffects + Sync,
{
    try_edit_vip_callback_text(
        effects,
        message,
        "Ваша подписка больше не активна. Отправьте /vip, чтобы подписаться снова.",
        "",
        None,
        "VIP inactive back edit",
    )
    .await;
    try_answer_vip_callback(effects, &query.id, "", false, 0, "VIP inactive back ack").await;
}

async fn vip_cancellation_success_text_at<Store>(
    store: &Store,
    user_id: i64,
    now: OffsetDateTime,
) -> String
where
    Store: VipCancellationStore + Sync,
{
    let text = "✅ Автопродление VIP подписки отключено.";
    match store.get_vip_summary_by_user(user_id).await {
        Ok(Some(summary)) if summary.is_active && now < summary.effective_expires_at => format!(
            "{text}\n\nВаш VIP статус остаётся активным до <b>{}</b> ({} дней осталось).",
            go_date(summary.effective_expires_at),
            vip_display_days_left_at(summary.effective_expires_at, now),
        ),
        Ok(_) => text.to_owned(),
        Err(error) => {
            tracing::warn!(message = %error, user_id, "failed to load VIP summary after cancellation");
            text.to_owned()
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct VipCallbackMessage {
    chat: openplotva_telegram::ChatRef,
    message_id: i64,
}

fn vip_callback_message(query: &TelegramCallbackQuery) -> Option<VipCallbackMessage> {
    match query.message.as_ref()? {
        MaybeInaccessibleMessage::Message(message) => Some(VipCallbackMessage {
            chat: openplotva_telegram::ChatRef {
                id: message.chat.get_id().into(),
                is_forum: message.message_thread_id.is_some(),
            },
            message_id: message.id,
        }),
        MaybeInaccessibleMessage::InaccessibleMessage(message) => Some(VipCallbackMessage {
            chat: openplotva_telegram::ChatRef {
                id: message.chat.get_id().into(),
                is_forum: false,
            },
            message_id: message.message_id,
        }),
    }
}

fn vip_cancellation_callback_action(query: &TelegramCallbackQuery) -> Option<&str> {
    let openplotva_telegram::CallbackActionParse::Action { action, .. } =
        openplotva_telegram::parse_callback_action(query.data.as_deref().unwrap_or_default())
    else {
        return None;
    };
    match action.as_str() {
        "cancel_vip" => Some("cancel_vip"),
        "confirm_cancel_vip" => Some("confirm_cancel_vip"),
        "back_to_vip_status" => Some("back_to_vip_status"),
        _ => None,
    }
}

fn vip_cancel_status_keyboard() -> openplotva_telegram::InlineKeyboardMarkup {
    openplotva_telegram::build_inline_keyboard_markup([
        openplotva_telegram::build_inline_keyboard_row([
            openplotva_telegram::build_inline_keyboard_button_data(
                "Отменить подписку",
                "{\"action\":\"cancel_vip\"}",
            ),
        ]),
    ])
}

async fn try_edit_vip_callback_text<Effects>(
    effects: &Effects,
    message: VipCallbackMessage,
    text: &str,
    render_as: &str,
    reply_markup: Option<openplotva_telegram::InlineKeyboardMarkup>,
    context: &'static str,
) where
    Effects: VipCancellationEffects + Sync,
{
    match openplotva_telegram::build_edit_text_message_method(
        &openplotva_telegram::EditTextMessageRequest {
            chat: message.chat,
            message_id: message.message_id,
            text: text.to_owned(),
            render_as: render_as.to_owned(),
            reply_markup,
        },
    ) {
        Ok(method) => try_execute_vip_callback_method(effects, method.into(), context).await,
        Err(error) => {
            tracing::warn!(message = %error, context, "failed to build VIP callback edit")
        }
    }
}

async fn try_answer_vip_callback<Effects>(
    effects: &Effects,
    query_id: &str,
    text: &str,
    show_alert: bool,
    cache_time: i64,
    context: &'static str,
) where
    Effects: VipCancellationEffects + Sync,
{
    let request = openplotva_telegram::CallbackAnswerRequest {
        callback_query_id: query_id.to_owned(),
        text: text.to_owned(),
        show_alert,
        url: String::new(),
        cache_time,
    };
    try_execute_vip_callback_method(
        effects,
        openplotva_telegram::build_callback_answer_method(&request).into(),
        context,
    )
    .await;
}

async fn try_execute_vip_callback_method<Effects>(
    effects: &Effects,
    method: openplotva_telegram::TelegramOutboundMethod,
    context: &'static str,
) where
    Effects: VipCancellationEffects + Sync,
{
    let method_name = method.method_name();
    if let Err(error) = effects.execute_vip_cancellation_method(method).await {
        tracing::warn!(
            message = %error,
            method = method_name,
            context,
            "VIP cancellation callback Telegram side effect failed"
        );
    }
}

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

    fn get_vip_summary_by_user<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error> {
        Box::pin(async move { self.vip.get_vip_summary_by_user(user_id).await })
    }

    fn get_vip_event_by_subscription_id<'a>(
        &'a self,
        subscription_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipEventRecord>, Self::Error> {
        Box::pin(async move {
            self.vip
                .get_vip_event_by_subscription_id(subscription_id)
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

impl AdminGrantVipStore for PostgresSuccessfulPaymentStore {
    type Error = StorageError;

    fn get_user_id_by_username<'a>(
        &'a self,
        username: &'a str,
    ) -> PaymentStoreFuture<'a, Option<i64>, Self::Error> {
        Box::pin(async move { self.users.get_user_id_by_username(username).await })
    }

    fn create_admin_vip_event<'a>(
        &'a self,
        event: VipEventCreate<'a>,
    ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error> {
        Box::pin(async move { self.vip.create_vip_event(event).await })
    }
}

impl AdminCancelVipStore for PostgresSuccessfulPaymentStore {
    type Error = StorageError;

    fn get_user_id_by_username<'a>(
        &'a self,
        username: &'a str,
    ) -> PaymentStoreFuture<'a, Option<i64>, Self::Error> {
        Box::pin(async move { self.users.get_user_id_by_username(username).await })
    }

    fn get_vip_summary_by_user<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error> {
        Box::pin(async move { self.vip.get_vip_summary_by_user(user_id).await })
    }

    fn create_admin_cancel_vip_event<'a>(
        &'a self,
        event: VipEventCreate<'a>,
    ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error> {
        Box::pin(async move { self.vip.create_vip_event(event).await })
    }

    fn get_active_subscription<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
        Box::pin(async move { self.payments.get_active_subscription(user_id).await })
    }

    fn mark_subscription_refunded<'a>(
        &'a self,
        id: i64,
    ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error> {
        Box::pin(async move { self.payments.mark_subscription_refunded(id).await })
    }
}

impl AdminRefundStore for PostgresSuccessfulPaymentStore {
    type Error = StorageError;

    fn get_admin_refund_subscription_by_charge_id<'a>(
        &'a self,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
        Box::pin(async move {
            self.payments
                .get_subscription_by_telegram_payment_charge_id(telegram_payment_charge_id)
                .await
        })
    }

    fn get_admin_refund_donation_by_charge_id<'a>(
        &'a self,
        telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<DonationRecord>, Self::Error> {
        Box::pin(async move {
            self.payments
                .get_donation_by_telegram_payment_charge_id(telegram_payment_charge_id)
                .await
        })
    }

    fn mark_admin_refund_subscription_refunded<'a>(
        &'a self,
        id: i64,
    ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error> {
        Box::pin(async move { self.payments.mark_subscription_refunded(id).await })
    }

    fn create_admin_refund_vip_event<'a>(
        &'a self,
        event: VipEventCreate<'a>,
    ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error> {
        Box::pin(async move { self.vip.create_vip_event(event).await })
    }

    fn delete_admin_refund_donation<'a>(
        &'a self,
        id: i64,
    ) -> PaymentStoreFuture<'a, DonationRecord, Self::Error> {
        Box::pin(async move { self.payments.delete_donation(id).await })
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

impl VipCancellationStore for PostgresSuccessfulPaymentStore {
    fn mark_subscription_canceled<'a>(
        &'a self,
        id: i64,
    ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error> {
        Box::pin(async move { self.payments.mark_subscription_canceled(id).await })
    }
}

impl ExternalVipCacheStore for PostgresSuccessfulPaymentStore {
    type Error = StorageError;

    fn upsert_vip_cache<'a>(
        &'a self,
        cache: VipCacheUpsert,
    ) -> PaymentStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.vip.upsert_vip_cache(cache).await })
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

pub const VIP_TELEGRAM_CACHE_TTL: TimeDuration = TimeDuration::hours(24);
pub const NON_VIP_TELEGRAM_CACHE_TTL: TimeDuration = TimeDuration::minutes(10);

#[derive(Clone, Debug)]
pub struct VipStatusWithExternalMembership<Store, Membership> {
    store: Store,
    membership: Membership,
    vip_chat_id: i64,
}

impl<Store, Membership> VipStatusWithExternalMembership<Store, Membership> {
    #[must_use]
    pub fn new(store: Store, membership: Membership, vip_chat_id: i64) -> Self {
        Self {
            store,
            membership,
            vip_chat_id,
        }
    }
}

impl<Store, Membership> VipStatusStore for VipStatusWithExternalMembership<Store, Membership>
where
    Store: VipStatusStore + Sync,
    Membership: Send + Sync,
{
    type Error = Store::Error;

    fn get_vip_summary_by_user<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error> {
        self.store.get_vip_summary_by_user(user_id)
    }

    fn get_active_subscription<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
        self.store.get_active_subscription(user_id)
    }

    fn get_vip_cache<'a>(
        &'a self,
        user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipCacheRecord>, Self::Error> {
        self.store.get_vip_cache(user_id)
    }
}

impl<Store, Membership> VipStatusChecker for VipStatusWithExternalMembership<Store, Membership>
where
    Store: VipStatusStore + ExternalVipCacheStore + Sync,
    Membership: ExternalVipMembershipChecker + Sync,
{
    fn is_vip_at<'a>(&'a self, user_id: i64, now: OffsetDateTime) -> VipStatusCheckFuture<'a> {
        Box::pin(async move {
            if user_id <= 0 {
                return false;
            }
            if let Ok(Some(summary)) = self.store.get_vip_summary_by_user(user_id).await
                && summary.is_active
                && now < summary.effective_expires_at
            {
                return true;
            }
            match self.store.get_vip_cache(user_id).await {
                Ok(Some(cache)) if now < cache.expires_at => return cache.is_vip,
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, user_id, "failed to read VIP cache before external check");
                }
            }
            if self.vip_chat_id == 0 {
                return false;
            }
            let is_vip = match self
                .membership
                .is_external_vip_member(self.vip_chat_id, user_id)
                .await
            {
                Ok(is_vip) => is_vip,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        user_id,
                        vip_chat_id = self.vip_chat_id,
                        "failed to check external VIP chat membership"
                    );
                    return false;
                }
            };
            let ttl = vip_telegram_cache_ttl(is_vip);
            if let Err(error) = self
                .store
                .upsert_vip_cache(VipCacheUpsert {
                    user_id,
                    is_vip,
                    expires_at: now + ttl,
                })
                .await
            {
                tracing::warn!(%error, user_id, "failed to update external VIP cache");
            }
            is_vip
        })
    }
}

#[derive(Clone, Debug)]
pub struct TelegramExternalVipMembershipChecker {
    telegram: openplotva_telegram::TelegramClient,
}

impl TelegramExternalVipMembershipChecker {
    #[must_use]
    pub fn new(telegram: openplotva_telegram::TelegramClient) -> Self {
        Self { telegram }
    }
}

impl ExternalVipMembershipChecker for TelegramExternalVipMembershipChecker {
    type Error = carapax::api::ExecuteError;

    fn is_external_vip_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> ExternalVipMembershipFuture<'a, Self::Error> {
        Box::pin(async move {
            let member = self
                .telegram
                .execute(openplotva_telegram::build_get_chat_member_method(
                    chat_id, user_id,
                ))
                .await?;
            Ok(telegram_member_has_vip_status(&member))
        })
    }
}

#[must_use]
pub fn telegram_member_has_vip_status(member: &TelegramChatMember) -> bool {
    matches!(
        member,
        TelegramChatMember::Creator(_)
            | TelegramChatMember::Administrator(_)
            | TelegramChatMember::Member { .. }
    )
}

#[must_use]
pub fn vip_telegram_cache_ttl(is_vip: bool) -> TimeDuration {
    if is_vip {
        VIP_TELEGRAM_CACHE_TTL
    } else {
        NON_VIP_TELEGRAM_CACHE_TTL
    }
}

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
    let paid_at = OffsetDateTime::from_unix_timestamp(message.date).ok();
    let subscription_expiration_date =
        payment_subscription_expiration_date(payment).or_else(|| {
            paid_at.and_then(|paid_at| {
                payment_subscription_period_seconds_from_expiration(payment, paid_at)
                    .map(|seconds| paid_at + TimeDuration::seconds(seconds))
            })
        });

    let mut data = ControlJobData {
        kind: ControlKind::SuccessfulPayment,
        chat_type: chat_type_name(&message.chat).to_owned(),
        payment: Some(ControlPayment {
            currency: payment.currency.clone(),
            total_amount: i32::try_from(payment.total_amount).ok()?,
            invoice_payload: payment.invoice_payload.clone(),
            telegram_payment_charge_id: payment.telegram_payment_charge_id.clone(),
            provider_payment_charge_id: payment.provider_payment_charge_id.clone(),
            paid_at: paid_at.map(format_payment_timestamp),
            subscription_period_seconds: paid_at.and_then(|paid_at| {
                payment_subscription_period_seconds_from_expiration(payment, paid_at)
            }),
            subscription_expiration_date: subscription_expiration_date
                .map(format_payment_timestamp),
            is_recurring: payment.is_recurring,
            is_first_recurring: payment.is_first_recurring,
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
            .with_priority(TRANSACTION_PRIORITY),
    )
}

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

pub const SUBSCRIPTION_PRICE_STARS: i64 = 300;
pub const DEFAULT_DONATION_STARS: i64 = 600;
pub const MIN_DONATION_STARS: i64 = 10;
pub const MAX_DONATION_STARS: i64 = 10_000;

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

    match command.name {
        "vip" if !matches!(message.chat, TelegramChat::Private(_)) => {
            PaymentInvoiceControlJobBuild::NonPrivateChat
        }
        "vip" => vip_invoice_control_job_from_message_at(message, created),
        "donate" if !matches!(message.chat, TelegramChat::Private(_)) => {
            PaymentInvoiceControlJobBuild::NonPrivateChat
        }
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
    if !matches!(command.name, "vip" | "donate") {
        handle_other(update).await?;
        return Ok(PaymentInvoiceCommandUpdateRoute::Delegated);
    }
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

#[derive(Clone, Debug)]
pub struct PaymentUpdateHandler<Queue, Vip, Effects, Next> {
    queue: Arc<Queue>,
    vip: Arc<Vip>,
    effects: Arc<Effects>,
    next: Arc<Next>,
    bot_username: String,
}

impl<Queue, Vip, Effects, Next> PaymentUpdateHandler<Queue, Vip, Effects, Next> {
    /// Build a payment-aware handler around the real downstream update handler.
    pub fn new(queue: Arc<Queue>, vip: Arc<Vip>, effects: Arc<Effects>, next: Arc<Next>) -> Self {
        Self {
            queue,
            vip,
            effects,
            next,
            bot_username: String::new(),
        }
    }

    #[must_use]
    pub fn with_bot_username(mut self, bot_username: impl Into<String>) -> Self {
        self.bot_username = bot_username.into();
        self
    }
}

impl<Queue, Vip, Effects, Next> UpdateHandler for PaymentUpdateHandler<Queue, Vip, Effects, Next>
where
    Queue: PaymentControlJobQueue + Send + Sync,
    Vip: VipStatusChecker + VipStatusStore + Send + Sync,
    Effects: PreCheckoutPaymentEffects + VipStatusEffects + PaymentRedirectEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            if non_private_payment_command_targets_other_bot(&update, &self.bot_username) {
                self.next.handle_update(update).await?;
                return Ok(());
            }
            let redirect = payment_redirect_message_from_update(&update, &self.bot_username);
            let now = OffsetDateTime::now_utc();
            let route = handle_payment_update_or_else_at(
                self.queue.as_ref(),
                self.vip.as_ref(),
                self.effects.as_ref(),
                update,
                now,
                now,
                |update| self.next.handle_update(update),
            )
            .await?;
            if matches!(
                route,
                PaymentUpdateRoute::InvoiceCommand(
                    PaymentInvoiceCommandUpdateRoute::NonPrivateChat
                )
            ) && let Some(message) = redirect
                && let Err(error) = self.effects.send_payment_redirect_message(message).await
            {
                tracing::warn!(%error, "failed to send payment redirect message");
            }
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminVipCommandUpdateOutcome {
    /// Update was not owned by this slice and went downstream.
    Delegated,
    MalformedCommand,
    Unauthorized,
    UnauthorizedSendError {
        /// Queue/build error text.
        message: String,
    },
    /// `/admin_grant_vip` was authorized and executed.
    Grant(AdminGrantVipCommandReport),
    /// `/admin_cancel_vip` was authorized and executed.
    Cancel(AdminCancelVipCommandReport),
    /// `/admin_refund` was authorized and executed.
    Refund(AdminRefundCommandReport),
}

#[derive(Clone)]
pub struct AdminVipCommandUpdateHandler<Vip, Effects, Next> {
    queue: Arc<openplotva_telegram::DispatcherQueue>,
    vip: Arc<Vip>,
    effects: Arc<Effects>,
    admin_ids: Arc<[i64]>,
    bot_username: String,
    next: Arc<Next>,
    next_virtual_id: VirtualIdFactory,
}

impl<Vip, Effects, Next> AdminVipCommandUpdateHandler<Vip, Effects, Next> {
    /// Build an admin VIP command handler around the real downstream update handler.
    #[must_use]
    pub fn new(
        queue: Arc<openplotva_telegram::DispatcherQueue>,
        vip: Arc<Vip>,
        effects: Arc<Effects>,
        admin_ids: Vec<i64>,
        bot_username: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            queue,
            vip,
            effects,
            admin_ids: Arc::from(admin_ids.into_boxed_slice()),
            bot_username: bot_username.into(),
            next,
            next_virtual_id: monotonic_virtual_id_factory("admin-vip-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl<Vip, Effects, Next> UpdateHandler for AdminVipCommandUpdateHandler<Vip, Effects, Next>
where
    Vip: AdminGrantVipStore + AdminCancelVipStore + AdminRefundStore + Send + Sync,
    Effects: AdminGrantVipEffects + AdminCancelVipEffects + AdminRefundEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_admin_vip_command_update_or_else_at(
                AdminVipCommandUpdateRuntime {
                    queue: self.queue.as_ref(),
                    vip: self.vip.as_ref(),
                    effects: self.effects.as_ref(),
                    admin_ids: &self.admin_ids,
                    bot_username: &self.bot_username,
                    next_virtual_id: &self.next_virtual_id,
                    now: OffsetDateTime::now_utc(),
                },
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Copy)]
pub struct AdminVipCommandUpdateRuntime<'a, Vip, Effects> {
    pub queue: &'a openplotva_telegram::DispatcherQueue,
    /// Payment/VIP storage boundary.
    pub vip: &'a Vip,
    /// Payment/admin side-effect boundary.
    pub effects: &'a Effects,
    /// Authorized Telegram admin user IDs.
    pub admin_ids: &'a [i64],
    /// Current bot username from `getMe`.
    pub bot_username: &'a str,
    /// Virtual ID generator for denial replies.
    pub next_virtual_id: &'a VirtualIdFactory,
    /// Clock value used for `/admin_cancel_vip` active-status checks.
    pub now: OffsetDateTime,
}

pub async fn handle_admin_vip_command_update_or_else_at<
    Vip,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    runtime: AdminVipCommandUpdateRuntime<'_, Vip, Effects>,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<AdminVipCommandUpdateOutcome, HandleError>
where
    Vip: AdminGrantVipStore + AdminCancelVipStore + AdminRefundStore + Sync,
    Effects: AdminGrantVipEffects + AdminCancelVipEffects + AdminRefundEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update).await?;
        return Ok(AdminVipCommandUpdateOutcome::Delegated);
    };
    let Some(command) = admin_vip_command_for_message(message, runtime.bot_username) else {
        handle_other(update).await?;
        return Ok(AdminVipCommandUpdateOutcome::Delegated);
    };

    let actor_user_id = message
        .sender
        .get_user()
        .map(|user| i64::from(user.id))
        .unwrap_or_default();
    if !runtime.admin_ids.contains(&actor_user_id) {
        return match send_admin_command_unauthorized_text(
            runtime.queue,
            runtime.next_virtual_id,
            message,
        )
        .await
        {
            Ok(()) => Ok(AdminVipCommandUpdateOutcome::Unauthorized),
            Err(error) => {
                tracing::warn!(%error, actor_user_id, "failed to queue admin command denial");
                Ok(AdminVipCommandUpdateOutcome::UnauthorizedSendError {
                    message: error.to_string(),
                })
            }
        };
    }

    match command.name {
        "admin_grant_vip" => {
            let Some(command) =
                admin_grant_vip_command_from_message(message, command.arguments, actor_user_id)
            else {
                return Ok(AdminVipCommandUpdateOutcome::MalformedCommand);
            };
            Ok(AdminVipCommandUpdateOutcome::Grant(
                handle_admin_grant_vip_command_at(runtime.vip, runtime.effects, command).await,
            ))
        }
        "admin_cancel_vip" => {
            let Some(command) =
                admin_cancel_vip_command_from_message(message, command.arguments, actor_user_id)
            else {
                return Ok(AdminVipCommandUpdateOutcome::MalformedCommand);
            };
            Ok(AdminVipCommandUpdateOutcome::Cancel(
                handle_admin_cancel_vip_command_at(
                    runtime.vip,
                    runtime.effects,
                    command,
                    runtime.now,
                )
                .await,
            ))
        }
        "admin_refund" => {
            let Some(command) =
                admin_refund_command_from_message(message, command.arguments, actor_user_id)
            else {
                return Ok(AdminVipCommandUpdateOutcome::MalformedCommand);
            };
            let Some(telegram_payment_charge_id) = first_admin_refund_charge_id(command.arguments)
            else {
                return match send_admin_command_ephemeral_text(
                    runtime.queue,
                    runtime.next_virtual_id,
                    message,
                    ADMIN_REFUND_MISSING_ARGUMENT_TEXT,
                )
                .await
                {
                    Ok(()) => Ok(AdminVipCommandUpdateOutcome::Refund(
                        AdminRefundCommandReport {
                            outcome: AdminRefundCommandOutcome::MissingChargeId,
                            telegram_payment_charge_id: None,
                            target_user_id: None,
                            amount_stars: None,
                            error: None,
                        },
                    )),
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            actor_user_id,
                            "failed to queue admin refund usage reply"
                        );
                        Ok(AdminVipCommandUpdateOutcome::Refund(
                            AdminRefundCommandReport {
                                outcome: AdminRefundCommandOutcome::MissingChargeId,
                                telegram_payment_charge_id: None,
                                target_user_id: None,
                                amount_stars: None,
                                error: Some(error.to_string()),
                            },
                        ))
                    }
                };
            };
            Ok(AdminVipCommandUpdateOutcome::Refund(
                handle_admin_refund_command_at(
                    runtime.vip,
                    runtime.effects,
                    AdminRefundCommand {
                        telegram_payment_charge_id,
                        ..command
                    },
                )
                .await,
            ))
        }
        _ => {
            handle_other(update).await?;
            Ok(AdminVipCommandUpdateOutcome::Delegated)
        }
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
        tracing::info!(pre_checkout_query_id, "pre-checkout answer requested");
        let outcome = match effects
            .answer_pre_checkout_query(pre_checkout_query_id)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    pre_checkout_query_id,
                    answer_failed = false,
                    "pre-checkout answer completed"
                );
                PreCheckoutOutcome::Acknowledged
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    pre_checkout_query_id,
                    "failed to answer pre-checkout query"
                );
                tracing::info!(
                    pre_checkout_query_id,
                    answer_failed = true,
                    "pre-checkout answer completed"
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
    target: Option<&'a str>,
    arguments: &'a str,
}

fn admin_vip_command_for_message<'a>(
    message: &'a TelegramMessage,
    bot_username: &str,
) -> Option<PaymentCommand<'a>> {
    let command = payment_command_from_message(message)?;
    if !matches!(
        command.name,
        "admin_grant_vip" | "admin_cancel_vip" | "admin_refund"
    ) {
        return None;
    }
    if matches!(message.chat, TelegramChat::Private(_)) {
        return Some(command);
    }
    (command.target == Some(bot_username)).then_some(command)
}

fn admin_grant_vip_command_from_message<'a>(
    message: &'a TelegramMessage,
    arguments: &'a str,
    actor_user_id: i64,
) -> Option<AdminGrantVipCommand<'a>> {
    Some(AdminGrantVipCommand {
        chat_id: message.chat.get_id().into(),
        message_id: i32::try_from(message.id).ok()?,
        actor_user_id,
        reply_user_id: admin_reply_user_id(message),
        arguments,
    })
}

fn admin_cancel_vip_command_from_message<'a>(
    message: &'a TelegramMessage,
    arguments: &'a str,
    actor_user_id: i64,
) -> Option<AdminCancelVipCommand<'a>> {
    Some(AdminCancelVipCommand {
        chat_id: message.chat.get_id().into(),
        message_id: i32::try_from(message.id).ok()?,
        actor_user_id,
        arguments,
    })
}

fn admin_refund_command_from_message<'a>(
    message: &'a TelegramMessage,
    arguments: &'a str,
    actor_user_id: i64,
) -> Option<AdminRefundCommand<'a>> {
    Some(AdminRefundCommand {
        chat_id: message.chat.get_id().into(),
        message_id: i32::try_from(message.id).ok()?,
        actor_user_id,
        arguments,
        telegram_payment_charge_id: "",
    })
}

fn first_admin_refund_charge_id(arguments: &str) -> Option<&str> {
    arguments.split_whitespace().next()
}

fn admin_reply_user_id(message: &TelegramMessage) -> Option<i64> {
    let TelegramReplyTo::Message(reply) = message.reply_to.as_ref()? else {
        return None;
    };
    reply.sender.get_user().map(|user| user.id.into())
}

async fn send_admin_command_unauthorized_text(
    queue: &openplotva_telegram::DispatcherQueue,
    next_virtual_id: &VirtualIdFactory,
    message: &TelegramMessage,
) -> Result<(), openplotva_telegram::OutboundBuildError> {
    send_admin_command_ephemeral_text(
        queue,
        next_virtual_id,
        message,
        ADMIN_COMMAND_UNAUTHORIZED_TEXT,
    )
    .await
}

async fn send_admin_command_ephemeral_text(
    queue: &openplotva_telegram::DispatcherQueue,
    next_virtual_id: &VirtualIdFactory,
    message: &TelegramMessage,
    text: &str,
) -> Result<(), openplotva_telegram::OutboundBuildError> {
    let chat = openplotva_telegram::ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    };
    let request = openplotva_telegram::TextMessageRequest {
        chat: Some(chat),
        message_thread_id: message.message_thread_id.unwrap_or_default(),
        disable_notification: false,
        allow_sending_without_reply: None,
        text: text.to_owned(),
        render_as: String::new(),
        reply_markup: None,
    };
    let reply_to = openplotva_telegram::ReplyMessageRef {
        message_id: message.id,
        chat,
        is_topic_message: message.message_thread_id.is_some(),
        message_thread_id: message.message_thread_id.unwrap_or_default(),
    };
    queue_text_message_parts(
        queue,
        QueueTextRequest {
            protected: false,
            debounce_key: None,
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: true,
            bypass_chat_restrictions: false,
            ephemeral_delete_after: Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER),
        },
        || next_virtual_id(),
    )
    .await?;
    Ok(())
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
    let (name, target) = command_with_at
        .split_once('@')
        .map_or((command_with_at, None), |(name, target)| {
            (name, Some(target))
        });
    let arguments = command_arguments_after_command(&text.data, command_end)?;
    Some(PaymentCommand {
        name,
        target,
        arguments,
    })
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdminGrantVipCommandArgs {
    /// First positional argument, the number of days.
    pub duration: String,
    /// Second positional argument, the user target.
    pub target: String,
    /// Remaining arguments collapsed with single spaces.
    pub reason: String,
    /// Whether a duration argument was present.
    pub has_duration: bool,
    /// Whether a target argument was present.
    pub has_target: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminGrantVipCommand<'value> {
    /// Chat where the admin command was received.
    pub chat_id: i64,
    pub message_id: i32,
    /// Telegram user ID of the admin who issued the command.
    pub actor_user_id: i64,
    /// Sender of the replied-to message, when the command targets by reply.
    pub reply_user_id: Option<i64>,
    /// Raw `CommandArguments()` text after `/admin_grant_vip`.
    pub arguments: &'value str,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdminCancelVipCommandArgs {
    /// First positional argument, the user target.
    pub target: String,
    /// Whether the second positional argument requests a Telegram refund.
    pub with_refund: bool,
    /// Whether a target argument was present.
    pub has_target: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminCancelVipCommand<'value> {
    /// Chat where the admin command was received.
    pub chat_id: i64,
    pub message_id: i32,
    /// Telegram user ID of the admin who issued the command.
    pub actor_user_id: i64,
    /// Raw `CommandArguments()` text after `/admin_cancel_vip`.
    pub arguments: &'value str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminRefundCommand<'value> {
    /// Chat where the admin command was received.
    pub chat_id: i64,
    pub message_id: i32,
    /// Telegram user ID of the admin who issued the command.
    pub actor_user_id: i64,
    /// Raw `CommandArguments()` text after `/admin_refund`.
    pub arguments: &'value str,
    pub telegram_payment_charge_id: &'value str,
}

#[must_use]
pub fn parse_admin_grant_vip_command_args(raw: &str) -> AdminGrantVipCommandArgs {
    let mut args = AdminGrantVipCommandArgs::default();
    let mut reason = String::new();
    for (index, arg) in raw.split_whitespace().enumerate() {
        match index {
            0 => {
                args.duration = arg.to_owned();
                args.has_duration = true;
            }
            1 => {
                args.target = arg.to_owned();
                args.has_target = true;
            }
            _ => {
                if !reason.is_empty() {
                    reason.push(' ');
                }
                reason.push_str(arg);
            }
        }
    }
    args.reason = reason;
    args
}

#[must_use]
pub fn parse_admin_cancel_vip_command_args(raw: &str) -> AdminCancelVipCommandArgs {
    let mut args = AdminCancelVipCommandArgs::default();
    for (index, arg) in raw.split_whitespace().enumerate() {
        match index {
            0 => {
                args.target = arg.to_owned();
                args.has_target = true;
            }
            1 => {
                args.with_refund = admin_cancel_vip_refund_flag(arg);
                return args;
            }
            _ => return args,
        }
    }
    args
}

#[must_use]
pub fn admin_cancel_vip_refund_flag(refund_arg: &str) -> bool {
    refund_arg.eq_ignore_ascii_case("true")
        || refund_arg == "1"
        || refund_arg.eq_ignore_ascii_case("yes")
}

#[must_use]
pub fn admin_cancel_vip_parse_error_text(error: &str) -> &'static str {
    match error {
        "missing user identifier" => {
            "⚠️ Пожалуйста, укажите ID пользователя или @username.\n\
             Использование: /admin_cancel_vip [user_id или @username] [refund:true/false]\n\n\
             Пример:\n\
             • /admin_cancel_vip 123456\n\
             • /admin_cancel_vip 123456 true\n\
             • /admin_cancel_vip @username false"
        }
        "username resolution not supported yet" => {
            "❌ Поиск по username временно не поддерживается. Используйте User ID."
        }
        _ => "❌ Неверный формат User ID. Используйте числовой ID.",
    }
}

#[must_use]
pub fn admin_cancel_vip_success_text(
    target_user_id: i64,
    original_expires_at: OffsetDateTime,
    with_refund: bool,
    updated_summary: Option<&VipSummaryRecord>,
    now: OffsetDateTime,
) -> String {
    let mut text = format!(
        "✅ VIP статус успешно обработан!\n\n\
         👤 User ID: {target_user_id}\n\
         📅 Был действителен до: {}\n",
        go_date_time(original_expires_at)
    );
    if with_refund {
        text.push_str("💸 Возврат средств: выполнен\n");
    } else {
        text.push_str(&format!(
            "⏰ Отозван администратором: {}\n",
            go_date_time(now)
        ));
    }
    if let Some(summary) = updated_summary {
        text.push_str(&format!(
            "📌 Остаток VIP после операции: до {}\n",
            go_date_time(summary.effective_expires_at)
        ));
    }
    text
}

#[must_use]
pub fn parse_admin_grant_vip_duration(arg: &str, has_duration: bool) -> (i64, bool) {
    if !has_duration {
        return (1, false);
    }
    match arg.parse::<i64>() {
        Ok(days) if days != 0 && (-365..=365).contains(&days) => (days, false),
        _ => (1, true),
    }
}

#[must_use]
pub fn admin_grant_vip_reason(reason: &str, actor_user_id: i64) -> String {
    let trimmed = reason.trim();
    if trimmed.is_empty() {
        format!("telegram admin adjustment by {actor_user_id}")
    } else {
        trimmed.to_owned()
    }
}

#[must_use]
pub fn admin_grant_vip_action_text(duration_days: i64) -> &'static str {
    if duration_days < 0 {
        "списано"
    } else {
        "добавлено"
    }
}

#[must_use]
pub fn admin_grant_vip_usage_text() -> &'static str {
    "⚠️ Пожалуйста, укажите пользователя:\n\
     • Ответьте на сообщение пользователя, или\n\
     • Укажите во втором аргументе: user_id, @username или https://t.me/username\n\n\
     Использование: /admin_grant_vip [дни=1|-1] [user_id/@username/https://t.me/username] [причина]"
}

#[must_use]
pub fn admin_grant_vip_success_text(
    target_user_id: i64,
    duration_days: i64,
    expires_at: OffsetDateTime,
    reason: &str,
    event_id: i64,
) -> String {
    format!(
        "🎉 VIP корректировка успешно записана!\n\n\
         👤 User ID: {target_user_id}\n\
         ⏱️ Корректировка: {duration_days} дней ({})\n\
         📅 Действует до: {}\n\
         📝 Причина: {reason}\n\n\
         🎁 Пользователь получает:\n{VIP_BENEFITS_TEXT}\n\n\
         🆔 Event ID: {event_id}",
        admin_grant_vip_action_text(duration_days),
        go_date_time(expires_at)
    )
}

const ADMIN_GRANT_VIP_INVALID_DURATION_TEXT: &str = "⚠️ Неверное количество дней в первом аргументе. Используйте число от -365 до 365, кроме 0. Применена корректировка по умолчанию: 1 день.";
const ADMIN_GRANT_VIP_LEDGER_ERROR_TEXT: &str =
    "❌ Не удалось записать VIP событие. Ошибка базы данных.";

pub async fn handle_admin_grant_vip_command_at<Store, Effects>(
    store: &Store,
    effects: &Effects,
    command: AdminGrantVipCommand<'_>,
) -> AdminGrantVipCommandReport
where
    Store: AdminGrantVipStore + Sync,
    Effects: AdminGrantVipEffects + Sync,
{
    let args = parse_admin_grant_vip_command_args(command.arguments);
    let target = match admin_grant_vip_target(store, command.reply_user_id, &args).await {
        AdminGrantVipTargetResolution::Resolved {
            target_user_id,
            reason,
        } => (target_user_id, reason),
        AdminGrantVipTargetResolution::Missing => {
            send_admin_grant_vip_text(effects, &command, admin_grant_vip_usage_text()).await;
            return AdminGrantVipCommandReport {
                outcome: AdminGrantVipCommandOutcome::MissingTarget,
                target_user_id: None,
                event_id: None,
                error: None,
            };
        }
        AdminGrantVipTargetResolution::Error(error) => {
            let text = format!("❌ {error}");
            send_admin_grant_vip_text(effects, &command, &text).await;
            return AdminGrantVipCommandReport {
                outcome: AdminGrantVipCommandOutcome::TargetError,
                target_user_id: None,
                event_id: None,
                error: Some(error),
            };
        }
    };

    let (duration_days, invalid_duration) =
        parse_admin_grant_vip_duration(&args.duration, args.has_duration);
    if invalid_duration {
        send_admin_grant_vip_text(effects, &command, ADMIN_GRANT_VIP_INVALID_DURATION_TEXT).await;
    }

    let reason = admin_grant_vip_reason(&target.1, command.actor_user_id);
    let event = match store
        .create_admin_vip_event(VipEventCreate {
            user_id: target.0,
            event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT,
            delta_seconds: vip_days_to_seconds(duration_days),
            subscription_id: None,
            actor_user_id: Some(command.actor_user_id),
            reason: Some(&reason),
        })
        .await
    {
        Ok(event) => event,
        Err(error) => {
            send_admin_grant_vip_text(effects, &command, ADMIN_GRANT_VIP_LEDGER_ERROR_TEXT).await;
            return AdminGrantVipCommandReport {
                outcome: AdminGrantVipCommandOutcome::StorageError,
                target_user_id: Some(target.0),
                event_id: None,
                error: Some(error.to_string()),
            };
        }
    };

    let _ = effects.invalidate_admin_grant_vip_cache(target.0).await;
    let success = admin_grant_vip_success_text(
        target.0,
        duration_days,
        event.effective_expires_at,
        &reason,
        event.id,
    );
    send_admin_grant_vip_text(effects, &command, &success).await;

    AdminGrantVipCommandReport {
        outcome: if invalid_duration {
            AdminGrantVipCommandOutcome::GrantedWithDefaultDuration
        } else {
            AdminGrantVipCommandOutcome::Granted
        },
        target_user_id: Some(target.0),
        event_id: Some(event.id),
        error: None,
    }
}

enum AdminGrantVipTargetResolution {
    Resolved { target_user_id: i64, reason: String },
    Missing,
    Error(String),
}

async fn admin_grant_vip_target<Store>(
    store: &Store,
    reply_user_id: Option<i64>,
    args: &AdminGrantVipCommandArgs,
) -> AdminGrantVipTargetResolution
where
    Store: AdminGrantVipStore + Sync,
{
    if args.has_target {
        return match parse_admin_grant_vip_target_identifier(store, &args.target).await {
            Ok(target_user_id) => AdminGrantVipTargetResolution::Resolved {
                target_user_id,
                reason: args.reason.clone(),
            },
            Err(error) => AdminGrantVipTargetResolution::Error(error),
        };
    }

    match reply_user_id {
        Some(target_user_id) => AdminGrantVipTargetResolution::Resolved {
            target_user_id,
            reason: String::new(),
        },
        None => AdminGrantVipTargetResolution::Missing,
    }
}

async fn parse_admin_grant_vip_target_identifier<Store>(
    store: &Store,
    identifier: &str,
) -> Result<i64, String>
where
    Store: AdminGrantVipStore + Sync,
{
    if let Ok(user_id) = identifier.parse::<i64>() {
        return Ok(user_id);
    }
    if let Some(username) = identifier.strip_prefix('@') {
        return resolve_admin_grant_vip_username(store, identifier, username).await;
    }
    if let Some(username) = identifier.strip_prefix("https://t.me/") {
        return resolve_admin_grant_vip_username(store, identifier, username).await;
    }
    Err(format!(
        "invalid user identifier format: {identifier} (must be user_id, @username, or https://t.me/username)"
    ))
}

async fn resolve_admin_grant_vip_username<Store>(
    store: &Store,
    identifier: &str,
    username: &str,
) -> Result<i64, String>
where
    Store: AdminGrantVipStore + Sync,
{
    match store.get_user_id_by_username(username).await {
        Ok(Some(user_id)) => Ok(user_id),
        Ok(None) => Err(format!("user with username {identifier} not found")),
        Err(error) => Err(format!(
            "user with username {identifier} not found: {error}"
        )),
    }
}

async fn send_admin_grant_vip_text<Effects>(
    effects: &Effects,
    command: &AdminGrantVipCommand<'_>,
    text: &str,
) where
    Effects: AdminGrantVipEffects + Sync,
{
    let _ = effects
        .send_admin_grant_vip_text(PaymentTextMessage {
            chat_id: command.chat_id,
            reply_to_message_id: command.message_id,
            text,
            parse_mode: "",
        })
        .await;
}

const ADMIN_CANCEL_VIP_STATUS_ERROR_TEXT: &str = "❌ Ошибка при проверке статуса VIP пользователя.";
const ADMIN_CANCEL_VIP_REVOKE_ERROR_TEXT: &str = "❌ Не удалось отозвать VIP. Ошибка базы данных.";
const ADMIN_CANCEL_VIP_PAID_SUBSCRIPTION_ERROR_TEXT: &str =
    "❌ Ошибка при проверке оплаченной подписки пользователя.";

pub async fn handle_admin_cancel_vip_command_at<Store, Effects>(
    store: &Store,
    effects: &Effects,
    command: AdminCancelVipCommand<'_>,
    now: OffsetDateTime,
) -> AdminCancelVipCommandReport
where
    Store: AdminCancelVipStore + Sync,
    Effects: AdminCancelVipEffects + Sync,
{
    let args = parse_admin_cancel_vip_command_args(command.arguments);
    let target_user_id = match admin_cancel_vip_target(store, &args).await {
        Ok(target_user_id) => target_user_id,
        Err(error) => {
            send_admin_cancel_vip_text(
                effects,
                &command,
                admin_cancel_vip_parse_error_text(&error),
            )
            .await;
            return AdminCancelVipCommandReport {
                outcome: AdminCancelVipCommandOutcome::TargetError,
                target_user_id: None,
                event_id: None,
                error: Some(error),
            };
        }
    };

    let summary = match store.get_vip_summary_by_user(target_user_id).await {
        Ok(Some(summary)) if active_admin_cancel_vip_summary_at(&summary, now) => summary,
        Ok(_) => {
            let text = format!("ℹ️ У пользователя {target_user_id} нет активного VIP статуса.");
            send_admin_cancel_vip_text(effects, &command, &text).await;
            return AdminCancelVipCommandReport {
                outcome: AdminCancelVipCommandOutcome::NoActiveVip,
                target_user_id: Some(target_user_id),
                event_id: None,
                error: None,
            };
        }
        Err(error) => {
            send_admin_cancel_vip_text(effects, &command, ADMIN_CANCEL_VIP_STATUS_ERROR_TEXT).await;
            return AdminCancelVipCommandReport {
                outcome: AdminCancelVipCommandOutcome::StatusLookupError,
                target_user_id: Some(target_user_id),
                event_id: None,
                error: Some(error.to_string()),
            };
        }
    };

    let (event, outcome) = if args.with_refund {
        let subscription = match store.get_active_subscription(target_user_id).await {
            Ok(Some(subscription)) => subscription,
            Ok(None) => {
                let text = format!(
                    "ℹ️ У пользователя {target_user_id} нет активной оплаченной VIP подписки для возврата."
                );
                send_admin_cancel_vip_text(effects, &command, &text).await;
                return AdminCancelVipCommandReport {
                    outcome: AdminCancelVipCommandOutcome::NoPaidSubscription,
                    target_user_id: Some(target_user_id),
                    event_id: None,
                    error: None,
                };
            }
            Err(error) => {
                send_admin_cancel_vip_text(
                    effects,
                    &command,
                    ADMIN_CANCEL_VIP_PAID_SUBSCRIPTION_ERROR_TEXT,
                )
                .await;
                return AdminCancelVipCommandReport {
                    outcome: AdminCancelVipCommandOutcome::PaidSubscriptionLookupError,
                    target_user_id: Some(target_user_id),
                    event_id: None,
                    error: Some(error.to_string()),
                };
            }
        };
        match refund_admin_cancelled_vip_subscription(
            store,
            effects,
            &subscription,
            command.actor_user_id,
        )
        .await
        {
            Ok(event) => (event, AdminCancelVipCommandOutcome::Refunded),
            Err(error) => {
                let text = format!(
                    "❌ Не удалось выполнить возврат средств: {error}\n\nПодписка не была отменена."
                );
                send_admin_cancel_vip_text(effects, &command, &text).await;
                return AdminCancelVipCommandReport {
                    outcome: AdminCancelVipCommandOutcome::RefundFailed,
                    target_user_id: Some(target_user_id),
                    event_id: None,
                    error: Some(error),
                };
            }
        }
    } else {
        let reason = format!("telegram admin revoke by {}", command.actor_user_id);
        let event = match store
            .create_admin_cancel_vip_event(VipEventCreate {
                user_id: target_user_id,
                event_type: VIP_EVENT_TYPE_ADMIN_REVOKE,
                delta_seconds: -summary.remaining_seconds,
                subscription_id: None,
                actor_user_id: Some(command.actor_user_id),
                reason: Some(&reason),
            })
            .await
        {
            Ok(event) => event,
            Err(error) => {
                send_admin_cancel_vip_text(effects, &command, ADMIN_CANCEL_VIP_REVOKE_ERROR_TEXT)
                    .await;
                return AdminCancelVipCommandReport {
                    outcome: AdminCancelVipCommandOutcome::RevokeStorageError,
                    target_user_id: Some(target_user_id),
                    event_id: None,
                    error: Some(error.to_string()),
                };
            }
        };
        (event, AdminCancelVipCommandOutcome::Revoked)
    };

    let _ = effects
        .invalidate_admin_cancel_vip_cache(target_user_id)
        .await;
    let updated_summary = match store.get_vip_summary_by_user(target_user_id).await {
        Ok(Some(summary)) if active_admin_cancel_vip_summary_at(&summary, now) => Some(summary),
        _ => None,
    };
    let success = admin_cancel_vip_success_text(
        target_user_id,
        summary.effective_expires_at,
        args.with_refund,
        updated_summary.as_ref(),
        now,
    );
    send_admin_cancel_vip_text(effects, &command, &success).await;

    AdminCancelVipCommandReport {
        outcome,
        target_user_id: Some(target_user_id),
        event_id: Some(event.id),
        error: None,
    }
}

async fn refund_admin_cancelled_vip_subscription<Store, Effects>(
    store: &Store,
    effects: &Effects,
    subscription: &SubscriptionRecord,
    actor_user_id: i64,
) -> Result<VipEventRecord, String>
where
    Store: AdminCancelVipStore + Sync,
    Effects: AdminCancelVipEffects + Sync,
{
    let telegram_payment_charge_id = subscription.telegram_payment_charge_id.as_str();
    if is_synthetic_subscription_charge_id(telegram_payment_charge_id) {
        return Err(
            "ручная VIP-корректировка не является платежом Telegram и не может быть возвращена"
                .to_owned(),
        );
    }

    effects
        .refund_admin_cancel_vip_payment(subscription.user_id, telegram_payment_charge_id)
        .await
        .map_err(|error| error.to_string())?;

    let refund_notice = admin_cancel_vip_refund_notice_text(
        telegram_payment_charge_id,
        subscription_refund_price_stars(subscription.user_id),
    );
    let _ = effects
        .send_admin_cancel_vip_user_text(subscription.user_id, &refund_notice)
        .await;

    store
        .mark_subscription_refunded(subscription.id)
        .await
        .map_err(|error| format!("не удалось пометить подписку как refunded: {error}"))?;

    let reason = format!("telegram admin refund by {actor_user_id}");
    let event = store
        .create_admin_cancel_vip_event(VipEventCreate {
            user_id: subscription.user_id,
            event_type: VIP_EVENT_TYPE_REFUND_REVERSAL,
            delta_seconds: -subscription_delta_seconds(
                telegram_payment_charge_id,
                subscription.created_at,
                subscription.expires_at,
                openplotva_telegram::SUBSCRIPTION_DURATION_DAYS,
            ),
            subscription_id: Some(subscription.id),
            actor_user_id: Some(actor_user_id),
            reason: Some(&reason),
        })
        .await
        .map_err(|error| format!("не удалось записать VIP refund_reversal: {error}"))?;

    let _ = effects
        .invalidate_admin_cancel_vip_cache(subscription.user_id)
        .await;
    Ok(event)
}

fn subscription_refund_price_stars(user_id: i64) -> i64 {
    if user_id == 1_717_359_759 {
        1
    } else {
        openplotva_telegram::SUBSCRIPTION_PRICE_STARS
    }
}

fn admin_cancel_vip_refund_notice_text(
    telegram_payment_charge_id: &str,
    price_stars: i64,
) -> String {
    format!(
        "💸 Вам был возвращен платеж в размере {price_stars} звезд по транзакции {telegram_payment_charge_id}."
    )
}

async fn admin_cancel_vip_target<Store>(
    store: &Store,
    args: &AdminCancelVipCommandArgs,
) -> Result<i64, String>
where
    Store: AdminCancelVipStore + Sync,
{
    if !args.has_target {
        return Err("missing user identifier".to_owned());
    }
    parse_admin_cancel_vip_target_identifier(store, &args.target).await
}

async fn parse_admin_cancel_vip_target_identifier<Store>(
    store: &Store,
    identifier: &str,
) -> Result<i64, String>
where
    Store: AdminCancelVipStore + Sync,
{
    if let Ok(user_id) = identifier.parse::<i64>() {
        return Ok(user_id);
    }
    if let Some(username) = identifier.strip_prefix('@') {
        return match store.get_user_id_by_username(username).await {
            Ok(Some(user_id)) => Ok(user_id),
            Ok(None) => Err(format!("user with username {identifier} not found")),
            Err(error) => Err(format!(
                "user with username {identifier} not found: {error}"
            )),
        };
    }
    Err(format!(
        "invalid user identifier format: {identifier} (must be user_id or @username)"
    ))
}

fn active_admin_cancel_vip_summary_at(summary: &VipSummaryRecord, now: OffsetDateTime) -> bool {
    summary.is_active && now < summary.effective_expires_at
}

async fn send_admin_cancel_vip_text<Effects>(
    effects: &Effects,
    command: &AdminCancelVipCommand<'_>,
    text: &str,
) where
    Effects: AdminCancelVipEffects + Sync,
{
    let _ = effects
        .send_admin_cancel_vip_text(PaymentTextMessage {
            chat_id: command.chat_id,
            reply_to_message_id: command.message_id,
            text,
            parse_mode: "",
        })
        .await;
}

const ADMIN_REFUND_MISSING_ARGUMENT_TEXT: &str = "⚠️ Пожалуйста, укажите ID транзакции для возврата (telegram_payment_charge_id).\nИспользование: /refund <telegram_payment_charge_id>";

#[derive(Clone, Debug, Eq, PartialEq)]
enum AdminRefundTarget {
    Subscription(SubscriptionRecord),
    Donation(DonationRecord),
}

impl AdminRefundTarget {
    fn user_id(&self) -> i64 {
        match self {
            Self::Subscription(subscription) => subscription.user_id,
            Self::Donation(donation) => donation.user_id,
        }
    }

    fn amount_stars(&self) -> i64 {
        match self {
            Self::Subscription(subscription) => {
                subscription_refund_price_stars(subscription.user_id)
            }
            Self::Donation(donation) => donation.amount_stars,
        }
    }

    fn outcome(&self) -> AdminRefundCommandOutcome {
        match self {
            Self::Subscription(_) => AdminRefundCommandOutcome::RefundedSubscription,
            Self::Donation(_) => AdminRefundCommandOutcome::RefundedDonation,
        }
    }

    fn is_valid(&self) -> bool {
        self.user_id() != 0 && self.amount_stars() != 0
    }
}

fn admin_refund_subscription_target(subscription: SubscriptionRecord) -> Option<AdminRefundTarget> {
    (subscription.id > 0).then_some(AdminRefundTarget::Subscription(subscription))
}

fn admin_refund_donation_target(donation: DonationRecord) -> Option<AdminRefundTarget> {
    (donation.id > 0).then_some(AdminRefundTarget::Donation(donation))
}

enum AdminRefundLookup {
    Found(AdminRefundTarget),
    NotFound,
    SubscriptionError(String),
    DonationError(String),
}

pub async fn handle_admin_refund_command_at<Store, Effects>(
    store: &Store,
    effects: &Effects,
    command: AdminRefundCommand<'_>,
) -> AdminRefundCommandReport
where
    Store: AdminRefundStore + Sync,
    Effects: AdminRefundEffects + Sync,
{
    let charge_id = command.telegram_payment_charge_id;
    let target = match lookup_admin_refund_target(store, charge_id).await {
        AdminRefundLookup::Found(target) => target,
        AdminRefundLookup::SubscriptionError(error) => {
            let text = format!("❌ Ошибка при поиске подписки с ID транзакции {charge_id}.");
            send_admin_refund_text(effects, &command, &text).await;
            return AdminRefundCommandReport {
                outcome: AdminRefundCommandOutcome::SubscriptionLookupError,
                telegram_payment_charge_id: Some(charge_id.to_owned()),
                target_user_id: None,
                amount_stars: None,
                error: Some(error),
            };
        }
        AdminRefundLookup::DonationError(error) => {
            let text = format!("❌ Ошибка при поиске доната с ID транзакции {charge_id}.");
            send_admin_refund_text(effects, &command, &text).await;
            return AdminRefundCommandReport {
                outcome: AdminRefundCommandOutcome::DonationLookupError,
                telegram_payment_charge_id: Some(charge_id.to_owned()),
                target_user_id: None,
                amount_stars: None,
                error: Some(error),
            };
        }
        AdminRefundLookup::NotFound => {
            let text =
                format!("🤷 Транзакция с ID {charge_id} не найдена (ни подписка, ни донат).");
            send_admin_refund_text(effects, &command, &text).await;
            return AdminRefundCommandReport {
                outcome: AdminRefundCommandOutcome::NotFound,
                telegram_payment_charge_id: Some(charge_id.to_owned()),
                target_user_id: None,
                amount_stars: None,
                error: None,
            };
        }
    };

    let target_user_id = target.user_id();
    let amount_stars = target.amount_stars();
    if !target.is_valid() {
        let text = format!("❌ Не удалось определить детали транзакции {charge_id} для возврата.");
        send_admin_refund_text(effects, &command, &text).await;
        return AdminRefundCommandReport {
            outcome: AdminRefundCommandOutcome::InvalidTarget,
            telegram_payment_charge_id: Some(charge_id.to_owned()),
            target_user_id: Some(target_user_id),
            amount_stars: Some(amount_stars),
            error: None,
        };
    }

    if let AdminRefundTarget::Subscription(subscription) = &target
        && is_synthetic_subscription_charge_id(&subscription.telegram_payment_charge_id)
    {
        let text = admin_refund_synthetic_subscription_text(charge_id);
        send_admin_refund_text(effects, &command, &text).await;
        return AdminRefundCommandReport {
            outcome: AdminRefundCommandOutcome::SyntheticSubscription,
            telegram_payment_charge_id: Some(charge_id.to_owned()),
            target_user_id: Some(target_user_id),
            amount_stars: Some(amount_stars),
            error: None,
        };
    }

    if let Err(error) = effects
        .refund_admin_refund_payment(target_user_id, charge_id)
        .await
    {
        let error = error.to_string();
        let text = admin_refund_api_error_text(charge_id, &error);
        send_admin_refund_text(effects, &command, &text).await;
        return AdminRefundCommandReport {
            outcome: AdminRefundCommandOutcome::RefundApiError,
            telegram_payment_charge_id: Some(charge_id.to_owned()),
            target_user_id: Some(target_user_id),
            amount_stars: Some(amount_stars),
            error: Some(error),
        };
    }

    record_admin_refund(store, effects, &command, &target).await;

    let success = admin_refund_success_text(charge_id, target_user_id, amount_stars);
    send_admin_refund_text(effects, &command, &success).await;
    let notice = admin_refund_notice_text(charge_id, amount_stars);
    let _ = effects
        .send_admin_refund_user_text(target_user_id, &notice)
        .await;

    AdminRefundCommandReport {
        outcome: target.outcome(),
        telegram_payment_charge_id: Some(charge_id.to_owned()),
        target_user_id: Some(target_user_id),
        amount_stars: Some(amount_stars),
        error: None,
    }
}

async fn lookup_admin_refund_target<Store>(store: &Store, charge_id: &str) -> AdminRefundLookup
where
    Store: AdminRefundStore + Sync,
{
    match store
        .get_admin_refund_subscription_by_charge_id(charge_id)
        .await
    {
        Ok(Some(subscription)) => {
            return admin_refund_subscription_target(subscription)
                .map(AdminRefundLookup::Found)
                .unwrap_or_else(|| AdminRefundLookup::SubscriptionError(String::new()));
        }
        Ok(None) => {}
        Err(error) => return AdminRefundLookup::SubscriptionError(error.to_string()),
    }

    match store
        .get_admin_refund_donation_by_charge_id(charge_id)
        .await
    {
        Ok(Some(donation)) => admin_refund_donation_target(donation)
            .map(AdminRefundLookup::Found)
            .unwrap_or_else(|| AdminRefundLookup::DonationError(String::new())),
        Ok(None) => AdminRefundLookup::NotFound,
        Err(error) => AdminRefundLookup::DonationError(error.to_string()),
    }
}

async fn record_admin_refund<Store, Effects>(
    store: &Store,
    effects: &Effects,
    command: &AdminRefundCommand<'_>,
    target: &AdminRefundTarget,
) where
    Store: AdminRefundStore + Sync,
    Effects: AdminRefundEffects + Sync,
{
    match target {
        AdminRefundTarget::Subscription(subscription) => {
            let mark_ok = store
                .mark_admin_refund_subscription_refunded(subscription.id)
                .await
                .is_ok();
            if mark_ok {
                let reason = format!("admin refund {}", command.telegram_payment_charge_id);
                let _ = store
                    .create_admin_refund_vip_event(VipEventCreate {
                        user_id: subscription.user_id,
                        event_type: VIP_EVENT_TYPE_REFUND_REVERSAL,
                        delta_seconds: -subscription_delta_seconds(
                            &subscription.telegram_payment_charge_id,
                            subscription.created_at,
                            subscription.expires_at,
                            openplotva_telegram::SUBSCRIPTION_DURATION_DAYS,
                        ),
                        subscription_id: Some(subscription.id),
                        actor_user_id: Some(command.actor_user_id),
                        reason: Some(&reason),
                    })
                    .await;
            }
            let _ = effects
                .invalidate_admin_refund_cache(subscription.user_id)
                .await;
        }
        AdminRefundTarget::Donation(donation) => {
            let _ = store.delete_admin_refund_donation(donation.id).await;
        }
    }
}

fn admin_refund_synthetic_subscription_text(telegram_payment_charge_id: &str) -> String {
    format!(
        "❌ Транзакция {telegram_payment_charge_id} относится к legacy admin grant и не может быть возвращена через Telegram refund."
    )
}

fn admin_refund_api_error_text(telegram_payment_charge_id: &str, error: &str) -> String {
    if error.contains("telegram API сообщил, что возврат не был успешен")
    {
        return admin_refund_api_unsuccessful_text(telegram_payment_charge_id, "");
    }
    format!("❌ Ошибка при возврате средств для транзакции {telegram_payment_charge_id}: {error}")
}

fn admin_refund_api_unsuccessful_text(telegram_payment_charge_id: &str, response: &str) -> String {
    format!(
        "❌ Telegram API сообщил, что возврат для транзакции {telegram_payment_charge_id} не был успешен. Ответ: {response}"
    )
}

fn admin_refund_success_text(
    telegram_payment_charge_id: &str,
    user_id: i64,
    amount_stars: i64,
) -> String {
    format!(
        "✅ Возврат средств для транзакции {telegram_payment_charge_id} (пользователь ID {user_id}, сумма {amount_stars} звезд) успешно выполнен."
    )
}

fn admin_refund_notice_text(telegram_payment_charge_id: &str, amount_stars: i64) -> String {
    format!(
        "💸 Вам был возвращен платеж в размере {amount_stars} звезд по транзакции {telegram_payment_charge_id}."
    )
}

async fn send_admin_refund_text<Effects>(
    effects: &Effects,
    command: &AdminRefundCommand<'_>,
    text: &str,
) where
    Effects: AdminRefundEffects + Sync,
{
    let _ = effects
        .send_admin_refund_text(PaymentTextMessage {
            chat_id: command.chat_id,
            reply_to_message_id: command.message_id,
            text,
            parse_mode: "",
        })
        .await;
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
            .with_priority(TRANSACTION_PRIORITY),
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

pub(crate) fn payment_control_job_failure_message(
    report: &PaymentControlJobReport,
) -> Option<String> {
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
            paid_at: parse_payment_timestamp(payment.paid_at.as_deref()),
            subscription_period_seconds: payment.subscription_period_seconds,
            subscription_expiration_date: parse_payment_timestamp(
                payment.subscription_expiration_date.as_deref(),
            ),
            vip_target_expires_at: None,
            is_recurring: payment.is_recurring,
            is_first_recurring: payment.is_first_recurring,
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
    let mut payment = successful_payment_from_telegram(payment);
    payment.paid_at = OffsetDateTime::from_unix_timestamp(message.date).ok();
    if payment.subscription_period_seconds.is_none()
        && let (Some(paid_at), Some(expires_at)) =
            (payment.paid_at, payment.subscription_expiration_date)
    {
        payment.subscription_period_seconds = Some((expires_at - paid_at).whole_seconds());
    }
    Some(SuccessfulPaymentMessage {
        chat_id: message.chat.get_id().into(),
        message_id: i32::try_from(message.id).ok()?,
        user: user_state_from_telegram_user(user),
        payment,
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
        paid_at: None,
        subscription_period_seconds: None,
        subscription_expiration_date: payment_subscription_expiration_date(payment),
        vip_target_expires_at: None,
        is_recurring: payment.is_recurring,
        is_first_recurring: payment.is_first_recurring,
    }
}

fn payment_subscription_expiration_date(
    payment: &TelegramSuccessfulPayment,
) -> Option<OffsetDateTime> {
    payment
        .subscription_expiration_date
        .and_then(|timestamp| OffsetDateTime::from_unix_timestamp(timestamp).ok())
}

fn payment_subscription_period_seconds_from_expiration(
    payment: &TelegramSuccessfulPayment,
    paid_at: OffsetDateTime,
) -> Option<i64> {
    payment_subscription_expiration_date(payment)
        .map(|expires_at| (expires_at - paid_at).whole_seconds())
        .filter(|seconds| *seconds > 0)
}

fn format_payment_timestamp(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| timestamp.unix_timestamp().to_string())
}

fn parse_payment_timestamp(value: Option<&str>) -> Option<OffsetDateTime> {
    value.and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
}

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

    tracing::info!(pre_checkout_query_id, "pre-checkout answer requested");
    match effects
        .answer_pre_checkout_query(pre_checkout_query_id)
        .await
    {
        Ok(()) => {
            tracing::info!(
                pre_checkout_query_id,
                answer_failed = false,
                "pre-checkout answer completed"
            );
            Ok(PreCheckoutUpdateRoute::Processed(
                PreCheckoutOutcome::Acknowledged,
            ))
        }
        Err(error) => {
            tracing::warn!(
                %error,
                pre_checkout_query_id,
                "failed to answer pre-checkout query"
            );
            tracing::info!(
                pre_checkout_query_id,
                answer_failed = true,
                "pre-checkout answer completed"
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
    let expires_at = subscription_payment_target_expires_at(&message.payment, now);
    let vip_target_expires_at = message.payment.vip_target_expires_at.unwrap_or(expires_at);
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

    match store
        .get_vip_event_by_subscription_id(subscription.id)
        .await
    {
        Ok(Some(event)) => {
            send_payment_text(
                effects,
                message,
                &subscription_success_text(event.effective_expires_at),
                "",
            )
            .await;
            let _ = effects.invalidate_vip_cache(message.user.id).await;
            return SuccessfulPaymentReport::new(
                SuccessfulPaymentOutcome::SubscriptionDuplicateProcessed,
            );
        }
        Ok(None) => {}
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

    let current_effective_expires_at = match store.get_vip_summary_by_user(message.user.id).await {
        Ok(summary) => summary.map(|summary| summary.effective_expires_at),
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
    };
    let delta_seconds = subscription_delta_seconds_to_target(
        vip_target_expires_at,
        now,
        current_effective_expires_at,
    );
    let reason = subscription_payment_reason(&message.payment.telegram_payment_charge_id);
    let event = match store
        .create_vip_event(VipEventCreate {
            user_id: message.user.id,
            event_type: VIP_EVENT_TYPE_PAYMENT,
            delta_seconds,
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

#[must_use]
pub fn subscription_payment_target_expires_at(
    payment: &SuccessfulPayment,
    now: OffsetDateTime,
) -> OffsetDateTime {
    if let Some(expires_at) = payment.subscription_expiration_date
        && expires_at > now
    {
        return expires_at;
    }
    let paid_at = payment.paid_at.unwrap_or(now);
    paid_at + TimeDuration::seconds(subscription_payment_period_seconds(payment))
}

#[must_use]
pub fn subscription_payment_period_seconds(payment: &SuccessfulPayment) -> i64 {
    payment
        .subscription_period_seconds
        .filter(|seconds| *seconds > 0)
        .unwrap_or(openplotva_telegram::SUBSCRIPTION_PERIOD_SECONDS)
}

#[must_use]
pub fn subscription_delta_seconds_to_target(
    target_expires_at: OffsetDateTime,
    now: OffsetDateTime,
    current_effective_expires_at: Option<OffsetDateTime>,
) -> i64 {
    let base = current_effective_expires_at
        .filter(|expires_at| *expires_at > now)
        .unwrap_or(now);
    (target_expires_at - base).whole_seconds().max(0)
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

#[must_use]
pub fn subscription_success_text(expires_at: OffsetDateTime) -> String {
    format!(
        "✅ Спасибо за подписку! Ваш VIP статус активирован до {}.\n\n",
        go_date(expires_at)
    )
}

#[must_use]
pub fn donation_success_text(amount_stars: i64) -> String {
    format!(
        "❤️ Большое спасибо за ваш донат в размере {amount_stars} звезд!\n\n\
         Ваша поддержка безумно согревает моё сердце и вдохновляет работать над ботом дальше! \
         Благодаря вам Плотва становится лучше каждый день."
    )
}

#[must_use]
pub fn subscription_invoice_message_text() -> String {
    format!(
        "✨ Нажмите на кнопку ниже, чтобы оформить VIP подписку на <b>30 дней</b> и получить:\n\n{}",
        VIP_BENEFITS_EXTENDED
    )
}

fn donation_invoice_message_text() -> String {
    format!(
        "💖 Нажмите на кнопку ниже, чтобы поддержать разработчика!\n\n\
         Ваша поддержка:\n\
         ❤️ Согревает сердце разработчика\n\
         🚀 Помогает развивать бота\n\
         ✨ Вдохновляет на создание новых функций\n\n{}\n\n\
         <i>Чтобы установить другую сумму - используйте формат <code>/donate XXX</code>, где <code>XXX</code> - сумма в звездах.</i>",
        crypto_donation_html()
    )
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

fn go_date_time(value: OffsetDateTime) -> String {
    format!(
        "{:02}.{:02}.{:04} {:02}:{:02}",
        value.day(),
        u8::from(value.month()),
        value.year(),
        value.hour(),
        value.minute()
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
const ADMIN_COMMAND_UNAUTHORIZED_TEXT: &str = "❌ У вас нет прав на выполнение этой команды.";
const ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER: Duration = Duration::from_secs(60);
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
        env,
        error::Error,
        fmt, io,
        sync::atomic::{AtomicU64, Ordering},
        sync::{Arc, Mutex, MutexGuard},
        time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
    };

    use crate::{
        callbacks::{
            CallbackQueryEffects, CallbackQueryFuture, CallbackQueryUpdateRoute,
            CallbackRateLimitFuture, CallbackRateLimitPolicy, handle_callback_query_update_or_else,
        },
        control_jobs::{
            ControlJobExecution, ControlJobExecutionResult, ControlJobExecutorFuture,
            ControlJobExecutorRegistry, process_control_job_once_at,
        },
        help::{HelpBotIdentity, HelpCommandUpdateHandler, HelpDispatcherEffects},
        updates::{
            TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
            UpdateStateStoreFuture, process_update_with_state_store_at,
        },
        virtual_messages::VirtualIdFactory,
    };
    use carapax::types::Update as TelegramUpdate;
    use openplotva_core::{
        UserState, VIP_EVENT_TYPE_ADMIN_ADJUSTMENT, VIP_EVENT_TYPE_ADMIN_REVOKE,
        VIP_EVENT_TYPE_PAYMENT, VIP_EVENT_TYPE_REFUND_REVERSAL, subscription_delta_seconds,
        vip_days_to_seconds,
    };
    use openplotva_storage::{
        DonationCreate, DonationRecord, SubscriptionCreate, SubscriptionRecord, VipCacheRecord,
        VipCacheUpsert, VipEventCreate, VipEventRecord, VipSummaryRecord,
    };
    use openplotva_taskman::{
        CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, ControlPayment,
        DEFAULT_PRIORITY, HIGH_PRIORITY, InMemoryTaskQueue, JobPayload, JobType, StatelessJobItem,
        TRANSACTION_PRIORITY, new_control_job_at,
    };
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::json;
    use time::OffsetDateTime;

    use super::{
        AdminVipCommandUpdateHandler, AdminVipCommandUpdateOutcome, AdminVipCommandUpdateRuntime,
        CRYPTO_DONATION_ADDRESSES, InMemoryPaymentControlJobQueue, InMemoryPaymentControlJobStatus,
        InvoiceButtonMessage, InvoiceControlJobOutcome, InvoiceControlJobReport,
        PaymentControlJobOutcome, PaymentControlJobQueue, PaymentControlJobQueueFuture,
        PaymentControlJobReport, PaymentControlJobWorkItem, PaymentControlJobWorkerFuture,
        PaymentControlJobWorkerQueue, PaymentEffectsFuture, PaymentInvoiceCommandUpdateRoute,
        PaymentInvoiceControlJobBuild, PaymentInvoiceEffects, PaymentInvoiceLinkFuture,
        PaymentRedirectMessage, PaymentRuntimeEffects, PaymentStoreFuture, PaymentTextMessage,
        PaymentUpdateHandler, PaymentUpdateRoute, PreCheckoutOutcome, PreCheckoutPaymentEffects,
        PreCheckoutUpdateRoute, SuccessfulPayment, SuccessfulPaymentControlJobUpdateRoute,
        SuccessfulPaymentDispatcherEffects, SuccessfulPaymentEffects, SuccessfulPaymentMessage,
        SuccessfulPaymentOutcome, SuccessfulPaymentStore, SuccessfulPaymentUpdateRoute,
        VipCancellationCallbackOutcome, VipCancellationEffectFuture, VipCancellationEffects,
        VipCancellationStore, crypto_donation_html, crypto_donation_plain_text,
        crypto_donation_rich_html, donation_invoice_message_text,
        enqueue_payment_invoice_command_update_or_else_at,
        enqueue_payment_invoice_command_update_with_vip_status_or_else_at,
        enqueue_successful_payment_update_or_else_at, execute_donate_invoice_control_job,
        execute_payment_control_job_at, execute_vip_invoice_control_job,
        handle_admin_vip_command_update_or_else_at, handle_payment_update_or_else_at,
        handle_pre_checkout_update_or_else, handle_successful_payment_update_or_else_at,
        handle_vip_cancellation_callback_update_or_else_at, invoice_button_send_message_method,
        payment_control_job_failure_message, payment_invoice_control_job_from_update_at,
        process_payment_control_job_once_at, process_successful_payment_at,
        subscription_invoice_message_text, subscription_success_text,
        successful_payment_control_job_from_update_at, successful_payment_message_from_control_job,
        successful_payment_message_from_update,
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
    async fn subscription_payment_uses_payment_time_for_delayed_processing()
    -> Result<(), Box<dyn Error>> {
        let paid_at = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let now = paid_at + time::Duration::days(2);
        let expires_at = paid_at + time::Duration::days(30);
        let store = StoreStub::new().with_next_vip_event(VipEventRecord {
            id: 77,
            user_id: 42,
            event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
            delta_seconds: vip_days_to_seconds(28),
            effective_expires_at: expires_at,
            subscription_id: Some(10),
            actor_user_id: None,
            reason: "payment telegram-charge".to_owned(),
            created_at: now,
        });
        let effects = EffectsStub::default();
        let mut message = sample_message("subscription_42", 300);
        message.payment.paid_at = Some(paid_at);
        message.payment.subscription_period_seconds =
            Some(openplotva_telegram::SUBSCRIPTION_PERIOD_SECONDS);

        let report = process_successful_payment_at(&store, &effects, &message, now).await;

        assert_eq!(
            report.outcome,
            SuccessfulPaymentOutcome::SubscriptionProcessed
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
                delta_seconds: vip_days_to_seconds(28),
                subscription_id: Some(10),
                actor_user_id: None,
                reason: Some("payment telegram-charge"),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn subscription_payment_extends_from_current_effective_expiry()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let current_expires_at = now + time::Duration::days(10);
        let target_expires_at = now + time::Duration::days(30);
        let store = StoreStub::new()
            .with_successful_payment_summary(Some(VipSummaryRecord {
                latest_event_id: 76,
                user_id: 42,
                latest_event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
                latest_delta_seconds: vip_days_to_seconds(10),
                effective_expires_at: current_expires_at,
                is_active: true,
                remaining_seconds: vip_days_to_seconds(10),
                latest_subscription_id: Some(9),
                latest_actor_user_id: None,
                latest_reason: "payment previous-charge".to_owned(),
                latest_created_at: now - time::Duration::days(20),
            }))
            .with_next_vip_event(VipEventRecord {
                id: 77,
                user_id: 42,
                event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
                delta_seconds: vip_days_to_seconds(20),
                effective_expires_at: target_expires_at,
                subscription_id: Some(10),
                actor_user_id: None,
                reason: "payment telegram-charge".to_owned(),
                created_at: now,
            });
        let effects = EffectsStub::default();
        let mut message = sample_message("subscription_42", 300);
        message.payment.paid_at = Some(now);
        message.payment.subscription_period_seconds =
            Some(openplotva_telegram::SUBSCRIPTION_PERIOD_SECONDS);

        let report = process_successful_payment_at(&store, &effects, &message, now).await;

        assert_eq!(
            report.outcome,
            SuccessfulPaymentOutcome::SubscriptionProcessed
        );
        assert_eq!(
            store.vip_events(),
            vec![VipEventCreate {
                user_id: 42,
                event_type: VIP_EVENT_TYPE_PAYMENT,
                delta_seconds: vip_days_to_seconds(20),
                subscription_id: Some(10),
                actor_user_id: None,
                reason: Some("payment telegram-charge"),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn subscription_payment_does_not_duplicate_existing_subscription_vip_event()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let expires_at = now + time::Duration::days(30);
        let existing_event = VipEventRecord {
            id: 77,
            user_id: 42,
            event_type: VIP_EVENT_TYPE_PAYMENT.to_owned(),
            delta_seconds: vip_days_to_seconds(30),
            effective_expires_at: expires_at,
            subscription_id: Some(10),
            actor_user_id: None,
            reason: "payment telegram-charge".to_owned(),
            created_at: now,
        };
        let store = StoreStub::new().with_existing_vip_event_by_subscription(existing_event);
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
        assert!(store.vip_events().is_empty());
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
        assert_eq!(job.priority, TRANSACTION_PRIORITY);
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
                paid_at: Some("2024-03-09T16:00:00Z".to_owned()),
                ..ControlPayment::default()
            })
        );
        Ok(())
    }

    #[test]
    fn successful_payment_update_preserves_recurring_subscription_metadata()
    -> Result<(), Box<dyn Error>> {
        let update = sample_recurring_successful_payment_update("subscription_42", 300)?;

        let message = successful_payment_message_from_update(&update)
            .expect("successful payment should be extracted");

        assert_eq!(
            message.payment.paid_at,
            Some(OffsetDateTime::from_unix_timestamp(1_710_000_000)?)
        );
        assert_eq!(
            message.payment.subscription_expiration_date,
            Some(OffsetDateTime::from_unix_timestamp(1_712_592_000)?)
        );
        assert_eq!(message.payment.is_recurring, Some(true));
        assert_eq!(message.payment.is_first_recurring, Some(false));

        let job = successful_payment_control_job_from_update_at(
            &update,
            OffsetDateTime::from_unix_timestamp(1_779_193_800)?,
        )
        .expect("successful payment should build control job");
        let control_payment = job
            .data
            .control_data
            .as_ref()
            .and_then(|data| data.payment.as_ref())
            .expect("control payment");
        assert_eq!(
            control_payment.paid_at.as_deref(),
            Some("2024-03-09T16:00:00Z")
        );
        assert_eq!(
            control_payment.subscription_expiration_date.as_deref(),
            Some("2024-04-08T16:00:00Z")
        );
        assert_eq!(control_payment.is_recurring, Some(true));
        assert_eq!(control_payment.is_first_recurring, Some(false));
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
        assert_eq!(assigned[0].1.priority, TRANSACTION_PRIORITY);
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
        assert_eq!(job.priority, TRANSACTION_PRIORITY);
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
    fn admin_grant_vip_command_args_preserve_go_fields_join_semantics() {
        let args = super::parse_admin_grant_vip_command_args("  -3   @user   custom   reason  ");
        assert!(args.has_target);
        assert_eq!(args.duration, "-3");
        assert_eq!(args.target, "@user");
        assert_eq!(args.reason, "custom reason");

        let missing_target = super::parse_admin_grant_vip_command_args("  5  ");
        assert!(missing_target.has_duration);
        assert!(!missing_target.has_target);
        assert_eq!(missing_target.duration, "5");
        assert_eq!(missing_target.reason, "");
    }

    #[test]
    fn admin_grant_vip_duration_reason_and_success_text_match_go() -> Result<(), Box<dyn Error>> {
        assert_eq!(super::parse_admin_grant_vip_duration("7", true), (7, false));
        assert_eq!(super::parse_admin_grant_vip_duration("0", true), (1, true));
        assert_eq!(
            super::parse_admin_grant_vip_duration("366", true),
            (1, true)
        );
        assert_eq!(super::parse_admin_grant_vip_duration("", false), (1, false));
        assert_eq!(super::admin_grant_vip_reason("  custom  ", 99), "custom");
        assert_eq!(
            super::admin_grant_vip_reason(" ", 99),
            "telegram admin adjustment by 99"
        );
        assert_eq!(super::admin_grant_vip_action_text(-3), "списано");
        assert_eq!(super::admin_grant_vip_action_text(3), "добавлено");

        let expires_at = time::Date::from_calendar_date(2026, time::Month::May, 19)?
            .with_hms(12, 30, 0)?
            .assume_utc();
        let text = super::admin_grant_vip_success_text(42, -3, expires_at, "reason", 77);
        for expected in [
            "User ID: 42",
            "Корректировка: -3 дней (списано)",
            "Действует до: 19.05.2026 12:30",
            "Причина: reason",
            "Event ID: 77",
            "Приоритетное выполнение запросов вне очереди",
        ] {
            assert!(
                text.contains(expected),
                "success text should contain {expected:?}:\n{text}"
            );
        }

        assert!(
            super::admin_grant_vip_usage_text().contains(
                "Использование: /admin_grant_vip [дни=1|-1] [user_id/@username/https://t.me/username] [причина]"
            )
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_grant_vip_command_records_ledger_event_and_success_reply()
    -> Result<(), Box<dyn Error>> {
        let expires_at = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(13, 0, 0)?
            .assume_utc();
        let store = StoreStub::new().with_next_vip_event(VipEventRecord {
            id: 88,
            user_id: 42,
            event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT.to_owned(),
            delta_seconds: vip_days_to_seconds(3),
            effective_expires_at: expires_at,
            subscription_id: None,
            actor_user_id: Some(7),
            reason: "manual grant".to_owned(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        });
        let effects = EffectsStub::default();

        let report = super::handle_admin_grant_vip_command_at(
            &store,
            &effects,
            super::AdminGrantVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                reply_user_id: None,
                arguments: "3 42 manual grant",
            },
        )
        .await;

        assert_eq!(report.outcome, super::AdminGrantVipCommandOutcome::Granted);
        assert_eq!(
            store.vip_events(),
            vec![VipEventCreate {
                user_id: 42,
                event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT,
                delta_seconds: vip_days_to_seconds(3),
                subscription_id: None,
                actor_user_id: Some(7),
                reason: Some("manual grant"),
            }]
        );
        assert_eq!(effects.invalidated_vip_users(), vec![42]);
        let sent = effects.sent_payment_texts();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].chat_id, 900);
        assert_eq!(sent[0].reply_to_message_id, 101);
        assert_eq!(sent[0].parse_mode, "");
        assert!(sent[0].text.contains("VIP корректировка успешно записана"));
        assert!(sent[0].text.contains("User ID: 42"));
        assert!(sent[0].text.contains("Корректировка: 3 дней (добавлено)"));
        assert!(sent[0].text.contains("Event ID: 88"));
        Ok(())
    }

    #[tokio::test]
    async fn admin_grant_vip_command_sends_usage_without_target() {
        let store = StoreStub::new();
        let effects = EffectsStub::default();

        let report = super::handle_admin_grant_vip_command_at(
            &store,
            &effects,
            super::AdminGrantVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                reply_user_id: None,
                arguments: "",
            },
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminGrantVipCommandOutcome::MissingTarget
        );
        assert!(store.vip_events().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        assert_eq!(
            effects.sent_texts(),
            vec![super::admin_grant_vip_usage_text()]
        );
    }

    #[tokio::test]
    async fn admin_grant_vip_command_uses_reply_target_and_default_reason()
    -> Result<(), Box<dyn Error>> {
        let expires_at = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(13, 0, 0)?
            .assume_utc();
        let store = StoreStub::new().with_next_vip_event(VipEventRecord {
            id: 89,
            user_id: 55,
            event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT.to_owned(),
            delta_seconds: vip_days_to_seconds(5),
            effective_expires_at: expires_at,
            subscription_id: None,
            actor_user_id: Some(7),
            reason: "telegram admin adjustment by 7".to_owned(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        });
        let effects = EffectsStub::default();

        let report = super::handle_admin_grant_vip_command_at(
            &store,
            &effects,
            super::AdminGrantVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                reply_user_id: Some(55),
                arguments: "5",
            },
        )
        .await;

        assert_eq!(report.outcome, super::AdminGrantVipCommandOutcome::Granted);
        assert_eq!(report.target_user_id, Some(55));
        assert_eq!(
            store.vip_events()[0],
            VipEventCreate {
                user_id: 55,
                event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT,
                delta_seconds: vip_days_to_seconds(5),
                subscription_id: None,
                actor_user_id: Some(7),
                reason: Some("telegram admin adjustment by 7"),
            }
        );
        assert!(
            effects.sent_texts()[0].contains("Причина: telegram admin adjustment by 7"),
            "success text should include default admin reason"
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_grant_vip_command_warns_and_defaults_invalid_duration()
    -> Result<(), Box<dyn Error>> {
        let expires_at = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(13, 0, 0)?
            .assume_utc();
        let store = StoreStub::new().with_next_vip_event(VipEventRecord {
            id: 90,
            user_id: 42,
            event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT.to_owned(),
            delta_seconds: vip_days_to_seconds(1),
            effective_expires_at: expires_at,
            subscription_id: None,
            actor_user_id: Some(7),
            reason: "bad duration".to_owned(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        });
        let effects = EffectsStub::default();

        let report = super::handle_admin_grant_vip_command_at(
            &store,
            &effects,
            super::AdminGrantVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                reply_user_id: None,
                arguments: "0 42 bad duration",
            },
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminGrantVipCommandOutcome::GrantedWithDefaultDuration
        );
        assert_eq!(store.vip_events()[0].delta_seconds, vip_days_to_seconds(1));
        let sent = effects.sent_texts();
        assert_eq!(sent.len(), 2);
        assert!(sent[0].contains("Неверное количество дней"));
        assert!(sent[1].contains("Корректировка: 1 дней (добавлено)"));
        Ok(())
    }

    #[tokio::test]
    async fn admin_grant_vip_command_resolves_username_and_tme_targets()
    -> Result<(), Box<dyn Error>> {
        let expires_at = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(13, 0, 0)?
            .assume_utc();
        let store = StoreStub::new()
            .with_username_user("bob", 42)
            .with_username_user("carol", 43)
            .with_next_vip_event(VipEventRecord {
                id: 91,
                user_id: 42,
                event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT.to_owned(),
                delta_seconds: vip_days_to_seconds(2),
                effective_expires_at: expires_at,
                subscription_id: None,
                actor_user_id: Some(7),
                reason: "username".to_owned(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .with_next_vip_event(VipEventRecord {
                id: 92,
                user_id: 43,
                event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT.to_owned(),
                delta_seconds: vip_days_to_seconds(4),
                effective_expires_at: expires_at,
                subscription_id: None,
                actor_user_id: Some(7),
                reason: "link".to_owned(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            });
        let effects = EffectsStub::default();

        let username_report = super::handle_admin_grant_vip_command_at(
            &store,
            &effects,
            super::AdminGrantVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                reply_user_id: None,
                arguments: "2 @bob username",
            },
        )
        .await;
        let link_report = super::handle_admin_grant_vip_command_at(
            &store,
            &effects,
            super::AdminGrantVipCommand {
                chat_id: 900,
                message_id: 102,
                actor_user_id: 7,
                reply_user_id: None,
                arguments: "4 https://t.me/carol link",
            },
        )
        .await;

        assert_eq!(username_report.target_user_id, Some(42));
        assert_eq!(link_report.target_user_id, Some(43));
        assert_eq!(store.vip_events()[0].user_id, 42);
        assert_eq!(store.vip_events()[1].user_id, 43);
        assert_eq!(effects.invalidated_vip_users(), vec![42, 43]);
        Ok(())
    }

    #[tokio::test]
    async fn admin_grant_vip_command_reports_target_resolution_error() {
        let store = StoreStub::new();
        let effects = EffectsStub::default();

        let report = super::handle_admin_grant_vip_command_at(
            &store,
            &effects,
            super::AdminGrantVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                reply_user_id: None,
                arguments: "3 https://t.me/missing reason",
            },
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminGrantVipCommandOutcome::TargetError
        );
        assert!(store.vip_events().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        assert_eq!(
            effects.sent_texts(),
            vec!["❌ user with username https://t.me/missing not found"]
        );
    }

    #[test]
    fn admin_cancel_vip_command_args_and_error_texts_match_go() {
        let missing = super::parse_admin_cancel_vip_command_args(" ");
        assert!(!missing.has_target);
        assert!(!missing.with_refund);

        let revoke = super::parse_admin_cancel_vip_command_args("42");
        assert!(revoke.has_target);
        assert_eq!(revoke.target, "42");
        assert!(!revoke.with_refund);

        for raw in ["42 true", "42 TRUE", "42 1", "42 yes", "42 YES"] {
            assert!(
                super::parse_admin_cancel_vip_command_args(raw).with_refund,
                "{raw} should request refund"
            );
        }
        for raw in ["42 false", "42 no", "42 0", "42 nope"] {
            assert!(
                !super::parse_admin_cancel_vip_command_args(raw).with_refund,
                "{raw} should not request refund"
            );
        }

        assert!(
            super::admin_cancel_vip_parse_error_text("missing user identifier")
                .contains("Использование: /admin_cancel_vip")
        );
        assert_eq!(
            super::admin_cancel_vip_parse_error_text("user with username @missing not found"),
            "❌ Неверный формат User ID. Используйте числовой ID."
        );
    }

    #[tokio::test]
    async fn admin_cancel_vip_command_records_revoke_event_and_success_reply()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(12, 0, 0)?
            .assume_utc();
        let expires_at = now + time::Duration::days(5);
        let store = StoreStub::new()
            .with_admin_cancel_summary(active_vip_summary(now, expires_at))
            .with_admin_cancel_updated_summary(None)
            .with_next_vip_event(VipEventRecord {
                id: 93,
                user_id: 42,
                event_type: VIP_EVENT_TYPE_ADMIN_REVOKE.to_owned(),
                delta_seconds: -time::Duration::days(5).whole_seconds(),
                effective_expires_at: now,
                subscription_id: None,
                actor_user_id: Some(7),
                reason: "telegram admin revoke by 7".to_owned(),
                created_at: now,
            });
        let effects = EffectsStub::default();

        let report = super::handle_admin_cancel_vip_command_at(
            &store,
            &effects,
            super::AdminCancelVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "42",
            },
            now,
        )
        .await;

        assert_eq!(report.outcome, super::AdminCancelVipCommandOutcome::Revoked);
        assert_eq!(
            store.vip_events(),
            vec![VipEventCreate {
                user_id: 42,
                event_type: VIP_EVENT_TYPE_ADMIN_REVOKE,
                delta_seconds: -time::Duration::days(5).whole_seconds(),
                subscription_id: None,
                actor_user_id: Some(7),
                reason: Some("telegram admin revoke by 7"),
            }]
        );
        assert_eq!(effects.invalidated_vip_users(), vec![42]);
        let sent = effects.sent_payment_texts();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].chat_id, 900);
        assert_eq!(sent[0].reply_to_message_id, 101);
        assert!(sent[0].text.contains("VIP статус успешно обработан"));
        assert!(sent[0].text.contains("User ID: 42"));
        assert!(
            sent[0]
                .text
                .contains("Был действителен до: 25.05.2026 12:00")
        );
        assert!(
            sent[0]
                .text
                .contains("Отозван администратором: 20.05.2026 12:00")
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_cancel_vip_command_refunds_active_subscription_and_records_reversal()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(12, 0, 0)?
            .assume_utc();
        let expires_at = now + time::Duration::days(30);
        let subscription = active_subscription(now, expires_at);
        let store = StoreStub::new()
            .with_admin_cancel_summary(active_vip_summary(now, expires_at))
            .with_admin_cancel_active_subscription(Some(subscription.clone()))
            .with_admin_cancel_updated_summary(None)
            .with_next_vip_event(VipEventRecord {
                id: 94,
                user_id: 42,
                event_type: VIP_EVENT_TYPE_REFUND_REVERSAL.to_owned(),
                delta_seconds: -subscription_delta_seconds(
                    &subscription.telegram_payment_charge_id,
                    subscription.created_at,
                    subscription.expires_at,
                    openplotva_telegram::SUBSCRIPTION_DURATION_DAYS,
                ),
                effective_expires_at: now,
                subscription_id: Some(subscription.id),
                actor_user_id: Some(7),
                reason: "telegram admin refund by 7".to_owned(),
                created_at: now,
            });
        let effects = EffectsStub::default();

        let report = super::handle_admin_cancel_vip_command_at(
            &store,
            &effects,
            super::AdminCancelVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "42 true",
            },
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminCancelVipCommandOutcome::Refunded
        );
        assert_eq!(report.event_id, Some(94));
        assert_eq!(
            effects.refund_requests(),
            vec![(42, "telegram-charge".to_owned())]
        );
        assert_eq!(store.refunded_subscription_ids(), vec![10]);
        assert_eq!(
            store.vip_events(),
            vec![VipEventCreate {
                user_id: 42,
                event_type: VIP_EVENT_TYPE_REFUND_REVERSAL,
                delta_seconds: -subscription_delta_seconds(
                    &subscription.telegram_payment_charge_id,
                    subscription.created_at,
                    subscription.expires_at,
                    openplotva_telegram::SUBSCRIPTION_DURATION_DAYS,
                ),
                subscription_id: Some(10),
                actor_user_id: Some(7),
                reason: Some("telegram admin refund by 7"),
            }]
        );
        assert_eq!(
            effects.direct_texts(),
            vec![(
                42,
                "💸 Вам был возвращен платеж в размере 300 звезд по транзакции telegram-charge."
                    .to_owned()
            )]
        );
        assert_eq!(effects.invalidated_vip_users(), vec![42, 42]);
        let sent = effects.sent_payment_texts();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].text.contains("VIP статус успешно обработан"));
        assert!(sent[0].text.contains("Возврат средств: выполнен"));
        assert!(!sent[0].text.contains("Отозван администратором"));
        Ok(())
    }

    #[tokio::test]
    async fn admin_cancel_vip_command_refund_reports_missing_paid_subscription()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(12, 0, 0)?
            .assume_utc();
        let store = StoreStub::new()
            .with_admin_cancel_summary(active_vip_summary(now, now + time::Duration::days(30)))
            .with_admin_cancel_active_subscription(None);
        let effects = EffectsStub::default();

        let report = super::handle_admin_cancel_vip_command_at(
            &store,
            &effects,
            super::AdminCancelVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "42 true",
            },
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminCancelVipCommandOutcome::NoPaidSubscription
        );
        assert!(effects.refund_requests().is_empty());
        assert!(store.refunded_subscription_ids().is_empty());
        assert!(store.vip_events().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        assert_eq!(
            effects.sent_texts(),
            vec!["ℹ️ У пользователя 42 нет активной оплаченной VIP подписки для возврата."]
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_cancel_vip_command_refund_reports_paid_subscription_lookup_error()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(12, 0, 0)?
            .assume_utc();
        let store = StoreStub::new()
            .with_admin_cancel_summary(active_vip_summary(now, now + time::Duration::days(30)))
            .with_admin_cancel_active_subscription_error(StubError::Request);
        let effects = EffectsStub::default();

        let report = super::handle_admin_cancel_vip_command_at(
            &store,
            &effects,
            super::AdminCancelVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "42 true",
            },
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminCancelVipCommandOutcome::PaidSubscriptionLookupError
        );
        assert!(effects.refund_requests().is_empty());
        assert!(store.refunded_subscription_ids().is_empty());
        assert!(store.vip_events().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        assert_eq!(
            effects.sent_texts(),
            vec!["❌ Ошибка при проверке оплаченной подписки пользователя."]
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_cancel_vip_command_refund_failure_stops_before_db_writes()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(12, 0, 0)?
            .assume_utc();
        let store = StoreStub::new()
            .with_admin_cancel_summary(active_vip_summary(now, now + time::Duration::days(30)))
            .with_admin_cancel_active_subscription(Some(active_subscription(
                now,
                now + time::Duration::days(30),
            )));
        let effects = EffectsStub::default().with_next_refund_error(StubError::Request);

        let report = super::handle_admin_cancel_vip_command_at(
            &store,
            &effects,
            super::AdminCancelVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "42 true",
            },
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminCancelVipCommandOutcome::RefundFailed
        );
        assert_eq!(
            effects.refund_requests(),
            vec![(42, "telegram-charge".to_owned())]
        );
        assert!(effects.direct_texts().is_empty());
        assert!(store.refunded_subscription_ids().is_empty());
        assert!(store.vip_events().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        assert_eq!(effects.sent_payment_texts().len(), 1);
        assert!(
            effects.sent_payment_texts()[0]
                .text
                .contains("Не удалось выполнить возврат средств: request")
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_cancel_vip_command_refund_db_failure_reports_not_cancelled()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(12, 0, 0)?
            .assume_utc();
        let store = StoreStub::new()
            .with_admin_cancel_summary(active_vip_summary(now, now + time::Duration::days(30)))
            .with_admin_cancel_active_subscription(Some(active_subscription(
                now,
                now + time::Duration::days(30),
            )))
            .with_mark_subscription_refunded_error(StubError::Request);
        let effects = EffectsStub::default();

        let report = super::handle_admin_cancel_vip_command_at(
            &store,
            &effects,
            super::AdminCancelVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "42 true",
            },
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminCancelVipCommandOutcome::RefundFailed
        );
        assert_eq!(
            effects.refund_requests(),
            vec![(42, "telegram-charge".to_owned())]
        );
        assert_eq!(store.refunded_subscription_ids(), vec![10]);
        assert_eq!(effects.direct_texts().len(), 1);
        assert!(store.vip_events().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        assert_eq!(effects.sent_payment_texts().len(), 1);
        assert!(
            effects.sent_payment_texts()[0]
                .text
                .contains("не удалось пометить подписку как refunded: request")
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_cancel_vip_command_reports_no_active_status() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let store = StoreStub::new().with_admin_cancel_updated_summary(None);
        let effects = EffectsStub::default();

        let report = super::handle_admin_cancel_vip_command_at(
            &store,
            &effects,
            super::AdminCancelVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "42",
            },
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminCancelVipCommandOutcome::NoActiveVip
        );
        assert!(store.vip_events().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        assert_eq!(
            effects.sent_texts(),
            vec!["ℹ️ У пользователя 42 нет активного VIP статуса."]
        );
    }

    #[tokio::test]
    async fn admin_cancel_vip_command_reports_status_lookup_error() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let store = StoreStub::new().with_admin_cancel_summary_error(StubError::Request);
        let effects = EffectsStub::default();

        let report = super::handle_admin_cancel_vip_command_at(
            &store,
            &effects,
            super::AdminCancelVipCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "42",
            },
            now,
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminCancelVipCommandOutcome::StatusLookupError
        );
        assert!(store.vip_events().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        assert_eq!(
            effects.sent_texts(),
            vec!["❌ Ошибка при проверке статуса VIP пользователя."]
        );
    }

    #[test]
    fn admin_refund_helpers_match_go_texts_and_targets() {
        let mut subscription =
            active_subscription(OffsetDateTime::UNIX_EPOCH, OffsetDateTime::UNIX_EPOCH);
        subscription.user_id = 1_717_359_759;
        let target =
            super::admin_refund_subscription_target(subscription).expect("subscription target");
        assert_eq!(target.user_id(), 1_717_359_759);
        assert_eq!(target.amount_stars(), 1);

        let zero_subscription = SubscriptionRecord {
            id: 0,
            user_id: 42,
            telegram_payment_charge_id: "charge-zero".to_owned(),
            provider_payment_charge_id: String::new(),
            expires_at: OffsetDateTime::UNIX_EPOCH,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            canceled_at: None,
            refunded_at: None,
        };
        assert!(super::admin_refund_subscription_target(zero_subscription).is_none());

        let donation = DonationRecord {
            id: 20,
            user_id: 42,
            telegram_payment_charge_id: "charge-donation".to_owned(),
            provider_payment_charge_id: String::new(),
            amount_stars: 123,
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let donation_target =
            super::admin_refund_donation_target(donation).expect("donation target");
        assert_eq!(donation_target.user_id(), 42);
        assert_eq!(donation_target.amount_stars(), 123);
        assert!(donation_target.is_valid());

        assert_eq!(
            super::first_admin_refund_charge_id("  charge-1 tail"),
            Some("charge-1")
        );
        assert_eq!(
            super::admin_refund_success_text("charge-1", 42, 300),
            "✅ Возврат средств для транзакции charge-1 (пользователь ID 42, сумма 300 звезд) успешно выполнен."
        );
        assert_eq!(
            super::admin_refund_notice_text("charge-1", 300),
            "💸 Вам был возвращен платеж в размере 300 звезд по транзакции charge-1."
        );
        assert!(
            super::admin_refund_synthetic_subscription_text("charge-1")
                .contains("legacy admin grant")
        );
    }

    #[tokio::test]
    async fn admin_refund_command_refunds_subscription_and_records_reversal()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(12, 0, 0)?
            .assume_utc();
        let expires_at = now + time::Duration::days(30);
        let mut subscription = active_subscription(now, expires_at);
        subscription.user_id = 1_717_359_759;
        subscription.telegram_payment_charge_id = "charge-sub".to_owned();
        let store = StoreStub::new()
            .with_admin_refund_subscription(Some(subscription.clone()))
            .with_next_vip_event(VipEventRecord {
                id: 95,
                user_id: subscription.user_id,
                event_type: VIP_EVENT_TYPE_REFUND_REVERSAL.to_owned(),
                delta_seconds: -subscription_delta_seconds(
                    &subscription.telegram_payment_charge_id,
                    subscription.created_at,
                    subscription.expires_at,
                    openplotva_telegram::SUBSCRIPTION_DURATION_DAYS,
                ),
                effective_expires_at: now,
                subscription_id: Some(subscription.id),
                actor_user_id: Some(7),
                reason: "admin refund charge-sub".to_owned(),
                created_at: now,
            });
        let effects = EffectsStub::default();

        let report = super::handle_admin_refund_command_at(
            &store,
            &effects,
            super::AdminRefundCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "charge-sub",
                telegram_payment_charge_id: "charge-sub",
            },
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminRefundCommandOutcome::RefundedSubscription
        );
        assert_eq!(report.target_user_id, Some(1_717_359_759));
        assert_eq!(report.amount_stars, Some(1));
        assert_eq!(
            effects.refund_requests(),
            vec![(1_717_359_759, "charge-sub".to_owned())]
        );
        assert_eq!(store.refunded_subscription_ids(), vec![10]);
        assert_eq!(
            store.vip_events(),
            vec![VipEventCreate {
                user_id: 1_717_359_759,
                event_type: VIP_EVENT_TYPE_REFUND_REVERSAL,
                delta_seconds: -subscription_delta_seconds(
                    &subscription.telegram_payment_charge_id,
                    subscription.created_at,
                    subscription.expires_at,
                    openplotva_telegram::SUBSCRIPTION_DURATION_DAYS,
                ),
                subscription_id: Some(10),
                actor_user_id: Some(7),
                reason: Some("admin refund charge-sub"),
            }]
        );
        assert_eq!(effects.invalidated_vip_users(), vec![1_717_359_759]);
        assert_eq!(
            effects.direct_texts(),
            vec![(
                1_717_359_759,
                "💸 Вам был возвращен платеж в размере 1 звезд по транзакции charge-sub."
                    .to_owned()
            )]
        );
        assert_eq!(effects.sent_payment_texts().len(), 1);
        assert!(
            effects.sent_payment_texts()[0]
                .text
                .contains("сумма 1 звезд")
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_refund_command_refunds_donation_and_deletes_row_best_effort()
    -> Result<(), Box<dyn Error>> {
        let donation = DonationRecord {
            id: 20,
            user_id: 42,
            telegram_payment_charge_id: "charge-donation".to_owned(),
            provider_payment_charge_id: String::new(),
            amount_stars: 123,
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let store = StoreStub::new()
            .with_admin_refund_subscription(None)
            .with_admin_refund_donation(Some(donation))
            .with_delete_donation_error(StubError::Request);
        let effects = EffectsStub::default();

        let report = super::handle_admin_refund_command_at(
            &store,
            &effects,
            super::AdminRefundCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "charge-donation",
                telegram_payment_charge_id: "charge-donation",
            },
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminRefundCommandOutcome::RefundedDonation
        );
        assert_eq!(
            effects.refund_requests(),
            vec![(42, "charge-donation".to_owned())]
        );
        assert_eq!(store.deleted_donation_ids(), vec![20]);
        assert!(store.vip_events().is_empty());
        assert!(effects.invalidated_vip_users().is_empty());
        assert_eq!(
            effects.direct_texts(),
            vec![(
                42,
                "💸 Вам был возвращен платеж в размере 123 звезд по транзакции charge-donation."
                    .to_owned()
            )]
        );
        assert!(
            effects.sent_payment_texts()[0]
                .text
                .contains("сумма 123 звезд")
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_refund_command_reports_lookup_errors_and_not_found() {
        let effects = EffectsStub::default();
        let subscription_error =
            StoreStub::new().with_admin_refund_subscription_error(StubError::Request);
        let report = super::handle_admin_refund_command_at(
            &subscription_error,
            &effects,
            super::AdminRefundCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "charge-sub",
                telegram_payment_charge_id: "charge-sub",
            },
        )
        .await;
        assert_eq!(
            report.outcome,
            super::AdminRefundCommandOutcome::SubscriptionLookupError
        );

        let effects = EffectsStub::default();
        let donation_error = StoreStub::new()
            .with_admin_refund_subscription(None)
            .with_admin_refund_donation_error(StubError::Request);
        let report = super::handle_admin_refund_command_at(
            &donation_error,
            &effects,
            super::AdminRefundCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "charge-donation",
                telegram_payment_charge_id: "charge-donation",
            },
        )
        .await;
        assert_eq!(
            report.outcome,
            super::AdminRefundCommandOutcome::DonationLookupError
        );

        let effects = EffectsStub::default();
        let not_found = StoreStub::new()
            .with_admin_refund_subscription(None)
            .with_admin_refund_donation(None);
        let report = super::handle_admin_refund_command_at(
            &not_found,
            &effects,
            super::AdminRefundCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "missing-charge",
                telegram_payment_charge_id: "missing-charge",
            },
        )
        .await;
        assert_eq!(report.outcome, super::AdminRefundCommandOutcome::NotFound);
        assert_eq!(
            effects.sent_texts(),
            vec!["🤷 Транзакция с ID missing-charge не найдена (ни подписка, ни донат)."]
        );
    }

    #[tokio::test]
    async fn admin_refund_command_rejects_synthetic_subscription_before_api()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::UNIX_EPOCH;
        let mut subscription = active_subscription(now, now + time::Duration::days(1));
        subscription.telegram_payment_charge_id = "admin_grant_42".to_owned();
        let store = StoreStub::new().with_admin_refund_subscription(Some(subscription));
        let effects = EffectsStub::default();

        let report = super::handle_admin_refund_command_at(
            &store,
            &effects,
            super::AdminRefundCommand {
                chat_id: 900,
                message_id: 101,
                actor_user_id: 7,
                arguments: "admin_grant_42",
                telegram_payment_charge_id: "admin_grant_42",
            },
        )
        .await;

        assert_eq!(
            report.outcome,
            super::AdminRefundCommandOutcome::SyntheticSubscription
        );
        assert!(effects.refund_requests().is_empty());
        assert!(store.refunded_subscription_ids().is_empty());
        assert!(store.vip_events().is_empty());
        assert!(effects.sent_texts()[0].contains("legacy admin grant"));
        Ok(())
    }

    #[tokio::test]
    async fn admin_refund_update_missing_charge_queues_go_ephemeral_usage()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = openplotva_telegram::DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        );
        let store = StoreStub::new();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();
        let admin_ids = [42];
        let next_id = Arc::new(AtomicU64::new(1));
        let next_virtual_id: VirtualIdFactory = {
            let next_id = Arc::clone(&next_id);
            Arc::new(move || {
                format!(
                    "admin-refund-vmsg-{}",
                    next_id.fetch_add(1, Ordering::Relaxed)
                )
            })
        };

        let outcome = handle_admin_vip_command_update_or_else_at(
            AdminVipCommandUpdateRuntime {
                queue: &dispatcher,
                vip: &store,
                effects: &effects,
                admin_ids: &admin_ids,
                bot_username: "PlotvaBot",
                next_virtual_id: &next_virtual_id,
                now: OffsetDateTime::UNIX_EPOCH,
            },
            sample_payment_command_update("/admin_refund")?,
            |update| next.handle_update(update),
        )
        .await?;

        assert!(matches!(
            outcome,
            AdminVipCommandUpdateOutcome::Refund(report)
                if report.outcome == super::AdminRefundCommandOutcome::MissingChargeId
        ));
        assert!(next.calls().is_empty());
        assert!(effects.sent_texts().is_empty());
        let item = dispatcher
            .dequeue_immediate()
            .expect("queued admin refund usage");
        assert_eq!(
            item.ephemeral_delete_after(),
            Some(std::time::Duration::from_secs(60))
        );
        let method = match item.into_method().expect("admin refund usage method") {
            openplotva_telegram::TelegramOutboundMethod::SendMessage(method) => method,
            other => panic!("unexpected admin refund usage method: {other:?}"),
        };
        let payload = serde_json::to_value(method)?;
        assert_eq!(
            payload["text"],
            json!(super::ADMIN_REFUND_MISSING_ARGUMENT_TEXT)
        );
        assert_eq!(payload["reply_parameters"]["message_id"], json!(100));
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
        assert_eq!(assigned[0].1.priority, TRANSACTION_PRIORITY);
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
    async fn cancel_vip_callback_edits_confirmation_and_acks() -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let store = VipStatusStoreStub::new();
        let effects = EffectsStub::default();

        let outcome = handle_vip_cancellation_callback_update_or_else_at(
            &store,
            &effects,
            sample_vip_callback_update("cancel_vip", 42)?,
            now,
            |_update| async { Ok::<(), std::io::Error>(()) },
        )
        .await?;

        assert_eq!(outcome, VipCancellationCallbackOutcome::ConfirmationShown);
        let methods = effects.vip_callback_methods();
        assert_eq!(
            method_names(&methods),
            vec!["editMessageText", "answerCallbackQuery"]
        );
        assert_eq!(
            methods[0].1["text"],
            json!("Вы уверены, что хотите отменить вашу VIP подписку? 😢")
        );
        assert_eq!(methods[0].1["parse_mode"], json!("HTML"));
        assert_eq!(
            methods[0].1["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            json!("{\"action\":\"confirm_cancel_vip\"}")
        );
        assert_eq!(
            methods[0].1["reply_markup"]["inline_keyboard"][0][1]["callback_data"],
            json!("{\"action\":\"back_to_vip_status\"}")
        );
        assert!(methods[1].1.get("text").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn unknown_vip_callback_action_delegates_without_panic() -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let store = VipStatusStoreStub::new();
        let effects = EffectsStub::default();
        let delegated = Arc::new(AtomicU64::new(0));
        let delegated_ref = Arc::clone(&delegated);

        let outcome = handle_vip_cancellation_callback_update_or_else_at(
            &store,
            &effects,
            sample_vip_callback_update("unknown_vip_action", 42)?,
            now,
            move |_update| {
                let delegated_ref = Arc::clone(&delegated_ref);
                async move {
                    delegated_ref.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), std::io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(outcome, VipCancellationCallbackOutcome::Delegated);
        assert_eq!(delegated.load(Ordering::SeqCst), 1);
        assert!(effects.vip_callback_methods().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_cancel_vip_callback_delegates_and_edits_confirmation_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-cancel-vip-callback:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&sample_vip_callback_update("cancel_vip", 42)?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(StdDuration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded VIP callback update"))?;

        let state_store = UpdateStateStoreStub::default();
        let callback_effects = CallbackAckEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::default();
        let store = VipStatusStoreStub::new();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();
        let prehandler_route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&prehandler_route);
        let vip_outcome = Arc::new(Mutex::new(None));
        let captured_outcome = Arc::clone(&vip_outcome);
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();

        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: StdDuration::from_millis(1),
                state_timeout: StdDuration::from_secs(1),
                handle_timeout: StdDuration::from_secs(1),
                side_effect_max_age: StdDuration::from_secs(60),
                worker_limit: 1,
            },
            UNIX_EPOCH + StdDuration::from_secs(1_710_000_000),
            &state_store,
            |update| async {
                let route = handle_callback_query_update_or_else(
                    &callback_effects,
                    &rate_limit,
                    update,
                    |update| async {
                        let outcome = handle_vip_cancellation_callback_update_or_else_at(
                            &store,
                            &effects,
                            update,
                            now,
                            |update| next.handle_update(update),
                        )
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))?;
                        *captured_outcome.lock().expect("VIP outcome") = Some(outcome);
                        Ok::<(), io::Error>(())
                    },
                )
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
                *captured_route.lock().expect("callback route") = Some(route);
                Ok::<(), io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 1001);
        assert_eq!(report.update_name, "callback_query");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned()
            ]
        );
        assert_eq!(
            *prehandler_route.lock().expect("callback route"),
            Some(CallbackQueryUpdateRoute::HandlerDelegated {
                handler: openplotva_telegram::CallbackHandlerKind::CancelVip,
                action: "cancel_vip".to_owned(),
            })
        );
        assert_eq!(
            *vip_outcome.lock().expect("VIP outcome"),
            Some(VipCancellationCallbackOutcome::ConfirmationShown)
        );
        assert!(callback_effects.methods().is_empty());
        assert_eq!(next.calls(), Vec::<i64>::new());

        let methods = effects.vip_callback_methods();
        assert_eq!(
            method_names(&methods),
            vec!["editMessageText", "answerCallbackQuery"]
        );
        assert_eq!(methods[0].1["chat_id"], json!(42));
        assert_eq!(methods[0].1["message_id"], json!(555));
        assert_eq!(
            methods[0].1["text"],
            json!("Вы уверены, что хотите отменить вашу VIP подписку? 😢")
        );
        assert_eq!(
            methods[0].1["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            json!("{\"action\":\"confirm_cancel_vip\"}")
        );
        assert_eq!(methods[1].1["callback_query_id"], json!("vip-callback-id"));
        assert!(methods[1].1.get("text").is_none());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_back_to_vip_status_callback_restores_status_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-back-vip-callback:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&sample_vip_callback_update("back_to_vip_status", 42)?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(StdDuration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded VIP back callback update"))?;

        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let state_store = UpdateStateStoreStub::default();
        let callback_effects = CallbackAckEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::default();
        let store = VipStatusStoreStub::new()
            .with_summary(active_vip_summary(now, now + time::Duration::days(3)))
            .with_active_subscription(active_subscription(now, now + time::Duration::days(3)));
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();
        let prehandler_route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&prehandler_route);
        let vip_outcome = Arc::new(Mutex::new(None));
        let captured_outcome = Arc::clone(&vip_outcome);

        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: StdDuration::from_millis(1),
                state_timeout: StdDuration::from_secs(1),
                handle_timeout: StdDuration::from_secs(1),
                side_effect_max_age: StdDuration::from_secs(60),
                worker_limit: 1,
            },
            UNIX_EPOCH + StdDuration::from_secs(1_710_000_000),
            &state_store,
            |update| async {
                let route = handle_callback_query_update_or_else(
                    &callback_effects,
                    &rate_limit,
                    update,
                    |update| async {
                        let outcome = handle_vip_cancellation_callback_update_or_else_at(
                            &store,
                            &effects,
                            update,
                            now,
                            |update| next.handle_update(update),
                        )
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))?;
                        *captured_outcome.lock().expect("VIP outcome") = Some(outcome);
                        Ok::<(), io::Error>(())
                    },
                )
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
                *captured_route.lock().expect("callback route") = Some(route);
                Ok::<(), io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 1001);
        assert_eq!(report.update_name, "callback_query");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned()
            ]
        );
        assert_eq!(
            *prehandler_route.lock().expect("callback route"),
            Some(CallbackQueryUpdateRoute::HandlerDelegated {
                handler: openplotva_telegram::CallbackHandlerKind::BackToVipStatus,
                action: "back_to_vip_status".to_owned(),
            })
        );
        assert_eq!(
            *vip_outcome.lock().expect("VIP outcome"),
            Some(VipCancellationCallbackOutcome::BackStatusShown)
        );
        assert!(callback_effects.methods().is_empty());
        assert_eq!(next.calls(), Vec::<i64>::new());

        let methods = effects.vip_callback_methods();
        assert_eq!(
            method_names(&methods),
            vec!["editMessageText", "answerCallbackQuery"]
        );
        assert_eq!(methods[0].1["chat_id"], json!(42));
        assert_eq!(methods[0].1["message_id"], json!(555));
        assert!(
            methods[0].1["text"]
                .as_str()
                .expect("status text")
                .contains("У вас уже есть активный VIP статус")
        );
        assert_eq!(
            methods[0].1["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            json!("{\"action\":\"cancel_vip\"}")
        );
        assert_eq!(methods[1].1["callback_query_id"], json!("vip-callback-id"));
        assert!(methods[1].1.get("text").is_none());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn confirm_cancel_vip_callback_cancels_marks_invalidates_edits_success_and_acks()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let expires_at = now + time::Duration::days(9);
        let store = VipStatusStoreStub::new()
            .with_summary(active_vip_summary(now, expires_at))
            .with_active_subscription(active_subscription(now, expires_at));
        let effects = EffectsStub::default();

        let outcome = handle_vip_cancellation_callback_update_or_else_at(
            &store,
            &effects,
            sample_vip_callback_update("confirm_cancel_vip", 42)?,
            now,
            |_update| async { Ok::<(), std::io::Error>(()) },
        )
        .await?;

        assert_eq!(outcome, VipCancellationCallbackOutcome::Canceled);
        assert_eq!(store.canceled_subscription_ids(), vec![10]);
        assert_eq!(effects.invalidated_vip_users(), vec![42]);
        let methods = effects.vip_callback_methods();
        assert_eq!(
            method_names(&methods),
            vec![
                "editUserStarSubscription",
                "editMessageText",
                "answerCallbackQuery"
            ]
        );
        assert_eq!(methods[0].1["user_id"], json!(42));
        assert_eq!(
            methods[0].1["telegram_payment_charge_id"],
            json!("telegram-charge")
        );
        assert_eq!(methods[0].1["is_canceled"], json!(true));
        assert!(
            methods[1].1["text"]
                .as_str()
                .expect("success text")
                .contains(
                    "Ваш VIP статус остаётся активным до <b>25.06.2026</b> (9 дней осталось)."
                )
        );
        assert_eq!(methods[1].1["reply_markup"]["inline_keyboard"], json!([]));
        assert_eq!(methods[2].1["text"], json!("Подписка отменена."));
        Ok(())
    }

    #[tokio::test]
    async fn confirm_cancel_vip_callback_missing_subscription_sends_go_alert()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let store = VipStatusStoreStub::new();
        let effects = EffectsStub::default();

        let outcome = handle_vip_cancellation_callback_update_or_else_at(
            &store,
            &effects,
            sample_vip_callback_update("confirm_cancel_vip", 42)?,
            now,
            |_update| async { Ok::<(), std::io::Error>(()) },
        )
        .await?;

        assert_eq!(outcome, VipCancellationCallbackOutcome::MissingSubscription);
        let methods = effects.vip_callback_methods();
        assert_eq!(method_names(&methods), vec!["answerCallbackQuery"]);
        assert_eq!(
            methods[0].1["text"],
            json!("Не удалось отменить подписку. Не найдена активная подписка.")
        );
        assert_eq!(methods[0].1["show_alert"], json!(true));
        assert!(store.canceled_subscription_ids().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn back_to_vip_status_callback_restores_status_or_inactive_text()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let active_store = VipStatusStoreStub::new()
            .with_summary(active_vip_summary(now, now + time::Duration::days(3)))
            .with_active_subscription(active_subscription(now, now + time::Duration::days(3)));
        let active_effects = EffectsStub::default();

        let active_outcome = handle_vip_cancellation_callback_update_or_else_at(
            &active_store,
            &active_effects,
            sample_vip_callback_update("back_to_vip_status", 42)?,
            now,
            |_update| async { Ok::<(), std::io::Error>(()) },
        )
        .await?;

        assert_eq!(
            active_outcome,
            VipCancellationCallbackOutcome::BackStatusShown
        );
        let active_methods = active_effects.vip_callback_methods();
        assert_eq!(
            method_names(&active_methods),
            vec!["editMessageText", "answerCallbackQuery"]
        );
        assert!(
            active_methods[0].1["text"]
                .as_str()
                .expect("status text")
                .contains("У вас уже есть активный VIP статус")
        );
        assert_eq!(
            active_methods[0].1["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            json!("{\"action\":\"cancel_vip\"}")
        );

        let inactive_store = VipStatusStoreStub::new();
        let inactive_effects = EffectsStub::default();
        let inactive_outcome = handle_vip_cancellation_callback_update_or_else_at(
            &inactive_store,
            &inactive_effects,
            sample_vip_callback_update("back_to_vip_status", 42)?,
            now,
            |_update| async { Ok::<(), std::io::Error>(()) },
        )
        .await?;

        assert_eq!(
            inactive_outcome,
            VipCancellationCallbackOutcome::BackInactive
        );
        let inactive_methods = inactive_effects.vip_callback_methods();
        assert_eq!(
            inactive_methods[0].1["text"],
            json!("Ваша подписка больше не активна. Отправьте /vip, чтобы подписаться снова.")
        );
        assert!(inactive_methods[0].1.get("parse_mode").is_none());
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
    async fn payment_update_handler_sends_group_redirects_only_for_targeted_payment_commands()
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
        )
        .with_bot_username("PlotvaBot");

        handler
            .handle_update(sample_group_payment_command_update("/vip@PlotvaBot")?)
            .await?;
        handler
            .handle_update(sample_group_payment_command_update(
                "/donate@PlotvaBot 777",
            )?)
            .await?;
        handler
            .handle_update(sample_group_payment_command_update("/donate 777")?)
            .await?;
        handler
            .handle_update(sample_group_payment_command_update("/vip@OtherBot")?)
            .await?;

        assert_eq!(next.calls(), vec![999, 999]);
        assert!(queue.assigned().is_empty());
        let redirects = effects.payment_redirect_messages();
        assert_eq!(redirects.len(), 2);
        assert_eq!(redirects[0].chat_id, -10042);
        assert_eq!(redirects[0].reply_to_message_id, 555);
        assert_eq!(redirects[0].message_thread_id, Some(77));
        assert_eq!(redirects[0].button_text, "💎 Оформить VIP подписку");
        assert_eq!(redirects[0].url, "https://t.me/PlotvaBot?start=vip");
        assert!(redirects[0].text.contains("VIP даёт вам:"));
        assert_eq!(redirects[1].button_text, "💝 Сделать донат");
        assert_eq!(redirects[1].url, "https://t.me/PlotvaBot?start=donate_777");
        Ok(())
    }

    #[tokio::test]
    async fn admin_vip_handler_rejects_unauthorized_command_with_go_ephemeral_text()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = Arc::new(openplotva_telegram::DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        ));
        let store = Arc::new(StoreStub::new());
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let next_id = Arc::new(AtomicU64::new(1));
        let handler = AdminVipCommandUpdateHandler::new(
            Arc::clone(&dispatcher),
            store,
            effects.clone(),
            vec![7],
            "PlotvaBot",
            Arc::clone(&next),
        )
        .with_virtual_id_factory({
            let next_id = Arc::clone(&next_id);
            Arc::new(move || format!("admin-vmsg-{}", next_id.fetch_add(1, Ordering::Relaxed)))
        });

        handler
            .handle_update(sample_payment_command_update("/admin_grant_vip 1 99 test")?)
            .await?;

        assert!(next.calls().is_empty());
        assert!(effects.sent_texts().is_empty());
        let item = dispatcher.dequeue_immediate().expect("queued admin denial");
        assert_eq!(
            item.ephemeral_delete_after(),
            Some(std::time::Duration::from_secs(60))
        );
        let method = match item.into_method().expect("admin denial method") {
            openplotva_telegram::TelegramOutboundMethod::SendMessage(method) => method,
            other => panic!("unexpected admin denial method: {other:?}"),
        };
        let payload = serde_json::to_value(method)?;
        assert_eq!(
            payload["text"],
            json!("❌ У вас нет прав на выполнение этой команды.")
        );
        assert_eq!(payload["reply_parameters"]["message_id"], json!(100));
        Ok(())
    }

    #[tokio::test]
    async fn admin_grant_vip_update_executes_for_authorized_targeted_group_command()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let expires_at = now + time::Duration::days(7);
        let store = StoreStub::new().with_next_vip_event(VipEventRecord {
            id: 88,
            user_id: 99,
            event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT.to_owned(),
            delta_seconds: vip_days_to_seconds(7),
            effective_expires_at: expires_at,
            subscription_id: None,
            actor_user_id: Some(42),
            reason: "manual test".to_owned(),
            created_at: now,
        });
        let dispatcher = openplotva_telegram::DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        );
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();
        let admin_ids = [42];

        let next_virtual_id =
            crate::virtual_messages::monotonic_virtual_id_factory("admin-test-vmsg");
        let outcome = handle_admin_vip_command_update_or_else_at(
            AdminVipCommandUpdateRuntime {
                queue: &dispatcher,
                vip: &store,
                effects: &effects,
                admin_ids: &admin_ids,
                bot_username: "PlotvaBot",
                next_virtual_id: &next_virtual_id,
                now,
            },
            sample_group_payment_command_update("/admin_grant_vip@PlotvaBot 7 99 manual test")?,
            |update| next.handle_update(update),
        )
        .await?;

        assert!(matches!(
            outcome,
            AdminVipCommandUpdateOutcome::Grant(report)
                if report.outcome == super::AdminGrantVipCommandOutcome::Granted
                    && report.target_user_id == Some(99)
                    && report.event_id == Some(88)
        ));
        assert!(next.calls().is_empty());
        assert_eq!(
            store.vip_events(),
            vec![VipEventCreate {
                user_id: 99,
                event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT,
                delta_seconds: vip_days_to_seconds(7),
                subscription_id: None,
                actor_user_id: Some(42),
                reason: Some("manual test"),
            }]
        );
        assert!(effects.sent_texts()[0].contains("VIP корректировка успешно записана"));
        assert_eq!(effects.invalidated_vip_users(), vec![99]);
        Ok(())
    }

    #[tokio::test]
    async fn group_admin_vip_command_without_bot_target_delegates() -> Result<(), Box<dyn Error>> {
        let dispatcher = openplotva_telegram::DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        );
        let store = StoreStub::new();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();
        let admin_ids = [42];

        let next_virtual_id =
            crate::virtual_messages::monotonic_virtual_id_factory("admin-test-vmsg");
        let outcome = handle_admin_vip_command_update_or_else_at(
            AdminVipCommandUpdateRuntime {
                queue: &dispatcher,
                vip: &store,
                effects: &effects,
                admin_ids: &admin_ids,
                bot_username: "PlotvaBot",
                next_virtual_id: &next_virtual_id,
                now: OffsetDateTime::UNIX_EPOCH,
            },
            sample_group_payment_command_update("/admin_grant_vip 7 99 manual test")?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, AdminVipCommandUpdateOutcome::Delegated);
        assert_eq!(next.calls(), vec![999]);
        assert!(effects.sent_texts().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn successful_payment_dispatcher_effects_queue_text_reply_and_invalidate_vip_cache()
    -> Result<(), Box<dyn Error>> {
        let queue = Arc::new(openplotva_telegram::DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        ));
        let invalidator = VipCacheInvalidatorStub::default();
        let next_id = Arc::new(AtomicU64::new(1));
        let effects =
            SuccessfulPaymentDispatcherEffects::new(Arc::clone(&queue), invalidator.clone())
                .with_virtual_id_factory({
                    let next_id = Arc::clone(&next_id);
                    Arc::new(move || {
                        format!("payment-vmsg-{}", next_id.fetch_add(1, Ordering::Relaxed))
                    })
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
        assert_eq!(
            payload["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
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
            format!(
                "💖 Для отправки доната, пожалуйста, перейдите в личные сообщения с ботом.\n\n\
                 Ваша поддержка:\n\
                 ❤️ Согреет сердце разработчика\n\
                 🚀 Поможет развивать бота\n\
                 ✨ Добавит мотивацию на создание новых функций\n\n{}",
                crypto_donation_plain_text()
            )
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
    fn crypto_donation_addresses_are_fixed_and_shared_by_all_formats() {
        assert_eq!(
            CRYPTO_DONATION_ADDRESSES,
            [
                ("BTC", "bc1qyv0lvyr4g5gnulqszyapuqrdycppt49p26re0v"),
                ("Ethereum EVM", "0x4fc711f4aa1744012b31911c1d5c451026ec2191"),
                ("Solana", "GJdwsdhp8pxEUc5g3mjigcp1BpCAnPuThTmyD3FFSTz1"),
            ]
        );

        let rich = crypto_donation_rich_html();
        let html = crypto_donation_html();
        let plain = crypto_donation_plain_text();
        for (network, address) in CRYPTO_DONATION_ADDRESSES {
            assert!(rich.contains(network));
            assert!(rich.contains(address));
            assert!(html.contains(address));
            assert!(plain.contains(address));
        }
        assert_eq!(openplotva_telegram::sanitize_rich_html(&rich), rich);
    }

    #[test]
    fn payment_redirect_message_from_update_builds_non_private_command_redirect()
    -> Result<(), Box<dyn Error>> {
        let message = super::payment_redirect_message_from_update(
            &sample_group_payment_command_update("/donate@PlotvaBot 777")?,
            "PlotvaBot",
        )
        .expect("group donate command should build a redirect");

        assert_eq!(message.chat_id, -10042);
        assert_eq!(message.reply_to_message_id, 555);
        assert_eq!(message.message_thread_id, Some(77));
        assert_eq!(message.url, "https://t.me/PlotvaBot?start=donate_777");
        assert!(
            super::payment_redirect_message_from_update(
                &sample_group_payment_command_update("/donate 777")?,
                "PlotvaBot"
            )
            .is_none()
        );
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
    fn vip_status_rich_html_uses_headings_paragraphs_and_list() {
        let html = super::vip_status_rich_html(
            "✅ У вас активен VIP статус.\n\n\
             Доступ подтверждён через внешний источник.\n\n\
             Вы получаете:\n\
             ⚡ Приоритетное выполнение запросов вне очереди\n\
             🔄 Вдвое больше рисунков за то же время",
        );

        assert_eq!(
            html,
            "<h3>✅ У вас активен VIP статус.</h3>\n\n\
<p>Доступ подтверждён через внешний источник.</p>\n\n\
<h3>Вы получаете</h3>\n\n\
<ul><li>⚡ Приоритетное выполнение запросов вне очереди</li><li>🔄 Вдвое больше рисунков за то же время</li></ul>"
        );
        assert_eq!(openplotva_telegram::sanitize_rich_html(&html), html);
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
    async fn vip_status_checker_uses_external_vip_chat_membership_when_ledger_and_cache_are_empty()
    -> Result<(), Box<dyn Error>> {
        let now = time::Date::from_calendar_date(2026, time::Month::June, 16)?
            .with_hms(9, 30, 0)?
            .assume_utc();
        let store = VipStatusStoreStub::new();
        let membership = ExternalVipMembershipStub::new(true);
        let checker = super::VipStatusWithExternalMembership::new(
            store.clone(),
            membership.clone(),
            -1001998670656,
        );

        assert!(super::VipStatusChecker::is_vip_at(&checker, 42, now).await);

        assert_eq!(membership.calls(), vec![(-1001998670656, 42)]);
        assert_eq!(
            store.calls(),
            vec![
                "get_vip_summary_by_user",
                "get_vip_cache",
                "upsert_vip_cache"
            ]
        );
        assert_eq!(
            store.upserted_caches(),
            vec![VipCacheUpsert {
                user_id: 42,
                is_vip: true,
                expires_at: now + super::VIP_TELEGRAM_CACHE_TTL,
            }]
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
    async fn live_redis_decoded_pre_checkout_acknowledges_without_downstream_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-pre-checkout:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(PaymentControlJobQueueStub::default());
        let vip = Arc::new(VipStatusStoreStub::new());
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = PaymentUpdateHandler::new(
            Arc::clone(&queue),
            Arc::clone(&vip),
            Arc::clone(&effects),
            Arc::clone(&next),
        );

        let update = sample_pre_checkout_update()?;
        update_queue.enqueue_update(&update).await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(StdDuration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded pre-checkout update"))?;

        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: StdDuration::from_millis(1),
                state_timeout: StdDuration::from_secs(1),
                handle_timeout: StdDuration::from_secs(1),
                side_effect_max_age: StdDuration::from_secs(60),
                worker_limit: 1,
            },
            UNIX_EPOCH + StdDuration::from_secs(1_779_193_800),
            &state_store,
            |update| handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 999);
        assert_eq!(report.update_name, "pre_checkout_query");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!report.skipped_handle);
        assert_eq!(state_store.calls(), vec!["user:42:Alice:alice".to_owned()]);
        assert_eq!(
            effects.answered_pre_checkout_queries(),
            vec!["pre-checkout-id".to_owned()]
        );
        assert!(queue.assigned().is_empty());
        assert!(next.calls().is_empty());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_payment_producers_route_control_jobs_and_status_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let processor_now = UNIX_EPOCH + StdDuration::from_secs(1_710_000_000);
        let active_status_now = OffsetDateTime::now_utc();

        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-successful-payment:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(PaymentControlJobQueueStub::default());
        let vip = Arc::new(VipStatusStoreStub::new().with_is_vip(false));
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = PaymentUpdateHandler::new(
            Arc::clone(&queue),
            Arc::clone(&vip),
            Arc::clone(&effects),
            Arc::clone(&next),
        );
        let update = sample_successful_payment_update_with_thread("subscription_42", 300, 77)?;
        update_queue.enqueue_update(&update).await?;
        let decoded = update_queue
            .dequeue_update(StdDuration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded successful-payment update"))?;
        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: StdDuration::from_millis(1),
                state_timeout: StdDuration::from_secs(1),
                handle_timeout: StdDuration::from_secs(1),
                side_effect_max_age: StdDuration::from_secs(60),
                worker_limit: 1,
            },
            processor_now,
            &state_store,
            |update| handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 999);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned()
            ]
        );
        assert!(next.calls().is_empty());
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "successful payment");
        assert_eq!(assigned[0].1.priority, TRANSACTION_PRIORITY);
        let telegram = assigned[0]
            .1
            .data
            .telegram_data
            .as_ref()
            .expect("successful payment telegram data");
        assert_eq!(telegram.thread_message_id, Some(77));
        let control = assigned[0]
            .1
            .data
            .control_data
            .as_ref()
            .expect("successful payment control data");
        assert_eq!(control.kind, ControlKind::SuccessfulPayment);
        assert_eq!(
            control
                .payment
                .as_ref()
                .map(|payment| payment.invoice_payload.as_str()),
            Some("subscription_42")
        );
        assert_eq!(update_queue.len().await?, 0);
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-donate-invoice:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(PaymentControlJobQueueStub::default());
        let vip = Arc::new(VipStatusStoreStub::new().with_is_vip(false));
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = PaymentUpdateHandler::new(
            Arc::clone(&queue),
            Arc::clone(&vip),
            Arc::clone(&effects),
            Arc::clone(&next),
        );
        let update = sample_payment_command_update("/donate 777")?;
        update_queue.enqueue_update(&update).await?;
        let decoded = update_queue
            .dequeue_update(StdDuration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded donate invoice update"))?;
        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: StdDuration::from_millis(1),
                state_timeout: StdDuration::from_secs(1),
                handle_timeout: StdDuration::from_secs(1),
                side_effect_max_age: StdDuration::from_secs(60),
                worker_limit: 1,
            },
            processor_now,
            &state_store,
            |update| handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 999);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned()
            ]
        );
        assert!(next.calls().is_empty());
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "donate invoice");
        assert_eq!(assigned[0].1.priority, TRANSACTION_PRIORITY);
        let control = assigned[0]
            .1
            .data
            .control_data
            .as_ref()
            .expect("donate invoice control data");
        assert_eq!(control.kind, ControlKind::DonateInvoice);
        assert_eq!(control.amount, 777);
        assert_eq!(update_queue.len().await?, 0);
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-vip-invoice:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(PaymentControlJobQueueStub::default());
        let vip = Arc::new(VipStatusStoreStub::new().with_is_vip(false));
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = PaymentUpdateHandler::new(
            Arc::clone(&queue),
            Arc::clone(&vip),
            Arc::clone(&effects),
            Arc::clone(&next),
        );
        let update = sample_payment_command_update("/vip")?;
        update_queue.enqueue_update(&update).await?;
        let decoded = update_queue
            .dequeue_update(StdDuration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded vip invoice update"))?;
        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: StdDuration::from_millis(1),
                state_timeout: StdDuration::from_secs(1),
                handle_timeout: StdDuration::from_secs(1),
                side_effect_max_age: StdDuration::from_secs(60),
                worker_limit: 1,
            },
            processor_now,
            &state_store,
            |update| handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 999);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned()
            ]
        );
        assert!(next.calls().is_empty());
        assert!(effects.vip_status_messages().is_empty());
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "vip invoice");
        assert_eq!(assigned[0].1.priority, TRANSACTION_PRIORITY);
        let control = assigned[0]
            .1
            .data
            .control_data
            .as_ref()
            .expect("vip invoice control data");
        assert_eq!(control.kind, ControlKind::VipInvoice);
        assert_eq!(control.amount, 300);
        assert_eq!(vip.calls(), vec!["is_vip_at"]);
        assert_eq!(update_queue.len().await?, 0);
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-vip-existing-status:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(PaymentControlJobQueueStub::default());
        let vip = Arc::new(
            VipStatusStoreStub::new()
                .with_is_vip(true)
                .with_summary(active_vip_summary(
                    active_status_now,
                    active_status_now + time::Duration::days(3),
                ))
                .with_active_subscription(active_subscription(
                    active_status_now,
                    active_status_now + time::Duration::days(3),
                )),
        );
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = PaymentUpdateHandler::new(
            Arc::clone(&queue),
            Arc::clone(&vip),
            Arc::clone(&effects),
            Arc::clone(&next),
        );
        let update = sample_payment_command_update("/vip")?;
        update_queue.enqueue_update(&update).await?;
        let decoded = update_queue
            .dequeue_update(StdDuration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded existing VIP status update"))?;
        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: StdDuration::from_millis(1),
                state_timeout: StdDuration::from_secs(1),
                handle_timeout: StdDuration::from_secs(1),
                side_effect_max_age: StdDuration::from_secs(60),
                worker_limit: 1,
            },
            processor_now,
            &state_store,
            |update| handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 999);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned()
            ]
        );
        assert!(next.calls().is_empty());
        assert!(queue.assigned().is_empty());
        let status_messages = effects.vip_status_messages();
        assert_eq!(status_messages.len(), 1);
        assert!(
            status_messages[0]
                .text
                .contains("У вас уже есть активный VIP статус")
        );
        assert!(status_messages[0].show_cancel_button);
        assert_eq!(
            vip.calls(),
            vec![
                "is_vip_at",
                "get_vip_summary_by_user",
                "get_active_subscription"
            ]
        );
        assert_eq!(update_queue.len().await?, 0);
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_private_start_payment_payloads_delegate_into_payment_handler_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-start-payment:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let mut start_vip = sample_payment_command_update("/start vip")?;
        start_vip.id = 1201;
        let mut start_donate_default = sample_payment_command_update("/start donate")?;
        start_donate_default.id = 1202;
        let mut start_donate_amount = sample_payment_command_update("/start donate_777")?;
        start_donate_amount.id = 1203;
        let mut start_donate_bad = sample_payment_command_update("/start donate_bad")?;
        start_donate_bad.id = 1204;
        update_queue.enqueue_update(&start_vip).await?;
        update_queue.enqueue_update(&start_donate_default).await?;
        update_queue.enqueue_update(&start_donate_amount).await?;
        update_queue.enqueue_update(&start_donate_bad).await?;
        assert_eq!(update_queue.len().await?, 4);

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(PaymentControlJobQueueStub::default());
        let vip = Arc::new(VipStatusStoreStub::new().with_is_vip(false));
        let effects = Arc::new(EffectsStub::default());
        let terminal = Arc::new(UpdateHandlerStub::default());
        let payment_handler = Arc::new(PaymentUpdateHandler::new(
            Arc::clone(&queue),
            Arc::clone(&vip),
            Arc::clone(&effects),
            Arc::clone(&terminal),
        ));
        let dispatcher = Arc::new(openplotva_telegram::DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        ));
        let telegram = openplotva_telegram::telegram_client("123:token")?;
        let help_effects = Arc::new(HelpDispatcherEffects::new(
            telegram,
            Arc::clone(&payment_handler),
            Arc::new(crate::rich::MockRichSender::default()),
        ));
        let help_handler = HelpCommandUpdateHandler::new(
            HelpBotIdentity {
                first_name: "Plotva".to_owned(),
                username: "PlotvaBot".to_owned(),
                token: "123:token".to_owned(),
            },
            help_effects,
            Arc::clone(&terminal),
        );
        let config = UpdateConsumerConfig {
            dequeue_timeout: StdDuration::from_millis(1),
            state_timeout: StdDuration::from_secs(1),
            handle_timeout: StdDuration::from_secs(1),
            side_effect_max_age: StdDuration::from_secs(60),
            worker_limit: 1,
        };

        for expected_update_id in [1201, 1202, 1203, 1204] {
            let decoded = update_queue
                .dequeue_update(StdDuration::from_secs(1))
                .await?
                .ok_or_else(|| io::Error::other("expected decoded private start payment update"))?;
            let report = process_update_with_state_store_at(
                decoded,
                config,
                UNIX_EPOCH + StdDuration::from_secs(1_710_000_010),
                &state_store,
                |update| help_handler.handle_update(update),
            )
            .await;
            assert_eq!(report.update_id, expected_update_id);
            assert_eq!(report.update_name, "message");
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed)
            );
            assert!(!report.skipped_handle);
        }

        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned(),
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned(),
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned(),
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned(),
            ]
        );
        assert_eq!(terminal.calls(), Vec::<i64>::new());
        let dispatcher_snapshot = dispatcher.snapshot();
        assert!(dispatcher_snapshot.immediate.is_empty());
        assert!(dispatcher_snapshot.regular.is_empty());
        assert!(effects.vip_status_messages().is_empty());
        assert!(effects.payment_redirect_messages().is_empty());

        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 4);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "vip invoice");
        let vip_control = assigned[0]
            .1
            .data
            .control_data
            .as_ref()
            .expect("vip invoice control data");
        assert_eq!(vip_control.kind, ControlKind::VipInvoice);
        assert_eq!(vip_control.amount, 300);

        assert_eq!(assigned[1].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[1].1.title, "donate invoice");
        let donate_default_control = assigned[1]
            .1
            .data
            .control_data
            .as_ref()
            .expect("default donate invoice control data");
        assert_eq!(donate_default_control.kind, ControlKind::DonateInvoice);
        assert_eq!(donate_default_control.amount, 600);

        assert_eq!(assigned[2].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[2].1.title, "donate invoice");
        let donate_amount_control = assigned[2]
            .1
            .data
            .control_data
            .as_ref()
            .expect("amount donate invoice control data");
        assert_eq!(donate_amount_control.kind, ControlKind::DonateInvoice);
        assert_eq!(donate_amount_control.amount, 777);

        assert_eq!(assigned[3].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[3].1.title, "donate invoice");
        let donate_bad_control = assigned[3]
            .1
            .data
            .control_data
            .as_ref()
            .expect("malformed donate invoice control data");
        assert_eq!(donate_bad_control.kind, ControlKind::DonateInvoice);
        assert_eq!(donate_bad_control.amount, 600);
        assert_eq!(vip.calls(), vec!["is_vip_at"]);
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_group_payment_redirects_send_direct_payloads_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-payment-redirect:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        for update in [
            sample_group_payment_command_update("/vip@PlotvaBot")?,
            sample_group_payment_command_update("/donate@PlotvaBot 777")?,
            sample_group_payment_command_update("/donate 777")?,
            sample_group_payment_command_update("/vip@OtherBot")?,
            sample_group_payment_command_update("/settings@PlotvaBot")?,
        ] {
            update_queue.enqueue_update(&update).await?;
        }
        assert_eq!(update_queue.len().await?, 5);

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(PaymentControlJobQueueStub::default());
        let vip = Arc::new(VipStatusStoreStub::new().with_is_vip(false));
        let effects = Arc::new(EffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = PaymentUpdateHandler::new(
            Arc::clone(&queue),
            Arc::clone(&vip),
            Arc::clone(&effects),
            Arc::clone(&next),
        )
        .with_bot_username("PlotvaBot");

        let mut reports = Vec::new();
        for expected in 0..5 {
            let decoded = update_queue
                .dequeue_update(StdDuration::from_secs(1))
                .await?
                .ok_or_else(|| io::Error::other("expected decoded payment redirect update"))?;
            let report = process_update_with_state_store_at(
                decoded,
                UpdateConsumerConfig {
                    dequeue_timeout: StdDuration::from_millis(1),
                    state_timeout: StdDuration::from_secs(1),
                    handle_timeout: StdDuration::from_secs(1),
                    side_effect_max_age: StdDuration::from_secs(60),
                    worker_limit: 1,
                },
                UNIX_EPOCH + StdDuration::from_secs(1_710_000_000 + expected),
                &state_store,
                |update| handler.handle_update(update),
            )
            .await;
            reports.push(report);
        }

        for report in &reports {
            assert_eq!(report.update_id, 999);
            assert_eq!(report.update_name, "message");
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed)
            );
            assert!(!report.skipped_handle);
        }
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:-10042:supergroup::".to_owned(),
                "user:42:Alice:alice".to_owned(),
                "chat:-10042:supergroup::".to_owned(),
                "user:42:Alice:alice".to_owned(),
                "chat:-10042:supergroup::".to_owned(),
                "user:42:Alice:alice".to_owned(),
                "chat:-10042:supergroup::".to_owned(),
                "user:42:Alice:alice".to_owned(),
                "chat:-10042:supergroup::".to_owned(),
                "user:42:Alice:alice".to_owned(),
            ]
        );
        assert!(queue.assigned().is_empty());
        assert_eq!(next.calls(), vec![999, 999, 999]);

        let redirects = effects.payment_redirect_messages();
        assert_eq!(redirects.len(), 2);
        assert_eq!(redirects[0].chat_id, -10042);
        assert_eq!(redirects[0].reply_to_message_id, 555);
        assert_eq!(redirects[0].message_thread_id, Some(77));
        assert_eq!(redirects[0].button_text, "💎 Оформить VIP подписку");
        assert_eq!(redirects[0].url, "https://t.me/PlotvaBot?start=vip");
        assert!(redirects[0].text.contains("VIP даёт вам:"));
        assert_eq!(redirects[1].button_text, "💝 Сделать донат");
        assert_eq!(redirects[1].url, "https://t.me/PlotvaBot?start=donate_777");
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_admin_payment_commands_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let processor_now = UNIX_EPOCH + StdDuration::from_secs(1_710_000_000);
        let now = time::Date::from_calendar_date(2026, time::Month::May, 20)?
            .with_hms(12, 0, 0)?
            .assume_utc();
        let expires_at = now + time::Duration::days(30);
        let mut refund_subscription = active_subscription(now, expires_at);
        refund_subscription.user_id = 1_717_359_759;
        refund_subscription.telegram_payment_charge_id = "charge-sub".to_owned();

        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-admin-payment:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&sample_payment_command_update(
                "/admin_grant_vip 7 99 manual test",
            )?)
            .await?;
        update_queue
            .enqueue_update(&sample_group_payment_command_update(
                "/admin_cancel_vip@PlotvaBot 42",
            )?)
            .await?;
        update_queue
            .enqueue_update(&sample_payment_command_update("/admin_refund charge-sub")?)
            .await?;
        assert_eq!(update_queue.len().await?, 3);

        let state_store = UpdateStateStoreStub::default();
        let dispatcher = openplotva_telegram::DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        );
        let store = StoreStub::new()
            .with_admin_cancel_summary(active_vip_summary(now, expires_at))
            .with_admin_cancel_updated_summary(None)
            .with_admin_refund_subscription(Some(refund_subscription.clone()))
            .with_next_vip_event(VipEventRecord {
                id: 201,
                user_id: 99,
                event_type: VIP_EVENT_TYPE_ADMIN_ADJUSTMENT.to_owned(),
                delta_seconds: vip_days_to_seconds(7),
                effective_expires_at: now + time::Duration::days(7),
                subscription_id: None,
                actor_user_id: Some(42),
                reason: "manual test".to_owned(),
                created_at: now,
            })
            .with_next_vip_event(VipEventRecord {
                id: 202,
                user_id: 42,
                event_type: VIP_EVENT_TYPE_ADMIN_REVOKE.to_owned(),
                delta_seconds: -time::Duration::days(30).whole_seconds(),
                effective_expires_at: now,
                subscription_id: None,
                actor_user_id: Some(42),
                reason: "telegram admin revoke by 42".to_owned(),
                created_at: now,
            })
            .with_next_vip_event(VipEventRecord {
                id: 203,
                user_id: refund_subscription.user_id,
                event_type: VIP_EVENT_TYPE_REFUND_REVERSAL.to_owned(),
                delta_seconds: -subscription_delta_seconds(
                    &refund_subscription.telegram_payment_charge_id,
                    refund_subscription.created_at,
                    refund_subscription.expires_at,
                    openplotva_telegram::SUBSCRIPTION_DURATION_DAYS,
                ),
                effective_expires_at: now,
                subscription_id: Some(refund_subscription.id),
                actor_user_id: Some(42),
                reason: "admin refund charge-sub".to_owned(),
                created_at: now,
            });
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();
        let next_virtual_id =
            crate::virtual_messages::monotonic_virtual_id_factory("admin-payment-vmsg");
        let mut outcomes = Vec::new();

        for expected_name in ["message", "message", "message"] {
            let decoded = update_queue
                .dequeue_update(StdDuration::from_secs(1))
                .await?
                .ok_or_else(|| io::Error::other("expected decoded admin payment update"))?;
            let report = process_update_with_state_store_at(
                decoded,
                UpdateConsumerConfig {
                    dequeue_timeout: StdDuration::from_millis(1),
                    state_timeout: StdDuration::from_secs(1),
                    handle_timeout: StdDuration::from_secs(1),
                    side_effect_max_age: StdDuration::from_secs(60),
                    worker_limit: 1,
                },
                processor_now,
                &state_store,
                |update| async {
                    let outcome = handle_admin_vip_command_update_or_else_at(
                        AdminVipCommandUpdateRuntime {
                            queue: &dispatcher,
                            vip: &store,
                            effects: &effects,
                            admin_ids: &[42],
                            bot_username: "PlotvaBot",
                            next_virtual_id: &next_virtual_id,
                            now,
                        },
                        update,
                        |update| next.handle_update(update),
                    )
                    .await?;
                    outcomes.push(outcome);
                    Ok::<(), io::Error>(())
                },
            )
            .await;

            assert_eq!(report.update_id, 999);
            assert_eq!(report.update_name, expected_name);
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed)
            );
            assert!(!report.skipped_handle);
        }

        assert_eq!(update_queue.len().await?, 0);
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned(),
                "chat:-10042:supergroup::".to_owned(),
                "user:42:Alice:alice".to_owned(),
                "chat:42:private:Alice:alice".to_owned(),
                "user:42:Alice:alice".to_owned(),
            ]
        );
        assert!(next.calls().is_empty());
        assert!(dispatcher.dequeue_immediate().is_none());
        assert_eq!(outcomes.len(), 3);
        assert!(matches!(
            &outcomes[0],
            AdminVipCommandUpdateOutcome::Grant(report)
                if report.outcome == super::AdminGrantVipCommandOutcome::Granted
                    && report.target_user_id == Some(99)
                    && report.event_id == Some(201)
        ));
        assert!(matches!(
            &outcomes[1],
            AdminVipCommandUpdateOutcome::Cancel(report)
                if report.outcome == super::AdminCancelVipCommandOutcome::Revoked
                    && report.target_user_id == Some(42)
                    && report.event_id == Some(202)
        ));
        assert!(matches!(
            &outcomes[2],
            AdminVipCommandUpdateOutcome::Refund(report)
                if report.outcome == super::AdminRefundCommandOutcome::RefundedSubscription
                    && report.target_user_id == Some(1_717_359_759)
                    && report.amount_stars == Some(1)
        ));
        assert_eq!(
            effects.refund_requests(),
            vec![(1_717_359_759, "charge-sub".to_owned())]
        );
        assert_eq!(effects.invalidated_vip_users(), vec![99, 42, 1_717_359_759]);
        assert_eq!(store.refunded_subscription_ids(), vec![10]);
        assert_eq!(
            store
                .vip_events()
                .into_iter()
                .map(|event| (
                    event.user_id,
                    event.event_type.to_owned(),
                    event.actor_user_id,
                    event.reason.map(str::to_owned)
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    99,
                    VIP_EVENT_TYPE_ADMIN_ADJUSTMENT.to_owned(),
                    Some(42),
                    Some("manual test".to_owned())
                ),
                (
                    42,
                    VIP_EVENT_TYPE_ADMIN_REVOKE.to_owned(),
                    Some(42),
                    Some("telegram admin revoke by 42".to_owned())
                ),
                (
                    1_717_359_759,
                    VIP_EVENT_TYPE_REFUND_REVERSAL.to_owned(),
                    Some(42),
                    Some("admin refund charge-sub".to_owned())
                ),
            ]
        );
        let sent = effects.sent_payment_texts();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[0].chat_id, 42);
        assert_eq!(sent[0].reply_to_message_id, 100);
        assert!(sent[0].text.contains("VIP корректировка успешно записана"));
        assert_eq!(sent[1].chat_id, -10042);
        assert_eq!(sent[1].reply_to_message_id, 555);
        assert!(sent[1].text.contains("VIP статус успешно обработан"));
        assert_eq!(sent[2].chat_id, 42);
        assert_eq!(sent[2].reply_to_message_id, 100);
        assert!(
            sent[2]
                .text
                .contains("Возврат средств для транзакции charge-sub")
        );
        assert_eq!(
            effects.direct_texts(),
            vec![(
                1_717_359_759,
                "💸 Вам был возвращен платеж в размере 1 звезд по транзакции charge-sub."
                    .to_owned()
            )]
        );

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_donate_invoice_executes_unified_control_job_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let processor_now = UNIX_EPOCH + StdDuration::from_secs(1_710_000_000);
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-donate-invoice-worker:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(InMemoryPaymentControlJobQueue::new());
        let vip = Arc::new(VipStatusStoreStub::new().with_is_vip(false));
        let effects = Arc::new(EffectsStub::default().with_next_invoice_url("https://t.me/donate"));
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = PaymentUpdateHandler::new(
            Arc::clone(&queue),
            Arc::clone(&vip),
            Arc::clone(&effects),
            Arc::clone(&next),
        );

        update_queue
            .enqueue_update(&sample_payment_command_update("/donate 777")?)
            .await?;
        let decoded = update_queue
            .dequeue_update(StdDuration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded donate invoice update"))?;
        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: StdDuration::from_millis(1),
                state_timeout: StdDuration::from_secs(1),
                handle_timeout: StdDuration::from_secs(1),
                side_effect_max_age: StdDuration::from_secs(60),
                worker_limit: 1,
            },
            processor_now,
            &state_store,
            |update| handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 999);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(next.calls().is_empty());
        assert_eq!(queue.snapshot().len(), 1);
        assert_eq!(queue.snapshot()[0].job.title, "donate invoice");
        assert_eq!(
            queue.snapshot()[0].status,
            InMemoryPaymentControlJobStatus::Pending
        );

        let store = StoreStub::new();
        let registry = PaymentOnlyControlJobRegistry {
            store: &store,
            effects: effects.as_ref(),
        };
        let worker = process_control_job_once_at(queue.as_ref(), &registry, now).await;

        assert!(worker.dequeued);
        assert_eq!(worker.kind, Some(ControlKind::DonateInvoice));
        assert!(worker.completed);
        assert!(!worker.failed);
        assert_eq!(
            worker.execution,
            Some(ControlJobExecution::Payment(
                PaymentControlJobReport::from_invoice(
                    PaymentControlJobOutcome::DonateInvoice,
                    InvoiceControlJobReport::new(InvoiceControlJobOutcome::InvoiceSent)
                )
            ))
        );
        assert_eq!(
            effects.donation_invoice_requests(),
            vec![openplotva_telegram::DonationInvoiceLinkRequest {
                user_id: 42,
                amount_stars: 777,
            }]
        );
        let invoice_messages = effects.invoice_messages();
        assert_eq!(invoice_messages.len(), 1);
        assert_eq!(invoice_messages[0].chat_id, 42);
        assert_eq!(invoice_messages[0].url, "https://t.me/donate");
        assert_eq!(
            invoice_messages[0].button_text,
            "🌟 Отправить донат 777 звезд?"
        );
        assert_eq!(
            queue.snapshot()[0].status,
            InMemoryPaymentControlJobStatus::Completed
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
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
    async fn payment_runtime_effects_keep_invoice_and_successful_payment_sides_separate()
    -> Result<(), Box<dyn Error>> {
        let invoice_effects =
            EffectsStub::default().with_next_invoice_url("https://t.me/invoice-vip");
        let successful_payment_effects = EffectsStub::default();
        let effects =
            PaymentRuntimeEffects::new(invoice_effects.clone(), successful_payment_effects.clone());

        let invoice_url = effects
            .create_subscription_invoice_link(
                &openplotva_telegram::SubscriptionInvoiceLinkRequest {
                    user_id: 42,
                    user_name: "alice".to_owned(),
                    amount_stars: 300,
                },
            )
            .await?;
        effects
            .send_invoice_button_message(InvoiceButtonMessage {
                chat_id: 1000,
                text: expected_subscription_invoice_message_text(),
                button_text: "💎 Оформить VIP подписку".to_owned(),
                url: invoice_url,
                parse_mode: "HTML".to_owned(),
            })
            .await?;
        effects
            .send_text(PaymentTextMessage {
                chat_id: 1000,
                reply_to_message_id: 77,
                text: "Спасибо за поддержку!",
                parse_mode: "HTML",
            })
            .await?;
        effects.invalidate_vip_cache(42).await?;

        assert_eq!(
            invoice_effects.subscription_invoice_requests(),
            vec![openplotva_telegram::SubscriptionInvoiceLinkRequest {
                user_id: 42,
                user_name: "alice".to_owned(),
                amount_stars: 300,
            }]
        );
        assert_eq!(invoice_effects.invoice_messages().len(), 1);
        assert!(invoice_effects.sent_payment_texts().is_empty());
        assert_eq!(
            successful_payment_effects.sent_payment_texts(),
            vec![SentPaymentText {
                chat_id: 1000,
                reply_to_message_id: 77,
                text: "Спасибо за поддержку!".to_owned(),
                parse_mode: "HTML".to_owned(),
            }]
        );
        assert_eq!(successful_payment_effects.invalidated_vip_users(), vec![42]);
        assert!(successful_payment_effects.invoice_messages().is_empty());
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
                text: donation_invoice_message_text(),
                button_text: "🌟 Отправить донат 777 звезд?".to_owned(),
                url: "https://t.me/invoice-donation".to_owned(),
                parse_mode: "HTML".to_owned(),
            }]
        );
        for (_, address) in CRYPTO_DONATION_ADDRESSES {
            assert!(effects.invoice_messages()[0].text.contains(address));
        }
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
            image_data: None,
            music_data: None,
            dialog_data: None,
            asr_data: None,
            control_data: None,
            agent_data: None,
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
    async fn in_memory_payment_control_job_queue_skips_non_payment_control_jobs()
    -> Result<(), Box<dyn Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = super::InMemoryPaymentControlJobQueue::new();
        let translate = new_control_job_at(sample_invoice_job(ControlKind::Translate), created)
            .with_name("translate")
            .with_priority(HIGH_PRIORITY);
        let donate = new_control_job_at(
            sample_invoice_job(ControlKind::DonateInvoice),
            created + time::Duration::seconds(1),
        )
        .with_name("donate")
        .with_priority(DEFAULT_PRIORITY);

        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, translate)
            .await?;
        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, donate)
            .await?;

        let item = queue
            .dequeue_payment_control_job(CONTROL_QUEUE_NAME)
            .await?
            .expect("payment item");
        assert_eq!(item.job.title, "donate");
        let snapshot = queue.snapshot();
        assert_eq!(snapshot[0].job.title, "translate");
        assert_eq!(
            snapshot[0].status,
            super::InMemoryPaymentControlJobStatus::Pending
        );
        assert_eq!(snapshot[1].job.title, "donate");
        assert_eq!(
            snapshot[1].status,
            super::InMemoryPaymentControlJobStatus::Processing
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
    async fn persistent_control_queue_task_enqueue_limit_blocks_before_assignment()
    -> Result<(), Box<dyn Error>> {
        let limiter = Arc::new(TaskEnqueueRateLimitStub::limited());
        let queue =
            super::PersistentPaymentControlJobQueue::from_task_queue(InMemoryTaskQueue::new())
                .with_task_enqueue_rate_limit(limiter.clone());
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_control_job_at(sample_invoice_job(ControlKind::Translate), created)
            .with_name("translate")
            .with_priority(HIGH_PRIORITY);

        let error = queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, job)
            .await
            .expect_err("task enqueue limit should block assignment");

        assert!(matches!(
            error,
            super::PersistentPaymentControlJobQueueError::TaskEnqueueRateLimited
        ));
        assert!(queue.snapshot().is_empty());
        assert_eq!(limiter.check_calls(), vec![(1000, 42)]);
        assert!(limiter.grow_calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn persistent_control_queue_grows_task_enqueue_limit_after_assignment()
    -> Result<(), Box<dyn Error>> {
        let limiter = Arc::new(TaskEnqueueRateLimitStub::default());
        let queue =
            super::PersistentPaymentControlJobQueue::from_task_queue(InMemoryTaskQueue::new())
                .with_task_enqueue_rate_limit(limiter.clone());
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_control_job_at(sample_invoice_job(ControlKind::Translate), created)
            .with_name("translate")
            .with_priority(HIGH_PRIORITY);

        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, job)
            .await?;

        assert_eq!(queue.snapshot().len(), 1);
        assert_eq!(limiter.check_calls(), vec![(1000, 42)]);
        assert_eq!(limiter.grow_calls(), vec![(1000, 42)]);
        Ok(())
    }

    #[tokio::test]
    async fn persistent_control_queue_does_not_rate_limit_payment_invoices()
    -> Result<(), Box<dyn Error>> {
        // Payment-intent commands (/vip, /donate) must never be dropped by the limiter.
        for kind in [ControlKind::VipInvoice, ControlKind::DonateInvoice] {
            let limiter = Arc::new(TaskEnqueueRateLimitStub::limited());
            let queue =
                super::PersistentPaymentControlJobQueue::from_task_queue(InMemoryTaskQueue::new())
                    .with_task_enqueue_rate_limit(limiter.clone());
            let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
            let job = new_control_job_at(sample_invoice_job(kind), created)
                .with_name("payment invoice")
                .with_priority(HIGH_PRIORITY);

            queue
                .assign_payment_control_job(CONTROL_QUEUE_NAME, job)
                .await?;

            assert_eq!(queue.snapshot().len(), 1, "{kind:?} must be queued");
            assert!(
                limiter.check_calls().is_empty(),
                "{kind:?} must skip the limiter"
            );
            assert!(limiter.grow_calls().is_empty());
        }
        Ok(())
    }

    #[tokio::test]
    async fn persistent_control_queue_does_not_rate_limit_successful_payment()
    -> Result<(), Box<dyn Error>> {
        let limiter = Arc::new(TaskEnqueueRateLimitStub::limited());
        let queue =
            super::PersistentPaymentControlJobQueue::from_task_queue(InMemoryTaskQueue::new())
                .with_task_enqueue_rate_limit(limiter.clone());
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_control_job_at(sample_invoice_job(ControlKind::SuccessfulPayment), created)
            .with_name("successful payment")
            .with_priority(HIGH_PRIORITY);

        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, job)
            .await?;

        assert_eq!(queue.snapshot().len(), 1);
        assert!(limiter.check_calls().is_empty());
        assert!(limiter.grow_calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn persistent_control_queue_does_not_rate_limit_member_state_sync()
    -> Result<(), Box<dyn Error>> {
        for kind in [ControlKind::ChatAdminsSync, ControlKind::ChatMemberSync] {
            let limiter = Arc::new(TaskEnqueueRateLimitStub::limited());
            let queue =
                super::PersistentPaymentControlJobQueue::from_task_queue(InMemoryTaskQueue::new())
                    .with_task_enqueue_rate_limit(limiter.clone());
            let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
            let job = new_control_job_at(sample_invoice_job(kind), created)
                .with_name("member state sync")
                .with_priority(HIGH_PRIORITY);

            queue
                .assign_payment_control_job(CONTROL_QUEUE_NAME, job)
                .await?;

            assert_eq!(queue.snapshot().len(), 1, "{kind:?} must be queued");
            assert!(
                limiter.check_calls().is_empty(),
                "{kind:?} must skip the limiter"
            );
            assert!(limiter.grow_calls().is_empty());
        }
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

    #[derive(Default)]
    struct TaskEnqueueRateLimitStub {
        limited: bool,
        check_calls: Mutex<Vec<(i64, i64)>>,
        grow_calls: Mutex<Vec<(i64, i64)>>,
    }

    impl TaskEnqueueRateLimitStub {
        fn limited() -> Self {
            Self {
                limited: true,
                ..Self::default()
            }
        }

        fn check_calls(&self) -> Vec<(i64, i64)> {
            self.check_calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        fn grow_calls(&self) -> Vec<(i64, i64)> {
            self.grow_calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl crate::rate_limits::TaskEnqueueRateLimit for TaskEnqueueRateLimitStub {
        fn check_task_enqueue_rate_limit<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
            _now: OffsetDateTime,
        ) -> crate::rate_limits::TaskEnqueueRateLimitFuture<'a> {
            Box::pin(async move {
                self.check_calls
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((chat_id, user_id));
                crate::rate_limits::TaskEnqueueRateLimitReport {
                    limited: self.limited,
                    scope: self
                        .limited
                        .then_some(crate::rate_limits::TaskEnqueueRateLimitScope::UserChat),
                    error: None,
                }
            })
        }

        fn grow_task_enqueue_rate_limit<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
            _now: OffsetDateTime,
        ) -> crate::rate_limits::TaskEnqueueRateLimitGrowFuture<'a> {
            Box::pin(async move {
                self.grow_calls
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((chat_id, user_id));
                crate::rate_limits::TaskEnqueueRateLimitGrowReport::default()
            })
        }
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
                paid_at: None,
                subscription_period_seconds: None,
                subscription_expiration_date: None,
                vip_target_expires_at: None,
                is_recurring: None,
                is_first_recurring: None,
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
            ..ControlPayment::default()
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

    struct PaymentOnlyControlJobRegistry<'a> {
        store: &'a StoreStub,
        effects: &'a EffectsStub,
    }

    impl ControlJobExecutorRegistry for PaymentOnlyControlJobRegistry<'_> {
        fn execute_control_job<'a>(
            &'a self,
            params: &'a ControlJobParams,
            now: OffsetDateTime,
        ) -> ControlJobExecutorFuture<'a> {
            Box::pin(async move {
                let report =
                    execute_payment_control_job_at(self.store, self.effects, params, now).await;
                let execution = ControlJobExecution::Payment(report.clone());
                if let Some(error) = payment_control_job_failure_message(&report) {
                    ControlJobExecutionResult::Failed {
                        execution: Some(execution),
                        error,
                    }
                } else {
                    ControlJobExecutionResult::Completed(execution)
                }
            })
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

    fn sample_recurring_successful_payment_update(
        payload: &str,
        total_amount: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let mut update =
            serde_json::to_value(sample_successful_payment_update(payload, total_amount)?)?;
        if let Some(payment) = update
            .pointer_mut("/message/successful_payment")
            .and_then(serde_json::Value::as_object_mut)
        {
            payment.insert("is_recurring".to_owned(), json!(true));
            payment.insert("is_first_recurring".to_owned(), json!(false));
            payment.insert(
                "subscription_expiration_date".to_owned(),
                json!(1_712_592_000),
            );
        }
        serde_json::from_value(update)
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

    fn sample_vip_callback_update(
        action: &str,
        user_id: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 1001,
            "callback_query": {
                "id": "vip-callback-id",
                "from": {
                    "id": user_id,
                    "is_bot": false,
                    "first_name": "Alice",
                    "username": "alice"
                },
                "chat_instance": "chat-instance",
                "data": format!(r#"{{"action":"{action}"}}"#),
                "message": {
                    "message_id": 555,
                    "date": 1_710_000_000,
                    "chat": {
                        "id": 42,
                        "type": "private",
                        "first_name": "Alice",
                        "username": "alice"
                    },
                    "text": "VIP status"
                }
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

    fn method_names(methods: &[(String, serde_json::Value)]) -> Vec<&str> {
        methods.iter().map(|(name, _)| name.as_str()).collect()
    }

    fn telegram_method_payload(
        method: openplotva_telegram::TelegramOutboundMethod,
    ) -> Result<serde_json::Value, StubError> {
        match method {
            openplotva_telegram::TelegramOutboundMethod::AnswerCallbackQuery(method) => {
                serde_json::to_value(method.as_ref())
            }
            openplotva_telegram::TelegramOutboundMethod::EditMessageText(method) => {
                serde_json::to_value(method.as_ref())
            }
            openplotva_telegram::TelegramOutboundMethod::EditUserStarSubscription(method) => {
                serde_json::to_value(method.as_ref())
            }
            other => {
                return Err(StubError::Other(format!(
                    "unexpected Telegram method {}",
                    other.method_name()
                )));
            }
        }
        .map_err(|error| StubError::Other(error.to_string()))
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
        upserted_caches: Vec<VipCacheUpsert>,
        canceled_subscription_ids: Vec<i64>,
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

        fn canceled_subscription_ids(&self) -> Vec<i64> {
            self.lock().canceled_subscription_ids.clone()
        }

        fn upserted_caches(&self) -> Vec<VipCacheUpsert> {
            self.lock().upserted_caches.clone()
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

    impl super::ExternalVipCacheStore for VipStatusStoreStub {
        type Error = StubError;

        fn upsert_vip_cache<'a>(
            &'a self,
            cache: VipCacheUpsert,
        ) -> PaymentStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.calls.push("upsert_vip_cache");
                state.upserted_caches.push(cache);
                Ok(())
            })
        }
    }

    #[derive(Clone, Default)]
    struct ExternalVipMembershipStub {
        state: Arc<Mutex<ExternalVipMembershipState>>,
    }

    #[derive(Default)]
    struct ExternalVipMembershipState {
        is_member: bool,
        calls: Vec<(i64, i64)>,
    }

    impl ExternalVipMembershipStub {
        fn new(is_member: bool) -> Self {
            let stub = Self::default();
            stub.state
                .lock()
                .expect("external VIP membership")
                .is_member = is_member;
            stub
        }

        fn calls(&self) -> Vec<(i64, i64)> {
            self.state
                .lock()
                .expect("external VIP membership")
                .calls
                .clone()
        }
    }

    impl super::ExternalVipMembershipChecker for ExternalVipMembershipStub {
        type Error = StubError;

        fn is_external_vip_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> super::ExternalVipMembershipFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("external VIP membership");
                state.calls.push((chat_id, user_id));
                Ok(state.is_member)
            })
        }
    }

    impl VipCancellationStore for VipStatusStoreStub {
        fn mark_subscription_canceled<'a>(
            &'a self,
            id: i64,
        ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.calls.push("mark_subscription_canceled");
                state.canceled_subscription_ids.push(id);
                let mut subscription = state.active_subscription.clone().unwrap_or_else(|| {
                    active_subscription(OffsetDateTime::UNIX_EPOCH, OffsetDateTime::UNIX_EPOCH)
                });
                subscription.id = id;
                subscription.canceled_at = Some(OffsetDateTime::UNIX_EPOCH);
                Ok(subscription)
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
        username_users: Vec<(String, i64)>,
        next_subscription_id: i64,
        next_subscription_error: Option<StubError>,
        existing_subscription: Option<SubscriptionRecord>,
        successful_payment_summaries: VecDeque<Result<Option<VipSummaryRecord>, StubError>>,
        existing_vip_event_by_subscription: Option<VipEventRecord>,
        next_donations: VecDeque<DonationRecord>,
        next_vip_events: VecDeque<VipEventRecord>,
        admin_cancel_summaries: VecDeque<Result<Option<VipSummaryRecord>, StubError>>,
        admin_cancel_active_subscriptions: VecDeque<Result<Option<SubscriptionRecord>, StubError>>,
        refunded_subscription_ids: Vec<i64>,
        mark_subscription_refunded_error: Option<StubError>,
        admin_refund_subscriptions: VecDeque<Result<Option<SubscriptionRecord>, StubError>>,
        admin_refund_donations: VecDeque<Result<Option<DonationRecord>, StubError>>,
        deleted_donation_ids: Vec<i64>,
        delete_donation_error: Option<StubError>,
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

        fn with_successful_payment_summary(self, summary: Option<VipSummaryRecord>) -> Self {
            self.lock()
                .successful_payment_summaries
                .push_back(Ok(summary));
            self
        }

        fn with_existing_vip_event_by_subscription(self, event: VipEventRecord) -> Self {
            self.lock().existing_vip_event_by_subscription = Some(event);
            self
        }

        fn with_username_user(self, username: &str, user_id: i64) -> Self {
            self.lock()
                .username_users
                .push((username.to_owned(), user_id));
            self
        }

        fn with_admin_cancel_summary(self, summary: VipSummaryRecord) -> Self {
            self.lock()
                .admin_cancel_summaries
                .push_back(Ok(Some(summary)));
            self
        }

        fn with_admin_cancel_summary_error(self, error: StubError) -> Self {
            self.lock().admin_cancel_summaries.push_back(Err(error));
            self
        }

        fn with_admin_cancel_updated_summary(self, summary: Option<VipSummaryRecord>) -> Self {
            self.lock().admin_cancel_summaries.push_back(Ok(summary));
            self
        }

        fn with_admin_cancel_active_subscription(
            self,
            subscription: Option<SubscriptionRecord>,
        ) -> Self {
            self.lock()
                .admin_cancel_active_subscriptions
                .push_back(Ok(subscription));
            self
        }

        fn with_admin_cancel_active_subscription_error(self, error: StubError) -> Self {
            self.lock()
                .admin_cancel_active_subscriptions
                .push_back(Err(error));
            self
        }

        fn with_mark_subscription_refunded_error(self, error: StubError) -> Self {
            self.lock().mark_subscription_refunded_error = Some(error);
            self
        }

        fn with_admin_refund_subscription(self, subscription: Option<SubscriptionRecord>) -> Self {
            self.lock()
                .admin_refund_subscriptions
                .push_back(Ok(subscription));
            self
        }

        fn with_admin_refund_subscription_error(self, error: StubError) -> Self {
            self.lock().admin_refund_subscriptions.push_back(Err(error));
            self
        }

        fn with_admin_refund_donation(self, donation: Option<DonationRecord>) -> Self {
            self.lock().admin_refund_donations.push_back(Ok(donation));
            self
        }

        fn with_admin_refund_donation_error(self, error: StubError) -> Self {
            self.lock().admin_refund_donations.push_back(Err(error));
            self
        }

        fn with_delete_donation_error(self, error: StubError) -> Self {
            self.lock().delete_donation_error = Some(error);
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

        fn refunded_subscription_ids(&self) -> Vec<i64> {
            self.lock().refunded_subscription_ids.clone()
        }

        fn deleted_donation_ids(&self) -> Vec<i64> {
            self.lock().deleted_donation_ids.clone()
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

        fn get_vip_summary_by_user<'a>(
            &'a self,
            _user_id: i64,
        ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .lock()
                    .successful_payment_summaries
                    .pop_front()
                    .transpose()?
                    .flatten())
            })
        }

        fn get_vip_event_by_subscription_id<'a>(
            &'a self,
            _subscription_id: i64,
        ) -> PaymentStoreFuture<'a, Option<VipEventRecord>, Self::Error> {
            Box::pin(async move { Ok(self.lock().existing_vip_event_by_subscription.clone()) })
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

    impl super::AdminGrantVipStore for StoreStub {
        type Error = StubError;

        fn get_user_id_by_username<'a>(
            &'a self,
            username: &'a str,
        ) -> PaymentStoreFuture<'a, Option<i64>, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .lock()
                    .username_users
                    .iter()
                    .find_map(|(stored, user_id)| (stored == username).then_some(*user_id)))
            })
        }

        fn create_admin_vip_event<'a>(
            &'a self,
            event: VipEventCreate<'a>,
        ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error> {
            <Self as SuccessfulPaymentStore>::create_vip_event(self, event)
        }
    }

    impl super::AdminCancelVipStore for StoreStub {
        type Error = StubError;

        fn get_user_id_by_username<'a>(
            &'a self,
            username: &'a str,
        ) -> PaymentStoreFuture<'a, Option<i64>, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .lock()
                    .username_users
                    .iter()
                    .find_map(|(stored, user_id)| (stored == username).then_some(*user_id)))
            })
        }

        fn get_vip_summary_by_user<'a>(
            &'a self,
            _user_id: i64,
        ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .lock()
                    .admin_cancel_summaries
                    .pop_front()
                    .transpose()?
                    .flatten())
            })
        }

        fn create_admin_cancel_vip_event<'a>(
            &'a self,
            event: VipEventCreate<'a>,
        ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error> {
            <Self as SuccessfulPaymentStore>::create_vip_event(self, event)
        }

        fn get_active_subscription<'a>(
            &'a self,
            _user_id: i64,
        ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .lock()
                    .admin_cancel_active_subscriptions
                    .pop_front()
                    .transpose()?
                    .flatten())
            })
        }

        fn mark_subscription_refunded<'a>(
            &'a self,
            id: i64,
        ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.refunded_subscription_ids.push(id);
                if let Some(error) = state.mark_subscription_refunded_error.take() {
                    return Err(error);
                }
                Ok(SubscriptionRecord {
                    id,
                    user_id: 42,
                    telegram_payment_charge_id: "telegram-charge".to_owned(),
                    provider_payment_charge_id: String::new(),
                    expires_at: OffsetDateTime::UNIX_EPOCH,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    updated_at: OffsetDateTime::UNIX_EPOCH,
                    canceled_at: None,
                    refunded_at: Some(OffsetDateTime::UNIX_EPOCH),
                })
            })
        }
    }

    impl super::AdminRefundStore for StoreStub {
        type Error = StubError;

        fn get_admin_refund_subscription_by_charge_id<'a>(
            &'a self,
            _telegram_payment_charge_id: &'a str,
        ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                match state.admin_refund_subscriptions.pop_front() {
                    Some(result) => result,
                    None => Ok(state.existing_subscription.clone()),
                }
            })
        }

        fn get_admin_refund_donation_by_charge_id<'a>(
            &'a self,
            _telegram_payment_charge_id: &'a str,
        ) -> PaymentStoreFuture<'a, Option<DonationRecord>, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .lock()
                    .admin_refund_donations
                    .pop_front()
                    .transpose()?
                    .flatten())
            })
        }

        fn mark_admin_refund_subscription_refunded<'a>(
            &'a self,
            id: i64,
        ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error> {
            <Self as super::AdminCancelVipStore>::mark_subscription_refunded(self, id)
        }

        fn create_admin_refund_vip_event<'a>(
            &'a self,
            event: VipEventCreate<'a>,
        ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error> {
            <Self as SuccessfulPaymentStore>::create_vip_event(self, event)
        }

        fn delete_admin_refund_donation<'a>(
            &'a self,
            id: i64,
        ) -> PaymentStoreFuture<'a, DonationRecord, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.deleted_donation_ids.push(id);
                if let Some(error) = state.delete_donation_error.take() {
                    return Err(error);
                }
                Ok(DonationRecord {
                    id,
                    user_id: 42,
                    telegram_payment_charge_id: "donation-charge".to_owned(),
                    provider_payment_charge_id: String::new(),
                    amount_stars: 123,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                })
            })
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
        vip_callback_methods: Vec<(String, serde_json::Value)>,
        refund_requests: Vec<(i64, String)>,
        next_refund_error: Option<StubError>,
        direct_texts: Vec<(i64, String)>,
        payment_redirect_messages: Vec<PaymentRedirectMessage>,
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

        fn with_next_refund_error(self, error: StubError) -> Self {
            self.lock().next_refund_error = Some(error);
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

        fn refund_requests(&self) -> Vec<(i64, String)> {
            self.lock().refund_requests.clone()
        }

        fn direct_texts(&self) -> Vec<(i64, String)> {
            self.lock().direct_texts.clone()
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

        fn vip_callback_methods(&self) -> Vec<(String, serde_json::Value)> {
            self.lock().vip_callback_methods.clone()
        }

        fn payment_redirect_messages(&self) -> Vec<PaymentRedirectMessage> {
            self.lock().payment_redirect_messages.clone()
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

    impl super::AdminCancelVipEffects for EffectsStub {
        type Error = StubError;

        fn send_admin_cancel_vip_text<'a>(
            &'a self,
            message: PaymentTextMessage<'a>,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            <Self as SuccessfulPaymentEffects>::send_text(self, message)
        }

        fn invalidate_admin_cancel_vip_cache<'a>(
            &'a self,
            user_id: i64,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            <Self as SuccessfulPaymentEffects>::invalidate_vip_cache(self, user_id)
        }

        fn refund_admin_cancel_vip_payment<'a>(
            &'a self,
            user_id: i64,
            telegram_payment_charge_id: &'a str,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state
                    .refund_requests
                    .push((user_id, telegram_payment_charge_id.to_owned()));
                match state.next_refund_error.take() {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            })
        }

        fn send_admin_cancel_vip_user_text<'a>(
            &'a self,
            user_id: i64,
            text: &'a str,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().direct_texts.push((user_id, text.to_owned()));
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

    impl super::PaymentRedirectEffects for EffectsStub {
        type Error = StubError;

        fn send_payment_redirect_message<'a>(
            &'a self,
            message: PaymentRedirectMessage,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().payment_redirect_messages.push(message);
                Ok(())
            })
        }
    }

    impl VipCancellationEffects for EffectsStub {
        type Error = StubError;

        fn execute_vip_cancellation_method<'a>(
            &'a self,
            method: openplotva_telegram::TelegramOutboundMethod,
        ) -> VipCancellationEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                let name = method.method_name().to_owned();
                let payload = telegram_method_payload(method)?;
                self.lock().vip_callback_methods.push((name, payload));
                Ok(())
            })
        }

        fn invalidate_vip_cancellation_cache<'a>(
            &'a self,
            user_id: i64,
        ) -> VipCancellationEffectFuture<'a, Self::Error> {
            <Self as SuccessfulPaymentEffects>::invalidate_vip_cache(self, user_id)
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

    #[derive(Default)]
    struct CallbackAckEffectsStub {
        methods: Mutex<
            Vec<(
                openplotva_telegram::TelegramOutboundMethodKind,
                serde_json::Value,
            )>,
        >,
    }

    impl CallbackAckEffectsStub {
        fn methods(
            &self,
        ) -> Vec<(
            openplotva_telegram::TelegramOutboundMethodKind,
            serde_json::Value,
        )> {
            self.methods.lock().expect("callback ack methods").clone()
        }
    }

    impl CallbackQueryEffects for CallbackAckEffectsStub {
        type Error = io::Error;

        fn answer_callback_query<'a>(
            &'a self,
            method: openplotva_telegram::TelegramOutboundMethod,
        ) -> CallbackQueryFuture<'a, Self::Error> {
            Box::pin(async move {
                let kind = method.kind();
                let payload = match method {
                    openplotva_telegram::TelegramOutboundMethod::AnswerCallbackQuery(method) => {
                        serde_json::to_value(method.as_ref()).map_err(io::Error::other)?
                    }
                    _ => return Err(io::Error::other("unexpected callback ack method")),
                };
                self.methods
                    .lock()
                    .expect("callback ack methods")
                    .push((kind, payload));
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct CallbackRateLimitStub {
        limited_chat_id: Option<i64>,
    }

    impl CallbackRateLimitPolicy for CallbackRateLimitStub {
        fn is_callback_chat_rate_limited<'a>(
            &'a self,
            chat_id: i64,
        ) -> CallbackRateLimitFuture<'a> {
            Box::pin(async move { self.limited_chat_id == Some(chat_id) })
        }
    }

    #[derive(Clone, Default)]
    struct UpdateStateStoreStub {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl UpdateStateStoreStub {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("state calls").clone()
        }
    }

    impl UpdateStateStore for UpdateStateStoreStub {
        type Error = io::Error;

        fn upsert_chat_state<'a>(
            &'a self,
            chat: &'a openplotva_core::ChatState,
        ) -> UpdateStateStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!(
                        "chat:{}:{}:{}:{}",
                        chat.id,
                        chat.chat_type,
                        chat.first_name.as_deref().unwrap_or_default(),
                        chat.username.as_deref().unwrap_or_default()
                    ));
                Ok(())
            })
        }

        fn upsert_user_state<'a>(
            &'a self,
            user: &'a UserState,
        ) -> UpdateStateStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!(
                        "user:{}:{}:{}",
                        user.id,
                        user.first_name,
                        user.username.as_deref().unwrap_or_default()
                    ));
                Ok(())
            })
        }

        fn upsert_telegram_file_metadata<'a>(
            &'a self,
            params: &'a openplotva_storage::TelegramFileMetadataUpsert,
        ) -> TelegramFileMetadataStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!("file:{}", params.file_unique_id));
                Ok(())
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
        Other(String),
        Request,
    }

    impl fmt::Display for StubError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Duplicate => f.write_str("duplicate"),
                Self::Other(message) => f.write_str(message),
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
