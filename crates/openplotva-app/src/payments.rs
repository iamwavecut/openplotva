use std::{fmt, future::Future, pin::Pin};

use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, MessageData as TelegramMessageData,
    PreCheckoutQuery as TelegramPreCheckoutQuery, SuccessfulPayment as TelegramSuccessfulPayment,
    Update as TelegramUpdate, UpdateType as TelegramUpdateType, User as TelegramUser,
};
use openplotva_core::{UserState, VIP_EVENT_TYPE_PAYMENT, vip_days_to_seconds};
use openplotva_storage::{
    DonationCreate, DonationRecord, PostgresPaymentStore, PostgresVipStore,
    PostgresVirtualMessageStore, StorageError, SubscriptionCreate, SubscriptionRecord,
    VipEventCreate, VipEventRecord,
};
use openplotva_taskman::{
    CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, ControlPayment,
    HIGH_PRIORITY, StatelessJobItem, new_control_job_at,
};
use thiserror::Error;
use time::OffsetDateTime;

/// Boxed future returned by payment storage calls.
pub type PaymentStoreFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by payment side-effect calls.
pub type PaymentEffectsFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by direct pre-checkout acknowledgement calls.
pub type PreCheckoutFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by direct invoice-link creation calls.
pub type PaymentInvoiceLinkFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<String, E>> + Send + 'a>>;

/// Boxed future returned by taskman control-job assignment calls.
pub type PaymentControlJobQueueFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

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

/// Payment processing result class.
#[derive(Clone, Debug, Eq, PartialEq)]
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

/// Control-job invoice execution result class.
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvoiceControlJobReport {
    /// Result class.
    pub outcome: InvoiceControlJobOutcome,
    /// Displayable side-effect error, if the outcome represents a failure.
    pub error: Option<String>,
}

/// Payment-specific taskman control-job routing result.
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaymentControlJobReport {
    /// Result class.
    pub outcome: PaymentControlJobOutcome,
    /// Displayable storage/side-effect error, if the routed executor reported one.
    pub error: Option<String>,
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

/// Side-effect boundary for Go successful-payment processing.
pub trait SuccessfulPaymentEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Send a user-facing text response. Go ignores send failures here.
    fn send_text<'a>(
        &'a self,
        chat_id: i64,
        reply_to_message_id: i32,
        text: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error>;

    /// Invalidate cached VIP status after subscription activation.
    fn invalidate_vip_cache<'a>(&'a self, user_id: i64) -> PaymentEffectsFuture<'a, Self::Error>;
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
                    send_payment_text(effects, message, SUBSCRIPTION_DETAILS_ERROR_TEXT).await;
                    return SuccessfulPaymentReport::with_error(
                        SuccessfulPaymentOutcome::SubscriptionStorageError,
                        error,
                    );
                }
                Err(error) => {
                    send_payment_text(effects, message, SUBSCRIPTION_DETAILS_ERROR_TEXT).await;
                    return SuccessfulPaymentReport::with_error(
                        SuccessfulPaymentOutcome::SubscriptionStorageError,
                        error,
                    );
                }
            }
        }
        Err(error) => {
            send_payment_text(effects, message, SUBSCRIPTION_SAVE_ERROR_TEXT).await;
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
            send_payment_text(effects, message, SUBSCRIPTION_LEDGER_ERROR_TEXT).await;
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
                    send_payment_text(effects, message, DONATION_DETAILS_ERROR_TEXT).await;
                    return SuccessfulPaymentReport::with_error(
                        SuccessfulPaymentOutcome::DonationStorageError,
                        error,
                    );
                }
                Err(error) => {
                    send_payment_text(effects, message, DONATION_DETAILS_ERROR_TEXT).await;
                    return SuccessfulPaymentReport::with_error(
                        SuccessfulPaymentOutcome::DonationStorageError,
                        error,
                    );
                }
            }
        }
        Err(error) => {
            send_payment_text(effects, message, DONATION_SAVE_ERROR_TEXT).await;
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
) where
    Effects: SuccessfulPaymentEffects + Sync,
{
    let _ = effects
        .send_text(message.chat_id, message.message_id, text)
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

const SUBSCRIPTION_SAVE_ERROR_TEXT: &str = "❌ Платеж прошел успешно, но возникла ошибка при активации подписки. Пожалуйста, обратитесь в [поддержку](https://t.me/WaveCut)";
const SUBSCRIPTION_DETAILS_ERROR_TEXT: &str = "✅ Платеж успешно обработан, но возникла ошибка при получении деталей подписки. Пожалуйста, обратитесь в [поддержку](https://t.me/WaveCut)";
const SUBSCRIPTION_LEDGER_ERROR_TEXT: &str = "❌ Платеж прошел успешно, но возникла ошибка при фиксации VIP периода. Пожалуйста, обратитесь в [поддержку](https://t.me/WaveCut)";
const DONATION_SAVE_ERROR_TEXT: &str = "❤️ Спасибо за донат! К сожалению, возникла ошибка при сохранении информации о платеже. Можете сообщить об этом в [поддержку](https://t.me/WaveCut)";
const DONATION_DETAILS_ERROR_TEXT: &str = "❤️ Спасибо за ваш донат! Платеж успешно обработан, но возникла ошибка при получении деталей. Можете сообщить об этом в [поддержку](https://t.me/WaveCut)";
const USER_IDENTIFICATION_ERROR_TEXT: &str = "❌ Не удалось определить пользователя.";
const SUBSCRIPTION_INVOICE_CREATE_ERROR_TEXT: &str =
    "❌ Не удалось создать счет для подписки. Пожалуйста, попробуйте позже.";
const DONATION_INVOICE_CREATE_ERROR_TEXT: &str =
    "❌ Не удалось создать счет для доната. Пожалуйста, попробуйте позже.";
const SUBSCRIPTION_INVOICE_BUTTON_TEXT: &str = "💎 Оформить VIP подписку";
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
        sync::{Arc, Mutex, MutexGuard},
    };

    use carapax::types::Update as TelegramUpdate;
    use openplotva_core::{UserState, VIP_EVENT_TYPE_PAYMENT, vip_days_to_seconds};
    use openplotva_storage::{
        DonationCreate, DonationRecord, SubscriptionCreate, SubscriptionRecord, VipEventCreate,
        VipEventRecord,
    };
    use openplotva_taskman::{
        CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, ControlPayment,
        HIGH_PRIORITY, JobType, StatelessJobItem,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::{
        InvoiceButtonMessage, InvoiceControlJobOutcome, PaymentControlJobOutcome,
        PaymentControlJobQueue, PaymentControlJobQueueFuture, PaymentEffectsFuture,
        PaymentInvoiceEffects, PaymentInvoiceLinkFuture, PaymentStoreFuture, PreCheckoutOutcome,
        PreCheckoutPaymentEffects, PreCheckoutUpdateRoute, SuccessfulPayment,
        SuccessfulPaymentControlJobUpdateRoute, SuccessfulPaymentEffects, SuccessfulPaymentMessage,
        SuccessfulPaymentOutcome, SuccessfulPaymentStore, SuccessfulPaymentUpdateRoute,
        enqueue_successful_payment_update_or_else_at, execute_donate_invoice_control_job,
        execute_payment_control_job_at, execute_vip_invoice_control_job,
        handle_pre_checkout_update_or_else, handle_successful_payment_update_or_else_at,
        invoice_button_send_message_method, process_successful_payment_at,
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
    struct EffectsStub {
        state: Arc<Mutex<EffectsState>>,
    }

    #[derive(Default)]
    struct EffectsState {
        sent_texts: Vec<String>,
        invalidated_vip_users: Vec<i64>,
        answered_pre_checkout_queries: Vec<String>,
        next_pre_checkout_error: Option<StubError>,
        subscription_invoice_requests: Vec<openplotva_telegram::SubscriptionInvoiceLinkRequest>,
        donation_invoice_requests: Vec<openplotva_telegram::DonationInvoiceLinkRequest>,
        next_invoice_urls: VecDeque<String>,
        invoice_messages: Vec<InvoiceButtonMessage>,
        invoice_error_texts: Vec<(i64, i32, String, String)>,
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
            _chat_id: i64,
            _reply_to_message_id: i32,
            text: &'a str,
        ) -> PaymentEffectsFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().sent_texts.push(text.to_owned());
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
