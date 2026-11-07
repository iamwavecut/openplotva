use super::*;
use crate::checkin::*;
use crate::members::*;
use crate::payments::*;
use crate::settings::*;
use crate::translate::*;
use openplotva_core::UserState;
use openplotva_storage::*;

pub struct UnusedControlPorts;

impl SuccessfulPaymentStore for UnusedControlPorts {
    type Error = std::convert::Infallible;
    fn upsert_payment_user<'a>(
        &'a self,
        _user: &'a UserState,
    ) -> PaymentStoreFuture<'a, (), Self::Error> {
        panic!("unexpected control executor port")
    }
    fn create_subscription<'a>(
        &'a self,
        _subscription: SubscriptionCreate<'a>,
    ) -> PaymentStoreFuture<'a, SubscriptionRecord, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn get_subscription_by_telegram_payment_charge_id<'a>(
        &'a self,
        _telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<SubscriptionRecord>, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn get_vip_summary_by_user<'a>(
        &'a self,
        _user_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipSummaryRecord>, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn get_vip_event_by_subscription_id<'a>(
        &'a self,
        _subscription_id: i64,
    ) -> PaymentStoreFuture<'a, Option<VipEventRecord>, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn create_donation<'a>(
        &'a self,
        _donation: DonationCreate<'a>,
    ) -> PaymentStoreFuture<'a, DonationRecord, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn get_donation_by_telegram_payment_charge_id<'a>(
        &'a self,
        _telegram_payment_charge_id: &'a str,
    ) -> PaymentStoreFuture<'a, Option<DonationRecord>, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn create_vip_event<'a>(
        &'a self,
        _event: VipEventCreate<'a>,
    ) -> PaymentStoreFuture<'a, VipEventRecord, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn is_duplicate_insert_no_row(_error: &Self::Error) -> bool {
        panic!("unexpected control executor port")
    }
}

impl PaymentInvoiceEffects for UnusedControlPorts {
    type Error = std::convert::Infallible;
    fn create_subscription_invoice_link<'a>(
        &'a self,
        _request: &'a openplotva_telegram::SubscriptionInvoiceLinkRequest,
    ) -> PaymentInvoiceLinkFuture<'a, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn subscription_price_override(&self, _user_id: i64) -> Option<i64> {
        panic!("unexpected control executor port")
    }
    fn create_donation_invoice_link<'a>(
        &'a self,
        _request: &'a openplotva_telegram::DonationInvoiceLinkRequest,
    ) -> PaymentInvoiceLinkFuture<'a, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn send_invoice_button_message<'a>(
        &'a self,
        _message: InvoiceButtonMessage,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn send_invoice_error_text<'a>(
        &'a self,
        _chat_id: i64,
        _reply_to_message_id: i32,
        _text: &'a str,
        _parse_mode: &'a str,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        panic!("unexpected control executor port")
    }
}

impl SuccessfulPaymentEffects for UnusedControlPorts {
    type Error = std::convert::Infallible;
    fn send_text<'a>(
        &'a self,
        _message: PaymentTextMessage<'a>,
    ) -> PaymentEffectsFuture<'a, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn invalidate_vip_cache<'a>(&'a self, _user_id: i64) -> PaymentEffectsFuture<'a, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn support_url(&self) -> &str {
        panic!("unexpected control executor port")
    }
}

impl GroupSettingsControlJobEffects for UnusedControlPorts {
    type Error = std::convert::Infallible;
    fn can_open_group_settings<'a>(
        &'a self,
        _chat_id: i64,
        _user_id: i64,
    ) -> GroupSettingsControlJobFuture<'a, bool, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn sync_chat_admins<'a>(&'a self, _chat_id: i64) -> GroupSettingsSyncFuture<'a> {
        panic!("unexpected control executor port")
    }
}

impl NewMembersFollowupControlJobEffects for UnusedControlPorts {
    fn enable_chat_communication<'a>(&'a self, _chat_id: i64) -> NewMembersFollowupFuture<'a, ()> {
        panic!("unexpected control executor port")
    }
    fn is_chat_blocked<'a>(&'a self, _chat_id: i64) -> NewMembersFollowupFuture<'a, bool> {
        panic!("unexpected control executor port")
    }
    fn try_send_greeting_for_join_wave<'a>(
        &'a self,
        _greeting: NewMembersGreeting,
    ) -> NewMembersFollowupFuture<'a, ()> {
        panic!("unexpected control executor port")
    }
}

impl TextTranslator for UnusedControlPorts {
    type Error = std::convert::Infallible;
    fn translate_text<'a>(
        &'a self,
        _text: &'a str,
        _target_lang: &'a str,
    ) -> TranslateProviderFuture<'a, Self::Error> {
        panic!("unexpected control executor port")
    }
}

impl TranslateEffects for UnusedControlPorts {
    type Error = std::convert::Infallible;
    fn send_translate_notice<'a>(
        &'a self,
        _message: TranslateCommandMessage,
        _text: String,
        _delete_after: Duration,
    ) -> TranslateEffectFuture<'a, Self::Error> {
        panic!("unexpected control executor port")
    }
    fn send_translation_result<'a>(
        &'a self,
        _plan: TranslateResultPlan,
    ) -> TranslateEffectFuture<'a, Self::Error> {
        panic!("unexpected control executor port")
    }
}

impl MemberStateControlJobEffects for UnusedControlPorts {
    fn sync_chat_admins<'a>(&'a self, _chat_id: i64) -> MemberStateControlJobFuture<'a> {
        panic!("unexpected control executor port")
    }
}

impl CheckinControlJobEffects for UnusedControlPorts {
    type Error = std::convert::Infallible;
    fn run_checkin_game<'a>(
        &'a self,
        _job_id: i64,
        _params: &'a ControlJobParams,
    ) -> CheckinControlJobFuture<'a, Self::Error> {
        panic!("unexpected control executor port")
    }
}
