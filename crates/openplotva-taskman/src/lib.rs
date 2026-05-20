//! Observable and recoverable background task orchestration.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "taskman";

/// Go control queue name used for fetcher-side maintenance jobs.
pub const CONTROL_QUEUE_NAME: &str = "control";

/// Go task priority value.
pub type Priority = i32;

/// Go default task priority.
pub const DEFAULT_PRIORITY: Priority = 0;
/// Go high task priority.
pub const HIGH_PRIORITY: Priority = 2;

/// Go task payload type.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    Control,
}

/// Go control job kind.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    #[default]
    #[serde(rename = "")]
    Unknown,
    Translate,
    GroupSettings,
    VipInvoice,
    DonateInvoice,
    SuccessfulPayment,
    Checkin,
    ChatAdminsSync,
    ChatMemberSync,
    NewMembersFollowup,
}

/// Go task payload wrapper.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobPayload {
    /// Payload kind. Serialized as Go's JSON `type` field.
    #[serde(rename = "type")]
    pub job_type: JobType,
    /// Telegram routing metadata used by task executors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub telegram_data: Option<TelegramData>,
    /// Control-job-specific payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_data: Option<ControlJobData>,
}

/// Go Telegram metadata embedded into task payloads.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TelegramData {
    pub chat_id: i64,
    pub user_id: i64,
    pub message_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_message_id: Option<i32>,
    pub user_full_name: String,
    pub chat_title: String,
}

/// Go control job data.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControlJobData {
    pub kind: ControlKind,
    #[serde(skip_serializing_if = "is_default_i64")]
    pub amount: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub target_lang: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub theme: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub reply_text: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub chat_type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub user_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub first_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub last_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub language_code: String,
    #[serde(skip_serializing_if = "is_false")]
    pub is_premium: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub callback_query_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub new_chat_member_ids: Vec<i64>,
    #[serde(skip_serializing_if = "is_false")]
    pub bot_was_added: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payment: Option<ControlPayment>,
}

/// Go successful-payment payload stored inside a control job.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControlPayment {
    pub currency: String,
    pub total_amount: i32,
    pub invoice_payload: String,
    pub telegram_payment_charge_id: String,
    pub provider_payment_charge_id: String,
}

/// Input for constructing a Go-shaped control job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlJobParams {
    pub chat_id: i64,
    pub message_id: i32,
    pub user_id: i64,
    pub user_full_name: String,
    pub thread_id: Option<i32>,
    pub data: ControlJobData,
}

/// Go stateless task item.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StatelessJobItem {
    pub title: String,
    pub created: OffsetDateTime,
    pub priority: Priority,
    pub data: JobPayload,
}

/// Build Go `NewControlJob` shape with an injected clock for tests/runtime determinism.
#[must_use]
pub fn new_control_job_at(params: ControlJobParams, created: OffsetDateTime) -> StatelessJobItem {
    StatelessJobItem {
        title: "control".to_owned(),
        created,
        priority: DEFAULT_PRIORITY,
        data: JobPayload {
            job_type: JobType::Control,
            telegram_data: Some(TelegramData {
                chat_id: params.chat_id,
                user_id: params.user_id,
                message_id: params.message_id,
                thread_message_id: params.thread_id,
                user_full_name: clean_unicode_non_printables(&params.user_full_name),
                chat_title: String::new(),
            }),
            control_data: Some(params.data),
        },
    }
}

impl StatelessJobItem {
    /// Match Go `WithName`.
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.title = name.into();
        self
    }

    /// Match Go `WithPriority`.
    #[must_use]
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }
}

fn is_default_i64(value: &i64) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn clean_unicode_non_printables(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }

    let mut result = String::with_capacity(text.len());
    let mut pending_space = false;
    for ch in text.chars() {
        let Some(ch) = clean_unicode_char(ch) else {
            continue;
        };
        if ch == ' ' || ch == '\t' {
            pending_space = true;
            continue;
        }
        if pending_space {
            result.push(' ');
            pending_space = false;
        }
        result.push(ch);
    }
    result.trim().to_owned()
}

fn clean_unicode_char(ch: char) -> Option<char> {
    match ch {
        '\u{00a0}' | '\u{2000}' | '\u{2001}' | '\u{2002}' | '\u{2003}' | '\u{2004}'
        | '\u{2005}' | '\u{2006}' | '\u{2007}' | '\u{2008}' | '\u{2009}' | '\u{200a}' => Some(' '),
        '\u{200b}' | '\u{200c}' | '\u{200d}' | '\u{200e}' | '\u{200f}' | '\u{feff}' => None,
        _ if ch.is_control() && !matches!(ch, '\t' | '\n' | '\r') => None,
        _ => Some(ch),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use time::OffsetDateTime;

    use super::{
        CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, HIGH_PRIORITY, JobType,
        new_control_job_at,
    };

    #[test]
    fn control_job_payload_matches_go_shape_for_vip_invoice_queueing()
    -> Result<(), Box<dyn std::error::Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_control_job_at(
            ControlJobParams {
                chat_id: 10,
                message_id: 20,
                user_id: 30,
                user_full_name: " Alice\u{200f}\tSmith ".to_owned(),
                thread_id: Some(40),
                data: ControlJobData {
                    kind: ControlKind::VipInvoice,
                    amount: 300,
                    user_name: "Alice".to_owned(),
                    first_name: "Alice".to_owned(),
                    ..ControlJobData::default()
                },
            },
            created,
        )
        .with_name("vip invoice")
        .with_priority(HIGH_PRIORITY);

        assert_eq!(CONTROL_QUEUE_NAME, "control");
        assert_eq!(job.title, "vip invoice");
        assert_eq!(job.priority, HIGH_PRIORITY);
        assert_eq!(job.data.job_type, JobType::Control);
        let telegram_data = job
            .data
            .telegram_data
            .as_ref()
            .expect("control job should include Telegram data");
        assert_eq!(telegram_data.user_full_name, "Alice Smith");
        assert_eq!(telegram_data.thread_message_id, Some(40));

        let payload = serde_json::to_value(&job.data)?;
        assert_eq!(
            payload,
            json!({
                "type": "control",
                "telegram_data": {
                    "chat_id": 10,
                    "user_id": 30,
                    "message_id": 20,
                    "thread_message_id": 40,
                    "user_full_name": "Alice Smith",
                    "chat_title": ""
                },
                "control_data": {
                    "kind": "vip_invoice",
                    "amount": 300,
                    "user_name": "Alice",
                    "first_name": "Alice"
                }
            })
        );
        Ok(())
    }

    #[test]
    fn control_job_data_serializes_donation_and_successful_payment_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let donation = ControlJobData {
            kind: ControlKind::DonateInvoice,
            amount: 600,
            ..ControlJobData::default()
        };
        assert_eq!(
            serde_json::to_value(donation)?,
            json!({
                "kind": "donate_invoice",
                "amount": 600
            })
        );

        let successful_payment = ControlJobData {
            kind: ControlKind::SuccessfulPayment,
            payment: Some(super::ControlPayment {
                currency: "XTR".to_owned(),
                total_amount: 300,
                invoice_payload: "subscription_42".to_owned(),
                telegram_payment_charge_id: "telegram-charge".to_owned(),
                provider_payment_charge_id: "provider-charge".to_owned(),
            }),
            ..ControlJobData::default()
        };
        assert_eq!(
            serde_json::to_value(successful_payment)?,
            json!({
                "kind": "successful_payment",
                "payment": {
                    "currency": "XTR",
                    "total_amount": 300,
                    "invoice_payload": "subscription_42",
                    "telegram_payment_charge_id": "telegram-charge",
                    "provider_payment_charge_id": "provider-charge"
                }
            })
        );
        Ok(())
    }
}
