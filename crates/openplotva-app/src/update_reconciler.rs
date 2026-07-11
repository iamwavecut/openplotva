//! One-time startup repair for inbox rows created before compact lane heads.

use carapax::types::{
    MessageData as TelegramMessageData, Update as TelegramUpdate, UpdateType as TelegramUpdateType,
    User as TelegramUser,
};
use openplotva_storage::{
    PostgresTelegramDeliveryStore, StorageError, TelegramUpdateStartupReconcileReport,
};
use openplotva_updates::{
    decode_telegram_update_json_slice, is_passive_update, is_payment_update, parse_if_addressed,
};
use time::OffsetDateTime;

const STARTUP_RECONCILE_PAGE_ROWS: usize = 2_000;
const STARTUP_RECONCILE_MIN_AGE: time::Duration = time::Duration::minutes(5);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UpdateBacklogReconcileReport {
    pub scanned_rows: u64,
    pub decode_failures: u64,
    pub storage: TelegramUpdateStartupReconcileReport,
}

pub async fn reconcile_update_backlog_once(
    store: &PostgresTelegramDeliveryStore,
    bot_id: i64,
    bot_user: &TelegramUser,
) -> Result<UpdateBacklogReconcileReport, StorageError> {
    let job_name = format!("telegram-inbox-backlog-v1:{bot_id}");
    let Some(mut guard) = store.begin_startup_reconciliation(&job_name).await? else {
        return Ok(UpdateBacklogReconcileReport {
            storage: TelegramUpdateStartupReconcileReport {
                already_completed: true,
                ..TelegramUpdateStartupReconcileReport::default()
            },
            ..UpdateBacklogReconcileReport::default()
        });
    };

    let before = OffsetDateTime::now_utc() - STARTUP_RECONCILE_MIN_AGE;
    let mut after_id = 0_i64;
    let mut ignored_ids = Vec::new();
    let mut report = UpdateBacklogReconcileReport::default();
    loop {
        let candidates = store
            .startup_reconcile_candidates(
                &mut guard,
                bot_id,
                before,
                after_id,
                STARTUP_RECONCILE_PAGE_ROWS,
            )
            .await?;
        if candidates.is_empty() {
            break;
        }
        report.scanned_rows = report.scanned_rows.saturating_add(candidates.len() as u64);
        for candidate in &candidates {
            match decode_telegram_update_json_slice(&candidate.raw_payload) {
                Ok(update) if should_ignore_startup_backlog(&update, bot_user) => {
                    ignored_ids.push(candidate.id);
                }
                Ok(_) => {}
                Err(_) => {
                    report.decode_failures = report.decode_failures.saturating_add(1);
                }
            }
        }
        after_id = candidates.last().map_or(after_id, |candidate| candidate.id);
        if candidates.len() < STARTUP_RECONCILE_PAGE_ROWS {
            break;
        }
    }

    report.storage = store
        .complete_startup_reconciliation(guard, bot_id, &ignored_ids)
        .await?;
    Ok(report)
}

fn should_ignore_startup_backlog(update: &TelegramUpdate, bot_user: &TelegramUser) -> bool {
    if is_payment_update(update) {
        return false;
    }
    if is_passive_update(update) {
        return true;
    }
    match &update.update_type {
        TelegramUpdateType::InlineQuery(_) => true,
        TelegramUpdateType::Message(message) | TelegramUpdateType::EditedMessage(message) => {
            if matches!(
                message.data,
                TelegramMessageData::NewChatMembers(_) | TelegramMessageData::LeftChatMember(_)
            ) {
                return false;
            }
            let parsed = parse_if_addressed(message, bot_user);
            if parsed.is_addressed {
                return false;
            }
            !parsed.first_word.starts_with(['/', '!', '%'])
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use carapax::types::{Update as TelegramUpdate, User as TelegramUser};
    use serde_json::json;

    use super::should_ignore_startup_backlog;

    fn bot_user() -> Result<TelegramUser, serde_json::Error> {
        serde_json::from_value(json!({
            "id": 777,
            "is_bot": true,
            "first_name": "Plotva",
            "username": "PlotvaBot"
        }))
    }

    fn update(value: serde_json::Value) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(value)
    }

    #[test]
    fn startup_reconciliation_keeps_addressed_private_and_payment_messages()
    -> Result<(), Box<dyn Error>> {
        let bot = bot_user()?;
        let private = update(json!({
            "update_id": 1,
            "message": {
                "message_id": 1,
                "date": 1_710_000_000,
                "chat": {"id": 10, "type": "private", "first_name": "Ada"},
                "from": {"id": 10, "is_bot": false, "first_name": "Ada"},
                "text": "hello"
            }
        }))?;
        let addressed = update(json!({
            "update_id": 2,
            "message": {
                "message_id": 2,
                "date": 1_710_000_000,
                "chat": {"id": -10, "type": "group", "title": "Group"},
                "from": {"id": 10, "is_bot": false, "first_name": "Ada"},
                "text": "@PlotvaBot hello"
            }
        }))?;
        let payment = update(json!({
            "update_id": 3,
            "pre_checkout_query": {
                "id": "checkout",
                "from": {"id": 10, "is_bot": false, "first_name": "Ada"},
                "currency": "XTR",
                "total_amount": 10,
                "invoice_payload": "vip"
            }
        }))?;

        assert!(!should_ignore_startup_backlog(&private, &bot));
        assert!(!should_ignore_startup_backlog(&addressed, &bot));
        assert!(!should_ignore_startup_backlog(&payment, &bot));
        Ok(())
    }

    #[test]
    fn startup_reconciliation_drops_stale_group_noise_and_passive_reactions()
    -> Result<(), Box<dyn Error>> {
        let bot = bot_user()?;
        let noise = update(json!({
            "update_id": 4,
            "message": {
                "message_id": 4,
                "date": 1_710_000_000,
                "chat": {"id": -10, "type": "group", "title": "Group"},
                "from": {"id": 10, "is_bot": false, "first_name": "Ada"},
                "text": "ordinary group chatter"
            }
        }))?;
        let reaction = update(json!({
            "update_id": 5,
            "message_reaction": {
                "chat": {"id": -10, "type": "group", "title": "Group"},
                "message_id": 4,
                "date": 1_710_000_000,
                "user": {"id": 10, "is_bot": false, "first_name": "Ada"},
                "old_reaction": [],
                "new_reaction": [{"type": "emoji", "emoji": "👍"}]
            }
        }))?;

        assert!(should_ignore_startup_backlog(&noise, &bot));
        assert!(should_ignore_startup_backlog(&reaction, &bot));
        Ok(())
    }
}
