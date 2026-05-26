use std::{fmt, future::Future, sync::Arc};

use carapax::types::{Update as TelegramUpdate, UpdateType};
use thiserror::Error;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkippedUpdateRoute {
    Delegated,
    Skipped { update_name: &'static str },
}

/// Error returned when delegated handling fails.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SkippedUpdateError {
    /// Downstream handler failed after delegation.
    #[error("downstream update handler: {message}")]
    Downstream {
        /// Display form of the downstream error.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct SkippedUpdateHandler<Next> {
    next: Arc<Next>,
}

impl<Next> SkippedUpdateHandler<Next> {
    /// Build a skipped-update handler around the real downstream update handler.
    pub fn new(next: Arc<Next>) -> Self {
        Self { next }
    }
}

impl<Next> UpdateHandler for SkippedUpdateHandler<Next>
where
    Next: UpdateHandler + Send + Sync,
{
    type Error = SkippedUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_skipped_update_or_else(update, |update| self.next.handle_update(update))
                .await
                .map(|_| ())
        })
    }
}

pub async fn handle_skipped_update_or_else<HandleFn, HandleFuture, HandleError>(
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<SkippedUpdateRoute, SkippedUpdateError>
where
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    if let Some(update_name) = go_skipped_update_name(&update.update_type) {
        tracing::debug!(update_name, "skipping accepted update without processor");
        return Ok(SkippedUpdateRoute::Skipped { update_name });
    }

    handle_other(update)
        .await
        .map_err(|error| SkippedUpdateError::Downstream {
            message: error.to_string(),
        })?;
    Ok(SkippedUpdateRoute::Delegated)
}

#[must_use]
pub fn go_skipped_update_name(update: &UpdateType) -> Option<&'static str> {
    match update {
        UpdateType::ChannelPost(_) => Some("channel_post"),
        UpdateType::EditedChannelPost(_) => Some("edited_channel_post"),
        UpdateType::ChosenInlineResult(_) => Some("chosen_inline_result"),
        UpdateType::ShippingQuery(_) => Some("shipping_query"),
        UpdateType::Poll(_) => Some("poll"),
        UpdateType::PollAnswer(_) => Some("poll_answer"),
        UpdateType::BotStatus(_) => Some("my_chat_member"),
        UpdateType::UserStatus(_) => Some("chat_member"),
        UpdateType::ChatJoinRequest(_) => Some("chat_join_request"),
        UpdateType::MessageReaction(_) => Some("message_reaction"),
        UpdateType::MessageReactionCount(_) => Some("message_reaction_count"),
        UpdateType::BusinessConnection(_)
        | UpdateType::BusinessMessage(_)
        | UpdateType::ChatBoostRemoved(_)
        | UpdateType::ChatBoostUpdated(_)
        | UpdateType::DeletedBusinessMessages(_)
        | UpdateType::EditedBusinessMessage(_)
        | UpdateType::ManagedBot(_)
        | UpdateType::PurchasedPaidMedia(_)
        | UpdateType::Unknown(_) => Some("unknown"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        error::Error,
        io,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use carapax::types::Update as TelegramUpdate;
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome, producer_update_name};
    use serde_json::json;

    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
        UpdateStateStoreFuture, process_update_with_state_store_at,
    };

    use super::{SkippedUpdateHandler, SkippedUpdateRoute, handle_skipped_update_or_else};

    #[tokio::test]
    async fn go_skipped_update_types_are_consumed_without_delegation() -> Result<(), Box<dyn Error>>
    {
        for (update_name, update) in go_skipped_update_cases()? {
            let next = UpdateHandlerStub::default();

            let route =
                handle_skipped_update_or_else(update, |update| next.handle_update(update)).await?;

            assert_eq!(route, SkippedUpdateRoute::Skipped { update_name });
            assert_eq!(next.handled_count(), 0, "{update_name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn message_update_delegates_once() -> Result<(), Box<dyn Error>> {
        let next = UpdateHandlerStub::default();

        let route =
            handle_skipped_update_or_else(text_update()?, |update| next.handle_update(update))
                .await?;

        assert_eq!(route, SkippedUpdateRoute::Delegated);
        assert_eq!(next.handled_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn handler_adapter_skips_before_wrapped_handler() -> Result<(), Box<dyn Error>> {
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = SkippedUpdateHandler::new(Arc::clone(&next));

        handler.handle_update(chosen_inline_update()?).await?;

        assert_eq!(next.handled_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_skipped_updates_stop_before_terminal_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-skipped:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let cases = live_redis_skipped_update_cases()?;
        for (_, _, _, update) in &cases {
            update_queue.enqueue_update(update).await?;
        }
        assert_eq!(update_queue.len().await?, cases.len() as i64);

        let state_store = UpdateStateStoreStub::default();
        let terminal = Arc::new(UpdateHandlerStub::default());
        let skipped = SkippedUpdateHandler::new(Arc::clone(&terminal));
        let config = UpdateConsumerConfig {
            dequeue_timeout: Duration::from_millis(1),
            state_timeout: Duration::from_secs(1),
            handle_timeout: Duration::from_secs(1),
            side_effect_max_age: Duration::from_secs(60),
            worker_limit: 1,
        };
        let now = UNIX_EPOCH + Duration::from_secs(1_710_000_010);

        for (expected_producer_name, expected_update_id, update_name, _) in cases {
            let decoded = update_queue
                .dequeue_update(Duration::from_secs(1))
                .await?
                .ok_or_else(|| {
                    io::Error::other(format!("expected {expected_producer_name} update"))
                })?;
            assert_eq!(producer_update_name(&decoded), expected_producer_name);

            let report =
                process_update_with_state_store_at(decoded, config, now, &state_store, |update| {
                    skipped.handle_update(update)
                })
                .await;
            assert_eq!(
                report.update_id, expected_update_id,
                "{expected_producer_name}"
            );
            assert_eq!(report.update_name, update_name, "{expected_producer_name}");
            assert_eq!(
                report.state.outcome,
                UpdateStageOutcome::Completed,
                "{expected_producer_name}"
            );
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed),
                "{expected_producer_name}"
            );
            assert!(!report.skipped_handle, "{expected_producer_name}");
            assert_eq!(terminal.handled_count(), 0, "{expected_producer_name}");
        }
        let state_calls = state_store.calls();
        assert!(state_calls.contains(&"chat:-10042:channel::".to_owned()));
        assert!(state_calls.contains(&"chat:-10043:supergroup::".to_owned()));
        assert!(state_calls.contains(&"chat:-10042:supergroup::".to_owned()));
        assert!(state_calls.contains(&"chat:42:private:Ada:".to_owned()));
        assert!(state_calls.contains(&"user:111:Ada:".to_owned()));
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.starts_with("file:"))
                .count(),
            0
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[derive(Clone, Debug, Default)]
    struct UpdateHandlerStub {
        handled: Arc<Mutex<usize>>,
    }

    impl UpdateHandlerStub {
        fn handled_count(&self) -> usize {
            *self.handled.lock().expect("handled")
        }
    }

    impl UpdateHandler for UpdateHandlerStub {
        type Error = io::Error;

        fn handle_update<'a>(
            &'a self,
            _update: TelegramUpdate,
        ) -> UpdateHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                *self.handled.lock().expect("handled") += 1;
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug, Default)]
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
            user: &'a openplotva_core::UserState,
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

    fn chosen_inline_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 201,
            "chosen_inline_result": {
                "result_id": "result-1",
                "from": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada"
                },
                "query": "hello"
            }
        }))
    }

    fn go_skipped_update_cases() -> Result<Vec<(&'static str, TelegramUpdate)>, serde_json::Error> {
        let mut cases = vec![
            ("channel_post", channel_post_update()?),
            ("edited_channel_post", edited_channel_post_update()?),
            ("chosen_inline_result", chosen_inline_update()?),
            ("shipping_query", shipping_query_update()?),
            ("poll", poll_update()?),
            ("poll_answer", poll_answer_update()?),
            ("my_chat_member", chat_member_update("my_chat_member")?),
            ("chat_member", chat_member_update("chat_member")?),
            ("chat_join_request", chat_join_request_update()?),
            ("message_reaction", message_reaction_update()?),
            ("message_reaction_count", message_reaction_count_update()?),
            ("unknown", unknown_update()?),
        ];
        cases.extend(
            newer_go_unknown_update_cases()?
                .into_iter()
                .map(|(_, update)| ("unknown", update)),
        );
        Ok(cases)
    }

    fn live_redis_skipped_update_cases()
    -> Result<Vec<(&'static str, i64, &'static str, TelegramUpdate)>, serde_json::Error> {
        let mut cases = vec![
            ("channel_post", 204, "channel_post", channel_post_update()?),
            (
                "edited_channel_post",
                205,
                "edited_channel_post",
                edited_channel_post_update()?,
            ),
            (
                "chosen_inline_result",
                201,
                "chosen_inline_result",
                chosen_inline_update()?,
            ),
            (
                "shipping_query",
                206,
                "shipping_query",
                shipping_query_update()?,
            ),
            ("poll", 207, "poll", poll_update()?),
            ("poll_answer", 208, "poll_answer", poll_answer_update()?),
            (
                "poll_answer",
                213,
                "poll_answer",
                poll_answer_voter_chat_update()?,
            ),
            (
                "my_chat_member",
                212,
                "my_chat_member",
                chat_member_update("my_chat_member")?,
            ),
            (
                "chat_member",
                212,
                "chat_member",
                chat_member_update("chat_member")?,
            ),
            (
                "chat_join_request",
                202,
                "chat_join_request",
                chat_join_request_update()?,
            ),
            (
                "message_reaction",
                209,
                "message_reaction",
                message_reaction_update()?,
            ),
            (
                "message_reaction_count",
                210,
                "message_reaction_count",
                message_reaction_count_update()?,
            ),
            ("unknown", 211, "unknown", unknown_update()?),
        ];
        cases.extend(
            newer_go_unknown_update_cases()?
                .into_iter()
                .map(|(update_id, update)| ("unknown", update_id, "unknown", update)),
        );
        Ok(cases)
    }

    fn newer_go_unknown_update_cases() -> Result<Vec<(i64, TelegramUpdate)>, serde_json::Error> {
        Ok(vec![
            (214, business_connection_update()?),
            (215, business_message_update("business_message", 215)?),
            (
                216,
                business_message_update("edited_business_message", 216)?,
            ),
            (217, deleted_business_messages_update()?),
            (218, chat_boost_update()?),
            (219, removed_chat_boost_update()?),
            (220, managed_bot_update()?),
            (221, purchased_paid_media_update()?),
        ])
    }

    fn channel_post_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 204,
            "channel_post": {
                "message_id": 2,
                "date": 1_710_000_000,
                "chat": {"id": -10042, "type": "channel", "title": "News"},
                "text": "post"
            }
        }))
    }

    fn edited_channel_post_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 205,
            "edited_channel_post": {
                "message_id": 2,
                "date": 1_710_000_000,
                "chat": {"id": -10042, "type": "channel", "title": "News"},
                "edit_date": 1_710_000_001,
                "text": "post edited"
            }
        }))
    }

    fn shipping_query_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 206,
            "shipping_query": {
                "id": "ship-1",
                "from": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada"
                },
                "invoice_payload": "payload",
                "shipping_address": {
                    "country_code": "RU",
                    "state": "Chechen Republic",
                    "city": "Gudermes",
                    "street_line1": "Nuradilov st., 12",
                    "street_line2": "",
                    "post_code": "366200"
                }
            }
        }))
    }

    fn chat_member_update(field: &str) -> Result<TelegramUpdate, serde_json::Error> {
        let mut update = json!({"update_id": 212});
        update[field] = json!({
            "chat": {"id": -10042, "type": "supergroup", "title": "Lab"},
            "from": {"id": 111, "is_bot": false, "first_name": "Ada"},
            "date": 1_710_000_000,
            "old_chat_member": {
                "status": "left",
                "user": {"id": 222, "is_bot": false, "first_name": "User"}
            },
            "new_chat_member": {
                "status": "member",
                "user": {"id": 222, "is_bot": false, "first_name": "User"}
            }
        });
        serde_json::from_value(update)
    }

    fn poll_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 207,
            "poll": {
                "id": "poll-1",
                "question": "Rust?",
                "options": [
                    {"persistent_id": "1", "text": "Yes", "voter_count": 1},
                    {"persistent_id": "2", "text": "No", "voter_count": 0}
                ],
                "total_voter_count": 1,
                "is_closed": false,
                "is_anonymous": true,
                "type": "regular",
                "allows_multiple_answers": false,
                "allows_revoting": false,
                "members_only": false
            }
        }))
    }

    fn poll_answer_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 208,
            "poll_answer": {
                "poll_id": "poll-1",
                "user": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada"
                },
                "option_ids": [0],
                "option_persistent_ids": []
            }
        }))
    }

    fn poll_answer_voter_chat_update() -> Result<TelegramUpdate, serde_json::Error> {
        openplotva_updates::decode_telegram_update_value(json!({
            "update_id": 213,
            "poll_answer": {
                "poll_id": "poll-2",
                "voter_chat": {
                    "id": -10043,
                    "type": "supergroup",
                    "title": "Poll Team"
                },
                "option_ids": [1],
                "option_persistent_ids": []
            }
        }))
    }

    fn chat_join_request_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 202,
            "chat_join_request": {
                "chat": {"id": -10042, "type": "supergroup", "title": "Team"},
                "from": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada"
                },
                "user_chat_id": 111,
                "date": 1_710_000_000
            }
        }))
    }

    fn message_reaction_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 209,
            "message_reaction": {
                "chat": {"id": 42, "type": "private", "first_name": "Ada"},
                "message_id": 1,
                "date": 1_710_000_000,
                "old_reaction": [{"type": "emoji", "emoji": "\u{1f44e}"}],
                "new_reaction": [{"type": "emoji", "emoji": "\u{1f44d}"}]
            }
        }))
    }

    fn message_reaction_count_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 210,
            "message_reaction_count": {
                "chat": {"id": 42, "type": "private", "first_name": "Ada"},
                "message_id": 1,
                "date": 1_710_000_000,
                "reactions": [{
                    "type": {"type": "emoji", "emoji": "\u{1f44d}"},
                    "total_count": 1
                }]
            }
        }))
    }

    fn unknown_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 211,
            "future_update_shape": {"value": true}
        }))
    }

    fn business_connection_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 214,
            "business_connection": {
                "id": "business-1",
                "user": user_json(114, "Business", false),
                "user_chat_id": 114,
                "date": 1_710_000_000,
                "is_enabled": true
            }
        }))
    }

    fn business_message_update(
        field: &str,
        update_id: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let mut update = json!({"update_id": update_id});
        update[field] = json!({
            "message_id": 1,
            "date": 1_710_000_000,
            "business_connection_id": "business-1",
            "chat": private_chat_json(115, "BusinessChat"),
            "from": user_json(115, "BusinessChat", false),
            "text": "business text"
        });
        serde_json::from_value(update)
    }

    fn deleted_business_messages_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 217,
            "deleted_business_messages": {
                "business_connection_id": "business-1",
                "chat": private_chat_json(115, "BusinessChat"),
                "message_ids": [1, 2]
            }
        }))
    }

    fn chat_boost_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 218,
            "chat_boost": {
                "chat": supergroup_chat_json(-10045, "Boost Team"),
                "boost": {
                    "boost_id": "boost-1",
                    "add_date": 1_710_000_000,
                    "expiration_date": 1_710_086_400,
                    "source": boost_source_json()
                }
            }
        }))
    }

    fn removed_chat_boost_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 219,
            "removed_chat_boost": {
                "boost_id": "boost-1",
                "chat": supergroup_chat_json(-10045, "Boost Team"),
                "remove_date": 1_710_086_400,
                "source": boost_source_json()
            }
        }))
    }

    fn managed_bot_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 220,
            "managed_bot": {
                "bot": user_json(900, "ManagedBot", true),
                "user": user_json(116, "Owner", false)
            }
        }))
    }

    fn purchased_paid_media_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 221,
            "purchased_paid_media": {
                "from": user_json(117, "Buyer", false),
                "paid_media_payload": "paid-media-payload"
            }
        }))
    }

    fn boost_source_json() -> serde_json::Value {
        json!({
            "source": "gift_code",
            "user": user_json(118, "Booster", false)
        })
    }

    fn private_chat_json(id: i64, first_name: &str) -> serde_json::Value {
        json!({
            "id": id,
            "type": "private",
            "first_name": first_name
        })
    }

    fn supergroup_chat_json(id: i64, title: &str) -> serde_json::Value {
        json!({
            "id": id,
            "type": "supergroup",
            "title": title
        })
    }

    fn user_json(id: i64, first_name: &str, is_bot: bool) -> serde_json::Value {
        json!({
            "id": id,
            "is_bot": is_bot,
            "first_name": first_name
        })
    }

    fn text_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 203,
            "message": {
                "message_id": 1,
                "date": 1_710_000_000,
                "chat": {"id": 42, "type": "private", "first_name": "Ada"},
                "from": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada"
                },
                "text": "hello"
            }
        }))
    }
}
