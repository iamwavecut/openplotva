//! Durable Telegram delivery for Gradius utility placements.

use std::sync::Arc;

use openplotva_storage::gradius_ads::{GradiusUtilityAdEnqueue, PostgresGradiusAdStore};
use openplotva_storage::{
    PostgresTelegramOutboxStore, TelegramDeliveryPolicy, TelegramOutboxBatchInput,
    TelegramOutboxPartInput,
};
use openplotva_telegram::{
    ChatRef, EditRichMessage, OutboundCommand, ReplyMessageRef, RichSendOptions, SendRichMessage,
    TELEGRAM_PARSE_MODE_HTML, TELEGRAM_TEXT_MAX_BYTES, TelegramOutboundMethod, TextMessageRequest,
    build_text_message_method_without_link_preview, format_rich_html, is_valid_telegram_html,
    rich_message_within_char_limit,
};
use time::OffsetDateTime;

use crate::gradius_ads::{GradiusUtilityAdRequest, GradiusUtilityAdService, GradiusUtilitySurface};

#[derive(Clone)]
pub struct GradiusUtilityImageAds {
    pub ads: Arc<GradiusUtilityAdService>,
    pub outbox: Arc<GradiusUtilityOutbox>,
    pub telegram: openplotva_telegram::TelegramClient,
    pub bot_id: i64,
}

impl std::fmt::Debug for GradiusUtilityImageAds {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GradiusUtilityImageAds")
            .finish_non_exhaustive()
    }
}

impl GradiusUtilityImageAds {
    #[allow(clippy::too_many_arguments)]
    pub async fn offer(
        &self,
        job_id: i64,
        attempt_key: String,
        chat_id: i64,
        user_id: i64,
        thread_id: Option<i32>,
        prompt: String,
        first_photo_id: i32,
    ) -> Result<(), String> {
        let delivery = image_ad_delivery(&self.telegram, self.bot_id, chat_id).await?;
        if delivery == ImageAdDelivery::Persistent
            && chat_id < 0
            && !self
                .outbox
                .store
                .public_image_group_available(chat_id)
                .await
                .map_err(|error| error.to_string())?
        {
            return Ok(());
        }
        let source_id = job_id.to_string();
        let Some(ad) = self
            .ads
            .prepare(GradiusUtilityAdRequest {
                surface: GradiusUtilitySurface::Image,
                include_vip_appendix: chat_id > 0 || delivery == ImageAdDelivery::Ephemeral,
                source_id: source_id.clone(),
                attempt_key,
                user_id,
                chat_id,
                thread_id,
                prompt: Some(prompt),
                result_context: Some("Image generation completed".to_owned()),
                completed_at: OffsetDateTime::now_utc(),
            })
            .await?
        else {
            return Ok(());
        };
        self.outbox
            .queue_message(
                &self.ads,
                &format!("image-job:{source_id}"),
                ad.opportunity_id,
                chat_id,
                user_id,
                delivery,
                thread_id,
                i64::from(first_photo_id),
                ad.html,
            )
            .await?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageAdDelivery {
    Persistent,
    Ephemeral,
}

async fn image_ad_delivery<Api: crate::settings::GroupSettingsMemberApi>(
    api: &Api,
    bot_id: i64,
    chat_id: i64,
) -> Result<ImageAdDelivery, String> {
    if chat_id > 0 {
        return Ok(ImageAdDelivery::Persistent);
    }
    let member = api
        .get_chat_member(chat_id, bot_id)
        .await
        .map_err(|_| "Failed to resolve bot membership for image advertising".to_owned())?;
    if member.get_user().id != bot_id || !member.get_user().is_bot || !member.is_member() {
        return Err("Image advertising requires active bot membership".to_owned());
    }
    Ok(match member {
        carapax::types::ChatMember::Administrator(_) | carapax::types::ChatMember::Creator(_) => {
            ImageAdDelivery::Ephemeral
        }
        _ => ImageAdDelivery::Persistent,
    })
}

#[derive(Clone, Debug)]
pub struct GradiusUtilityOutbox {
    store: PostgresGradiusAdStore,
    bot_id: i64,
}

impl GradiusUtilityOutbox {
    #[must_use]
    pub fn new(store: PostgresTelegramOutboxStore, bot_id: i64) -> Self {
        Self {
            store: PostgresGradiusAdStore::new(store.pool().clone()),
            bot_id,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn queue_message(
        &self,
        ads: &GradiusUtilityAdService,
        source_key: &str,
        opportunity_id: i64,
        chat_id: i64,
        user_id: i64,
        delivery: ImageAdDelivery,
        thread_id: Option<i32>,
        reply_to_message_id: i64,
        html: String,
    ) -> Result<Option<String>, String> {
        validate_final_html(ads, opportunity_id, &html).await?;
        let method = image_ad_message(
            chat_id,
            user_id,
            delivery,
            thread_id,
            reply_to_message_id,
            &html,
        );
        let method = match method {
            Ok(method) => method,
            Err(error) => {
                let _ = ads
                    .mark_delivery_failed(opportunity_id, "outbox_build_failed")
                    .await;
                return Err(error);
            }
        };
        self.queue(
            ads,
            source_key,
            opportunity_id,
            chat_id,
            thread_id,
            reply_to_message_id,
            TelegramDeliveryPolicy::Create,
            TelegramOutboundMethod::from(method),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn queue_rich_message(
        &self,
        ads: &GradiusUtilityAdService,
        source_key: &str,
        opportunity_id: i64,
        chat_id: i64,
        thread_id: Option<i32>,
        reply_to_message_id: i64,
        html: String,
    ) -> Result<Option<String>, String> {
        let method = utility_rich_send(chat_id, thread_id, reply_to_message_id, &html);
        let method = match method {
            Ok(method) => method,
            Err(error) => {
                let _ = ads
                    .mark_delivery_failed(opportunity_id, "final_rich_html_invalid_or_too_long")
                    .await;
                return Err(error);
            }
        };
        self.queue(
            ads,
            source_key,
            opportunity_id,
            chat_id,
            thread_id,
            reply_to_message_id,
            TelegramDeliveryPolicy::Create,
            method,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn queue_final_edit(
        &self,
        ads: &GradiusUtilityAdService,
        source_key: &str,
        opportunity_id: i64,
        chat_id: i64,
        thread_id: Option<i32>,
        message_id: i64,
        html: String,
    ) -> Result<Option<String>, String> {
        let method = match utility_rich_edit(chat_id, message_id, &html) {
            Ok(method) => method,
            Err(error) => {
                let _ = ads
                    .mark_delivery_failed(opportunity_id, "final_rich_html_invalid_or_too_long")
                    .await;
                return Err(error);
            }
        };
        self.queue(
            ads,
            source_key,
            opportunity_id,
            chat_id,
            thread_id,
            message_id,
            TelegramDeliveryPolicy::TargetIdempotent,
            method,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn queue(
        &self,
        ads: &GradiusUtilityAdService,
        source_key: &str,
        opportunity_id: i64,
        chat_id: i64,
        thread_id: Option<i32>,
        trigger_message_id: i64,
        policy: TelegramDeliveryPolicy,
        method: TelegramOutboundMethod,
    ) -> Result<Option<String>, String> {
        let command = match OutboundCommand::try_from_method(method) {
            Ok(command) => command,
            Err(error) => {
                let _ = ads
                    .mark_delivery_failed(opportunity_id, "outbox_build_failed")
                    .await;
                return Err(error.to_string());
            }
        };
        let (method_kind, payload_version, payload) = match command.into_storage_parts() {
            Ok(parts) => parts,
            Err(error) => {
                let _ = ads
                    .mark_delivery_failed(opportunity_id, "outbox_build_failed")
                    .await;
                return Err(error.to_string());
            }
        };
        let batch_id = format!("gradius-utility:v1:{}:{source_key}", self.bot_id);
        let now = OffsetDateTime::now_utc();
        let batch = TelegramOutboxBatchInput {
            batch_id: batch_id.clone(),
            bot_id: self.bot_id,
            chat_id: Some(chat_id),
            thread_id,
            ordering_key: format!(
                "gradius-utility:{}:{chat_id}:{}",
                self.bot_id,
                thread_id.unwrap_or_default()
            ),
            causation_update_id: None,
            dialog_job_id: None,
            trigger_message_id: Some(trigger_message_id),
            delivery_policy: policy,
            protected: true,
            priority: 0,
            parts: vec![TelegramOutboxPartInput {
                method_kind: method_kind.to_owned(),
                payload_version,
                payload,
                blob: None,
                available_at: now,
                expires_at: None,
            }],
        };
        match self.store.enqueue_utility_ad(opportunity_id, &batch).await {
            Ok(GradiusUtilityAdEnqueue::Queued) => Ok(Some(batch_id)),
            Ok(GradiusUtilityAdEnqueue::PublicGroupRateLimited) => {
                tracing::info!(
                    opportunity_id,
                    chat_id,
                    "public image ad skipped by group limit"
                );
                Ok(None)
            }
            Err(error) => {
                let _ = ads
                    .mark_delivery_failed(opportunity_id, "outbox_enqueue_failed")
                    .await;
                Err(error.to_string())
            }
        }
    }
}

async fn validate_final_html(
    ads: &GradiusUtilityAdService,
    opportunity_id: i64,
    html: &str,
) -> Result<(), String> {
    if final_html_fits_telegram(html) {
        return Ok(());
    }
    ads.mark_delivery_failed(opportunity_id, "final_html_invalid_or_too_long")
        .await?;
    Err("Gradius utility message exceeds Telegram HTML limits".to_owned())
}

pub(crate) fn image_ad_message(
    chat_id: i64,
    user_id: i64,
    delivery: ImageAdDelivery,
    thread_id: Option<i32>,
    reply_to_message_id: i64,
    html: &str,
) -> Result<carapax::types::SendMessage, String> {
    if user_id <= 0 || chat_id == 0 || (chat_id > 0 && chat_id != user_id) {
        return Err("Image advertising requires an identified initiator".to_owned());
    }
    if delivery == ImageAdDelivery::Ephemeral && chat_id > 0 {
        return Err("Ephemeral image advertising requires a group chat".to_owned());
    }
    let chat = ChatRef {
        id: chat_id,
        is_forum: thread_id.is_some(),
    };
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: i64::from(thread_id.unwrap_or_default()),
        disable_notification: false,
        allow_sending_without_reply: None,
        text: html.to_owned(),
        render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
        reply_markup: None,
    };
    let reply = ReplyMessageRef {
        message_id: reply_to_message_id,
        chat,
        is_topic_message: thread_id.is_some(),
        message_thread_id: i64::from(thread_id.unwrap_or_default()),
    };
    let method =
        build_text_message_method_without_link_preview(&request, chat, Some(&reply), html, true)
            .map_err(|error| error.to_string())?;
    Ok(if delivery == ImageAdDelivery::Ephemeral {
        method
            .with_reply_parameters(
                carapax::types::ReplyParameters::new(reply_to_message_id)
                    .with_allow_sending_without_reply(false),
            )
            .with_ephemeral_message_parameters(user_id)
    } else {
        method
    })
}

fn final_html_fits_telegram(html: &str) -> bool {
    html.len() <= TELEGRAM_TEXT_MAX_BYTES && is_valid_telegram_html(html)
}

pub(crate) fn compose_rich_utility_html(content: &str, ad: &str) -> String {
    format_rich_html(&format!("{content}<hr/>{ad}"))
}

fn utility_rich_html(html: &str) -> Result<String, String> {
    let html = format_rich_html(html);
    if html.is_empty() || !rich_message_within_char_limit(&html) {
        return Err("Gradius utility message exceeds Telegram Rich HTML limits".to_owned());
    }
    Ok(html)
}

fn utility_rich_send(
    chat_id: i64,
    thread_id: Option<i32>,
    reply_to: i64,
    html: &str,
) -> Result<TelegramOutboundMethod, String> {
    Ok(SendRichMessage {
        chat_id,
        html: utility_rich_html(html)?,
        options: RichSendOptions {
            message_thread_id: thread_id.map(i64::from),
            reply_to_message_id: Some(reply_to),
            ..Default::default()
        },
    }
    .into())
}

fn utility_rich_edit(
    chat_id: i64,
    message_id: i64,
    html: &str,
) -> Result<TelegramOutboundMethod, String> {
    Ok(EditRichMessage {
        chat_id,
        message_id,
        html: utility_rich_html(html)?,
        reply_markup: None,
    }
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_ads_preserve_the_selected_visibility_on_replay() {
        for (chat, user, delivery, thread) in [
            (-100, 42, ImageAdDelivery::Ephemeral, Some(12)),
            (-100, 42, ImageAdDelivery::Persistent, Some(12)),
            (42, 42, ImageAdDelivery::Persistent, None),
        ] {
            let method = image_ad_message(chat, user, delivery, thread, 77, "📢 <b>Offer</b>")
                .expect("image ad");
            let command = OutboundCommand::try_from_method(method.into()).expect("persistable ad");
            let (kind, version, payload) = command.into_storage_parts().expect("payload");
            let replay = OutboundCommand::decode(
                version,
                kind,
                &serde_json::to_vec(&payload).expect("JSON"),
            )
            .expect("replay");
            let TelegramOutboundMethod::SendMessage(replayed) = replay.into_method() else {
                panic!("text ad");
            };
            let replayed = serde_json::to_value(replayed).expect("request");
            assert_eq!(replayed["chat_id"], chat);
            assert_eq!(replayed["reply_parameters"]["message_id"], 77);
            assert_eq!(replayed["parse_mode"], "HTML");
            assert_eq!(replayed["link_preview_options"]["is_disabled"], true);
            if let Some(thread) = thread {
                assert_eq!(replayed["message_thread_id"], thread);
            }
            if delivery == ImageAdDelivery::Ephemeral {
                assert_eq!(
                    replayed["ephemeral_message_parameters"]["receiver_user_id"],
                    user
                );
                assert_eq!(
                    replayed["reply_parameters"]["allow_sending_without_reply"],
                    false
                );
                assert!(replayed["reply_parameters"].get("chat_id").is_none());
            } else {
                assert!(replayed.get("ephemeral_message_parameters").is_none());
            }
        }
        for (chat, user, delivery) in [
            (-100, 0, ImageAdDelivery::Ephemeral),
            (-100, 0, ImageAdDelivery::Persistent),
            (42, 43, ImageAdDelivery::Persistent),
            (42, 42, ImageAdDelivery::Ephemeral),
        ] {
            assert!(image_ad_message(chat, user, delivery, None, 77, "Offer").is_err());
        }
    }

    struct MemberApi {
        result: Result<carapax::types::ChatMember, &'static str>,
        calls: std::sync::Mutex<Vec<(i64, i64)>>,
    }

    impl crate::settings::GroupSettingsMemberApi for MemberApi {
        type Error = &'static str;

        fn get_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> crate::settings::GroupSettingsMemberFuture<'a, carapax::types::ChatMember, Self::Error>
        {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("member calls")
                    .push((chat_id, user_id));
                self.result.clone()
            })
        }
    }

    fn member_api(status: &str, user_id: i64, is_bot: bool) -> MemberApi {
        let member = serde_json::from_value(serde_json::json!({
            "status":status,"user":{"id":user_id,"is_bot":is_bot,"first_name":"Bot"},
            "is_anonymous":false,"can_be_edited":false,"can_change_info":false,
            "can_delete_messages":false,"can_invite_users":false,"can_manage_chat":true,
            "can_manage_video_chats":false,"can_promote_members":false,
            "can_restrict_members":false,"can_send_welcome_messages":false
        }))
        .expect("member fixture");
        MemberApi {
            result: Ok(member),
            calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    #[tokio::test]
    async fn image_ad_visibility_uses_current_bot_membership_and_private_needs_no_lookup() {
        for (status, expected) in [
            ("administrator", ImageAdDelivery::Ephemeral),
            ("creator", ImageAdDelivery::Ephemeral),
            ("member", ImageAdDelivery::Persistent),
        ] {
            let api = member_api(status, 7, true);
            assert_eq!(image_ad_delivery(&api, 7, -100).await, Ok(expected));
            assert_eq!(*api.calls.lock().expect("member calls"), vec![(-100, 7)]);
        }
        let api = MemberApi {
            result: Err("unavailable"),
            calls: std::sync::Mutex::new(Vec::new()),
        };
        assert_eq!(
            image_ad_delivery(&api, 7, 42).await,
            Ok(ImageAdDelivery::Persistent)
        );
        assert!(api.calls.lock().expect("member calls").is_empty());
    }

    #[tokio::test]
    async fn image_ad_visibility_does_not_publish_on_unknown_or_invalid_bot_membership() {
        for api in [
            MemberApi {
                result: Err("unavailable"),
                calls: std::sync::Mutex::new(Vec::new()),
            },
            member_api("administrator", 42, true),
            member_api("administrator", 7, false),
            member_api("left", 7, true),
        ] {
            assert!(image_ad_delivery(&api, 7, -100).await.is_err());
        }
        let mut api = member_api("administrator", 7, true);
        assert_eq!(
            image_ad_delivery(&api, 7, -100).await,
            Ok(ImageAdDelivery::Ephemeral)
        );
        api.result = member_api("member", 7, true).result;
        assert_eq!(
            image_ad_delivery(&api, 7, -100).await,
            Ok(ImageAdDelivery::Persistent)
        );
        assert_eq!(api.calls.lock().expect("member calls").len(), 2);
    }

    #[test]
    fn rich_utility_send_and_edit_preserve_content_and_ad_separator_on_replay() {
        for content in [
            "<table><tr><td>USD</td><td>90 ₽</td></tr></table>",
            "<h2>Winner</h2><p>Game completed</p>",
        ] {
            let html = compose_rich_utility_html(
                content,
                "📢 <a href=\"https://example.com\">Offer</a><tg-spoiler>VIP</tg-spoiler>",
            );
            assert!(html.starts_with(format_rich_html(content).trim()));
            assert!(html.contains("<hr/>"));
            assert!(html.find("<hr/>").expect("separator") < html.find("📢").expect("ad"));
            for method in [
                utility_rich_send(42, Some(7), 9, &html).expect("rich send"),
                utility_rich_edit(42, 9, &html).expect("rich edit"),
            ] {
                let (kind, version, payload) = OutboundCommand::try_from_method(method)
                    .expect("command")
                    .into_storage_parts()
                    .expect("stored command");
                let replay = OutboundCommand::decode(
                    version,
                    kind,
                    &serde_json::to_vec(&payload).expect("encoded payload"),
                )
                .expect("replayed command");
                let (_, _, replayed) = replay.into_storage_parts().expect("replayed payload");
                assert_eq!(replayed["html"], html);
                assert!(replayed.get("text").is_none());
                assert!(replayed.get("link_preview_options").is_none());
                if kind == "sendRichMessage" {
                    assert_eq!(replayed["options"]["reply_to_message_id"], 9);
                    assert_eq!(replayed["options"]["message_thread_id"], 7);
                } else {
                    assert_eq!(kind, "editMessageText");
                    assert_eq!(replayed["message_id"], 9);
                }
            }
        }
    }

    #[test]
    fn rich_utility_uses_rich_limit_without_plain_html_downgrade() {
        let html = compose_rich_utility_html(&"x".repeat(5000), "📢 Offer");
        assert!(utility_rich_send(42, None, 9, &html).is_ok());
        assert!(utility_rich_edit(42, 9, &html).is_ok());
        let oversized = "x".repeat(32769);
        assert!(utility_rich_send(42, None, 9, &oversized).is_err());
        assert!(utility_rich_edit(42, 9, &oversized).is_err());
    }

    #[test]
    fn final_utility_message_must_fit_with_result_and_ad_together() {
        assert!(final_html_fits_telegram(
            "Курс: 90 ₽\n\n📢 <b>Предложение</b>"
        ));
        assert!(!final_html_fits_telegram(&format!(
            "{}📢 объявление",
            "x".repeat(TELEGRAM_TEXT_MAX_BYTES)
        )));
        assert!(!final_html_fits_telegram(
            "📢 <a href=\"javascript:bad\">X</a>"
        ));
    }
}
