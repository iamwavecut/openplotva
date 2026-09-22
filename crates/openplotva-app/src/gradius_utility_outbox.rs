//! Durable Telegram delivery for Gradius utility placements.

use std::sync::Arc;

use carapax::types::LinkPreviewOptions;
use openplotva_storage::gradius_ads::PostgresGradiusAdStore;
use openplotva_storage::{
    PostgresTelegramOutboxStore, TelegramDeliveryPolicy, TelegramOutboxBatchInput,
    TelegramOutboxPartInput,
};
use openplotva_telegram::{
    ChatRef, EditTextMessageRequest, OutboundCommand, ReplyMessageRef, TELEGRAM_PARSE_MODE_HTML,
    TELEGRAM_TEXT_MAX_BYTES, TelegramOutboundMethod, TextMessageRequest,
    build_edit_text_message_method, build_text_message_method_without_link_preview,
    is_valid_telegram_html,
};
use time::OffsetDateTime;

use crate::gradius_ads::{GradiusUtilityAdRequest, GradiusUtilityAdService, GradiusUtilitySurface};

#[derive(Clone)]
pub struct GradiusUtilityImageAds {
    pub ads: Arc<GradiusUtilityAdService>,
    pub outbox: Arc<GradiusUtilityOutbox>,
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
        let source_id = job_id.to_string();
        let Some(ad) = self
            .ads
            .prepare(GradiusUtilityAdRequest {
                surface: GradiusUtilitySurface::Image,
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
                thread_id,
                i64::from(first_photo_id),
                ad.html,
            )
            .await?;
        Ok(())
    }
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
        thread_id: Option<i32>,
        reply_to_message_id: i64,
        html: String,
    ) -> Result<String, String> {
        validate_final_html(ads, opportunity_id, &html).await?;
        let chat = ChatRef {
            id: chat_id,
            is_forum: thread_id.is_some(),
        };
        let request = TextMessageRequest {
            chat: Some(chat),
            message_thread_id: i64::from(thread_id.unwrap_or_default()),
            disable_notification: false,
            allow_sending_without_reply: None,
            text: html.clone(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_markup: None,
        };
        let reply = ReplyMessageRef {
            message_id: reply_to_message_id,
            chat,
            is_topic_message: thread_id.is_some(),
            message_thread_id: i64::from(thread_id.unwrap_or_default()),
        };
        let method = build_text_message_method_without_link_preview(
            &request,
            chat,
            Some(&reply),
            html,
            true,
        )
        .map_err(|error| error.to_string());
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
    pub async fn queue_final_edit(
        &self,
        ads: &GradiusUtilityAdService,
        source_key: &str,
        opportunity_id: i64,
        chat_id: i64,
        thread_id: Option<i32>,
        message_id: i64,
        html: String,
    ) -> Result<String, String> {
        validate_final_html(ads, opportunity_id, &html).await?;
        let request = EditTextMessageRequest {
            chat: ChatRef {
                id: chat_id,
                is_forum: thread_id.is_some(),
            },
            message_id,
            text: html,
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_markup: None,
        };
        let method = match build_edit_text_message_method(&request) {
            Ok(method) => method,
            Err(error) => {
                let _ = ads
                    .mark_delivery_failed(opportunity_id, "outbox_build_failed")
                    .await;
                return Err(error.to_string());
            }
        }
        .with_link_preview_options(LinkPreviewOptions::default().with_is_disabled(true));
        self.queue(
            ads,
            source_key,
            opportunity_id,
            chat_id,
            thread_id,
            message_id,
            TelegramDeliveryPolicy::TargetIdempotent,
            TelegramOutboundMethod::from(method),
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
    ) -> Result<String, String> {
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
        if let Err(error) = self.store.enqueue_utility_ad(opportunity_id, &batch).await {
            let _ = ads
                .mark_delivery_failed(opportunity_id, "outbox_enqueue_failed")
                .await;
            return Err(error.to_string());
        }
        Ok(batch_id)
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

fn final_html_fits_telegram(html: &str) -> bool {
    html.len() <= TELEGRAM_TEXT_MAX_BYTES && is_valid_telegram_html(html)
}

#[cfg(test)]
mod tests {
    use super::final_html_fits_telegram;
    use openplotva_telegram::TELEGRAM_TEXT_MAX_BYTES;

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
