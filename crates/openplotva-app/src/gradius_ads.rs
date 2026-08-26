use std::{future::Future, pin::Pin, sync::Arc};

use openplotva_llm::gradius::vip_hint_for_impression;
use openplotva_llm::gradius::{
    GradiusApiExchange, GradiusClient, GradiusDialogueRole, GradiusDialogueTurn,
    GradiusIntegrationKind, GradiusPlacement, GradiusPrivacyRedactor, GradiusSyntheticIds,
    ReqwestGradiusTransport,
};
use openplotva_storage::gradius_ads::{
    GradiusAdOpportunityInput, GradiusAdReservation, GradiusApiCallRecord, GradiusStoredAd,
    PostgresGradiusAdStore,
};
use openplotva_telegram::{
    TELEGRAM_TEXT_MAX_BYTES, escape_telegram_html_text, is_valid_telegram_html,
    telegram_html_from_markdown,
};
use serde_json::json;
use time::OffsetDateTime;

const VIP_URL: &str = "https://t.me/PlotvoBot?start=vip";
const LINKED_VIP: &str = "<a href=\"https://t.me/PlotvoBot?start=vip\">VIP</a>";
const GRADIUS_AD_LABEL: &str = "📢 ";

pub type GradiusServiceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

pub struct GradiusDialogueCall {
    pub result: Result<Option<GradiusPlacement>, String>,
    pub exchange: Option<GradiusApiExchange>,
}

pub trait GradiusDialogueClient: Send + Sync {
    fn dialogue<'a>(
        &'a self,
        turn: GradiusDialogueTurn,
    ) -> Pin<Box<dyn Future<Output = GradiusDialogueCall> + Send + 'a>>;
}

impl GradiusDialogueClient for GradiusClient<ReqwestGradiusTransport> {
    fn dialogue<'a>(
        &'a self,
        turn: GradiusDialogueTurn,
    ) -> Pin<Box<dyn Future<Output = GradiusDialogueCall> + Send + 'a>> {
        Box::pin(async move {
            match GradiusClient::dialogue(self, turn).await {
                Ok(result) => GradiusDialogueCall {
                    result: Ok(result.placement),
                    exchange: Some(result.exchange),
                },
                Err(failure) => GradiusDialogueCall {
                    result: Err(failure.to_string()),
                    exchange: failure.exchange.map(|exchange| *exchange),
                },
            }
        })
    }
}

pub trait GradiusTextRedactor: Send + Sync {
    fn redact<'a>(&'a self, text: &'a str) -> GradiusServiceFuture<'a, String>;
}

impl GradiusTextRedactor for GradiusPrivacyRedactor {
    fn redact<'a>(&'a self, text: &'a str) -> GradiusServiceFuture<'a, String> {
        Box::pin(async move {
            self.redact_text(text)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

pub trait GradiusAdLedger: Send + Sync {
    fn reserve<'a>(
        &'a self,
        input: GradiusAdOpportunityInput,
    ) -> GradiusServiceFuture<'a, GradiusAdReservation>;

    fn record_api_call<'a>(&'a self, call: GradiusApiCallRecord) -> GradiusServiceFuture<'a, ()>;

    fn finish_ad<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
        ad: GradiusStoredAd,
    ) -> GradiusServiceFuture<'a, GradiusStoredAd>;

    fn finish_no_ad<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
    ) -> GradiusServiceFuture<'a, ()>;

    fn finish_provider_error<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
    ) -> GradiusServiceFuture<'a, ()>;

    fn finish_privacy_error<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
    ) -> GradiusServiceFuture<'a, ()>;

    fn finish_unsupported_surface<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
    ) -> GradiusServiceFuture<'a, ()>;

    fn finish_render_error<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()>;

    fn mark_render_error<'a>(
        &'a self,
        opportunity_id: i64,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()>;

    fn mark_queued<'a>(
        &'a self,
        opportunity_id: i64,
        batch_id: &'a str,
    ) -> GradiusServiceFuture<'a, ()>;

    fn reconcile_delivered<'a>(
        &'a self,
        opportunity_id: i64,
        batch_id: &'a str,
    ) -> GradiusServiceFuture<'a, ()>;

    fn mark_delivery_failed_by_batch<'a>(
        &'a self,
        batch_id: &'a str,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()>;

    fn mark_delivery_failed<'a>(
        &'a self,
        opportunity_id: i64,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()>;
}

impl GradiusAdLedger for PostgresGradiusAdStore {
    fn reserve<'a>(
        &'a self,
        input: GradiusAdOpportunityInput,
    ) -> GradiusServiceFuture<'a, GradiusAdReservation> {
        Box::pin(async move {
            PostgresGradiusAdStore::reserve_opportunity(self, input)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn record_api_call<'a>(&'a self, call: GradiusApiCallRecord) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::record_api_call(self, call)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn finish_ad<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
        ad: GradiusStoredAd,
    ) -> GradiusServiceFuture<'a, GradiusStoredAd> {
        Box::pin(async move {
            PostgresGradiusAdStore::finish_ad(self, opportunity_id, attempt_generation, ad)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn finish_no_ad<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::finish_no_ad(
                self,
                opportunity_id,
                attempt_generation,
                OffsetDateTime::now_utc(),
            )
            .await
            .map_err(|error| error.to_string())
        })
    }

    fn finish_provider_error<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::finish_provider_error(
                self,
                opportunity_id,
                attempt_generation,
                OffsetDateTime::now_utc(),
            )
            .await
            .map_err(|error| error.to_string())
        })
    }

    fn finish_privacy_error<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::finish_privacy_error(
                self,
                opportunity_id,
                attempt_generation,
                OffsetDateTime::now_utc(),
            )
            .await
            .map_err(|error| error.to_string())
        })
    }

    fn finish_unsupported_surface<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::finish_unsupported_surface(
                self,
                opportunity_id,
                attempt_generation,
                OffsetDateTime::now_utc(),
            )
            .await
            .map_err(|error| error.to_string())
        })
    }

    fn finish_render_error<'a>(
        &'a self,
        opportunity_id: i64,
        attempt_generation: i32,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::finish_render_error(
                self,
                opportunity_id,
                attempt_generation,
                error,
                OffsetDateTime::now_utc(),
            )
            .await
            .map_err(|store_error| store_error.to_string())
        })
    }

    fn mark_render_error<'a>(
        &'a self,
        opportunity_id: i64,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::mark_render_error(
                self,
                opportunity_id,
                error,
                OffsetDateTime::now_utc(),
            )
            .await
            .map_err(|error| error.to_string())
        })
    }

    fn mark_queued<'a>(
        &'a self,
        opportunity_id: i64,
        batch_id: &'a str,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::mark_queued(
                self,
                opportunity_id,
                batch_id,
                OffsetDateTime::now_utc(),
            )
            .await
            .map_err(|error| error.to_string())
        })
    }

    fn reconcile_delivered<'a>(
        &'a self,
        opportunity_id: i64,
        batch_id: &'a str,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::reconcile_delivered(
                self,
                opportunity_id,
                batch_id,
                OffsetDateTime::now_utc(),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
        })
    }

    fn mark_delivery_failed_by_batch<'a>(
        &'a self,
        batch_id: &'a str,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::mark_delivery_failed_by_batch(
                self,
                batch_id,
                error,
                OffsetDateTime::now_utc(),
            )
            .await
            .map(|_| ())
            .map_err(|store_error| store_error.to_string())
        })
    }

    fn mark_delivery_failed<'a>(
        &'a self,
        opportunity_id: i64,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move {
            PostgresGradiusAdStore::mark_delivery_failed(
                self,
                opportunity_id,
                error,
                OffsetDateTime::now_utc(),
            )
            .await
            .map(|_| ())
            .map_err(|store_error| store_error.to_string())
        })
    }
}

pub trait GradiusVipChecker: Send + Sync {
    fn verified_is_vip<'a>(
        &'a self,
        user_id: i64,
        now: OffsetDateTime,
    ) -> GradiusServiceFuture<'a, bool>;
}

impl<T> GradiusVipChecker for T
where
    T: crate::payments::VipStatusChecker + Send + Sync,
{
    fn verified_is_vip<'a>(
        &'a self,
        user_id: i64,
        now: OffsetDateTime,
    ) -> GradiusServiceFuture<'a, bool> {
        Box::pin(async move {
            crate::payments::VipStatusChecker::verified_is_vip_at(self, user_id, now).await
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GradiusAdAppendRequest {
    pub dialog_job_id: i64,
    pub attempt_key: String,
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    pub user_id: i64,
    pub user_text: String,
    pub assistant_text: String,
    pub language: String,
    pub model_version: Option<String>,
    pub completed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GradiusAdTail {
    pub opportunity_id: i64,
    pub html: String,
}

pub trait GradiusAdAppender: Send + Sync {
    fn append<'a>(
        &'a self,
        request: GradiusAdAppendRequest,
    ) -> GradiusServiceFuture<'a, Option<GradiusAdTail>>;

    fn record_unsupported_surface<'a>(
        &'a self,
        request: GradiusAdAppendRequest,
    ) -> GradiusServiceFuture<'a, ()>;

    fn mark_render_error<'a>(
        &'a self,
        opportunity_id: i64,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()>;

    fn mark_queued<'a>(
        &'a self,
        opportunity_id: i64,
        batch_id: &'a str,
    ) -> GradiusServiceFuture<'a, ()>;

    fn mark_delivered<'a>(
        &'a self,
        opportunity_id: i64,
        batch_id: &'a str,
    ) -> GradiusServiceFuture<'a, ()>;

    fn mark_delivery_failed<'a>(
        &'a self,
        opportunity_id: i64,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()>;
}

#[derive(Clone)]
pub struct GradiusAdService {
    dialogue: Arc<dyn GradiusDialogueClient>,
    redactor: Arc<dyn GradiusTextRedactor>,
    ledger: Arc<dyn GradiusAdLedger>,
    vip: Arc<dyn GradiusVipChecker>,
}

impl GradiusAdService {
    #[must_use]
    pub fn new(
        dialogue: Arc<dyn GradiusDialogueClient>,
        redactor: Arc<dyn GradiusTextRedactor>,
        ledger: Arc<dyn GradiusAdLedger>,
        vip: Arc<dyn GradiusVipChecker>,
    ) -> Self {
        Self {
            dialogue,
            redactor,
            ledger,
            vip,
        }
    }

    pub async fn append(
        &self,
        request: GradiusAdAppendRequest,
    ) -> Result<Option<GradiusAdTail>, String> {
        if request.chat_id <= 0
            || self
                .vip
                .verified_is_vip(request.user_id, request.completed_at)
                .await?
        {
            return Ok(None);
        }
        let Some(ids) =
            GradiusSyntheticIds::derive(request.chat_id, request.thread_id, request.user_id)
        else {
            return Ok(None);
        };
        let reservation = self
            .ledger
            .reserve(GradiusAdOpportunityInput {
                opportunity_key: format!("dialog-job:{}", request.dialog_job_id),
                attempt_key: request.attempt_key.clone(),
                dialog_job_id: Some(request.dialog_job_id),
                integration_kind: GradiusIntegrationKind::NativeDialogue.as_str().to_owned(),
                user_id: request.user_id,
                chat_id: request.chat_id,
                thread_id: request.thread_id.unwrap_or_default(),
                model_version: request.model_version.clone(),
                completed_at: request.completed_at,
            })
            .await?;
        let opportunity_id = reservation.opportunity_id();
        let attempt_generation = match reservation {
            GradiusAdReservation::Replay { ad, .. } => {
                return Ok(Some(GradiusAdTail {
                    opportunity_id,
                    html: ad.rendered_html,
                }));
            }
            GradiusAdReservation::Reserved {
                attempt_generation, ..
            } => attempt_generation,
            GradiusAdReservation::Ineligible { .. }
            | GradiusAdReservation::Pending { .. }
            | GradiusAdReservation::Completed { .. } => return Ok(None),
        };

        let result = self
            .fetch_ad(opportunity_id, attempt_generation, &request, &ids)
            .await;
        let ad = match result {
            Ok(Some(GradiusPlacement::NativeDialogue(ad))) => ad,
            Ok(Some(_)) => {
                let error = "Gradius returned an unsupported placement for native_dialogue";
                let _ = self
                    .ledger
                    .finish_render_error(opportunity_id, attempt_generation, error)
                    .await;
                return Err(error.to_owned());
            }
            Ok(None) => {
                self.ledger
                    .finish_no_ad(opportunity_id, attempt_generation)
                    .await?;
                return Ok(None);
            }
            Err(error) => {
                let _ = self
                    .ledger
                    .finish_provider_error(opportunity_id, attempt_generation)
                    .await;
                return Err(error);
            }
        };
        let prepared = (|| {
            let insert_index = i32::try_from(ad.insert_index)
                .map_err(|_| "Gradius insert index exceeds storage range".to_owned())?;
            let selected_placement = json!({
                "type": "native-text-ad",
                "content": {
                    "insert_index": ad.insert_index,
                    "content": ad.markdown.clone(),
                },
                "show_price": ad.show_price,
                "click_price": ad.click_price,
            });
            let tail = render_ad_tail(opportunity_id, request.dialog_job_id, &ad.markdown)?;
            let stored = GradiusStoredAd {
                markdown: ad.markdown,
                rendered_html: tail.html.clone(),
                selected_placement,
                insert_index: Some(insert_index),
                show_price: ad.show_price,
                click_price: ad.click_price,
                prepared_at: request.completed_at,
                shown_at: None,
            };
            Ok::<_, String>((stored, tail))
        })();
        let (stored, tail) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = self
                    .ledger
                    .finish_render_error(opportunity_id, attempt_generation, &error)
                    .await;
                return Err(error);
            }
        };
        self.ledger
            .finish_ad(opportunity_id, attempt_generation, stored)
            .await?;
        tracing::info!(
            opportunity_id,
            job_id = request.dialog_job_id,
            integration_kind = GradiusIntegrationKind::NativeDialogue.as_str(),
            outcome = "ad",
            delivery_state = "prepared",
            "Gradius advertising opportunity prepared"
        );
        Ok(Some(tail))
    }

    pub async fn record_unsupported_surface(
        &self,
        request: GradiusAdAppendRequest,
    ) -> Result<(), String> {
        if request.chat_id <= 0
            || self
                .vip
                .verified_is_vip(request.user_id, request.completed_at)
                .await?
        {
            return Ok(());
        }
        let reservation = self
            .ledger
            .reserve(GradiusAdOpportunityInput {
                opportunity_key: format!("dialog-job:{}", request.dialog_job_id),
                attempt_key: request.attempt_key,
                dialog_job_id: Some(request.dialog_job_id),
                integration_kind: GradiusIntegrationKind::NativeDialogue.as_str().to_owned(),
                user_id: request.user_id,
                chat_id: request.chat_id,
                thread_id: request.thread_id.unwrap_or_default(),
                model_version: request.model_version,
                completed_at: request.completed_at,
            })
            .await?;
        if let GradiusAdReservation::Reserved {
            opportunity_id,
            attempt_generation,
            ..
        } = reservation
        {
            self.ledger
                .finish_unsupported_surface(opportunity_id, attempt_generation)
                .await?;
        }
        Ok(())
    }

    async fn fetch_ad(
        &self,
        opportunity_id: i64,
        attempt_generation: i32,
        request: &GradiusAdAppendRequest,
        ids: &GradiusSyntheticIds,
    ) -> Result<Option<GradiusPlacement>, String> {
        let user_text = match self.redactor.redact(&request.user_text).await {
            Ok(text) => text,
            Err(error) => {
                let _ = self
                    .ledger
                    .finish_privacy_error(opportunity_id, attempt_generation)
                    .await;
                return Err(error);
            }
        };
        let language = gradius_language(&request.language);
        let user_call = self
            .call_and_record(
                opportunity_id,
                attempt_generation,
                1,
                ids,
                request.completed_at,
                GradiusDialogueTurn {
                    chat_id: ids.chat_id.clone(),
                    user_id: ids.user_id.clone(),
                    role: GradiusDialogueRole::User,
                    language: language.clone(),
                    model_version: request.model_version.clone(),
                    text: user_text,
                },
            )
            .await?;
        let unexpected_user_placement = user_call.is_some();
        if unexpected_user_placement {
            tracing::info!(
                opportunity_id,
                job_id = request.dialog_job_id,
                integration_kind = GradiusIntegrationKind::NativeDialogue.as_str(),
                role = GradiusDialogueRole::User.as_str(),
                outcome = "ad",
                "Gradius returned a placement for the user turn"
            );
        }
        let assistant_text = match self.redactor.redact(&request.assistant_text).await {
            Ok(text) => text,
            Err(error) => {
                if unexpected_user_placement {
                    let _ = self
                        .ledger
                        .finish_render_error(
                            opportunity_id,
                            attempt_generation,
                            "Gradius returned a placement for the user turn",
                        )
                        .await;
                } else {
                    let _ = self
                        .ledger
                        .finish_privacy_error(opportunity_id, attempt_generation)
                        .await;
                }
                return Err(error);
            }
        };
        let assistant_call = self
            .call_and_record(
                opportunity_id,
                attempt_generation,
                2,
                ids,
                request.completed_at,
                GradiusDialogueTurn {
                    chat_id: ids.chat_id.clone(),
                    user_id: ids.user_id.clone(),
                    role: GradiusDialogueRole::Assistant,
                    language,
                    model_version: request.model_version.clone(),
                    text: assistant_text.clone(),
                },
            )
            .await;
        let ad = match assistant_call {
            Ok(ad) => ad,
            Err(error) if unexpected_user_placement => {
                let _ = self
                    .ledger
                    .finish_render_error(
                        opportunity_id,
                        attempt_generation,
                        "Gradius returned a placement for the user turn",
                    )
                    .await;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if unexpected_user_placement {
            let error = "Gradius returned a placement for the user turn";
            let _ = self
                .ledger
                .finish_render_error(opportunity_id, attempt_generation, error)
                .await;
            return Err(error.to_owned());
        }
        if ad.as_ref().is_some_and(|placement| match placement {
            GradiusPlacement::NativeDialogue(ad) => {
                ad.insert_index != assistant_text.chars().count()
            }
            GradiusPlacement::Standalone { .. } => true,
        }) {
            let error = "Gradius returned a non-terminal or unsupported placement";
            let _ = self
                .ledger
                .finish_render_error(opportunity_id, attempt_generation, error)
                .await;
            return Err(error.to_owned());
        }
        Ok(ad)
    }

    async fn call_and_record(
        &self,
        opportunity_id: i64,
        attempt_generation: i32,
        sequence: i16,
        ids: &GradiusSyntheticIds,
        created_at: OffsetDateTime,
        turn: GradiusDialogueTurn,
    ) -> Result<Option<GradiusPlacement>, String> {
        let role = turn.role;
        let call = self.dialogue.dialogue(turn).await;
        let error = call.result.as_ref().err().cloned();
        if let Some(exchange) = call.exchange {
            let status = exchange.status.map(i32::from);
            let duration_ms = exchange.duration_ms;
            let outcome = exchange.outcome.as_str().to_owned();
            self.ledger
                .record_api_call(GradiusApiCallRecord {
                    opportunity_id,
                    attempt_generation,
                    sequence,
                    role: exchange.role.map(|role| role.as_str().to_owned()),
                    synthetic_chat_id: ids.chat_id.clone(),
                    synthetic_user_id: ids.user_id.clone(),
                    endpoint: exchange.endpoint,
                    request_body: exchange.request_body,
                    response_status: status,
                    response_body: exchange.response_body,
                    response_json: exchange.response_json,
                    response_truncated: exchange.response_truncated,
                    duration_ms,
                    outcome: outcome.clone(),
                    error: error.clone(),
                    created_at,
                })
                .await?;
            tracing::info!(
                opportunity_id,
                integration_kind = GradiusIntegrationKind::NativeDialogue.as_str(),
                role = role.as_str(),
                status,
                duration_ms,
                outcome,
                "Gradius API exchange audited"
            );
        }
        call.result
    }
}

impl GradiusAdAppender for GradiusAdService {
    fn append<'a>(
        &'a self,
        request: GradiusAdAppendRequest,
    ) -> GradiusServiceFuture<'a, Option<GradiusAdTail>> {
        Box::pin(async move { GradiusAdService::append(self, request).await })
    }

    fn record_unsupported_surface<'a>(
        &'a self,
        request: GradiusAdAppendRequest,
    ) -> GradiusServiceFuture<'a, ()> {
        Box::pin(async move { GradiusAdService::record_unsupported_surface(self, request).await })
    }

    fn mark_render_error<'a>(
        &'a self,
        opportunity_id: i64,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()> {
        self.ledger.mark_render_error(opportunity_id, error)
    }

    fn mark_queued<'a>(
        &'a self,
        opportunity_id: i64,
        batch_id: &'a str,
    ) -> GradiusServiceFuture<'a, ()> {
        self.ledger.mark_queued(opportunity_id, batch_id)
    }

    fn mark_delivered<'a>(
        &'a self,
        opportunity_id: i64,
        batch_id: &'a str,
    ) -> GradiusServiceFuture<'a, ()> {
        self.ledger.reconcile_delivered(opportunity_id, batch_id)
    }

    fn mark_delivery_failed<'a>(
        &'a self,
        opportunity_id: i64,
        error: &'a str,
    ) -> GradiusServiceFuture<'a, ()> {
        self.ledger.mark_delivery_failed(opportunity_id, error)
    }
}

fn render_ad_tail(
    opportunity_id: i64,
    dialog_job_id: i64,
    markdown: &str,
) -> Result<GradiusAdTail, String> {
    let ad_html = telegram_html_from_markdown(markdown).map_err(|error| error.to_string())?;
    let appendix = render_gradius_vip_appendix(&format!("dialog-job-{dialog_job_id}"))
        .ok_or_else(|| "Gradius VIP appendix catalog is empty".to_owned())?;
    let html = format!("{GRADIUS_AD_LABEL}{ad_html}\n\n{appendix}");
    if html.len() > TELEGRAM_TEXT_MAX_BYTES {
        return Err("Gradius advertising tail exceeds the Telegram text limit".to_owned());
    }
    if !is_valid_telegram_html(&html) {
        return Err("Gradius advertising tail is not valid Telegram HTML".to_owned());
    }
    Ok(GradiusAdTail {
        opportunity_id,
        html,
    })
}

#[must_use]
pub fn gradius_privacy_config(
    config: &openplotva_config::AppConfig,
) -> openplotva_memory::DiscoveryRedactorConfig {
    let memory = &config.memory;
    openplotva_memory::DiscoveryRedactorConfig {
        base_url: config.llm.discovery.base_url.clone(),
        service_name: memory.redaction_service_name.clone(),
        endpoint_name: memory.redaction_endpoint_name.clone(),
        timeout: positive_seconds(memory.redaction_timeout_seconds),
        task_timeout: positive_seconds(memory.redaction_task_timeout_seconds),
        poll_interval: positive_seconds(memory.redaction_poll_seconds),
        capacity_wait: positive_seconds(memory.redaction_capacity_wait_seconds),
        capacity_poll_interval: positive_seconds(memory.redaction_capacity_poll_seconds),
        categories: Vec::new(),
    }
    .with_defaults()
}

fn positive_seconds(seconds: i32) -> std::time::Duration {
    std::time::Duration::from_secs(u64::try_from(seconds.max(0)).unwrap_or_default())
}

fn gradius_language(locale: &str) -> String {
    let language =
        locale.trim().split(['-', '_']).next().filter(|value| {
            value.len() == 2 && value.bytes().all(|byte| byte.is_ascii_alphabetic())
        });
    language
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| "ru".to_owned())
}

#[must_use]
pub fn render_gradius_vip_appendix(impression_key: &str) -> Option<String> {
    vip_hint_for_impression(impression_key).map(render_vip_hint_html)
}

#[must_use]
pub fn render_vip_hint_html(hint: &str) -> String {
    let mut output = String::with_capacity(hint.len() + 96);
    output.push_str("<tg-spoiler>");
    let mut cursor = 0;
    for (index, _) in hint.match_indices("VIP") {
        let end = index + "VIP".len();
        let previous_is_word = hint[..index]
            .chars()
            .next_back()
            .is_some_and(is_word_character);
        let next_is_word = hint[end..].chars().next().is_some_and(is_word_character);
        if previous_is_word || next_is_word {
            continue;
        }
        output.push_str(&escape_telegram_html_text(&hint[cursor..index]));
        output.push_str(LINKED_VIP);
        cursor = end;
    }
    output.push_str(&escape_telegram_html_text(&hint[cursor..]));
    output.push_str("</tg-spoiler>");
    debug_assert!(is_valid_telegram_html(&output));
    debug_assert!(output.contains(VIP_URL));
    output
}

fn is_word_character(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
    };

    use openplotva_llm::gradius::{
        GradiusApiExchange, GradiusCallOutcome, GradiusDialogueAd, GradiusDialogueRole,
        GradiusDialogueTurn, GradiusIntegrationKind, GradiusPlacement,
    };
    use openplotva_storage::gradius_ads::{
        GradiusAdOpportunityInput, GradiusAdReservation, GradiusApiCallRecord, GradiusStoredAd,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::{
        GradiusAdAppendRequest, GradiusAdLedger, GradiusAdService, GradiusDialogueCall,
        GradiusDialogueClient, GradiusTextRedactor, GradiusVipChecker, gradius_privacy_config,
        render_vip_hint_html,
    };

    type TestFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;
    type DialogueResponses = VecDeque<Result<Option<GradiusPlacement>, String>>;

    #[derive(Clone, Default)]
    struct DialogueStub {
        calls: Arc<Mutex<Vec<GradiusDialogueTurn>>>,
        responses: Arc<Mutex<DialogueResponses>>,
    }

    impl GradiusDialogueClient for DialogueStub {
        fn dialogue<'a>(
            &'a self,
            turn: GradiusDialogueTurn,
        ) -> Pin<Box<dyn Future<Output = GradiusDialogueCall> + Send + 'a>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("dialogue calls")
                    .push(turn.clone());
                let result = self
                    .responses
                    .lock()
                    .expect("dialogue responses")
                    .pop_front()
                    .expect("stub response");
                let outcome = match &result {
                    Ok(Some(_)) => GradiusCallOutcome::Ad,
                    Ok(None) => GradiusCallOutcome::NoAd,
                    Err(_) => GradiusCallOutcome::TransportError,
                };
                GradiusDialogueCall {
                    result,
                    exchange: Some(GradiusApiExchange {
                        integration_kind: GradiusIntegrationKind::NativeDialogue,
                        role: Some(turn.role),
                        endpoint: format!("https://ads.example/dialogue?chat_id={}", turn.chat_id),
                        request_body: json!({"text": turn.text, "user_metadata": {}}),
                        status: Some(200),
                        response_body: Some("[]".to_owned()),
                        response_json: Some(json!([])),
                        response_truncated: false,
                        duration_ms: 7,
                        outcome,
                    }),
                }
            })
        }
    }

    #[derive(Clone, Default)]
    struct RedactorStub {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl GradiusTextRedactor for RedactorStub {
        fn redact<'a>(&'a self, text: &'a str) -> TestFuture<'a, String> {
            Box::pin(async move {
                let mut calls = self.calls.lock().expect("redactor calls");
                calls.push(text.to_owned());
                Ok(if calls.len() == 1 {
                    "user-safe".to_owned()
                } else {
                    "assistant-safe".to_owned()
                })
            })
        }
    }

    #[derive(Clone, Default)]
    struct LedgerStub {
        reservation: Arc<Mutex<Option<GradiusAdReservation>>>,
        reserved_inputs: Arc<Mutex<Vec<GradiusAdOpportunityInput>>>,
        api_calls: Arc<Mutex<Vec<GradiusApiCallRecord>>>,
        finished_ads: Arc<Mutex<Vec<(i64, GradiusStoredAd)>>>,
        finished_no_ads: Arc<Mutex<Vec<i64>>>,
        finished_provider_errors: Arc<Mutex<Vec<i64>>>,
        finished_privacy_errors: Arc<Mutex<Vec<i64>>>,
        finished_unsupported: Arc<Mutex<Vec<i64>>>,
        render_errors: Arc<Mutex<Vec<(i64, String)>>>,
        queued: Arc<Mutex<Vec<(i64, String)>>>,
        delivered_batches: Arc<Mutex<Vec<String>>>,
        failed_delivery_batches: Arc<Mutex<Vec<(String, String)>>>,
        failed_delivery_opportunities: Arc<Mutex<Vec<(i64, String)>>>,
    }

    impl LedgerStub {
        fn with_reservation(reservation: GradiusAdReservation) -> Self {
            let stub = Self::default();
            *stub.reservation.lock().expect("reservation") = Some(reservation);
            stub
        }
    }

    impl GradiusAdLedger for LedgerStub {
        fn reserve<'a>(
            &'a self,
            input: GradiusAdOpportunityInput,
        ) -> TestFuture<'a, GradiusAdReservation> {
            Box::pin(async move {
                self.reserved_inputs
                    .lock()
                    .expect("reserved inputs")
                    .push(input.clone());
                Ok(self
                    .reservation
                    .lock()
                    .expect("reservation")
                    .take()
                    .unwrap_or(GradiusAdReservation::Reserved {
                        opportunity_id: 1,
                        interaction_started_at: input.completed_at,
                        completed_answers: 3,
                        attempt_generation: 1,
                    }))
            })
        }

        fn record_api_call<'a>(&'a self, call: GradiusApiCallRecord) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.api_calls.lock().expect("api calls").push(call);
                Ok(())
            })
        }

        fn finish_ad<'a>(
            &'a self,
            opportunity_id: i64,
            _attempt_generation: i32,
            ad: GradiusStoredAd,
        ) -> TestFuture<'a, GradiusStoredAd> {
            Box::pin(async move {
                self.finished_ads
                    .lock()
                    .expect("finished ads")
                    .push((opportunity_id, ad.clone()));
                Ok(ad)
            })
        }

        fn finish_no_ad<'a>(
            &'a self,
            opportunity_id: i64,
            _attempt_generation: i32,
        ) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.finished_no_ads
                    .lock()
                    .expect("finished no ads")
                    .push(opportunity_id);
                Ok(())
            })
        }

        fn finish_provider_error<'a>(
            &'a self,
            opportunity_id: i64,
            _attempt_generation: i32,
        ) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.finished_provider_errors
                    .lock()
                    .expect("finished provider errors")
                    .push(opportunity_id);
                Ok(())
            })
        }

        fn finish_privacy_error<'a>(
            &'a self,
            opportunity_id: i64,
            _attempt_generation: i32,
        ) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.finished_privacy_errors
                    .lock()
                    .expect("finished privacy errors")
                    .push(opportunity_id);
                Ok(())
            })
        }

        fn finish_unsupported_surface<'a>(
            &'a self,
            opportunity_id: i64,
            _attempt_generation: i32,
        ) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.finished_unsupported
                    .lock()
                    .expect("finished unsupported surfaces")
                    .push(opportunity_id);
                Ok(())
            })
        }

        fn finish_render_error<'a>(
            &'a self,
            opportunity_id: i64,
            _attempt_generation: i32,
            error: &'a str,
        ) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.render_errors
                    .lock()
                    .expect("render errors")
                    .push((opportunity_id, error.to_owned()));
                Ok(())
            })
        }

        fn mark_render_error<'a>(
            &'a self,
            opportunity_id: i64,
            error: &'a str,
        ) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.render_errors
                    .lock()
                    .expect("render errors")
                    .push((opportunity_id, error.to_owned()));
                Ok(())
            })
        }

        fn mark_queued<'a>(&'a self, opportunity_id: i64, batch_id: &'a str) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.queued
                    .lock()
                    .expect("queued")
                    .push((opportunity_id, batch_id.to_owned()));
                Ok(())
            })
        }

        fn reconcile_delivered<'a>(
            &'a self,
            _opportunity_id: i64,
            batch_id: &'a str,
        ) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.delivered_batches
                    .lock()
                    .expect("delivered batches")
                    .push(batch_id.to_owned());
                Ok(())
            })
        }

        fn mark_delivery_failed_by_batch<'a>(
            &'a self,
            batch_id: &'a str,
            error: &'a str,
        ) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.failed_delivery_batches
                    .lock()
                    .expect("failed delivery batches")
                    .push((batch_id.to_owned(), error.to_owned()));
                Ok(())
            })
        }

        fn mark_delivery_failed<'a>(
            &'a self,
            opportunity_id: i64,
            error: &'a str,
        ) -> TestFuture<'a, ()> {
            Box::pin(async move {
                self.failed_delivery_opportunities
                    .lock()
                    .expect("failed delivery opportunities")
                    .push((opportunity_id, error.to_owned()));
                Ok(())
            })
        }
    }

    #[derive(Clone, Copy)]
    struct FailingRedactor;

    impl GradiusTextRedactor for FailingRedactor {
        fn redact<'a>(&'a self, _text: &'a str) -> TestFuture<'a, String> {
            Box::pin(async { Err("privacy unavailable".to_owned()) })
        }
    }

    #[derive(Clone, Default)]
    struct SecondRedactionFails {
        calls: Arc<Mutex<usize>>,
    }

    impl GradiusTextRedactor for SecondRedactionFails {
        fn redact<'a>(&'a self, _text: &'a str) -> TestFuture<'a, String> {
            Box::pin(async move {
                let mut calls = self.calls.lock().expect("redaction calls");
                *calls += 1;
                if *calls == 1 {
                    Ok("user-safe".to_owned())
                } else {
                    Err("assistant privacy unavailable".to_owned())
                }
            })
        }
    }

    #[derive(Clone, Copy)]
    struct VipStub(bool);

    impl GradiusVipChecker for VipStub {
        fn verified_is_vip<'a>(
            &'a self,
            _user_id: i64,
            _now: OffsetDateTime,
        ) -> TestFuture<'a, bool> {
            Box::pin(async move { Ok(self.0) })
        }
    }

    #[derive(Clone, Copy)]
    struct FailingVipStub;

    impl GradiusVipChecker for FailingVipStub {
        fn verified_is_vip<'a>(
            &'a self,
            _user_id: i64,
            _now: OffsetDateTime,
        ) -> TestFuture<'a, bool> {
            Box::pin(async { Err("VIP lookup unavailable".to_owned()) })
        }
    }

    fn test_request(dialog_job_id: i64) -> GradiusAdAppendRequest {
        GradiusAdAppendRequest {
            dialog_job_id,
            attempt_key: "test-claim-1".to_owned(),
            chat_id: 42,
            thread_id: None,
            user_id: 42,
            user_text: "вопрос".to_owned(),
            assistant_text: "ответ".to_owned(),
            language: "ru".to_owned(),
            model_version: None,
            completed_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn vip_hint_links_exact_words_inside_one_spoiler() {
        assert_eq!(
            render_vip_hint_html("Кстати, пользователи VIP не видят рекламы"),
            "<tg-spoiler>Кстати, пользователи <a href=\"https://t.me/PlotvoBot?start=vip\">VIP</a> не видят рекламы</tg-spoiler>"
        );
        assert_eq!(
            render_vip_hint_html("VIP и не-VIP: 2 < 3 & SVIPX"),
            "<tg-spoiler><a href=\"https://t.me/PlotvoBot?start=vip\">VIP</a> и не-<a href=\"https://t.me/PlotvoBot?start=vip\">VIP</a>: 2 &lt; 3 &amp; SVIPX</tg-spoiler>"
        );
    }

    #[tokio::test]
    async fn service_only_reserves_private_non_vip_dialogues() {
        let dialogue = DialogueStub::default();
        let redactor = RedactorStub::default();
        let ledger = LedgerStub::default();
        let group_service = GradiusAdService::new(
            Arc::new(dialogue.clone()),
            Arc::new(redactor.clone()),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(false)),
        );
        let mut group_request = test_request(70);
        group_request.chat_id = -10042;
        assert_eq!(group_service.append(group_request).await, Ok(None));

        let vip_service = GradiusAdService::new(
            Arc::new(dialogue.clone()),
            Arc::new(redactor.clone()),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(true)),
        );
        assert_eq!(vip_service.append(test_request(71)).await, Ok(None));

        assert!(dialogue.calls.lock().expect("dialogue calls").is_empty());
        assert!(redactor.calls.lock().expect("redactor calls").is_empty());
        assert!(
            ledger
                .reserved_inputs
                .lock()
                .expect("reserved inputs")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn service_audits_rich_surface_without_privacy_or_provider_calls() {
        let dialogue = DialogueStub::default();
        let redactor = RedactorStub::default();
        let ledger = LedgerStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue.clone()),
            Arc::new(redactor.clone()),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(false)),
        );

        service
            .record_unsupported_surface(test_request(711))
            .await
            .expect("unsupported surface audit");

        assert!(dialogue.calls.lock().expect("dialogue calls").is_empty());
        assert!(redactor.calls.lock().expect("redactor calls").is_empty());
        assert_eq!(
            ledger
                .finished_unsupported
                .lock()
                .expect("unsupported surfaces")
                .as_slice(),
            &[1]
        );
    }

    #[tokio::test]
    async fn service_fails_closed_when_vip_status_cannot_be_verified() {
        let dialogue = DialogueStub::default();
        let ledger = LedgerStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue.clone()),
            Arc::new(RedactorStub::default()),
            Arc::new(ledger.clone()),
            Arc::new(FailingVipStub),
        );

        assert!(service.append(test_request(72)).await.is_err());
        assert!(dialogue.calls.lock().expect("dialogue calls").is_empty());
        assert!(
            ledger
                .reserved_inputs
                .lock()
                .expect("reserved inputs")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn service_records_no_ad_without_consuming_an_impression() {
        let dialogue = DialogueStub::default();
        {
            let mut responses = dialogue.responses.lock().expect("dialogue responses");
            responses.push_back(Ok(None));
            responses.push_back(Ok(None));
        }
        let ledger = LedgerStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue),
            Arc::new(RedactorStub::default()),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(false)),
        );

        assert_eq!(service.append(test_request(73)).await, Ok(None));
        assert_eq!(
            ledger
                .finished_no_ads
                .lock()
                .expect("finished no ads")
                .as_slice(),
            &[1]
        );
        assert!(ledger.finished_ads.lock().expect("finished ads").is_empty());
    }

    #[tokio::test]
    async fn service_records_unexpected_user_placement_as_render_error() {
        let dialogue = DialogueStub::default();
        {
            let mut responses = dialogue.responses.lock().expect("dialogue responses");
            responses.push_back(Ok(Some(GradiusPlacement::NativeDialogue(
                GradiusDialogueAd {
                    insert_index: "user-safe".chars().count(),
                    markdown: "unexpected user ad".to_owned(),
                    show_price: Some(1.0),
                    click_price: None,
                },
            ))));
            responses.push_back(Ok(None));
        }
        let ledger = LedgerStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue.clone()),
            Arc::new(RedactorStub::default()),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(false)),
        );

        assert!(service.append(test_request(730)).await.is_err());
        assert_eq!(dialogue.calls.lock().expect("dialogue calls").len(), 2);
        assert_eq!(ledger.api_calls.lock().expect("api calls").len(), 2);
        assert!(
            ledger
                .finished_no_ads
                .lock()
                .expect("finished no ads")
                .is_empty()
        );
        assert_eq!(
            ledger
                .render_errors
                .lock()
                .expect("render errors")
                .as_slice(),
            &[(
                1,
                "Gradius returned a placement for the user turn".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn unexpected_user_placement_stays_a_render_error_when_assistant_call_fails() {
        let dialogue = DialogueStub::default();
        {
            let mut responses = dialogue.responses.lock().expect("dialogue responses");
            responses.push_back(Ok(Some(GradiusPlacement::NativeDialogue(
                GradiusDialogueAd {
                    insert_index: "user-safe".chars().count(),
                    markdown: "unexpected user ad".to_owned(),
                    show_price: Some(1.0),
                    click_price: None,
                },
            ))));
            responses.push_back(Err("assistant provider failed".to_owned()));
        }
        let ledger = LedgerStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue.clone()),
            Arc::new(RedactorStub::default()),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(false)),
        );

        assert!(service.append(test_request(7301)).await.is_err());
        assert_eq!(dialogue.calls.lock().expect("dialogue calls").len(), 2);
        assert_eq!(ledger.api_calls.lock().expect("api calls").len(), 2);
        assert_eq!(
            ledger
                .render_errors
                .lock()
                .expect("render errors")
                .as_slice(),
            &[(
                1,
                "Gradius returned a placement for the user turn".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn service_preserves_partial_user_exchange_when_assistant_privacy_fails() {
        let dialogue = DialogueStub::default();
        dialogue
            .responses
            .lock()
            .expect("dialogue responses")
            .push_back(Ok(None));
        let ledger = LedgerStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue.clone()),
            Arc::new(SecondRedactionFails::default()),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(false)),
        );

        assert!(service.append(test_request(731)).await.is_err());
        assert_eq!(dialogue.calls.lock().expect("dialogue calls").len(), 1);
        assert_eq!(ledger.api_calls.lock().expect("api calls").len(), 1);
        assert_eq!(
            ledger
                .finished_privacy_errors
                .lock()
                .expect("privacy errors")
                .as_slice(),
            &[1]
        );
    }

    #[tokio::test]
    async fn service_rejects_a_non_terminal_insert_index() {
        let dialogue = DialogueStub::default();
        {
            let mut responses = dialogue.responses.lock().expect("dialogue responses");
            responses.push_back(Ok(None));
            responses.push_back(Ok(Some(GradiusPlacement::NativeDialogue(
                GradiusDialogueAd {
                    insert_index: 0,
                    markdown: "**Реклама**".to_owned(),
                    show_price: None,
                    click_price: None,
                },
            ))));
        }
        let ledger = LedgerStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue),
            Arc::new(RedactorStub::default()),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(false)),
        );

        assert!(service.append(test_request(74)).await.is_err());
        assert!(ledger.finished_ads.lock().expect("finished ads").is_empty());
        assert_eq!(
            ledger
                .render_errors
                .lock()
                .expect("render errors")
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[tokio::test]
    async fn service_replays_a_saved_ad_without_resending_dialogue_context() {
        let stored = GradiusStoredAd {
            markdown: "**Реклама** [перейти](https://ads.example/saved)".to_owned(),
            rendered_html: "<b>Реклама</b> <a href=\"https://ads.example/saved\">перейти</a>"
                .to_owned(),
            selected_placement: json!({"type": "native-text-ad"}),
            insert_index: Some(5),
            show_price: Some(1.0),
            click_price: Some(2.0),
            prepared_at: OffsetDateTime::UNIX_EPOCH,
            shown_at: None,
        };
        let ledger = LedgerStub::with_reservation(GradiusAdReservation::Replay {
            opportunity_id: 75,
            ad: stored,
        });
        let dialogue = DialogueStub::default();
        let redactor = RedactorStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue.clone()),
            Arc::new(redactor.clone()),
            Arc::new(ledger),
            Arc::new(VipStub(false)),
        );

        let tail = service
            .append(test_request(75))
            .await
            .expect("saved ad replay")
            .expect("saved advertising tail");
        assert!(tail.html.contains("https://ads.example/saved"));
        assert!(dialogue.calls.lock().expect("dialogue calls").is_empty());
        assert!(redactor.calls.lock().expect("redactor calls").is_empty());
    }

    #[tokio::test]
    async fn service_redacts_both_turns_calls_user_then_assistant_and_builds_tail() {
        let dialogue = DialogueStub::default();
        {
            let mut responses = dialogue.responses.lock().expect("dialogue responses");
            responses.push_back(Ok(None));
            responses.push_back(Ok(Some(GradiusPlacement::NativeDialogue(
                GradiusDialogueAd {
                    insert_index: "assistant-safe".chars().count(),
                    markdown: "**Реклама** [перейти](https://ads.example/r/42)".to_owned(),
                    show_price: Some(1.2),
                    click_price: Some(45.0),
                },
            ))));
        }
        let redactor = RedactorStub::default();
        let ledger = LedgerStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue.clone()),
            Arc::new(redactor.clone()),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(false)),
        );
        let now = OffsetDateTime::UNIX_EPOCH;

        let tail = service
            .append(GradiusAdAppendRequest {
                dialog_job_id: 77,
                attempt_key: "test-claim-77".to_owned(),
                chat_id: 42,
                thread_id: None,
                user_id: 42,
                user_text: "alice@example.test хочет скидку".to_owned(),
                assistant_text: "Ответ для alice@example.test".to_owned(),
                language: "ru-RU".to_owned(),
                model_version: Some("plotva-model".to_owned()),
                completed_at: now,
            })
            .await
            .expect("Gradius orchestration");

        let tail = tail.expect("advertising tail");
        assert_eq!(tail.opportunity_id, 1);
        assert!(tail.html.starts_with("📢 <b>Реклама</b>"));
        assert!(tail.html.contains("https://ads.example/r/42"));
        assert!(tail.html.contains("<tg-spoiler>"));
        let calls = dialogue.calls.lock().expect("dialogue calls");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].role, GradiusDialogueRole::User);
        assert_eq!(calls[0].text, "user-safe");
        assert_eq!(calls[0].language, "ru");
        assert_eq!(calls[1].role, GradiusDialogueRole::Assistant);
        assert_eq!(calls[1].text, "assistant-safe");
        assert!(calls[0].chat_id.starts_with("chat_"));
        assert!(calls[0].user_id.starts_with("user_"));
        assert_eq!(redactor.calls.lock().expect("redactor calls").len(), 2);
        assert_eq!(ledger.finished_ads.lock().expect("finished ads").len(), 1);
        let api_calls = ledger.api_calls.lock().expect("api calls");
        assert_eq!(api_calls.len(), 2);
        assert_eq!(api_calls[0].sequence, 1);
        assert_eq!(api_calls[0].role.as_deref(), Some("user"));
        assert_eq!(api_calls[0].request_body["text"], json!("user-safe"));
        assert_eq!(api_calls[1].sequence, 2);
        assert_eq!(api_calls[1].role.as_deref(), Some("assistant"));
        assert_eq!(api_calls[1].request_body["text"], json!("assistant-safe"));
        let audit = format!("{api_calls:?}");
        assert!(!audit.contains("alice@example.test"));
        assert!(!audit.contains("Auth"));
    }

    #[test]
    fn gradius_privacy_config_is_independent_of_memory_redaction_toggle() {
        let config = openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig {
            memory_redaction_enabled: Some("false".to_owned()),
            memory_redaction_service_name: Some("privacy-filter".to_owned()),
            memory_redaction_endpoint_name: Some("redact-gradius".to_owned()),
            ..Default::default()
        })
        .expect("app config");

        let privacy = gradius_privacy_config(&config);
        assert_eq!(privacy.service_name, "privacy-filter");
        assert_eq!(privacy.endpoint_name, "redact-gradius");
    }

    #[tokio::test]
    async fn service_skips_provider_and_closes_reservation_when_privacy_fails() {
        let dialogue = DialogueStub::default();
        let ledger = LedgerStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue.clone()),
            Arc::new(FailingRedactor),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(false)),
        );

        let result = service
            .append(GradiusAdAppendRequest {
                dialog_job_id: 88,
                attempt_key: "test-claim-88".to_owned(),
                chat_id: 42,
                thread_id: None,
                user_id: 42,
                user_text: "alice@example.test".to_owned(),
                assistant_text: "ответ".to_owned(),
                language: "ru".to_owned(),
                model_version: None,
                completed_at: OffsetDateTime::UNIX_EPOCH,
            })
            .await;

        assert!(result.is_err());
        assert!(dialogue.calls.lock().expect("dialogue calls").is_empty());
        assert_eq!(
            ledger
                .finished_privacy_errors
                .lock()
                .expect("finished privacy errors")
                .as_slice(),
            &[1]
        );
    }

    #[tokio::test]
    async fn service_does_not_count_an_ad_when_markdown_rendering_fails() {
        let dialogue = DialogueStub::default();
        {
            let mut responses = dialogue.responses.lock().expect("dialogue responses");
            responses.push_back(Ok(None));
            responses.push_back(Ok(Some(GradiusPlacement::NativeDialogue(
                GradiusDialogueAd {
                    insert_index: "assistant-safe".chars().count(),
                    markdown: "[опасно](javascript:alert(1))".to_owned(),
                    show_price: None,
                    click_price: None,
                },
            ))));
        }
        let ledger = LedgerStub::default();
        let service = GradiusAdService::new(
            Arc::new(dialogue),
            Arc::new(RedactorStub::default()),
            Arc::new(ledger.clone()),
            Arc::new(VipStub(false)),
        );

        let result = service
            .append(GradiusAdAppendRequest {
                dialog_job_id: 89,
                attempt_key: "test-claim-89".to_owned(),
                chat_id: 42,
                thread_id: None,
                user_id: 42,
                user_text: "вопрос".to_owned(),
                assistant_text: "ответ".to_owned(),
                language: "ru".to_owned(),
                model_version: None,
                completed_at: OffsetDateTime::UNIX_EPOCH,
            })
            .await;

        assert!(result.is_err());
        assert!(ledger.finished_ads.lock().expect("finished ads").is_empty());
        assert_eq!(
            ledger
                .render_errors
                .lock()
                .expect("render errors")
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }
}
