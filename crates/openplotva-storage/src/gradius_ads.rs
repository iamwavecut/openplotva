use serde_json::Value;
use sqlx::{PgPool, Row};
use thiserror::Error;
use time::{Duration, OffsetDateTime};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GradiusInteractionState {
    pub started_at: OffsetDateTime,
    pub last_activity_at: OffsetDateTime,
    pub completed_answers: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GradiusAdEligibilityInput {
    pub now: OffsetDateTime,
    pub interaction_started_at: OffsetDateTime,
    pub completed_answers: i32,
    pub shown_last_24_hours: i64,
    pub last_shown_at: Option<OffsetDateTime>,
    /// Latest attempt timestamp retained for policy diagnostics, not an eligibility gate.
    pub last_attempt_at: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GradiusAdIneligibility {
    InteractionThreshold,
    UserDailyCap,
    UserImpressionGap,
    /// Historical persisted outcome retained for audit compatibility.
    AttemptCooldown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GradiusAdPolicy {
    interaction_timeout: Duration,
    minimum_completed_answers: i32,
    minimum_interaction_age: Duration,
    user_daily_cap: i64,
    user_impression_gap: Duration,
}

impl Default for GradiusAdPolicy {
    fn default() -> Self {
        Self {
            interaction_timeout: Duration::minutes(30),
            minimum_completed_answers: 3,
            minimum_interaction_age: Duration::minutes(5),
            user_daily_cap: 10,
            user_impression_gap: Duration::hours(1),
        }
    }
}

impl GradiusAdPolicy {
    #[must_use]
    pub fn next_interaction(
        &self,
        previous: Option<GradiusInteractionState>,
        now: OffsetDateTime,
    ) -> GradiusInteractionState {
        let Some(previous) = previous.filter(|previous| {
            now >= previous.last_activity_at
                && now - previous.last_activity_at < self.interaction_timeout
        }) else {
            return GradiusInteractionState {
                started_at: now,
                last_activity_at: now,
                completed_answers: 1,
            };
        };
        GradiusInteractionState {
            started_at: previous.started_at,
            last_activity_at: now,
            completed_answers: previous.completed_answers.saturating_add(1),
        }
    }

    #[must_use]
    pub fn ineligibility(
        &self,
        input: &GradiusAdEligibilityInput,
    ) -> Option<GradiusAdIneligibility> {
        if input.completed_answers < self.minimum_completed_answers
            && input.now - input.interaction_started_at < self.minimum_interaction_age
        {
            return Some(GradiusAdIneligibility::InteractionThreshold);
        }
        if input.shown_last_24_hours >= self.user_daily_cap {
            return Some(GradiusAdIneligibility::UserDailyCap);
        }
        if input
            .last_shown_at
            .is_some_and(|last| input.now - last < self.user_impression_gap)
        {
            return Some(GradiusAdIneligibility::UserImpressionGap);
        }
        None
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GradiusAdOpportunityInput {
    pub opportunity_key: String,
    pub attempt_key: String,
    pub dialog_job_id: Option<i64>,
    pub integration_kind: String,
    pub user_id: i64,
    pub chat_id: i64,
    pub thread_id: i32,
    pub model_version: Option<String>,
    pub completed_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GradiusStoredAd {
    pub markdown: String,
    pub rendered_html: String,
    pub selected_placement: Value,
    pub insert_index: Option<i32>,
    pub show_price: Option<f64>,
    pub click_price: Option<f64>,
    pub prepared_at: OffsetDateTime,
    pub shown_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GradiusApiCallRecord {
    pub opportunity_id: i64,
    pub attempt_generation: i32,
    pub sequence: i16,
    pub role: Option<String>,
    pub synthetic_chat_id: String,
    pub synthetic_user_id: String,
    pub endpoint: String,
    pub request_body: Value,
    pub response_status: Option<i32>,
    pub response_body: Option<String>,
    pub response_json: Option<Value>,
    pub response_truncated: bool,
    pub duration_ms: i64,
    pub outcome: String,
    pub error: Option<String>,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub enum GradiusAdReservation {
    Reserved {
        opportunity_id: i64,
        interaction_started_at: OffsetDateTime,
        completed_answers: i32,
        attempt_generation: i32,
    },
    Ineligible {
        opportunity_id: i64,
        reason: GradiusAdIneligibility,
    },
    Replay {
        opportunity_id: i64,
        ad: GradiusStoredAd,
    },
    Pending {
        opportunity_id: i64,
    },
    Completed {
        opportunity_id: i64,
    },
}

impl GradiusAdReservation {
    #[must_use]
    pub const fn opportunity_id(&self) -> i64 {
        match self {
            Self::Reserved { opportunity_id, .. }
            | Self::Ineligible { opportunity_id, .. }
            | Self::Replay { opportunity_id, .. }
            | Self::Pending { opportunity_id }
            | Self::Completed { opportunity_id } => *opportunity_id,
        }
    }
}

#[derive(Debug, Error)]
pub enum GradiusAdStoreError {
    #[error("Gradius ad storage failed: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("invalid Gradius opportunity field: {0}")]
    InvalidInput(&'static str),
    #[error("Gradius opportunity {opportunity_key} was reused with different identity")]
    IdentityConflict { opportunity_key: String },
    #[error("Gradius opportunity {opportunity_id} has an invalid state transition")]
    InvalidTransition { opportunity_id: i64 },
    #[error("Gradius opportunity {opportunity_id} contains an invalid saved ad")]
    InvalidSavedAd { opportunity_id: i64 },
}

#[derive(Clone, Debug)]
pub struct PostgresGradiusAdStore {
    pool: PgPool,
    policy: GradiusAdPolicy,
}

impl PostgresGradiusAdStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            policy: GradiusAdPolicy::default(),
        }
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn reserve_opportunity(
        &self,
        input: GradiusAdOpportunityInput,
    ) -> Result<GradiusAdReservation, GradiusAdStoreError> {
        validate_opportunity(&input)?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "gradius-ad-user:{}:{}",
                input.integration_kind, input.user_id
            ))
            .execute(&mut *transaction)
            .await?;

        if let Some(row) = sqlx::query(
            "SELECT id, opportunity_key, dialog_job_id, integration_kind, user_id, chat_id, \
                    thread_id, interaction_started_at, completed_answers, outcome, \
                    ineligibility_reason, selected_placement, ad_markdown, \
                    rendered_html, insert_index, show_price, click_price, prepared_at, shown_at, \
                    attempt_key, attempt_generation \
             FROM gradius_ad_opportunities WHERE opportunity_key = $1",
        )
        .bind(&input.opportunity_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            validate_existing_identity(&row, &input)?;
            let opportunity_id: i64 = row.get("id");
            if row.get::<String, _>("outcome") == "reserved" {
                if row.get::<String, _>("attempt_key") == input.attempt_key {
                    transaction.commit().await?;
                    return Ok(GradiusAdReservation::Pending { opportunity_id });
                }
                let attempt_generation: i32 = sqlx::query_scalar(
                    "UPDATE gradius_ad_opportunities SET attempt_reserved_at = $2, \
                         attempt_key = $3, attempt_generation = attempt_generation + 1, updated_at = now() \
                     WHERE id = $1 AND outcome = 'reserved' RETURNING attempt_generation",
                )
                .bind(opportunity_id)
                .bind(input.completed_at)
                .bind(&input.attempt_key)
                .fetch_one(&mut *transaction)
                .await?;
                transaction.commit().await?;
                return Ok(GradiusAdReservation::Reserved {
                    opportunity_id,
                    interaction_started_at: row.get("interaction_started_at"),
                    completed_answers: row.get("completed_answers"),
                    attempt_generation,
                });
            }
            let reservation = reservation_from_existing(&row)?;
            transaction.commit().await?;
            return Ok(reservation);
        }

        let previous = sqlx::query(
            "SELECT interaction_started_at, completed_at, completed_answers \
             FROM gradius_ad_opportunities \
             WHERE integration_kind = $1 AND user_id = $2 AND chat_id = $3 AND thread_id = $4 \
             ORDER BY completed_at DESC, id DESC LIMIT 1",
        )
        .bind(&input.integration_kind)
        .bind(input.user_id)
        .bind(input.chat_id)
        .bind(input.thread_id)
        .fetch_optional(&mut *transaction)
        .await?
        .map(|row| GradiusInteractionState {
            started_at: row.get("interaction_started_at"),
            last_activity_at: row.get("completed_at"),
            completed_answers: row.get("completed_answers"),
        });
        let interaction = self.policy.next_interaction(previous, input.completed_at);

        let limits = sqlx::query(
            "SELECT COUNT(*) FILTER (WHERE shown_at > $3)::BIGINT AS shown_last_24_hours, \
                    MAX(shown_at) AS last_shown_at, MAX(attempt_reserved_at) AS last_attempt_at \
             FROM gradius_ad_opportunities WHERE integration_kind = $1 AND user_id = $2",
        )
        .bind(&input.integration_kind)
        .bind(input.user_id)
        .bind(input.completed_at - Duration::hours(24))
        .fetch_one(&mut *transaction)
        .await?;
        let eligibility = GradiusAdEligibilityInput {
            now: input.completed_at,
            interaction_started_at: interaction.started_at,
            completed_answers: interaction.completed_answers,
            shown_last_24_hours: limits.get("shown_last_24_hours"),
            last_shown_at: limits.get("last_shown_at"),
            last_attempt_at: limits.get("last_attempt_at"),
        };
        let ineligibility = self.policy.ineligibility(&eligibility);
        let outcome = if ineligibility.is_some() {
            "ineligible"
        } else {
            "reserved"
        };
        let reason = ineligibility.map(ineligibility_reason);
        let attempt_reserved_at = ineligibility.is_none().then_some(input.completed_at);
        let opportunity_id: i64 = sqlx::query_scalar(
            "INSERT INTO gradius_ad_opportunities (\
                 opportunity_key, source_kind, dialog_job_id, integration_kind, user_id, chat_id, \
                 thread_id, model_version, interaction_started_at, completed_at, completed_answers, outcome, \
                 ineligibility_reason, attempt_reserved_at, attempt_key, attempt_generation\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) RETURNING id",
        )
        .bind(&input.opportunity_key)
        .bind(source_kind(&input.opportunity_key))
        .bind(input.dialog_job_id)
        .bind(&input.integration_kind)
        .bind(input.user_id)
        .bind(input.chat_id)
        .bind(input.thread_id)
        .bind(input.model_version)
        .bind(interaction.started_at)
        .bind(input.completed_at)
        .bind(interaction.completed_answers)
        .bind(outcome)
        .bind(reason)
        .bind(attempt_reserved_at)
        .bind(&input.attempt_key)
        .bind(i32::from(attempt_reserved_at.is_some()))
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(match ineligibility {
            Some(reason) => GradiusAdReservation::Ineligible {
                opportunity_id,
                reason,
            },
            None => GradiusAdReservation::Reserved {
                opportunity_id,
                interaction_started_at: interaction.started_at,
                completed_answers: interaction.completed_answers,
                attempt_generation: 1,
            },
        })
    }

    pub async fn record_api_call(
        &self,
        call: GradiusApiCallRecord,
    ) -> Result<(), GradiusAdStoreError> {
        if call.sequence <= 0 {
            return Err(GradiusAdStoreError::InvalidInput("sequence"));
        }
        let opportunity_id = call.opportunity_id;
        let result = sqlx::query(
            "INSERT INTO gradius_api_calls (\
                 opportunity_id, attempt_generation, sequence, role, synthetic_chat_id, synthetic_user_id, endpoint, \
                 request_body, response_status, response_body, response_json, response_truncated, \
                 duration_ms, outcome, error, created_at\
             ) SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16 \
             FROM gradius_ad_opportunities \
             WHERE id = $1 AND outcome = 'reserved' AND attempt_generation = $2 \
             ON CONFLICT (opportunity_id, attempt_generation, sequence) DO NOTHING",
        )
        .bind(call.opportunity_id)
        .bind(call.attempt_generation)
        .bind(call.sequence)
        .bind(call.role)
        .bind(call.synthetic_chat_id)
        .bind(call.synthetic_user_id)
        .bind(call.endpoint)
        .bind(sqlx::types::Json(call.request_body))
        .bind(call.response_status)
        .bind(call.response_body)
        .bind(call.response_json.map(sqlx::types::Json))
        .bind(call.response_truncated)
        .bind(call.duration_ms)
        .bind(call.outcome)
        .bind(call.error)
        .bind(call.created_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(GradiusAdStoreError::InvalidTransition { opportunity_id })
        }
    }

    pub async fn finish_ad(
        &self,
        opportunity_id: i64,
        attempt_generation: i32,
        ad: GradiusStoredAd,
    ) -> Result<GradiusStoredAd, GradiusAdStoreError> {
        if ad.markdown.trim().is_empty()
            || ad.rendered_html.trim().is_empty()
            || ad.insert_index.is_some_and(|index| index < 0)
        {
            return Err(GradiusAdStoreError::InvalidSavedAd { opportunity_id });
        }
        let row = sqlx::query(
            "UPDATE gradius_ad_opportunities SET outcome = 'ad', provider_outcome = 'ad', \
                 provider_completed_at = $2, selected_placement = $3, ad_markdown = $4, \
                 rendered_html = $5, insert_index = $6, show_price = $7, click_price = $8, \
                 delivery_state = 'prepared', prepared_at = $2, updated_at = $2 \
             WHERE id = $1 AND outcome = 'reserved' AND attempt_generation = $9 \
             RETURNING selected_placement, ad_markdown, rendered_html, insert_index, show_price, \
                       click_price, prepared_at, shown_at",
        )
        .bind(opportunity_id)
        .bind(ad.prepared_at)
        .bind(sqlx::types::Json(&ad.selected_placement))
        .bind(&ad.markdown)
        .bind(&ad.rendered_html)
        .bind(ad.insert_index)
        .bind(ad.show_price)
        .bind(ad.click_price)
        .bind(attempt_generation)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = row {
            return stored_ad_from_row(&row, opportunity_id);
        }
        let existing = sqlx::query(
            "SELECT outcome, selected_placement, ad_markdown, rendered_html, insert_index, \
                    show_price, click_price, prepared_at, shown_at \
             FROM gradius_ad_opportunities WHERE id = $1 AND attempt_generation = $2",
        )
        .bind(opportunity_id)
        .bind(attempt_generation)
        .fetch_optional(&self.pool)
        .await?;
        match existing {
            Some(row) if row.get::<String, _>("outcome") == "ad" => {
                stored_ad_from_row(&row, opportunity_id)
            }
            _ => Err(GradiusAdStoreError::InvalidTransition { opportunity_id }),
        }
    }

    pub async fn finish_no_ad(
        &self,
        opportunity_id: i64,
        attempt_generation: i32,
        completed_at: OffsetDateTime,
    ) -> Result<(), GradiusAdStoreError> {
        self.finish_without_ad(
            opportunity_id,
            attempt_generation,
            "no_ad",
            Some("no_ad"),
            completed_at,
        )
        .await
    }

    pub async fn finish_provider_error(
        &self,
        opportunity_id: i64,
        attempt_generation: i32,
        completed_at: OffsetDateTime,
    ) -> Result<(), GradiusAdStoreError> {
        self.finish_without_ad(
            opportunity_id,
            attempt_generation,
            "provider_error",
            Some("error"),
            completed_at,
        )
        .await
    }

    pub async fn finish_privacy_error(
        &self,
        opportunity_id: i64,
        attempt_generation: i32,
        completed_at: OffsetDateTime,
    ) -> Result<(), GradiusAdStoreError> {
        self.finish_without_ad(
            opportunity_id,
            attempt_generation,
            "privacy_error",
            None,
            completed_at,
        )
        .await
    }

    pub async fn finish_unsupported_surface(
        &self,
        opportunity_id: i64,
        attempt_generation: i32,
        completed_at: OffsetDateTime,
    ) -> Result<(), GradiusAdStoreError> {
        self.finish_without_ad(
            opportunity_id,
            attempt_generation,
            "unsupported_surface",
            None,
            completed_at,
        )
        .await
    }

    pub async fn finish_render_error(
        &self,
        opportunity_id: i64,
        attempt_generation: i32,
        error: &str,
        completed_at: OffsetDateTime,
    ) -> Result<(), GradiusAdStoreError> {
        let result = sqlx::query(
            "UPDATE gradius_ad_opportunities SET outcome = 'render_error', \
                 provider_outcome = 'ad', provider_completed_at = $4, \
                 delivery_state = 'failed', delivery_failed_at = $4, \
                 delivery_error = left($3, 2048), updated_at = $4 \
             WHERE id = $1 AND attempt_generation = $2 AND outcome = 'reserved'",
        )
        .bind(opportunity_id)
        .bind(attempt_generation)
        .bind(error)
        .bind(completed_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() > 0 {
            return Ok(());
        }
        self.accept_existing_state(opportunity_id, attempt_generation, &["render_error"])
            .await
    }

    pub async fn mark_render_error(
        &self,
        opportunity_id: i64,
        error: &str,
        completed_at: OffsetDateTime,
    ) -> Result<(), GradiusAdStoreError> {
        let result = sqlx::query(
            "UPDATE gradius_ad_opportunities SET outcome = 'render_error', \
                 delivery_state = 'failed', delivery_failed_at = $3, \
                 delivery_error = left($2, 2048), updated_at = $3 \
             WHERE id = $1 AND outcome = 'ad' AND delivery_state = 'prepared'",
        )
        .bind(opportunity_id)
        .bind(error)
        .bind(completed_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() > 0 {
            return Ok(());
        }
        self.accept_existing_state_without_attempt(opportunity_id, &["render_error"])
            .await
    }

    pub async fn mark_queued(
        &self,
        opportunity_id: i64,
        batch_id: &str,
        queued_at: OffsetDateTime,
    ) -> Result<(), GradiusAdStoreError> {
        let result = sqlx::query(
            "UPDATE gradius_ad_opportunities SET delivery_state = 'queued', \
                 outbox_batch_id = $2, \
                 queued_at = CASE WHEN outbox_batch_id IS DISTINCT FROM $2 \
                     THEN $3 ELSE COALESCE(queued_at, $3) END, \
                 delivery_failed_at = NULL, delivery_error = NULL, updated_at = $3 \
             WHERE id = $1 AND outcome = 'ad' AND delivery_state IN ('prepared', 'queued', 'failed') \
               AND (outbox_batch_id IS NULL OR outbox_batch_id = $2 OR delivery_state = 'failed')",
        )
        .bind(opportunity_id)
        .bind(batch_id)
        .bind(queued_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() > 0 {
            return Ok(());
        }
        let existing = sqlx::query(
            "SELECT delivery_state, outbox_batch_id FROM gradius_ad_opportunities WHERE id = $1",
        )
        .bind(opportunity_id)
        .fetch_optional(&self.pool)
        .await?;
        match existing {
            Some(row)
                if matches!(
                    row.get::<Option<String>, _>("delivery_state").as_deref(),
                    Some("queued" | "delivered")
                ) && row.get::<Option<String>, _>("outbox_batch_id").as_deref()
                    == Some(batch_id) =>
            {
                Ok(())
            }
            _ => Err(GradiusAdStoreError::InvalidTransition { opportunity_id }),
        }
    }

    pub async fn mark_delivered_by_batch(
        &self,
        batch_id: &str,
        delivered_at: OffsetDateTime,
    ) -> Result<bool, GradiusAdStoreError> {
        let result = sqlx::query(
            "UPDATE gradius_ad_opportunities SET delivery_state = 'delivered', \
                 delivered_at = COALESCE(delivered_at, $2), shown_at = COALESCE(shown_at, $2), \
                 delivery_failed_at = NULL, delivery_error = NULL, updated_at = $2 \
             WHERE outbox_batch_id = $1 AND outcome = 'ad' \
               AND delivery_state IN ('queued', 'delivered')",
        )
        .bind(batch_id)
        .bind(delivered_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn reconcile_delivered(
        &self,
        opportunity_id: i64,
        batch_id: &str,
        delivered_at: OffsetDateTime,
    ) -> Result<bool, GradiusAdStoreError> {
        let result = sqlx::query(
            "UPDATE gradius_ad_opportunities SET delivery_state = 'delivered', \
                 outbox_batch_id = COALESCE(outbox_batch_id, $2), \
                 queued_at = COALESCE(queued_at, $3), \
                 delivered_at = COALESCE(delivered_at, $3), shown_at = COALESCE(shown_at, $3), \
                 delivery_failed_at = NULL, delivery_error = NULL, updated_at = $3 \
             WHERE id = $1 AND outcome = 'ad' \
               AND ((outbox_batch_id = $2 AND delivery_state IN ('queued', 'delivered')) \
                 OR (outbox_batch_id IS NULL AND delivery_state = 'prepared'))",
        )
        .bind(opportunity_id)
        .bind(batch_id)
        .bind(delivered_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn mark_delivery_failed_by_batch(
        &self,
        batch_id: &str,
        error: &str,
        failed_at: OffsetDateTime,
    ) -> Result<bool, GradiusAdStoreError> {
        let result = sqlx::query(
            "UPDATE gradius_ad_opportunities SET delivery_state = 'failed', \
                 delivery_failed_at = COALESCE(delivery_failed_at, $3), \
                 delivery_error = left($2, 2048), updated_at = $3 \
             WHERE outbox_batch_id = $1 AND outcome = 'ad' \
               AND delivery_state IN ('queued', 'failed')",
        )
        .bind(batch_id)
        .bind(error)
        .bind(failed_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn reconcile_dialogue_delivered(
        &self,
        dialog_job_id: i64,
        batch_id: &str,
        delivered_at: OffsetDateTime,
    ) -> Result<bool, GradiusAdStoreError> {
        let result = sqlx::query(
            "UPDATE gradius_ad_opportunities SET delivery_state = 'delivered', \
                 outbox_batch_id = COALESCE(outbox_batch_id, $2), \
                 queued_at = COALESCE(queued_at, $3), \
                 delivered_at = COALESCE(delivered_at, $3), shown_at = COALESCE(shown_at, $3), \
                 delivery_failed_at = NULL, delivery_error = NULL, updated_at = $3 \
             WHERE dialog_job_id = $1 AND integration_kind = 'native_dialogue' \
               AND outcome = 'ad' \
               AND ((outbox_batch_id = $2 AND delivery_state IN ('queued', 'delivered')) \
                 OR (outbox_batch_id IS NULL AND delivery_state = 'prepared'))",
        )
        .bind(dialog_job_id)
        .bind(batch_id)
        .bind(delivered_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn reconcile_dialogue_delivery_failed(
        &self,
        dialog_job_id: i64,
        batch_id: &str,
        error: &str,
        failed_at: OffsetDateTime,
    ) -> Result<bool, GradiusAdStoreError> {
        let result = sqlx::query(
            "UPDATE gradius_ad_opportunities SET delivery_state = 'failed', \
                 outbox_batch_id = COALESCE(outbox_batch_id, $2), \
                 queued_at = COALESCE(queued_at, $4), \
                 delivery_failed_at = COALESCE(delivery_failed_at, $4), \
                 delivery_error = left($3, 2048), updated_at = $4 \
             WHERE dialog_job_id = $1 AND integration_kind = 'native_dialogue' \
               AND outcome = 'ad' \
               AND ((outbox_batch_id = $2 AND delivery_state IN ('queued', 'failed')) \
                 OR (outbox_batch_id IS NULL AND delivery_state = 'prepared'))",
        )
        .bind(dialog_job_id)
        .bind(batch_id)
        .bind(error)
        .bind(failed_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn mark_delivery_failed(
        &self,
        opportunity_id: i64,
        error: &str,
        failed_at: OffsetDateTime,
    ) -> Result<bool, GradiusAdStoreError> {
        let result = sqlx::query(
            "UPDATE gradius_ad_opportunities SET delivery_state = 'failed', \
                 delivery_failed_at = COALESCE(delivery_failed_at, $3), \
                 delivery_error = left($2, 2048), updated_at = $3 \
             WHERE id = $1 AND outcome = 'ad' \
               AND delivery_state IN ('prepared', 'failed')",
        )
        .bind(opportunity_id)
        .bind(error)
        .bind(failed_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn finish_without_ad(
        &self,
        opportunity_id: i64,
        attempt_generation: i32,
        outcome: &'static str,
        provider_outcome: Option<&'static str>,
        completed_at: OffsetDateTime,
    ) -> Result<(), GradiusAdStoreError> {
        let result = sqlx::query(
            "UPDATE gradius_ad_opportunities SET outcome = $3, provider_outcome = $4, \
                 provider_completed_at = $5, updated_at = $5 \
             WHERE id = $1 AND attempt_generation = $2 AND outcome = 'reserved'",
        )
        .bind(opportunity_id)
        .bind(attempt_generation)
        .bind(outcome)
        .bind(provider_outcome)
        .bind(completed_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() > 0 {
            return Ok(());
        }
        self.accept_existing_state(opportunity_id, attempt_generation, &[outcome])
            .await
    }

    async fn accept_existing_state(
        &self,
        opportunity_id: i64,
        attempt_generation: i32,
        expected: &[&str],
    ) -> Result<(), GradiusAdStoreError> {
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT outcome FROM gradius_ad_opportunities \
                 WHERE id = $1 AND attempt_generation = $2",
        )
        .bind(opportunity_id)
        .bind(attempt_generation)
        .fetch_optional(&self.pool)
        .await?;
        if existing
            .as_deref()
            .is_some_and(|outcome| expected.contains(&outcome))
        {
            Ok(())
        } else {
            Err(GradiusAdStoreError::InvalidTransition { opportunity_id })
        }
    }

    async fn accept_existing_state_without_attempt(
        &self,
        opportunity_id: i64,
        expected: &[&str],
    ) -> Result<(), GradiusAdStoreError> {
        let existing: Option<String> =
            sqlx::query_scalar("SELECT outcome FROM gradius_ad_opportunities WHERE id = $1")
                .bind(opportunity_id)
                .fetch_optional(&self.pool)
                .await?;
        if existing
            .as_deref()
            .is_some_and(|outcome| expected.contains(&outcome))
        {
            Ok(())
        } else {
            Err(GradiusAdStoreError::InvalidTransition { opportunity_id })
        }
    }
}

fn validate_opportunity(input: &GradiusAdOpportunityInput) -> Result<(), GradiusAdStoreError> {
    if input.opportunity_key.trim().is_empty() {
        return Err(GradiusAdStoreError::InvalidInput("opportunity_key"));
    }
    if input.attempt_key.trim().is_empty() {
        return Err(GradiusAdStoreError::InvalidInput("attempt_key"));
    }
    if input.integration_kind.trim().is_empty() {
        return Err(GradiusAdStoreError::InvalidInput("integration_kind"));
    }
    Ok(())
}

fn source_kind(opportunity_key: &str) -> &str {
    opportunity_key
        .split_once(':')
        .map_or("external", |(source, _)| source)
}

fn validate_existing_identity(
    row: &sqlx::postgres::PgRow,
    input: &GradiusAdOpportunityInput,
) -> Result<(), GradiusAdStoreError> {
    if row.get::<Option<i64>, _>("dialog_job_id") != input.dialog_job_id
        || row.get::<String, _>("integration_kind") != input.integration_kind
        || row.get::<i64, _>("user_id") != input.user_id
        || row.get::<i64, _>("chat_id") != input.chat_id
        || row.get::<i32, _>("thread_id") != input.thread_id
    {
        return Err(GradiusAdStoreError::IdentityConflict {
            opportunity_key: input.opportunity_key.clone(),
        });
    }
    Ok(())
}

fn reservation_from_existing(
    row: &sqlx::postgres::PgRow,
) -> Result<GradiusAdReservation, GradiusAdStoreError> {
    let opportunity_id: i64 = row.get("id");
    let outcome: String = row.get("outcome");
    match outcome.as_str() {
        "ad" => Ok(GradiusAdReservation::Replay {
            opportunity_id,
            ad: stored_ad_from_row(row, opportunity_id)?,
        }),
        "reserved" => Ok(GradiusAdReservation::Pending { opportunity_id }),
        "ineligible" => row
            .get::<Option<String>, _>("ineligibility_reason")
            .as_deref()
            .and_then(parse_ineligibility_reason)
            .map(|reason| GradiusAdReservation::Ineligible {
                opportunity_id,
                reason,
            })
            .ok_or(GradiusAdStoreError::InvalidTransition { opportunity_id }),
        "no_ad" | "provider_error" | "privacy_error" | "render_error" | "unsupported_surface" => {
            Ok(GradiusAdReservation::Completed { opportunity_id })
        }
        _ => Err(GradiusAdStoreError::InvalidTransition { opportunity_id }),
    }
}

fn stored_ad_from_row(
    row: &sqlx::postgres::PgRow,
    opportunity_id: i64,
) -> Result<GradiusStoredAd, GradiusAdStoreError> {
    let selected_placement = row
        .get::<Option<sqlx::types::Json<Value>>, _>("selected_placement")
        .map(|value| value.0);
    let markdown = row.get::<Option<String>, _>("ad_markdown");
    let rendered_html = row.get::<Option<String>, _>("rendered_html");
    let prepared_at = row.get::<Option<OffsetDateTime>, _>("prepared_at");
    match (selected_placement, markdown, rendered_html, prepared_at) {
        (Some(selected_placement), Some(markdown), Some(rendered_html), Some(prepared_at))
            if !markdown.trim().is_empty() && !rendered_html.trim().is_empty() =>
        {
            Ok(GradiusStoredAd {
                markdown,
                rendered_html,
                selected_placement,
                insert_index: row.get("insert_index"),
                show_price: row.get("show_price"),
                click_price: row.get("click_price"),
                prepared_at,
                shown_at: row.get("shown_at"),
            })
        }
        _ => Err(GradiusAdStoreError::InvalidSavedAd { opportunity_id }),
    }
}

const fn ineligibility_reason(reason: GradiusAdIneligibility) -> &'static str {
    match reason {
        GradiusAdIneligibility::InteractionThreshold => "interaction_threshold",
        GradiusAdIneligibility::UserDailyCap => "user_daily_cap",
        GradiusAdIneligibility::UserImpressionGap => "user_impression_gap",
        GradiusAdIneligibility::AttemptCooldown => "attempt_cooldown",
    }
}

fn parse_ineligibility_reason(value: &str) -> Option<GradiusAdIneligibility> {
    match value {
        "interaction_threshold" => Some(GradiusAdIneligibility::InteractionThreshold),
        "user_daily_cap" => Some(GradiusAdIneligibility::UserDailyCap),
        "user_impression_gap" => Some(GradiusAdIneligibility::UserImpressionGap),
        "attempt_cooldown" => Some(GradiusAdIneligibility::AttemptCooldown),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error};

    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;
    use time::{Duration, OffsetDateTime};

    use super::{
        GradiusAdEligibilityInput, GradiusAdIneligibility, GradiusAdOpportunityInput,
        GradiusAdPolicy, GradiusAdReservation, GradiusApiCallRecord, GradiusInteractionState,
        GradiusStoredAd, PostgresGradiusAdStore,
    };

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + Duration::seconds(seconds)
    }

    #[test]
    fn interaction_resets_after_thirty_minutes_of_inactivity() {
        let policy = GradiusAdPolicy::default();
        let previous = GradiusInteractionState {
            started_at: at(100),
            last_activity_at: at(200),
            completed_answers: 2,
        };

        assert_eq!(
            policy.next_interaction(Some(previous), at(200 + 30 * 60 - 1)),
            GradiusInteractionState {
                started_at: at(100),
                last_activity_at: at(200 + 30 * 60 - 1),
                completed_answers: 3,
            }
        );
        assert_eq!(
            policy.next_interaction(Some(previous), at(200 + 30 * 60)),
            GradiusInteractionState {
                started_at: at(200 + 30 * 60),
                last_activity_at: at(200 + 30 * 60),
                completed_answers: 1,
            }
        );
    }

    #[test]
    fn third_answer_or_five_minutes_unlocks_the_first_attempt() {
        let policy = GradiusAdPolicy::default();
        let mut input = GradiusAdEligibilityInput {
            now: at(300),
            interaction_started_at: at(1),
            completed_answers: 2,
            shown_last_24_hours: 0,
            last_shown_at: None,
            last_attempt_at: None,
        };
        assert_eq!(
            policy.ineligibility(&input),
            Some(GradiusAdIneligibility::InteractionThreshold)
        );
        input.completed_answers = 3;
        assert_eq!(policy.ineligibility(&input), None);
        input.completed_answers = 1;
        input.now = at(301);
        assert_eq!(policy.ineligibility(&input), None);
    }

    #[test]
    fn ten_ad_cap_and_hourly_gap_are_enforced_at_the_boundary() {
        let policy = GradiusAdPolicy::default();
        let base = GradiusAdEligibilityInput {
            now: at(100_000),
            interaction_started_at: at(99_000),
            completed_answers: 3,
            shown_last_24_hours: 0,
            last_shown_at: None,
            last_attempt_at: None,
        };

        assert_eq!(
            policy.ineligibility(&GradiusAdEligibilityInput {
                shown_last_24_hours: 9,
                ..base
            }),
            None
        );
        assert_eq!(
            policy.ineligibility(&GradiusAdEligibilityInput {
                shown_last_24_hours: 10,
                ..base
            }),
            Some(GradiusAdIneligibility::UserDailyCap)
        );
        assert_eq!(
            policy.ineligibility(&GradiusAdEligibilityInput {
                last_shown_at: Some(base.now - Duration::hours(1) + Duration::seconds(1)),
                ..base
            }),
            Some(GradiusAdIneligibility::UserImpressionGap)
        );
        assert_eq!(
            policy.ineligibility(&GradiusAdEligibilityInput {
                last_shown_at: Some(base.now - Duration::hours(1)),
                ..base
            }),
            None
        );
    }

    #[test]
    fn recent_attempt_does_not_block_an_eligible_opportunity() {
        let policy = GradiusAdPolicy::default();

        assert_eq!(
            policy.ineligibility(&GradiusAdEligibilityInput {
                now: at(100_000),
                interaction_started_at: at(99_000),
                completed_answers: 3,
                shown_last_24_hours: 0,
                last_shown_at: None,
                last_attempt_at: Some(at(100_000)),
            }),
            None
        );
    }

    #[test]
    fn migration_supports_generic_opportunities_calls_and_delivery_lifecycle() {
        const UP: &str = include_str!("../../../migrations/183_gradius_ad_audit.up.sql");

        assert!(UP.contains("CREATE TABLE gradius_ad_opportunities"));
        assert!(UP.contains("integration_kind TEXT NOT NULL"));
        assert!(UP.contains("model_version TEXT"));
        assert!(UP.contains("opportunity_key TEXT NOT NULL UNIQUE"));
        assert!(UP.contains("UNIQUE (integration_kind, dialog_job_id)"));
        assert!(UP.contains("delivery_state TEXT"));
        assert!(UP.contains("attempt_key TEXT NOT NULL"));
        assert!(UP.contains("attempt_generation INTEGER NOT NULL"));
        assert!(UP.contains("UNIQUE (opportunity_id, attempt_generation, sequence)"));
        assert!(UP.contains("shown_at TIMESTAMPTZ"));
        assert!(UP.contains("selected_placement JSONB"));
        assert!(UP.contains("rendered_html TEXT"));
        assert!(UP.contains("CREATE TABLE gradius_api_calls"));
        assert!(UP.contains("request_body JSONB NOT NULL"));
        assert!(UP.contains("response_body TEXT"));
        assert!(UP.contains("response_json JSONB"));
        assert!(!UP.contains("Auth"));
    }

    #[tokio::test]
    async fn postgres_store_audits_calls_and_counts_only_delivered_ads_per_integration()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await?;
        crate::run_migrations_on(&pool).await?;
        let user_id = 9_820_825_001_i64;
        sqlx::query("DELETE FROM gradius_ad_opportunities WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await?;
        let store = PostgresGradiusAdStore::new(pool.clone());

        for answer in 1..=2_i64 {
            let reservation = store
                .reserve_opportunity(GradiusAdOpportunityInput {
                    opportunity_key: format!("dialog-job:{}", 9_820_825_100 + answer),
                    attempt_key: format!("policy-test-claim-{answer}"),
                    dialog_job_id: Some(9_820_825_100 + answer),
                    integration_kind: "native_dialogue".to_owned(),
                    user_id,
                    chat_id: user_id,
                    thread_id: 0,
                    model_version: None,
                    completed_at: at(10_000 + answer),
                })
                .await?;
            assert!(matches!(
                reservation,
                GradiusAdReservation::Ineligible {
                    reason: GradiusAdIneligibility::InteractionThreshold,
                    ..
                }
            ));
        }

        let third = GradiusAdOpportunityInput {
            opportunity_key: "dialog-job:9820825103".to_owned(),
            attempt_key: "dialog-claim-1".to_owned(),
            dialog_job_id: Some(9_820_825_103),
            integration_kind: "native_dialogue".to_owned(),
            user_id,
            chat_id: user_id,
            thread_id: 0,
            model_version: Some("test-model".to_owned()),
            completed_at: at(10_003),
        };
        let reserved = store.reserve_opportunity(third.clone()).await?;
        let opportunity_id = reserved.opportunity_id();
        let attempt_generation = match reserved {
            GradiusAdReservation::Reserved {
                completed_answers: 3,
                attempt_generation,
                ..
            } => attempt_generation,
            other => panic!("expected reserved opportunity, got {other:?}"),
        };
        assert_eq!(attempt_generation, 1);

        store
            .record_api_call(GradiusApiCallRecord {
                opportunity_id,
                attempt_generation,
                sequence: 1,
                role: Some("user".to_owned()),
                synthetic_chat_id: "chat_safe".to_owned(),
                synthetic_user_id: "user_safe".to_owned(),
                endpoint: "https://api.adlean.pro/v1/native/dialogue_model/chat?role=user"
                    .to_owned(),
                request_body: json!({"text": "[private_person]", "user_metadata": {}}),
                response_status: Some(200),
                response_body: Some("[]".to_owned()),
                response_json: Some(json!([])),
                response_truncated: false,
                duration_ms: 7,
                outcome: "no_ad".to_owned(),
                error: None,
                created_at: third.completed_at,
            })
            .await?;
        assert_eq!(
            store.reserve_opportunity(third.clone()).await?,
            GradiusAdReservation::Pending { opportunity_id }
        );
        let mut retried = third.clone();
        retried.attempt_key = "dialog-claim-2".to_owned();
        let reclaimed_generation = match store.reserve_opportunity(retried).await? {
            GradiusAdReservation::Reserved {
                opportunity_id: reclaimed_id,
                completed_answers: 3,
                attempt_generation,
                ..
            } if reclaimed_id == opportunity_id => attempt_generation,
            other => panic!("expected reclaimed opportunity, got {other:?}"),
        };
        assert_eq!(reclaimed_generation, 2);
        let preserved_generations: Vec<i32> = sqlx::query_scalar(
            "SELECT attempt_generation FROM gradius_api_calls \
             WHERE opportunity_id = $1 ORDER BY attempt_generation, sequence",
        )
        .bind(opportunity_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(preserved_generations, vec![attempt_generation]);
        assert!(
            store
                .record_api_call(GradiusApiCallRecord {
                    opportunity_id,
                    attempt_generation,
                    sequence: 2,
                    role: Some("assistant".to_owned()),
                    synthetic_chat_id: "chat_safe".to_owned(),
                    synthetic_user_id: "user_safe".to_owned(),
                    endpoint: "https://api.adlean.pro/v1/native/dialogue_model/chat?role=assistant"
                        .to_owned(),
                    request_body: json!({"text": "stale attempt", "user_metadata": {}}),
                    response_status: Some(200),
                    response_body: Some("[]".to_owned()),
                    response_json: Some(json!([])),
                    response_truncated: false,
                    duration_ms: 7,
                    outcome: "no_ad".to_owned(),
                    error: None,
                    created_at: third.completed_at,
                })
                .await
                .is_err()
        );
        assert!(
            store
                .finish_no_ad(opportunity_id, attempt_generation, third.completed_at)
                .await
                .is_err()
        );
        store
            .record_api_call(GradiusApiCallRecord {
                opportunity_id,
                attempt_generation: reclaimed_generation,
                sequence: 1,
                role: Some("user".to_owned()),
                synthetic_chat_id: "chat_safe".to_owned(),
                synthetic_user_id: "user_safe".to_owned(),
                endpoint: "https://api.adlean.pro/v1/native/dialogue_model/chat?role=user"
                    .to_owned(),
                request_body: json!({"text": "[private_person]", "user_metadata": {}}),
                response_status: Some(200),
                response_body: Some("[]".to_owned()),
                response_json: Some(json!([])),
                response_truncated: false,
                duration_ms: 8,
                outcome: "no_ad".to_owned(),
                error: None,
                created_at: third.completed_at,
            })
            .await?;
        let retained_attempts: Vec<i32> = sqlx::query_scalar(
            "SELECT attempt_generation FROM gradius_api_calls \
             WHERE opportunity_id = $1 ORDER BY attempt_generation, sequence",
        )
        .bind(opportunity_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            retained_attempts,
            vec![attempt_generation, reclaimed_generation]
        );

        let stored = store
            .finish_ad(
                opportunity_id,
                reclaimed_generation,
                GradiusStoredAd {
                    markdown: "**Реклама**".to_owned(),
                    rendered_html: "<b>Реклама</b>".to_owned(),
                    selected_placement: json!({"type": "native-text-ad"}),
                    insert_index: Some(17),
                    show_price: Some(1.2),
                    click_price: Some(45.0),
                    prepared_at: third.completed_at,
                    shown_at: None,
                },
            )
            .await?;
        assert_eq!(stored.shown_at, None);
        let shown_before_delivery: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM gradius_ad_opportunities WHERE user_id = $1 AND shown_at IS NOT NULL",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(shown_before_delivery, 0);
        assert!(
            !store
                .mark_delivered_by_batch("dialog-intermediate:v1:test", at(10_005))
                .await?
        );
        assert!(
            store
                .reconcile_dialogue_delivery_failed(
                    9_820_825_103,
                    "dialog-answer:v1:test",
                    "final batch failed",
                    at(10_005),
                )
                .await?
        );
        assert!(
            !store
                .mark_delivered_by_batch("dialog-intermediate:v1:test", at(10_005))
                .await?
        );
        let failed_delivery: (String, Option<String>, Option<OffsetDateTime>) = sqlx::query_as(
            "SELECT delivery_state, outbox_batch_id, shown_at \
             FROM gradius_ad_opportunities WHERE id = $1",
        )
        .bind(opportunity_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            failed_delivery,
            (
                "failed".to_owned(),
                Some("dialog-answer:v1:test".to_owned()),
                None
            )
        );
        store
            .mark_queued(opportunity_id, "dialog-answer:v1:failed", at(10_005))
            .await?;
        store
            .mark_delivery_failed_by_batch(
                "dialog-answer:v1:failed",
                "telegram rejected batch",
                at(10_006),
            )
            .await?;
        store
            .mark_queued(opportunity_id, "dialog-answer:v1:retry", at(10_007))
            .await?;
        assert!(
            !store
                .mark_delivered_by_batch("dialog-answer:v1:failed", at(10_008))
                .await?
        );
        store
            .mark_delivered_by_batch("dialog-answer:v1:retry", at(10_008))
            .await?;
        let shown_after_delivery: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM gradius_ad_opportunities WHERE user_id = $1 AND shown_at IS NOT NULL",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(shown_after_delivery, 1);
        assert_eq!(
            store.reserve_opportunity(third).await?,
            GradiusAdReservation::Replay {
                opportunity_id,
                ad: GradiusStoredAd {
                    shown_at: Some(at(10_008)),
                    ..stored
                },
            }
        );

        let other_surface = store
            .reserve_opportunity(GradiusAdOpportunityInput {
                opportunity_key: "generation-job:9820825200".to_owned(),
                attempt_key: "generation-claim-1".to_owned(),
                dialog_job_id: Some(9_820_825_200),
                integration_kind: "native_generation".to_owned(),
                user_id,
                chat_id: user_id,
                thread_id: 0,
                model_version: None,
                completed_at: at(10_006),
            })
            .await?;
        assert!(matches!(
            other_surface,
            GradiusAdReservation::Ineligible {
                reason: GradiusAdIneligibility::InteractionThreshold,
                ..
            }
        ));

        sqlx::query("DELETE FROM gradius_ad_opportunities WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await?;
        Ok(())
    }
}
