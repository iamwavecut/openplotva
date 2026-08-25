//! Privacy-safe Gradius audit reader shared by the runtime API and admin UI.

use openplotva_server::{
    RuntimeGradiusAuditReader, RuntimeGradiusAuditReaderFuture, RuntimeGradiusOpportunitiesFilter,
    RuntimeGradiusSummaryFilter,
};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Debug)]
pub struct PostgresRuntimeGradiusAuditReader {
    pool: PgPool,
}

impl PostgresRuntimeGradiusAuditReader {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn opportunities(
        &self,
        filter: RuntimeGradiusOpportunitiesFilter,
    ) -> Result<Value, String> {
        let cutoff = range_cutoff(&filter.range)?;
        let mut query = QueryBuilder::<Postgres>::new(
            r#"SELECT o.*,
                      COUNT(*) OVER() AS filtered_count,
                      COALESCE((
                          SELECT jsonb_agg(jsonb_build_object(
                              'id', c.id,
                              'attempt_generation', c.attempt_generation,
                              'sequence', c.sequence,
                              'role', c.role,
                              'synthetic_chat_id', c.synthetic_chat_id,
                              'synthetic_user_id', c.synthetic_user_id,
                              'endpoint', c.endpoint,
                              'request_body', c.request_body,
                              'response_status', c.response_status,
                              'response_body', c.response_body,
                              'response_json', c.response_json,
                              'response_truncated', c.response_truncated,
                              'duration_ms', c.duration_ms,
                              'outcome', c.outcome,
                              'error', c.error,
                              'created_at', to_char(c.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
                          ) ORDER BY c.attempt_generation, c.sequence)
                          FROM gradius_api_calls c
                          WHERE c.opportunity_id = o.id),
                          '[]'::jsonb
                      ) AS api_calls
               FROM gradius_ad_opportunities o
               WHERE TRUE"#,
        );
        if let Some(cutoff) = cutoff {
            query.push(" AND o.completed_at >= ").push_bind(cutoff);
        }
        push_text_filter(&mut query, "o.integration_kind", &filter.integration_kind);
        push_text_filter(&mut query, "o.outcome", &filter.outcome);
        push_text_filter(&mut query, "o.delivery_state", &filter.delivery_state);
        push_text_filter(&mut query, "o.model_version", &filter.model);
        if let Some(user_id) = filter.user_id {
            query.push(" AND o.user_id = ").push_bind(user_id);
        }
        if let Some(chat_id) = filter.chat_id {
            query.push(" AND o.chat_id = ").push_bind(chat_id);
        }
        if let Some(dialog_job_id) = filter.dialog_job_id {
            query
                .push(" AND o.dialog_job_id = ")
                .push_bind(dialog_job_id);
        }
        if !filter.q.trim().is_empty() {
            let q = format!("%{}%", filter.q.trim());
            query
                .push(" AND (o.opportunity_key ILIKE ")
                .push_bind(q.clone())
                .push(" OR o.user_id::text ILIKE ")
                .push_bind(q.clone())
                .push(" OR o.chat_id::text ILIKE ")
                .push_bind(q.clone())
                .push(" OR COALESCE(o.dialog_job_id::text, '') ILIKE ")
                .push_bind(q.clone())
                .push(" OR COALESCE(o.ad_markdown, '') ILIKE ")
                .push_bind(q.clone())
                .push(" OR EXISTS (SELECT 1 FROM gradius_api_calls qc WHERE qc.opportunity_id = o.id AND (qc.request_body::text ILIKE ")
                .push_bind(q.clone())
                .push(" OR COALESCE(qc.response_body, '') ILIKE ")
                .push_bind(q)
                .push(")))");
        }
        query
            .push(" ORDER BY o.completed_at DESC, o.id DESC LIMIT ")
            .push_bind(filter.limit)
            .push(" OFFSET ")
            .push_bind(filter.offset);

        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(error_text)?;
        let total = rows
            .first()
            .map_or(0, |row| row.get::<i64, _>("filtered_count"));
        let items = rows
            .iter()
            .map(opportunity_json)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(json!({
            "count": total,
            "offset": filter.offset,
            "limit": filter.limit,
            "items": items,
        }))
    }

    async fn summary(&self, filter: RuntimeGradiusSummaryFilter) -> Result<Value, String> {
        let cutoff = range_cutoff(&filter.range)?;
        let mut query = QueryBuilder::<Postgres>::new(
            r#"SELECT
                   COALESCE(SUM(attempt_generation), 0)::BIGINT AS attempts,
                   COUNT(*) FILTER (WHERE provider_outcome = 'ad')::BIGINT AS returned,
                   COUNT(*) FILTER (WHERE outcome = 'no_ad')::BIGINT AS no_ad,
                   COUNT(*) FILTER (WHERE delivery_state = 'queued')::BIGINT AS queued,
                   COUNT(*) FILTER (WHERE delivery_state = 'delivered')::BIGINT AS delivered,
                   COUNT(*) FILTER (WHERE delivery_state = 'failed')::BIGINT AS failed,
                   COUNT(*) FILTER (WHERE outcome IN ('provider_error', 'privacy_error', 'render_error'))::BIGINT AS errors,
                   COUNT(*) FILTER (WHERE provider_outcome = 'ad' AND delivery_state IS DISTINCT FROM 'delivered')::BIGINT AS returned_not_delivered,
                   COALESCE(SUM(show_price) FILTER (WHERE delivery_state = 'delivered'), 0)::DOUBLE PRECISION AS confirmed_show_price
               FROM gradius_ad_opportunities
               WHERE TRUE"#,
        );
        if let Some(cutoff) = cutoff {
            query.push(" AND completed_at >= ").push_bind(cutoff);
        }
        push_text_filter(&mut query, "integration_kind", &filter.integration_kind);
        let row = query
            .build()
            .fetch_one(&self.pool)
            .await
            .map_err(error_text)?;
        let attempts = row.get::<i64, _>("attempts");
        let returned = row.get::<i64, _>("returned");
        let fill_rate = if attempts == 0 {
            0.0
        } else {
            returned as f64 / attempts as f64
        };
        Ok(json!({
            "range": normalized_range(&filter.range)?,
            "integration_kind": optional_text(&filter.integration_kind),
            "attempts": attempts,
            "fill_rate": fill_rate,
            "returned": returned,
            "no_ad": row.get::<i64, _>("no_ad"),
            "queued": row.get::<i64, _>("queued"),
            "delivered": row.get::<i64, _>("delivered"),
            "failed": row.get::<i64, _>("failed"),
            "errors": row.get::<i64, _>("errors"),
            "returned_not_delivered": row.get::<i64, _>("returned_not_delivered"),
            "confirmed_show_price_rub": row.get::<f64, _>("confirmed_show_price"),
            "click_reporting": "provider_only",
        }))
    }
}

impl RuntimeGradiusAuditReader for PostgresRuntimeGradiusAuditReader {
    fn gradius_ad_opportunities<'a>(
        &'a self,
        filter: RuntimeGradiusOpportunitiesFilter,
    ) -> RuntimeGradiusAuditReaderFuture<'a> {
        Box::pin(async move { self.opportunities(filter).await })
    }

    fn gradius_ad_summary<'a>(
        &'a self,
        filter: RuntimeGradiusSummaryFilter,
    ) -> RuntimeGradiusAuditReaderFuture<'a> {
        Box::pin(async move { self.summary(filter).await })
    }
}

fn push_text_filter(query: &mut QueryBuilder<Postgres>, column: &str, value: &str) {
    if !value.trim().is_empty() {
        query
            .push(" AND ")
            .push(column)
            .push(" = ")
            .push_bind(value.trim().to_owned());
    }
}

fn opportunity_json(row: &sqlx::postgres::PgRow) -> Result<Value, String> {
    let selected_placement = row
        .get::<Option<sqlx::types::Json<Value>>, _>("selected_placement")
        .map(|value| value.0);
    let api_calls = row.get::<sqlx::types::Json<Value>, _>("api_calls").0;
    Ok(json!({
        "id": row.get::<i64, _>("id"),
        "opportunity_key": row.get::<String, _>("opportunity_key"),
        "source": row.get::<String, _>("source_kind"),
        "dialog_job_id": row.get::<Option<i64>, _>("dialog_job_id"),
        "integration_kind": row.get::<String, _>("integration_kind"),
        "user_id": row.get::<i64, _>("user_id"),
        "chat_id": row.get::<i64, _>("chat_id"),
        "thread_id": row.get::<i32, _>("thread_id"),
        "model_version": row.get::<Option<String>, _>("model_version"),
        "interaction_started_at": timestamp(row.get("interaction_started_at"))?,
        "completed_at": timestamp(row.get("completed_at"))?,
        "completed_answers": row.get::<i32, _>("completed_answers"),
        "outcome": row.get::<String, _>("outcome"),
        "ineligibility_reason": row.get::<Option<String>, _>("ineligibility_reason"),
        "attempt_reserved_at": optional_timestamp(row.get("attempt_reserved_at"))?,
        "attempt_generation": row.get::<i32, _>("attempt_generation"),
        "provider_outcome": row.get::<Option<String>, _>("provider_outcome"),
        "provider_completed_at": optional_timestamp(row.get("provider_completed_at"))?,
        "selected_placement": selected_placement,
        "provider_markdown": row.get::<Option<String>, _>("ad_markdown"),
        "telegram_html": row.get::<Option<String>, _>("rendered_html"),
        "insert_index": row.get::<Option<i32>, _>("insert_index"),
        "show_price": row.get::<Option<f64>, _>("show_price"),
        "click_price": row.get::<Option<f64>, _>("click_price"),
        "delivery_state": row.get::<Option<String>, _>("delivery_state"),
        "telegram_outbox_batch_id": row.get::<Option<String>, _>("outbox_batch_id"),
        "prepared_at": optional_timestamp(row.get("prepared_at"))?,
        "queued_at": optional_timestamp(row.get("queued_at"))?,
        "delivered_at": optional_timestamp(row.get("delivered_at"))?,
        "delivery_failed_at": optional_timestamp(row.get("delivery_failed_at"))?,
        "delivery_error": row.get::<Option<String>, _>("delivery_error"),
        "shown_at": optional_timestamp(row.get("shown_at"))?,
        "llm_trace_job_id": row.get::<Option<i64>, _>("dialog_job_id"),
        "api_calls": api_calls,
        "returned_not_delivered": row.get::<Option<String>, _>("provider_outcome").as_deref() == Some("ad")
            && row.get::<Option<String>, _>("delivery_state").as_deref() != Some("delivered"),
    }))
}

fn range_cutoff(value: &str) -> Result<Option<OffsetDateTime>, String> {
    let duration = match normalized_range(value)? {
        "1h" => Some(Duration::hours(1)),
        "24h" => Some(Duration::hours(24)),
        "7d" => Some(Duration::days(7)),
        "30d" => Some(Duration::days(30)),
        "all" => None,
        _ => unreachable!("normalized Gradius range"),
    };
    Ok(duration.map(|duration| OffsetDateTime::now_utc() - duration))
}

fn normalized_range(value: &str) -> Result<&'static str, String> {
    match value.trim() {
        "" | "24h" => Ok("24h"),
        "1h" => Ok("1h"),
        "7d" => Ok("7d"),
        "30d" => Ok("30d"),
        "all" => Ok("all"),
        _ => Err("invalid Gradius range".to_owned()),
    }
}

fn optional_text(value: &str) -> Option<&str> {
    (!value.trim().is_empty()).then(|| value.trim())
}

fn timestamp(value: OffsetDateTime) -> Result<String, String> {
    value.format(&Rfc3339).map_err(error_text)
}

fn optional_timestamp(value: Option<OffsetDateTime>) -> Result<Option<String>, String> {
    value.map(timestamp).transpose()
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error};

    use openplotva_storage::gradius_ads::{
        GradiusAdOpportunityInput, GradiusAdReservation, GradiusApiCallRecord, GradiusStoredAd,
        PostgresGradiusAdStore,
    };
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[test]
    fn supported_ranges_are_bounded_and_invalid_ranges_fail() {
        assert!(range_cutoff("").expect("default range").is_some());
        assert!(range_cutoff("30d").expect("30 day range").is_some());
        assert_eq!(range_cutoff("all").expect("all range"), None);
        assert_eq!(
            range_cutoff("forever"),
            Err("invalid Gradius range".to_owned())
        );
    }

    #[tokio::test]
    async fn postgres_reader_filters_audit_and_sums_only_delivered_price()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await?;
        openplotva_storage::run_migrations_on(&pool).await?;
        let user_id = 9_825_250_001_i64;
        sqlx::query("DELETE FROM gradius_ad_opportunities WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await?;
        let store = PostgresGradiusAdStore::new(pool.clone());
        let now = OffsetDateTime::now_utc();
        for answer in 1..=3_i64 {
            let reservation = store
                .reserve_opportunity(GradiusAdOpportunityInput {
                    opportunity_key: format!("dialog-job:{}", 9_825_250_100 + answer),
                    attempt_key: format!("audit-test-claim-{answer}"),
                    dialog_job_id: Some(9_825_250_100 + answer),
                    integration_kind: "native_dialogue".to_owned(),
                    user_id,
                    chat_id: user_id,
                    thread_id: 0,
                    model_version: Some("audit-model".to_owned()),
                    completed_at: now + Duration::seconds(answer),
                })
                .await?;
            if answer == 3 {
                let opportunity_id = reservation.opportunity_id();
                let GradiusAdReservation::Reserved {
                    attempt_generation, ..
                } = reservation
                else {
                    panic!("third answer must reserve a Gradius opportunity");
                };
                store
                    .record_api_call(GradiusApiCallRecord {
                        opportunity_id,
                        attempt_generation,
                        sequence: 1,
                        role: Some("user".to_owned()),
                        synthetic_chat_id: "chat_safe".to_owned(),
                        synthetic_user_id: "user_safe".to_owned(),
                        endpoint: "https://api.example/dialogue".to_owned(),
                        request_body: json!({"text": "[private_email]"}),
                        response_status: Some(200),
                        response_body: Some("[]".to_owned()),
                        response_json: Some(json!([])),
                        response_truncated: false,
                        duration_ms: 10,
                        outcome: "no_ad".to_owned(),
                        error: None,
                        created_at: now,
                    })
                    .await?;
                store
                    .finish_ad(
                        opportunity_id,
                        attempt_generation,
                        GradiusStoredAd {
                            markdown: "**Ad**".to_owned(),
                            rendered_html: "<b>Ad</b>".to_owned(),
                            selected_placement: json!({"type": "native-text-ad"}),
                            insert_index: Some(10),
                            show_price: Some(1.25),
                            click_price: None,
                            prepared_at: now,
                            shown_at: None,
                        },
                    )
                    .await?;
                store
                    .mark_queued(opportunity_id, "batch-audit", now)
                    .await?;
                store.mark_delivered_by_batch("batch-audit", now).await?;
            }
        }

        let reader = PostgresRuntimeGradiusAuditReader::new(pool.clone());
        let opportunities = reader
            .gradius_ad_opportunities(RuntimeGradiusOpportunitiesFilter {
                range: "all".to_owned(),
                integration_kind: "native_dialogue".to_owned(),
                outcome: "ad".to_owned(),
                delivery_state: "delivered".to_owned(),
                user_id: Some(user_id),
                model: "audit-model".to_owned(),
                q: "private_email".to_owned(),
                limit: 10,
                ..RuntimeGradiusOpportunitiesFilter::default()
            })
            .await?;
        assert_eq!(opportunities["count"], 1);
        assert_eq!(opportunities["items"][0]["attempt_generation"], 1);
        assert_eq!(
            opportunities["items"][0]["api_calls"][0]["attempt_generation"],
            1
        );
        assert_eq!(opportunities["items"][0]["api_calls"][0]["role"], "user");
        assert_eq!(opportunities["items"][0]["telegram_html"], "<b>Ad</b>");

        let summary = reader
            .gradius_ad_summary(RuntimeGradiusSummaryFilter {
                range: "all".to_owned(),
                integration_kind: "native_dialogue".to_owned(),
            })
            .await?;
        assert_eq!(summary["attempts"], 1);
        assert_eq!(summary["delivered"], 1);
        assert_eq!(summary["confirmed_show_price_rub"], 1.25);

        sqlx::query("DELETE FROM gradius_ad_opportunities WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await?;
        Ok(())
    }
}
