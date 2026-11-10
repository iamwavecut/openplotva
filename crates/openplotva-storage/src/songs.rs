//! Generated song material, prepared payloads, and confirmed delivery links.

use crate::StorageError;
use crate::database as sqlx;
use sqlx::{PgPool, Row, postgres::PgRow};
use time::OffsetDateTime;

/// One generated song's material, persisted for tracing.
#[derive(Clone, Debug, PartialEq)]
pub struct GeneratedSongRecord {
    pub prepared_delivery: Option<serde_json::Value>,
    pub id: i64,
    pub job_id: Option<i64>,
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    pub user_id: i64,
    pub user_full_name: String,
    pub trigger_message_id: i32,
    pub result_message_id: Option<i32>,
    pub request_text: String,
    pub topic: String,
    pub title: String,
    pub vocal_language: String,
    pub vocals: String,
    pub tags: String,
    pub style_summary: String,
    pub lyrics: String,
    pub duration_seconds: i32,
    pub brief: serde_json::Value,
    pub seed: Option<i64>,
    pub audio_seconds: Option<f32>,
    pub created_at: OffsetDateTime,
}

/// Insert payload for [`PostgresGeneratedSongStore::save_prepared`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NewGeneratedSong {
    pub job_id: Option<i64>,
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    pub user_id: i64,
    pub user_full_name: String,
    pub trigger_message_id: i32,
    pub request_text: String,
    pub topic: String,
    pub title: String,
    pub vocal_language: String,
    pub vocals: String,
    pub tags: String,
    pub style_summary: String,
    pub lyrics: String,
    pub duration_seconds: i32,
    pub brief: serde_json::Value,
    pub seed: Option<i64>,
    pub audio_seconds: Option<f32>,
}

const SQL_INSERT_GENERATED_SONG: &str = "INSERT INTO generated_songs (job_id, chat_id, thread_id, user_id, user_full_name, trigger_message_id, request_text, topic, title, vocal_language, vocals, tags, style_summary, lyrics, duration_seconds, brief, seed, audio_seconds, prepared_delivery) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) ON CONFLICT (job_id) WHERE job_id IS NOT NULL AND prepared_delivery IS NOT NULL DO UPDATE SET job_id = EXCLUDED.job_id WHERE generated_songs.chat_id = EXCLUDED.chat_id AND generated_songs.thread_id IS NOT DISTINCT FROM EXCLUDED.thread_id AND generated_songs.user_id = EXCLUDED.user_id AND generated_songs.trigger_message_id = EXCLUDED.trigger_message_id RETURNING prepared_delivery";

const SQL_GET_GENERATED_SONG: &str = "SELECT id, prepared_delivery, job_id, chat_id, thread_id, user_id, user_full_name, trigger_message_id, result_message_id, request_text, topic, title, vocal_language, vocals, tags, style_summary, lyrics, duration_seconds, brief, seed, audio_seconds, created_at FROM generated_songs WHERE id = $1";

const SQL_SET_GENERATED_SONG_RESULT_MESSAGE: &str =
    "UPDATE generated_songs SET result_message_id = $2 WHERE id = $1";

/// Postgres store for generated song material.
#[derive(Clone, Debug)]
pub struct PostgresGeneratedSongStore {
    pool: PgPool,
}

impl PostgresGeneratedSongStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Save material and its prepared payload together; a replay keeps the first preparation.
    pub async fn save_prepared(
        &self,
        song: &NewGeneratedSong,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, StorageError> {
        if song.job_id.is_none() {
            return Err(StorageError::GeneratedSongDelivery {
                job_id: 0,
                reason: "preparation requires a job identity",
            });
        }
        let row = sqlx::query(SQL_INSERT_GENERATED_SONG)
            .bind(song.job_id)
            .bind(song.chat_id)
            .bind(song.thread_id)
            .bind(song.user_id)
            .bind(&song.user_full_name)
            .bind(song.trigger_message_id)
            .bind(&song.request_text)
            .bind(&song.topic)
            .bind(&song.title)
            .bind(&song.vocal_language)
            .bind(&song.vocals)
            .bind(&song.tags)
            .bind(&song.style_summary)
            .bind(&song.lyrics)
            .bind(song.duration_seconds)
            .bind(&song.brief)
            .bind(song.seed)
            .bind(song.audio_seconds)
            .bind(payload)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get("prepared_delivery")?)
    }

    pub async fn prepared_for_job(
        &self,
        job_id: i64,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        Ok(sqlx::query_scalar("SELECT prepared_delivery FROM generated_songs WHERE job_id = $1 AND prepared_delivery IS NOT NULL")
            .bind(job_id).fetch_optional(&self.pool).await?)
    }

    /// Commit real Telegram IDs before taskman can report successful music delivery.
    pub async fn link_delivery_receipts(
        &self,
        job_id: i64,
        batch_id: &str,
    ) -> Result<i32, StorageError> {
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query("SELECT song.id, operation.state, operation.telegram_message_ids[1] AS message_id FROM telegram_outbox AS operation LEFT JOIN generated_songs AS song ON song.job_id = operation.dialog_job_id AND song.prepared_delivery IS NOT NULL WHERE operation.batch_id = $1 AND operation.dialog_job_id = $2 ORDER BY operation.part_index")
            .bind(batch_id).bind(job_id).fetch_all(&mut *tx).await?;
        if rows.len() != 1 {
            return Err(StorageError::GeneratedSongDelivery {
                job_id,
                reason: "music delivery must contain one prepared variant",
            });
        }
        let row = &rows[0];
        let id: Option<i64> = row.try_get("id")?;
        let message_id: Option<i64> = row.try_get("message_id")?;
        let state: String = row.try_get("state")?;
        let Some((id, message_id)) = id.zip(
            message_id
                .and_then(|id| i32::try_from(id).ok())
                .filter(|id| *id > 0),
        ) else {
            return Err(StorageError::GeneratedSongDelivery {
                job_id,
                reason: "delivery has no song or real message receipt",
            });
        };
        if state != "delivered" {
            return Err(StorageError::GeneratedSongDelivery {
                job_id,
                reason: "delivery is not confirmed",
            });
        }
        sqlx::query(SQL_SET_GENERATED_SONG_RESULT_MESSAGE)
            .bind(id)
            .bind(message_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(message_id)
    }

    pub async fn get(&self, id: i64) -> Result<Option<GeneratedSongRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_GENERATED_SONG)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(generated_song_from_row).transpose()
    }
}

fn generated_song_from_row(row: PgRow) -> Result<GeneratedSongRecord, StorageError> {
    Ok(GeneratedSongRecord {
        prepared_delivery: row.try_get("prepared_delivery")?,
        id: row.try_get("id")?,
        job_id: row.try_get("job_id")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        user_id: row.try_get("user_id")?,
        user_full_name: row.try_get("user_full_name")?,
        trigger_message_id: row.try_get("trigger_message_id")?,
        result_message_id: row.try_get("result_message_id")?,
        request_text: row.try_get("request_text")?,
        topic: row.try_get("topic")?,
        title: row.try_get("title")?,
        vocal_language: row.try_get("vocal_language")?,
        vocals: row.try_get("vocals")?,
        tags: row.try_get("tags")?,
        style_summary: row.try_get("style_summary")?,
        lyrics: row.try_get("lyrics")?,
        duration_seconds: row.try_get("duration_seconds")?,
        brief: row.try_get("brief")?,
        seed: row.try_get("seed")?,
        audio_seconds: row.try_get("audio_seconds")?,
        created_at: row.try_get("created_at")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database as sqlx;
    use serde_json::json;

    #[tokio::test]
    async fn postgres_prepared_song_replay_preserves_payload_metadata_and_legacy_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(dsn) = std::env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&dsn)
            .await?;
        crate::run_migrations_on(&pool).await?;
        let job_id = OffsetDateTime::now_utc().unix_timestamp_nanos() as i64;
        let legacy_id: i64 = sqlx::query_scalar("INSERT INTO generated_songs (job_id, chat_id, user_id, result_message_id) VALUES ($1, 42, 7, 55) RETURNING id")
            .bind(job_id).fetch_one(&pool).await?;
        let store = PostgresGeneratedSongStore::new(pool.clone());
        assert_eq!(store.prepared_for_job(job_id).await?, None);
        let mut song = NewGeneratedSong {
            job_id: Some(job_id),
            chat_id: 42,
            user_id: 7,
            title: "Original".into(),
            seed: Some(123),
            ..Default::default()
        };
        let original = json!({"html":"<audio src=\"https://example.test/original.mp3\"></audio>","chat_id":42});
        assert_eq!(store.save_prepared(&song, &original).await?, original);
        song.title = "Regenerated".into();
        song.seed = Some(456);
        assert_eq!(
            store
                .save_prepared(&song, &json!({"html":"changed"}))
                .await?,
            original
        );
        let saved: (String, Option<i64>, Option<i32>) = sqlx::query_as("SELECT title, seed, result_message_id FROM generated_songs WHERE job_id = $1 AND prepared_delivery IS NOT NULL")
            .bind(job_id).fetch_one(&pool).await?;
        assert_eq!(saved, ("Original".into(), Some(123), None));
        song.chat_id = 99;
        assert!(store.save_prepared(&song, &original).await.is_err());
        assert!(
            store
                .link_delivery_receipts(job_id, "missing-batch")
                .await
                .is_err()
        );
        let legacy = store.get(legacy_id).await?.expect("legacy song");
        assert_eq!(legacy.prepared_delivery, None);
        assert_eq!(legacy.result_message_id, Some(55));
        sqlx::query("DELETE FROM generated_songs WHERE job_id = $1")
            .bind(job_id)
            .execute(&pool)
            .await?;
        Ok(())
    }
}
