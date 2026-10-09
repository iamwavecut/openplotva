//! Caller-scoped queries used by dialog tools.
use crate::{PostgresHistoryStore, PostgresMemoryStore, StorageError};
use serde_json::{Value, json};
use sqlx::Row;

impl PostgresHistoryStore {
    pub async fn agent_messages_since(
        &self,
        chat_id: i64,
        thread_id: i32,
        after: i32,
    ) -> Result<Vec<Value>, StorageError> {
        let rows = sqlx::query("SELECT message_id, sender_id, payload FROM chat_history_entries WHERE chat_id=$1 AND thread_id=$2 AND message_id>$3 AND kind='text' AND occurred_at > COALESCE((SELECT max(reset_at) FROM chat_history_resets r WHERE r.chat_id=chat_history_entries.chat_id AND (r.thread_id=0 OR r.thread_id=chat_history_entries.thread_id)), '-infinity') ORDER BY message_id ASC LIMIT 100")
            .bind(chat_id).bind(thread_id).bind(after).fetch_all(&self.pool).await?;
        rows.into_iter().map(|row| Ok(json!({"message_id":row.try_get::<i32,_>("message_id")?,"user_id":row.try_get::<i64,_>("sender_id")?,"message":row.try_get::<Value,_>("payload")?}))).collect()
    }

    /// Read retained text messages in one topic, respecting history resets.
    pub async fn agent_messages(
        &self,
        chat_id: i64,
        thread_id: i32,
        ids: &[i32],
        query: &str,
        author_id: i64,
    ) -> Result<Vec<Value>, StorageError> {
        let rows = sqlx::query(r#"SELECT message_id, sender_id, occurred_at, payload
            FROM chat_history_entries e
            WHERE chat_id = $1 AND thread_id = $2 AND kind = 'text'
              AND occurred_at > COALESCE((SELECT max(reset_at) FROM chat_history_resets r
                  WHERE r.chat_id = e.chat_id AND (r.thread_id = 0 OR r.thread_id = e.thread_id)), '-infinity')
              AND (cardinality($3::integer[]) = 0 OR message_id = ANY($3))
              AND ($4 = '' OR payload::text ILIKE '%' || $4 || '%')
              AND ($5::bigint = 0 OR sender_id = $5)
            ORDER BY occurred_at DESC, message_id DESC LIMIT 40"#)
            .bind(chat_id).bind(thread_id).bind(ids).bind(query).bind(author_id)
            .fetch_all(&self.pool).await?;
        rows.into_iter().map(|row| Ok(json!({
            "message_id": row.try_get::<i32,_>("message_id")?,
            "user_id": row.try_get::<i64,_>("sender_id")?,
            "timestamp": row.try_get::<time::OffsetDateTime,_>("occurred_at")?.unix_timestamp(),
            "message": row.try_get::<Value,_>("payload")?,
        }))).collect()
    }

    pub async fn agent_message_neighbors(
        &self,
        chat_id: i64,
        thread_id: i32,
        ids: &[i32],
    ) -> Result<Vec<Value>, StorageError> {
        let rows = sqlx::query("SELECT DISTINCT message_id FROM unnest($3::integer[]) AS requested(id) CROSS JOIN LATERAL ((SELECT message_id FROM chat_history_entries WHERE chat_id=$1 AND thread_id=$2 AND kind='text' AND message_id<requested.id ORDER BY message_id DESC LIMIT 2) UNION (SELECT message_id FROM chat_history_entries WHERE chat_id=$1 AND thread_id=$2 AND kind='text' AND message_id>requested.id ORDER BY message_id ASC LIMIT 2)) AS neighbors LIMIT 40")
            .bind(chat_id).bind(thread_id).bind(ids).fetch_all(&self.pool).await?;
        let ids = rows
            .into_iter()
            .map(|row| row.try_get::<i32, _>("message_id"))
            .collect::<Result<Vec<_>, _>>()?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.agent_messages(chat_id, thread_id, &ids, "", 0).await
    }

    pub async fn agent_job_status(
        &self,
        chat_id: i64,
        user_id: i64,
        job_id: i64,
    ) -> Result<Option<Value>, StorageError> {
        let row = sqlx::query("SELECT id, status, job_type FROM taskman_jobs WHERE id=$1 AND chat_id=$2 AND user_id=$3 AND deleted_at IS NULL")
            .bind(job_id).bind(chat_id).bind(user_id).fetch_optional(&self.pool).await?;
        row.map(|row| Ok(json!({"job_id":row.try_get::<i64,_>("id")?, "status":row.try_get::<String,_>("status")?, "kind":row.try_get::<String,_>("job_type")?}))).transpose()
    }
}

impl PostgresMemoryStore {
    pub async fn agent_own_memory(&self, user_id: i64) -> Result<Vec<Value>, StorageError> {
        let rows = sqlx::query("SELECT id, fact_text FROM memory_cards WHERE user_id=$1 AND user_id<>0 AND status IN ('active','competing') AND valid_until IS NULL AND (expires_at IS NULL OR expires_at>now()) ORDER BY updated_at DESC LIMIT 100")
            .bind(user_id).fetch_all(&self.pool).await?;
        rows.into_iter().map(|row| Ok(json!({"id":row.try_get::<i64,_>("id")?,"text":row.try_get::<String,_>("fact_text")?,"user_id":user_id,"scope":"self_global"}))).collect()
    }

    /// Restrict mutations at the SQL boundary, including concurrent changes.
    pub async fn agent_change_memory(
        &self,
        scope: &openplotva_memory::RetrievalScope,
        card_id: i64,
        action: &str,
        text: &str,
        memory_scope: &str,
    ) -> Result<u64, StorageError> {
        let (chat_id, thread_id, user_id) = (scope.chat_id, scope.thread_id, scope.user_id);
        let global = memory_scope == "self_global";
        if action == "update" {
            let mut tx = self.pool.begin().await?;
            let row = sqlx::query("SELECT visibility, chat_id, thread_id, user_id, subject, predicate FROM memory_cards WHERE id=$1 AND chat_id=$2 AND (thread_id=0 OR thread_id=$3) AND ((user_id=$4 AND $5='self') OR (user_id=0 AND visibility IN ('chat','thread') AND $5='chat')) AND status IN ('active','competing') FOR UPDATE")
                .bind(card_id).bind(chat_id).bind(thread_id).bind(user_id).bind(memory_scope).fetch_optional(&mut *tx).await?;
            let Some(row) = row else { return Ok(0) };
            let card = openplotva_memory::CardInput {
                subject: row.try_get("subject")?,
                predicate: row.try_get("predicate")?,
                object: text.into(),
                fact_text: text.into(),
                ..Default::default()
            };
            let hash = super::memory_card_dedup_hash(
                &row.try_get::<String, _>("visibility")?,
                row.try_get("chat_id")?,
                row.try_get("thread_id")?,
                row.try_get("user_id")?,
                &card,
            );
            let changed = sqlx::query("UPDATE memory_cards SET fact_text=$2, object=$2, dedup_hash=$3, embedding=NULL, updated_at=now() WHERE id=$1")
                .bind(card_id).bind(text).bind(hash).execute(&mut *tx).await?.rows_affected();
            tx.commit().await?;
            return Ok(changed);
        }
        let query = match action {
            "forget" if global => {
                "UPDATE memory_cards SET status='deleted', deleted_at=now(), retracted_at=now(), deleted_by_user_id=$3, updated_at=now() WHERE user_id=$3 AND user_id<>0 AND status IN ('active','competing') AND fact_text=(SELECT fact_text FROM memory_cards WHERE id=$4 AND user_id=$3) AND $1::bigint=$3 AND $2::integer=0 AND $5::text='' AND $6::text='self_global'"
            }
            "forget" => {
                "UPDATE memory_cards SET status='deleted', deleted_at=now(), retracted_at=now(), deleted_by_user_id=$3, updated_at=now() WHERE id=$4 AND chat_id=$1 AND (thread_id=0 OR thread_id=$2) AND ((user_id=$3 AND $6='self') OR (user_id=0 AND visibility IN ('chat','thread') AND $6='chat')) AND status IN ('active','competing') AND $5::text=''"
            }
            _ => return Ok(0),
        };
        let result = sqlx::query(query)
            .bind(chat_id)
            .bind(thread_id)
            .bind(user_id)
            .bind(card_id)
            .bind(text)
            .bind(memory_scope)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn live_agent_tools_enforce_owner_topic_reset_and_memory_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(dsn) = std::env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await?;
        crate::run_migrations_on(&pool).await?;
        let now = time::OffsetDateTime::now_utc();
        let suffix = now.unix_timestamp_nanos() as i64 % 100_000_000;
        let chat_id = -7_000_000_000 - suffix;
        let user_id = 7_000_000_000 + suffix;
        let memory = PostgresMemoryStore::new(pool.clone());
        let scope = openplotva_memory::RetrievalScope {
            chat_id,
            thread_id: 7,
            user_id,
            chat_type: "supergroup".into(),
            ..Default::default()
        };
        let card = openplotva_memory::CardInput {
            observation_scope: openplotva_memory::ObservationScope {
                chat_id,
                thread_id: 7,
                user_id,
                kind: "user".into(),
                chat_type: "supergroup".into(),
                ..Default::default()
            },
            card_type: "technical_fact".into(),
            subject: format!("user:{user_id}"),
            predicate: "fact".into(),
            object: "Uses Rust".into(),
            fact_text: "Uses Rust".into(),
            observed_at: now,
            valid_from: now,
            ..Default::default()
        };
        let (_, ids) = memory
            .upsert_cards_lexical(std::slice::from_ref(&card))
            .await?;
        let id = ids[0];
        let other = openplotva_memory::RetrievalScope {
            user_id: user_id + 1,
            ..scope.clone()
        };
        assert_eq!(
            memory
                .agent_change_memory(&other, id, "forget", "", "self")
                .await?,
            0
        );
        assert_eq!(
            memory
                .agent_change_memory(&scope, id, "update", "Uses Go", "chat")
                .await?,
            0
        );
        assert_eq!(
            memory
                .agent_change_memory(&scope, id, "update", "Uses Go", "self")
                .await?,
            1
        );
        let updated = openplotva_memory::CardInput {
            object: "Uses Go".into(),
            fact_text: "Uses Go".into(),
            ..card
        };
        let (_, duplicate) = memory
            .upsert_cards_lexical(std::slice::from_ref(&updated))
            .await?;
        assert_eq!(
            duplicate,
            vec![id],
            "updates preserve canonical deduplication"
        );
        let second = openplotva_memory::CardInput {
            observation_scope: openplotva_memory::ObservationScope {
                chat_id: chat_id - 1,
                ..updated.observation_scope.clone()
            },
            ..updated
        };
        memory.upsert_cards_lexical(&[second]).await?;
        assert_eq!(
            memory
                .agent_change_memory(&scope, id, "forget", "", "self_global")
                .await?,
            0
        );
        let private = openplotva_memory::RetrievalScope {
            chat_id: user_id,
            thread_id: 0,
            ..scope.clone()
        };
        assert_eq!(memory.agent_own_memory(user_id).await?.len(), 2);
        assert_eq!(
            memory
                .agent_change_memory(&private, id, "forget", "", "self_global")
                .await?,
            2
        );
        let history = PostgresHistoryStore::new(pool.clone());
        // Use the store's partition creation path before inserting synthetic retained messages.
        sqlx::query("SELECT ensure_chat_history_partition(CURRENT_DATE)")
            .execute(&pool)
            .await?;
        for (message_id, thread_id, sender_id) in
            [(1, 7, user_id), (2, 8, user_id), (3, 7, user_id + 1)]
        {
            sqlx::query("INSERT INTO chat_history_entries (bucket_day,chat_id,thread_id,message_id,entry_id,kind,role,occurred_at,sender_id,payload) VALUES (CURRENT_DATE,$1,$2,$3,$4,'text','user',now(),$5,'{\"text\":\"link context\"}')")
                .bind(chat_id).bind(thread_id).bind(message_id).bind(format!("agent-test-{message_id}")).bind(sender_id).execute(&pool).await?;
        }
        assert_eq!(
            history
                .agent_messages(chat_id, 7, &[], "link", user_id)
                .await?
                .len(),
            1
        );
        assert_eq!(history.agent_messages_since(chat_id, 7, 0).await?.len(), 2);
        assert_eq!(
            history
                .agent_message_neighbors(chat_id, 7, &[1])
                .await?
                .len(),
            1
        );
        assert!(
            history
                .agent_messages(chat_id, 7, &[2], "", 0)
                .await?
                .is_empty()
        );
        assert!(
            history
                .agent_job_status(chat_id, user_id, -1)
                .await?
                .is_none()
        );
        sqlx::query(
            "INSERT INTO chat_history_resets (chat_id,thread_id,reset_at) VALUES ($1,7,now())",
        )
        .bind(chat_id)
        .execute(&pool)
        .await?;
        assert!(
            history
                .agent_messages_since(chat_id, 7, 0)
                .await?
                .is_empty()
        );
        assert!(
            history
                .agent_messages(chat_id, 7, &[], "", 0)
                .await?
                .is_empty()
        );
        for query in [
            "DELETE FROM chat_history_entries WHERE chat_id=$1 OR chat_id=$2",
            "DELETE FROM chat_history_resets WHERE chat_id=$1 OR chat_id=$2",
            "DELETE FROM memory_cards WHERE chat_id=$1 OR chat_id=$2",
        ] {
            sqlx::query(query)
                .bind(chat_id)
                .bind(chat_id - 1)
                .execute(&pool)
                .await?;
        }
        Ok(())
    }
}
