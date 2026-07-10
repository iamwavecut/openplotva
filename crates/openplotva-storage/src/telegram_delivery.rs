//! Durable Postgres boundary for materialized Telegram updates.
//!
//! Redis Streams own ingress buffering. This module atomically materializes one
//! whole Stream batch and provides the leased, fenced processing boundary used
//! after that commit.

use std::collections::HashMap;

use serde::de::DeserializeOwned;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};
use time::OffsetDateTime;

use crate::StorageError;

/// Conservative bind budget below PostgreSQL's protocol limit.
pub const POSTGRES_MATERIALIZATION_BIND_BUDGET: usize = 60_000;
/// Number of parameters emitted for one inbox row.
pub const MATERIALIZED_UPDATE_BINDS_PER_ROW: usize = 21;
/// Number of parameters emitted for one quarantine row.
pub const QUARANTINED_UPDATE_BINDS_PER_ROW: usize = 12;
/// Longest claim lifetime before another worker may reclaim an update.
pub const UPDATE_PROCESSING_LEASE_SECONDS: i64 = 90;

const SQL_INSERT_UPDATES_PREFIX: &str = "INSERT INTO telegram_update_inbox (\
    bot_id, update_id, schema_version, source, stream_ms, stream_seq, last_stream_ms, \
    last_stream_seq, raw_payload, payload_sha256, payload_conflict, update_type, \
    telegram_event_at, first_received_at, last_received_at, delivery_count, ordering_key, \
    priority, chat_id, thread_id, user_id)";

const SQL_INSERT_UPDATES_SUFFIX: &str = " ON CONFLICT (bot_id, update_id) DO UPDATE SET \
    delivery_count = telegram_update_inbox.delivery_count + CASE \
        WHEN (EXCLUDED.last_stream_ms, EXCLUDED.last_stream_seq) \
           > (telegram_update_inbox.last_stream_ms, telegram_update_inbox.last_stream_seq) \
        THEN EXCLUDED.delivery_count ELSE 0 END, \
    last_stream_ms = CASE \
        WHEN (EXCLUDED.last_stream_ms, EXCLUDED.last_stream_seq) \
           > (telegram_update_inbox.last_stream_ms, telegram_update_inbox.last_stream_seq) \
        THEN EXCLUDED.last_stream_ms ELSE telegram_update_inbox.last_stream_ms END, \
    last_stream_seq = CASE \
        WHEN (EXCLUDED.last_stream_ms, EXCLUDED.last_stream_seq) \
           > (telegram_update_inbox.last_stream_ms, telegram_update_inbox.last_stream_seq) \
        THEN EXCLUDED.last_stream_seq ELSE telegram_update_inbox.last_stream_seq END, \
    first_received_at = LEAST(telegram_update_inbox.first_received_at, EXCLUDED.first_received_at), \
    last_received_at = GREATEST(telegram_update_inbox.last_received_at, EXCLUDED.last_received_at), \
    payload_conflict = telegram_update_inbox.payload_conflict \
        OR EXCLUDED.payload_conflict \
        OR telegram_update_inbox.payload_sha256 <> EXCLUDED.payload_sha256, \
    updated_at = now() \
    RETURNING (xmax = 0) AS inserted, payload_conflict";

const SQL_INSERT_QUARANTINE_PREFIX: &str = "INSERT INTO telegram_update_quarantine (\
    bot_id, stream_ms, stream_seq, schema_version, source, raw_payload, payload_sha256, \
    first_received_at, last_received_at, delivery_count, error_class, error)";

const SQL_INSERT_QUARANTINE_SUFFIX: &str = " ON CONFLICT (bot_id, stream_ms, stream_seq) DO UPDATE SET \
    delivery_count = GREATEST(telegram_update_quarantine.delivery_count, EXCLUDED.delivery_count), \
    first_received_at = LEAST(telegram_update_quarantine.first_received_at, EXCLUDED.first_received_at), \
    last_received_at = GREATEST(telegram_update_quarantine.last_received_at, EXCLUDED.last_received_at), \
    updated_at = now()";

const SQL_CLAIM_UPDATES: &str = r#"
WITH candidates AS (
    SELECT inbox.id
    FROM telegram_update_inbox AS inbox
    WHERE (
        (inbox.status IN ('pending', 'retry_wait') AND inbox.available_at <= statement_timestamp())
        OR (
            inbox.status = 'processing'
            AND (inbox.leased_until IS NULL OR inbox.leased_until <= statement_timestamp())
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM telegram_update_inbox AS earlier
        WHERE earlier.bot_id = inbox.bot_id
          AND earlier.ordering_key = inbox.ordering_key
          AND (earlier.stream_ms, earlier.stream_seq, earlier.id)
              < (inbox.stream_ms, inbox.stream_seq, inbox.id)
          AND earlier.status IN ('pending', 'processing', 'retry_wait')
    )
    ORDER BY inbox.stream_ms, inbox.stream_seq, inbox.bot_id, inbox.id
    FOR UPDATE OF inbox SKIP LOCKED
    LIMIT $1
), claimed AS (
    UPDATE telegram_update_inbox AS inbox
    SET status = 'processing',
        attempt_count = inbox.attempt_count + 1,
        lease_token = inbox.lease_token + 1,
        lease_owner = $2,
        leased_until = statement_timestamp() + interval '90 seconds',
        processing_started_at = statement_timestamp(),
        updated_at = statement_timestamp()
    FROM candidates
    WHERE inbox.id = candidates.id
    RETURNING inbox.*
), attempts AS (
    INSERT INTO telegram_update_attempts (
        inbox_id, attempt, lease_token, worker_id, claimed_at,
        state_started_at, state_completed_at, handler_started_at
    )
    SELECT
        claimed.id,
        claimed.attempt_count,
        claimed.lease_token,
        $2,
        statement_timestamp(),
        CASE WHEN claimed.state_applied_at IS NULL THEN statement_timestamp() END,
        CASE WHEN claimed.state_applied_at IS NOT NULL THEN statement_timestamp() END,
        CASE WHEN claimed.state_applied_at IS NOT NULL THEN statement_timestamp() END
    FROM claimed
    RETURNING inbox_id
)
SELECT claimed.*
FROM claimed
JOIN attempts ON attempts.inbox_id = claimed.id
ORDER BY claimed.stream_ms, claimed.stream_seq, claimed.bot_id, claimed.id
"#;

const SQL_RENEW_UPDATE_LEASE: &str = r#"
UPDATE telegram_update_inbox
SET leased_until = statement_timestamp() + interval '90 seconds',
    updated_at = statement_timestamp()
WHERE id = $1
  AND lease_token = $2
  AND status = 'processing'
  AND leased_until > statement_timestamp()
RETURNING id
"#;

const SQL_MARK_STATE_APPLIED: &str = r#"
WITH checkpoint AS (
    UPDATE telegram_update_inbox
    SET state_applied_at = COALESCE(state_applied_at, statement_timestamp()),
        updated_at = statement_timestamp()
    WHERE id = $1
      AND lease_token = $2
      AND status = 'processing'
      AND leased_until > statement_timestamp()
    RETURNING id, attempt_count
), attempt_checkpoint AS (
    UPDATE telegram_update_attempts AS attempt
    SET state_completed_at = COALESCE(attempt.state_completed_at, statement_timestamp()),
        handler_started_at = COALESCE(attempt.handler_started_at, statement_timestamp())
    FROM checkpoint
    WHERE attempt.inbox_id = checkpoint.id
      AND attempt.attempt = checkpoint.attempt_count
    RETURNING attempt.inbox_id
)
SELECT EXISTS(SELECT 1 FROM attempt_checkpoint)
"#;

const SQL_COMPLETE_UPDATE: &str = r#"
WITH finished AS (
    UPDATE telegram_update_inbox
    SET status = 'completed',
        handler_completed_at = statement_timestamp(),
        completed_at = statement_timestamp(),
        outcome = 'handled',
        lease_owner = NULL,
        leased_until = NULL,
        updated_at = statement_timestamp()
    WHERE id = $1
      AND lease_token = $2
      AND status = 'processing'
      AND state_applied_at IS NOT NULL
      AND leased_until > statement_timestamp()
    RETURNING id, attempt_count
), attempt_finished AS (
    UPDATE telegram_update_attempts AS attempt
    SET handler_completed_at = statement_timestamp(),
        finished_at = statement_timestamp(),
        outcome = 'handled'
    FROM finished
    WHERE attempt.inbox_id = finished.id
      AND attempt.attempt = finished.attempt_count
    RETURNING attempt.inbox_id
)
SELECT EXISTS(SELECT 1 FROM attempt_finished)
"#;

const SQL_RETRY_UPDATE: &str = r#"
WITH finished AS (
    UPDATE telegram_update_inbox
    SET status = 'retry_wait',
        available_at = $3,
        outcome = 'retry',
        last_error_class = left($4, 128),
        last_error = left($5, 2048),
        lease_owner = NULL,
        leased_until = NULL,
        updated_at = statement_timestamp()
    WHERE id = $1
      AND lease_token = $2
      AND status = 'processing'
      AND leased_until > statement_timestamp()
    RETURNING id, attempt_count
), attempt_finished AS (
    UPDATE telegram_update_attempts AS attempt
    SET finished_at = statement_timestamp(),
        outcome = 'retry',
        error_class = left($4, 128),
        error = left($5, 2048)
    FROM finished
    WHERE attempt.inbox_id = finished.id
      AND attempt.attempt = finished.attempt_count
    RETURNING attempt.inbox_id
)
SELECT EXISTS(SELECT 1 FROM attempt_finished)
"#;

const SQL_IGNORE_UPDATE: &str = r#"
WITH finished AS (
    UPDATE telegram_update_inbox
    SET status = 'ignored',
        handler_completed_at = statement_timestamp(),
        completed_at = statement_timestamp(),
        outcome = 'ignored',
        ignored_reason = left($3, 512),
        lease_owner = NULL,
        leased_until = NULL,
        updated_at = statement_timestamp()
    WHERE id = $1
      AND lease_token = $2
      AND status = 'processing'
      AND leased_until > statement_timestamp()
    RETURNING id, attempt_count
), attempt_finished AS (
    UPDATE telegram_update_attempts AS attempt
    SET handler_completed_at = statement_timestamp(),
        finished_at = statement_timestamp(),
        outcome = 'ignored'
    FROM finished
    WHERE attempt.inbox_id = finished.id
      AND attempt.attempt = finished.attempt_count
    RETURNING attempt.inbox_id
)
SELECT EXISTS(SELECT 1 FROM attempt_finished)
"#;

const SQL_DEAD_LETTER_UPDATE: &str = r#"
WITH finished AS (
    UPDATE telegram_update_inbox
    SET status = 'dead_letter',
        handler_completed_at = statement_timestamp(),
        completed_at = statement_timestamp(),
        outcome = 'dead_letter',
        last_error_class = left($3, 128),
        last_error = left($4, 2048),
        lease_owner = NULL,
        leased_until = NULL,
        updated_at = statement_timestamp()
    WHERE id = $1
      AND lease_token = $2
      AND status = 'processing'
      AND leased_until > statement_timestamp()
    RETURNING id, attempt_count
), attempt_finished AS (
    UPDATE telegram_update_attempts AS attempt
    SET handler_completed_at = statement_timestamp(),
        finished_at = statement_timestamp(),
        outcome = 'dead_letter',
        error_class = left($3, 128),
        error = left($4, 2048)
    FROM finished
    WHERE attempt.inbox_id = finished.id
      AND attempt.attempt = finished.attempt_count
    RETURNING attempt.inbox_id
)
SELECT EXISTS(SELECT 1 FROM attempt_finished)
"#;

const SQL_INBOX_STATS: &str = r#"
SELECT
    count(*) FILTER (WHERE status = 'pending')::bigint AS pending,
    count(*) FILTER (WHERE status = 'processing')::bigint AS processing,
    count(*) FILTER (WHERE status = 'retry_wait')::bigint AS retry_wait,
    count(*) FILTER (WHERE status = 'completed')::bigint AS completed,
    count(*) FILTER (WHERE status = 'ignored')::bigint AS ignored,
    count(*) FILTER (WHERE status = 'dead_letter')::bigint AS dead_letter,
    COALESCE(sum(GREATEST(delivery_count - 1, 0)), 0)::bigint AS duplicates,
    COALESCE(sum(delivery_count), 0)::bigint AS total_deliveries,
    count(*) FILTER (WHERE payload_conflict)::bigint AS payload_conflicts,
    count(*) FILTER (
        WHERE status = 'processing' AND leased_until <= statement_timestamp()
    )::bigint AS expired_leases,
    min(materialized_at) FILTER (WHERE status IN ('pending', 'retry_wait')) AS oldest_pending_at,
    min(available_at) FILTER (WHERE status = 'retry_wait') AS oldest_retry_at,
    min(leased_until) FILTER (WHERE status = 'processing') AS oldest_lease_expiry,
    (SELECT count(*)::bigint FROM telegram_update_quarantine) AS quarantined
FROM telegram_update_inbox
"#;

const SQL_LIST_INBOX_ITEMS: &str = r#"
SELECT id, bot_id, update_id, schema_version, source, stream_ms, stream_seq,
       last_stream_ms, last_stream_seq, octet_length(raw_payload)::bigint AS payload_size_bytes,
       payload_sha256, payload_conflict,
       update_type, telegram_event_at, first_received_at, last_received_at,
       materialized_at, delivery_count, ordering_key, priority, chat_id,
       thread_id, user_id, status, available_at, attempt_count, lease_owner,
       lease_token, leased_until, processing_started_at, state_applied_at,
       handler_completed_at, completed_at, outcome, ignored_reason,
       last_error_class, last_error, created_at, updated_at
FROM telegram_update_inbox
WHERE ($1::bigint IS NULL OR id < $1)
  AND ($2::text IS NULL OR status = $2)
ORDER BY id DESC
LIMIT $3
"#;

const SQL_GET_INBOX_ITEM: &str = r#"
SELECT id, bot_id, update_id, schema_version, source, stream_ms, stream_seq,
       last_stream_ms, last_stream_seq, octet_length(raw_payload)::bigint AS payload_size_bytes,
       payload_sha256, payload_conflict,
       update_type, telegram_event_at, first_received_at, last_received_at,
       materialized_at, delivery_count, ordering_key, priority, chat_id,
       thread_id, user_id, status, available_at, attempt_count, lease_owner,
       lease_token, leased_until, processing_started_at, state_applied_at,
       handler_completed_at, completed_at, outcome, ignored_reason,
       last_error_class, last_error, created_at, updated_at
FROM telegram_update_inbox
WHERE id = $1
"#;

const SQL_LIST_INBOX_ATTEMPTS: &str = r#"
SELECT attempt, lease_token, worker_id, claimed_at, state_started_at,
       state_completed_at, handler_started_at, handler_completed_at,
       finished_at, outcome, error_class, error
FROM telegram_update_attempts
WHERE inbox_id = $1
ORDER BY attempt DESC
LIMIT $2
"#;

/// One already-validated Redis Stream entry ready for Postgres.
#[derive(Clone, Debug, PartialEq)]
pub struct MaterializedUpdateInput {
    pub bot_id: i64,
    pub update_id: i64,
    pub schema_version: i16,
    pub source: String,
    /// First Redis Stream entry represented by this aggregate.
    pub stream_ms: i64,
    pub stream_seq: i64,
    /// Last Redis Stream entry represented by this aggregate.
    pub last_stream_ms: i64,
    pub last_stream_seq: i64,
    pub raw_payload: Vec<u8>,
    pub payload_sha256: Vec<u8>,
    pub payload_conflict: bool,
    pub update_type: Option<String>,
    pub telegram_event_at: Option<OffsetDateTime>,
    pub first_received_at: OffsetDateTime,
    pub last_received_at: OffsetDateTime,
    pub delivery_count: i64,
    pub ordering_key: String,
    pub priority: i32,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub user_id: Option<i64>,
}

/// One Stream entry that cannot be converted into an inbox update.
#[derive(Clone, Debug, PartialEq)]
pub struct QuarantinedUpdateInput {
    pub bot_id: i64,
    pub stream_ms: i64,
    pub stream_seq: i64,
    pub schema_version: i16,
    pub source: String,
    pub raw_payload: Vec<u8>,
    pub payload_sha256: Vec<u8>,
    pub first_received_at: OffsetDateTime,
    pub last_received_at: OffsetDateTime,
    pub delivery_count: i64,
    pub error_class: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq)]
struct PreparedUpdate {
    canonical: MaterializedUpdateInput,
    last_stream_ms: i64,
    last_stream_seq: i64,
}

/// Aggregate result for one committed Redis batch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaterializationReport {
    /// New unique inbox rows.
    pub inserted: u64,
    /// Deliveries that mapped to an existing or same-batch update ID.
    pub duplicates: u64,
    /// Affected inbox keys whose canonical payload conflicts with a duplicate.
    pub conflicted: u64,
    /// Quarantined deliveries accepted by the transaction.
    pub quarantined: u64,
}

/// A fenced update lease returned to one processing worker.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaimedTelegramUpdate {
    pub id: i64,
    pub bot_id: i64,
    pub update_id: i64,
    pub schema_version: i16,
    pub source: String,
    pub stream_ms: i64,
    pub stream_seq: i64,
    pub raw_payload: Vec<u8>,
    pub payload_sha256: Vec<u8>,
    pub update_type: Option<String>,
    pub telegram_event_at: Option<OffsetDateTime>,
    pub first_received_at: OffsetDateTime,
    pub last_received_at: OffsetDateTime,
    pub materialized_at: OffsetDateTime,
    pub delivery_count: i64,
    pub ordering_key: String,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub user_id: Option<i64>,
    pub attempt: i32,
    pub lease_token: i64,
    pub leased_until: OffsetDateTime,
    pub state_applied_at: Option<OffsetDateTime>,
}

/// Aggregate operator diagnostics for the materialized update inbox.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TelegramUpdateInboxStats {
    pub pending: i64,
    pub processing: i64,
    pub retry_wait: i64,
    pub completed: i64,
    pub ignored: i64,
    pub dead_letter: i64,
    pub duplicates: i64,
    pub total_deliveries: i64,
    pub payload_conflicts: i64,
    pub expired_leases: i64,
    pub quarantined: i64,
    pub oldest_pending_at: Option<OffsetDateTime>,
    pub oldest_retry_at: Option<OffsetDateTime>,
    pub oldest_lease_expiry: Option<OffsetDateTime>,
}

/// One operator-visible inbox row without the raw Telegram payload.
#[derive(Clone, Debug, PartialEq)]
pub struct TelegramUpdateInboxItem {
    pub id: i64,
    pub bot_id: i64,
    pub update_id: i64,
    pub schema_version: i16,
    pub source: String,
    pub stream_ms: i64,
    pub stream_seq: i64,
    pub last_stream_ms: i64,
    pub last_stream_seq: i64,
    pub payload_size_bytes: i64,
    pub payload_sha256: Vec<u8>,
    pub payload_conflict: bool,
    pub update_type: Option<String>,
    pub telegram_event_at: Option<OffsetDateTime>,
    pub first_received_at: OffsetDateTime,
    pub last_received_at: OffsetDateTime,
    pub materialized_at: OffsetDateTime,
    pub delivery_count: i64,
    pub ordering_key: String,
    pub priority: i32,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub user_id: Option<i64>,
    pub status: String,
    pub available_at: OffsetDateTime,
    pub attempt_count: i32,
    pub lease_owner: Option<String>,
    pub lease_token: i64,
    pub leased_until: Option<OffsetDateTime>,
    pub processing_started_at: Option<OffsetDateTime>,
    pub state_applied_at: Option<OffsetDateTime>,
    pub handler_completed_at: Option<OffsetDateTime>,
    pub completed_at: Option<OffsetDateTime>,
    pub outcome: Option<String>,
    pub ignored_reason: Option<String>,
    pub last_error_class: Option<String>,
    pub last_error: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// One durable inbox processing attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct TelegramUpdateAttempt {
    pub attempt: i32,
    pub lease_token: i64,
    pub worker_id: String,
    pub claimed_at: OffsetDateTime,
    pub state_started_at: Option<OffsetDateTime>,
    pub state_completed_at: Option<OffsetDateTime>,
    pub handler_started_at: Option<OffsetDateTime>,
    pub handler_completed_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub outcome: Option<String>,
    pub error_class: Option<String>,
    pub error: Option<String>,
}

impl ClaimedTelegramUpdate {
    /// Decode the versioned raw JSON without coupling storage to a Telegram SDK.
    pub fn decode_payload<T: DeserializeOwned>(&self) -> Result<T, StorageError> {
        serde_json::from_slice(&self.raw_payload)
            .map_err(|source| StorageError::TelegramUpdateCodec { source })
    }

    /// Whether this attempt must run the state/history stage before the handler.
    #[must_use]
    pub fn needs_state_application(&self) -> bool {
        self.state_applied_at.is_none()
    }
}

/// Postgres-backed materialization and inbox processing store.
#[derive(Clone, Debug)]
pub struct PostgresTelegramDeliveryStore {
    pool: PgPool,
}

impl PostgresTelegramDeliveryStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Commit a Stream batch with one inbox multi-insert and, when needed, one
    /// quarantine multi-insert. No per-entry SQL is executed.
    pub async fn materialize_update_batch(
        &self,
        updates: &[MaterializedUpdateInput],
        quarantine: &[QuarantinedUpdateInput],
    ) -> Result<MaterializationReport, StorageError> {
        materialize_update_batch(&self.pool, updates, quarantine).await
    }

    /// Claim the earliest processable entry per ordering key.
    pub async fn claim_updates(
        &self,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<ClaimedTelegramUpdate>, StorageError> {
        let limit = i64::try_from(limit.clamp(1, 1_000)).unwrap_or(1_000);
        let rows = sqlx::query(SQL_CLAIM_UPDATES)
            .bind(limit)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(claimed_update_from_row).collect()
    }

    /// Extend a live lease. An expired or superseded token cannot be renewed.
    pub async fn renew_update_lease(
        &self,
        inbox_id: i64,
        lease_token: i64,
    ) -> Result<bool, StorageError> {
        let renewed = sqlx::query_scalar::<_, i64>(SQL_RENEW_UPDATE_LEASE)
            .bind(inbox_id)
            .bind(lease_token)
            .fetch_optional(&self.pool)
            .await?;
        Ok(renewed.is_some())
    }

    /// Persist the state/history checkpoint before invoking the handler.
    pub async fn mark_update_state_applied(
        &self,
        inbox_id: i64,
        lease_token: i64,
    ) -> Result<bool, StorageError> {
        fenced_transition(&self.pool, SQL_MARK_STATE_APPLIED, inbox_id, lease_token).await
    }

    /// Mark a handled update complete after its durable handoff has committed.
    pub async fn complete_update(
        &self,
        inbox_id: i64,
        lease_token: i64,
    ) -> Result<bool, StorageError> {
        fenced_transition(&self.pool, SQL_COMPLETE_UPDATE, inbox_id, lease_token).await
    }

    /// Release a failed attempt for retry at a caller-selected time.
    pub async fn retry_update(
        &self,
        inbox_id: i64,
        lease_token: i64,
        available_at: OffsetDateTime,
        error_class: &str,
        error: &str,
    ) -> Result<bool, StorageError> {
        sqlx::query_scalar::<_, bool>(SQL_RETRY_UPDATE)
            .bind(inbox_id)
            .bind(lease_token)
            .bind(available_at)
            .bind(error_class)
            .bind(error)
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    /// Finish an intentionally ignored update with a durable reason.
    pub async fn ignore_update(
        &self,
        inbox_id: i64,
        lease_token: i64,
        reason: &str,
    ) -> Result<bool, StorageError> {
        sqlx::query_scalar::<_, bool>(SQL_IGNORE_UPDATE)
            .bind(inbox_id)
            .bind(lease_token)
            .bind(reason)
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    /// Finish a deterministic or exhausted failure in the durable dead letter queue.
    pub async fn dead_letter_update(
        &self,
        inbox_id: i64,
        lease_token: i64,
        error_class: &str,
        error: &str,
    ) -> Result<bool, StorageError> {
        sqlx::query_scalar::<_, bool>(SQL_DEAD_LETTER_UPDATE)
            .bind(inbox_id)
            .bind(lease_token)
            .bind(error_class)
            .bind(error)
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    /// Keyset-paginated operator view without raw update bodies.
    pub async fn list_items(
        &self,
        limit: usize,
        before_id: Option<i64>,
        status: Option<&str>,
    ) -> Result<Vec<TelegramUpdateInboxItem>, StorageError> {
        let limit = i64::try_from(limit.clamp(1, 500)).unwrap_or(500);
        sqlx::query(SQL_LIST_INBOX_ITEMS)
            .bind(before_id)
            .bind(status)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(inbox_item_from_row)
            .collect()
    }

    pub async fn item(
        &self,
        inbox_id: i64,
    ) -> Result<Option<TelegramUpdateInboxItem>, StorageError> {
        sqlx::query(SQL_GET_INBOX_ITEM)
            .bind(inbox_id)
            .fetch_optional(&self.pool)
            .await?
            .map(inbox_item_from_row)
            .transpose()
    }

    pub async fn attempts(
        &self,
        inbox_id: i64,
        limit: usize,
    ) -> Result<Vec<TelegramUpdateAttempt>, StorageError> {
        let limit = i64::try_from(limit.clamp(1, 500)).unwrap_or(500);
        sqlx::query(SQL_LIST_INBOX_ATTEMPTS)
            .bind(inbox_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(inbox_attempt_from_row)
            .collect()
    }

    pub async fn stats(&self) -> Result<TelegramUpdateInboxStats, StorageError> {
        let row = sqlx::query(SQL_INBOX_STATS).fetch_one(&self.pool).await?;
        Ok(TelegramUpdateInboxStats {
            pending: row.try_get("pending")?,
            processing: row.try_get("processing")?,
            retry_wait: row.try_get("retry_wait")?,
            completed: row.try_get("completed")?,
            ignored: row.try_get("ignored")?,
            dead_letter: row.try_get("dead_letter")?,
            duplicates: row.try_get("duplicates")?,
            total_deliveries: row.try_get("total_deliveries")?,
            payload_conflicts: row.try_get("payload_conflicts")?,
            expired_leases: row.try_get("expired_leases")?,
            quarantined: row.try_get("quarantined")?,
            oldest_pending_at: row.try_get("oldest_pending_at")?,
            oldest_retry_at: row.try_get("oldest_retry_at")?,
            oldest_lease_expiry: row.try_get("oldest_lease_expiry")?,
        })
    }
}

/// Maximum rows allowed by the bind budget for the inbox statement.
#[must_use]
pub const fn materialization_bind_row_limit() -> usize {
    POSTGRES_MATERIALIZATION_BIND_BUDGET / MATERIALIZED_UPDATE_BINDS_PER_ROW
}

/// Free-function boundary for callers that already own a pool.
pub async fn materialize_update_batch(
    pool: &PgPool,
    updates: &[MaterializedUpdateInput],
    quarantine: &[QuarantinedUpdateInput],
) -> Result<MaterializationReport, StorageError> {
    let prepared_updates = prepare_updates(updates);
    let prepared_quarantine = prepare_quarantine(quarantine);
    let max_rows = materialization_bind_row_limit();
    if prepared_updates.len() > max_rows {
        return Err(StorageError::TelegramMaterializationBatchTooLarge {
            rows: prepared_updates.len(),
            max_rows,
        });
    }
    let max_quarantine_rows =
        POSTGRES_MATERIALIZATION_BIND_BUDGET / QUARANTINED_UPDATE_BINDS_PER_ROW;
    if prepared_quarantine.len() > max_quarantine_rows {
        return Err(StorageError::TelegramMaterializationBatchTooLarge {
            rows: prepared_quarantine.len(),
            max_rows: max_quarantine_rows,
        });
    }
    if prepared_updates.is_empty() && prepared_quarantine.is_empty() {
        return Ok(MaterializationReport::default());
    }

    let accepted_deliveries = prepared_updates
        .iter()
        .map(|update| delivery_count_as_u64(update.canonical.delivery_count))
        .fold(0_u64, u64::saturating_add);
    let quarantined = prepared_quarantine
        .iter()
        .map(|update| delivery_count_as_u64(update.delivery_count))
        .fold(0_u64, u64::saturating_add);

    let mut tx = pool.begin().await?;
    let mut report = MaterializationReport {
        quarantined,
        ..MaterializationReport::default()
    };

    if !prepared_updates.is_empty() {
        let mut builder = materialized_update_insert_builder(&prepared_updates);
        let rows = builder.build().fetch_all(&mut *tx).await?;
        for row in rows {
            if row.try_get::<bool, _>("inserted")? {
                report.inserted = report.inserted.saturating_add(1);
            }
            if row.try_get::<bool, _>("payload_conflict")? {
                report.conflicted = report.conflicted.saturating_add(1);
            }
        }
    }

    if !prepared_quarantine.is_empty() {
        let mut builder = quarantine_insert_builder(&prepared_quarantine);
        builder.build().execute(&mut *tx).await?;
    }

    tx.commit().await?;
    report.duplicates = accepted_deliveries.saturating_sub(report.inserted);
    Ok(report)
}

async fn fenced_transition(
    pool: &PgPool,
    statement: &'static str,
    inbox_id: i64,
    lease_token: i64,
) -> Result<bool, StorageError> {
    sqlx::query_scalar::<_, bool>(statement)
        .bind(inbox_id)
        .bind(lease_token)
        .fetch_one(pool)
        .await
        .map_err(StorageError::from)
}

fn prepare_updates(updates: &[MaterializedUpdateInput]) -> Vec<PreparedUpdate> {
    let mut index_by_key: HashMap<(i64, i64), usize> = HashMap::with_capacity(updates.len());
    let mut prepared: Vec<PreparedUpdate> = Vec::with_capacity(updates.len());
    for update in updates {
        let key = (update.bot_id, update.update_id);
        if let Some(index) = index_by_key.get(&key).copied() {
            let existing = &mut prepared[index];
            existing.canonical.payload_conflict |= existing.canonical.payload_sha256
                != update.payload_sha256
                || update.payload_conflict;
            existing.canonical.delivery_count = existing
                .canonical
                .delivery_count
                .saturating_add(update.delivery_count.max(1));
            if update.first_received_at < existing.canonical.first_received_at {
                existing.canonical.first_received_at = update.first_received_at;
            }
            if update.last_received_at > existing.canonical.last_received_at {
                existing.canonical.last_received_at = update.last_received_at;
            }
            if (update.last_stream_ms, update.last_stream_seq)
                > (existing.last_stream_ms, existing.last_stream_seq)
            {
                existing.last_stream_ms = update.last_stream_ms;
                existing.last_stream_seq = update.last_stream_seq;
            }
            continue;
        }
        index_by_key.insert(key, prepared.len());
        let mut update = update.clone();
        update.delivery_count = update.delivery_count.max(1);
        if (update.last_stream_ms, update.last_stream_seq) < (update.stream_ms, update.stream_seq) {
            update.last_stream_ms = update.stream_ms;
            update.last_stream_seq = update.stream_seq;
        }
        if update.last_received_at < update.first_received_at {
            std::mem::swap(&mut update.first_received_at, &mut update.last_received_at);
        }
        prepared.push(PreparedUpdate {
            last_stream_ms: update.last_stream_ms,
            last_stream_seq: update.last_stream_seq,
            canonical: update,
        });
    }
    prepared
}

fn prepare_quarantine(quarantine: &[QuarantinedUpdateInput]) -> Vec<QuarantinedUpdateInput> {
    let mut index_by_key: HashMap<(i64, i64, i64), usize> =
        HashMap::with_capacity(quarantine.len());
    let mut prepared: Vec<QuarantinedUpdateInput> = Vec::with_capacity(quarantine.len());
    for update in quarantine {
        let key = (update.bot_id, update.stream_ms, update.stream_seq);
        if let Some(index) = index_by_key.get(&key).copied() {
            let existing = &mut prepared[index];
            existing.delivery_count = existing
                .delivery_count
                .saturating_add(update.delivery_count.max(1));
            if update.first_received_at < existing.first_received_at {
                existing.first_received_at = update.first_received_at;
            }
            if update.last_received_at > existing.last_received_at {
                existing.last_received_at = update.last_received_at;
            }
            continue;
        }
        index_by_key.insert(key, prepared.len());
        let mut update = update.clone();
        update.delivery_count = update.delivery_count.max(1);
        if update.last_received_at < update.first_received_at {
            std::mem::swap(&mut update.first_received_at, &mut update.last_received_at);
        }
        update.error_class = truncate_chars(&update.error_class, 128);
        update.error = truncate_chars(&update.error, 2_048);
        prepared.push(update);
    }
    prepared
}

fn materialized_update_insert_builder(updates: &[PreparedUpdate]) -> QueryBuilder<Postgres> {
    let mut builder = QueryBuilder::new(SQL_INSERT_UPDATES_PREFIX);
    builder.push(" ");
    builder.push_values(updates.iter(), |mut row, update| {
        let canonical = &update.canonical;
        row.push_bind(canonical.bot_id)
            .push_bind(canonical.update_id)
            .push_bind(canonical.schema_version)
            .push_bind(canonical.source.clone())
            .push_bind(canonical.stream_ms)
            .push_bind(canonical.stream_seq)
            .push_bind(update.last_stream_ms)
            .push_bind(update.last_stream_seq)
            .push_bind(canonical.raw_payload.clone())
            .push_bind(canonical.payload_sha256.clone())
            .push_bind(canonical.payload_conflict)
            .push_bind(canonical.update_type.clone())
            .push_bind(canonical.telegram_event_at)
            .push_bind(canonical.first_received_at)
            .push_bind(canonical.last_received_at)
            .push_bind(canonical.delivery_count)
            .push_bind(canonical.ordering_key.clone())
            .push_bind(canonical.priority)
            .push_bind(canonical.chat_id)
            .push_bind(canonical.thread_id)
            .push_bind(canonical.user_id);
    });
    builder.push(SQL_INSERT_UPDATES_SUFFIX);
    builder
}

fn quarantine_insert_builder(updates: &[QuarantinedUpdateInput]) -> QueryBuilder<Postgres> {
    let mut builder = QueryBuilder::new(SQL_INSERT_QUARANTINE_PREFIX);
    builder.push(" ");
    builder.push_values(updates.iter(), |mut row, update| {
        row.push_bind(update.bot_id)
            .push_bind(update.stream_ms)
            .push_bind(update.stream_seq)
            .push_bind(update.schema_version)
            .push_bind(update.source.clone())
            .push_bind(update.raw_payload.clone())
            .push_bind(update.payload_sha256.clone())
            .push_bind(update.first_received_at)
            .push_bind(update.last_received_at)
            .push_bind(update.delivery_count)
            .push_bind(update.error_class.clone())
            .push_bind(update.error.clone());
    });
    builder.push(SQL_INSERT_QUARANTINE_SUFFIX);
    builder
}

fn claimed_update_from_row(row: PgRow) -> Result<ClaimedTelegramUpdate, StorageError> {
    Ok(ClaimedTelegramUpdate {
        id: row.try_get("id")?,
        bot_id: row.try_get("bot_id")?,
        update_id: row.try_get("update_id")?,
        schema_version: row.try_get("schema_version")?,
        source: row.try_get("source")?,
        stream_ms: row.try_get("stream_ms")?,
        stream_seq: row.try_get("stream_seq")?,
        raw_payload: row.try_get("raw_payload")?,
        payload_sha256: row.try_get("payload_sha256")?,
        update_type: row.try_get("update_type")?,
        telegram_event_at: row.try_get("telegram_event_at")?,
        first_received_at: row.try_get("first_received_at")?,
        last_received_at: row.try_get("last_received_at")?,
        materialized_at: row.try_get("materialized_at")?,
        delivery_count: row.try_get("delivery_count")?,
        ordering_key: row.try_get("ordering_key")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        user_id: row.try_get("user_id")?,
        attempt: row.try_get("attempt_count")?,
        lease_token: row.try_get("lease_token")?,
        leased_until: row.try_get("leased_until")?,
        state_applied_at: row.try_get("state_applied_at")?,
    })
}

fn inbox_item_from_row(row: PgRow) -> Result<TelegramUpdateInboxItem, StorageError> {
    Ok(TelegramUpdateInboxItem {
        id: row.try_get("id")?,
        bot_id: row.try_get("bot_id")?,
        update_id: row.try_get("update_id")?,
        schema_version: row.try_get("schema_version")?,
        source: row.try_get("source")?,
        stream_ms: row.try_get("stream_ms")?,
        stream_seq: row.try_get("stream_seq")?,
        last_stream_ms: row.try_get("last_stream_ms")?,
        last_stream_seq: row.try_get("last_stream_seq")?,
        payload_size_bytes: row.try_get("payload_size_bytes")?,
        payload_sha256: row.try_get("payload_sha256")?,
        payload_conflict: row.try_get("payload_conflict")?,
        update_type: row.try_get("update_type")?,
        telegram_event_at: row.try_get("telegram_event_at")?,
        first_received_at: row.try_get("first_received_at")?,
        last_received_at: row.try_get("last_received_at")?,
        materialized_at: row.try_get("materialized_at")?,
        delivery_count: row.try_get("delivery_count")?,
        ordering_key: row.try_get("ordering_key")?,
        priority: row.try_get("priority")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        user_id: row.try_get("user_id")?,
        status: row.try_get("status")?,
        available_at: row.try_get("available_at")?,
        attempt_count: row.try_get("attempt_count")?,
        lease_owner: row.try_get("lease_owner")?,
        lease_token: row.try_get("lease_token")?,
        leased_until: row.try_get("leased_until")?,
        processing_started_at: row.try_get("processing_started_at")?,
        state_applied_at: row.try_get("state_applied_at")?,
        handler_completed_at: row.try_get("handler_completed_at")?,
        completed_at: row.try_get("completed_at")?,
        outcome: row.try_get("outcome")?,
        ignored_reason: row.try_get("ignored_reason")?,
        last_error_class: row.try_get("last_error_class")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn inbox_attempt_from_row(row: PgRow) -> Result<TelegramUpdateAttempt, StorageError> {
    Ok(TelegramUpdateAttempt {
        attempt: row.try_get("attempt")?,
        lease_token: row.try_get("lease_token")?,
        worker_id: row.try_get("worker_id")?,
        claimed_at: row.try_get("claimed_at")?,
        state_started_at: row.try_get("state_started_at")?,
        state_completed_at: row.try_get("state_completed_at")?,
        handler_started_at: row.try_get("handler_started_at")?,
        handler_completed_at: row.try_get("handler_completed_at")?,
        finished_at: row.try_get("finished_at")?,
        outcome: row.try_get("outcome")?,
        error_class: row.try_get("error_class")?,
        error: row.try_get("error")?,
    })
}

fn delivery_count_as_u64(count: i64) -> u64 {
    u64::try_from(count.max(1)).unwrap_or(u64::MAX)
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error, time::SystemTime};

    use sqlx::{Execute as _, postgres::PgPoolOptions};

    use super::*;

    fn update(
        bot_id: i64,
        update_id: i64,
        stream_seq: i64,
        payload: &[u8],
        hash_byte: u8,
    ) -> MaterializedUpdateInput {
        let now = OffsetDateTime::now_utc();
        MaterializedUpdateInput {
            bot_id,
            update_id,
            schema_version: 1,
            source: "webhook".to_owned(),
            stream_ms: 1_700_000_000_000,
            stream_seq,
            last_stream_ms: 1_700_000_000_000,
            last_stream_seq: stream_seq,
            raw_payload: payload.to_vec(),
            payload_sha256: vec![hash_byte; 32],
            payload_conflict: false,
            update_type: Some("message".to_owned()),
            telegram_event_at: Some(now),
            first_received_at: now,
            last_received_at: now,
            delivery_count: 1,
            ordering_key: format!("dialog:{bot_id}:42:0"),
            priority: 0,
            chat_id: Some(42),
            thread_id: Some(0),
            user_id: Some(7),
        }
    }

    fn quarantine(bot_id: i64) -> QuarantinedUpdateInput {
        let now = OffsetDateTime::now_utc();
        QuarantinedUpdateInput {
            bot_id,
            stream_ms: 1_700_000_000_000,
            stream_seq: 99,
            schema_version: 1,
            source: "webhook".to_owned(),
            raw_payload: b"{".to_vec(),
            payload_sha256: vec![9; 32],
            first_received_at: now,
            last_received_at: now,
            delivery_count: 1,
            error_class: "invalid_json".to_owned(),
            error: "unexpected end of input".to_owned(),
        }
    }

    #[test]
    fn full_bulk_uses_one_values_statement_for_all_valid_rows() {
        let updates = (0..512)
            .map(|index| {
                update(
                    1,
                    i64::from(index),
                    i64::from(index),
                    br#"{"update_id":1}"#,
                    1,
                )
            })
            .collect::<Vec<_>>();
        let prepared = prepare_updates(&updates);
        let mut builder = materialized_update_insert_builder(&prepared);
        let sql = builder.build().sql().as_ref().to_owned();

        assert_eq!(sql.matches("INSERT INTO telegram_update_inbox").count(), 1);
        assert_eq!(sql.matches("ON CONFLICT").count(), 1);
        assert_eq!(
            sql.matches('$').count(),
            512 * MATERIALIZED_UPDATE_BINDS_PER_ROW
        );
    }

    #[test]
    fn duplicate_keys_are_aggregated_before_query_building() {
        let prepared = prepare_updates(&[
            update(1, 7, 1, br#"{"update_id":7}"#, 1),
            update(1, 7, 2, br#"{"update_id":7,"different":true}"#, 2),
        ]);

        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].canonical.delivery_count, 2);
        assert!(prepared[0].canonical.payload_conflict);
        assert_eq!(prepared[0].canonical.raw_payload, br#"{"update_id":7}"#);
        assert_eq!(
            (prepared[0].last_stream_ms, prepared[0].last_stream_seq),
            (1_700_000_000_000, 2)
        );
    }

    #[test]
    fn preaggregated_input_preserves_first_and_advances_last_stream_position() {
        let mut aggregate = update(1, 7, 1, br#"{"update_id":7}"#, 1);
        aggregate.last_stream_seq = 4;
        aggregate.delivery_count = 4;

        let prepared = prepare_updates(&[aggregate]);

        assert_eq!(
            (
                prepared[0].canonical.stream_ms,
                prepared[0].canonical.stream_seq
            ),
            (1_700_000_000_000, 1)
        );
        assert_eq!(
            (prepared[0].last_stream_ms, prepared[0].last_stream_seq),
            (1_700_000_000_000, 4)
        );
        assert_eq!(prepared[0].canonical.delivery_count, 4);
    }

    #[test]
    fn quarantine_uses_one_bulk_statement() {
        let prepared = prepare_quarantine(&[quarantine(1), quarantine(2)]);
        let mut builder = quarantine_insert_builder(&prepared);
        let sql = builder.build().sql().as_ref().to_owned();

        assert_eq!(
            sql.matches("INSERT INTO telegram_update_quarantine")
                .count(),
            1
        );
        assert_eq!(sql.matches("ON CONFLICT").count(), 1);
    }

    #[test]
    fn quarantine_error_truncation_respects_unicode_boundaries() {
        let mut input = quarantine(1);
        input.error = "🦀".repeat(2_100);

        let prepared = prepare_quarantine(&[input]);

        assert_eq!(prepared[0].error.chars().count(), 2_048);
    }

    #[test]
    fn claim_sql_is_ordered_fenced_and_skip_locked() {
        assert!(SQL_CLAIM_UPDATES.contains("FOR UPDATE OF inbox SKIP LOCKED"));
        assert!(SQL_CLAIM_UPDATES.contains("ORDER BY inbox.stream_ms, inbox.stream_seq"));
        assert!(SQL_CLAIM_UPDATES.contains("lease_token = inbox.lease_token + 1"));
        assert!(SQL_CLAIM_UPDATES.contains("interval '90 seconds'"));
        assert!(SQL_CLAIM_UPDATES.contains("NOT EXISTS"));
    }

    /// Release-mode transaction benchmark for the configured materializer sizes.
    ///
    /// Run with:
    /// `OPENPLOTVA_TEST_POSTGRES_DSN=... cargo test -p openplotva-storage --release materialization_release_benchmark -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires a disposable Postgres and is run explicitly for release evidence"]
    async fn materialization_release_benchmark() -> Result<(), Box<dyn Error>> {
        let dsn = env::var("OPENPLOTVA_TEST_POSTGRES_DSN")?;
        let iterations = env::var("UPDATE_MATERIALIZER_BENCH_ITERATIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(20)
            .max(5);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await?;
        crate::run_migrations_on(&pool).await?;
        let store = PostgresTelegramDeliveryStore::new(pool.clone());
        let seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seed = i64::try_from(seed.min(i64::MAX as u128)).unwrap_or(i64::MAX);

        println!("rows,iterations,rows_per_second,p50_ms,p95_ms");
        for (size_index, rows) in [1_usize, 64, 256, 512, 1_000].into_iter().enumerate() {
            let mut samples = Vec::with_capacity(iterations);
            for iteration in 0..iterations {
                let identity = size_index
                    .checked_mul(iterations)
                    .and_then(|value| value.checked_add(iteration))
                    .and_then(|value| i64::try_from(value).ok())
                    .unwrap_or(i64::MAX - 1);
                let bot_id = seed.saturating_sub(identity).saturating_neg();
                let updates = (0..rows)
                    .map(|index| {
                        let update_id = i64::try_from(index).unwrap_or(i64::MAX);
                        let stream_seq = update_id;
                        update(
                            bot_id,
                            update_id,
                            stream_seq,
                            br#"{"update_id":1}"#,
                            u8::try_from(index % 251).unwrap_or_default(),
                        )
                    })
                    .collect::<Vec<_>>();

                let started = std::time::Instant::now();
                let report = store.materialize_update_batch(&updates, &[]).await?;
                samples.push(started.elapsed());
                assert_eq!(report.inserted, u64::try_from(rows).unwrap_or(u64::MAX));

                sqlx::query("DELETE FROM telegram_update_inbox WHERE bot_id = $1")
                    .bind(bot_id)
                    .execute(&pool)
                    .await?;
            }

            samples.sort_unstable();
            let total_seconds = samples
                .iter()
                .map(std::time::Duration::as_secs_f64)
                .sum::<f64>();
            let total_rows = rows.saturating_mul(iterations);
            let p50 = samples[(samples.len() - 1) * 50 / 100].as_secs_f64() * 1_000.0;
            let p95 = samples[(samples.len() - 1) * 95 / 100].as_secs_f64() * 1_000.0;
            println!(
                "{rows},{iterations},{:.2},{p50:.3},{p95:.3}",
                total_rows as f64 / total_seconds.max(f64::EPSILON),
            );
        }
        Ok(())
    }

    #[test]
    fn claimed_payload_decodes_without_telegram_dependency() -> Result<(), Box<dyn Error>> {
        #[derive(Debug, serde::Deserialize, PartialEq)]
        struct Envelope {
            update_id: i64,
        }

        let input = update(1, 42, 1, br#"{"update_id":42}"#, 1);
        let claim = ClaimedTelegramUpdate {
            id: 1,
            bot_id: input.bot_id,
            update_id: input.update_id,
            schema_version: input.schema_version,
            source: input.source,
            stream_ms: input.stream_ms,
            stream_seq: input.stream_seq,
            raw_payload: input.raw_payload,
            payload_sha256: input.payload_sha256,
            update_type: input.update_type,
            telegram_event_at: input.telegram_event_at,
            first_received_at: input.first_received_at,
            last_received_at: input.last_received_at,
            materialized_at: OffsetDateTime::now_utc(),
            delivery_count: 1,
            ordering_key: input.ordering_key,
            chat_id: input.chat_id,
            thread_id: input.thread_id,
            user_id: input.user_id,
            attempt: 1,
            lease_token: 1,
            leased_until: OffsetDateTime::now_utc() + time::Duration::seconds(90),
            state_applied_at: None,
        };

        assert_eq!(
            claim.decode_payload::<Envelope>()?,
            Envelope { update_id: 42 }
        );
        assert!(claim.needs_state_application());
        Ok(())
    }

    #[tokio::test]
    async fn materialization_and_fenced_processing_roundtrip() -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await?;
        crate::run_migrations_on(&pool).await?;
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let bot_id = -i64::try_from(unique.min(i64::MAX as u128)).unwrap_or(i64::MAX);
        let store = PostgresTelegramDeliveryStore::new(pool.clone());
        let first = update(bot_id, 7, 1, br#"{"update_id":7}"#, 1);
        let conflicting = update(bot_id, 7, 2, br#"{"update_id":7,"different":true}"#, 2);

        let report = store
            .materialize_update_batch(&[first.clone(), conflicting], &[quarantine(bot_id)])
            .await?;
        assert_eq!(
            report,
            MaterializationReport {
                inserted: 1,
                duplicates: 1,
                conflicted: 1,
                quarantined: 1,
            }
        );

        let row = sqlx::query(
            "SELECT raw_payload, delivery_count, payload_conflict \
             FROM telegram_update_inbox WHERE bot_id = $1 AND update_id = 7",
        )
        .bind(bot_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.try_get::<Vec<u8>, _>("raw_payload")?, first.raw_payload);
        assert_eq!(row.try_get::<i64, _>("delivery_count")?, 2);
        assert!(row.try_get::<bool, _>("payload_conflict")?);

        let replay = store
            .materialize_update_batch(std::slice::from_ref(&first), &[])
            .await?;
        assert_eq!(replay.inserted, 0);
        assert_eq!(replay.duplicates, 1);
        let replayed_delivery_count: i64 = sqlx::query_scalar(
            "SELECT delivery_count FROM telegram_update_inbox \
             WHERE bot_id = $1 AND update_id = 7",
        )
        .bind(bot_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(replayed_delivery_count, 2);

        let next = update(bot_id, 8, 3, br#"{"update_id":8}"#, 3);
        store.materialize_update_batch(&[next], &[]).await?;
        let first_claim = store.claim_updates("storage-test", 10).await?;
        assert_eq!(first_claim.len(), 1, "same ordering key must serialize");
        assert_eq!(first_claim[0].update_id, 7);
        assert!(
            !store
                .complete_update(first_claim[0].id, first_claim[0].lease_token + 1)
                .await?,
            "wrong fencing token must not finish the row"
        );
        assert!(
            store
                .mark_update_state_applied(first_claim[0].id, first_claim[0].lease_token)
                .await?
        );
        assert!(
            store
                .complete_update(first_claim[0].id, first_claim[0].lease_token)
                .await?
        );

        let second_claim = store.claim_updates("storage-test", 10).await?;
        assert_eq!(second_claim.len(), 1);
        assert_eq!(second_claim[0].update_id, 8);
        assert!(
            store
                .ignore_update(
                    second_claim[0].id,
                    second_claim[0].lease_token,
                    "test_cleanup",
                )
                .await?
        );

        let third = update(bot_id, 9, 4, br#"{"update_id":9}"#, 4);
        store.materialize_update_batch(&[third], &[]).await?;
        let third_claim = store.claim_updates("storage-test", 10).await?;
        assert_eq!(third_claim.len(), 1);
        assert!(
            store
                .renew_update_lease(third_claim[0].id, third_claim[0].lease_token)
                .await?
        );
        assert!(
            store
                .mark_update_state_applied(third_claim[0].id, third_claim[0].lease_token)
                .await?
        );
        assert!(
            store
                .retry_update(
                    third_claim[0].id,
                    third_claim[0].lease_token,
                    OffsetDateTime::now_utc(),
                    "transient",
                    "retry from integration test",
                )
                .await?
        );
        let retried_claim = store.claim_updates("storage-test", 10).await?;
        assert_eq!(retried_claim.len(), 1);
        assert_eq!(retried_claim[0].update_id, 9);
        assert_eq!(retried_claim[0].attempt, 2);
        assert!(!retried_claim[0].needs_state_application());
        assert!(
            store
                .dead_letter_update(
                    retried_claim[0].id,
                    retried_claim[0].lease_token,
                    "deterministic",
                    "dead letter from integration test",
                )
                .await?
        );

        let stats = store.stats().await?;
        assert!(stats.completed >= 1);
        assert!(stats.ignored >= 1);
        assert!(stats.dead_letter >= 1);
        assert!(stats.duplicates >= 1);
        assert!(stats.payload_conflicts >= 1);
        assert!(stats.quarantined >= 1);
        let first_item = store.item(first_claim[0].id).await?.ok_or("item missing")?;
        assert_eq!(first_item.status, "completed");
        assert_eq!(first_item.payload_sha256, first.payload_sha256);
        let items = store.list_items(10, None, Some("dead_letter")).await?;
        assert!(items.iter().any(|item| item.update_id == 9));
        let attempts = store.attempts(retried_claim[0].id, 10).await?;
        assert_eq!(attempts.len(), 2);

        sqlx::query("DELETE FROM telegram_update_inbox WHERE bot_id = $1")
            .bind(bot_id)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM telegram_update_quarantine WHERE bot_id = $1")
            .bind(bot_id)
            .execute(&pool)
            .await?;
        Ok(())
    }
}
