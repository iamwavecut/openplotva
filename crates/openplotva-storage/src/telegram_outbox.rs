//! Durable Postgres outbox for Telegram operations.

use std::collections::HashMap;

use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};
use time::OffsetDateTime;

use crate::{SQL_ENSURE_CHAT_HISTORY_PARTITION, SQL_UPSERT_HISTORY_ENTRY, StorageError};

/// Conservative bind budget below PostgreSQL's protocol maximum.
pub const POSTGRES_OUTBOX_BIND_BUDGET: usize = 60_000;
/// Number of binds emitted for one outbound operation.
pub const OUTBOX_OPERATION_BINDS_PER_ROW: usize = 20;
/// Number of binds emitted for one media blob.
pub const OUTBOX_BLOB_BINDS_PER_ROW: usize = 5;
/// Outbound processing lease duration.
pub const OUTBOX_LEASE_SECONDS: i64 = 90;

const SQL_INSERT_BLOBS_PREFIX: &str = "INSERT INTO telegram_outbox_blobs \
    (sha256, media_type, file_name, bytes, metadata)";
const SQL_INSERT_BLOBS_SUFFIX: &str = " ON CONFLICT (sha256) DO UPDATE SET \
    sha256 = EXCLUDED.sha256 \
    WHERE telegram_outbox_blobs.bytes = EXCLUDED.bytes \
    RETURNING id, sha256";

const SQL_INSERT_OPERATIONS_PREFIX: &str = "INSERT INTO telegram_outbox (\
    operation_id, batch_id, part_index, bot_id, chat_id, thread_id, ordering_key, \
    causation_update_id, dialog_job_id, trigger_message_id, method_kind, payload_version, \
    original_payload, payload, blob_id, delivery_policy, protected, priority, available_at, expires_at)";
const SQL_INSERT_OPERATIONS_SUFFIX: &str = " ON CONFLICT (operation_id) DO UPDATE SET \
    operation_id = EXCLUDED.operation_id \
    WHERE telegram_outbox.batch_id = EXCLUDED.batch_id \
      AND telegram_outbox.part_index = EXCLUDED.part_index \
      AND telegram_outbox.bot_id = EXCLUDED.bot_id \
      AND telegram_outbox.chat_id IS NOT DISTINCT FROM EXCLUDED.chat_id \
      AND telegram_outbox.thread_id IS NOT DISTINCT FROM EXCLUDED.thread_id \
      AND telegram_outbox.ordering_key = EXCLUDED.ordering_key \
      AND telegram_outbox.causation_update_id IS NOT DISTINCT FROM EXCLUDED.causation_update_id \
      AND telegram_outbox.dialog_job_id IS NOT DISTINCT FROM EXCLUDED.dialog_job_id \
      AND telegram_outbox.trigger_message_id IS NOT DISTINCT FROM EXCLUDED.trigger_message_id \
      AND telegram_outbox.method_kind = EXCLUDED.method_kind \
      AND telegram_outbox.payload_version = EXCLUDED.payload_version \
      AND telegram_outbox.original_payload = EXCLUDED.original_payload \
      AND telegram_outbox.blob_id IS NOT DISTINCT FROM EXCLUDED.blob_id \
      AND telegram_outbox.delivery_policy = EXCLUDED.delivery_policy \
      AND telegram_outbox.protected = EXCLUDED.protected \
      AND telegram_outbox.priority = EXCLUDED.priority \
      AND telegram_outbox.expires_at IS NOT DISTINCT FROM EXCLUDED.expires_at \
    RETURNING id, operation_id, part_index, state, (xmax = 0) AS inserted";

const SQL_CLAIM_OPERATIONS: &str = r#"
WITH candidates AS (
    SELECT operation.id
    FROM telegram_outbox AS operation
    WHERE operation.state IN ('pending', 'retry_wait')
      AND operation.available_at <= statement_timestamp()
      AND (operation.expires_at IS NULL OR operation.expires_at > statement_timestamp())
      AND NOT EXISTS (
          SELECT 1
          FROM telegram_outbox AS earlier
          WHERE earlier.bot_id = operation.bot_id
            AND earlier.ordering_key = operation.ordering_key
            AND earlier.id < operation.id
            AND earlier.state IN ('pending', 'leased', 'retry_wait')
      )
      AND NOT EXISTS (
          SELECT 1
          FROM telegram_outbox AS prior_part
          WHERE prior_part.batch_id = operation.batch_id
            AND prior_part.part_index < operation.part_index
            AND prior_part.state <> 'delivered'
      )
    ORDER BY operation.priority DESC, operation.id
    FOR UPDATE OF operation SKIP LOCKED
    LIMIT $1
), claimed AS (
    UPDATE telegram_outbox AS operation
    SET state = 'leased',
        attempt_count = operation.attempt_count + 1,
        lease_token = operation.lease_token + 1,
        lease_owner = $2,
        leased_until = statement_timestamp() + interval '90 seconds',
        updated_at = statement_timestamp()
    FROM candidates
    WHERE operation.id = candidates.id
    RETURNING operation.*
), attempts AS (
    INSERT INTO telegram_outbox_attempts (
        outbox_id, attempt, lease_token, worker_id, claimed_at
    )
    SELECT id, attempt_count, lease_token, $2, statement_timestamp()
    FROM claimed
    RETURNING outbox_id
)
SELECT
    claimed.*,
    blob.sha256 AS blob_sha256,
    blob.media_type AS blob_media_type,
    blob.file_name AS blob_file_name,
    blob.bytes AS blob_bytes,
    blob.metadata AS blob_metadata
FROM claimed
JOIN attempts ON attempts.outbox_id = claimed.id
LEFT JOIN telegram_outbox_blobs AS blob ON blob.id = claimed.blob_id
ORDER BY claimed.priority DESC, claimed.id
"#;

/// Retry semantics for one Telegram method family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramDeliveryPolicy {
    Create,
    TargetIdempotent,
    Ephemeral,
    Financial,
}

impl TelegramDeliveryPolicy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::TargetIdempotent => "target_idempotent",
            Self::Ephemeral => "ephemeral",
            Self::Financial => "financial",
        }
    }
}

/// Optional media bytes referenced by one or more operations in a batch.
#[derive(Clone, Debug, PartialEq)]
pub struct TelegramOutboxBlobInput {
    pub sha256: Vec<u8>,
    pub media_type: Option<String>,
    pub file_name: Option<String>,
    pub bytes: Vec<u8>,
    pub metadata: Value,
}

impl TelegramOutboxBlobInput {
    /// Construct a blob and compute its content digest.
    #[must_use]
    pub fn new(
        bytes: Vec<u8>,
        media_type: Option<String>,
        file_name: Option<String>,
        metadata: Value,
    ) -> Self {
        let sha256 = Sha256::digest(&bytes).to_vec();
        Self {
            sha256,
            media_type,
            file_name,
            bytes,
            metadata,
        }
    }
}

/// One ordered method call in an outbound batch.
#[derive(Clone, Debug, PartialEq)]
pub struct TelegramOutboxPartInput {
    pub method_kind: String,
    pub payload_version: i16,
    pub payload: Value,
    pub blob: Option<TelegramOutboxBlobInput>,
    pub available_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
}

/// Canonical history row derived from a Telegram delivery receipt.
#[derive(Clone, Debug, PartialEq)]
pub struct TelegramReceiptHistoryEntry {
    pub bucket_day: time::Date,
    pub chat_id: i64,
    pub thread_id: i32,
    pub message_id: i32,
    pub entry_id: String,
    pub kind: String,
    pub role: String,
    pub occurred_at: OffsetDateTime,
    pub sender_id: i64,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PendingTelegramHistoryReceipt {
    pub outbox_id: i64,
    pub batch_id: String,
    pub bot_id: i64,
    pub receipt: Value,
}

/// Immutable batch metadata plus ordered parts.
#[derive(Clone, Debug, PartialEq)]
pub struct TelegramOutboxBatchInput {
    pub batch_id: String,
    pub bot_id: i64,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub ordering_key: String,
    pub causation_update_id: Option<i64>,
    pub dialog_job_id: Option<i64>,
    pub trigger_message_id: Option<i64>,
    pub delivery_policy: TelegramDeliveryPolicy,
    pub protected: bool,
    pub priority: i32,
    pub parts: Vec<TelegramOutboxPartInput>,
}

/// Durable identity and current state of an enqueued part.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuedTelegramOutboxPart {
    pub id: i64,
    pub operation_id: String,
    pub part_index: i32,
    pub state: String,
    pub inserted: bool,
}

/// Receipt returned once every part is committed or already existed identically.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuedTelegramOutboxBatch {
    pub batch_id: String,
    pub parts: Vec<QueuedTelegramOutboxPart>,
}

/// Blob material needed by the transport worker.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaimedTelegramOutboxBlob {
    pub sha256: Vec<u8>,
    pub media_type: Option<String>,
    pub file_name: Option<String>,
    pub bytes: Vec<u8>,
    pub metadata: Value,
}

/// One fenced outbound lease.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaimedTelegramOutboxOperation {
    pub id: i64,
    pub operation_id: String,
    pub batch_id: String,
    pub part_index: i32,
    pub bot_id: i64,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub ordering_key: String,
    pub causation_update_id: Option<i64>,
    pub dialog_job_id: Option<i64>,
    pub trigger_message_id: Option<i64>,
    pub method_kind: String,
    pub payload_version: i16,
    pub payload: Value,
    pub blob: Option<ClaimedTelegramOutboxBlob>,
    pub delivery_policy: String,
    pub protected: bool,
    pub expires_at: Option<OffsetDateTime>,
    pub attempt: i32,
    pub lease_token: i64,
    pub leased_until: OffsetDateTime,
}

#[derive(Clone, Debug)]
struct PreparedOutboxPart {
    operation_id: String,
    part_index: i32,
    blob_id: Option<i64>,
    part: TelegramOutboxPartInput,
}

/// Stable operation ID derived solely from immutable batch coordinates.
#[must_use]
pub fn deterministic_telegram_operation_id(
    batch_id: &str,
    method_kind: &str,
    part_index: i32,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"openplotva:telegram-outbox:v1\0");
    hasher.update(batch_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(method_kind.as_bytes());
    hasher.update(b"\0");
    hasher.update(part_index.to_be_bytes());
    format!("tgop:v1:{}", hex::encode(hasher.finalize()))
}

/// Postgres-backed Telegram outbox store.
#[derive(Clone, Debug)]
pub struct PostgresTelegramOutboxStore {
    pool: PgPool,
}

impl PostgresTelegramOutboxStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Atomically insert all distinct blobs and every ordered operation.
    pub async fn enqueue_batch(
        &self,
        batch: &TelegramOutboxBatchInput,
    ) -> Result<QueuedTelegramOutboxBatch, StorageError> {
        enqueue_telegram_outbox_batch(&self.pool, batch).await
    }

    /// Claim at most one currently-sendable operation per ordering key.
    pub async fn claim_operations(
        &self,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<ClaimedTelegramOutboxOperation>, StorageError> {
        let limit = i64::try_from(limit.clamp(1, 256)).unwrap_or(256);
        let rows = sqlx::query(SQL_CLAIM_OPERATIONS)
            .bind(limit)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(claimed_operation_from_row).collect()
    }
}

/// Atomically enqueue one immutable outbound batch.
pub async fn enqueue_telegram_outbox_batch(
    pool: &PgPool,
    batch: &TelegramOutboxBatchInput,
) -> Result<QueuedTelegramOutboxBatch, StorageError> {
    if batch.parts.is_empty() {
        return Err(StorageError::TelegramOutboxEmptyBatch);
    }
    let max_parts = POSTGRES_OUTBOX_BIND_BUDGET / OUTBOX_OPERATION_BINDS_PER_ROW;
    if batch.parts.len() > max_parts {
        return Err(StorageError::TelegramOutboxBatchTooLarge {
            rows: batch.parts.len(),
            max_rows: max_parts,
        });
    }
    let blobs = prepare_blobs(&batch.parts)?;
    let max_blobs = POSTGRES_OUTBOX_BIND_BUDGET / OUTBOX_BLOB_BINDS_PER_ROW;
    if blobs.len() > max_blobs {
        return Err(StorageError::TelegramOutboxBatchTooLarge {
            rows: blobs.len(),
            max_rows: max_blobs,
        });
    }

    let mut tx = pool.begin().await?;
    let mut blob_ids = HashMap::with_capacity(blobs.len());
    if !blobs.is_empty() {
        let mut builder = outbox_blob_insert_builder(&blobs);
        let rows = builder.build().fetch_all(&mut *tx).await?;
        if rows.len() != blobs.len() {
            return Err(StorageError::TelegramOutboxBlobConflict);
        }
        for row in rows {
            blob_ids.insert(
                row.try_get::<Vec<u8>, _>("sha256")?,
                row.try_get::<i64, _>("id")?,
            );
        }
    }

    let parts = prepare_parts(batch, &blob_ids)?;
    let mut builder = outbox_operation_insert_builder(batch, &parts);
    let rows = builder.build().fetch_all(&mut *tx).await?;
    if rows.len() != parts.len() {
        return Err(StorageError::TelegramOutboxIdempotencyConflict {
            batch_id: batch.batch_id.clone(),
        });
    }
    let mut queued = rows
        .into_iter()
        .map(|row| {
            Ok(QueuedTelegramOutboxPart {
                id: row.try_get("id")?,
                operation_id: row.try_get("operation_id")?,
                part_index: row.try_get("part_index")?,
                state: row.try_get("state")?,
                inserted: row.try_get("inserted")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    queued.sort_by_key(|part| part.part_index);
    tx.commit().await?;
    Ok(QueuedTelegramOutboxBatch {
        batch_id: batch.batch_id.clone(),
        parts: queued,
    })
}

fn prepare_blobs(
    parts: &[TelegramOutboxPartInput],
) -> Result<Vec<TelegramOutboxBlobInput>, StorageError> {
    let mut index_by_hash: HashMap<Vec<u8>, usize> = HashMap::with_capacity(parts.len());
    let mut blobs: Vec<TelegramOutboxBlobInput> = Vec::new();
    for blob in parts.iter().filter_map(|part| part.blob.as_ref()) {
        if blob.sha256.len() != 32 {
            return Err(StorageError::TelegramOutboxInvalidBlobHash);
        }
        if let Some(index) = index_by_hash.get(&blob.sha256).copied() {
            if blobs[index].bytes != blob.bytes {
                return Err(StorageError::TelegramOutboxBlobConflict);
            }
            continue;
        }
        index_by_hash.insert(blob.sha256.clone(), blobs.len());
        blobs.push(blob.clone());
    }
    Ok(blobs)
}

fn prepare_parts(
    batch: &TelegramOutboxBatchInput,
    blob_ids: &HashMap<Vec<u8>, i64>,
) -> Result<Vec<PreparedOutboxPart>, StorageError> {
    batch
        .parts
        .iter()
        .enumerate()
        .map(|(index, part)| {
            let part_index =
                i32::try_from(index).map_err(|_| StorageError::TelegramOutboxBatchTooLarge {
                    rows: batch.parts.len(),
                    max_rows: i32::MAX as usize,
                })?;
            let blob_id = part
                .blob
                .as_ref()
                .map(|blob| {
                    blob_ids
                        .get(&blob.sha256)
                        .copied()
                        .ok_or(StorageError::TelegramOutboxBlobConflict)
                })
                .transpose()?;
            Ok(PreparedOutboxPart {
                operation_id: deterministic_telegram_operation_id(
                    &batch.batch_id,
                    &part.method_kind,
                    part_index,
                ),
                part_index,
                blob_id,
                part: part.clone(),
            })
        })
        .collect()
}

fn outbox_blob_insert_builder(blobs: &[TelegramOutboxBlobInput]) -> QueryBuilder<Postgres> {
    let mut builder = QueryBuilder::new(SQL_INSERT_BLOBS_PREFIX);
    builder.push(" ");
    builder.push_values(blobs.iter(), |mut row, blob| {
        row.push_bind(blob.sha256.clone())
            .push_bind(blob.media_type.clone())
            .push_bind(blob.file_name.clone())
            .push_bind(blob.bytes.clone())
            .push_bind(sqlx::types::Json(blob.metadata.clone()));
    });
    builder.push(SQL_INSERT_BLOBS_SUFFIX);
    builder
}

fn outbox_operation_insert_builder(
    batch: &TelegramOutboxBatchInput,
    parts: &[PreparedOutboxPart],
) -> QueryBuilder<Postgres> {
    let mut builder = QueryBuilder::new(SQL_INSERT_OPERATIONS_PREFIX);
    builder.push(" ");
    builder.push_values(parts.iter(), |mut row, prepared| {
        let part = &prepared.part;
        row.push_bind(prepared.operation_id.clone())
            .push_bind(batch.batch_id.clone())
            .push_bind(prepared.part_index)
            .push_bind(batch.bot_id)
            .push_bind(batch.chat_id)
            .push_bind(batch.thread_id)
            .push_bind(batch.ordering_key.clone())
            .push_bind(batch.causation_update_id)
            .push_bind(batch.dialog_job_id)
            .push_bind(batch.trigger_message_id)
            .push_bind(part.method_kind.clone())
            .push_bind(part.payload_version)
            .push_bind(sqlx::types::Json(part.payload.clone()))
            .push_bind(sqlx::types::Json(part.payload.clone()))
            .push_bind(prepared.blob_id)
            .push_bind(batch.delivery_policy.as_str())
            .push_bind(batch.protected)
            .push_bind(batch.priority)
            .push_bind(part.available_at)
            .push_bind(part.expires_at);
    });
    builder.push(SQL_INSERT_OPERATIONS_SUFFIX);
    builder
}

const SQL_RENEW_OPERATION_LEASE: &str = r#"
UPDATE telegram_outbox
SET leased_until = statement_timestamp() + interval '90 seconds',
    updated_at = statement_timestamp()
WHERE id = $1
  AND lease_token = $2
  AND state = 'leased'
  AND leased_until > statement_timestamp()
RETURNING id
"#;

const SQL_START_OPERATION_REQUEST: &str = r#"
WITH active AS (
    UPDATE telegram_outbox
    SET updated_at = statement_timestamp()
    WHERE id = $1
      AND lease_token = $2
      AND state = 'leased'
      AND leased_until > statement_timestamp()
    RETURNING id, attempt_count
), started AS (
    UPDATE telegram_outbox_attempts AS attempt
    SET request_started_at = COALESCE(attempt.request_started_at, statement_timestamp())
    FROM active
    WHERE attempt.outbox_id = active.id
      AND attempt.attempt = active.attempt_count
    RETURNING attempt.outbox_id
)
SELECT EXISTS(SELECT 1 FROM started)
"#;

const SQL_DELIVER_OPERATION: &str = r#"
WITH finished AS (
    UPDATE telegram_outbox AS operation
    SET state = 'delivered',
        response_kind = $3,
        telegram_message_ids = $4,
        receipt = $5,
        confirmed_at = statement_timestamp(),
        lease_owner = NULL,
        leased_until = NULL,
        last_error_class = 'history_pending',
        last_error = 'Telegram receipt committed; canonical history pending',
        updated_at = statement_timestamp()
    WHERE operation.id = $1
      AND operation.lease_token = $2
      AND operation.state = 'leased'
      AND operation.leased_until > statement_timestamp()
      AND EXISTS (
          SELECT 1 FROM telegram_outbox_attempts AS attempt
          WHERE attempt.outbox_id = operation.id
            AND attempt.attempt = operation.attempt_count
            AND attempt.request_started_at IS NOT NULL
      )
    RETURNING operation.id, operation.attempt_count
), attempt_finished AS (
    UPDATE telegram_outbox_attempts AS attempt
    SET response_received_at = statement_timestamp(),
        finished_at = statement_timestamp(),
        outcome = 'delivered',
        latency_ms = GREATEST(
            0,
            (EXTRACT(EPOCH FROM (statement_timestamp() - attempt.request_started_at)) * 1000)::bigint
        )
    FROM finished
    WHERE attempt.outbox_id = finished.id
      AND attempt.attempt = finished.attempt_count
    RETURNING attempt.outbox_id
)
SELECT EXISTS(SELECT 1 FROM attempt_finished)
"#;

const SQL_PENDING_HISTORY_RECEIPTS: &str = r#"
SELECT id, batch_id, bot_id, receipt
FROM telegram_outbox
WHERE state = 'delivered'
  AND last_error_class = 'history_pending'
ORDER BY confirmed_at, id
LIMIT $1
"#;

const SQL_MARK_HISTORY_COMMITTED: &str = r#"
UPDATE telegram_outbox
SET last_error_class = NULL,
    last_error = NULL,
    updated_at = statement_timestamp()
WHERE id = $1
  AND state = 'delivered'
  AND last_error_class = 'history_pending'
RETURNING id
"#;

const SQL_RETRY_OPERATION: &str = r#"
WITH finished AS (
    UPDATE telegram_outbox
    SET state = 'retry_wait',
        available_at = $3,
        last_error_class = left($4, 128),
        last_error = left($5, 2048),
        payload = COALESCE($7, payload),
        lease_owner = NULL,
        leased_until = NULL,
        updated_at = statement_timestamp()
    WHERE id = $1
      AND lease_token = $2
      AND state = 'leased'
      AND leased_until > statement_timestamp()
    RETURNING id, attempt_count
), attempt_finished AS (
    UPDATE telegram_outbox_attempts AS attempt
    SET response_received_at = CASE
            WHEN attempt.request_started_at IS NULL THEN NULL
            ELSE statement_timestamp()
        END,
        finished_at = statement_timestamp(),
        outcome = 'retry',
        http_status = $6,
        latency_ms = CASE
            WHEN attempt.request_started_at IS NULL THEN NULL
            ELSE GREATEST(
                0,
                (EXTRACT(EPOCH FROM (statement_timestamp() - attempt.request_started_at)) * 1000)::bigint
            )
        END,
        error_class = left($4, 128),
        error = left($5, 2048)
    FROM finished
    WHERE attempt.outbox_id = finished.id
      AND attempt.attempt = finished.attempt_count
    RETURNING attempt.outbox_id
)
SELECT EXISTS(SELECT 1 FROM attempt_finished)
"#;

const SQL_AMBIGUOUS_OPERATION: &str = r#"
WITH finished AS (
    UPDATE telegram_outbox AS operation
    SET state = 'ambiguous',
        last_error_class = left($3, 128),
        last_error = left($4, 2048),
        lease_owner = NULL,
        leased_until = NULL,
        updated_at = statement_timestamp()
    WHERE operation.id = $1
      AND operation.lease_token = $2
      AND operation.state = 'leased'
      AND operation.leased_until > statement_timestamp()
      AND EXISTS (
          SELECT 1 FROM telegram_outbox_attempts AS attempt
          WHERE attempt.outbox_id = operation.id
            AND attempt.attempt = operation.attempt_count
            AND attempt.request_started_at IS NOT NULL
      )
    RETURNING operation.id, operation.batch_id, operation.part_index, operation.attempt_count
), cancelled_parts AS (
    UPDATE telegram_outbox AS remaining
    SET state = 'cancelled',
        last_error_class = 'batch_predecessor_ambiguous',
        last_error = 'an earlier batch part has ambiguous delivery',
        updated_at = statement_timestamp()
    FROM finished
    WHERE remaining.batch_id = finished.batch_id
      AND remaining.part_index > finished.part_index
      AND remaining.state IN ('pending', 'retry_wait')
    RETURNING remaining.id
), attempt_finished AS (
    UPDATE telegram_outbox_attempts AS attempt
    SET response_received_at = statement_timestamp(),
        finished_at = statement_timestamp(),
        outcome = 'ambiguous',
        error_class = left($3, 128),
        error = left($4, 2048),
        latency_ms = GREATEST(
            0,
            (EXTRACT(EPOCH FROM (statement_timestamp() - attempt.request_started_at)) * 1000)::bigint
        )
    FROM finished
    WHERE attempt.outbox_id = finished.id
      AND attempt.attempt = finished.attempt_count
    RETURNING attempt.outbox_id
)
SELECT EXISTS(SELECT 1 FROM attempt_finished)
"#;

const SQL_DEAD_LETTER_OPERATION: &str = r#"
WITH finished AS (
    UPDATE telegram_outbox
    SET state = 'dead_letter',
        last_error_class = left($3, 128),
        last_error = left($4, 2048),
        lease_owner = NULL,
        leased_until = NULL,
        updated_at = statement_timestamp()
    WHERE id = $1
      AND lease_token = $2
      AND state = 'leased'
      AND leased_until > statement_timestamp()
    RETURNING id, batch_id, part_index, attempt_count
), cancelled_parts AS (
    UPDATE telegram_outbox AS remaining
    SET state = 'cancelled',
        last_error_class = 'batch_predecessor_dead_letter',
        last_error = 'an earlier batch part reached dead letter',
        updated_at = statement_timestamp()
    FROM finished
    WHERE remaining.batch_id = finished.batch_id
      AND remaining.part_index > finished.part_index
      AND remaining.state IN ('pending', 'retry_wait')
    RETURNING remaining.id
), attempt_finished AS (
    UPDATE telegram_outbox_attempts AS attempt
    SET response_received_at = CASE
            WHEN attempt.request_started_at IS NULL THEN NULL
            ELSE statement_timestamp()
        END,
        finished_at = statement_timestamp(),
        outcome = 'dead_letter',
        http_status = $5,
        error_class = left($3, 128),
        error = left($4, 2048),
        latency_ms = CASE
            WHEN attempt.request_started_at IS NULL THEN NULL
            ELSE GREATEST(
                0,
                (EXTRACT(EPOCH FROM (statement_timestamp() - attempt.request_started_at)) * 1000)::bigint
            )
        END
    FROM finished
    WHERE attempt.outbox_id = finished.id
      AND attempt.attempt = finished.attempt_count
    RETURNING attempt.outbox_id
)
SELECT EXISTS(SELECT 1 FROM attempt_finished)
"#;

impl PostgresTelegramOutboxStore {
    /// Extend a live lease. Superseded workers cannot renew it.
    pub async fn renew_operation_lease(
        &self,
        outbox_id: i64,
        lease_token: i64,
    ) -> Result<bool, StorageError> {
        let id = sqlx::query_scalar::<_, i64>(SQL_RENEW_OPERATION_LEASE)
            .bind(outbox_id)
            .bind(lease_token)
            .fetch_optional(&self.pool)
            .await?;
        Ok(id.is_some())
    }

    /// Persist the point after which a create request may have reached Telegram.
    pub async fn mark_request_started(
        &self,
        outbox_id: i64,
        lease_token: i64,
    ) -> Result<bool, StorageError> {
        outbox_bool_transition(
            &self.pool,
            SQL_START_OPERATION_REQUEST,
            outbox_id,
            lease_token,
        )
        .await
    }

    /// Record a real Telegram acknowledgement and its receipt.
    pub async fn mark_delivered(
        &self,
        outbox_id: i64,
        lease_token: i64,
        response_kind: &str,
        telegram_message_ids: &[i64],
        receipt: &Value,
    ) -> Result<bool, StorageError> {
        sqlx::query_scalar::<_, bool>(SQL_DELIVER_OPERATION)
            .bind(outbox_id)
            .bind(lease_token)
            .bind(response_kind)
            .bind(telegram_message_ids)
            .bind(sqlx::types::Json(receipt))
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    /// Durably record the Telegram acknowledgement, then commit canonical history.
    /// A history failure leaves a delivered `history_pending` marker for repair.
    pub async fn mark_delivered_with_history(
        &self,
        outbox_id: i64,
        lease_token: i64,
        response_kind: &str,
        telegram_message_ids: &[i64],
        receipt: &Value,
        history_entries: &[TelegramReceiptHistoryEntry],
    ) -> Result<bool, StorageError> {
        let delivered = self
            .mark_delivered(
                outbox_id,
                lease_token,
                response_kind,
                telegram_message_ids,
                receipt,
            )
            .await?;
        if !delivered {
            return Ok(false);
        }
        self.commit_pending_history(outbox_id, history_entries)
            .await?;
        Ok(true)
    }

    pub async fn pending_history_receipts(
        &self,
        limit: usize,
    ) -> Result<Vec<PendingTelegramHistoryReceipt>, StorageError> {
        let rows = sqlx::query(SQL_PENDING_HISTORY_RECEIPTS)
            .bind(i64::try_from(limit.max(1)).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(PendingTelegramHistoryReceipt {
                    outbox_id: row.try_get("id")?,
                    batch_id: row.try_get("batch_id")?,
                    bot_id: row.try_get("bot_id")?,
                    receipt: row.try_get::<sqlx::types::Json<Value>, _>("receipt")?.0,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(StorageError::from)
    }

    pub async fn commit_pending_history(
        &self,
        outbox_id: i64,
        history_entries: &[TelegramReceiptHistoryEntry],
    ) -> Result<bool, StorageError> {
        let mut transaction = self.pool.begin().await?;
        let mut ensured_days = Vec::new();
        for entry in history_entries {
            if !ensured_days.contains(&entry.bucket_day) {
                sqlx::query(SQL_ENSURE_CHAT_HISTORY_PARTITION)
                    .bind(entry.bucket_day)
                    .execute(&mut *transaction)
                    .await?;
                ensured_days.push(entry.bucket_day);
            }
            sqlx::query(SQL_UPSERT_HISTORY_ENTRY)
                .bind(entry.bucket_day)
                .bind(entry.chat_id)
                .bind(entry.thread_id)
                .bind(entry.message_id)
                .bind(&entry.entry_id)
                .bind(&entry.kind)
                .bind(&entry.role)
                .bind(entry.occurred_at)
                .bind(entry.sender_id)
                .bind(sqlx::types::Json(&entry.payload))
                .execute(&mut *transaction)
                .await?;
        }
        let committed = sqlx::query_scalar::<_, i64>(SQL_MARK_HISTORY_COMMITTED)
            .bind(outbox_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
        transaction.commit().await?;
        Ok(committed)
    }

    /// Release a definitive transient failure. `replacement_payload` supports
    /// the one real reply-missing retry without mutating the operation identity.
    #[allow(clippy::too_many_arguments)]
    pub async fn retry_operation(
        &self,
        outbox_id: i64,
        lease_token: i64,
        available_at: OffsetDateTime,
        error_class: &str,
        error: &str,
        http_status: Option<i32>,
        replacement_payload: Option<&Value>,
    ) -> Result<bool, StorageError> {
        let replacement_payload = replacement_payload.map(sqlx::types::Json);
        sqlx::query_scalar::<_, bool>(SQL_RETRY_OPERATION)
            .bind(outbox_id)
            .bind(lease_token)
            .bind(available_at)
            .bind(error_class)
            .bind(error)
            .bind(http_status)
            .bind(replacement_payload)
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    /// Mark a started request ambiguous and cancel unsent later parts.
    pub async fn mark_ambiguous(
        &self,
        outbox_id: i64,
        lease_token: i64,
        error_class: &str,
        error: &str,
    ) -> Result<bool, StorageError> {
        sqlx::query_scalar::<_, bool>(SQL_AMBIGUOUS_OPERATION)
            .bind(outbox_id)
            .bind(lease_token)
            .bind(error_class)
            .bind(error)
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    /// Mark an exhausted or deterministic failure and cancel later parts.
    pub async fn dead_letter_operation(
        &self,
        outbox_id: i64,
        lease_token: i64,
        error_class: &str,
        error: &str,
        http_status: Option<i32>,
    ) -> Result<bool, StorageError> {
        sqlx::query_scalar::<_, bool>(SQL_DEAD_LETTER_OPERATION)
            .bind(outbox_id)
            .bind(lease_token)
            .bind(error_class)
            .bind(error)
            .bind(http_status)
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::from)
    }
}

async fn outbox_bool_transition(
    pool: &PgPool,
    statement: &'static str,
    outbox_id: i64,
    lease_token: i64,
) -> Result<bool, StorageError> {
    sqlx::query_scalar::<_, bool>(statement)
        .bind(outbox_id)
        .bind(lease_token)
        .fetch_one(pool)
        .await
        .map_err(StorageError::from)
}

fn claimed_operation_from_row(row: PgRow) -> Result<ClaimedTelegramOutboxOperation, StorageError> {
    let blob_sha256: Option<Vec<u8>> = row.try_get("blob_sha256")?;
    let blob = blob_sha256
        .map(|sha256| -> Result<ClaimedTelegramOutboxBlob, sqlx::Error> {
            Ok(ClaimedTelegramOutboxBlob {
                sha256,
                media_type: row.try_get("blob_media_type")?,
                file_name: row.try_get("blob_file_name")?,
                bytes: row.try_get("blob_bytes")?,
                metadata: row
                    .try_get::<sqlx::types::Json<Value>, _>("blob_metadata")?
                    .0,
            })
        })
        .transpose()?;
    Ok(ClaimedTelegramOutboxOperation {
        id: row.try_get("id")?,
        operation_id: row.try_get("operation_id")?,
        batch_id: row.try_get("batch_id")?,
        part_index: row.try_get("part_index")?,
        bot_id: row.try_get("bot_id")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        ordering_key: row.try_get("ordering_key")?,
        causation_update_id: row.try_get("causation_update_id")?,
        dialog_job_id: row.try_get("dialog_job_id")?,
        trigger_message_id: row.try_get("trigger_message_id")?,
        method_kind: row.try_get("method_kind")?,
        payload_version: row.try_get("payload_version")?,
        payload: row.try_get::<sqlx::types::Json<Value>, _>("payload")?.0,
        blob,
        delivery_policy: row.try_get("delivery_policy")?,
        protected: row.try_get("protected")?,
        expires_at: row.try_get("expires_at")?,
        attempt: row.try_get("attempt_count")?,
        lease_token: row.try_get("lease_token")?,
        leased_until: row.try_get("leased_until")?,
    })
}

const SQL_RECOVER_EXPIRED_LEASES: &str = r#"
WITH stale AS (
    SELECT
        operation.id,
        operation.attempt_count,
        operation.batch_id,
        operation.part_index,
        operation.delivery_policy,
        operation.expires_at,
        attempt.request_started_at
    FROM telegram_outbox AS operation
    LEFT JOIN telegram_outbox_attempts AS attempt
      ON attempt.outbox_id = operation.id
     AND attempt.attempt = operation.attempt_count
    WHERE operation.state = 'leased'
      AND operation.leased_until <= statement_timestamp()
    ORDER BY operation.leased_until, operation.id
    FOR UPDATE OF operation SKIP LOCKED
    LIMIT $1
), recovered AS (
    UPDATE telegram_outbox AS operation
    SET state = CASE
            WHEN stale.expires_at IS NOT NULL AND stale.expires_at <= statement_timestamp()
                THEN 'expired'
            WHEN stale.request_started_at IS NULL
                THEN 'retry_wait'
            WHEN stale.delivery_policy IN ('target_idempotent', 'ephemeral')
                THEN 'retry_wait'
            ELSE 'ambiguous'
        END,
        available_at = statement_timestamp(),
        lease_owner = NULL,
        leased_until = NULL,
        last_error_class = 'lease_expired',
        last_error = CASE
            WHEN stale.request_started_at IS NULL
                THEN 'worker lease expired before request start'
            ELSE 'worker lease expired after request start'
        END,
        updated_at = statement_timestamp()
    FROM stale
    WHERE operation.id = stale.id
    RETURNING operation.id, operation.attempt_count, operation.batch_id,
              operation.part_index, operation.state
), cancelled_parts AS (
    UPDATE telegram_outbox AS remaining
    SET state = 'cancelled',
        last_error_class = 'batch_predecessor_terminal',
        last_error = 'an earlier batch part became ambiguous or expired',
        updated_at = statement_timestamp()
    FROM recovered
    WHERE recovered.state IN ('ambiguous', 'expired')
      AND remaining.batch_id = recovered.batch_id
      AND remaining.part_index > recovered.part_index
      AND remaining.state IN ('pending', 'retry_wait')
    RETURNING remaining.id
), attempt_finished AS (
    UPDATE telegram_outbox_attempts AS attempt
    SET finished_at = statement_timestamp(),
        outcome = recovered.state,
        error_class = 'lease_expired',
        error = CASE
            WHEN attempt.request_started_at IS NULL
                THEN 'worker lease expired before request start'
            ELSE 'worker lease expired after request start'
        END
    FROM recovered
    WHERE attempt.outbox_id = recovered.id
      AND attempt.attempt = recovered.attempt_count
    RETURNING attempt.outbox_id
)
SELECT recovered.state, count(*)::bigint AS count
FROM recovered
JOIN attempt_finished ON attempt_finished.outbox_id = recovered.id
GROUP BY recovered.state
"#;

const SQL_EXPIRE_DUE_OPERATIONS: &str = r#"
WITH candidates AS (
    SELECT id
    FROM telegram_outbox
    WHERE state IN ('pending', 'retry_wait')
      AND expires_at IS NOT NULL
      AND expires_at <= statement_timestamp()
    ORDER BY expires_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
), expired AS (
    UPDATE telegram_outbox AS operation
    SET state = 'expired',
        last_error_class = 'ttl_expired',
        last_error = 'operation TTL expired before delivery',
        updated_at = statement_timestamp()
    FROM candidates
    WHERE operation.id = candidates.id
    RETURNING operation.id, operation.batch_id, operation.part_index
), cancelled_parts AS (
    UPDATE telegram_outbox AS remaining
    SET state = 'cancelled',
        last_error_class = 'batch_predecessor_expired',
        last_error = 'an earlier batch part expired',
        updated_at = statement_timestamp()
    FROM expired
    WHERE remaining.batch_id = expired.batch_id
      AND remaining.part_index > expired.part_index
      AND remaining.state IN ('pending', 'retry_wait')
    RETURNING remaining.id
)
SELECT count(*)::bigint FROM expired
"#;

const SQL_CANCEL_OPERATION: &str = r#"
WITH cancelled AS (
    UPDATE telegram_outbox
    SET state = 'cancelled',
        last_error_class = 'operator_cancelled',
        last_error = left($2, 2048),
        lease_owner = NULL,
        leased_until = NULL,
        updated_at = statement_timestamp()
    WHERE operation_id = $1
      AND (
          state IN ('pending', 'retry_wait', 'ambiguous', 'dead_letter')
          OR (state = 'leased' AND leased_until <= statement_timestamp())
      )
    RETURNING id, batch_id, part_index
), cancelled_parts AS (
    UPDATE telegram_outbox AS remaining
    SET state = 'cancelled',
        last_error_class = 'batch_cancelled',
        last_error = 'an earlier batch part was cancelled by an operator',
        updated_at = statement_timestamp()
    FROM cancelled
    WHERE remaining.batch_id = cancelled.batch_id
      AND remaining.part_index > cancelled.part_index
      AND remaining.state IN ('pending', 'retry_wait')
    RETURNING remaining.id
)
SELECT EXISTS(SELECT 1 FROM cancelled)
"#;

const SQL_OUTBOX_STATS: &str = r#"
SELECT
    count(*) FILTER (WHERE state = 'pending')::bigint AS pending,
    count(*) FILTER (WHERE state = 'leased')::bigint AS leased,
    count(*) FILTER (WHERE state = 'retry_wait')::bigint AS retry_wait,
    count(*) FILTER (WHERE state = 'delivered')::bigint AS delivered,
    count(*) FILTER (WHERE state = 'ambiguous')::bigint AS ambiguous,
    count(*) FILTER (WHERE state = 'dead_letter')::bigint AS dead_letter,
    count(*) FILTER (WHERE state = 'expired')::bigint AS expired,
    count(*) FILTER (WHERE state = 'cancelled')::bigint AS cancelled,
    count(*) FILTER (
        WHERE protected
          AND (state IN ('pending', 'leased', 'retry_wait', 'ambiguous', 'dead_letter')
               OR last_error_class = 'history_pending')
    )::bigint AS protected_unresolved,
    min(created_at) FILTER (WHERE state IN ('pending', 'retry_wait')) AS oldest_pending_at,
    min(leased_until) FILTER (WHERE state = 'leased') AS oldest_lease_expiry
FROM telegram_outbox
"#;

const SQL_LIST_OUTBOX_ITEMS: &str = r#"
SELECT id, operation_id, batch_id, part_index, bot_id, chat_id, thread_id,
       ordering_key, causation_update_id, dialog_job_id, trigger_message_id,
       method_kind, delivery_policy, protected, priority, state, available_at,
       expires_at, attempt_count, lease_owner, leased_until, last_error_class,
       last_error, response_kind, telegram_message_ids, receipt, confirmed_at,
       created_at, updated_at
FROM telegram_outbox
WHERE ($1::bigint IS NULL OR id < $1)
  AND ($2::text IS NULL OR state = $2)
ORDER BY id DESC
LIMIT $3
"#;

const SQL_GET_OUTBOX_ITEM: &str = r#"
SELECT id, operation_id, batch_id, part_index, bot_id, chat_id, thread_id,
       ordering_key, causation_update_id, dialog_job_id, trigger_message_id,
       method_kind, delivery_policy, protected, priority, state, available_at,
       expires_at, attempt_count, lease_owner, leased_until, last_error_class,
       last_error, response_kind, telegram_message_ids, receipt, confirmed_at,
       created_at, updated_at
FROM telegram_outbox
WHERE operation_id = $1
"#;

const SQL_LIST_OUTBOX_ATTEMPTS: &str = r#"
SELECT attempt, lease_token, worker_id, claimed_at, request_started_at,
       response_received_at, finished_at, outcome, http_status, latency_ms,
       error_class, error
FROM telegram_outbox_attempts
WHERE outbox_id = $1
ORDER BY attempt DESC
LIMIT $2
"#;

/// Outcomes from reclaiming crashed outbound workers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TelegramOutboxRecoveryReport {
    pub retry_wait: u64,
    pub ambiguous: u64,
    pub expired: u64,
}

/// Aggregate operator diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TelegramOutboxStats {
    pub pending: i64,
    pub leased: i64,
    pub retry_wait: i64,
    pub delivered: i64,
    pub ambiguous: i64,
    pub dead_letter: i64,
    pub expired: i64,
    pub cancelled: i64,
    pub protected_unresolved: i64,
    pub oldest_pending_at: Option<OffsetDateTime>,
    pub oldest_lease_expiry: Option<OffsetDateTime>,
}

/// One operator-visible outbox row without raw Telegram payload or media bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct TelegramOutboxItem {
    pub id: i64,
    pub operation_id: String,
    pub batch_id: String,
    pub part_index: i32,
    pub bot_id: i64,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub ordering_key: String,
    pub causation_update_id: Option<i64>,
    pub dialog_job_id: Option<i64>,
    pub trigger_message_id: Option<i64>,
    pub method_kind: String,
    pub delivery_policy: String,
    pub protected: bool,
    pub priority: i32,
    pub state: String,
    pub available_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
    pub attempt_count: i32,
    pub lease_owner: Option<String>,
    pub leased_until: Option<OffsetDateTime>,
    pub last_error_class: Option<String>,
    pub last_error: Option<String>,
    pub response_kind: Option<String>,
    pub telegram_message_ids: Vec<i64>,
    pub receipt: Option<Value>,
    pub confirmed_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// One durable network attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct TelegramOutboxAttempt {
    pub attempt: i32,
    pub lease_token: i64,
    pub worker_id: String,
    pub claimed_at: OffsetDateTime,
    pub request_started_at: Option<OffsetDateTime>,
    pub response_received_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub outcome: Option<String>,
    pub http_status: Option<i32>,
    pub latency_ms: Option<i64>,
    pub error_class: Option<String>,
    pub error: Option<String>,
}

impl PostgresTelegramOutboxStore {
    /// Resolve expired leases without blindly replaying ambiguous creates.
    pub async fn recover_expired_leases(
        &self,
        limit: usize,
    ) -> Result<TelegramOutboxRecoveryReport, StorageError> {
        let limit = i64::try_from(limit.clamp(1, 1_000)).unwrap_or(1_000);
        let rows = sqlx::query(SQL_RECOVER_EXPIRED_LEASES)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        let mut report = TelegramOutboxRecoveryReport::default();
        for row in rows {
            let count = u64::try_from(row.try_get::<i64, _>("count")?).unwrap_or(0);
            match row.try_get::<String, _>("state")?.as_str() {
                "retry_wait" => report.retry_wait = count,
                "ambiguous" => report.ambiguous = count,
                "expired" => report.expired = count,
                _ => {}
            }
        }
        Ok(report)
    }

    /// Expire TTL-bound operations that never reached a worker.
    pub async fn expire_due_operations(&self, limit: usize) -> Result<u64, StorageError> {
        let limit = i64::try_from(limit.clamp(1, 10_000)).unwrap_or(10_000);
        let count = sqlx::query_scalar::<_, i64>(SQL_EXPIRE_DUE_OPERATIONS)
            .bind(limit)
            .fetch_one(&self.pool)
            .await?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    /// Cancel an inactive operation and every unsent later part.
    pub async fn cancel_operation(
        &self,
        operation_id: &str,
        reason: &str,
    ) -> Result<bool, StorageError> {
        sqlx::query_scalar::<_, bool>(SQL_CANCEL_OPERATION)
            .bind(operation_id)
            .bind(reason)
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    /// Explicitly requeue an ambiguous or dead-letter operation.
    pub async fn retry_operation_manually(
        &self,
        operation_id: &str,
        accept_duplicate_risk: bool,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT state, delivery_policy, expires_at, batch_id, part_index \
             FROM telegram_outbox \
             WHERE operation_id = $1 FOR UPDATE",
        )
        .bind(operation_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            return Err(StorageError::TelegramOutboxNotRetryable {
                operation_id: operation_id.to_owned(),
                state: "not_found".to_owned(),
            });
        };
        let state: String = row.try_get("state")?;
        let policy: String = row.try_get("delivery_policy")?;
        let expires_at: Option<OffsetDateTime> = row.try_get("expires_at")?;
        let batch_id: String = row.try_get("batch_id")?;
        let part_index: i32 = row.try_get("part_index")?;
        if !matches!(state.as_str(), "ambiguous" | "dead_letter")
            || expires_at.is_some_and(|expires_at| expires_at <= OffsetDateTime::now_utc())
        {
            return Err(StorageError::TelegramOutboxNotRetryable {
                operation_id: operation_id.to_owned(),
                state,
            });
        }
        if state == "ambiguous"
            && matches!(policy.as_str(), "create" | "financial")
            && !accept_duplicate_risk
        {
            return Err(
                StorageError::TelegramOutboxDuplicateRiskConfirmationRequired {
                    operation_id: operation_id.to_owned(),
                },
            );
        }
        sqlx::query(
            "UPDATE telegram_outbox SET state = 'pending', available_at = statement_timestamp(), \
             lease_owner = NULL, leased_until = NULL, updated_at = statement_timestamp() \
             WHERE operation_id = $1",
        )
        .bind(operation_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE telegram_outbox \
             SET state = 'pending', available_at = statement_timestamp(), \
                 last_error_class = NULL, last_error = NULL, updated_at = statement_timestamp() \
             WHERE batch_id = $1 AND part_index > $2 AND state = 'cancelled' \
               AND last_error_class IN (\
                   'batch_predecessor_ambiguous', 'batch_predecessor_dead_letter', \
                   'batch_predecessor_terminal'\
               )",
        )
        .bind(batch_id)
        .bind(part_index)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn list_items(
        &self,
        limit: usize,
        before_id: Option<i64>,
        state: Option<&str>,
    ) -> Result<Vec<TelegramOutboxItem>, StorageError> {
        let limit = i64::try_from(limit.clamp(1, 500)).unwrap_or(500);
        sqlx::query(SQL_LIST_OUTBOX_ITEMS)
            .bind(before_id)
            .bind(state)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(outbox_item_from_row)
            .collect()
    }

    pub async fn item(
        &self,
        operation_id: &str,
    ) -> Result<Option<TelegramOutboxItem>, StorageError> {
        sqlx::query(SQL_GET_OUTBOX_ITEM)
            .bind(operation_id)
            .fetch_optional(&self.pool)
            .await?
            .map(outbox_item_from_row)
            .transpose()
    }

    pub async fn attempts(
        &self,
        outbox_id: i64,
        limit: usize,
    ) -> Result<Vec<TelegramOutboxAttempt>, StorageError> {
        let limit = i64::try_from(limit.clamp(1, 500)).unwrap_or(500);
        sqlx::query(SQL_LIST_OUTBOX_ATTEMPTS)
            .bind(outbox_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(outbox_attempt_from_row)
            .collect()
    }

    pub async fn stats(&self) -> Result<TelegramOutboxStats, StorageError> {
        let row = sqlx::query(SQL_OUTBOX_STATS).fetch_one(&self.pool).await?;
        Ok(TelegramOutboxStats {
            pending: row.try_get("pending")?,
            leased: row.try_get("leased")?,
            retry_wait: row.try_get("retry_wait")?,
            delivered: row.try_get("delivered")?,
            ambiguous: row.try_get("ambiguous")?,
            dead_letter: row.try_get("dead_letter")?,
            expired: row.try_get("expired")?,
            cancelled: row.try_get("cancelled")?,
            protected_unresolved: row.try_get("protected_unresolved")?,
            oldest_pending_at: row.try_get("oldest_pending_at")?,
            oldest_lease_expiry: row.try_get("oldest_lease_expiry")?,
        })
    }
}

fn outbox_item_from_row(row: PgRow) -> Result<TelegramOutboxItem, StorageError> {
    Ok(TelegramOutboxItem {
        id: row.try_get("id")?,
        operation_id: row.try_get("operation_id")?,
        batch_id: row.try_get("batch_id")?,
        part_index: row.try_get("part_index")?,
        bot_id: row.try_get("bot_id")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        ordering_key: row.try_get("ordering_key")?,
        causation_update_id: row.try_get("causation_update_id")?,
        dialog_job_id: row.try_get("dialog_job_id")?,
        trigger_message_id: row.try_get("trigger_message_id")?,
        method_kind: row.try_get("method_kind")?,
        delivery_policy: row.try_get("delivery_policy")?,
        protected: row.try_get("protected")?,
        priority: row.try_get("priority")?,
        state: row.try_get("state")?,
        available_at: row.try_get("available_at")?,
        expires_at: row.try_get("expires_at")?,
        attempt_count: row.try_get("attempt_count")?,
        lease_owner: row.try_get("lease_owner")?,
        leased_until: row.try_get("leased_until")?,
        last_error_class: row.try_get("last_error_class")?,
        last_error: row.try_get("last_error")?,
        response_kind: row.try_get("response_kind")?,
        telegram_message_ids: row.try_get("telegram_message_ids")?,
        receipt: row
            .try_get::<Option<sqlx::types::Json<Value>>, _>("receipt")?
            .map(|receipt| receipt.0),
        confirmed_at: row.try_get("confirmed_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn outbox_attempt_from_row(row: PgRow) -> Result<TelegramOutboxAttempt, StorageError> {
    Ok(TelegramOutboxAttempt {
        attempt: row.try_get("attempt")?,
        lease_token: row.try_get("lease_token")?,
        worker_id: row.try_get("worker_id")?,
        claimed_at: row.try_get("claimed_at")?,
        request_started_at: row.try_get("request_started_at")?,
        response_received_at: row.try_get("response_received_at")?,
        finished_at: row.try_get("finished_at")?,
        outcome: row.try_get("outcome")?,
        http_status: row.try_get("http_status")?,
        latency_ms: row.try_get("latency_ms")?,
        error_class: row.try_get("error_class")?,
        error: row.try_get("error")?,
    })
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error, time::SystemTime};

    use sqlx::{Execute as _, postgres::PgPoolOptions};

    use super::*;

    fn batch(batch_id: &str, part_count: usize) -> TelegramOutboxBatchInput {
        let now = OffsetDateTime::now_utc() - time::Duration::seconds(1);
        let bytes = format!("outbox-test-blob:{batch_id}").into_bytes();
        let blob = TelegramOutboxBlobInput::new(
            bytes,
            Some("text/plain".to_owned()),
            Some("test.txt".to_owned()),
            serde_json::json!({"test": true}),
        );
        TelegramOutboxBatchInput {
            batch_id: batch_id.to_owned(),
            bot_id: 77,
            chat_id: Some(42),
            thread_id: Some(0),
            ordering_key: format!("dialog:77:42:0:{batch_id}"),
            causation_update_id: Some(100),
            dialog_job_id: Some(200),
            trigger_message_id: Some(300),
            delivery_policy: TelegramDeliveryPolicy::Create,
            protected: true,
            priority: 10,
            parts: (0..part_count)
                .map(|index| TelegramOutboxPartInput {
                    method_kind: "sendMessage".to_owned(),
                    payload_version: 1,
                    payload: serde_json::json!({"text": format!("part {index}")}),
                    blob: (index < 2).then(|| blob.clone()),
                    available_at: now,
                    expires_at: None,
                })
                .collect(),
        }
    }

    #[test]
    fn operation_id_is_stable_and_part_specific() {
        let first = deterministic_telegram_operation_id("job:42", "sendMessage", 0);
        assert_eq!(
            first,
            deterministic_telegram_operation_id("job:42", "sendMessage", 0)
        );
        assert_ne!(
            first,
            deterministic_telegram_operation_id("job:42", "sendMessage", 1)
        );
        assert_ne!(
            first,
            deterministic_telegram_operation_id("job:42", "editMessageText", 0)
        );
    }

    #[test]
    fn enqueue_builders_emit_one_statement_per_table() -> Result<(), Box<dyn Error>> {
        let batch = batch("builder-test", 3);
        let blobs = prepare_blobs(&batch.parts)?;
        let blob_ids = blobs
            .iter()
            .enumerate()
            .map(|(index, blob)| (blob.sha256.clone(), i64::try_from(index).unwrap_or(0) + 1))
            .collect();
        let parts = prepare_parts(&batch, &blob_ids)?;

        let mut blob_builder = outbox_blob_insert_builder(&blobs);
        let blob_sql = blob_builder.build().sql().as_ref().to_owned();
        let mut operation_builder = outbox_operation_insert_builder(&batch, &parts);
        let operation_sql = operation_builder.build().sql().as_ref().to_owned();

        assert_eq!(
            blob_sql
                .matches("INSERT INTO telegram_outbox_blobs")
                .count(),
            1
        );
        assert_eq!(
            operation_sql.matches("INSERT INTO telegram_outbox").count(),
            1
        );
        assert_eq!(
            operation_sql.matches('$').count(),
            3 * OUTBOX_OPERATION_BINDS_PER_ROW
        );
        assert!(operation_sql.contains("original_payload"));
        assert!(operation_sql.contains("ON CONFLICT (operation_id)"));
        Ok(())
    }

    #[test]
    fn claim_query_serializes_ordering_keys_and_multipart() {
        assert!(SQL_CLAIM_OPERATIONS.contains("FOR UPDATE OF operation SKIP LOCKED"));
        assert!(SQL_CLAIM_OPERATIONS.contains("earlier.ordering_key = operation.ordering_key"));
        assert!(SQL_CLAIM_OPERATIONS.contains("prior_part.part_index < operation.part_index"));
        assert!(SQL_CLAIM_OPERATIONS.contains("interval '90 seconds'"));
    }

    #[test]
    fn same_hash_with_different_bytes_is_rejected_before_sql() {
        let mut input = batch("blob-conflict", 2);
        let hash = vec![7; 32];
        input.parts[0].blob.as_mut().expect("first blob").sha256 = hash.clone();
        let second = input.parts[1].blob.as_mut().expect("second blob");
        second.sha256 = hash;
        second.bytes = b"different".to_vec();

        assert!(matches!(
            prepare_blobs(&input.parts),
            Err(StorageError::TelegramOutboxBlobConflict)
        ));
    }

    #[tokio::test]
    async fn outbox_roundtrip_is_idempotent_ordered_and_fenced() -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await?;
        crate::run_migrations_on(&pool).await?;
        let suffix = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let batch_id = format!("outbox-storage-test:{suffix}");
        let input = batch(&batch_id, 3);
        let blob_hash = input.parts[0]
            .blob
            .as_ref()
            .expect("test blob")
            .sha256
            .clone();
        let store = PostgresTelegramOutboxStore::new(pool.clone());

        let queued = store.enqueue_batch(&input).await?;
        assert_eq!(queued.parts.len(), 3);
        assert!(queued.parts.iter().all(|part| part.inserted));
        let replayed = store.enqueue_batch(&input).await?;
        assert!(replayed.parts.iter().all(|part| !part.inserted));
        let row_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM telegram_outbox WHERE batch_id = $1")
                .bind(&batch_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(row_count, 3);
        let blob_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM telegram_outbox_blobs WHERE sha256 = $1")
                .bind(&blob_hash)
                .fetch_one(&pool)
                .await?;
        assert_eq!(blob_count, 1);

        let first = store.claim_operations("outbox-test", 10).await?;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].part_index, 0);
        assert!(first[0].blob.is_some());
        assert!(
            !store
                .mark_delivered(
                    first[0].id,
                    first[0].lease_token,
                    "message",
                    &[11],
                    &serde_json::json!({"message_id": 11}),
                )
                .await?,
            "delivery requires the durable request-start checkpoint"
        );
        assert!(
            store
                .mark_request_started(first[0].id, first[0].lease_token)
                .await?
        );
        assert!(
            store
                .mark_delivered(
                    first[0].id,
                    first[0].lease_token,
                    "message",
                    &[11],
                    &serde_json::json!({"kind": "message", "response": {"message_id": 11}}),
                )
                .await?
        );
        let pending = store.pending_history_receipts(10).await?;
        assert!(
            pending
                .iter()
                .any(|receipt| receipt.outbox_id == first[0].id)
        );
        assert!(
            store
                .commit_pending_history(
                    first[0].id,
                    &[TelegramReceiptHistoryEntry {
                        bucket_day: OffsetDateTime::now_utc().date(),
                        chat_id: 42,
                        thread_id: 0,
                        message_id: 11,
                        entry_id: format!("outbox-history:{suffix}"),
                        kind: "text".to_owned(),
                        role: "model".to_owned(),
                        occurred_at: OffsetDateTime::now_utc(),
                        sender_id: 7,
                        payload: serde_json::json!({"text": "confirmed"}),
                    }],
                )
                .await?
        );
        assert!(
            store
                .pending_history_receipts(10)
                .await?
                .iter()
                .all(|receipt| receipt.outbox_id != first[0].id)
        );
        let history_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM chat_history_entries WHERE chat_id = 42 AND entry_id = $1",
        )
        .bind(format!("outbox-history:{suffix}"))
        .fetch_one(&pool)
        .await?;
        assert_eq!(history_count, 1);

        let second = store.claim_operations("outbox-test", 10).await?;
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].part_index, 1);
        assert!(
            store
                .mark_request_started(second[0].id, second[0].lease_token)
                .await?
        );
        sqlx::query(
            "UPDATE telegram_outbox SET leased_until = now() - interval '1 second' WHERE id = $1",
        )
        .bind(second[0].id)
        .execute(&pool)
        .await?;
        let recovered = store.recover_expired_leases(10).await?;
        assert_eq!(recovered.ambiguous, 1);
        let second_item = store
            .item(&second[0].operation_id)
            .await?
            .ok_or("second item missing")?;
        assert_eq!(second_item.state, "ambiguous");
        assert!(matches!(
            store
                .retry_operation_manually(&second[0].operation_id, false)
                .await,
            Err(StorageError::TelegramOutboxDuplicateRiskConfirmationRequired { .. })
        ));
        store
            .retry_operation_manually(&second[0].operation_id, true)
            .await?;

        let second_retry = store.claim_operations("outbox-test", 10).await?;
        assert_eq!(second_retry.len(), 1);
        assert_eq!(second_retry[0].part_index, 1);
        assert!(
            store
                .mark_request_started(second_retry[0].id, second_retry[0].lease_token)
                .await?
        );
        let fallback = serde_json::json!({"text": "part 1", "without_reply": true});
        assert!(
            store
                .retry_operation(
                    second_retry[0].id,
                    second_retry[0].lease_token,
                    OffsetDateTime::now_utc() - time::Duration::seconds(1),
                    "reply_missing",
                    "reply target is unavailable",
                    Some(400),
                    Some(&fallback),
                )
                .await?
        );
        let replay_after_fallback = store.enqueue_batch(&input).await?;
        assert_eq!(replay_after_fallback.parts.len(), 3);

        let fallback_claim = store.claim_operations("outbox-test", 10).await?;
        assert_eq!(fallback_claim[0].payload, fallback);
        assert!(
            store
                .mark_request_started(fallback_claim[0].id, fallback_claim[0].lease_token)
                .await?
        );
        assert!(
            store
                .mark_delivered(
                    fallback_claim[0].id,
                    fallback_claim[0].lease_token,
                    "message",
                    &[12],
                    &serde_json::json!({"message_id": 12}),
                )
                .await?
        );

        let third = store.claim_operations("outbox-test", 10).await?;
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].part_index, 2);
        assert!(
            store
                .dead_letter_operation(
                    third[0].id,
                    third[0].lease_token,
                    "forbidden",
                    "Telegram rejected the operation",
                    Some(403),
                )
                .await?
        );
        let attempts = store.attempts(second[0].id, 10).await?;
        assert_eq!(attempts.len(), 3);
        let stats = store.stats().await?;
        assert!(stats.delivered >= 2);
        assert!(stats.dead_letter >= 1);

        sqlx::query("DELETE FROM telegram_outbox WHERE batch_id = $1")
            .bind(&batch_id)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM telegram_outbox_blobs WHERE sha256 = $1")
            .bind(blob_hash)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM chat_history_entries WHERE chat_id = 42 AND entry_id = $1")
            .bind(format!("outbox-history:{suffix}"))
            .execute(&pool)
            .await?;
        Ok(())
    }
}
