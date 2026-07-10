//! Crash-safe migration from the retired Redis List update queue.

use std::time::{SystemTime, UNIX_EPOCH};

use openplotva_updates::{
    DEFAULT_UPDATE_QUEUE_KEY, EncodedUpdate, RedisUpdateStream, UpdateStreamAppend,
    UpdateStreamSource,
};
use thiserror::Error;

const LEGACY_UPDATE_IMPORT_STAGING_SUFFIX: &str = ":stream-import-staging:v1";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LegacyUpdateImportReport {
    pub moved_to_staging: usize,
    pub appended: usize,
    pub malformed: usize,
    pub duplicate_replays_possible: usize,
}

#[derive(Debug, Error)]
pub enum LegacyUpdateImportError {
    #[error("legacy update Redis operation failed: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("serialize legacy Telegram update: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("append imported update to Redis Stream: {0}")]
    Stream(#[from] openplotva_updates::UpdateStreamError),
    #[error("legacy staging entry disappeared after Stream append")]
    StagingEntryDisappeared,
}

/// Drain the legacy List through a one-entry staging List. The two Redis
/// instances do not need to be the same: a crash after LMOVE retains the
/// staging entry, while a crash after XADD can only create a duplicate update
/// ID that Postgres materialization absorbs.
pub async fn import_legacy_update_list(
    legacy_client: redis::Client,
    stream: &RedisUpdateStream,
    bot_id: i64,
) -> Result<LegacyUpdateImportReport, LegacyUpdateImportError> {
    import_legacy_update_list_from_key(legacy_client, DEFAULT_UPDATE_QUEUE_KEY, stream, bot_id)
        .await
}

pub async fn import_legacy_update_list_from_key(
    legacy_client: redis::Client,
    source_key: &str,
    stream: &RedisUpdateStream,
    bot_id: i64,
) -> Result<LegacyUpdateImportReport, LegacyUpdateImportError> {
    let staging_key = format!("{source_key}{LEGACY_UPDATE_IMPORT_STAGING_SUFFIX}");
    let mut connection = legacy_client.get_multiplexed_async_connection().await?;
    let mut report = LegacyUpdateImportReport::default();

    loop {
        let staged: Option<Vec<u8>> = redis::cmd("LINDEX")
            .arg(&staging_key)
            .arg(0)
            .query_async(&mut connection)
            .await?;
        let value = match staged {
            Some(value) => {
                report.duplicate_replays_possible =
                    report.duplicate_replays_possible.saturating_add(1);
                value
            }
            None => {
                let moved: Option<Vec<u8>> = redis::cmd("LMOVE")
                    .arg(source_key)
                    .arg(&staging_key)
                    .arg("LEFT")
                    .arg("RIGHT")
                    .query_async(&mut connection)
                    .await?;
                let Some(value) = moved else {
                    break;
                };
                report.moved_to_staging = report.moved_to_staging.saturating_add(1);
                value
            }
        };

        let encoded = EncodedUpdate::from_queue_value(value.clone());
        let (update_id, raw_payload) = match encoded.decode_update() {
            Ok(update) => (Some(update.id), serde_json::to_vec(&update)?),
            Err(error) => {
                report.malformed = report.malformed.saturating_add(1);
                tracing::warn!(%error, "legacy Redis List update will be quarantined after import");
                (None, value.clone())
            }
        };
        stream
            .append(&UpdateStreamAppend {
                bot_id,
                update_id,
                source: UpdateStreamSource::Legacy,
                received_at_unix_ms: unix_millis_now(),
                raw_payload,
            })
            .await?;

        let removed: i64 = redis::cmd("LREM")
            .arg(&staging_key)
            .arg(1)
            .arg(&value)
            .query_async(&mut connection)
            .await?;
        if removed != 1 {
            return Err(LegacyUpdateImportError::StagingEntryDisappeared);
        }
        report.appended = report.appended.saturating_add(1);
    }

    Ok(report)
}

fn unix_millis_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
