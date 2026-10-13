//! Write-behind journal of memory-card uses.
//!
//! Dialog turns record which cards they recalled without waiting for Postgres;
//! a worker applies the collected uses in one statement per interval and once
//! more on shutdown.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use openplotva_storage::{MemoryCardCache, MemoryCardUsage, PostgresMemoryStore};
use time::OffsetDateTime;

pub const MEMORY_CARD_USAGE_FLUSH_INTERVAL: Duration = Duration::from_secs(30);
/// Cache statistics are logged every this many flush ticks (5 minutes).
const CACHE_STATS_EVERY_TICKS: u64 = 10;

#[derive(Clone, Debug, Default)]
pub struct MemoryCardUsageJournal {
    pending: Arc<Mutex<HashMap<i64, (OffsetDateTime, i64)>>>,
}

impl MemoryCardUsageJournal {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Count one use of each card at `used_at`.
    pub fn record(&self, card_ids: &[i64], used_at: OffsetDateTime) {
        let mut pending = self.lock();
        for card_id in card_ids.iter().copied().filter(|id| *id != 0) {
            let entry = pending.entry(card_id).or_insert((used_at, 0));
            entry.0 = entry.0.max(used_at);
            entry.1 += 1;
        }
    }

    fn drain(&self) -> Vec<MemoryCardUsage> {
        self.lock()
            .drain()
            .map(|(card_id, (last_used_at, uses))| MemoryCardUsage {
                card_id,
                last_used_at,
                uses,
            })
            .collect()
    }

    /// Put back uses whose flush failed, merging with uses recorded since.
    fn restore(&self, usage: Vec<MemoryCardUsage>) {
        let mut pending = self.lock();
        for entry in usage {
            let slot = pending
                .entry(entry.card_id)
                .or_insert((entry.last_used_at, 0));
            slot.0 = slot.0.max(entry.last_used_at);
            slot.1 += entry.uses;
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<i64, (OffsetDateTime, i64)>> {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryCardUsageWorkerReport {
    pub flushes: u64,
    pub cards_updated: u64,
    pub errors: u64,
}

/// Flush card uses every `interval` until `stop`, then flush what is left.
pub async fn run_memory_card_usage_flush_worker_until(
    journal: MemoryCardUsageJournal,
    store: PostgresMemoryStore,
    card_cache: Option<Arc<MemoryCardCache>>,
    interval: Duration,
    stop: impl std::future::Future<Output = ()>,
) -> MemoryCardUsageWorkerReport {
    let mut report = MemoryCardUsageWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    tokio::pin!(stop);
    let mut ticks = 0u64;
    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                ticks += 1;
                flush_card_usage(&journal, &store, &mut report).await;
                if let Some(cache) = &card_cache
                    && ticks.is_multiple_of(CACHE_STATS_EVERY_TICKS)
                {
                    let stats = cache.stats();
                    tracing::info!(
                        entries = stats.entries,
                        bytes = stats.bytes,
                        hits = stats.hits,
                        misses = stats.misses,
                        expired = stats.expired,
                        evictions = stats.evictions,
                        invalidated = stats.invalidated,
                        "memory card cache stats"
                    );
                }
            }
        }
    }
    flush_card_usage(&journal, &store, &mut report).await;
    report
}

async fn flush_card_usage(
    journal: &MemoryCardUsageJournal,
    store: &PostgresMemoryStore,
    report: &mut MemoryCardUsageWorkerReport,
) {
    let usage = journal.drain();
    if usage.is_empty() {
        return;
    }
    match store.record_card_usage(&usage).await {
        Ok(updated) => {
            report.flushes += 1;
            report.cards_updated += updated;
        }
        Err(error) => {
            report.errors += 1;
            tracing::warn!(%error, cards = usage.len(), "failed to flush memory card usage; keeping it for the next flush");
            journal.restore(usage);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_779_193_800 + seconds).expect("valid time")
    }

    fn by_card(usage: Vec<MemoryCardUsage>) -> Vec<(i64, OffsetDateTime, i64)> {
        let mut usage = usage
            .into_iter()
            .map(|entry| (entry.card_id, entry.last_used_at, entry.uses))
            .collect::<Vec<_>>();
        usage.sort_unstable();
        usage
    }

    #[test]
    fn journal_counts_uses_and_keeps_the_latest_time_per_card() {
        let journal = MemoryCardUsageJournal::new();
        journal.record(&[1, 2, 0], at(10));
        journal.record(&[2], at(5));
        journal.record(&[1], at(20));

        assert_eq!(by_card(journal.drain()), [(1, at(20), 2), (2, at(10), 2)]);
        assert!(journal.drain().is_empty());
    }

    #[test]
    fn failed_flush_is_merged_with_uses_recorded_meanwhile() {
        let journal = MemoryCardUsageJournal::new();
        journal.record(&[1], at(10));
        let failed = journal.drain();
        journal.record(&[1, 3], at(30));

        journal.restore(failed);

        assert_eq!(by_card(journal.drain()), [(1, at(30), 2), (3, at(30), 1)]);
    }
}
