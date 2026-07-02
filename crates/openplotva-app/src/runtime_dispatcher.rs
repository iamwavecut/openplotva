use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use openplotva_server::{
    RuntimeDispatchFailureData, RuntimeDispatcherFailureInspector, RuntimeDispatcherInspector,
    RuntimeDispatcherStatsData,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Default)]
pub(crate) struct RuntimeDispatcherInspectorHandle {
    queue: Arc<Mutex<Option<Arc<openplotva_telegram::DispatcherQueue>>>>,
}

impl RuntimeDispatcherInspectorHandle {
    pub(crate) fn set_queue(&self, queue: Arc<openplotva_telegram::DispatcherQueue>) {
        *self
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(queue);
    }
}

impl RuntimeDispatcherInspector for RuntimeDispatcherInspectorHandle {
    fn stats(&self) -> RuntimeDispatcherStatsData {
        let Some(queue) = self
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        else {
            return RuntimeDispatcherStatsData::default();
        };
        let stats = queue.stats();
        RuntimeDispatcherStatsData {
            regular_queue_size: stats.regular_queue_size.min(i32::MAX as usize) as i32,
            immediate_queue_size: stats.immediate_queue_size.min(i32::MAX as usize) as i32,
            processed_total: stats.processed_total,
            deduped_total: stats.deduped_total,
            oldest_regular_age_ms: stats.oldest_regular_age.as_millis().min(i32::MAX as u128)
                as i32,
            oldest_immediate_age_ms: stats.oldest_immediate_age.as_millis().min(i32::MAX as u128)
                as i32,
        }
    }
}

/// Ring class recorded when a chat-level persisted Telegram rate limit drops the item.
pub const DISPATCH_FAILURE_CLASS_CHAT_RATE_LIMITED: &str = "chat_rate_limited";
/// Ring class recorded when a queued item carries no sendable method (programmer error).
pub const DISPATCH_FAILURE_CLASS_MISSING_METHOD: &str = "missing_method";

/// Maximum retained terminal-send-failure records.
const DISPATCH_FAILURE_RING_CAP: usize = 1024;

/// One terminal outbound dispatcher send failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchFailureRecord {
    pub at: OffsetDateTime,
    pub virtual_id: String,
    pub chat_id: i64,
    pub method_kind: String,
    pub error: String,
    /// `OutboundSendErrorClass::as_str` value or a dispatcher-local skip class.
    pub class: &'static str,
    /// Whether the failed item was protected (dialog answer / watcher notice).
    pub protected: bool,
    /// Trigger message id recovered from the reply-scoped `r{message_id}`
    /// debounce key, when the producer set one.
    pub reply_to_message_id: Option<i64>,
}

/// Bounded in-memory ring of terminal outbound send failures. Producers are
/// the dispatcher workers; consumers are the delivery-obligation watcher
/// (protected failures, via the sequence cursor) and the runtime GraphQL
/// `dispatcherSendFailures` query (snapshots, so watcher consumption never
/// hides records from diagnostics).
#[derive(Default)]
pub struct DispatchFailureRing {
    state: Mutex<DispatchFailureRingState>,
}

#[derive(Default)]
struct DispatchFailureRingState {
    records: VecDeque<(u64, DispatchFailureRecord)>,
    next_seq: u64,
}

impl DispatchFailureRing {
    pub fn record(&self, record: DispatchFailureRecord) {
        let mut state = self.lock();
        let seq = state.next_seq;
        state.next_seq += 1;
        state.records.push_back((seq, record));
        while state.records.len() > DISPATCH_FAILURE_RING_CAP {
            state.records.pop_front();
        }
    }

    /// Most recent failures, newest first.
    pub fn snapshot(&self, limit: usize) -> Vec<DispatchFailureRecord> {
        let state = self.lock();
        state
            .records
            .iter()
            .rev()
            .take(limit)
            .map(|(_, record)| record.clone())
            .collect()
    }

    /// Protected failures recorded after `cursor`, oldest first, plus the new
    /// cursor value to persist for the next scan.
    pub fn protected_failures_since(&self, cursor: u64) -> (Vec<DispatchFailureRecord>, u64) {
        let state = self.lock();
        let next_cursor = state.next_seq;
        let records = state
            .records
            .iter()
            .filter(|(seq, record)| *seq >= cursor && record.protected)
            .map(|(_, record)| record.clone())
            .collect();
        (records, next_cursor)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DispatchFailureRingState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl RuntimeDispatcherFailureInspector for DispatchFailureRing {
    fn send_failures(&self, limit: i32) -> Vec<RuntimeDispatchFailureData> {
        self.snapshot(usize::try_from(limit).unwrap_or(0))
            .into_iter()
            .map(|record| RuntimeDispatchFailureData {
                at: record
                    .at
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| record.at.to_string()),
                virtual_id: record.virtual_id,
                chat_id: record.chat_id,
                method_kind: record.method_kind,
                error: record.error,
                class: record.class.to_owned(),
                protected: record.protected,
                reply_to_message_id: record.reply_to_message_id,
            })
            .collect()
    }
}

/// Recover the trigger message id from a reply-scoped `r{message_id}` debounce
/// key, which producers append as the fourth `:`-separated fingerprint
/// segment. Chosen over threading `reply_to_message_id` through the dispatcher
/// metadata chain because the key already encodes it losslessly and the
/// metadata shape is persisted (Redis snapshot/restore) — least invasive.
pub fn reply_message_id_from_fingerprint_key(fingerprint_key: &str) -> Option<i64> {
    let debounce_key = fingerprint_key.splitn(4, ':').nth(3)?;
    debounce_key.strip_prefix('r')?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(virtual_id: &str, protected: bool) -> DispatchFailureRecord {
        DispatchFailureRecord {
            at: OffsetDateTime::from_unix_timestamp(1_782_993_600).expect("timestamp"),
            virtual_id: virtual_id.to_owned(),
            chat_id: -100,
            method_kind: "SendMessage".to_owned(),
            error: "Forbidden: bot was blocked by the user".to_owned(),
            class: "terminal_permission",
            protected,
            reply_to_message_id: Some(77),
        }
    }

    #[test]
    fn ring_records_and_snapshots_newest_first_within_cap() {
        let ring = DispatchFailureRing::default();
        for index in 0..(DISPATCH_FAILURE_RING_CAP + 2) {
            ring.record(failure(&format!("v{index}"), false));
        }

        let snapshot = ring.snapshot(2);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(
            snapshot[0].virtual_id,
            format!("v{}", DISPATCH_FAILURE_RING_CAP + 1)
        );
        assert_eq!(
            snapshot[1].virtual_id,
            format!("v{}", DISPATCH_FAILURE_RING_CAP)
        );
        assert_eq!(ring.snapshot(usize::MAX).len(), DISPATCH_FAILURE_RING_CAP);
        let oldest = ring
            .snapshot(usize::MAX)
            .pop()
            .expect("oldest retained record");
        assert_eq!(oldest.virtual_id, "v2", "oldest records were dropped");
    }

    #[test]
    fn protected_scan_returns_only_new_protected_records_and_advances_cursor() {
        let ring = DispatchFailureRing::default();
        ring.record(failure("v0", true));
        ring.record(failure("v1", false));

        let (first, cursor) = ring.protected_failures_since(0);
        assert_eq!(
            first
                .iter()
                .map(|record| record.virtual_id.as_str())
                .collect::<Vec<_>>(),
            vec!["v0"]
        );

        let (empty, cursor) = ring.protected_failures_since(cursor);
        assert!(
            empty.is_empty(),
            "already-consumed records are not rescanned"
        );

        ring.record(failure("v2", true));
        let (second, _) = ring.protected_failures_since(cursor);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].virtual_id, "v2");
    }

    #[test]
    fn inspector_serves_ring_records_for_graphql() {
        let ring = DispatchFailureRing::default();
        ring.record(failure("v0", true));

        let data = ring.send_failures(10);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].at, "2026-07-02T12:00:00Z");
        assert_eq!(data[0].virtual_id, "v0");
        assert_eq!(data[0].chat_id, -100);
        assert_eq!(data[0].class, "terminal_permission");
        assert!(data[0].protected);
        assert_eq!(data[0].reply_to_message_id, Some(77));
    }

    #[test]
    fn reply_message_id_parses_reply_scoped_debounce_keys_only() {
        assert_eq!(
            reply_message_id_from_fingerprint_key("42:text:abc123:r100"),
            Some(100)
        );
        assert_eq!(
            reply_message_id_from_fingerprint_key("42:text:abc123"),
            None
        );
        assert_eq!(
            reply_message_id_from_fingerprint_key("42:text:abc123:custom"),
            None
        );
        assert_eq!(
            reply_message_id_from_fingerprint_key("42:text:abc123:rnot-a-number"),
            None
        );
    }
}
