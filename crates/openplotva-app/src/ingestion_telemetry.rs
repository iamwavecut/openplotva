//! Cheap counters for the ingestion gates that consume a user message before
//! any dialog job exists. Gating semantics stay unchanged; these only make the
//! "never became a job" silences visible to operators via `updatesRuntime.gates`.

use std::sync::atomic::{AtomicU64, Ordering};

use openplotva_server::RuntimeIngestionGatesData;

use crate::dialog_messages::DialogMessageUpdateRoute;

#[derive(Debug, Default)]
pub struct IngestionGateCounters {
    task_rate_limited: AtomicU64,
    debounce_coalesced: AtomicU64,
    bot_sampling_skipped: AtomicU64,
    empty_trigger_skipped: AtomicU64,
    invalid_message: AtomicU64,
    random_skipped_roll: AtomicU64,
    random_skipped_reactivity_off: AtomicU64,
    random_skipped_gate: AtomicU64,
    random_skipped_user_disabled: AtomicU64,
}

impl IngestionGateCounters {
    pub fn record_route(&self, route: &DialogMessageUpdateRoute) {
        match route {
            DialogMessageUpdateRoute::SkippedTaskRateLimit => {
                self.task_rate_limited.fetch_add(1, Ordering::Relaxed);
            }
            DialogMessageUpdateRoute::Scheduled { replaced: true, .. } => {
                self.debounce_coalesced.fetch_add(1, Ordering::Relaxed);
            }
            DialogMessageUpdateRoute::SkippedBotSampling => {
                self.bot_sampling_skipped.fetch_add(1, Ordering::Relaxed);
            }
            DialogMessageUpdateRoute::SkippedEmptyDialogTrigger => {
                self.empty_trigger_skipped.fetch_add(1, Ordering::Relaxed);
            }
            DialogMessageUpdateRoute::IgnoredInvalidMessage => {
                self.invalid_message.fetch_add(1, Ordering::Relaxed);
            }
            DialogMessageUpdateRoute::RandomSkippedRoll { .. } => {
                self.random_skipped_roll.fetch_add(1, Ordering::Relaxed);
            }
            DialogMessageUpdateRoute::RandomSkippedReactivityOff => {
                self.random_skipped_reactivity_off
                    .fetch_add(1, Ordering::Relaxed);
            }
            DialogMessageUpdateRoute::RandomSkippedGate => {
                self.random_skipped_gate.fetch_add(1, Ordering::Relaxed);
            }
            DialogMessageUpdateRoute::RandomSkippedUserDisabled => {
                self.random_skipped_user_disabled
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> RuntimeIngestionGatesData {
        RuntimeIngestionGatesData {
            task_rate_limited: self.task_rate_limited.load(Ordering::Relaxed) as i64,
            debounce_coalesced: self.debounce_coalesced.load(Ordering::Relaxed) as i64,
            bot_sampling_skipped: self.bot_sampling_skipped.load(Ordering::Relaxed) as i64,
            empty_trigger_skipped: self.empty_trigger_skipped.load(Ordering::Relaxed) as i64,
            invalid_message: self.invalid_message.load(Ordering::Relaxed) as i64,
            random_skipped_roll: self.random_skipped_roll.load(Ordering::Relaxed) as i64,
            random_skipped_reactivity_off: self
                .random_skipped_reactivity_off
                .load(Ordering::Relaxed) as i64,
            random_skipped_gate: self.random_skipped_gate.load(Ordering::Relaxed) as i64,
            random_skipped_user_disabled: self.random_skipped_user_disabled.load(Ordering::Relaxed)
                as i64,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn route_outcomes_increment_matching_counters() {
        let counters = IngestionGateCounters::default();
        counters.record_route(&DialogMessageUpdateRoute::SkippedTaskRateLimit);
        counters.record_route(&DialogMessageUpdateRoute::Scheduled {
            queue_name: "dialog-aifarm".to_owned(),
            delay: Duration::from_secs(3),
            replaced: true,
        });
        counters.record_route(&DialogMessageUpdateRoute::Scheduled {
            queue_name: "dialog-aifarm".to_owned(),
            delay: Duration::from_secs(3),
            replaced: false,
        });
        counters.record_route(&DialogMessageUpdateRoute::SkippedBotSampling);
        counters.record_route(&DialogMessageUpdateRoute::SkippedEmptyDialogTrigger);
        counters.record_route(&DialogMessageUpdateRoute::IgnoredInvalidMessage);
        counters.record_route(&DialogMessageUpdateRoute::RandomSkippedRoll {
            reactivity: 10,
            roll: 55,
        });
        counters.record_route(&DialogMessageUpdateRoute::RandomSkippedReactivityOff);
        counters.record_route(&DialogMessageUpdateRoute::RandomSkippedGate);
        counters.record_route(&DialogMessageUpdateRoute::RandomSkippedUserDisabled);
        counters.record_route(&DialogMessageUpdateRoute::Delegated);

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.task_rate_limited, 1);
        assert_eq!(snapshot.debounce_coalesced, 1);
        assert_eq!(snapshot.bot_sampling_skipped, 1);
        assert_eq!(snapshot.empty_trigger_skipped, 1);
        assert_eq!(snapshot.invalid_message, 1);
        assert_eq!(snapshot.random_skipped_roll, 1);
        assert_eq!(snapshot.random_skipped_reactivity_off, 1);
        assert_eq!(snapshot.random_skipped_gate, 1);
        assert_eq!(snapshot.random_skipped_user_disabled, 1);
    }
}
