use std::sync::{Arc, Mutex};

use openplotva_server::{RuntimeDispatcherInspector, RuntimeDispatcherStatsData};

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
