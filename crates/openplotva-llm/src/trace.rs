//! Low-level LLM call trace sink. Mirrors go-plotva `llmtrace`: an injectable observer
//! that every model round-trip reports to, so dialog, auxiliary flows, and each pool
//! attempt are counted at the layer where the call actually happens.

use std::sync::{Arc, OnceLock};

use openplotva_dialog::DialogTraceArtifacts;

/// Caller identity for a single model round-trip. Supplies the fields the low-level
/// client cannot know on its own (chat/user/message); flow/source/model live on the
/// artifact.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LlmCallContext {
    /// Chat the call belongs to (0 when not chat-scoped).
    pub chat_id: i64,
    /// Forum/topic thread id.
    pub thread_id: Option<i32>,
    /// Chat title for diagnostics.
    pub chat_title: String,
    /// User the call is attributed to (0 when not user-scoped).
    pub user_id: i64,
    /// User display name for diagnostics.
    pub full_name: String,
    /// Triggering message id (0 when not message-scoped).
    pub message_id: i32,
}

/// One model round-trip observation: identity context plus the existing trace artifact
/// (provider/source/flow/model/usage/timings/inference_params/error/sizes).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LlmCallRecord {
    /// Caller identity.
    pub context: LlmCallContext,
    /// Provider-side call artifact.
    pub artifact: DialogTraceArtifacts,
}

/// Sink for low-level model-call observations. Implemented in `openplotva-app`.
pub trait LlmCallObserver: Send + Sync {
    /// Record a single model round-trip.
    fn observe(&self, record: LlmCallRecord);
}

/// Holds the registered observer. A concrete type (not free fns) so tests get isolated
/// instances; production uses the [`global_registry`] singleton.
#[derive(Default)]
pub struct LlmCallTraceRegistry {
    observer: OnceLock<Arc<dyn LlmCallObserver>>,
}

impl LlmCallTraceRegistry {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            observer: OnceLock::new(),
        }
    }

    /// Register the observer once. Returns `false` if one was already set.
    pub fn set(&self, observer: Arc<dyn LlmCallObserver>) -> bool {
        self.observer.set(observer).is_ok()
    }

    /// Forward a record to the registered observer; no-op when none is set.
    pub fn observe(&self, record: LlmCallRecord) {
        if let Some(observer) = self.observer.get() {
            observer.observe(record);
        }
    }
}

static GLOBAL: OnceLock<LlmCallTraceRegistry> = OnceLock::new();

/// Process-wide registry (lazily initialized).
pub fn global_registry() -> &'static LlmCallTraceRegistry {
    GLOBAL.get_or_init(LlmCallTraceRegistry::new)
}

/// Register the process-wide observer once (analogue of Go `llmtrace.SetEventEnqueuer`).
pub fn set_observer(observer: Arc<dyn LlmCallObserver>) -> bool {
    global_registry().set(observer)
}

/// Report a model round-trip to the process-wide observer (analogue of Go `EmitEvent`).
pub fn observe(record: LlmCallRecord) {
    global_registry().observe(record);
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use openplotva_dialog::DialogTraceArtifacts;

    use super::*;

    #[derive(Default)]
    struct CollectingObserver(Arc<Mutex<Vec<LlmCallRecord>>>);

    impl LlmCallObserver for CollectingObserver {
        fn observe(&self, record: LlmCallRecord) {
            self.0.lock().expect("observer mutex").push(record);
        }
    }

    #[test]
    fn observe_forwards_to_registered_observer() {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let registry = LlmCallTraceRegistry::new();
        assert!(registry.set(Arc::new(CollectingObserver(Arc::clone(&sink)))));
        registry.observe(LlmCallRecord {
            context: LlmCallContext {
                chat_id: -100,
                user_id: 7,
                ..LlmCallContext::default()
            },
            artifact: DialogTraceArtifacts {
                flow: "memory_extraction".to_owned(),
                model: "Gemma".to_owned(),
                ..DialogTraceArtifacts::default()
            },
        });
        let got = sink.lock().expect("sink mutex");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].context.chat_id, -100);
        assert_eq!(got[0].artifact.flow, "memory_extraction");
    }

    #[test]
    fn observe_without_observer_is_noop() {
        let registry = LlmCallTraceRegistry::new();
        registry.observe(LlmCallRecord::default());
    }

    #[test]
    fn set_is_idempotent() {
        let registry = LlmCallTraceRegistry::new();
        assert!(registry.set(Arc::new(CollectingObserver::default())));
        assert!(!registry.set(Arc::new(CollectingObserver::default())));
    }
}
