//! Low-level LLM call trace sink. Mirrors go-plotva `llmtrace`: an injectable observer
//! that every model round-trip reports to, so dialog, auxiliary flows, and each pool
//! attempt are counted at the layer where the call actually happens.

use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use openplotva_dialog::DialogTraceArtifacts;

/// Caller identity for a single model round-trip. Supplies the fields the low-level
/// client cannot know on its own (chat/user/message); flow/source/model live on
/// [`LlmCallTags`] / the artifact.
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

/// Routing tags for a single model round-trip. Mirrors the go-plotva trace metadata so
/// the persisted rows group by the same `provider`/`source`/`flow` as before.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LlmCallTags {
    /// Provider label (e.g. `aifarm`, `genkit`).
    pub provider: String,
    /// Source label used by analytics normalization.
    pub source: String,
    /// Logical flow (e.g. `dialog`, `memory_extraction`).
    pub flow: String,
    /// Mode (e.g. `tools`, `json`).
    pub mode: String,
    /// Request kind (e.g. `openai.chat.completions`).
    pub request_kind: String,
    /// Agent tool-loop iteration (1-based; 0 for non-iterative flows).
    pub iteration: usize,
    /// Reference-doc character count attributed to this call.
    pub docs_chars: i32,
}

/// Trace metadata carried alongside a request into the low-level client.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LlmCallTrace {
    /// Caller identity.
    pub context: LlmCallContext,
    /// Routing tags.
    pub tags: LlmCallTags,
}

/// One model round-trip observation: identity context, the existing trace artifact
/// (provider/source/flow/model/usage/timings/inference_params/error/sizes), and the
/// measured wall-clock duration.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LlmCallRecord {
    /// Caller identity.
    pub context: LlmCallContext,
    /// Provider-side call artifact.
    pub artifact: DialogTraceArtifacts,
    /// Measured wall-clock duration in milliseconds.
    pub duration_ms: i32,
    /// Agent run this call belongs to; stamped from the ambient
    /// [`LlmRunScope`] task-local when the emitter left it unset.
    pub run: Option<LlmRunScope>,
}

/// Correlates every model round-trip on the current task with one agent run
/// (a dialog session, a song/image optimizer run, a console turn).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LlmRunScope {
    /// Stable run key, e.g. `job-8123`, `song-1783...-0`, `console-abc-m4`.
    pub run_id: String,
    /// Run kind, e.g. `dialog`, `song_optimizer`, `image_optimizer`, `console`.
    pub run_kind: String,
}

tokio::task_local! {
    static LLM_RUN_SCOPE: Option<LlmRunScope>;
}

/// Run `fut` with the given run scope ambient on the task: every
/// [`LlmCallRecord`] observed inside (including nested aux calls such as
/// vision materialization) is stamped with it.
pub async fn with_run_scope<F: std::future::Future>(scope: LlmRunScope, fut: F) -> F::Output {
    LLM_RUN_SCOPE.scope(Some(scope), fut).await
}

/// The ambient run scope of the current task, when inside [`with_run_scope`].
#[must_use]
pub fn current_run_scope() -> Option<LlmRunScope> {
    LLM_RUN_SCOPE.try_with(Clone::clone).ok().flatten()
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

impl fmt::Debug for LlmCallTraceRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LlmCallTraceRegistry")
            .field("observer_set", &self.observer.get().is_some())
            .finish()
    }
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
    pub fn observe(&self, mut record: LlmCallRecord) {
        if record.run.is_none() {
            record.run = current_run_scope();
        }
        if let Some(observer) = self.observer.get() {
            observer.observe(record);
        }
    }
}

static GLOBAL: OnceLock<Arc<LlmCallTraceRegistry>> = OnceLock::new();

/// Process-wide registry handle (lazily initialized). Low-level clients hold a clone of
/// this by default, so registering an observer here makes every model call observable.
pub fn global_registry() -> Arc<LlmCallTraceRegistry> {
    GLOBAL
        .get_or_init(|| Arc::new(LlmCallTraceRegistry::new()))
        .clone()
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
            run: None,
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
            duration_ms: 42,
        });
        let got = sink.lock().expect("sink mutex");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].context.chat_id, -100);
        assert_eq!(got[0].artifact.flow, "memory_extraction");
        assert_eq!(got[0].duration_ms, 42);
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

    #[derive(Default)]
    struct RunCollectingObserver {
        runs: std::sync::Mutex<Vec<Option<LlmRunScope>>>,
    }

    impl LlmCallObserver for RunCollectingObserver {
        fn observe(&self, record: LlmCallRecord) {
            self.runs.lock().expect("runs").push(record.run);
        }
    }

    #[tokio::test]
    async fn observe_stamps_the_ambient_run_scope_and_keeps_explicit_ones() {
        let registry = Arc::new(LlmCallTraceRegistry::new());
        let observer = Arc::new(RunCollectingObserver::default());
        assert!(registry.set(Arc::clone(&observer) as Arc<dyn LlmCallObserver>));

        // Outside any scope: no run.
        registry.observe(LlmCallRecord::default());

        // Inside a scope: stamped, including nested awaits on the same task.
        let scoped_registry = Arc::clone(&registry);
        with_run_scope(
            LlmRunScope {
                run_id: "job-1".to_owned(),
                run_kind: "dialog".to_owned(),
            },
            async move {
                scoped_registry.observe(LlmCallRecord::default());
                // A pre-stamped record wins over the ambient scope.
                scoped_registry.observe(LlmCallRecord {
                    run: Some(LlmRunScope {
                        run_id: "song-9".to_owned(),
                        run_kind: "song_optimizer".to_owned(),
                    }),
                    ..LlmCallRecord::default()
                });
            },
        )
        .await;

        let runs = observer.runs.lock().expect("runs").clone();
        assert_eq!(runs.len(), 3);
        assert_eq!(runs[0], None);
        assert_eq!(runs[1].as_ref().map(|s| s.run_id.as_str()), Some("job-1"));
        assert_eq!(runs[2].as_ref().map(|s| s.run_id.as_str()), Some("song-9"));
    }

    #[tokio::test]
    async fn parallel_run_scopes_do_not_bleed_across_tasks() {
        let registry = Arc::new(LlmCallTraceRegistry::new());
        let observer = Arc::new(RunCollectingObserver::default());
        assert!(registry.set(Arc::clone(&observer) as Arc<dyn LlmCallObserver>));

        let spawn_scoped = |run_id: &str| {
            let registry = Arc::clone(&registry);
            let scope = LlmRunScope {
                run_id: run_id.to_owned(),
                run_kind: "dialog".to_owned(),
            };
            tokio::spawn(async move {
                with_run_scope(scope, async move {
                    tokio::task::yield_now().await;
                    registry.observe(LlmCallRecord::default());
                })
                .await;
            })
        };
        let (a, b) = (spawn_scoped("job-a"), spawn_scoped("job-b"));
        a.await.expect("task a");
        b.await.expect("task b");

        let mut ids: Vec<String> = observer
            .runs
            .lock()
            .expect("runs")
            .iter()
            .map(|run| run.as_ref().expect("scoped").run_id.clone())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["job-a".to_owned(), "job-b".to_owned()]);
    }
}
