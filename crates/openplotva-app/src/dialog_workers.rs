//! Dynamic dialog worker supervision.
//!
//! The dialog worker count is derived from the routing table (the summed slot
//! budgets of the dialog workflow's capacity pools) and can change on any
//! admin routing edit. The supervisor watches the desired count and spawns or
//! retires workers to match: a retired worker receives a oneshot signal that
//! resolves its stop future, so it finishes its current job and exits at the
//! next loop tick — no job is ever abandoned mid-turn. On global runtime stop
//! the supervisor joins every worker it owns before exiting, preserving the
//! drain semantics of the old fixed spawn loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

/// How often the supervisor reaps crashed/finished workers and respawns up to
/// the desired count even without a scale change.
const SUPERVISOR_REAP_INTERVAL: Duration = Duration::from_secs(30);

/// Builds one dialog worker task. The worker must exit soon after `retire`
/// resolves (fired or dropped) or the global runtime stop flips; the concrete
/// spawner composes both into the worker loop's stop future.
pub trait DialogWorkerSpawner: Send + Sync + 'static {
    fn spawn(&self, index: usize, retire: oneshot::Receiver<()>) -> JoinHandle<()>;
}

impl<F> DialogWorkerSpawner for F
where
    F: Fn(usize, oneshot::Receiver<()>) -> JoinHandle<()> + Send + Sync + 'static,
{
    fn spawn(&self, index: usize, retire: oneshot::Receiver<()>) -> JoinHandle<()> {
        self(index, retire)
    }
}

/// Live worker-scale gauge for diagnostics and the admin status endpoint.
#[derive(Debug, Default)]
pub struct WorkerGauge {
    desired: AtomicU32,
    running: AtomicU32,
}

impl WorkerGauge {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn desired(&self) -> u32 {
        self.desired.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn running(&self) -> u32 {
        self.running.load(Ordering::Relaxed)
    }
}

/// Run the supervisor until the global stop flips (or every signal source
/// closes). Reconciles the worker set with `desired_rx` on every change and on
/// a periodic reap tick, then joins all owned workers before returning.
pub fn spawn_dialog_worker_supervisor(
    spawner: Arc<dyn DialogWorkerSpawner>,
    mut desired_rx: watch::Receiver<u32>,
    gauge: Arc<WorkerGauge>,
    mut global_stop: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut next_index = 0usize;
        let mut active: Vec<(oneshot::Sender<()>, JoinHandle<()>)> = Vec::new();
        let mut retired: Vec<JoinHandle<()>> = Vec::new();
        loop {
            let desired = *desired_rx.borrow_and_update() as usize;
            gauge.desired.store(desired as u32, Ordering::Relaxed);

            active.retain(|(_, handle)| !handle.is_finished());
            retired.retain(|handle| !handle.is_finished());
            while active.len() < desired {
                let (retire_tx, retire_rx) = oneshot::channel();
                let handle = spawner.spawn(next_index, retire_rx);
                next_index += 1;
                active.push((retire_tx, handle));
            }
            while active.len() > desired {
                if let Some((retire_tx, handle)) = active.pop() {
                    let _ = retire_tx.send(());
                    retired.push(handle);
                }
            }
            gauge.running.store(active.len() as u32, Ordering::Relaxed);

            let stopped = tokio::select! {
                changed = desired_rx.changed() => changed.is_err(),
                _ = tokio::time::sleep(SUPERVISOR_REAP_INTERVAL) => false,
                stop = wait_for_stop(&mut global_stop) => stop,
            };
            if stopped {
                break;
            }
        }
        // Drain: workers exit via their own global-stop subscriptions (or the
        // retire signal already sent); join them all so shutdown waits for
        // in-flight jobs exactly like the old fixed spawn loop did.
        for (_, handle) in active {
            let _ = handle.await;
        }
        for handle in retired {
            let _ = handle.await;
        }
        gauge.running.store(0, Ordering::Relaxed);
    })
}

/// Resolves `true` when the runtime stop flips (or the channel closes).
async fn wait_for_stop(stop: &mut watch::Receiver<bool>) -> bool {
    loop {
        if *stop.borrow() {
            return true;
        }
        if stop.changed().await.is_err() {
            return true;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Fake worker: parks until retire or global stop, records lifecycle.
    struct FakeSpawner {
        stop: watch::Receiver<bool>,
        spawned: Arc<AtomicU32>,
        exited: Arc<AtomicU32>,
        indices: Arc<Mutex<Vec<usize>>>,
    }

    impl DialogWorkerSpawner for FakeSpawner {
        fn spawn(&self, index: usize, retire: oneshot::Receiver<()>) -> JoinHandle<()> {
            self.spawned.fetch_add(1, Ordering::Relaxed);
            self.indices
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(index);
            let exited = Arc::clone(&self.exited);
            let mut stop = self.stop.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = retire => {}
                    _ = super::wait_for_stop(&mut stop) => {}
                }
                exited.fetch_add(1, Ordering::Relaxed);
            })
        }
    }

    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_scales_up_and_down_and_drains_on_stop() {
        let (desired_tx, desired_rx) = watch::channel(2u32);
        let (stop_tx, stop_rx) = watch::channel(false);
        let spawned = Arc::new(AtomicU32::new(0));
        let exited = Arc::new(AtomicU32::new(0));
        let gauge = Arc::new(WorkerGauge::new());
        let spawner = Arc::new(FakeSpawner {
            stop: stop_rx.clone(),
            spawned: Arc::clone(&spawned),
            exited: Arc::clone(&exited),
            indices: Arc::new(Mutex::new(Vec::new())),
        });

        let supervisor = spawn_dialog_worker_supervisor(
            spawner,
            desired_rx,
            Arc::clone(&gauge),
            stop_rx.clone(),
        );
        settle().await;
        assert_eq!(spawned.load(Ordering::Relaxed), 2);
        assert_eq!(gauge.desired(), 2);
        assert_eq!(gauge.running(), 2);

        desired_tx.send(5).expect("scale up");
        settle().await;
        assert_eq!(spawned.load(Ordering::Relaxed), 5);
        assert_eq!(gauge.running(), 5);

        desired_tx.send(1).expect("scale down");
        settle().await;
        assert_eq!(gauge.running(), 1);
        assert_eq!(exited.load(Ordering::Relaxed), 4, "retired workers exit");

        stop_tx.send(true).expect("global stop");
        supervisor.await.expect("supervisor joins its workers");
        assert_eq!(exited.load(Ordering::Relaxed), 5);
        assert_eq!(gauge.running(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_respawns_crashed_workers_on_reap_tick() {
        let (_desired_tx, desired_rx) = watch::channel(2u32);
        let (stop_tx, stop_rx) = watch::channel(false);
        let spawned = Arc::new(AtomicU32::new(0));
        let gauge = Arc::new(WorkerGauge::new());

        /// Worker that exits immediately, simulating a crash-loop.
        struct CrashingSpawner {
            spawned: Arc<AtomicU32>,
        }
        impl DialogWorkerSpawner for CrashingSpawner {
            fn spawn(&self, _index: usize, _retire: oneshot::Receiver<()>) -> JoinHandle<()> {
                self.spawned.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async {})
            }
        }

        let supervisor = spawn_dialog_worker_supervisor(
            Arc::new(CrashingSpawner {
                spawned: Arc::clone(&spawned),
            }),
            desired_rx,
            Arc::clone(&gauge),
            stop_rx,
        );
        settle().await;
        assert_eq!(spawned.load(Ordering::Relaxed), 2);

        // The reap tick notices the dead workers and respawns to the target.
        tokio::time::sleep(SUPERVISOR_REAP_INTERVAL + Duration::from_secs(1)).await;
        settle().await;
        assert!(
            spawned.load(Ordering::Relaxed) >= 4,
            "reap tick must respawn crashed workers"
        );

        stop_tx.send(true).expect("global stop");
        supervisor.await.expect("supervisor exits");
    }
}
