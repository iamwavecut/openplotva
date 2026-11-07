//! Scheduler, scaling, and worker lifetimes for memory consolidation.
use crate::{
    RuntimeWorkerGroup, RuntimeWorkerPhase, RuntimeWorkerStops, dialog_workers,
    embedder::RoutedDiscoveryEmbedder,
    memory_runtime::{
        self, MemoryServiceWorkerConfig, MemorySubjectMergeConfig, RoutedMemoryExtractor,
        RoutedSubjectMerger,
    },
    model_routing::LaneWorkerScale,
    wait_for_runtime_stop,
};
use openplotva_server::ReadinessCheck;
use openplotva_storage::PostgresMemoryStore;
use openplotva_taskman::InMemoryTaskQueue;
use std::sync::Arc;

pub(crate) struct MemoryTaskmanWorkerInputs {
    pub(crate) store: PostgresMemoryStore,
    pub(crate) queue: Arc<InMemoryTaskQueue>,
    pub(crate) config: MemoryServiceWorkerConfig,
    pub(crate) trigger: Arc<tokio::sync::Notify>,
    pub(crate) extractor: RoutedMemoryExtractor,
    pub(crate) embedder: Option<RoutedDiscoveryEmbedder>,
    pub(crate) merger: Option<(RoutedSubjectMerger, MemorySubjectMergeConfig)>,
    pub(crate) worker_floor: i32,
    pub(crate) workers_per_lane: i32,
    pub(crate) worker_cap: i32,
}

pub(crate) struct MemoryTaskmanWorkers {
    pub(crate) ingress: RuntimeWorkerGroup,
    pub(crate) processors: RuntimeWorkerGroup,
    pub(crate) scale: Option<LaneWorkerScale>,
    pub(crate) count: u32,
    pub(crate) readiness: Vec<ReadinessCheck>,
}

pub(crate) fn build_taskman_workers(
    inputs: MemoryTaskmanWorkerInputs,
    stops: &RuntimeWorkerStops,
    table: &openplotva_llm::router::RoutingTable,
) -> MemoryTaskmanWorkers {
    let MemoryTaskmanWorkerInputs {
        store,
        queue,
        config,
        trigger,
        extractor,
        embedder,
        merger,
        worker_floor,
        workers_per_lane,
        worker_cap,
    } = inputs;
    let mut ingress = RuntimeWorkerGroup::new(RuntimeWorkerPhase::Ingress);
    let mut processors = RuntimeWorkerGroup::new(RuntimeWorkerPhase::Processor);
    let mut readiness = Vec::with_capacity(2);
    let mut worker_scale = None;
    let memory_scheduler_store = store.clone();
    let memory_scheduler_queue = Arc::clone(&queue);
    let memory_scheduler_config = config.clone();
    let memory_scheduler_trigger = Arc::clone(&trigger);
    let memory_scheduler_stop = stops.subscribe(RuntimeWorkerPhase::Ingress);
    let memory_scheduler = tokio::spawn(async move {
        let report = memory_runtime::run_memory_consolidation_taskman_scheduler_with_trigger_until(
            &memory_scheduler_store,
            memory_scheduler_queue.as_ref(),
            memory_scheduler_config,
            memory_scheduler_trigger,
            wait_for_runtime_stop(memory_scheduler_stop),
        )
        .await;

        tracing::info!(?report, "memory-consolidation taskman scheduler stopped");
    });
    ingress.register("memory-consolidation-scheduler", memory_scheduler);

    // Memory workers follow the memory primary's lanes; the configured
    // count is the floor and `0` disables them.
    let configured_memory_worker_count = u32::try_from(worker_floor.max(0)).unwrap_or(0);
    let worker_count = if configured_memory_worker_count == 0 {
        0
    } else {
        let (scale, desired) = tokio::sync::watch::channel(configured_memory_worker_count);
        let lanes = LaneWorkerScale {
            workflow_key: memory_runtime::MEMORY_EXTRACTION_WORKFLOW_KEY,
            per_lane: u32::try_from(workers_per_lane.max(1)).unwrap_or(1),
            floor: configured_memory_worker_count,
            cap: u32::try_from(worker_cap.max(1)).unwrap_or(1),
            scale,
        };
        let initial = lanes
            .desired(table)
            .unwrap_or(configured_memory_worker_count);
        let _ = lanes.scale.send(initial);
        tracing::info!(
            configured_workers = configured_memory_worker_count,
            initial_workers = initial,
            cap = lanes.cap,
            "memory-consolidation worker scale derived from lane pools"
        );
        worker_scale = Some(lanes);

        let memory_worker_spawner = {
            let extractor = extractor.clone();
            let store = store.clone();
            let embedder = embedder.clone();
            let queue = Arc::clone(&queue);
            let process = config.clone().process;
            let desired = desired.clone();
            let stop_rx = stops.subscribe(RuntimeWorkerPhase::Processor);
            move |index: usize, retire: tokio::sync::oneshot::Receiver<()>| {
                let worker_extractor = extractor.clone();
                let worker_store = store.clone();
                let worker_embedder = embedder.clone();
                let worker_queue = Arc::clone(&queue);
                let worker_id = format!(
                    "{}-{index}",
                    memory_runtime::MEMORY_CONSOLIDATION_JOB_WORKER_PREFIX
                );
                let worker_config = memory_runtime::MemoryConsolidationQueueWorkerConfig {
                    process: process.clone(),
                    worker_id: worker_id.clone(),
                    interval: memory_runtime::MEMORY_CONSOLIDATION_JOB_POLL_INTERVAL,
                    pipeline_target: usize::try_from(*desired.borrow()).unwrap_or(1),
                };
                let worker_stop = stop_rx.clone();
                tokio::spawn(async move {
                    // A retired worker finishes its current run and exits
                    // at the next loop tick, exactly like a runtime stop.
                    let stop_future = async move {
                        tokio::select! {
                            () = wait_for_runtime_stop(worker_stop) => {}
                            _ = retire => {}
                        }
                    };
                    let report = memory_runtime::run_memory_consolidation_taskman_worker_until(
                        worker_queue.as_ref(),
                        &worker_extractor,
                        &worker_store,
                        worker_embedder.as_ref(),
                        worker_config,
                        stop_future,
                    )
                    .await;

                    tracing::info!(?report, %worker_id, "memory-consolidation taskman worker stopped");
                })
            }
        };
        let memory_supervisor = dialog_workers::spawn_worker_supervisor(
            Arc::new(memory_worker_spawner),
            desired,
            Arc::new(dialog_workers::WorkerGauge::new()),
            stops.subscribe(RuntimeWorkerPhase::Processor),
        );
        processors.register("memory-consolidation-worker-supervisor", memory_supervisor);
        initial
    };
    if let Some((merge_merger, merge_cfg)) = merger {
        let merge_store = store.clone();
        let merge_embedder = embedder.clone();
        let merge_stop = stops.subscribe(RuntimeWorkerPhase::Processor);
        let merge_worker = tokio::spawn(async move {
            let report = memory_runtime::run_memory_subject_merge_worker_until(
                &merge_merger,
                merge_store,
                merge_embedder.as_ref(),
                merge_cfg,
                wait_for_runtime_stop(merge_stop),
            )
            .await;
            tracing::info!(?report, "memory subject merge-pass worker stopped");
        });
        readiness.push(ReadinessCheck::ok(
                "memory_subject_merge",
                "Backlog over-extracted (scope,subject) card groups consolidated 24/7 via the LLM merge-pass",
            ));
        processors.register("memory-subject-merge", merge_worker);
    }
    readiness.push(ReadinessCheck::ok(
            "memory_service",
            format!(
                "Memory service schedules memory-consolidation taskman jobs and starts {worker_count} routed workers over SQLx persistence"
            ),
        ));
    MemoryTaskmanWorkers {
        ingress,
        processors,
        scale: worker_scale,
        count: worker_count,
        readiness,
    }
}
