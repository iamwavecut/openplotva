//! Composition-root processing for resolved pending virtual-message operations.

use std::{fmt, future::Future, pin::Pin, time::Duration};

use openplotva_core::{MessageIdMapping, PendingOp};
use openplotva_server::{
    index_mappings_by_virtual_id, pending_op_virtual_ids, pending_ops_ready_for_execution,
};
use openplotva_telegram::{PendingOpBuildError, TelegramOutboundMethod, build_pending_op_method};

/// Go pending-operation poll batch size.
pub const PENDING_OP_BATCH_LIMIT: i32 = 50;

/// Go pending-operation worker tick interval.
pub const PENDING_OP_POLL_INTERVAL: Duration = Duration::from_secs(1);

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Storage operations needed by the Go pending-op processing loop.
pub trait PendingOpStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// List pending operations in Go execution order.
    fn list_pending_ops<'a>(
        &'a self,
        limit: i32,
    ) -> BoxFuture<'a, Result<Vec<PendingOp>, Self::Error>>;

    /// Batch-load virtual-message mappings.
    fn list_mappings_by_virtual_ids<'a>(
        &'a self,
        vmsg_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<Vec<MessageIdMapping>, Self::Error>>;

    /// Load one virtual-message mapping.
    fn get_mapping_by_virtual<'a>(
        &'a self,
        vmsg_id: String,
    ) -> BoxFuture<'a, Result<Option<MessageIdMapping>, Self::Error>>;

    /// Mark a pending operation done.
    fn mark_op_done<'a>(&'a self, id: i64) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// Mark a pending operation failed.
    fn mark_op_failed<'a>(
        &'a self,
        id: i64,
        message: String,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;
}

impl PendingOpStore for openplotva_storage::PostgresVirtualMessageStore {
    type Error = openplotva_storage::StorageError;

    fn list_pending_ops<'a>(
        &'a self,
        limit: i32,
    ) -> BoxFuture<'a, Result<Vec<PendingOp>, Self::Error>> {
        Box::pin(async move { self.list_pending_ops(limit).await })
    }

    fn list_mappings_by_virtual_ids<'a>(
        &'a self,
        vmsg_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<Vec<MessageIdMapping>, Self::Error>> {
        Box::pin(async move { self.list_mappings_by_virtual_ids(&vmsg_ids).await })
    }

    fn get_mapping_by_virtual<'a>(
        &'a self,
        vmsg_id: String,
    ) -> BoxFuture<'a, Result<Option<MessageIdMapping>, Self::Error>> {
        Box::pin(async move { self.get_mapping_by_virtual(&vmsg_id).await })
    }

    fn mark_op_done<'a>(&'a self, id: i64) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move { self.mark_op_done(id).await })
    }

    fn mark_op_failed<'a>(
        &'a self,
        id: i64,
        message: String,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move { self.mark_op_failed(id, &message).await })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PendingOpProcessReport {
    /// Number of rows returned by `ListPendingOps`.
    pub listed: usize,
    /// Number of unique virtual-message IDs looked up.
    pub mapping_ids: usize,
    /// Whether the batch mapping lookup failed and single-lookups were used.
    pub batch_lookup_failed: bool,
    /// Number of single mapping lookups attempted after batch failure.
    pub single_lookup_attempts: usize,
    /// Number of rows skipped because no resolved real message ID was available.
    pub skipped_unresolved: usize,
    /// Number of rows ready for Telegram execution.
    pub ready: usize,
    /// Number of Telegram methods sent successfully.
    pub sent: usize,
    /// Number of successful operations marked done.
    pub marked_done: usize,
    /// Number of operations marked failed.
    pub marked_failed: usize,
    /// Number of rows that failed before sending because the operation was unknown or invalid.
    pub build_failed: usize,
    /// Number of Telegram send failures.
    pub send_failed: usize,
    /// Number of failed attempts to write a done/failed status back to storage.
    pub status_write_failed: usize,
    /// Storage error from listing pending ops; Go returns early on this case.
    pub list_error: Option<String>,
}

/// Accumulated summary for a pending-op worker run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PendingOpWorkerReport {
    /// Number of polling ticks processed.
    pub ticks: usize,
    /// Total rows returned by `ListPendingOps`.
    pub listed: usize,
    /// Total rows ready for Telegram execution.
    pub ready: usize,
    /// Total rows skipped because no resolved real message ID was available.
    pub skipped_unresolved: usize,
    /// Total Telegram methods sent successfully.
    pub sent: usize,
    /// Total successful operations marked done.
    pub marked_done: usize,
    /// Total operations marked failed.
    pub marked_failed: usize,
    /// Total rows that failed before sending because the operation was unknown or invalid.
    pub build_failed: usize,
    /// Total Telegram send failures.
    pub send_failed: usize,
    /// Total failed attempts to write a done/failed status back to storage.
    pub status_write_failed: usize,
    /// Number of ticks where pending-op listing failed.
    pub list_failures: usize,
    /// Number of ticks that used single-lookup fallback after batch mapping lookup failed.
    pub batch_lookup_failures: usize,
    /// Total single mapping lookups attempted after batch failures.
    pub single_lookup_attempts: usize,
}

impl PendingOpWorkerReport {
    fn record_tick(&mut self, tick: &PendingOpProcessReport) {
        self.ticks += 1;
        self.listed += tick.listed;
        self.ready += tick.ready;
        self.skipped_unresolved += tick.skipped_unresolved;
        self.sent += tick.sent;
        self.marked_done += tick.marked_done;
        self.marked_failed += tick.marked_failed;
        self.build_failed += tick.build_failed;
        self.send_failed += tick.send_failed;
        self.status_write_failed += tick.status_write_failed;
        self.list_failures += usize::from(tick.list_error.is_some());
        self.batch_lookup_failures += usize::from(tick.batch_lookup_failed);
        self.single_lookup_attempts += tick.single_lookup_attempts;
    }
}

pub async fn process_pending_ops<S, Send, SendFuture, SendError>(
    store: &S,
    mut send: Send,
) -> PendingOpProcessReport
where
    S: PendingOpStore + Sync,
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
{
    let mut report = PendingOpProcessReport::default();
    let rows = match store.list_pending_ops(PENDING_OP_BATCH_LIMIT).await {
        Ok(rows) => rows,
        Err(error) => {
            report.list_error = Some(error.to_string());
            return report;
        }
    };

    report.listed = rows.len();
    let mappings = load_pending_op_mappings(store, &rows, &mut report).await;
    let ready = pending_ops_ready_for_execution(&rows, &mappings);
    report.ready = ready.len();
    report.skipped_unresolved = rows.len().saturating_sub(ready.len());

    for op in ready {
        let method = match build_pending_op_method(&op) {
            Ok(method) => method,
            Err(error) => {
                report.build_failed += 1;
                mark_failed(
                    store,
                    op.id,
                    pending_op_build_error_message(error),
                    &mut report,
                )
                .await;
                continue;
            }
        };

        match send(method).await {
            Ok(()) => {
                report.sent += 1;
                match store.mark_op_done(op.id).await {
                    Ok(()) => report.marked_done += 1,
                    Err(_) => report.status_write_failed += 1,
                }
            }
            Err(error) => {
                report.send_failed += 1;
                mark_failed(store, op.id, error.to_string(), &mut report).await;
            }
        }
    }

    report
}

pub async fn run_pending_op_worker_until<S, Send, SendFuture, SendError, Stop>(
    store: &S,
    send: Send,
    stop: Stop,
) -> PendingOpWorkerReport
where
    S: PendingOpStore + Sync,
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
    Stop: Future<Output = ()>,
{
    run_pending_op_worker_every_until(store, send, PENDING_OP_POLL_INTERVAL, stop).await
}

async fn run_pending_op_worker_every_until<S, Send, SendFuture, SendError, Stop>(
    store: &S,
    mut send: Send,
    interval: Duration,
    stop: Stop,
) -> PendingOpWorkerReport
where
    S: PendingOpStore + Sync,
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
    Stop: Future<Output = ()>,
{
    let mut report = PendingOpWorkerReport::default();
    let mut stop = std::pin::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                let tick = process_pending_ops(store, &mut send).await;
                trace_pending_op_tick(&tick);
                report.record_tick(&tick);
            }
        }
    }

    report
}

fn trace_pending_op_tick(tick: &PendingOpProcessReport) {
    if tick.listed == 0 && tick.list_error.is_none() {
        return;
    }

    tracing::debug!(
        listed = tick.listed,
        ready = tick.ready,
        skipped_unresolved = tick.skipped_unresolved,
        sent = tick.sent,
        marked_done = tick.marked_done,
        marked_failed = tick.marked_failed,
        build_failed = tick.build_failed,
        send_failed = tick.send_failed,
        status_write_failed = tick.status_write_failed,
        list_error = tick.list_error.as_deref(),
        "processed pending Telegram operations"
    );
}

async fn load_pending_op_mappings<S>(
    store: &S,
    rows: &[PendingOp],
    report: &mut PendingOpProcessReport,
) -> std::collections::HashMap<String, MessageIdMapping>
where
    S: PendingOpStore + Sync,
{
    let ids = pending_op_virtual_ids(rows);
    report.mapping_ids = ids.len();
    if ids.is_empty() {
        return std::collections::HashMap::new();
    }

    match store.list_mappings_by_virtual_ids(ids.clone()).await {
        Ok(mappings) => index_mappings_by_virtual_id(&mappings),
        Err(_) => {
            report.batch_lookup_failed = true;
            let mut index = std::collections::HashMap::with_capacity(ids.len());
            for id in ids {
                report.single_lookup_attempts += 1;
                if let Ok(Some(mapping)) = store.get_mapping_by_virtual(id).await
                    && !mapping.vmsg_id.is_empty()
                {
                    index.insert(mapping.vmsg_id.clone(), mapping);
                }
            }
            index
        }
    }
}

async fn mark_failed<S>(store: &S, id: i64, message: String, report: &mut PendingOpProcessReport)
where
    S: PendingOpStore + Sync,
{
    match store.mark_op_failed(id, message).await {
        Ok(()) => report.marked_failed += 1,
        Err(_) => report.status_write_failed += 1,
    }
}

fn pending_op_build_error_message(error: PendingOpBuildError) -> String {
    match error {
        PendingOpBuildError::UnknownOp(_) => "unknown op".to_owned(),
        error => error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt,
        sync::{Arc, Mutex},
    };

    use openplotva_core::{MessageIdMapping, PendingOp};
    use openplotva_telegram::TelegramOutboundMethodKind;

    use super::{
        PENDING_OP_BATCH_LIMIT, PendingOpProcessReport, PendingOpStore, PendingOpWorkerReport,
        process_pending_ops, run_pending_op_worker_every_until,
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct StubError(&'static str);

    impl fmt::Display for StubError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl Error for StubError {}

    #[derive(Default)]
    struct StoreState {
        rows: Vec<PendingOp>,
        list_error: Option<StubError>,
        batch_rows: Vec<MessageIdMapping>,
        batch_error: Option<StubError>,
        single_rows: Vec<MessageIdMapping>,
        list_limit: Option<i32>,
        batch_ids: Vec<String>,
        single_calls: Vec<String>,
        done: Vec<i64>,
        failed: Vec<(i64, String)>,
    }

    #[derive(Clone, Default)]
    struct StoreStub {
        state: Arc<Mutex<StoreState>>,
    }

    impl StoreStub {
        fn with_state(state: StoreState) -> Self {
            Self {
                state: Arc::new(Mutex::new(state)),
            }
        }

        fn snapshot<T>(&self, inspect: impl FnOnce(&StoreState) -> T) -> T {
            let state = self.state.lock().expect("store state");
            inspect(&state)
        }
    }

    impl PendingOpStore for StoreStub {
        type Error = StubError;

        fn list_pending_ops<'a>(
            &'a self,
            limit: i32,
        ) -> super::BoxFuture<'a, Result<Vec<PendingOp>, Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.list_limit = Some(limit);
                if let Some(error) = &state.list_error {
                    Err(error.clone())
                } else {
                    Ok(state.rows.clone())
                }
            };
            Box::pin(async move { result })
        }

        fn list_mappings_by_virtual_ids<'a>(
            &'a self,
            vmsg_ids: Vec<String>,
        ) -> super::BoxFuture<'a, Result<Vec<MessageIdMapping>, Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.batch_ids = vmsg_ids;
                if let Some(error) = &state.batch_error {
                    Err(error.clone())
                } else {
                    Ok(state.batch_rows.clone())
                }
            };
            Box::pin(async move { result })
        }

        fn get_mapping_by_virtual<'a>(
            &'a self,
            vmsg_id: String,
        ) -> super::BoxFuture<'a, Result<Option<MessageIdMapping>, Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.single_calls.push(vmsg_id.clone());
                Ok(state
                    .single_rows
                    .iter()
                    .find(|mapping| mapping.vmsg_id == vmsg_id)
                    .cloned())
            };
            Box::pin(async move { result })
        }

        fn mark_op_done<'a>(&'a self, id: i64) -> super::BoxFuture<'a, Result<(), Self::Error>> {
            {
                let mut state = self.state.lock().expect("store state");
                state.done.push(id);
            }
            Box::pin(async { Ok(()) })
        }

        fn mark_op_failed<'a>(
            &'a self,
            id: i64,
            message: String,
        ) -> super::BoxFuture<'a, Result<(), Self::Error>> {
            {
                let mut state = self.state.lock().expect("store state");
                state.failed.push((id, message));
            }
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn processor_executes_ready_ops_in_row_order_and_marks_done() {
        let store = StoreStub::with_state(StoreState {
            rows: vec![
                pending_op(1, "v1", "edit", br#"{"text":"edited","parse_mode":"HTML"}"#),
                pending_op(2, "v2", "delete", b""),
                pending_op(3, "v3", "delete", b""),
            ],
            batch_rows: vec![
                MessageIdMapping::resolved("v1", 42, 77),
                MessageIdMapping::resolved("v2", 42, 78),
                MessageIdMapping::unresolved("v3", 42),
            ],
            ..StoreState::default()
        });
        let mut sent = Vec::new();

        let report = process_pending_ops(&store, |method| {
            sent.push(method.kind());
            async { Ok::<(), StubError>(()) }
        })
        .await;

        assert_eq!(
            report,
            PendingOpProcessReport {
                listed: 3,
                mapping_ids: 3,
                ready: 2,
                skipped_unresolved: 1,
                sent: 2,
                marked_done: 2,
                ..PendingOpProcessReport::default()
            }
        );
        assert_eq!(
            sent,
            vec![
                TelegramOutboundMethodKind::EditMessageText,
                TelegramOutboundMethodKind::DeleteMessage,
            ]
        );
        store.snapshot(|state| {
            assert_eq!(state.list_limit, Some(PENDING_OP_BATCH_LIMIT));
            assert_eq!(state.batch_ids, vec!["v1", "v2", "v3"]);
            assert_eq!(state.done, vec![1, 2]);
            assert!(state.failed.is_empty());
        });
    }

    #[tokio::test]
    async fn processor_marks_send_failures_and_unknown_ops_like_go() {
        let store = StoreStub::with_state(StoreState {
            rows: vec![
                pending_op(1, "v1", "pin", b""),
                pending_op(2, "v2", "edit", br#"{"text":"edited"}"#),
            ],
            batch_rows: vec![
                MessageIdMapping::resolved("v1", 42, 77),
                MessageIdMapping::resolved("v2", 42, 78),
            ],
            ..StoreState::default()
        });

        let report = process_pending_ops(&store, |_| async {
            Err::<(), StubError>(StubError("network"))
        })
        .await;

        assert_eq!(
            report,
            PendingOpProcessReport {
                listed: 2,
                mapping_ids: 2,
                ready: 2,
                marked_failed: 2,
                build_failed: 1,
                send_failed: 1,
                ..PendingOpProcessReport::default()
            }
        );
        store.snapshot(|state| {
            assert!(state.done.is_empty());
            assert_eq!(
                state.failed,
                vec![(1, "unknown op".to_owned()), (2, "network".to_owned())]
            );
        });
    }

    #[tokio::test]
    async fn processor_falls_back_to_single_mapping_lookups_after_batch_failure() {
        let store = StoreStub::with_state(StoreState {
            rows: vec![
                pending_op(1, "v1", "delete", b""),
                pending_op(2, "v2", "delete", b""),
            ],
            batch_error: Some(StubError("batch unavailable")),
            single_rows: vec![MessageIdMapping::resolved("v1", 42, 77)],
            ..StoreState::default()
        });
        let mut sent = Vec::new();

        let report = process_pending_ops(&store, |method| {
            sent.push(method.kind());
            async { Ok::<(), StubError>(()) }
        })
        .await;

        assert_eq!(
            report,
            PendingOpProcessReport {
                listed: 2,
                mapping_ids: 2,
                batch_lookup_failed: true,
                single_lookup_attempts: 2,
                skipped_unresolved: 1,
                ready: 1,
                sent: 1,
                marked_done: 1,
                ..PendingOpProcessReport::default()
            }
        );
        assert_eq!(sent, vec![TelegramOutboundMethodKind::DeleteMessage]);
        store.snapshot(|state| {
            assert_eq!(state.single_calls, vec!["v1", "v2"]);
            assert_eq!(state.done, vec![1]);
            assert!(state.failed.is_empty());
        });
    }

    #[tokio::test]
    async fn processor_returns_after_pending_list_error_like_go() {
        let store = StoreStub::with_state(StoreState {
            list_error: Some(StubError("database unavailable")),
            ..StoreState::default()
        });
        let mut called = false;

        let report = process_pending_ops(&store, |_| {
            called = true;
            async { Ok::<(), StubError>(()) }
        })
        .await;

        assert_eq!(
            report,
            PendingOpProcessReport {
                list_error: Some("database unavailable".to_owned()),
                ..PendingOpProcessReport::default()
            }
        );
        assert!(!called);
        store.snapshot(|state| {
            assert!(state.batch_ids.is_empty());
            assert!(state.single_calls.is_empty());
            assert!(state.done.is_empty());
            assert!(state.failed.is_empty());
        });
    }

    #[tokio::test]
    async fn worker_ticks_until_stop_and_accumulates_tick_reports() {
        let store = StoreStub::with_state(StoreState {
            rows: vec![pending_op(1, "v1", "delete", b"")],
            batch_rows: vec![MessageIdMapping::resolved("v1", 42, 77)],
            ..StoreState::default()
        });
        let (stop_send, stop_recv) = tokio::sync::oneshot::channel();
        let mut stop_send = Some(stop_send);
        let mut sent = Vec::new();

        let report = run_pending_op_worker_every_until(
            &store,
            |method| {
                sent.push(method.kind());
                if let Some(stop_send) = stop_send.take() {
                    let _ = stop_send.send(());
                }
                async { Ok::<(), StubError>(()) }
            },
            std::time::Duration::from_millis(1),
            async {
                let _ = stop_recv.await;
            },
        )
        .await;

        assert_eq!(
            report,
            PendingOpWorkerReport {
                ticks: 1,
                listed: 1,
                ready: 1,
                sent: 1,
                marked_done: 1,
                ..PendingOpWorkerReport::default()
            }
        );
        assert_eq!(sent, vec![TelegramOutboundMethodKind::DeleteMessage]);
        store.snapshot(|state| {
            assert_eq!(state.done, vec![1]);
            assert!(state.failed.is_empty());
        });
    }

    fn pending_op(id: i64, vmsg_id: &str, op: &str, payload: &[u8]) -> PendingOp {
        PendingOp {
            id,
            vmsg_id: vmsg_id.to_owned(),
            chat_id: 42,
            op: op.to_owned(),
            payload: payload.to_vec(),
            attempts: 0,
        }
    }
}
