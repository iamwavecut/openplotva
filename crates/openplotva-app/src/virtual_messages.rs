//! Composition-root virtual-message edit/delete behavior.

use std::{fmt, future::Future, pin::Pin};

use openplotva_core::{MessageIdMapping, ReadyPendingOp};
use openplotva_telegram::{
    PENDING_OP_DELETE, PENDING_OP_EDIT, PendingOpBuildError, TelegramOutboundMethod,
    build_pending_op_method,
};
use serde_json::json;
use thiserror::Error;

use crate::pending_ops::PendingOpHistory;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Storage operations used by Go's virtual edit/delete paths.
pub trait VirtualMessageStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load a virtual-message mapping by virtual ID.
    fn get_mapping_by_virtual<'a>(
        &'a self,
        vmsg_id: String,
    ) -> BoxFuture<'a, Result<Option<MessageIdMapping>, Self::Error>>;

    /// Enqueue a pending virtual-message operation.
    fn enqueue_message_op<'a>(
        &'a self,
        vmsg_id: String,
        chat_id: i64,
        op: &'static str,
        payload_json: Option<String>,
    ) -> BoxFuture<'a, Result<i64, Self::Error>>;

    /// Delete a resolved virtual-message mapping.
    fn delete_mapping_by_virtual<'a>(
        &'a self,
        vmsg_id: String,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;
}

impl VirtualMessageStore for openplotva_storage::PostgresVirtualMessageStore {
    type Error = openplotva_storage::StorageError;

    fn get_mapping_by_virtual<'a>(
        &'a self,
        vmsg_id: String,
    ) -> BoxFuture<'a, Result<Option<MessageIdMapping>, Self::Error>> {
        Box::pin(async move { self.get_mapping_by_virtual(&vmsg_id).await })
    }

    fn enqueue_message_op<'a>(
        &'a self,
        vmsg_id: String,
        chat_id: i64,
        op: &'static str,
        payload_json: Option<String>,
    ) -> BoxFuture<'a, Result<i64, Self::Error>> {
        Box::pin(async move {
            self.enqueue_message_op(&vmsg_id, chat_id, op, payload_json.as_deref())
                .await
        })
    }

    fn delete_mapping_by_virtual<'a>(
        &'a self,
        vmsg_id: String,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move { self.delete_mapping_by_virtual(&vmsg_id).await })
    }
}

/// Observable result of a virtual-message edit/delete request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VirtualMessageAction {
    /// The mapping was resolved and a Telegram method was sent immediately.
    SentNow,
    /// The mapping was missing or unresolved, so a pending operation was queued.
    Queued,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VirtualMessageReport {
    /// Whether the operation was sent immediately or queued for later.
    pub action: VirtualMessageAction,
    /// Real Telegram message ID used by immediate sends.
    pub real_message_id: Option<i32>,
    /// Pending operation row ID, when enqueue succeeded.
    pub enqueued_op_id: Option<i64>,
    /// Mapping lookup error ignored by Go before queueing a pending operation.
    pub lookup_error: Option<String>,
    /// Enqueue error ignored by Go after deciding to queue.
    pub enqueue_error: Option<String>,
    /// Number of queued dispatcher items removed by virtual ID.
    pub canceled: usize,
    /// Whether a successful edit was reflected into history.
    pub history_updated: bool,
    /// Whether a successful delete was reflected into history.
    pub history_deleted: bool,
    /// Whether a successful delete removed its virtual-message mapping.
    pub mapping_deleted: bool,
    /// Mapping-delete error ignored by Go after a successful Telegram delete.
    pub delete_mapping_error: Option<String>,
}

impl VirtualMessageReport {
    fn sent_now(real_message_id: i32) -> Self {
        Self {
            action: VirtualMessageAction::SentNow,
            real_message_id: Some(real_message_id),
            enqueued_op_id: None,
            lookup_error: None,
            enqueue_error: None,
            canceled: 0,
            history_updated: false,
            history_deleted: false,
            mapping_deleted: false,
            delete_mapping_error: None,
        }
    }

    fn queued(
        enqueued_op_id: Option<i64>,
        enqueue_error: Option<String>,
        lookup_error: Option<String>,
        canceled: usize,
    ) -> Self {
        Self {
            action: VirtualMessageAction::Queued,
            real_message_id: None,
            enqueued_op_id,
            lookup_error,
            enqueue_error,
            canceled,
            history_updated: false,
            history_deleted: false,
            mapping_deleted: false,
            delete_mapping_error: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VirtualEditRequest<'a> {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Virtual message ID.
    pub vmsg_id: &'a str,
    /// New message text.
    pub text: &'a str,
    /// Go parse mode string, such as `HTML`.
    pub parse_mode: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VirtualDeleteRequest<'a> {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Virtual message ID.
    pub vmsg_id: &'a str,
}

/// Recoverable errors from immediate virtual-message handling.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum VirtualMessageError {
    /// Go returns this before trying storage for virtual edits.
    #[error("text is empty")]
    EmptyText,
    /// The resolved operation could not be converted into a Telegram method.
    #[error("failed to build Telegram method: {0}")]
    Build(String),
    /// Telegram rejected the immediate operation.
    #[error("Telegram send failed: {0}")]
    Send(String),
}

pub async fn edit_text_virtual<S, H, Send, SendFuture, SendError, Cancel>(
    store: &S,
    history: &H,
    req: VirtualEditRequest<'_>,
    send: Send,
    cancel: Cancel,
) -> Result<VirtualMessageReport, VirtualMessageError>
where
    S: VirtualMessageStore + Sync,
    H: PendingOpHistory,
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
    Cancel: FnMut(&str) -> usize,
{
    if req.text.is_empty() {
        return Err(VirtualMessageError::EmptyText);
    }

    let payload_json = pending_edit_payload_json(req.text, req.parse_mode);
    let mapping = load_mapping(store, req.vmsg_id).await;
    let Some(real_message_id) = resolved_message_id(&mapping) else {
        return Ok(queue_virtual_message_op(
            store,
            req.vmsg_id,
            req.chat_id,
            PENDING_OP_EDIT,
            Some(payload_json),
            mapping.err().map(|error| error.to_string()),
            cancel,
        )
        .await);
    };

    let op = ready_virtual_op(
        req.vmsg_id,
        req.chat_id,
        PENDING_OP_EDIT,
        payload_json.into_bytes(),
        real_message_id,
    );
    send_ready_virtual_op(&op, send).await?;
    history.update_text(req.chat_id, real_message_id, req.text, req.parse_mode);

    let mut report = VirtualMessageReport::sent_now(real_message_id);
    report.history_updated = true;
    Ok(report)
}

pub async fn delete_message_virtual<S, H, Send, SendFuture, SendError, Cancel>(
    store: &S,
    history: &H,
    req: VirtualDeleteRequest<'_>,
    send: Send,
    cancel: Cancel,
) -> Result<VirtualMessageReport, VirtualMessageError>
where
    S: VirtualMessageStore + Sync,
    H: PendingOpHistory,
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
    Cancel: FnMut(&str) -> usize,
{
    let mapping = load_mapping(store, req.vmsg_id).await;
    let Some(real_message_id) = resolved_message_id(&mapping) else {
        return Ok(queue_virtual_message_op(
            store,
            req.vmsg_id,
            req.chat_id,
            PENDING_OP_DELETE,
            None,
            mapping.err().map(|error| error.to_string()),
            cancel,
        )
        .await);
    };

    let op = ready_virtual_op(
        req.vmsg_id,
        req.chat_id,
        PENDING_OP_DELETE,
        Vec::new(),
        real_message_id,
    );
    send_ready_virtual_op(&op, send).await?;
    history.delete_message(req.chat_id, real_message_id);

    let delete_mapping_result = store
        .delete_mapping_by_virtual(req.vmsg_id.to_owned())
        .await;
    let mut report = VirtualMessageReport::sent_now(real_message_id);
    report.history_deleted = true;
    match delete_mapping_result {
        Ok(()) => report.mapping_deleted = true,
        Err(error) => report.delete_mapping_error = Some(error.to_string()),
    }
    Ok(report)
}

async fn load_mapping<S>(store: &S, vmsg_id: &str) -> Result<Option<MessageIdMapping>, S::Error>
where
    S: VirtualMessageStore + Sync,
{
    store.get_mapping_by_virtual(vmsg_id.to_owned()).await
}

fn resolved_message_id<E>(mapping: &Result<Option<MessageIdMapping>, E>) -> Option<i32> {
    mapping
        .as_ref()
        .ok()
        .and_then(|mapping| mapping.as_ref())
        .and_then(|mapping| mapping.real_message_id)
}

async fn queue_virtual_message_op<S, Cancel>(
    store: &S,
    vmsg_id: &str,
    chat_id: i64,
    op: &'static str,
    payload_json: Option<String>,
    lookup_error: Option<String>,
    mut cancel: Cancel,
) -> VirtualMessageReport
where
    S: VirtualMessageStore + Sync,
    Cancel: FnMut(&str) -> usize,
{
    let enqueue_result = store
        .enqueue_message_op(vmsg_id.to_owned(), chat_id, op, payload_json)
        .await;
    let (enqueued_op_id, enqueue_error) = match enqueue_result {
        Ok(id) => (Some(id), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let canceled = cancel(vmsg_id);

    VirtualMessageReport::queued(enqueued_op_id, enqueue_error, lookup_error, canceled)
}

async fn send_ready_virtual_op<Send, SendFuture, SendError>(
    op: &ReadyPendingOp,
    mut send: Send,
) -> Result<(), VirtualMessageError>
where
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
{
    let method = build_pending_op_method(op)
        .map_err(|error| VirtualMessageError::Build(pending_build_error_message(error)))?;
    send(method)
        .await
        .map_err(|error| VirtualMessageError::Send(error.to_string()))
}

fn ready_virtual_op(
    vmsg_id: &str,
    chat_id: i64,
    op: &str,
    payload: Vec<u8>,
    real_message_id: i32,
) -> ReadyPendingOp {
    ReadyPendingOp {
        id: 0,
        vmsg_id: vmsg_id.to_owned(),
        chat_id,
        op: op.to_owned(),
        payload,
        real_message_id,
    }
}

fn pending_edit_payload_json(text: &str, parse_mode: &str) -> String {
    json!({
        "parse_mode": parse_mode,
        "text": text,
    })
    .to_string()
}

fn pending_build_error_message(error: PendingOpBuildError) -> String {
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

    use openplotva_core::MessageIdMapping;
    use openplotva_telegram::{TelegramOutboundMethod, TelegramOutboundMethodKind};
    use serde_json::json;

    use crate::pending_ops::PendingOpHistory;

    use super::{
        PENDING_OP_DELETE, PENDING_OP_EDIT, VirtualDeleteRequest, VirtualEditRequest,
        VirtualMessageAction, VirtualMessageError, VirtualMessageReport, VirtualMessageStore,
        delete_message_virtual, edit_text_virtual,
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
        mapping: Option<MessageIdMapping>,
        lookup_error: Option<StubError>,
        enqueue_error: Option<StubError>,
        delete_mapping_error: Option<StubError>,
        lookup_calls: Vec<String>,
        enqueued: Vec<(String, i64, &'static str, Option<String>)>,
        deleted_mappings: Vec<String>,
        events: Vec<String>,
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

        fn history(&self) -> HistoryStub {
            HistoryStub {
                state: Arc::clone(&self.state),
            }
        }
    }

    #[derive(Clone)]
    struct HistoryStub {
        state: Arc<Mutex<StoreState>>,
    }

    impl PendingOpHistory for HistoryStub {
        fn update_text(&self, chat_id: i64, message_id: i32, text: &str, parse_mode: &str) {
            self.state.lock().expect("store state").events.push(format!(
                "history:update:{chat_id}:{message_id}:{text}:{parse_mode}"
            ));
        }

        fn delete_message(&self, chat_id: i64, message_id: i32) {
            self.state
                .lock()
                .expect("store state")
                .events
                .push(format!("history:delete:{chat_id}:{message_id}"));
        }
    }

    impl VirtualMessageStore for StoreStub {
        type Error = StubError;

        fn get_mapping_by_virtual<'a>(
            &'a self,
            vmsg_id: String,
        ) -> super::BoxFuture<'a, Result<Option<MessageIdMapping>, Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.lookup_calls.push(vmsg_id);
                if let Some(error) = &state.lookup_error {
                    Err(error.clone())
                } else {
                    Ok(state.mapping.clone())
                }
            };
            Box::pin(async move { result })
        }

        fn enqueue_message_op<'a>(
            &'a self,
            vmsg_id: String,
            chat_id: i64,
            op: &'static str,
            payload_json: Option<String>,
        ) -> super::BoxFuture<'a, Result<i64, Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.enqueued.push((vmsg_id, chat_id, op, payload_json));
                if let Some(error) = &state.enqueue_error {
                    Err(error.clone())
                } else {
                    Ok(i64::try_from(state.enqueued.len()).expect("enqueued len fits i64"))
                }
            };
            Box::pin(async move { result })
        }

        fn delete_mapping_by_virtual<'a>(
            &'a self,
            vmsg_id: String,
        ) -> super::BoxFuture<'a, Result<(), Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.deleted_mappings.push(vmsg_id);
                if let Some(error) = &state.delete_mapping_error {
                    Err(error.clone())
                } else {
                    Ok(())
                }
            };
            Box::pin(async move { result })
        }
    }

    #[tokio::test]
    async fn edit_text_virtual_sends_now_when_mapping_is_resolved() -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            mapping: Some(MessageIdMapping::resolved("v1", 42, 77)),
            ..StoreState::default()
        });
        let history = store.history();
        let mut sent = Vec::new();
        let mut canceled = Vec::new();

        let report = edit_text_virtual(
            &store,
            &history,
            VirtualEditRequest {
                chat_id: 42,
                vmsg_id: "v1",
                text: "<b>edited</b>",
                parse_mode: "HTML",
            },
            |method| {
                sent.push(method_payload(method));
                async { Ok::<(), StubError>(()) }
            },
            |vmsg_id| {
                canceled.push(vmsg_id.to_owned());
                0
            },
        )
        .await?;

        assert_eq!(
            report,
            VirtualMessageReport {
                action: VirtualMessageAction::SentNow,
                real_message_id: Some(77),
                history_updated: true,
                enqueued_op_id: None,
                lookup_error: None,
                enqueue_error: None,
                canceled: 0,
                history_deleted: false,
                mapping_deleted: false,
                delete_mapping_error: None,
            }
        );
        assert_eq!(
            sent,
            vec![(
                TelegramOutboundMethodKind::EditMessageText,
                json!({
                    "chat_id": 42,
                    "message_id": 77,
                    "parse_mode": "HTML",
                    "text": "<b>edited</b>",
                })
            )]
        );
        assert!(canceled.is_empty());
        store.snapshot(|state| {
            assert_eq!(state.lookup_calls, vec!["v1"]);
            assert!(state.enqueued.is_empty());
            assert_eq!(
                state.events,
                vec!["history:update:42:77:<b>edited</b>:HTML"]
            );
        });

        Ok(())
    }

    #[tokio::test]
    async fn edit_text_virtual_rejects_empty_text_like_go() {
        let store = StoreStub::default();
        let history = store.history();

        let error = edit_text_virtual(
            &store,
            &history,
            VirtualEditRequest {
                chat_id: 42,
                vmsg_id: "v1",
                text: "",
                parse_mode: "",
            },
            |_| async { Ok::<(), StubError>(()) },
            |_| 0,
        )
        .await
        .expect_err("empty virtual edit should fail");

        assert_eq!(error, VirtualMessageError::EmptyText);
        assert_eq!(error.to_string(), "text is empty");
        store.snapshot(|state| {
            assert!(state.lookup_calls.is_empty());
            assert!(state.enqueued.is_empty());
        });
    }

    #[tokio::test]
    async fn edit_text_virtual_queues_and_cancels_when_mapping_is_unresolved()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            mapping: Some(MessageIdMapping::unresolved("v1", 42)),
            ..StoreState::default()
        });
        let history = store.history();
        let mut sent = Vec::new();
        let mut canceled = Vec::new();

        let report = edit_text_virtual(
            &store,
            &history,
            VirtualEditRequest {
                chat_id: 42,
                vmsg_id: "v1",
                text: "edited",
                parse_mode: "HTML",
            },
            |method| {
                sent.push(method.kind());
                async { Ok::<(), StubError>(()) }
            },
            |vmsg_id| {
                canceled.push(vmsg_id.to_owned());
                1
            },
        )
        .await?;

        assert_eq!(
            report,
            VirtualMessageReport {
                action: VirtualMessageAction::Queued,
                enqueued_op_id: Some(1),
                canceled: 1,
                real_message_id: None,
                lookup_error: None,
                enqueue_error: None,
                history_updated: false,
                history_deleted: false,
                mapping_deleted: false,
                delete_mapping_error: None,
            }
        );
        assert!(sent.is_empty());
        assert_eq!(canceled, vec!["v1"]);
        store.snapshot(|state| {
            assert_eq!(state.lookup_calls, vec!["v1"]);
            assert_eq!(state.enqueued.len(), 1);
            let (vmsg_id, chat_id, op, payload) = &state.enqueued[0];
            assert_eq!(vmsg_id, "v1");
            assert_eq!(*chat_id, 42);
            assert_eq!(*op, PENDING_OP_EDIT);
            let payload = payload.as_deref().expect("edit payload");
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(payload).expect("payload json"),
                json!({
                    "parse_mode": "HTML",
                    "text": "edited",
                })
            );
            assert!(state.events.is_empty());
        });

        Ok(())
    }

    #[tokio::test]
    async fn delete_message_virtual_sends_now_updates_history_and_deletes_mapping()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            mapping: Some(MessageIdMapping::resolved("v2", 42, 78)),
            ..StoreState::default()
        });
        let history = store.history();
        let mut sent = Vec::new();

        let report = delete_message_virtual(
            &store,
            &history,
            VirtualDeleteRequest {
                chat_id: 42,
                vmsg_id: "v2",
            },
            |method| {
                sent.push(method_payload(method));
                async { Ok::<(), StubError>(()) }
            },
            |_| 0,
        )
        .await?;

        assert_eq!(
            report,
            VirtualMessageReport {
                action: VirtualMessageAction::SentNow,
                real_message_id: Some(78),
                history_deleted: true,
                mapping_deleted: true,
                enqueued_op_id: None,
                lookup_error: None,
                enqueue_error: None,
                canceled: 0,
                history_updated: false,
                delete_mapping_error: None,
            }
        );
        assert_eq!(
            sent,
            vec![(
                TelegramOutboundMethodKind::DeleteMessage,
                json!({
                    "chat_id": 42,
                    "message_id": 78,
                })
            )]
        );
        store.snapshot(|state| {
            assert_eq!(state.lookup_calls, vec!["v2"]);
            assert_eq!(state.deleted_mappings, vec!["v2"]);
            assert_eq!(state.events, vec!["history:delete:42:78"]);
            assert!(state.enqueued.is_empty());
        });

        Ok(())
    }

    #[tokio::test]
    async fn delete_message_virtual_queues_after_mapping_lookup_failure()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            lookup_error: Some(StubError("db lookup")),
            enqueue_error: Some(StubError("enqueue failed")),
            ..StoreState::default()
        });
        let history = store.history();
        let mut sent = Vec::new();
        let mut canceled = Vec::new();

        let report = delete_message_virtual(
            &store,
            &history,
            VirtualDeleteRequest {
                chat_id: 42,
                vmsg_id: "v2",
            },
            |method| {
                sent.push(method.kind());
                async { Ok::<(), StubError>(()) }
            },
            |vmsg_id| {
                canceled.push(vmsg_id.to_owned());
                2
            },
        )
        .await?;

        assert_eq!(
            report,
            VirtualMessageReport {
                action: VirtualMessageAction::Queued,
                lookup_error: Some("db lookup".to_owned()),
                enqueue_error: Some("enqueue failed".to_owned()),
                canceled: 2,
                real_message_id: None,
                enqueued_op_id: None,
                history_updated: false,
                history_deleted: false,
                mapping_deleted: false,
                delete_mapping_error: None,
            }
        );
        assert!(sent.is_empty());
        assert_eq!(canceled, vec!["v2"]);
        store.snapshot(|state| {
            assert_eq!(
                state.enqueued,
                vec![("v2".to_owned(), 42, PENDING_OP_DELETE, None)]
            );
            assert!(state.events.is_empty());
            assert!(state.deleted_mappings.is_empty());
        });

        Ok(())
    }

    #[tokio::test]
    async fn immediate_send_error_returns_before_history_and_mapping_delete() {
        let store = StoreStub::with_state(StoreState {
            mapping: Some(MessageIdMapping::resolved("v2", 42, 78)),
            ..StoreState::default()
        });
        let history = store.history();

        let error = delete_message_virtual(
            &store,
            &history,
            VirtualDeleteRequest {
                chat_id: 42,
                vmsg_id: "v2",
            },
            |_| async { Err::<(), StubError>(StubError("telegram failed")) },
            |_| 0,
        )
        .await
        .expect_err("Telegram delete failure should propagate");

        assert_eq!(
            error,
            VirtualMessageError::Send("telegram failed".to_owned())
        );
        store.snapshot(|state| {
            assert!(state.events.is_empty());
            assert!(state.deleted_mappings.is_empty());
            assert!(state.enqueued.is_empty());
        });
    }

    fn method_payload(
        method: TelegramOutboundMethod,
    ) -> (TelegramOutboundMethodKind, serde_json::Value) {
        let kind = method.kind();
        let payload = match method {
            TelegramOutboundMethod::EditMessageText(method) => {
                serde_json::to_value(method.as_ref()).expect("edit payload")
            }
            TelegramOutboundMethod::DeleteMessage(method) => {
                serde_json::to_value(method.as_ref()).expect("delete payload")
            }
            other => panic!("unexpected method kind: {:?}", other.kind()),
        };
        (kind, payload)
    }
}
