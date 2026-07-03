//! Per-(chat, thread) dialog session registry.
//!
//! One active session per chat-thread: while a session runs, new debounced
//! jobs for the same key either merge into it (messages from the session's
//! INITIATOR only — third-party text must not steer a running loop) or park
//! on a wait-list and start their own turns after the session releases.
//!
//! The registry is in-memory by design: a crash loses only merge state —
//! merged/deferred jobs were already completed with their own ledger rows,
//! and the injected text lives in chat history, so the next turn sees it as
//! context. Same volatility class as the dialog debounce.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use openplotva_taskman::{DialogJobParams, StatelessJobItem};

/// Serialization key: one active session per chat-thread (absent thread → 0).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionKey {
    pub chat_id: i64,
    pub thread_id: i64,
}

impl SessionKey {
    #[must_use]
    pub fn new(chat_id: i64, thread_id: Option<i32>) -> Self {
        Self {
            chat_id,
            thread_id: thread_id.map(i64::from).unwrap_or_default(),
        }
    }
}

/// One chat message merged into a running session, with everything needed to
/// respawn a follow-up turn when the session ends before consuming it.
#[derive(Clone, Debug)]
pub struct InjectedMessage {
    pub params: DialogJobParams,
}

/// A third-party job parked until the active session releases.
#[derive(Debug)]
pub struct ParkedJob {
    pub queue_name: String,
    pub job: StatelessJobItem,
}

#[derive(Default)]
struct InboxState {
    injected: Vec<InjectedMessage>,
    sealed: bool,
}

/// Initiator messages waiting to enter the running session.
#[derive(Default)]
pub struct SessionInbox {
    state: Mutex<InboxState>,
}

impl SessionInbox {
    /// False when the session already sealed (release race) — the caller
    /// retries `claim` and runs its own turn instead.
    fn try_inject(&self, message: InjectedMessage) -> bool {
        let mut state = self.state.lock().expect("session inbox");
        if state.sealed {
            return false;
        }
        state.injected.push(message);
        true
    }

    /// Messages injected since the last drain; the engine calls this before
    /// every LLM iteration.
    #[must_use]
    pub fn drain_open(&self) -> Vec<InjectedMessage> {
        let mut state = self.state.lock().expect("session inbox");
        std::mem::take(&mut state.injected)
    }

    fn seal(&self) -> Vec<InjectedMessage> {
        let mut state = self.state.lock().expect("session inbox");
        state.sealed = true;
        std::mem::take(&mut state.injected)
    }
}

struct SessionSlot {
    job_id: i64,
    initiator_user_id: i64,
    inbox: Arc<SessionInbox>,
    parked: Vec<ParkedJob>,
}

/// App-wide registry of running dialog sessions.
#[derive(Default)]
pub struct DialogSessionRegistry {
    active: Mutex<HashMap<SessionKey, SessionSlot>>,
}

/// What a dequeued job found at its session key.
pub enum ClaimOutcome {
    /// This job now owns the session; release after finalize.
    Claimed(Arc<SessionInbox>),
    /// The SAME job id is already running (taskman re-delivered it past the
    /// processing timeout). Never self-inject — the caller proceeds into the
    /// turn, where the sent-marker re-entry guard resolves it.
    AlreadyOwned,
    /// Another job runs this chat-thread.
    Busy {
        session_job_id: i64,
        initiator_user_id: i64,
    },
}

/// Everything handed back when a session releases its key.
#[derive(Default)]
pub struct SessionRelease {
    /// Injected but never drained: the newest one respawns a follow-up turn
    /// (older ones are already persisted history and materialize as context).
    pub leftover_injected: Vec<InjectedMessage>,
    /// Third-party turns waiting for the key; all respawn, in arrival order.
    pub parked: Vec<ParkedJob>,
}

impl DialogSessionRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn claim(&self, key: SessionKey, job_id: i64, initiator_user_id: i64) -> ClaimOutcome {
        let mut active = self.active.lock().expect("session registry");
        if let Some(slot) = active.get(&key) {
            if slot.job_id == job_id {
                return ClaimOutcome::AlreadyOwned;
            }
            return ClaimOutcome::Busy {
                session_job_id: slot.job_id,
                initiator_user_id: slot.initiator_user_id,
            };
        }
        let inbox = Arc::new(SessionInbox::default());
        active.insert(
            key,
            SessionSlot {
                job_id,
                initiator_user_id,
                inbox: Arc::clone(&inbox),
                parked: Vec::new(),
            },
        );
        ClaimOutcome::Claimed(inbox)
    }

    /// Merge an initiator message into the running session. False on the
    /// release race (session gone or sealing) — retry `claim`.
    #[must_use]
    pub fn inject(&self, key: SessionKey, session_job_id: i64, message: InjectedMessage) -> bool {
        let inbox = {
            let active = self.active.lock().expect("session registry");
            match active.get(&key) {
                Some(slot) if slot.job_id == session_job_id => Arc::clone(&slot.inbox),
                _ => return false,
            }
        };
        inbox.try_inject(message)
    }

    /// Park a third-party job until the session releases. False on the
    /// release race — retry `claim`.
    #[must_use]
    pub fn park(&self, key: SessionKey, session_job_id: i64, parked: ParkedJob) -> bool {
        let mut active = self.active.lock().expect("session registry");
        match active.get_mut(&key) {
            Some(slot) if slot.job_id == session_job_id => {
                slot.parked.push(parked);
                true
            }
            _ => false,
        }
    }

    /// Release the key: seals the inbox under the registry lock (no message
    /// can slip in between the seal and the removal) and returns everything
    /// that still needs a turn.
    #[must_use]
    pub fn release(&self, key: SessionKey, job_id: i64) -> SessionRelease {
        let slot = {
            let mut active = self.active.lock().expect("session registry");
            match active.get(&key) {
                Some(slot) if slot.job_id == job_id => active.remove(&key),
                _ => None,
            }
        };
        let Some(slot) = slot else {
            return SessionRelease::default();
        };
        SessionRelease {
            leftover_injected: slot.inbox.seal(),
            parked: slot.parked,
        }
    }

    /// Active session count, for gauges and tests.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active.lock().expect("session registry").len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(user_id: i64, text: &str) -> InjectedMessage {
        InjectedMessage {
            params: DialogJobParams {
                chat_id: 42,
                message_id: 100,
                user_id,
                user_full_name: "Ada".to_owned(),
                message_text: text.to_owned(),
                original_text: String::new(),
                meta: serde_json::Value::Null,
                max_output_tokens: 512,
                thread_id: Some(9),
            },
        }
    }

    fn parked_job() -> StatelessJobItem {
        openplotva_taskman::new_dialog_job_at(
            message(8, "deferred").params,
            time::OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("timestamp"),
        )
    }

    #[test]
    fn claim_inject_release_roundtrip() {
        let registry = DialogSessionRegistry::new();
        let key = SessionKey::new(42, Some(9));

        let ClaimOutcome::Claimed(inbox) = registry.claim(key, 1, 7) else {
            panic!("first claim wins");
        };
        assert!(matches!(
            registry.claim(key, 1, 7),
            ClaimOutcome::AlreadyOwned
        ));
        assert!(matches!(
            registry.claim(key, 2, 8),
            ClaimOutcome::Busy {
                session_job_id: 1,
                initiator_user_id: 7,
            }
        ));

        assert!(registry.inject(key, 1, message(7, "ещё вот это")));
        assert_eq!(inbox.drain_open().len(), 1);
        assert!(registry.inject(key, 1, message(7, "и это")));

        assert!(registry.park(
            key,
            1,
            ParkedJob {
                queue_name: "dialog-aifarm".to_owned(),
                job: parked_job(),
            }
        ));

        let release = registry.release(key, 1);
        assert_eq!(release.leftover_injected.len(), 1, "undrained leftover");
        assert_eq!(release.parked.len(), 1);
        assert_eq!(registry.active_count(), 0);

        // Post-release: injection fails (race), a fresh claim succeeds.
        assert!(!registry.inject(key, 1, message(7, "поздно")));
        assert!(matches!(
            registry.claim(key, 3, 8),
            ClaimOutcome::Claimed(_)
        ));
    }

    #[test]
    fn release_by_a_stale_job_id_is_a_noop() {
        let registry = DialogSessionRegistry::new();
        let key = SessionKey::new(42, None);
        let ClaimOutcome::Claimed(_) = registry.claim(key, 1, 7) else {
            panic!("claimed");
        };
        let release = registry.release(key, 999);
        assert!(release.leftover_injected.is_empty());
        assert_eq!(registry.active_count(), 1, "the live session survives");
    }

    #[test]
    fn different_threads_of_one_chat_run_in_parallel() {
        let registry = DialogSessionRegistry::new();
        assert!(matches!(
            registry.claim(SessionKey::new(42, Some(1)), 1, 7),
            ClaimOutcome::Claimed(_)
        ));
        assert!(matches!(
            registry.claim(SessionKey::new(42, Some(2)), 2, 7),
            ClaimOutcome::Claimed(_)
        ));
        assert_eq!(registry.active_count(), 2);
    }
}
