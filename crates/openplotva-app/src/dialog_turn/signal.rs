//! Terminal user signal: a reaction on the trigger message when a dialog turn
//! gives up, with a plain-text emoji reply as fallback.
//!
//! Ordering is crash-safe by construction: the reaction is attempted first
//! (Telegram `setMessageReaction` is replace-idempotent, so a rerun can at
//! worst re-react), and the `terminal_user_signal` job-event marker gates only
//! the non-idempotent text fallback so it is sent at most once per job.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use openplotva_telegram::{
    ChatRef, DispatcherQueue, ReplyMessageRef, TELEGRAM_PARSE_MODE_HTML, TelegramClient,
    TextMessageRequest, build_message_reaction_method, execute_telegram_method,
};
use time::Duration as TimeDuration;

use crate::virtual_messages::{
    QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory, queue_text_message_parts,
};

/// Job event stage marking a delivered terminal user signal. Rides the job
/// record (events survive requeues) and gates only the text fallback.
pub const TERMINAL_USER_SIGNAL_STAGE: &str = "terminal_user_signal";

/// Default reaction emoji (`DIALOG_TERMINAL_REACTION_EMOJI`). Must stay inside
/// the Bot API allowed-reaction set — "🤔" is allowed, "⏳" is not.
pub const DEFAULT_DIALOG_TERMINAL_REACTION_EMOJI: &str = "🤔";

/// Default job age beyond which terminal failures skip the signal
/// (`DIALOG_TERMINAL_SIGNAL_MAX_AGE_SECS`).
pub const DEFAULT_DIALOG_TERMINAL_SIGNAL_MAX_AGE_SECS: i32 = 600;

/// Direct `setMessageReaction` execution bound; failures past it fall back.
pub const TERMINAL_REACTION_TIMEOUT: Duration = Duration::from_secs(3);

/// Boxed future returned by terminal user signal implementations.
pub type UserSignalFuture<'a> = Pin<Box<dyn Future<Output = UserSignalResult> + Send + 'a>>;

/// Where and how to signal one terminal turn failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignalTarget {
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    /// Trigger message the reaction lands on.
    pub message_id: i32,
    /// Reaction emoji for this call (terminal failures and acknowledgment
    /// cases use different emojis, so it is per-call).
    pub emoji: String,
    /// Whether the non-idempotent text fallback may run. False once the
    /// `terminal_user_signal` marker exists on the job.
    pub text_fallback_allowed: bool,
}

/// Observable outcome of one signal attempt, recorded in the ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UserSignalResult {
    ReactionSent,
    TextFallbackSent,
    Failed(String),
    Skipped(&'static str),
}

impl UserSignalResult {
    /// Ledger `user_signal` column value.
    #[must_use]
    pub fn ledger_value(&self) -> &str {
        match self {
            Self::ReactionSent => "reaction_sent",
            Self::TextFallbackSent => "text_fallback_sent",
            Self::Failed(_) => "failed",
            Self::Skipped(reason) => reason,
        }
    }
}

/// Signals a turn failure to the user; the future resolves to the real result
/// so finalization records what actually happened.
pub trait TerminalUserSignal: Send + Sync {
    fn signal_turn_failure<'a>(&'a self, target: SignalTarget) -> UserSignalFuture<'a>;
}

/// Per-worker signal wiring threaded through the dialog worker options.
#[derive(Clone, Copy)]
pub struct TurnSignalPolicy<'a> {
    pub signal: Option<&'a dyn TerminalUserSignal>,
    /// Emoji passed to the signal for terminal failures.
    pub reaction_emoji: &'a str,
    /// Job age beyond which terminal failures skip the signal.
    pub max_age: TimeDuration,
}

impl<'a> TurnSignalPolicy<'a> {
    #[must_use]
    pub fn new(
        signal: Option<&'a dyn TerminalUserSignal>,
        reaction_emoji: &'a str,
        max_age_secs: i32,
    ) -> Self {
        Self {
            signal,
            reaction_emoji,
            max_age: TimeDuration::seconds(i64::from(max_age_secs.max(1))),
        }
    }
}

impl Default for TurnSignalPolicy<'_> {
    fn default() -> Self {
        Self::new(
            None,
            DEFAULT_DIALOG_TERMINAL_REACTION_EMOJI,
            DEFAULT_DIALOG_TERMINAL_SIGNAL_MAX_AGE_SECS,
        )
    }
}

/// Production signal: direct bounded `setMessageReaction`, then the emoji as a
/// plain immediate reply through the dispatcher when the reaction fails.
#[derive(Clone)]
pub struct DispatcherTerminalUserSignal {
    client: TelegramClient,
    queue: Arc<DispatcherQueue>,
    next_virtual_id: VirtualIdFactory,
}

impl DispatcherTerminalUserSignal {
    #[must_use]
    pub fn new(client: TelegramClient, queue: Arc<DispatcherQueue>) -> Self {
        Self {
            client,
            queue,
            next_virtual_id: monotonic_virtual_id_factory("dialog-signal"),
        }
    }

    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }

    async fn send_reaction(&self, target: &SignalTarget) -> Result<(), String> {
        let method = build_message_reaction_method(
            target.chat_id,
            i64::from(target.message_id),
            &target.emoji,
        );
        match tokio::time::timeout(
            TERMINAL_REACTION_TIMEOUT,
            execute_telegram_method(&self.client, method),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err(format!(
                "setMessageReaction timed out after {}s",
                TERMINAL_REACTION_TIMEOUT.as_secs()
            )),
        }
    }

    async fn send_text_fallback(&self, target: &SignalTarget) -> Result<(), String> {
        let chat = ChatRef {
            id: target.chat_id,
            is_forum: target.thread_id.is_some(),
        };
        let reply_to = ReplyMessageRef {
            message_id: i64::from(target.message_id),
            chat,
            is_topic_message: target.thread_id.is_some(),
            message_thread_id: target.thread_id.map(i64::from).unwrap_or_default(),
        };
        let request = TextMessageRequest {
            chat: Some(chat),
            message_thread_id: target.thread_id.map(i64::from).unwrap_or_default(),
            disable_notification: false,
            allow_sending_without_reply: None,
            text: target.emoji.clone(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_markup: None,
        };
        queue_text_message_parts(
            &self.queue,
            QueueTextRequest {
                protected: false,
                debounce_key: None,
                message: &request,
                reply_to: Some(&reply_to),
                immediate_first: true,
                bypass_chat_restrictions: false,
                ephemeral_delete_after: None,
            },
            || (self.next_virtual_id)(),
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
    }
}

impl TerminalUserSignal for DispatcherTerminalUserSignal {
    fn signal_turn_failure<'a>(&'a self, target: SignalTarget) -> UserSignalFuture<'a> {
        Box::pin(async move {
            let reaction_error = match self.send_reaction(&target).await {
                Ok(()) => return UserSignalResult::ReactionSent,
                Err(error) => error,
            };
            tracing::warn!(
                chat_id = target.chat_id,
                message_id = target.message_id,
                error = %reaction_error,
                text_fallback_allowed = target.text_fallback_allowed,
                "terminal failure reaction failed"
            );
            if !target.text_fallback_allowed {
                return UserSignalResult::Failed(format!(
                    "reaction failed and text fallback already recorded for this job: {reaction_error}"
                ));
            }
            match self.send_text_fallback(&target).await {
                Ok(()) => UserSignalResult::TextFallbackSent,
                Err(fallback_error) => UserSignalResult::Failed(format!(
                    "reaction failed: {reaction_error}; text fallback failed: {fallback_error}"
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unreachable_signal() -> (DispatcherTerminalUserSignal, Arc<DispatcherQueue>) {
        // Nothing listens on the discard port, so the direct reaction call
        // fails fast with a connect error and exercises the fallback path.
        let client =
            openplotva_telegram::telegram_client_with_base_url("TEST-TOKEN", "http://127.0.0.1:9/")
                .expect("test telegram client");
        let queue = Arc::new(DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        ));
        let signal = DispatcherTerminalUserSignal::new(client, Arc::clone(&queue))
            .with_virtual_id_factory(Arc::new(|| "signal-v1".to_owned()));
        (signal, queue)
    }

    fn target(text_fallback_allowed: bool) -> SignalTarget {
        SignalTarget {
            chat_id: 42,
            thread_id: Some(9),
            message_id: 100,
            emoji: DEFAULT_DIALOG_TERMINAL_REACTION_EMOJI.to_owned(),
            text_fallback_allowed,
        }
    }

    #[tokio::test]
    async fn failed_reaction_queues_emoji_text_fallback_as_reply() {
        let (signal, queue) = unreachable_signal();

        let result = signal.signal_turn_failure(target(true)).await;

        assert_eq!(result, UserSignalResult::TextFallbackSent);
        let item = queue.dequeue_immediate().expect("queued fallback text");
        assert_eq!(
            item.method_kind(),
            Some(openplotva_telegram::TelegramOutboundMethodKind::SendMessage)
        );
        let (_, method) = item.into_parts();
        let Some(openplotva_telegram::TelegramOutboundMethod::SendMessage(method)) = method else {
            panic!("expected sendMessage fallback");
        };
        let value = serde_json::to_value(method.as_ref()).expect("method json");
        assert_eq!(value["chat_id"], 42);
        assert_eq!(value["text"], DEFAULT_DIALOG_TERMINAL_REACTION_EMOJI);
        assert_eq!(value["reply_parameters"]["message_id"], 100);
    }

    #[tokio::test]
    async fn failed_reaction_without_fallback_permission_reports_failed() {
        let (signal, queue) = unreachable_signal();

        let result = signal.signal_turn_failure(target(false)).await;

        let UserSignalResult::Failed(error) = result else {
            panic!("expected failed result, got {result:?}");
        };
        assert!(error.contains("text fallback already recorded"));
        assert!(queue.dequeue_immediate().is_none(), "no text was queued");
    }

    #[test]
    fn ledger_values_are_stable() {
        assert_eq!(
            UserSignalResult::ReactionSent.ledger_value(),
            "reaction_sent"
        );
        assert_eq!(
            UserSignalResult::TextFallbackSent.ledger_value(),
            "text_fallback_sent"
        );
        assert_eq!(
            UserSignalResult::Failed("boom".to_owned()).ledger_value(),
            "failed"
        );
        assert_eq!(
            UserSignalResult::Skipped("skipped_too_old").ledger_value(),
            "skipped_too_old"
        );
    }
}
