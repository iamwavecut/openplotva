//! Generation lifecycle reactions on the trigger message: 👀 while the ticket
//! waits in the queue, ✍ while the generation runs, cleared on delivery.
//! Failures keep the terminal 🤔 signal owned by the obligations watcher.
//!
//! Standard-emoji only: ⏳/🎨/✨ are not in the Bot API bot-reaction set, and
//! custom-emoji reactions depend on per-chat admin settings, so they are
//! deliberately out of scope here.

use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use openplotva_telegram::{
    TelegramClient, TelegramOutboundMethod, build_message_reaction_clear_method,
    build_message_reaction_method, execute_telegram_method,
};

/// Reaction shown while a generation ticket waits in the queue.
pub const GENERATION_QUEUED_REACTION_EMOJI: &str = "👀";

/// Reaction shown while the generation is actively running.
pub const GENERATION_PROGRESS_REACTION_EMOJI: &str = "✍";

/// Direct `setMessageReaction` execution bound; mirrors the terminal signal.
pub const GENERATION_REACTION_TIMEOUT: Duration = Duration::from_secs(3);

/// Boxed future for best-effort reaction updates.
pub type GenerationReactionFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Trigger message a generation lifecycle reaction lands on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationReactionTarget {
    pub chat_id: i64,
    pub message_id: i32,
}

/// Best-effort generation lifecycle reactions: every transition replaces the
/// bot's single reaction slot, so calls are idempotent and last-writer-wins.
/// Failures are logged and swallowed — reactions must never fail scheduling,
/// generation, or delivery.
pub trait GenerationReactionSink: Send + Sync + fmt::Debug {
    fn set_queued<'a>(&'a self, target: GenerationReactionTarget) -> GenerationReactionFuture<'a>;
    fn set_progress<'a>(&'a self, target: GenerationReactionTarget)
    -> GenerationReactionFuture<'a>;
    fn clear<'a>(&'a self, target: GenerationReactionTarget) -> GenerationReactionFuture<'a>;
}

/// Shared generation reaction sink handle.
pub type GenerationReactions = Arc<dyn GenerationReactionSink>;

/// Production sink: direct bounded `setMessageReaction` calls, matching the
/// terminal-signal pattern (no dispatcher queue — reactions are cheap, urgent,
/// and replace-idempotent, so dedup/rate machinery would only add latency).
pub struct GenerationReactionSignaler {
    client: TelegramClient,
}

impl GenerationReactionSignaler {
    #[must_use]
    pub fn new(client: TelegramClient) -> Self {
        Self { client }
    }

    async fn execute(
        &self,
        target: GenerationReactionTarget,
        phase: &'static str,
        method: TelegramOutboundMethod,
    ) {
        let result = tokio::time::timeout(
            GENERATION_REACTION_TIMEOUT,
            execute_telegram_method(&self.client, method),
        )
        .await;
        let error = match result {
            Ok(Ok(_)) => return,
            Ok(Err(error)) => error.to_string(),
            Err(_) => format!(
                "setMessageReaction timed out after {}s",
                GENERATION_REACTION_TIMEOUT.as_secs()
            ),
        };
        tracing::warn!(
            chat_id = target.chat_id,
            message_id = target.message_id,
            phase,
            %error,
            "generation reaction update failed"
        );
    }
}

impl fmt::Debug for GenerationReactionSignaler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenerationReactionSignaler")
            .finish_non_exhaustive()
    }
}

impl GenerationReactionSink for GenerationReactionSignaler {
    fn set_queued<'a>(&'a self, target: GenerationReactionTarget) -> GenerationReactionFuture<'a> {
        Box::pin(async move {
            self.execute(
                target,
                "queued",
                build_message_reaction_method(
                    target.chat_id,
                    i64::from(target.message_id),
                    GENERATION_QUEUED_REACTION_EMOJI,
                ),
            )
            .await;
        })
    }

    fn set_progress<'a>(
        &'a self,
        target: GenerationReactionTarget,
    ) -> GenerationReactionFuture<'a> {
        Box::pin(async move {
            self.execute(
                target,
                "progress",
                build_message_reaction_method(
                    target.chat_id,
                    i64::from(target.message_id),
                    GENERATION_PROGRESS_REACTION_EMOJI,
                ),
            )
            .await;
        })
    }

    fn clear<'a>(&'a self, target: GenerationReactionTarget) -> GenerationReactionFuture<'a> {
        Box::pin(async move {
            self.execute(
                target,
                "clear",
                build_message_reaction_clear_method(target.chat_id, i64::from(target.message_id)),
            )
            .await;
        })
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Mutex;

    use super::*;

    /// Recording sink for lifecycle tests: (phase, target) per call.
    #[derive(Debug, Default)]
    pub struct RecordingReactionSink {
        pub calls: Mutex<Vec<(&'static str, GenerationReactionTarget)>>,
    }

    impl GenerationReactionSink for RecordingReactionSink {
        fn set_queued<'a>(
            &'a self,
            target: GenerationReactionTarget,
        ) -> GenerationReactionFuture<'a> {
            self.calls
                .lock()
                .expect("reaction calls lock")
                .push(("queued", target));
            Box::pin(async {})
        }

        fn set_progress<'a>(
            &'a self,
            target: GenerationReactionTarget,
        ) -> GenerationReactionFuture<'a> {
            self.calls
                .lock()
                .expect("reaction calls lock")
                .push(("progress", target));
            Box::pin(async {})
        }

        fn clear<'a>(&'a self, target: GenerationReactionTarget) -> GenerationReactionFuture<'a> {
            self.calls
                .lock()
                .expect("reaction calls lock")
                .push(("clear", target));
            Box::pin(async {})
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unreachable_signaler() -> GenerationReactionSignaler {
        // Nothing listens on the discard port: the direct call fails fast and
        // must be swallowed (reactions are strictly best-effort).
        let client =
            openplotva_telegram::telegram_client_with_base_url("TEST-TOKEN", "http://127.0.0.1:9/")
                .expect("test telegram client");
        GenerationReactionSignaler::new(client)
    }

    #[tokio::test]
    async fn reaction_failures_are_swallowed() {
        let signaler = unreachable_signaler();
        let target = GenerationReactionTarget {
            chat_id: 42,
            message_id: 100,
        };

        signaler.set_queued(target).await;
        signaler.set_progress(target).await;
        signaler.clear(target).await;
    }

    #[test]
    fn lifecycle_emojis_stay_in_the_bot_allowed_set() {
        // The Bot API bot-reaction list includes 👀 and ✍ (and 🤔 for the
        // terminal signal); ⏳/🎨/✨ are NOT in it — pinned so a well-meaning
        // edit does not swap in an emoji Telegram will reject.
        assert_eq!(GENERATION_QUEUED_REACTION_EMOJI, "👀");
        assert_eq!(GENERATION_PROGRESS_REACTION_EMOJI, "✍");
    }
}
