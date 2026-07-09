//! Best-effort Telegram chat-action pulses for active long-running jobs.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::permissions::{self, ChatPermissionPolicy, ChatPermissionStore};
use openplotva_server::ACTION_SEND_TEXT;
use openplotva_telegram::{
    ChatActionRequest, ChatRef, TelegramOutboundMethod, TelegramOutboundMethodKind,
};
use time::OffsetDateTime;
use tokio::task::JoinHandle;

pub type TelegramActivityFuture<'a> =
    Pin<Box<dyn Future<Output = TelegramActivityReport> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramActivityAction {
    Typing,
    UploadPhoto,
    /// Official Bot API has no `upload_audio` chat action; songs are files.
    UploadDocumentForAudio,
}

impl TelegramActivityAction {
    #[must_use]
    pub const fn as_telegram_action(self) -> &'static str {
        match self {
            Self::Typing => "typing",
            Self::UploadPhoto => "upload_photo",
            Self::UploadDocumentForAudio => "upload_document",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TelegramActivityReport {
    SkippedDisabled,
    SkippedInvalidRequest,
    SkippedPermission,
    Sent,
    SendFailed { message: String },
}

impl TelegramActivityReport {
    fn detail(&self) -> Option<String> {
        match self {
            Self::SkippedDisabled => Some("disabled".to_owned()),
            Self::SkippedInvalidRequest => Some("invalid_request".to_owned()),
            Self::SkippedPermission => Some("permission_denied".to_owned()),
            Self::Sent => None,
            Self::SendFailed { message } => Some(message.clone()),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramActivitySnapshot {
    pub action: Option<TelegramActivityAction>,
    pub sent: u64,
    pub failed: u64,
    pub skipped: u64,
    pub last_error: Option<String>,
}

impl TelegramActivitySnapshot {
    #[must_use]
    pub fn for_action(action: TelegramActivityAction) -> Self {
        Self {
            action: Some(action),
            ..Self::default()
        }
    }

    fn record(&mut self, report: TelegramActivityReport) {
        match &report {
            TelegramActivityReport::Sent => self.sent += 1,
            TelegramActivityReport::SendFailed { .. } => self.failed += 1,
            TelegramActivityReport::SkippedDisabled
            | TelegramActivityReport::SkippedInvalidRequest
            | TelegramActivityReport::SkippedPermission => self.skipped += 1,
        }
        if let Some(detail) = report.detail() {
            self.last_error = Some(detail);
        }
    }

    #[must_use]
    pub fn has_activity(&self) -> bool {
        self.sent > 0 || self.failed > 0 || self.skipped > 0
    }
}

pub trait TelegramActivityEffects {
    fn send_activity_action<'a>(
        &'a self,
        chat_id: i64,
        thread_id: Option<i32>,
        action: TelegramActivityAction,
    ) -> TelegramActivityFuture<'a>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopTelegramActivityEffects;

impl TelegramActivityEffects for NoopTelegramActivityEffects {
    fn send_activity_action<'a>(
        &'a self,
        _chat_id: i64,
        _thread_id: Option<i32>,
        _action: TelegramActivityAction,
    ) -> TelegramActivityFuture<'a> {
        Box::pin(async { TelegramActivityReport::SkippedInvalidRequest })
    }
}

#[derive(Clone)]
pub struct TelegramActivityRuntimeEffects<Store> {
    telegram: openplotva_telegram::TelegramClient,
    permissions: Arc<ChatPermissionPolicy<Store>>,
}

impl<Store> TelegramActivityRuntimeEffects<Store> {
    #[must_use]
    pub fn new(
        telegram: openplotva_telegram::TelegramClient,
        permissions: Arc<ChatPermissionPolicy<Store>>,
    ) -> Self {
        Self {
            telegram,
            permissions,
        }
    }
}

impl<Store> fmt::Debug for TelegramActivityRuntimeEffects<Store> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TelegramActivityRuntimeEffects")
            .finish_non_exhaustive()
    }
}

impl<Store> TelegramActivityEffects for TelegramActivityRuntimeEffects<Store>
where
    Store: ChatPermissionStore + Send + Sync,
{
    fn send_activity_action<'a>(
        &'a self,
        chat_id: i64,
        thread_id: Option<i32>,
        action: TelegramActivityAction,
    ) -> TelegramActivityFuture<'a> {
        Box::pin(async move {
            if chat_id == 0 {
                return TelegramActivityReport::SkippedInvalidRequest;
            }

            let report = self
                .permissions
                .can_perform_action_at(chat_id, None, ACTION_SEND_TEXT, OffsetDateTime::now_utc())
                .await;
            if let Some(load_error) = report.load_error.as_deref() {
                tracing::debug!(
                    chat_id,
                    %load_error,
                    "failed to load Telegram permission state before activity action"
                );
            }
            if !report.allowed {
                return TelegramActivityReport::SkippedPermission;
            }

            let request = ChatActionRequest {
                chat: ChatRef {
                    id: chat_id,
                    is_forum: thread_id.is_some(),
                },
                message_thread_id: i64::from(thread_id.unwrap_or_default()),
                action: action.as_telegram_action().to_owned(),
            };
            let Ok(method) = openplotva_telegram::build_chat_action_method(&request) else {
                return TelegramActivityReport::SkippedInvalidRequest;
            };

            match openplotva_telegram::execute_telegram_method(
                &self.telegram,
                TelegramOutboundMethod::from(method),
            )
            .await
            {
                Ok(_) => TelegramActivityReport::Sent,
                Err(error) => {
                    if permissions::telegram_execute_error_is_permission_error(&error) {
                        let update = self
                            .permissions
                            .record_send_permission_error(
                                chat_id,
                                TelegramOutboundMethodKind::SendChatAction,
                            )
                            .await;
                        if let Some(load_error) = update.load_error.as_deref() {
                            tracing::warn!(
                                chat_id,
                                %load_error,
                                "failed to load chat permission state after activity permission error"
                            );
                        }
                        if let Some(save_error) = update.save_error.as_deref() {
                            tracing::warn!(
                                chat_id,
                                %save_error,
                                "failed to persist chat permission state after activity permission error"
                            );
                        }
                    }
                    TelegramActivityReport::SendFailed {
                        message: error.to_string(),
                    }
                }
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TelegramActivityPulseSettings {
    pub enabled: bool,
    pub interval: Duration,
    pub initial_delay: Duration,
}

impl TelegramActivityPulseSettings {
    #[must_use]
    pub fn from_millis(enabled: bool, interval_ms: i32, initial_delay_ms: i32) -> Self {
        Self {
            enabled,
            interval: Duration::from_millis(u64::try_from(interval_ms.max(1)).unwrap_or(1)),
            initial_delay: Duration::from_millis(
                u64::try_from(initial_delay_ms.max(0)).unwrap_or_default(),
            ),
        }
    }
}

impl From<&openplotva_config::TelegramActivityPulseConfig> for TelegramActivityPulseSettings {
    fn from(config: &openplotva_config::TelegramActivityPulseConfig) -> Self {
        Self::from_millis(config.enabled, config.interval_ms, config.initial_delay_ms)
    }
}

#[derive(Clone)]
pub struct TelegramActivityPulse {
    settings: TelegramActivityPulseSettings,
    effects: Arc<dyn TelegramActivityEffects + Send + Sync>,
}

impl TelegramActivityPulse {
    #[must_use]
    pub fn new(
        settings: TelegramActivityPulseSettings,
        effects: Arc<dyn TelegramActivityEffects + Send + Sync>,
    ) -> Self {
        Self { settings, effects }
    }

    #[must_use]
    pub fn disabled() -> Self {
        Self {
            settings: TelegramActivityPulseSettings::from_millis(false, 1_000, 0),
            effects: Arc::new(NoopTelegramActivityEffects),
        }
    }

    #[must_use]
    pub fn start(
        &self,
        chat_id: i64,
        thread_id: Option<i32>,
        action: TelegramActivityAction,
    ) -> ActiveTelegramActivityPulse {
        let snapshot = Arc::new(Mutex::new(TelegramActivitySnapshot::for_action(action)));
        if !self.settings.enabled {
            record_snapshot(
                &snapshot,
                TelegramActivityReport::SkippedDisabled,
                action,
                chat_id,
            );
            return ActiveTelegramActivityPulse {
                snapshot,
                handle: None,
            };
        }

        let settings = self.settings;
        let effects = Arc::clone(&self.effects);
        let task_snapshot = Arc::clone(&snapshot);
        let handle = tokio::spawn(async move {
            if !settings.initial_delay.is_zero() {
                tokio::time::sleep(settings.initial_delay).await;
            }
            loop {
                let report = effects
                    .send_activity_action(chat_id, thread_id, action)
                    .await;
                record_snapshot(&task_snapshot, report, action, chat_id);
                tokio::time::sleep(settings.interval).await;
            }
        });

        ActiveTelegramActivityPulse {
            snapshot,
            handle: Some(handle),
        }
    }
}

pub struct ActiveTelegramActivityPulse {
    snapshot: Arc<Mutex<TelegramActivitySnapshot>>,
    handle: Option<JoinHandle<()>>,
}

impl ActiveTelegramActivityPulse {
    #[must_use]
    pub fn snapshot(&self) -> TelegramActivitySnapshot {
        lock_snapshot(&self.snapshot).clone()
    }
}

impl Drop for ActiveTelegramActivityPulse {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

fn record_snapshot(
    snapshot: &Arc<Mutex<TelegramActivitySnapshot>>,
    report: TelegramActivityReport,
    action: TelegramActivityAction,
    chat_id: i64,
) {
    if let TelegramActivityReport::SendFailed { message } = &report {
        tracing::debug!(
            chat_id,
            action = action.as_telegram_action(),
            %message,
            "failed to send Telegram activity action"
        );
    }
    lock_snapshot(snapshot).record(report);
}

fn lock_snapshot(
    snapshot: &Arc<Mutex<TelegramActivitySnapshot>>,
) -> std::sync::MutexGuard<'_, TelegramActivitySnapshot> {
    snapshot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{
        TelegramActivityAction, TelegramActivityEffects, TelegramActivityFuture,
        TelegramActivityPulse, TelegramActivityPulseSettings, TelegramActivityReport,
    };

    #[derive(Debug)]
    struct RecordingEffects {
        report: TelegramActivityReport,
        calls: Mutex<Vec<(i64, Option<i32>, TelegramActivityAction)>>,
    }

    impl RecordingEffects {
        fn new(report: TelegramActivityReport) -> Self {
            Self {
                report,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<(i64, Option<i32>, TelegramActivityAction)> {
            self.calls.lock().expect("calls").clone()
        }
    }

    impl TelegramActivityEffects for RecordingEffects {
        fn send_activity_action<'a>(
            &'a self,
            chat_id: i64,
            thread_id: Option<i32>,
            action: TelegramActivityAction,
        ) -> TelegramActivityFuture<'a> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("calls")
                    .push((chat_id, thread_id, action));
                self.report.clone()
            })
        }
    }

    fn pulse_with(
        effects: Arc<RecordingEffects>,
        enabled: bool,
        interval_ms: i32,
        initial_delay_ms: i32,
    ) -> TelegramActivityPulse {
        TelegramActivityPulse::new(
            TelegramActivityPulseSettings::from_millis(enabled, interval_ms, initial_delay_ms),
            effects,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn pulse_sends_immediately_and_repeats() {
        let effects = Arc::new(RecordingEffects::new(TelegramActivityReport::Sent));
        let pulse = pulse_with(Arc::clone(&effects), true, 4_000, 0);
        let guard = pulse.start(42, Some(7), TelegramActivityAction::Typing);

        tokio::task::yield_now().await;
        assert_eq!(
            effects.calls(),
            vec![(42, Some(7), TelegramActivityAction::Typing)]
        );
        assert_eq!(guard.snapshot().sent, 1);

        tokio::time::advance(std::time::Duration::from_millis(4_000)).await;
        tokio::task::yield_now().await;
        assert_eq!(effects.calls().len(), 2);
        assert_eq!(guard.snapshot().sent, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn pulse_respects_initial_delay() {
        let effects = Arc::new(RecordingEffects::new(TelegramActivityReport::Sent));
        let pulse = pulse_with(Arc::clone(&effects), true, 4_000, 500);
        let guard = pulse.start(42, None, TelegramActivityAction::UploadPhoto);

        tokio::task::yield_now().await;
        assert!(effects.calls().is_empty());

        tokio::time::advance(std::time::Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            effects.calls(),
            vec![(42, None, TelegramActivityAction::UploadPhoto)]
        );
        assert_eq!(guard.snapshot().sent, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_pulse_records_skip_without_sending() {
        let effects = Arc::new(RecordingEffects::new(TelegramActivityReport::Sent));
        let pulse = pulse_with(Arc::clone(&effects), false, 4_000, 0);
        let guard = pulse.start(42, None, TelegramActivityAction::Typing);

        tokio::task::yield_now().await;
        assert!(effects.calls().is_empty());
        assert_eq!(guard.snapshot().skipped, 1);
        assert_eq!(guard.snapshot().last_error.as_deref(), Some("disabled"));
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_guard_stops_future_pulses() {
        let effects = Arc::new(RecordingEffects::new(TelegramActivityReport::Sent));
        let pulse = pulse_with(Arc::clone(&effects), true, 4_000, 0);
        let guard = pulse.start(42, None, TelegramActivityAction::Typing);

        tokio::task::yield_now().await;
        assert_eq!(effects.calls().len(), 1);
        drop(guard);

        tokio::time::advance(std::time::Duration::from_millis(8_000)).await;
        tokio::task::yield_now().await;
        assert_eq!(effects.calls().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn send_failure_is_recorded_nonfatally() {
        let effects = Arc::new(RecordingEffects::new(TelegramActivityReport::SendFailed {
            message: "telegram down".to_owned(),
        }));
        let pulse = pulse_with(Arc::clone(&effects), true, 4_000, 0);
        let guard = pulse.start(42, None, TelegramActivityAction::Typing);

        tokio::task::yield_now().await;
        let snapshot = guard.snapshot();
        assert_eq!(snapshot.failed, 1);
        assert_eq!(snapshot.last_error.as_deref(), Some("telegram down"));
    }
}
