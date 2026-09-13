//! App-level "another take" callback for generated songs: replays the stored
//! material through the regular song scheduler so the listener gets the same
//! tags and lyrics with a new seed, under the same VIP, rate and queue gates.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{
    CallbackQuery as TelegramCallbackQuery, MaybeInaccessibleMessage, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType,
};
use openplotva_core::ChatMessageMeta;
use openplotva_storage::{GeneratedSongRecord, PostgresGeneratedSongStore, StorageError};
use openplotva_telegram::{
    CallbackActionParse, CallbackAnswerRequest, SONG_RETAKE_ACTION, TelegramOutboundMethod,
    build_callback_answer_method, parse_callback_action, song_retake_callback_song_id,
};
use thiserror::Error;

use crate::{
    dialog_tools::{
        SongRetake, SongScheduleRejection, SongScheduleRequest, SongScheduleResult, SongScheduler,
    },
    updates::UpdateHandler,
};

pub const SONG_RETAKE_QUEUED_TEXT: &str = "🎲 Ещё один тейк поставлен в очередь";
const SONG_RETAKE_MISSING_TEXT: &str = "Не нашёл эту песню: она слишком старая, закажи заново.";
const SONG_RETAKE_FAILED_TEXT: &str = "Не получилось поставить тейк, попробуй позже.";

/// Boxed future returned by generated-song lookups.
pub type GeneratedSongLookupFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Option<GeneratedSongRecord>, E>> + Send + 'a>>;

pub trait GeneratedSongLookup {
    /// Concrete store error.
    type Error: fmt::Display + Send + Sync + 'static;

    fn generated_song<'a>(&'a self, id: i64) -> GeneratedSongLookupFuture<'a, Self::Error>;
}

impl GeneratedSongLookup for PostgresGeneratedSongStore {
    type Error = StorageError;

    fn generated_song<'a>(&'a self, id: i64) -> GeneratedSongLookupFuture<'a, Self::Error> {
        Box::pin(self.get(id))
    }
}

/// Boxed future returned by song retake effects.
pub type SongRetakeEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait SongRetakeEffects {
    /// Concrete effect error.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Execute one direct Telegram Bot API method.
    fn execute_song_retake_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> SongRetakeEffectFuture<'a, Self::Error>;
}

impl SongRetakeEffects for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn execute_song_retake_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> SongRetakeEffectFuture<'a, Self::Error> {
        Box::pin(async move { method.execute_with(self).await.map(|_| ()) })
    }
}

/// Result of one song retake callback update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SongRetakeCallbackOutcome {
    /// Update did not belong to this slice and was delegated.
    Delegated,
    BadDataAck,
    /// The stored song no longer exists.
    SongMissing,
    /// The scheduler refused the retake (VIP, limits, availability).
    Rejected(SongScheduleRejection),
    /// A retake job was queued.
    Scheduled {
        job_id: Option<i64>,
    },
    /// Lookup or scheduling failed.
    Failed,
}

/// Fatal errors from the song retake callback wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SongRetakeCallbackError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

pub struct SongRetakeCallbackUpdateHandler<Songs, Effects, Next> {
    songs: Arc<Songs>,
    scheduler: Arc<dyn SongScheduler>,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Songs, Effects, Next> SongRetakeCallbackUpdateHandler<Songs, Effects, Next> {
    /// Build a song retake callback handler around the real downstream update handler.
    pub fn new(
        songs: Arc<Songs>,
        scheduler: Arc<dyn SongScheduler>,
        effects: Arc<Effects>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            songs,
            scheduler,
            effects,
            next,
        }
    }
}

impl<Songs, Effects, Next> UpdateHandler for SongRetakeCallbackUpdateHandler<Songs, Effects, Next>
where
    Songs: GeneratedSongLookup + Send + Sync,
    Effects: SongRetakeEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = SongRetakeCallbackError;

    async fn handle_update(&self, update: TelegramUpdate) -> Result<(), Self::Error> {
        handle_song_retake_callback_update_or_else(
            self.songs.as_ref(),
            self.scheduler.as_ref(),
            self.effects.as_ref(),
            update,
            |update| self.next.handle_update(update),
        )
        .await
        .map(|_| ())
    }
}

pub async fn handle_song_retake_callback_update_or_else<
    Songs,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    songs: &Songs,
    scheduler: &dyn SongScheduler,
    effects: &Effects,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<SongRetakeCallbackOutcome, SongRetakeCallbackError>
where
    Songs: GeneratedSongLookup + Sync,
    Effects: SongRetakeEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let song_id = match &update.update_type {
        TelegramUpdateType::CallbackQuery(query) => song_retake_song_id_from_query(query),
        _ => None,
    };
    let Some(song_id) = song_id else {
        handle_other(update)
            .await
            .map_err(|error| SongRetakeCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(SongRetakeCallbackOutcome::Delegated);
    };
    let TelegramUpdateType::CallbackQuery(query) = &update.update_type else {
        unreachable!("callback query was already matched");
    };
    Ok(handle_song_retake_callback(songs, scheduler, effects, query, song_id).await)
}

fn song_retake_song_id_from_query(query: &TelegramCallbackQuery) -> Option<i64> {
    let raw = query.data.as_deref().unwrap_or_default();
    match parse_callback_action(raw) {
        CallbackActionParse::Action { data, action } if action == SONG_RETAKE_ACTION => {
            Some(song_retake_callback_song_id(&data))
        }
        _ => None,
    }
}

pub async fn handle_song_retake_callback<Songs, Effects>(
    songs: &Songs,
    scheduler: &dyn SongScheduler,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    song_id: i64,
) -> SongRetakeCallbackOutcome
where
    Songs: GeneratedSongLookup + Sync,
    Effects: SongRetakeEffects + Sync,
{
    if song_id <= 0 {
        try_answer_callback(effects, &query.id, "", false, "bad song retake data ack").await;
        return SongRetakeCallbackOutcome::BadDataAck;
    }
    let record = match songs.generated_song(song_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            try_answer_callback(
                effects,
                &query.id,
                SONG_RETAKE_MISSING_TEXT,
                true,
                "song retake missing alert",
            )
            .await;
            return SongRetakeCallbackOutcome::SongMissing;
        }
        Err(error) => {
            tracing::warn!(%error, song_id, "failed to load generated song for retake");
            try_answer_callback(
                effects,
                &query.id,
                SONG_RETAKE_FAILED_TEXT,
                true,
                "song retake lookup failure alert",
            )
            .await;
            return SongRetakeCallbackOutcome::Failed;
        }
    };
    let request = song_retake_schedule_request(query, &record);
    match scheduler.schedule_song(request).await {
        Ok(result) => {
            if let Some(rejection) = result.rejection {
                try_answer_callback(
                    effects,
                    &query.id,
                    rejection.model_facing_message(),
                    true,
                    "song retake rejection alert",
                )
                .await;
                return SongRetakeCallbackOutcome::Rejected(rejection);
            }
            try_answer_callback(
                effects,
                &query.id,
                &song_retake_queued_text(&result),
                false,
                "song retake queued ack",
            )
            .await;
            SongRetakeCallbackOutcome::Scheduled {
                job_id: result.job_id,
            }
        }
        Err(error) => {
            tracing::warn!(%error, song_id, "failed to schedule song retake");
            try_answer_callback(
                effects,
                &query.id,
                SONG_RETAKE_FAILED_TEXT,
                true,
                "song retake failure alert",
            )
            .await;
            SongRetakeCallbackOutcome::Failed
        }
    }
}

/// The retake replies to the song message the button sits on, is attributed to
/// the listener who pressed it, and carries the stored material verbatim.
fn song_retake_schedule_request(
    query: &TelegramCallbackQuery,
    record: &GeneratedSongRecord,
) -> SongScheduleRequest {
    let (chat_id, thread_id, message_id) = match query.message.as_ref() {
        Some(MaybeInaccessibleMessage::Message(message)) => (
            i64::from(message.chat.get_id()),
            message
                .message_thread_id
                .and_then(|thread_id| i32::try_from(thread_id).ok())
                .filter(|thread_id| *thread_id != 0),
            i32::try_from(message.id).unwrap_or(record.trigger_message_id),
        ),
        Some(MaybeInaccessibleMessage::InaccessibleMessage(message)) => (
            i64::from(message.chat.get_id()),
            record.thread_id,
            i32::try_from(message.message_id).unwrap_or(record.trigger_message_id),
        ),
        None => (
            record.chat_id,
            record.thread_id,
            record
                .result_message_id
                .unwrap_or(record.trigger_message_id),
        ),
    };
    let user_full_name = match query.from.last_name.as_deref() {
        Some(last_name) if !last_name.trim().is_empty() => {
            format!("{} {}", query.from.first_name, last_name.trim())
        }
        _ => query.from.first_name.clone(),
    };
    SongScheduleRequest {
        chat_id,
        thread_id,
        message_id,
        user_id: i64::from(query.from.id),
        user_full_name,
        topic: record.topic.clone(),
        message_text: record.request_text.clone(),
        message_meta: ChatMessageMeta::default(),
        reference_file_id: String::new(),
        reference_file_unique_id: String::new(),
        retake: Some(SongRetake {
            song_id: record.id,
            title: record.title.clone(),
            lyrics: record.lyrics.clone(),
            style: record.tags.clone(),
            style_summary: record.style_summary.clone(),
            vocal_language: record.vocal_language.clone(),
            vocals: record.vocals.clone(),
            duration_seconds: u32::try_from(record.duration_seconds).unwrap_or(0),
            brief: record.brief.clone(),
        }),
    }
}

fn song_retake_queued_text(result: &SongScheduleResult) -> String {
    match &result.queue_notice {
        Some(notice) => format!(
            "{SONG_RETAKE_QUEUED_TEXT} (позиция {}, ожидание около {})",
            notice.position, notice.estimated_wait
        ),
        None => SONG_RETAKE_QUEUED_TEXT.to_owned(),
    }
}

async fn try_answer_callback<Effects>(
    effects: &Effects,
    query_id: &str,
    text: &str,
    show_alert: bool,
    context: &'static str,
) where
    Effects: SongRetakeEffects + Sync,
{
    let request = CallbackAnswerRequest {
        callback_query_id: query_id.to_owned(),
        text: text.to_owned(),
        show_alert,
        url: String::new(),
        cache_time: 0,
    };
    let method: TelegramOutboundMethod = build_callback_answer_method(&request).into();
    let method_name = method.method_name();
    if let Err(error) = effects.execute_song_retake_method(method).await {
        tracing::warn!(
            message = %error,
            method = method_name,
            context,
            "song retake callback Telegram side effect failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{io, sync::Mutex};

    use openplotva_dialog::ToolboxError;
    use openplotva_telegram::song_retake_callback_data;
    use serde_json::json;
    use time::OffsetDateTime;

    use super::*;
    use crate::dialog_tools::{GenerateSongFuture, SongQueueNotice};

    fn retake_update(data: String) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 7171,
            "callback_query": {
                "id": "retake-callback-id",
                "from": {"id": 7, "is_bot": false, "first_name": "Ada", "last_name": "Lovelace"},
                "chat_instance": "chat-instance",
                "data": data,
                "message": {
                    "message_id": 66,
                    "date": 1_710_000_000,
                    "chat": {"id": -10042, "type": "supergroup", "title": "Plotva Lab"},
                    "text": "song"
                }
            }
        }))
    }

    fn text_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 7172,
            "message": {
                "message_id": 67,
                "date": 1_710_000_000,
                "chat": {"id": -10042, "type": "supergroup", "title": "Plotva Lab"},
                "from": {"id": 7, "is_bot": false, "first_name": "Ada"},
                "text": "hello"
            }
        }))
    }

    fn stored_song() -> GeneratedSongRecord {
        GeneratedSongRecord {
            id: 9,
            job_id: Some(555),
            retake_of: None,
            chat_id: -10042,
            thread_id: Some(3),
            user_id: 5,
            user_full_name: "Bob".to_owned(),
            trigger_message_id: 60,
            result_message_id: Some(66),
            request_text: "!song night city, female vocals".to_owned(),
            topic: "night city".to_owned(),
            title: "Night City".to_owned(),
            vocal_language: "en".to_owned(),
            vocals: "female".to_owned(),
            tags: "synthwave, 102 BPM, female clean vocals".to_owned(),
            style_summary: "synthwave · 102 BPM".to_owned(),
            lyrics: "[Chorus]\nnight city".to_owned(),
            duration_seconds: 150,
            brief: json!({"bpm": 102}),
            seed: Some(4242),
            audio_seconds: Some(180.5),
            created_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    struct SongsStub {
        record: Option<GeneratedSongRecord>,
        fail: bool,
    }

    impl GeneratedSongLookup for SongsStub {
        type Error = io::Error;

        fn generated_song<'a>(&'a self, id: i64) -> GeneratedSongLookupFuture<'a, Self::Error> {
            Box::pin(async move {
                if self.fail {
                    return Err(io::Error::other("db down"));
                }
                Ok(self.record.clone().filter(|record| record.id == id))
            })
        }
    }

    struct SchedulerStub {
        result: SongScheduleResult,
        calls: Mutex<Vec<SongScheduleRequest>>,
    }

    impl SchedulerStub {
        fn new(result: SongScheduleResult) -> Self {
            Self {
                result,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<SongScheduleRequest> {
            self.calls.lock().expect("calls").clone()
        }
    }

    impl SongScheduler for SchedulerStub {
        fn schedule_song<'a>(&'a self, request: SongScheduleRequest) -> GenerateSongFuture<'a> {
            Box::pin(async move {
                self.calls.lock().expect("calls").push(request);
                Ok::<SongScheduleResult, ToolboxError>(self.result.clone())
            })
        }
    }

    #[derive(Default)]
    struct EffectsStub {
        answers: Mutex<Vec<serde_json::Value>>,
    }

    impl EffectsStub {
        fn answers(&self) -> Vec<serde_json::Value> {
            self.answers.lock().expect("answers").clone()
        }
    }

    impl SongRetakeEffects for EffectsStub {
        type Error = io::Error;

        fn execute_song_retake_method<'a>(
            &'a self,
            method: TelegramOutboundMethod,
        ) -> SongRetakeEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                let TelegramOutboundMethod::AnswerCallbackQuery(method) = method else {
                    return Err(io::Error::other("unexpected method"));
                };
                let payload = serde_json::to_value(method.as_ref()).map_err(io::Error::other)?;
                self.answers.lock().expect("answers").push(payload);
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct UpdateHandlerStub {
        handled: Mutex<usize>,
    }

    impl UpdateHandler for UpdateHandlerStub {
        type Error = io::Error;

        async fn handle_update(&self, _update: TelegramUpdate) -> Result<(), Self::Error> {
            *self.handled.lock().expect("handled") += 1;
            Ok(())
        }
    }

    fn scheduled_result() -> SongScheduleResult {
        SongScheduleResult {
            status: "scheduled".to_owned(),
            job_id: Some(777),
            ..SongScheduleResult::default()
        }
    }

    #[tokio::test]
    async fn non_retake_updates_are_delegated() -> Result<(), Box<dyn std::error::Error>> {
        let songs = SongsStub {
            record: Some(stored_song()),
            fail: false,
        };
        let scheduler = SchedulerStub::new(scheduled_result());
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_song_retake_callback_update_or_else(
            &songs,
            &scheduler,
            &effects,
            text_update()?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, SongRetakeCallbackOutcome::Delegated);
        assert_eq!(*next.handled.lock().expect("handled"), 1);
        assert!(scheduler.calls().is_empty());
        assert!(effects.answers().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn retake_replays_stored_material_for_the_presser()
    -> Result<(), Box<dyn std::error::Error>> {
        let songs = SongsStub {
            record: Some(stored_song()),
            fail: false,
        };
        let scheduler = SchedulerStub::new(SongScheduleResult {
            queue_notice: Some(SongQueueNotice {
                position: 4,
                estimated_wait: "3m".to_owned(),
            }),
            ..scheduled_result()
        });
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_song_retake_callback_update_or_else(
            &songs,
            &scheduler,
            &effects,
            retake_update(song_retake_callback_data(9))?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            outcome,
            SongRetakeCallbackOutcome::Scheduled { job_id: Some(777) }
        );
        let calls = scheduler.calls();
        assert_eq!(calls.len(), 1);
        let request = &calls[0];
        assert_eq!(request.chat_id, -10042);
        assert_eq!(
            request.message_id, 66,
            "the retake replies to the song message"
        );
        assert_eq!(request.user_id, 7, "the presser is the requester");
        assert_eq!(request.user_full_name, "Ada Lovelace");
        assert_eq!(request.topic, "night city");
        assert_eq!(request.message_text, "!song night city, female vocals");
        let retake = request.retake.as_ref().expect("retake material");
        assert_eq!(retake.song_id, 9);
        assert_eq!(retake.style, "synthwave, 102 BPM, female clean vocals");
        assert_eq!(retake.lyrics, "[Chorus]\nnight city");
        assert_eq!(retake.vocal_language, "en");
        assert_eq!(retake.vocals, "female");
        assert_eq!(retake.duration_seconds, 150);
        assert_eq!(retake.brief, json!({"bpm": 102}));
        let answers = effects.answers();
        assert_eq!(answers.len(), 1);
        assert_eq!(
            answers[0]["text"],
            "🎲 Ещё один тейк поставлен в очередь (позиция 4, ожидание около 3m)"
        );
        assert_eq!(*next.handled.lock().expect("handled"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn retake_rejections_and_missing_songs_alert_the_presser()
    -> Result<(), Box<dyn std::error::Error>> {
        let songs = SongsStub {
            record: Some(stored_song()),
            fail: false,
        };
        let scheduler = SchedulerStub::new(SongScheduleResult {
            status: "not_scheduled".to_owned(),
            rejection: Some(SongScheduleRejection::VipOnly),
            ..SongScheduleResult::default()
        });
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();
        let outcome = handle_song_retake_callback_update_or_else(
            &songs,
            &scheduler,
            &effects,
            retake_update(song_retake_callback_data(9))?,
            |update| next.handle_update(update),
        )
        .await?;
        assert_eq!(
            outcome,
            SongRetakeCallbackOutcome::Rejected(SongScheduleRejection::VipOnly)
        );
        assert_eq!(
            effects.answers()[0]["text"],
            SongScheduleRejection::VipOnly.model_facing_message()
        );
        assert_eq!(effects.answers()[0]["show_alert"], true);

        let missing = SongsStub {
            record: None,
            fail: false,
        };
        let effects = EffectsStub::default();
        let outcome = handle_song_retake_callback_update_or_else(
            &missing,
            &scheduler,
            &effects,
            retake_update(song_retake_callback_data(9))?,
            |update| next.handle_update(update),
        )
        .await?;
        assert_eq!(outcome, SongRetakeCallbackOutcome::SongMissing);
        assert_eq!(effects.answers()[0]["text"], SONG_RETAKE_MISSING_TEXT);

        let broken = SongsStub {
            record: None,
            fail: true,
        };
        let effects = EffectsStub::default();
        let outcome = handle_song_retake_callback_update_or_else(
            &broken,
            &scheduler,
            &effects,
            retake_update(song_retake_callback_data(9))?,
            |update| next.handle_update(update),
        )
        .await?;
        assert_eq!(outcome, SongRetakeCallbackOutcome::Failed);
        assert_eq!(effects.answers()[0]["text"], SONG_RETAKE_FAILED_TEXT);

        let effects = EffectsStub::default();
        let outcome = handle_song_retake_callback_update_or_else(
            &songs,
            &scheduler,
            &effects,
            retake_update(r#"{"a":"song_rt","s":"nope"}"#.to_owned())?,
            |update| next.handle_update(update),
        )
        .await?;
        assert_eq!(outcome, SongRetakeCallbackOutcome::BadDataAck);
        assert!(effects.answers()[0].get("text").is_none());
        assert_eq!(
            scheduler.calls().len(),
            1,
            "only the first call reached the scheduler"
        );
        Ok(())
    }
}
