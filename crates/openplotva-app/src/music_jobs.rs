//! App-level music generation task execution.

use std::{
    collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc, time::Duration as StdDuration,
};

use futures_util::StreamExt;
use openplotva_llm::{
    aifarm::{AifarmHttpTransport, AifarmStructuredJsonGenerator, StatusUpdate},
    gemini::GeminiMediaPromptOptimizer,
};
use openplotva_media::acestep::{
    AceStepApiMode, AceStepClient, AceStepConfig, AceStepError, CompletionRequest,
    ReleaseTaskRequest, SongPromptRequest, SongPromptResult, build_song_file_name,
    build_song_release_prompt, detect_song_language, normalize_song_style, song_file_extension,
    song_file_title,
};
use openplotva_server::ACTION_SEND_AUDIO;
use openplotva_storage::{PostgresVirtualMessageStore, TelegramFileRecord};
use openplotva_taskman::{
    DEFAULT_LLM_JOB_MAX_ATTEMPTS, InMemoryTaskQueue, JobType, LLM_JOB_RETRY_EXHAUSTED_STAGE,
    LLM_JOB_RETRY_STAGE, MUSIC_VIP_QUEUE_NAME, MusicGenJobParams, TaskQueueError,
    TaskQueueJobEvent, TaskQueueWorkItem, music_gen_job_params_from_stateless_job,
};
use openplotva_telegram::{
    TelegramOutboundMethod, TelegramOutboundResponse, ensure_telegram_safe_text,
    escape_telegram_html_text, execute_telegram_method,
};
use rand::RngExt;
use time::OffsetDateTime;

use crate::permissions::{ChatPermissionPolicy, ChatPermissionStore};

pub const MUSIC_JOB_POLL_INTERVAL: StdDuration = StdDuration::from_secs(1);
pub const MUSIC_JOB_WORKER_QUEUES: [&str; 1] = [MUSIC_VIP_QUEUE_NAME];
pub const GO_MUSIC_GENERATION_TIMEOUT: StdDuration = StdDuration::from_secs(360);
pub const SUPPORT_ME_URL: &str = "https://t.me/PlotvoBot?start=donate";
pub const VIP_URL: &str = "https://t.me/PlotvoBot?start=vip";

const MUSIC_WORKER_ID: &str = "music-vip-worker";
const DEFAULT_SONG_STYLE: &str = "indie pop, clear vocal, acoustic guitar, emotional, 96 BPM";
const REFERENCE_AUDIO_FILE_NAME: &str = "reference_audio.mp3";
const SUPPORT_PHRASES: [&str; 15] = [
    "автору на вдохновение ✨",
    "Плотва в долгу ❤️",
    "за каждый мем 🐸",
    "на улучшения бота 🔧",
    "спасибо от всей стаи 🐟",
    "донат = волшебство ✨",
    "помоги расти 📈",
    "на новые эксперименты 🧪",
    "автору на счастье 😊",
    "Плотва любит донаты 💙",
    "за креатив без границ 🌈",
    "на развитие магии 🪄",
    "спасибо, ты лучший 🏆",
    "поддержи Плотвин дом 🏠",
    "поддержать ❤️",
];

/// Boxed future returned by music generation providers.
pub type MusicGenerationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GeneratedSongAudio, MusicGenerationError>> + Send + 'a>>;
/// Boxed future returned by song material providers.
pub type SongMaterialFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SongMaterial, MusicGenerationError>> + Send + 'a>>;
/// Boxed future returned by song prompt providers.
pub type SongPromptFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SongPromptResult, MusicGenerationError>> + Send + 'a>>;
/// Boxed future returned by user language stores.
pub type SongLanguageHintFuture<'a> = Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>>;
/// Boxed future returned by music job effects.
pub type MusicJobEffectFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Song material consumed by ACE-Step.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SongMaterial {
    /// Optional generated title.
    pub title: String,
    /// Section-marked lyrics.
    pub lyrics: String,
    /// Normalized ACE-Step style.
    pub style: String,
    /// Raw style shown to users.
    pub raw_style: String,
    /// Vocal language.
    pub vocal_language: String,
}

impl From<SongPromptResult> for SongMaterial {
    fn from(value: SongPromptResult) -> Self {
        Self {
            title: value.title,
            lyrics: value.lyrics,
            style: value.style,
            raw_style: value.raw_style,
            vocal_language: value.vocal_language,
        }
    }
}

/// Reference audio downloaded from Telegram.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MusicReferenceAudio {
    /// File bytes.
    pub data: Vec<u8>,
    /// Multipart filename.
    pub file_name: String,
}

/// Provider-neutral generation request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MusicGenerationRequest {
    /// Song topic.
    pub topic: String,
    /// Song material.
    pub material: SongMaterial,
    /// Optional reference audio.
    pub reference_audio: Option<MusicReferenceAudio>,
}

/// Generated audio.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GeneratedSongAudio {
    /// Audio bytes.
    pub data: Vec<u8>,
    /// Provider filename.
    pub file_name: String,
}

/// One send-song plan.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GeneratedSongSendPlan {
    pub chat_id: i64,
    pub message_id: i32,
    pub user_id: i64,
    pub user_full_name: String,
    pub thread_id: Option<i32>,
    pub topic: String,
    pub material: SongMaterial,
    pub generated: GeneratedSongAudio,
    pub is_vip: bool,
}

/// Errors from generation-side work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MusicGenerationError {
    /// Music topic is empty.
    EmptyTopic,
    /// Material provider failed.
    Material(String),
    /// ACE-Step/provider failed.
    Provider(String),
}

impl MusicGenerationError {
    fn message(&self) -> String {
        match self {
            Self::EmptyTopic => "music topic is empty".to_owned(),
            Self::Material(message) | Self::Provider(message) => message.clone(),
        }
    }
}

impl fmt::Display for MusicGenerationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for MusicGenerationError {}

impl From<AceStepError> for MusicGenerationError {
    fn from(error: AceStepError) -> Self {
        Self::Provider(error.to_string())
    }
}

pub trait SongMaterialProvider {
    /// Build song material.
    fn build_song_material<'a>(
        &'a self,
        params: &'a MusicGenJobParams,
        topic: &'a str,
    ) -> SongMaterialFuture<'a>;
}

pub trait SongPromptGenerator {
    /// Build provider-backed song prompt material.
    fn build_song_prompt<'a>(&'a self, request: SongPromptRequest) -> SongPromptFuture<'a>;
}

impl<T> SongPromptGenerator for Arc<T>
where
    T: SongPromptGenerator + ?Sized + Send + Sync,
{
    fn build_song_prompt<'a>(&'a self, request: SongPromptRequest) -> SongPromptFuture<'a> {
        self.as_ref().build_song_prompt(request)
    }
}

impl<T> SongPromptGenerator for AifarmStructuredJsonGenerator<T>
where
    T: AifarmHttpTransport + Send + Sync,
{
    fn build_song_prompt<'a>(&'a self, request: SongPromptRequest) -> SongPromptFuture<'a> {
        Box::pin(async move {
            let mut ignore_status = |_status: StatusUpdate| {};
            AifarmStructuredJsonGenerator::optimize_song_prompt(self, request, &mut ignore_status)
                .await
                .map_err(|err| {
                    MusicGenerationError::Material(format!("song reprompt failed: {err}"))
                })
        })
    }
}

impl<T> SongPromptGenerator for GeminiMediaPromptOptimizer<T>
where
    T: AifarmHttpTransport + Send + Sync,
{
    fn build_song_prompt<'a>(&'a self, request: SongPromptRequest) -> SongPromptFuture<'a> {
        Box::pin(async move {
            GeminiMediaPromptOptimizer::optimize_song_prompt(self, request)
                .await
                .map_err(|err| MusicGenerationError::Material(err.to_string()))
        })
    }
}

#[derive(Clone, Debug)]
pub struct FallbackSongPromptGenerator<Primary, Fallback> {
    primary: Primary,
    fallback: Fallback,
    primary_label: &'static str,
    fallback_label: &'static str,
}

impl<Primary, Fallback> FallbackSongPromptGenerator<Primary, Fallback> {
    pub const fn new(
        primary: Primary,
        fallback: Fallback,
        primary_label: &'static str,
        fallback_label: &'static str,
    ) -> Self {
        Self {
            primary,
            fallback,
            primary_label,
            fallback_label,
        }
    }

    pub const fn go_aifarm_gemini(primary: Primary, fallback: Fallback) -> Self {
        Self::new(
            primary,
            fallback,
            "aifarm song reprompt",
            "gemini song reprompt",
        )
    }
}

impl<Primary, Fallback> SongPromptGenerator for FallbackSongPromptGenerator<Primary, Fallback>
where
    Primary: SongPromptGenerator + Sync,
    Fallback: SongPromptGenerator + Sync,
{
    fn build_song_prompt<'a>(&'a self, request: SongPromptRequest) -> SongPromptFuture<'a> {
        Box::pin(async move {
            match self.primary.build_song_prompt(request.clone()).await {
                Ok(result) => Ok(result),
                Err(primary_error) => match self.fallback.build_song_prompt(request).await {
                    Ok(result) => Ok(result),
                    Err(fallback_error) => Err(MusicGenerationError::Material(format!(
                        "{} failed: {}; {} failed: {}",
                        self.primary_label,
                        song_prompt_error_leaf(&primary_error),
                        self.fallback_label,
                        song_prompt_error_leaf(&fallback_error)
                    ))),
                },
            }
        })
    }
}

pub trait SongLanguageHintStore {
    /// Load a non-empty user language code.
    fn song_language_hint<'a>(&'a self, user_id: i64) -> SongLanguageHintFuture<'a>;
}

impl SongLanguageHintStore for PostgresVirtualMessageStore {
    fn song_language_hint<'a>(&'a self, user_id: i64) -> SongLanguageHintFuture<'a> {
        Box::pin(async move {
            if user_id == 0 {
                return None;
            }
            match self.get_user_state(user_id).await {
                Ok(Some(user)) => user
                    .language_code
                    .as_deref()
                    .map(str::trim)
                    .filter(|language| !language.is_empty())
                    .map(str::to_owned),
                Ok(None) => None,
                Err(error) => {
                    tracing::debug!(
                        %error,
                        user_id,
                        "failed to load song language hint from user state"
                    );
                    None
                }
            }
        })
    }
}

/// No-op language store for tests and non-DB wiring.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoSongLanguageHintStore;

impl SongLanguageHintStore for NoSongLanguageHintStore {
    fn song_language_hint<'a>(&'a self, _user_id: i64) -> SongLanguageHintFuture<'a> {
        Box::pin(async { None })
    }
}

#[derive(Clone, Debug)]
pub struct AifarmSongMaterialProvider<Generator, Store = NoSongLanguageHintStore> {
    generator: Generator,
    language_store: Store,
}

impl<Generator> AifarmSongMaterialProvider<Generator, NoSongLanguageHintStore> {
    /// Build without a user-language store.
    pub const fn without_language_store(generator: Generator) -> Self {
        Self {
            generator,
            language_store: NoSongLanguageHintStore,
        }
    }
}

impl<Generator, Store> AifarmSongMaterialProvider<Generator, Store> {
    /// Build with a user-language store.
    pub const fn new(generator: Generator, language_store: Store) -> Self {
        Self {
            generator,
            language_store,
        }
    }
}

impl<Generator, Store> SongMaterialProvider for AifarmSongMaterialProvider<Generator, Store>
where
    Generator: SongPromptGenerator + Sync,
    Store: SongLanguageHintStore + Sync,
{
    fn build_song_material<'a>(
        &'a self,
        params: &'a MusicGenJobParams,
        topic: &'a str,
    ) -> SongMaterialFuture<'a> {
        Box::pin(async move {
            let language_hint = self
                .language_store
                .song_language_hint(params.user_id)
                .await
                .unwrap_or_else(|| detect_song_language(topic));
            let reprompt = self
                .generator
                .build_song_prompt(SongPromptRequest {
                    topic: topic.to_owned(),
                    user_id: params.user_id,
                    message_id: params.message_id,
                    language_hint,
                })
                .await?;
            Ok(song_material_from_reprompt(params, reprompt))
        })
    }
}

/// Optimized local materializer used until provider-backed song reprompt is wired.
#[derive(Clone, Copy, Debug, Default)]
pub struct HeuristicSongMaterialProvider;

impl SongMaterialProvider for HeuristicSongMaterialProvider {
    fn build_song_material<'a>(
        &'a self,
        params: &'a MusicGenJobParams,
        topic: &'a str,
    ) -> SongMaterialFuture<'a> {
        Box::pin(async move { Ok(song_material_from_params_or_fallback(params, topic)) })
    }
}

/// Boundary for ACE-Step song audio generation.
pub trait MusicGenerator {
    /// Generate one song audio file.
    fn generate_song<'a>(&'a self, request: MusicGenerationRequest) -> MusicGenerationFuture<'a>;
}

/// Concrete ACE-Step music generator.
#[derive(Clone, Debug)]
pub struct AceStepMusicGenerator {
    client: AceStepClient,
    audio_format: String,
}

impl AceStepMusicGenerator {
    /// Build generator.
    #[must_use]
    pub fn new(client: AceStepClient, audio_format: impl Into<String>) -> Self {
        Self {
            client,
            audio_format: audio_format.into(),
        }
    }
}

impl MusicGenerator for AceStepMusicGenerator {
    fn generate_song<'a>(&'a self, request: MusicGenerationRequest) -> MusicGenerationFuture<'a> {
        Box::pin(async move {
            let prompt = build_song_release_prompt(
                &request.material.style,
                &request.topic,
                &request.material.vocal_language,
            );
            if self.client.mode() == AceStepApiMode::Completion {
                let result = self
                    .client
                    .generate_completion(CompletionRequest {
                        prompt,
                        lyrics: request.material.lyrics,
                        vocal_language: request.material.vocal_language,
                        audio_format: self.audio_format.clone(),
                        thinking: true,
                        ..CompletionRequest::default()
                    })
                    .await?;
                return Ok(GeneratedSongAudio {
                    data: result.audio_data,
                    file_name: result.file_name,
                });
            }

            let reference = request.reference_audio.unwrap_or_default();
            let task_id = self
                .client
                .release_task(ReleaseTaskRequest {
                    prompt,
                    lyrics: request.material.lyrics,
                    vocal_language: request.material.vocal_language,
                    audio_format: self.audio_format.clone(),
                    reference_audio: reference.data,
                    reference_file_name: reference.file_name,
                    ..ReleaseTaskRequest::default()
                })
                .await?;
            let result = self.client.wait_result(&task_id).await?;
            let audio = self.client.download_audio(&result.first_file()).await?;
            Ok(GeneratedSongAudio {
                data: audio.data,
                file_name: audio.file_name,
            })
        })
    }
}

pub trait MusicJobEffects {
    /// Return whether audio may be sent to this chat.
    fn can_send_audio<'a>(&'a self, chat_id: i64) -> MusicJobEffectFuture<'a, bool>;

    /// Download optional reference audio.
    fn reference_audio<'a>(
        &'a self,
        params: &'a MusicGenJobParams,
    ) -> MusicJobEffectFuture<'a, Option<MusicReferenceAudio>>;

    /// Send the generated song as one rich message (audio + styles + lyrics + author).
    fn send_generated_song<'a>(
        &'a self,
        plan: GeneratedSongSendPlan,
    ) -> MusicJobEffectFuture<'a, Result<Option<i32>, String>>;
}

/// Telegram download boundary for reference audio.
pub trait MusicTelegramSender {
    /// Execute a Telegram method.
    fn send_music_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> MusicJobEffectFuture<'a, Result<TelegramOutboundResponse, String>>;

    /// Download Telegram file bytes by `file_id`.
    fn download_music_file<'a>(
        &'a self,
        file_id: &'a str,
    ) -> MusicJobEffectFuture<'a, Result<Vec<u8>, String>>;
}

impl MusicTelegramSender for openplotva_telegram::TelegramClient {
    fn send_music_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> MusicJobEffectFuture<'a, Result<TelegramOutboundResponse, String>> {
        Box::pin(async move {
            execute_telegram_method(self, method)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn download_music_file<'a>(
        &'a self,
        file_id: &'a str,
    ) -> MusicJobEffectFuture<'a, Result<Vec<u8>, String>> {
        Box::pin(async move {
            let file: carapax::types::File = self
                .execute(carapax::types::GetFile::new(file_id))
                .await
                .map_err(|error| error.to_string())?;
            let file_path = file
                .file_path
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .ok_or_else(|| "telegram file_path is empty".to_owned())?;
            let mut stream = self
                .download_file(file_path)
                .await
                .map_err(|error| error.to_string())?;
            let mut data = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|error| error.to_string())?;
                data.extend_from_slice(&chunk);
            }
            Ok(data)
        })
    }
}

/// Store boundary for resolving Telegram `file_unique_id` into latest `file_id`.
pub trait MusicReferenceFileStore {
    /// Concrete store error.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Lookup a Telegram file row.
    fn music_file_by_unique_id<'a>(
        &'a self,
        file_unique_id: &'a str,
    ) -> MusicJobEffectFuture<'a, Result<Option<TelegramFileRecord>, Self::Error>>;
}

impl MusicReferenceFileStore for openplotva_storage::PostgresTelegramFileStore {
    type Error = openplotva_storage::StorageError;

    fn music_file_by_unique_id<'a>(
        &'a self,
        file_unique_id: &'a str,
    ) -> MusicJobEffectFuture<'a, Result<Option<TelegramFileRecord>, Self::Error>> {
        Box::pin(async move { self.get_file(file_unique_id).await })
    }
}

/// Concrete music job effects over permissions, storage, and Telegram.
#[derive(Clone, Debug)]
pub struct TelegramMusicJobEffects<Permissions, Files, Sender> {
    permissions: Arc<Permissions>,
    files: Files,
    telegram: Sender,
    rich: Arc<dyn crate::rich::RichSender>,
}

impl<Permissions, Files, Sender> TelegramMusicJobEffects<Permissions, Files, Sender> {
    /// Build concrete effects.
    #[must_use]
    pub fn new(
        permissions: Arc<Permissions>,
        files: Files,
        telegram: Sender,
        rich: Arc<dyn crate::rich::RichSender>,
    ) -> Self {
        Self {
            permissions,
            files,
            telegram,
            rich,
        }
    }
}

impl<Store, Files, Sender> MusicJobEffects
    for TelegramMusicJobEffects<ChatPermissionPolicy<Store>, Files, Sender>
where
    Store: ChatPermissionStore + Send + Sync,
    Files: MusicReferenceFileStore + Send + Sync,
    Sender: MusicTelegramSender + Clone + Send + Sync + 'static,
{
    fn can_send_audio<'a>(&'a self, chat_id: i64) -> MusicJobEffectFuture<'a, bool> {
        Box::pin(async move {
            self.permissions
                .can_perform_action_at(chat_id, None, ACTION_SEND_AUDIO, OffsetDateTime::now_utc())
                .await
                .allowed
        })
    }

    fn reference_audio<'a>(
        &'a self,
        params: &'a MusicGenJobParams,
    ) -> MusicJobEffectFuture<'a, Option<MusicReferenceAudio>> {
        Box::pin(async move {
            let mut file_id = params.reference_file_id.trim().to_owned();
            if file_id.is_empty()
                && let Some(record) = self
                    .files
                    .music_file_by_unique_id(params.reference_file_unique_id.trim())
                    .await
                    .ok()
                    .flatten()
            {
                file_id = record.latest_file_id.trim().to_owned();
            }
            if file_id.is_empty() {
                return None;
            }
            let data = self.telegram.download_music_file(&file_id).await.ok()?;
            if data.is_empty() {
                return None;
            }
            Some(MusicReferenceAudio {
                data,
                file_name: REFERENCE_AUDIO_FILE_NAME.to_owned(),
            })
        })
    }

    fn send_generated_song<'a>(
        &'a self,
        plan: GeneratedSongSendPlan,
    ) -> MusicJobEffectFuture<'a, Result<Option<i32>, String>> {
        Box::pin(async move {
            let extension = song_file_extension(&plan.generated.file_name);
            let title = song_file_title(&plan.material.title, &plan.topic);
            let pretty_name = build_song_file_name(&plan.user_full_name, &title, &extension);
            let mime = song_audio_mime(&extension);
            let audio_url = self
                .rich
                .upload_bytes(plan.generated.data.clone(), mime, Some(&pretty_name))
                .await
                .map_err(|error| error.to_string())?;
            let footer_html =
                build_song_footer_html(&plan.user_full_name, plan.is_vip, random_support_phrase());
            let html = crate::rich::compose_song_message(&crate::rich::SongMessage {
                title: &title,
                styles: &plan.material.raw_style,
                audio_url: &audio_url,
                lyrics: &plan.material.lyrics,
                footer_html: &footer_html,
            });
            let options = openplotva_telegram::RichSendOptions {
                message_thread_id: plan.thread_id.map(i64::from),
                reply_to_message_id: Some(i64::from(plan.message_id)),
                allow_sending_without_reply: true,
                disable_notification: false,
                reply_markup: None,
            };
            let message_id = self
                .rich
                .send_rich(plan.chat_id, &html, &options)
                .await
                .map_err(|error| error.to_string())?;
            Ok(Some(message_id as i32))
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MusicJobExecutionOutcome {
    /// Job completed.
    Completed,
    SkippedPermission,
    /// Job failed.
    Failed,
}

/// Report from one music job execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MusicJobExecutionReport {
    pub outcome: MusicJobExecutionOutcome,
    pub topic: String,
    pub error: Option<String>,
    pub result_message_id: Option<i32>,
}

/// Result from one queue poll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MusicQueuePollOutcome {
    Idle,
    Completed,
    SkippedPermission,
    RetryRequeued,
    RetryExhausted,
    Failed,
    DecodeFailed,
}

/// Report from one music queue poll.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MusicQueuePollReport {
    pub queue_name: String,
    pub job_id: Option<i64>,
    pub outcome: MusicQueuePollOutcome,
    pub error: Option<String>,
}

/// Aggregate report from music worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MusicWorkerRunReport {
    pub ticks: u64,
    pub dequeued: u64,
    pub completed: u64,
    pub skipped_permission: u64,
    pub retry_requeued: u64,
    pub retry_exhausted: u64,
    pub failed: u64,
    pub decode_failed: u64,
    pub idle: u64,
    pub errors: u64,
}

impl MusicWorkerRunReport {
    fn record_poll(&mut self, report: &MusicQueuePollReport) {
        self.ticks += 1;
        if report.job_id.is_some() {
            self.dequeued += 1;
        }
        match report.outcome {
            MusicQueuePollOutcome::Idle => self.idle += 1,
            MusicQueuePollOutcome::Completed => self.completed += 1,
            MusicQueuePollOutcome::SkippedPermission => self.skipped_permission += 1,
            MusicQueuePollOutcome::RetryRequeued => self.retry_requeued += 1,
            MusicQueuePollOutcome::RetryExhausted => self.retry_exhausted += 1,
            MusicQueuePollOutcome::Failed => self.failed += 1,
            MusicQueuePollOutcome::DecodeFailed => self.decode_failed += 1,
        }
        if report.error.is_some() {
            self.errors += 1;
        }
    }
}

pub async fn execute_music_gen_job<MaterialProvider, Generator, Effects>(
    material_provider: &MaterialProvider,
    generator: &Generator,
    effects: &Effects,
    params: MusicGenJobParams,
) -> MusicJobExecutionReport
where
    MaterialProvider: SongMaterialProvider + Sync,
    Generator: MusicGenerator + Sync,
    Effects: MusicJobEffects + Sync,
{
    if !effects.can_send_audio(params.chat_id).await {
        return MusicJobExecutionReport {
            outcome: MusicJobExecutionOutcome::SkippedPermission,
            topic: String::new(),
            error: None,
            result_message_id: None,
        };
    }
    let topic = music_job_topic(&params);
    if topic.is_empty() {
        return MusicJobExecutionReport {
            outcome: MusicJobExecutionOutcome::Failed,
            topic,
            error: Some("music topic is empty".to_owned()),
            result_message_id: None,
        };
    }
    let material = match material_provider.build_song_material(&params, &topic).await {
        Ok(material) => material,
        Err(error) => {
            return MusicJobExecutionReport {
                outcome: MusicJobExecutionOutcome::Failed,
                topic,
                error: Some(error.message()),
                result_message_id: None,
            };
        }
    };
    let reference_audio = effects.reference_audio(&params).await;
    let generated = match generator
        .generate_song(MusicGenerationRequest {
            topic: topic.clone(),
            material: material.clone(),
            reference_audio,
        })
        .await
    {
        Ok(generated) => generated,
        Err(error) => {
            return MusicJobExecutionReport {
                outcome: MusicJobExecutionOutcome::Failed,
                topic,
                error: Some(error.message()),
                result_message_id: None,
            };
        }
    };
    match effects
        .send_generated_song(GeneratedSongSendPlan {
            chat_id: params.chat_id,
            message_id: params.message_id,
            user_id: params.user_id,
            user_full_name: params.user_full_name,
            thread_id: params.thread_id,
            topic: topic.clone(),
            material: material.clone(),
            generated,
            is_vip: true,
        })
        .await
    {
        Ok(Some(audio_message_id)) => MusicJobExecutionReport {
            outcome: MusicJobExecutionOutcome::Completed,
            topic,
            error: None,
            result_message_id: Some(audio_message_id),
        },
        Ok(None) => MusicJobExecutionReport {
            outcome: MusicJobExecutionOutcome::Completed,
            topic,
            error: None,
            result_message_id: None,
        },
        Err(error) => MusicJobExecutionReport {
            outcome: MusicJobExecutionOutcome::Failed,
            topic,
            error: Some(error),
            result_message_id: None,
        },
    }
}

/// Dequeue and process one music-generation job from a queue.
pub async fn run_music_queue_once<MaterialProvider, Generator, Effects>(
    queue: &InMemoryTaskQueue,
    queue_name: &str,
    material_provider: &MaterialProvider,
    generator: &Generator,
    effects: &Effects,
    now: OffsetDateTime,
) -> MusicQueuePollReport
where
    MaterialProvider: SongMaterialProvider + Sync,
    Generator: MusicGenerator + Sync,
    Effects: MusicJobEffects + Sync,
{
    run_music_queue_once_with_max_attempts(
        queue,
        queue_name,
        material_provider,
        generator,
        effects,
        DEFAULT_LLM_JOB_MAX_ATTEMPTS,
        now,
    )
    .await
}

/// Dequeue and process one music-generation job with a configured retry limit.
pub async fn run_music_queue_once_with_max_attempts<MaterialProvider, Generator, Effects>(
    queue: &InMemoryTaskQueue,
    queue_name: &str,
    material_provider: &MaterialProvider,
    generator: &Generator,
    effects: &Effects,
    max_llm_job_attempts: i32,
    now: OffsetDateTime,
) -> MusicQueuePollReport
where
    MaterialProvider: SongMaterialProvider + Sync,
    Generator: MusicGenerator + Sync,
    Effects: MusicJobEffects + Sync,
{
    let Some(work) = queue.dequeue_matching(queue_name, MUSIC_WORKER_ID, now, |job| {
        job.data.job_type == JobType::MusicGen
    }) else {
        return MusicQueuePollReport {
            queue_name: queue_name.to_owned(),
            job_id: None,
            outcome: MusicQueuePollOutcome::Idle,
            error: None,
        };
    };

    let params = match music_gen_job_params_from_stateless_job(&work.job) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            let _ = queue.fail(work.id, error.clone(), now);
            return MusicQueuePollReport {
                queue_name: queue_name.to_owned(),
                job_id: Some(work.id),
                outcome: MusicQueuePollOutcome::DecodeFailed,
                error: Some(error),
            };
        }
    };

    let _ = queue.set_execution_started(work.id, now);
    let execution = execute_music_gen_job(material_provider, generator, effects, params).await;
    let finished_at = OffsetDateTime::now_utc();
    match execution.outcome {
        MusicJobExecutionOutcome::Completed => {
            let _ = queue.update_job_result_message(work.id, execution.result_message_id);
            finalize_music_completed(
                queue,
                work.id,
                queue_name,
                MusicQueuePollOutcome::Completed,
                finished_at,
            )
        }
        MusicJobExecutionOutcome::SkippedPermission => finalize_music_completed(
            queue,
            work.id,
            queue_name,
            MusicQueuePollOutcome::SkippedPermission,
            finished_at,
        ),
        MusicJobExecutionOutcome::Failed => {
            let error = execution
                .error
                .unwrap_or_else(|| "music generation failed".to_owned());
            if let Some(report) = retry_or_exhaust_music_job(
                queue,
                &work,
                queue_name,
                &error,
                max_llm_job_attempts,
                finished_at,
            ) {
                return report;
            }
            let _ = queue.fail(work.id, error.clone(), finished_at);
            MusicQueuePollReport {
                queue_name: queue_name.to_owned(),
                job_id: Some(work.id),
                outcome: MusicQueuePollOutcome::Failed,
                error: Some(error),
            }
        }
    }
}

fn retry_or_exhaust_music_job(
    queue: &InMemoryTaskQueue,
    work: &TaskQueueWorkItem,
    queue_name: &str,
    error: &str,
    max_llm_job_attempts: i32,
    now: OffsetDateTime,
) -> Option<MusicQueuePollReport> {
    let reason = openplotva_llm::retry::retryable_reason_from_message(error)?;
    let attempt = next_music_llm_job_attempt(&work.events);
    let max_attempts = max_llm_job_attempts.max(1);
    let target_queue = retryable_music_job_target_queue(queue_name);
    let exhausted = attempt >= max_attempts;
    let stage = if exhausted {
        LLM_JOB_RETRY_EXHAUSTED_STAGE
    } else {
        LLM_JOB_RETRY_STAGE
    };
    let mut event = music_retry_job_event(
        stage,
        attempt,
        max_attempts,
        &infer_music_retry_provider(error),
        reason,
        &target_queue,
        error,
    );
    if exhausted {
        event.message = "retryable LLM provider error exhausted job attempts".to_owned();
    }
    queue.append_job_event(work.id, event, now).ok()?;
    if exhausted {
        let _ = queue.fail(work.id, error.to_owned(), now);
        return Some(MusicQueuePollReport {
            queue_name: queue_name.to_owned(),
            job_id: Some(work.id),
            outcome: MusicQueuePollOutcome::RetryExhausted,
            error: Some(error.to_owned()),
        });
    }
    queue.requeue_job_to_queue(work.id, &target_queue).ok()?;
    Some(MusicQueuePollReport {
        queue_name: queue_name.to_owned(),
        job_id: Some(work.id),
        outcome: MusicQueuePollOutcome::RetryRequeued,
        error: None,
    })
}

fn next_music_llm_job_attempt(events: &[TaskQueueJobEvent]) -> i32 {
    events
        .iter()
        .filter(|event| {
            event.stage == LLM_JOB_RETRY_STAGE || event.stage == LLM_JOB_RETRY_EXHAUSTED_STAGE
        })
        .map(|event| event.attempt)
        .max()
        .unwrap_or(0)
        + 1
}

fn retryable_music_job_target_queue(queue_name: &str) -> String {
    if queue_name.trim().is_empty() {
        MUSIC_VIP_QUEUE_NAME.to_owned()
    } else {
        queue_name.to_owned()
    }
}

fn infer_music_retry_provider(error: &str) -> String {
    if let Some((provider, _rest)) = error.trim().split_once(" provider ") {
        let provider = provider.trim();
        if !provider.is_empty()
            && provider
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
        {
            return provider.to_owned();
        }
    }
    "llm".to_owned()
}

fn music_retry_job_event(
    stage: &str,
    attempt: i32,
    max_attempts: i32,
    provider: &str,
    reason: openplotva_llm::retry::FailureReason,
    target_queue: &str,
    error: &str,
) -> TaskQueueJobEvent {
    let mut data = BTreeMap::new();
    data.insert("fallback_reason".to_owned(), reason.to_string());
    data.insert("max_attempts".to_owned(), max_attempts.to_string());
    data.insert("target_queue".to_owned(), target_queue.to_owned());

    TaskQueueJobEvent {
        level: "warn".to_owned(),
        stage: stage.to_owned(),
        attempt,
        provider: provider.to_owned(),
        message: "retryable LLM provider error, requeueing job".to_owned(),
        error: error.to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    }
}

/// Run music-generation worker until stopped.
pub async fn run_music_worker_every_until<MaterialProvider, Generator, Effects, Stop>(
    queue: &InMemoryTaskQueue,
    material_provider: &MaterialProvider,
    generator: &Generator,
    effects: &Effects,
    interval: StdDuration,
    stop: Stop,
) -> MusicWorkerRunReport
where
    MaterialProvider: SongMaterialProvider + Sync,
    Generator: MusicGenerator + Sync,
    Effects: MusicJobEffects + Sync,
    Stop: Future<Output = ()>,
{
    run_music_worker_every_until_with_max_attempts(
        queue,
        material_provider,
        generator,
        effects,
        interval,
        DEFAULT_LLM_JOB_MAX_ATTEMPTS,
        stop,
    )
    .await
}

/// Run music-generation worker until stopped with a configured retry limit.
pub async fn run_music_worker_every_until_with_max_attempts<
    MaterialProvider,
    Generator,
    Effects,
    Stop,
>(
    queue: &InMemoryTaskQueue,
    material_provider: &MaterialProvider,
    generator: &Generator,
    effects: &Effects,
    interval: StdDuration,
    max_llm_job_attempts: i32,
    stop: Stop,
) -> MusicWorkerRunReport
where
    MaterialProvider: SongMaterialProvider + Sync,
    Generator: MusicGenerator + Sync,
    Effects: MusicJobEffects + Sync,
    Stop: Future<Output = ()>,
{
    let mut report = MusicWorkerRunReport::default();
    let mut stop = std::pin::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                for queue_name in MUSIC_JOB_WORKER_QUEUES {
                    let tick = run_music_queue_once_with_max_attempts(
                        queue,
                        queue_name,
                        material_provider,
                        generator,
                        effects,
                        max_llm_job_attempts,
                        OffsetDateTime::now_utc(),
                    ).await;
                    trace_music_queue_tick(&tick);
                    report.record_poll(&tick);
                }
            }
        }
    }
    report
}

/// Build app ACE-Step config.
#[must_use]
pub fn acestep_config_from_app_config(config: &openplotva_config::AppConfig) -> AceStepConfig {
    AceStepConfig {
        base_url: config.music.acestep.base_url.clone(),
        api_key: config.music.acestep.api_key.clone(),
        api_mode: AceStepApiMode::from_go(&config.music.acestep.api_mode),
        request_timeout: seconds_or_zero(config.music.acestep.request_timeout_seconds),
        poll_interval: seconds_or_zero(config.music.acestep.poll_interval_seconds),
        task_timeout: seconds_or_zero(config.music.acestep.task_timeout_seconds),
        audio_format: config.music.acestep.audio_format.clone(),
        // Prefer the model selected in the admin for the `music` workflow (ACE-Step has no
        // discovery service, so this is unconditional; behavior-neutral against the seed).
        model: crate::model_routing::resolved_model_any("music")
            .unwrap_or_else(|| config.music.acestep.model.clone()),
    }
    .with_defaults()
}

#[must_use]
pub fn music_job_topic(params: &MusicGenJobParams) -> String {
    let topic = params.topic.trim();
    if !topic.is_empty() {
        return topic.to_owned();
    }
    params
        .meta
        .get("annotation")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

#[must_use]
pub fn build_song_caption_with_support(
    topic: &str,
    style: &str,
    author: &str,
    is_vip: bool,
    support_text: &str,
) -> String {
    let cleaned_topic = ensure_telegram_safe_text(topic.trim());
    let cleaned_style = ensure_telegram_safe_text(style.trim());
    let cleaned_author = ensure_telegram_safe_text(author.trim());

    let mut author_text = escape_telegram_html_text(&cleaned_author);
    if is_vip && !cleaned_author.is_empty() {
        author_text = format!("<a href=\"{VIP_URL}\">VIP-персоны  👑</a> {author_text}");
    }

    let mut topic_text = if cleaned_topic.is_empty() {
        "Сгенерированная песня".to_owned()
    } else {
        cleaned_topic
    };
    let style_suffix = if cleaned_style.is_empty() {
        String::new()
    } else {
        format!("\n🎵 {}", escape_telegram_html_text(&cleaned_style))
    };

    let build = |escaped_topic: &str| {
        format!(
            "<code>{escaped_topic}</code>{style_suffix} \nза авторством <i>{author_text}</i>\n<a href=\"{SUPPORT_ME_URL}\">{support_text}</a>"
        )
    };
    let mut caption = build(&escape_telegram_html_text(&topic_text));
    if caption.len() > 1024 {
        let overflow = caption.len().saturating_sub(1024);
        let max_topic_len = topic_text.len().saturating_sub(overflow).saturating_sub(3);
        topic_text = if max_topic_len > 0 {
            format!("{}...", truncate_utf8(&topic_text, max_topic_len))
        } else {
            "...".to_owned()
        };
        caption = build(&escape_telegram_html_text(&topic_text));
    }
    caption
}

#[must_use]
pub fn build_song_caption(topic: &str, style: &str, author: &str, is_vip: bool) -> String {
    build_song_caption_with_support(topic, style, author, is_vip, random_support_phrase())
}

/// Author + support footer fragment for the rich song message (title and styles are
/// rendered as separate blocks by `rich::compose_song_message`).
#[must_use]
pub fn build_song_footer_html(author: &str, is_vip: bool, support_text: &str) -> String {
    let cleaned_author = ensure_telegram_safe_text(author.trim());
    let mut author_text = escape_telegram_html_text(&cleaned_author);
    if is_vip && !cleaned_author.is_empty() {
        author_text = format!("<a href=\"{VIP_URL}\">VIP-персоны  👑</a> {author_text}");
    }
    format!(
        "за авторством <i>{author_text}</i> · <a href=\"{SUPPORT_ME_URL}\">{}</a>",
        escape_telegram_html_text(support_text)
    )
}

fn song_audio_mime(extension: &str) -> &'static str {
    if extension.eq_ignore_ascii_case("ogg") {
        "audio/ogg"
    } else if extension.eq_ignore_ascii_case("wav") {
        "audio/wav"
    } else {
        "audio/mpeg"
    }
}

fn song_material_from_reprompt(
    params: &MusicGenJobParams,
    reprompt: SongPromptResult,
) -> SongMaterial {
    let mut material = SongMaterial {
        title: reprompt.title.trim().to_owned(),
        lyrics: params.lyrics.trim().to_owned(),
        style: params.style.trim().to_owned(),
        raw_style: params.style.trim().to_owned(),
        vocal_language: params.vocal_language.trim().to_owned(),
    };
    if material.lyrics.is_empty() {
        material.lyrics = reprompt.lyrics.trim().to_owned();
    }
    if material.style.is_empty() {
        material.style = reprompt.style.trim().to_owned();
        material.raw_style = reprompt.raw_style.trim().to_owned();
    }
    if material.raw_style.is_empty() {
        material.raw_style.clone_from(&material.style);
    }
    if material.vocal_language.is_empty() {
        material.vocal_language = reprompt.vocal_language.trim().to_owned();
    }
    material
}

fn song_material_from_params_or_fallback(params: &MusicGenJobParams, topic: &str) -> SongMaterial {
    let vocal_language =
        first_non_empty([params.vocal_language.as_str(), &detect_song_language(topic)])
            .unwrap_or("en")
            .to_owned();
    let raw_style = params.style.trim();
    let style = if raw_style.is_empty() {
        DEFAULT_SONG_STYLE.to_owned()
    } else {
        let normalized = normalize_song_style(raw_style);
        if normalized.is_empty() {
            raw_style.to_owned()
        } else {
            normalized
        }
    };
    let raw_style = if raw_style.is_empty() {
        style.clone()
    } else {
        raw_style.to_owned()
    };
    let lyrics = if params.lyrics.trim().is_empty() {
        fallback_lyrics(topic, &vocal_language)
    } else {
        params.lyrics.trim().to_owned()
    };
    SongMaterial {
        title: title_from_topic(topic),
        lyrics,
        style,
        raw_style,
        vocal_language,
    }
}

fn fallback_lyrics(topic: &str, language: &str) -> String {
    let topic = topic.trim();
    if language.eq_ignore_ascii_case("ru") {
        return [
            "[Verse 1]",
            &format!("Сегодня тема: {topic}"),
            "И ритм ведет меня вперед",
            "В строках светится дорога",
            "И голос к припеву зовет",
            "[Chorus]",
            &format!("{topic}, звучит над нами"),
            "Музыка держит простой мотив",
            "Мы поднимаем это знамя",
            "Каждый куплет становится жив",
            "[Verse 2]",
            "Ночь собирает наши ноты",
            "Бит осторожно входит в дом",
            "Смысл выбирается из дремы",
            "И остается теплым сном",
            "[Chorus]",
            &format!("{topic}, звучит над нами"),
            "Музыка держит простой мотив",
            "Мы поднимаем это знамя",
            "Каждый куплет становится жив",
        ]
        .join("\n");
    }
    [
        "[Verse 1]",
        &format!("Tonight the theme is {topic}"),
        "The rhythm opens up the door",
        "A small bright spark becomes a chorus",
        "And keeps us reaching out for more",
        "[Chorus]",
        &format!("{topic}, we sing it higher"),
        "The melody carries every line",
        "The beat is warm, the room is brighter",
        "The hook returns right on time",
        "[Verse 2]",
        "The night collects the little details",
        "The bassline moves beneath the floor",
        "The story finds a steady motion",
        "And turns the key to something more",
        "[Chorus]",
        &format!("{topic}, we sing it higher"),
        "The melody carries every line",
        "The beat is warm, the room is brighter",
        "The hook returns right on time",
    ]
    .join("\n")
}

fn title_from_topic(topic: &str) -> String {
    topic
        .split_whitespace()
        .take(5)
        .collect::<Vec<_>>()
        .join(" ")
}

fn finalize_music_completed(
    queue: &InMemoryTaskQueue,
    job_id: i64,
    queue_name: &str,
    outcome: MusicQueuePollOutcome,
    now: OffsetDateTime,
) -> MusicQueuePollReport {
    let error = queue
        .complete(job_id, now)
        .err()
        .map(task_queue_error_message);
    MusicQueuePollReport {
        queue_name: queue_name.to_owned(),
        job_id: Some(job_id),
        outcome,
        error,
    }
}

fn trace_music_queue_tick(tick: &MusicQueuePollReport) {
    if tick.outcome == MusicQueuePollOutcome::Idle && tick.error.is_none() {
        return;
    }
    tracing::debug!(
        queue_name = tick.queue_name,
        job_id = tick.job_id,
        outcome = ?tick.outcome,
        error = tick.error.as_deref(),
        "processed music generation taskman worker tick"
    );
}

fn task_queue_error_message(error: TaskQueueError) -> String {
    error.to_string()
}

fn random_support_phrase() -> &'static str {
    let idx = rand::rng().random_range(0..SUPPORT_PHRASES.len());
    SUPPORT_PHRASES[idx]
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = 0;
    for (idx, ch) in value.char_indices() {
        let next = idx + ch.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }
    value[..end].to_owned()
}

fn seconds_or_zero(value: i32) -> StdDuration {
    u64::try_from(value)
        .ok()
        .filter(|seconds| *seconds > 0)
        .map_or(StdDuration::ZERO, StdDuration::from_secs)
}

fn first_non_empty<const N: usize>(values: [&str; N]) -> Option<&str> {
    values
        .into_iter()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

fn song_prompt_error_leaf(error: &MusicGenerationError) -> String {
    let message = error.to_string();
    message
        .strip_prefix("song reprompt failed: ")
        .unwrap_or(message.as_str())
        .trim()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        error::Error,
        sync::{Arc, Mutex, MutexGuard},
    };

    use openplotva_taskman::{
        HIGHEST_PRIORITY, JobStatus, LLM_JOB_RETRY_EXHAUSTED_STAGE, LLM_JOB_RETRY_STAGE,
        MUSIC_VIP_QUEUE_NAME, MusicGenJobParams, TaskQueueJobEvent, new_music_gen_job_at,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::{
        AifarmSongMaterialProvider, FallbackSongPromptGenerator, GeneratedSongAudio,
        GeneratedSongSendPlan, HeuristicSongMaterialProvider, MusicGenerationError,
        MusicGenerationFuture, MusicGenerationRequest, MusicGenerator, MusicJobEffectFuture,
        MusicJobEffects, MusicJobExecutionOutcome, MusicQueuePollOutcome, MusicReferenceAudio,
        NoSongLanguageHintStore, SongLanguageHintFuture, SongLanguageHintStore,
        SongMaterialProvider, SongPromptFuture, SongPromptGenerator,
        build_song_caption_with_support, build_song_release_prompt, execute_music_gen_job,
        music_job_topic, run_music_queue_once, run_music_queue_once_with_max_attempts,
    };

    #[test]
    fn song_topic_prefers_explicit_then_annotation() {
        let params = MusicGenJobParams {
            topic: "  explicit  ".to_owned(),
            meta: json!({"annotation":"fallback"}),
            ..MusicGenJobParams::default()
        };
        assert_eq!(music_job_topic(&params), "explicit");

        let params = MusicGenJobParams {
            meta: json!({"annotation":" fallback "}),
            ..MusicGenJobParams::default()
        };
        assert_eq!(music_job_topic(&params), "fallback");
    }

    #[test]
    fn song_caption_matches_go_shape_with_injected_support() {
        let caption = build_song_caption_with_support(
            "night <city>",
            "indie & pop",
            "Alice <Bob>",
            true,
            "support",
        );

        assert!(caption.contains("<code>night &lt;city&gt;</code>"));
        assert!(caption.contains("🎵 indie &amp; pop"));
        assert!(caption.contains(
            "<a href=\"https://t.me/PlotvoBot?start=vip\">VIP-персоны  👑</a> Alice &lt;Bob&gt;"
        ));
        assert!(caption.contains("<a href=\"https://t.me/PlotvoBot?start=donate\">support</a>"));
    }

    #[tokio::test]
    async fn aifarm_song_material_provider_uses_user_language_and_param_overrides() {
        let provider = AifarmSongMaterialProvider::new(
            PromptGeneratorStub::new(openplotva_media::acestep::SongPromptResult {
                title: "Generated Title".to_owned(),
                lyrics: " generated lyrics ".to_owned(),
                style: "synthwave, synth bass, neon mood, 102 BPM".to_owned(),
                raw_style: "Synthwave, Synth Bass, Neon Mood, 102 bpm".to_owned(),
                vocal_language: "ru".to_owned(),
                ..openplotva_media::acestep::SongPromptResult::default()
            }),
            LanguageStoreStub::new(Some("pl-PL")),
        );

        let material = provider
            .build_song_material(
                &MusicGenJobParams {
                    user_id: 7,
                    message_id: 42,
                    lyrics: " manual lyrics ".to_owned(),
                    style: " manual style ".to_owned(),
                    vocal_language: " en ".to_owned(),
                    ..MusicGenJobParams::default()
                },
                "ночной город",
            )
            .await
            .expect("song material");

        assert_eq!(material.title, "Generated Title");
        assert_eq!(material.lyrics, "manual lyrics");
        assert_eq!(material.style, "manual style");
        assert_eq!(material.raw_style, "manual style");
        assert_eq!(material.vocal_language, "en");
        let requests = provider.generator.requests();
        assert_eq!(requests[0].topic, "ночной город");
        assert_eq!(requests[0].user_id, 7);
        assert_eq!(requests[0].message_id, 42);
        assert_eq!(requests[0].language_hint, "pl-PL");
    }

    #[tokio::test]
    async fn aifarm_song_material_provider_falls_back_to_detected_language() {
        let provider = AifarmSongMaterialProvider::new(
            PromptGeneratorStub::new(openplotva_media::acestep::SongPromptResult {
                title: "Title".to_owned(),
                lyrics: "generated lyrics".to_owned(),
                style: "synthwave, synth bass, neon mood, 102 BPM".to_owned(),
                raw_style: "Synthwave, Synth Bass, Neon Mood, 102 bpm".to_owned(),
                vocal_language: "ru".to_owned(),
                ..openplotva_media::acestep::SongPromptResult::default()
            }),
            NoSongLanguageHintStore,
        );

        let material = provider
            .build_song_material(&MusicGenJobParams::default(), "ночной город")
            .await
            .expect("song material");

        assert_eq!(material.lyrics, "generated lyrics");
        assert_eq!(material.style, "synthwave, synth bass, neon mood, 102 BPM");
        assert_eq!(
            material.raw_style,
            "Synthwave, Synth Bass, Neon Mood, 102 bpm"
        );
        assert_eq!(material.vocal_language, "ru");
        assert_eq!(provider.generator.requests()[0].language_hint, "ru");
    }

    #[tokio::test]
    async fn song_prompt_generator_falls_back_to_gemini_after_aifarm_error() {
        let primary = PromptGeneratorStub::err(MusicGenerationError::Material(
            "song reprompt failed: discovery unavailable".to_owned(),
        ));
        let fallback = PromptGeneratorStub::new(openplotva_media::acestep::SongPromptResult {
            title: "Gemini Title".to_owned(),
            lyrics: "gemini lyrics".to_owned(),
            style: "synthwave, neon, driving bass, 102 BPM".to_owned(),
            raw_style: "Synthwave, Neon, Driving Bass, 102 bpm".to_owned(),
            vocal_language: "en".to_owned(),
            ..openplotva_media::acestep::SongPromptResult::default()
        });
        let generator =
            FallbackSongPromptGenerator::go_aifarm_gemini(primary.clone(), fallback.clone());

        let result = generator
            .build_song_prompt(openplotva_media::acestep::SongPromptRequest {
                topic: "night city".to_owned(),
                user_id: 7,
                message_id: 42,
                language_hint: "en".to_owned(),
            })
            .await
            .expect("gemini fallback result");

        assert_eq!(result.title, "Gemini Title");
        assert_eq!(primary.requests().len(), 1);
        assert_eq!(fallback.requests().len(), 1);
        assert_eq!(fallback.requests()[0].topic, "night city");
    }

    #[tokio::test]
    async fn song_prompt_generator_reports_go_shaped_dual_failure() {
        let generator = FallbackSongPromptGenerator::go_aifarm_gemini(
            PromptGeneratorStub::err(MusicGenerationError::Material(
                "song reprompt failed: discovery unavailable".to_owned(),
            )),
            PromptGeneratorStub::err(MusicGenerationError::Material(
                "generate optimizer: status 503".to_owned(),
            )),
        );

        let error = generator
            .build_song_prompt(openplotva_media::acestep::SongPromptRequest {
                topic: "night city".to_owned(),
                language_hint: "en".to_owned(),
                ..openplotva_media::acestep::SongPromptRequest::default()
            })
            .await
            .expect_err("dual failure");

        assert_eq!(
            error.to_string(),
            "aifarm song reprompt failed: discovery unavailable; gemini song reprompt failed: generate optimizer: status 503"
        );
    }

    #[tokio::test]
    async fn music_executor_generates_audio_and_lyrics() -> Result<(), Box<dyn Error>> {
        let material_provider = HeuristicSongMaterialProvider;
        let generator = GeneratorStub::new(Ok(GeneratedSongAudio {
            data: b"MP3".to_vec(),
            file_name: "song.mp3".to_owned(),
        }));
        let effects = EffectsStub::allowed();
        let report = execute_music_gen_job(
            &material_provider,
            &generator,
            &effects,
            MusicGenJobParams {
                chat_id: 42,
                message_id: 9,
                user_id: 7,
                user_full_name: "Alice".to_owned(),
                topic: "ночной город".to_owned(),
                thread_id: Some(77),
                ..MusicGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, MusicJobExecutionOutcome::Completed);
        let sent = effects.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].chat_id, 42);
        assert_eq!(sent[0].thread_id, Some(77));
        assert_eq!(generator.requests()[0].topic, "ночной город");
        assert_eq!(
            build_song_release_prompt(
                &generator.requests()[0].material.style,
                "ночной город",
                &generator.requests()[0].material.vocal_language,
            ),
            "indie pop, clear vocal, acoustic guitar, emotional, 96 BPM, песня о ночной город"
        );
        Ok(())
    }

    #[tokio::test]
    async fn music_queue_once_completes_permission_skip_like_go() -> Result<(), Box<dyn Error>> {
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        queue.assign(
            MUSIC_VIP_QUEUE_NAME,
            new_music_gen_job_at(
                MusicGenJobParams {
                    chat_id: 42,
                    message_id: 9,
                    user_id: 7,
                    topic: "city".to_owned(),
                    ..MusicGenJobParams::default()
                },
                OffsetDateTime::now_utc(),
            )
            .with_name("music")
            .with_priority(HIGHEST_PRIORITY),
        );
        let effects = EffectsStub::denied();
        let report = run_music_queue_once(
            &queue,
            MUSIC_VIP_QUEUE_NAME,
            &HeuristicSongMaterialProvider,
            &GeneratorStub::new(Ok(GeneratedSongAudio::default())),
            &effects,
            OffsetDateTime::now_utc(),
        )
        .await;

        assert_eq!(report.outcome, MusicQueuePollOutcome::SkippedPermission);
        assert_eq!(
            queue.records()[0].status,
            openplotva_taskman::JobStatus::Completed
        );
        Ok(())
    }

    #[tokio::test]
    async fn music_queue_once_records_result_message_id_when_completed()
    -> Result<(), Box<dyn Error>> {
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("time");
        let job_id = queue.assign(
            MUSIC_VIP_QUEUE_NAME,
            new_music_gen_job_at(
                MusicGenJobParams {
                    chat_id: 42,
                    message_id: 9,
                    user_id: 7,
                    user_full_name: "Alice".to_owned(),
                    topic: "city".to_owned(),
                    ..MusicGenJobParams::default()
                },
                now,
            )
            .with_name("music")
            .with_priority(HIGHEST_PRIORITY),
        );
        let effects = EffectsStub::allowed();

        let report = run_music_queue_once(
            &queue,
            MUSIC_VIP_QUEUE_NAME,
            &HeuristicSongMaterialProvider,
            &GeneratorStub::new(Ok(GeneratedSongAudio {
                data: b"MP3".to_vec(),
                file_name: "city.mp3".to_owned(),
            })),
            &effects,
            now,
        )
        .await;

        assert_eq!(report.outcome, MusicQueuePollOutcome::Completed);
        let record = queue.record(job_id).expect("music job");
        assert_eq!(record.status, JobStatus::Completed);
        assert_eq!(record.result_message_id, Some(100));
        assert_eq!(record.started_at, Some(now));
        assert_eq!(record.execution_started_at, Some(now));
        assert!(
            record.completed_at > Some(now),
            "completed_at should reflect the end of generation, not the dequeue time"
        );
        Ok(())
    }

    #[tokio::test]
    async fn music_queue_once_requeues_retryable_provider_error_like_go()
    -> Result<(), Box<dyn Error>> {
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("time");
        let job_id = queue.assign(
            MUSIC_VIP_QUEUE_NAME,
            new_music_gen_job_at(
                MusicGenJobParams {
                    chat_id: 42,
                    message_id: 9,
                    user_id: 7,
                    user_full_name: "Alice".to_owned(),
                    topic: "city".to_owned(),
                    ..MusicGenJobParams::default()
                },
                now,
            )
            .with_name("music")
            .with_priority(HIGHEST_PRIORITY),
        );
        let effects = EffectsStub::allowed();
        let report = run_music_queue_once(
            &queue,
            MUSIC_VIP_QUEUE_NAME,
            &HeuristicSongMaterialProvider,
            &GeneratorStub::new(Err(MusicGenerationError::Provider(
                "aifarm provider provider_unavailable: status 503".to_owned(),
            ))),
            &effects,
            now,
        )
        .await;

        assert_eq!(report.outcome, MusicQueuePollOutcome::RetryRequeued);
        assert_eq!(report.error, None);
        let record = queue.record(job_id).expect("job");
        assert_eq!(record.status, openplotva_taskman::JobStatus::Pending);
        assert_eq!(record.queue_name, MUSIC_VIP_QUEUE_NAME);
        assert_eq!(record.error, None);
        assert_eq!(record.worker_id, None);
        assert_eq!(record.started_at, None);
        assert_eq!(record.completed_at, None);
        assert_eq!(record.events.len(), 1);
        let event = &record.events[0];
        assert_eq!(event.stage, openplotva_taskman::LLM_JOB_RETRY_STAGE);
        assert_eq!(event.attempt, 1);
        assert_eq!(event.provider, "aifarm");
        assert_eq!(
            event.message,
            "retryable LLM provider error, requeueing job"
        );
        assert_eq!(
            event.error,
            "aifarm provider provider_unavailable: status 503"
        );
        assert_eq!(event.data["fallback_reason"], "provider_unavailable");
        assert_eq!(
            event.data["max_attempts"],
            openplotva_taskman::DEFAULT_LLM_JOB_MAX_ATTEMPTS.to_string()
        );
        assert_eq!(event.data["target_queue"], MUSIC_VIP_QUEUE_NAME);
        Ok(())
    }

    #[tokio::test]
    async fn music_queue_once_uses_configured_retry_attempt_limit_like_go()
    -> Result<(), Box<dyn Error>> {
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("time");
        let job_id = queue.assign(
            MUSIC_VIP_QUEUE_NAME,
            new_music_gen_job_at(
                MusicGenJobParams {
                    chat_id: 42,
                    message_id: 9,
                    user_id: 7,
                    user_full_name: "Alice".to_owned(),
                    topic: "city".to_owned(),
                    ..MusicGenJobParams::default()
                },
                now,
            )
            .with_name("music")
            .with_priority(HIGHEST_PRIORITY),
        );
        queue
            .append_job_event(
                job_id,
                TaskQueueJobEvent {
                    stage: LLM_JOB_RETRY_STAGE.to_owned(),
                    attempt: 1,
                    ..TaskQueueJobEvent::default()
                },
                now,
            )
            .expect("seed music retry event");

        let effects = EffectsStub::allowed();
        let report = run_music_queue_once_with_max_attempts(
            &queue,
            MUSIC_VIP_QUEUE_NAME,
            &HeuristicSongMaterialProvider,
            &GeneratorStub::new(Err(MusicGenerationError::Provider(
                "aifarm provider provider_unavailable: status 503".to_owned(),
            ))),
            &effects,
            2,
            now,
        )
        .await;

        assert_eq!(report.outcome, MusicQueuePollOutcome::RetryExhausted);
        let record = queue.record(job_id).expect("music job");
        assert_eq!(record.status, JobStatus::Failed);
        let event = record.events.last().expect("exhaustion event");
        assert_eq!(event.stage, LLM_JOB_RETRY_EXHAUSTED_STAGE);
        assert_eq!(event.attempt, 2);
        assert_eq!(event.data["max_attempts"], "2");
        assert_eq!(event.data["target_queue"], MUSIC_VIP_QUEUE_NAME);
        Ok(())
    }

    #[derive(Clone, Debug)]
    struct PromptGeneratorStub {
        result:
            Arc<Mutex<Result<openplotva_media::acestep::SongPromptResult, MusicGenerationError>>>,
        requests: Arc<Mutex<Vec<openplotva_media::acestep::SongPromptRequest>>>,
    }

    impl PromptGeneratorStub {
        fn new(result: openplotva_media::acestep::SongPromptResult) -> Self {
            Self {
                result: Arc::new(Mutex::new(Ok(result))),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn err(error: MusicGenerationError) -> Self {
            Self {
                result: Arc::new(Mutex::new(Err(error))),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<openplotva_media::acestep::SongPromptRequest> {
            lock(&self.requests).clone()
        }
    }

    impl SongPromptGenerator for PromptGeneratorStub {
        fn build_song_prompt<'a>(
            &'a self,
            request: openplotva_media::acestep::SongPromptRequest,
        ) -> SongPromptFuture<'a> {
            Box::pin(async move {
                lock(&self.requests).push(request);
                lock(&self.result).clone()
            })
        }
    }

    #[derive(Clone, Debug)]
    struct LanguageStoreStub {
        hint: Option<&'static str>,
    }

    impl LanguageStoreStub {
        const fn new(hint: Option<&'static str>) -> Self {
            Self { hint }
        }
    }

    impl SongLanguageHintStore for LanguageStoreStub {
        fn song_language_hint<'a>(&'a self, _user_id: i64) -> SongLanguageHintFuture<'a> {
            Box::pin(async move { self.hint.map(str::to_owned) })
        }
    }

    #[derive(Clone, Debug)]
    struct GeneratorStub {
        result: Arc<Mutex<Result<GeneratedSongAudio, MusicGenerationError>>>,
        requests: Arc<Mutex<Vec<MusicGenerationRequest>>>,
    }

    impl GeneratorStub {
        fn new(result: Result<GeneratedSongAudio, MusicGenerationError>) -> Self {
            Self {
                result: Arc::new(Mutex::new(result)),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<MusicGenerationRequest> {
            lock(&self.requests).clone()
        }
    }

    impl MusicGenerator for GeneratorStub {
        fn generate_song<'a>(
            &'a self,
            request: MusicGenerationRequest,
        ) -> MusicGenerationFuture<'a> {
            Box::pin(async move {
                lock(&self.requests).push(request);
                lock(&self.result).clone()
            })
        }
    }

    #[derive(Clone, Debug)]
    struct EffectsStub {
        can_send: bool,
        sent: Arc<Mutex<Vec<GeneratedSongSendPlan>>>,
        refs: Arc<Mutex<VecDeque<Option<MusicReferenceAudio>>>>,
    }

    impl EffectsStub {
        fn allowed() -> Self {
            Self {
                can_send: true,
                sent: Arc::new(Mutex::new(Vec::new())),
                refs: Arc::new(Mutex::new(VecDeque::new())),
            }
        }

        fn denied() -> Self {
            Self {
                can_send: false,
                ..Self::allowed()
            }
        }

        fn sent(&self) -> Vec<GeneratedSongSendPlan> {
            lock(&self.sent).clone()
        }
    }

    impl MusicJobEffects for EffectsStub {
        fn can_send_audio<'a>(&'a self, _chat_id: i64) -> MusicJobEffectFuture<'a, bool> {
            Box::pin(async move { self.can_send })
        }

        fn reference_audio<'a>(
            &'a self,
            _params: &'a MusicGenJobParams,
        ) -> MusicJobEffectFuture<'a, Option<MusicReferenceAudio>> {
            Box::pin(async move { lock(&self.refs).pop_front().flatten() })
        }

        fn send_generated_song<'a>(
            &'a self,
            plan: GeneratedSongSendPlan,
        ) -> MusicJobEffectFuture<'a, Result<Option<i32>, String>> {
            Box::pin(async move {
                lock(&self.sent).push(plan);
                Ok(Some(100))
            })
        }
    }

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
