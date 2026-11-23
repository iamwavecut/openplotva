//! Video jobs share the image queues: the image generation worker hands every dequeued
//! `VideoGen` ticket to a [`VideoJobRunner`]. The optimizer writes a FastFrames prompt,
//! draw-api renders the clip, and the bot replies with it as a Telegram animation captioned
//! like a drawing.

use std::{future::Future, pin::Pin, sync::Arc};

use openplotva_llm::aifarm::AifarmHttpTransport;
use openplotva_llm::retry::{FailureReason, retryable_reason_from_message};
use openplotva_media::{NsfwResult, video::VideoOptimize};
use openplotva_server::ACTION_SEND_VIDEO;
use openplotva_taskman::{
    InMemoryTaskQueue, TaskQueueWorkItem, VideoGenJobParams, video_gen_job_params_from_payload,
};
use openplotva_telegram::{
    ChatRef, ReplyParametersPlan, TELEGRAM_PARSE_MODE_HTML, TelegramClient, TelegramOutboundMethod,
    UploadMessageRequest, build_animation_message_method, build_text_message_methods,
};
use time::OffsetDateTime;

use crate::image_jobs::{
    AifarmDrawApiImageGenerator, DrawApiVideo, ImageGenerationError, ImageJobTelegramSender,
    build_image_generation_caption,
};
use crate::permissions::{ChatPermissionPolicy, ChatPermissionStore};
use crate::telegram_activity::{TelegramActivityAction, TelegramActivityPulse};

const VIDEO_FILE_NAME: &str = "video.mp4";

pub type VideoJobFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait VideoPromptOptimizer: Send + Sync {
    fn optimize_video_prompt<'a>(
        &'a self,
        request: &'a str,
        brief: &'a str,
    ) -> VideoJobFuture<'a, Result<VideoOptimize, String>>;
}

impl VideoPromptOptimizer for crate::media::RoutedMediaPromptOptimizer {
    fn optimize_video_prompt<'a>(
        &'a self,
        request: &'a str,
        brief: &'a str,
    ) -> VideoJobFuture<'a, Result<VideoOptimize, String>> {
        Box::pin(async move {
            crate::media::RoutedMediaPromptOptimizer::optimize_video_prompt(self, request, brief)
                .await
                .map_err(|error| format!("video prompt optimizer: {error}"))
        })
    }
}

pub trait VideoGenerator: Send + Sync {
    /// Render one clip; `job_id` makes the farm submission idempotent.
    fn generate_video<'a>(
        &'a self,
        prompt: &'a str,
        job_id: i64,
    ) -> VideoJobFuture<'a, Result<DrawApiVideo, String>>;
}

impl<T> VideoGenerator for AifarmDrawApiImageGenerator<T>
where
    T: AifarmHttpTransport + Send + Sync,
{
    fn generate_video<'a>(
        &'a self,
        prompt: &'a str,
        job_id: i64,
    ) -> VideoJobFuture<'a, Result<DrawApiVideo, String>> {
        Box::pin(async move {
            let first = self
                .generate_video_with_job_id(prompt, &format!("video-{job_id}"))
                .await;
            let error = match first {
                Err(error) if video_render_retryable(&error) => error,
                other => return other.map_err(|error| error.message()),
            };
            tracing::warn!(
                job_id,
                error = %error.message(),
                "clip render failed on the farm; submitting it once more"
            );
            self.generate_video_with_job_id(prompt, &format!("video-{job_id}-retry"))
                .await
                .map_err(|error| error.message())
        })
    }
}

/// A second farm job can render the clip when the first one met a draw-api server error
/// (a worker that died is loaded again) or an unreachable farm; a bad request, a safety
/// refusal or a watchdog timeout ends the clip.
fn video_render_retryable(error: &ImageGenerationError) -> bool {
    let ImageGenerationError::Provider(message) = error else {
        return false;
    };
    let server_error = message
        .split("draw-api status ")
        .skip(1)
        .any(|rest| rest.starts_with('5'));
    server_error
        || retryable_reason_from_message(message) == Some(FailureReason::ProviderUnavailable)
}

/// One clip ready for the chat.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VideoDelivery {
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    pub reply_to_message_id: i32,
    pub caption: String,
    pub video: DrawApiVideo,
    pub spoiler: bool,
}

pub trait VideoJobEffects: Send + Sync {
    fn can_send_video<'a>(&'a self, chat_id: i64) -> VideoJobFuture<'a, bool>;

    /// Best-effort: mark the trigger message with the "generating" reaction.
    fn signal_progress<'a>(&'a self, chat_id: i64, message_id: i32) -> VideoJobFuture<'a, ()>;

    /// Best-effort: remove the lifecycle reaction after the clip or the refusal is sent.
    fn clear_signal<'a>(&'a self, chat_id: i64, message_id: i32) -> VideoJobFuture<'a, ()>;

    /// Tell the requester that the request was blocked by the safety verdict.
    /// Returns the refusal's message id.
    fn send_blocked<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        thread_id: Option<i32>,
    ) -> VideoJobFuture<'a, Option<i32>>;

    /// Returns the sent animation's message id.
    fn send_video<'a>(
        &'a self,
        delivery: VideoDelivery,
    ) -> VideoJobFuture<'a, Result<Option<i32>, String>>;
}

/// Telegram effects: permission policy, reaction lifecycle and `sendAnimation`.
pub struct TelegramVideoJobEffects<Store> {
    permissions: Arc<ChatPermissionPolicy<Store>>,
    telegram: TelegramClient,
    reactions: Option<crate::reactions::GenerationReactions>,
}

impl<Store> TelegramVideoJobEffects<Store> {
    #[must_use]
    pub fn new(permissions: Arc<ChatPermissionPolicy<Store>>, telegram: TelegramClient) -> Self {
        Self {
            permissions,
            telegram,
            reactions: None,
        }
    }

    #[must_use]
    pub fn with_reaction_ux(mut self, reactions: crate::reactions::GenerationReactions) -> Self {
        self.reactions = Some(reactions);
        self
    }

    async fn react(&self, chat_id: i64, message_id: i32, progress: bool) {
        let Some(reactions) = self.reactions.as_deref() else {
            return;
        };
        let target = crate::reactions::GenerationReactionTarget {
            chat_id,
            message_id,
        };
        if progress {
            reactions.set_progress(target).await;
        } else {
            reactions.clear(target).await;
        }
    }
}

impl<Store> VideoJobEffects for TelegramVideoJobEffects<Store>
where
    Store: ChatPermissionStore + Send + Sync,
{
    fn can_send_video<'a>(&'a self, chat_id: i64) -> VideoJobFuture<'a, bool> {
        Box::pin(async move {
            self.permissions
                .can_perform_action_at(chat_id, None, ACTION_SEND_VIDEO, OffsetDateTime::now_utc())
                .await
                .allowed
        })
    }

    fn signal_progress<'a>(&'a self, chat_id: i64, message_id: i32) -> VideoJobFuture<'a, ()> {
        Box::pin(self.react(chat_id, message_id, true))
    }

    fn clear_signal<'a>(&'a self, chat_id: i64, message_id: i32) -> VideoJobFuture<'a, ()> {
        Box::pin(self.react(chat_id, message_id, false))
    }

    fn send_blocked<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        thread_id: Option<i32>,
    ) -> VideoJobFuture<'a, Option<i32>> {
        Box::pin(async move {
            let (request, reply_to) =
                crate::image_jobs::nsfw_blocked_message_request(chat_id, message_id, thread_id);
            let methods = build_text_message_methods(&request, Some(&reply_to)).ok()?;
            let mut first_message_id = None;
            for method in methods {
                if let Ok(response) = self
                    .telegram
                    .send_image_job_method(TelegramOutboundMethod::from(method))
                    .await
                {
                    first_message_id = first_message_id.or_else(|| {
                        crate::image_jobs::image_job_response_message_ids(&response)
                            .first()
                            .copied()
                    });
                }
            }
            first_message_id
        })
    }

    fn send_video<'a>(
        &'a self,
        delivery: VideoDelivery,
    ) -> VideoJobFuture<'a, Result<Option<i32>, String>> {
        Box::pin(async move {
            let method = animation_method(&delivery)?;
            crate::image_jobs::within_media_job_telegram_timeout(self.telegram.execute(method))
                .await
                .map_err(|error| format!("send video: {error}"))?
                .map(|message| i32::try_from(message.id).ok())
                .map_err(|error| format!("send video: {error}"))
        })
    }
}

fn animation_method(delivery: &VideoDelivery) -> Result<carapax::types::SendAnimation, String> {
    let thread_id = i64::from(delivery.thread_id.unwrap_or_default());
    let request = UploadMessageRequest {
        chat: ChatRef {
            direct_messages_topic_id: None,
            id: delivery.chat_id,
            is_forum: thread_id != 0,
        },
        message_thread_id: thread_id,
        disable_notification: false,
        file_name: VIDEO_FILE_NAME.to_owned(),
        bytes: delivery.video.bytes.clone(),
        caption: delivery.caption.clone(),
        render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
        reply_parameters: Some(ReplyParametersPlan {
            message_id: i64::from(delivery.reply_to_message_id),
            chat_id: delivery.chat_id,
            allow_sending_without_reply: true,
        }),
    };
    let mut method = build_animation_message_method(&request)
        .map_err(|error| format!("build sendAnimation: {error}"))?
        .with_has_spoiler(delivery.spoiler);
    let video = &delivery.video;
    if video.width > 0 && video.height > 0 {
        method = method.with_width(video.width).with_height(video.height);
    }
    if video.frames > 0 && video.fps > 0 {
        method = method.with_duration((video.frames + video.fps / 2) / video.fps);
    }
    Ok(method)
}

/// What one video job ended with. The message ids become the ticket's result
/// message, which the delivery obligation watcher needs to stay silent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoJobOutcome {
    Delivered {
        message_id: Option<i32>,
    },
    /// The optimizer classified the request as forbidden; the refusal was sent.
    Blocked {
        message_id: Option<i32>,
    },
    SkippedPermission,
}

impl VideoJobOutcome {
    #[must_use]
    pub fn result_message_id(self) -> Option<i32> {
        match self {
            Self::Delivered { message_id } | Self::Blocked { message_id } => message_id,
            Self::SkippedPermission => None,
        }
    }
}

pub async fn execute_video_gen_job<Optimizer, Generator, Effects>(
    optimizer: &Optimizer,
    generator: &Generator,
    effects: &Effects,
    params: &VideoGenJobParams,
    job_id: i64,
    vip: bool,
) -> Result<VideoJobOutcome, String>
where
    Optimizer: VideoPromptOptimizer + ?Sized,
    Generator: VideoGenerator + ?Sized,
    Effects: VideoJobEffects + ?Sized,
{
    if !effects.can_send_video(params.chat_id).await {
        return Ok(VideoJobOutcome::SkippedPermission);
    }
    effects
        .signal_progress(params.chat_id, params.message_id)
        .await;
    let optimized = optimizer
        .optimize_video_prompt(&params.request_text, &params.brief)
        .await?;
    if optimized.nsfw_result == NsfwResult::Forbidden {
        let message_id = effects
            .send_blocked(params.chat_id, params.message_id, params.thread_id)
            .await;
        effects
            .clear_signal(params.chat_id, params.message_id)
            .await;
        return Ok(VideoJobOutcome::Blocked { message_id });
    }
    let video = generator.generate_video(&optimized.prompt, job_id).await?;
    let adult = optimized.nsfw_result == NsfwResult::Adult;
    let message_id = effects
        .send_video(VideoDelivery {
            chat_id: params.chat_id,
            thread_id: params.thread_id,
            reply_to_message_id: params.message_id,
            caption: build_image_generation_caption(
                video_caption_text(params),
                &params.user_full_name,
                vip,
                adult,
            ),
            video,
            spoiler: adult,
        })
        .await?;
    effects
        .clear_signal(params.chat_id, params.message_id)
        .await;
    Ok(VideoJobOutcome::Delivered { message_id })
}

/// The clip title shown like a drawing's: the dialog caption, else the brief.
fn video_caption_text(params: &VideoGenJobParams) -> &str {
    [&params.caption, &params.brief, &params.request_text]
        .into_iter()
        .map(|text| text.trim())
        .find(|text| !text.is_empty())
        .unwrap_or_default()
}

/// What one video ticket ended with.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VideoQueuePoll {
    Finished {
        job_id: i64,
        outcome: VideoJobOutcome,
    },
    Failed {
        job_id: i64,
        error: String,
    },
}

/// Runs video tickets that a shared image-queue worker dequeued.
pub trait VideoJobRunner: Send + Sync {
    /// Run one dequeued `VideoGen` ticket to a terminal state. `vip` marks a ticket
    /// from the VIP queue, which gets the VIP caption.
    fn run_video_job<'a>(
        &'a self,
        queue: &'a InMemoryTaskQueue,
        work: TaskQueueWorkItem,
        vip: bool,
        activity_pulse: Option<&'a TelegramActivityPulse>,
    ) -> VideoJobFuture<'a, VideoQueuePoll>;
}

/// The FastFrames clip pipeline.
pub struct VideoPipeline<Optimizer, Generator, Effects> {
    pub optimizer: Optimizer,
    pub generator: Generator,
    pub effects: Effects,
}

impl<Optimizer, Generator, Effects> VideoJobRunner for VideoPipeline<Optimizer, Generator, Effects>
where
    Optimizer: VideoPromptOptimizer,
    Generator: VideoGenerator,
    Effects: VideoJobEffects,
{
    fn run_video_job<'a>(
        &'a self,
        queue: &'a InMemoryTaskQueue,
        work: TaskQueueWorkItem,
        vip: bool,
        activity_pulse: Option<&'a TelegramActivityPulse>,
    ) -> VideoJobFuture<'a, VideoQueuePoll> {
        Box::pin(run_dequeued_video_job(
            queue,
            work,
            vip,
            &self.optimizer,
            &self.generator,
            &self.effects,
            activity_pulse,
        ))
    }
}

/// Run one dequeued video ticket and record its terminal state.
pub async fn run_dequeued_video_job<Optimizer, Generator, Effects>(
    queue: &InMemoryTaskQueue,
    work: TaskQueueWorkItem,
    vip: bool,
    optimizer: &Optimizer,
    generator: &Generator,
    effects: &Effects,
    activity_pulse: Option<&TelegramActivityPulse>,
) -> VideoQueuePoll
where
    Optimizer: VideoPromptOptimizer + ?Sized,
    Generator: VideoGenerator + ?Sized,
    Effects: VideoJobEffects + ?Sized,
{
    let started_at = OffsetDateTime::now_utc();
    let params = match video_gen_job_params_from_payload(&work.job.data) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            let _ = queue.fail(work.id, error.clone(), started_at);
            tracing::warn!(job_id = work.id, %error, "video job payload is invalid");
            return VideoQueuePoll::Failed {
                job_id: work.id,
                error,
            };
        }
    };
    let _ = queue.set_execution_started(work.id, started_at);
    let activity = activity_pulse.map(|pulse| {
        pulse.start(
            params.chat_id,
            params.thread_id,
            TelegramActivityAction::UploadVideo,
        )
    });
    let result = execute_video_gen_job(optimizer, generator, effects, &params, work.id, vip).await;
    drop(activity);
    let finished_at = OffsetDateTime::now_utc();
    match result {
        Ok(outcome) => {
            if let Some(message_id) = outcome.result_message_id() {
                let _ = queue.update_job_result_message(work.id, Some(message_id));
            }
            let _ = queue.complete(work.id, finished_at);
            tracing::info!(job_id = work.id, vip, ?outcome, "video job finished");
            VideoQueuePoll::Finished {
                job_id: work.id,
                outcome,
            }
        }
        Err(error) => {
            let _ = queue.fail(work.id, error.clone(), finished_at);
            tracing::warn!(job_id = work.id, vip, %error, "video job failed");
            VideoQueuePoll::Failed {
                job_id: work.id,
                error,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use openplotva_taskman::{IMAGE_REGULAR_QUEUE_NAME, JobType, new_video_gen_job_at};

    use super::*;

    struct FixedOptimizer(Result<VideoOptimize, String>);

    impl VideoPromptOptimizer for FixedOptimizer {
        fn optimize_video_prompt<'a>(
            &'a self,
            _request: &'a str,
            _brief: &'a str,
        ) -> VideoJobFuture<'a, Result<VideoOptimize, String>> {
            let result = self.0.clone();
            Box::pin(async move { result })
        }
    }

    #[derive(Default)]
    struct RecordingGenerator {
        prompts: Mutex<Vec<(String, i64)>>,
        fail: bool,
    }

    impl VideoGenerator for RecordingGenerator {
        fn generate_video<'a>(
            &'a self,
            prompt: &'a str,
            job_id: i64,
        ) -> VideoJobFuture<'a, Result<DrawApiVideo, String>> {
            self.prompts
                .lock()
                .expect("prompts")
                .push((prompt.to_owned(), job_id));
            let fail = self.fail;
            Box::pin(async move {
                if fail {
                    return Err("draw-api provider timeout".to_owned());
                }
                Ok(clip())
            })
        }
    }

    fn clip() -> DrawApiVideo {
        DrawApiVideo {
            bytes: b"mp4".to_vec(),
            mime_type: "video/mp4".to_owned(),
            width: 832,
            height: 480,
            frames: 81,
            fps: 16,
        }
    }

    #[derive(Default)]
    struct RecordingEffects {
        denied: bool,
        events: Mutex<Vec<String>>,
        sent: Mutex<Vec<VideoDelivery>>,
    }

    impl RecordingEffects {
        fn events(&self) -> Vec<String> {
            self.events.lock().expect("events").clone()
        }
    }

    impl VideoJobEffects for RecordingEffects {
        fn can_send_video<'a>(&'a self, _chat_id: i64) -> VideoJobFuture<'a, bool> {
            let allowed = !self.denied;
            Box::pin(async move { allowed })
        }

        fn signal_progress<'a>(
            &'a self,
            _chat_id: i64,
            _message_id: i32,
        ) -> VideoJobFuture<'a, ()> {
            self.events
                .lock()
                .expect("events")
                .push("progress".to_owned());
            Box::pin(async {})
        }

        fn clear_signal<'a>(&'a self, _chat_id: i64, _message_id: i32) -> VideoJobFuture<'a, ()> {
            self.events.lock().expect("events").push("clear".to_owned());
            Box::pin(async {})
        }

        fn send_blocked<'a>(
            &'a self,
            _chat_id: i64,
            _message_id: i32,
            _thread_id: Option<i32>,
        ) -> VideoJobFuture<'a, Option<i32>> {
            self.events
                .lock()
                .expect("events")
                .push("blocked".to_owned());
            Box::pin(async { Some(BLOCKED_MESSAGE_ID) })
        }

        fn send_video<'a>(
            &'a self,
            delivery: VideoDelivery,
        ) -> VideoJobFuture<'a, Result<Option<i32>, String>> {
            self.events.lock().expect("events").push("video".to_owned());
            self.sent.lock().expect("sent").push(delivery);
            Box::pin(async { Ok(Some(VIDEO_MESSAGE_ID)) })
        }
    }

    const VIDEO_MESSAGE_ID: i32 = 501;
    const BLOCKED_MESSAGE_ID: i32 = 502;

    fn optimized(nsfw_result: NsfwResult) -> FixedOptimizer {
        FixedOptimizer(Ok(VideoOptimize {
            prompt: "A grey cat catches a snowflake.".to_owned(),
            nsfw_result,
        }))
    }

    fn params() -> VideoGenJobParams {
        VideoGenJobParams {
            chat_id: -100,
            message_id: 42,
            user_id: 7,
            user_full_name: "Alice".to_owned(),
            thread_id: Some(3),
            request_text: "сделай видео: кот ловит снежинку".to_owned(),
            brief: "кот ловит снежинку".to_owned(),
            caption: "Кот и снежинка".to_owned(),
        }
    }

    fn queue_with_job() -> (InMemoryTaskQueue, i64) {
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            new_video_gen_job_at(params(), OffsetDateTime::UNIX_EPOCH),
        );
        (queue, job_id)
    }

    async fn run_next(
        queue: &InMemoryTaskQueue,
        optimizer: &FixedOptimizer,
        generator: &RecordingGenerator,
        effects: &RecordingEffects,
    ) -> VideoQueuePoll {
        let work = queue
            .dequeue_matching(
                IMAGE_REGULAR_QUEUE_NAME,
                "image-regular-worker",
                OffsetDateTime::now_utc(),
                |job| job.data.job_type == JobType::VideoGen,
            )
            .expect("queued clip");
        run_dequeued_video_job(queue, work, false, optimizer, generator, effects, None).await
    }

    #[tokio::test]
    async fn a_clip_is_rendered_from_the_optimized_prompt_and_replies_to_the_request() {
        let (queue, job_id) = queue_with_job();
        let generator = RecordingGenerator::default();
        let effects = RecordingEffects::default();

        let poll = run_next(&queue, &optimized(NsfwResult::Safe), &generator, &effects).await;

        assert_eq!(
            poll,
            VideoQueuePoll::Finished {
                job_id,
                outcome: VideoJobOutcome::Delivered {
                    message_id: Some(VIDEO_MESSAGE_ID)
                }
            }
        );
        assert_eq!(
            generator.prompts.lock().expect("prompts").as_slice(),
            [("A grey cat catches a snowflake.".to_owned(), job_id)]
        );
        assert_eq!(effects.events(), ["progress", "video", "clear"]);
        let sent = effects.sent.lock().expect("sent");
        assert_eq!(sent.len(), 1);
        let delivery = &sent[0];
        assert_eq!(
            (
                delivery.chat_id,
                delivery.thread_id,
                delivery.reply_to_message_id
            ),
            (-100, Some(3), 42)
        );
        assert_eq!(delivery.video, clip());
        assert!(!delivery.spoiler);
        assert!(
            delivery
                .caption
                .starts_with("<code>Кот и снежинка</code> \nза авторством <i>Alice</i>\n<a href=\"https://t.me/PlotvoBot?start=donate\">"),
            "{}",
            delivery.caption
        );
        let record = queue.record(job_id).expect("video job");
        assert_eq!(record.status, openplotva_taskman::JobStatus::Completed);
        assert_eq!(record.result_message_id, Some(VIDEO_MESSAGE_ID));
    }

    #[tokio::test]
    async fn a_blocked_request_records_the_refusal_as_the_result_message() {
        let (queue, job_id) = queue_with_job();
        let poll = run_next(
            &queue,
            &optimized(NsfwResult::Forbidden),
            &RecordingGenerator::default(),
            &RecordingEffects::default(),
        )
        .await;

        assert_eq!(
            poll,
            VideoQueuePoll::Finished {
                job_id,
                outcome: VideoJobOutcome::Blocked {
                    message_id: Some(BLOCKED_MESSAGE_ID)
                }
            }
        );
        let record = queue.record(job_id).expect("video job");
        assert_eq!(record.status, openplotva_taskman::JobStatus::Completed);
        assert_eq!(record.result_message_id, Some(BLOCKED_MESSAGE_ID));
    }

    #[tokio::test]
    async fn an_adult_vip_clip_is_sent_under_a_spoiler_with_the_drawing_tags() {
        let effects = RecordingEffects::default();
        let outcome = execute_video_gen_job(
            &optimized(NsfwResult::Adult),
            &RecordingGenerator::default(),
            &effects,
            &params(),
            9,
            true,
        )
        .await;
        assert_eq!(
            outcome,
            Ok(VideoJobOutcome::Delivered {
                message_id: Some(VIDEO_MESSAGE_ID)
            })
        );
        let sent = effects.sent.lock().expect("sent");
        assert!(sent[0].spoiler);
        assert!(sent[0].caption.contains(
            "за авторством <i><a href=\"https://t.me/PlotvoBot?start=vip\">VIP-персоны  👑</a> Alice\n#nsfw 18+</i>"
        ));
    }

    #[test]
    fn a_clip_without_a_dialog_caption_is_titled_by_its_brief() {
        let untitled = VideoGenJobParams {
            caption: " ".to_owned(),
            ..params()
        };
        assert_eq!(video_caption_text(&untitled), "кот ловит снежинку");
    }

    #[tokio::test]
    async fn a_forbidden_request_is_refused_without_rendering() {
        let generator = RecordingGenerator::default();
        let effects = RecordingEffects::default();
        let outcome = execute_video_gen_job(
            &optimized(NsfwResult::Forbidden),
            &generator,
            &effects,
            &params(),
            9,
            false,
        )
        .await;
        assert_eq!(
            outcome,
            Ok(VideoJobOutcome::Blocked {
                message_id: Some(BLOCKED_MESSAGE_ID)
            })
        );
        assert!(generator.prompts.lock().expect("prompts").is_empty());
        assert_eq!(effects.events(), ["progress", "blocked", "clear"]);
    }

    #[tokio::test]
    async fn a_chat_that_forbids_videos_is_skipped() {
        let generator = RecordingGenerator::default();
        let effects = RecordingEffects {
            denied: true,
            ..RecordingEffects::default()
        };
        let outcome = execute_video_gen_job(
            &optimized(NsfwResult::Safe),
            &generator,
            &effects,
            &params(),
            9,
            false,
        )
        .await;
        assert_eq!(outcome, Ok(VideoJobOutcome::SkippedPermission));
        assert!(effects.events().is_empty());
    }

    #[tokio::test]
    async fn optimizer_and_render_failures_fail_the_job_and_keep_the_progress_signal() {
        let (queue, job_id) = queue_with_job();
        let effects = RecordingEffects::default();
        let poll = run_next(
            &queue,
            &FixedOptimizer(Err("video prompt optimizer: all attempts failed".to_owned())),
            &RecordingGenerator::default(),
            &effects,
        )
        .await;
        assert_eq!(
            poll,
            VideoQueuePoll::Failed {
                job_id,
                error: "video prompt optimizer: all attempts failed".to_owned()
            }
        );
        assert_eq!(effects.events(), ["progress"]);
        assert_eq!(
            queue.record(job_id).map(|record| record.status),
            Some(openplotva_taskman::JobStatus::Failed)
        );

        let render_failure = execute_video_gen_job(
            &optimized(NsfwResult::Safe),
            &RecordingGenerator {
                fail: true,
                ..RecordingGenerator::default()
            },
            &RecordingEffects::default(),
            &params(),
            9,
            false,
        )
        .await;
        assert_eq!(render_failure, Err("draw-api provider timeout".to_owned()));
    }

    #[test]
    fn the_animation_carries_the_clip_shape_and_spoiler() {
        let method = animation_method(&VideoDelivery {
            chat_id: -100,
            thread_id: Some(3),
            reply_to_message_id: 42,
            caption: "Кот и снежинка".to_owned(),
            video: clip(),
            spoiler: true,
        })
        .expect("method");
        let debug = format!("{method:?}");
        for expected in [
            "has_spoiler: Some(true)",
            "width: Some(832)",
            "height: Some(480)",
            "duration: Some(5)",
            "message_thread_id: Some(3)",
            "parse_mode: Some(Html)",
        ] {
            assert!(debug.contains(expected), "missing {expected} in {debug}");
        }
    }

    #[test]
    fn only_farm_side_render_failures_get_a_second_submission() {
        use crate::image_jobs::ImageGenerationError::{Forbidden, Provider};

        for retried in [
            r#"job failed: draw-api status 500: {"detail":"fastframes worker is not running"}"#,
            r#"job failed: draw-api status 503: {"detail":"service unavailable: fastframes is not loaded"}"#,
            "submit draw job: error sending request for url (http://farm.test/v1/jobs)",
        ] {
            assert!(
                video_render_retryable(&Provider(retried.to_owned())),
                "{retried}"
            );
        }
        for final_failure in [
            r#"job failed: draw-api status 400: {"detail":"fastframes needs a prompt"}"#,
            "draw-api provider timeout: inference watchdog expired while waiting to poll draw job video-1",
            "job completed but produced no video",
        ] {
            assert!(
                !video_render_retryable(&Provider(final_failure.to_owned())),
                "{final_failure}"
            );
        }
        assert!(!video_render_retryable(&Forbidden));
    }
}
