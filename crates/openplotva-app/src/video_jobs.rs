//! Video jobs: the optimizer writes a FastFrames prompt, draw-api renders the clip, and the
//! bot replies with it as a Telegram animation.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration as StdDuration};

use openplotva_llm::aifarm::AifarmHttpTransport;
use openplotva_media::{NsfwResult, video::VideoOptimize};
use openplotva_server::ACTION_SEND_VIDEO;
use openplotva_taskman::{
    InMemoryTaskQueue, JobType, VIDEO_QUEUE_NAME, VideoGenJobParams,
    video_gen_job_params_from_payload,
};
use openplotva_telegram::{
    ChatRef, ReplyParametersPlan, TelegramClient, TelegramOutboundMethod, UploadMessageRequest,
    build_animation_message_method, build_text_message_methods, ensure_telegram_safe_text,
};
use time::OffsetDateTime;

use crate::image_jobs::{AifarmDrawApiImageGenerator, DrawApiVideo, ImageJobTelegramSender};
use crate::permissions::{ChatPermissionPolicy, ChatPermissionStore};
use crate::telegram_activity::{TelegramActivityAction, TelegramActivityPulse};

pub const VIDEO_JOB_POLL_INTERVAL: StdDuration = StdDuration::from_secs(1);
const VIDEO_WORKER_ID: &str = "video-worker";
const VIDEO_FILE_NAME: &str = "video.mp4";
const TELEGRAM_CAPTION_MAX_CHARS: usize = 1024;

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
            self.generate_video_with_job_id(prompt, &format!("video-{job_id}"))
                .await
                .map_err(|error| error.message())
        })
    }
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
    fn send_blocked<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        thread_id: Option<i32>,
    ) -> VideoJobFuture<'a, ()>;

    fn send_video<'a>(&'a self, delivery: VideoDelivery) -> VideoJobFuture<'a, Result<(), String>>;
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
    ) -> VideoJobFuture<'a, ()> {
        Box::pin(async move {
            let (request, reply_to) =
                crate::image_jobs::nsfw_blocked_message_request(chat_id, message_id, thread_id);
            let Ok(methods) = build_text_message_methods(&request, Some(&reply_to)) else {
                return;
            };
            for method in methods {
                let _ = self
                    .telegram
                    .send_image_job_method(TelegramOutboundMethod::from(method))
                    .await;
            }
        })
    }

    fn send_video<'a>(&'a self, delivery: VideoDelivery) -> VideoJobFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let method = animation_method(&delivery)?;
            self.telegram
                .execute(method)
                .await
                .map(|_message| ())
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
        render_as: String::new(),
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

/// What one video job ended with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoJobOutcome {
    Delivered,
    /// The optimizer classified the request as forbidden; the refusal was sent.
    Blocked,
    SkippedPermission,
}

pub async fn execute_video_gen_job<Optimizer, Generator, Effects>(
    optimizer: &Optimizer,
    generator: &Generator,
    effects: &Effects,
    params: &VideoGenJobParams,
    job_id: i64,
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
        effects
            .send_blocked(params.chat_id, params.message_id, params.thread_id)
            .await;
        effects
            .clear_signal(params.chat_id, params.message_id)
            .await;
        return Ok(VideoJobOutcome::Blocked);
    }
    let video = generator.generate_video(&optimized.prompt, job_id).await?;
    effects
        .send_video(VideoDelivery {
            chat_id: params.chat_id,
            thread_id: params.thread_id,
            reply_to_message_id: params.message_id,
            caption: video_caption(&params.caption),
            video,
            spoiler: optimized.nsfw_result == NsfwResult::Adult,
        })
        .await?;
    effects
        .clear_signal(params.chat_id, params.message_id)
        .await;
    Ok(VideoJobOutcome::Delivered)
}

fn video_caption(caption: &str) -> String {
    ensure_telegram_safe_text(caption.trim())
        .chars()
        .take(TELEGRAM_CAPTION_MAX_CHARS)
        .collect()
}

/// What one poll of the video queue did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VideoQueuePoll {
    Idle,
    Finished {
        job_id: i64,
        outcome: VideoJobOutcome,
    },
    Failed {
        job_id: i64,
        error: String,
    },
}

/// Dequeue and run one video job.
pub async fn run_video_queue_once<Optimizer, Generator, Effects>(
    queue: &InMemoryTaskQueue,
    optimizer: &Optimizer,
    generator: &Generator,
    effects: &Effects,
    activity_pulse: Option<&TelegramActivityPulse>,
    now: OffsetDateTime,
) -> VideoQueuePoll
where
    Optimizer: VideoPromptOptimizer + ?Sized,
    Generator: VideoGenerator + ?Sized,
    Effects: VideoJobEffects + ?Sized,
{
    let Some(work) = queue.dequeue_matching(VIDEO_QUEUE_NAME, VIDEO_WORKER_ID, now, |job| {
        job.data.job_type == JobType::VideoGen
    }) else {
        return VideoQueuePoll::Idle;
    };
    let params = match video_gen_job_params_from_payload(&work.job.data) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            let _ = queue.fail(work.id, error.clone(), now);
            return VideoQueuePoll::Failed {
                job_id: work.id,
                error,
            };
        }
    };
    let _ = queue.set_execution_started(work.id, now);
    let activity = activity_pulse.map(|pulse| {
        pulse.start(
            params.chat_id,
            params.thread_id,
            TelegramActivityAction::UploadVideo,
        )
    });
    let result = execute_video_gen_job(optimizer, generator, effects, &params, work.id).await;
    drop(activity);
    let finished_at = OffsetDateTime::now_utc();
    match result {
        Ok(outcome) => {
            let _ = queue.complete(work.id, finished_at);
            VideoQueuePoll::Finished {
                job_id: work.id,
                outcome,
            }
        }
        Err(error) => {
            let _ = queue.fail(work.id, error.clone(), finished_at);
            VideoQueuePoll::Failed {
                job_id: work.id,
                error,
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VideoWorkerRunReport {
    pub delivered: u64,
    pub blocked: u64,
    pub skipped_permission: u64,
    pub failed: u64,
}

/// Run the video worker until `stop` resolves.
pub async fn run_video_worker_until<Optimizer, Generator, Effects, Stop>(
    queue: &InMemoryTaskQueue,
    optimizer: &Optimizer,
    generator: &Generator,
    effects: &Effects,
    activity_pulse: Option<&TelegramActivityPulse>,
    stop: Stop,
) -> VideoWorkerRunReport
where
    Optimizer: VideoPromptOptimizer + ?Sized,
    Generator: VideoGenerator + ?Sized,
    Effects: VideoJobEffects + ?Sized,
    Stop: Future<Output = ()>,
{
    let mut report = VideoWorkerRunReport::default();
    let mut stop = std::pin::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(VIDEO_JOB_POLL_INTERVAL) => {
                let poll = run_video_queue_once(
                    queue,
                    optimizer,
                    generator,
                    effects,
                    activity_pulse,
                    OffsetDateTime::now_utc(),
                )
                .await;
                match poll {
                    VideoQueuePoll::Idle => {}
                    VideoQueuePoll::Finished { job_id, outcome } => {
                        tracing::info!(job_id, ?outcome, "video job finished");
                        match outcome {
                            VideoJobOutcome::Delivered => report.delivered += 1,
                            VideoJobOutcome::Blocked => report.blocked += 1,
                            VideoJobOutcome::SkippedPermission => report.skipped_permission += 1,
                        }
                    }
                    VideoQueuePoll::Failed { job_id, error } => {
                        tracing::warn!(job_id, %error, "video job failed");
                        report.failed += 1;
                    }
                }
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use openplotva_taskman::new_video_gen_job_at;

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
        ) -> VideoJobFuture<'a, ()> {
            self.events
                .lock()
                .expect("events")
                .push("blocked".to_owned());
            Box::pin(async {})
        }

        fn send_video<'a>(
            &'a self,
            delivery: VideoDelivery,
        ) -> VideoJobFuture<'a, Result<(), String>> {
            self.events.lock().expect("events").push("video".to_owned());
            self.sent.lock().expect("sent").push(delivery);
            Box::pin(async { Ok(()) })
        }
    }

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
            VIDEO_QUEUE_NAME,
            new_video_gen_job_at(params(), OffsetDateTime::UNIX_EPOCH),
        );
        (queue, job_id)
    }

    #[tokio::test]
    async fn a_clip_is_rendered_from_the_optimized_prompt_and_replies_to_the_request() {
        let (queue, job_id) = queue_with_job();
        let generator = RecordingGenerator::default();
        let effects = RecordingEffects::default();

        let poll = run_video_queue_once(
            &queue,
            &optimized(NsfwResult::Safe),
            &generator,
            &effects,
            None,
            OffsetDateTime::now_utc(),
        )
        .await;

        assert_eq!(
            poll,
            VideoQueuePoll::Finished {
                job_id,
                outcome: VideoJobOutcome::Delivered
            }
        );
        assert_eq!(
            generator.prompts.lock().expect("prompts").as_slice(),
            [("A grey cat catches a snowflake.".to_owned(), job_id)]
        );
        assert_eq!(effects.events(), ["progress", "video", "clear"]);
        let sent = effects.sent.lock().expect("sent");
        assert_eq!(
            sent.as_slice(),
            [VideoDelivery {
                chat_id: -100,
                thread_id: Some(3),
                reply_to_message_id: 42,
                caption: "Кот и снежинка".to_owned(),
                video: clip(),
                spoiler: false,
            }]
        );
        assert_eq!(
            queue.record(job_id).map(|record| record.status),
            Some(openplotva_taskman::JobStatus::Completed)
        );
    }

    #[tokio::test]
    async fn an_adult_clip_is_sent_under_a_spoiler() {
        let effects = RecordingEffects::default();
        let outcome = execute_video_gen_job(
            &optimized(NsfwResult::Adult),
            &RecordingGenerator::default(),
            &effects,
            &params(),
            9,
        )
        .await;
        assert_eq!(outcome, Ok(VideoJobOutcome::Delivered));
        assert!(effects.sent.lock().expect("sent")[0].spoiler);
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
        )
        .await;
        assert_eq!(outcome, Ok(VideoJobOutcome::Blocked));
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
        )
        .await;
        assert_eq!(outcome, Ok(VideoJobOutcome::SkippedPermission));
        assert!(effects.events().is_empty());
    }

    #[tokio::test]
    async fn optimizer_and_render_failures_fail_the_job_and_keep_the_progress_signal() {
        let (queue, job_id) = queue_with_job();
        let effects = RecordingEffects::default();
        let poll = run_video_queue_once(
            &queue,
            &FixedOptimizer(Err("video prompt optimizer: all attempts failed".to_owned())),
            &RecordingGenerator::default(),
            &effects,
            None,
            OffsetDateTime::now_utc(),
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
        ] {
            assert!(debug.contains(expected), "missing {expected} in {debug}");
        }
    }
}
