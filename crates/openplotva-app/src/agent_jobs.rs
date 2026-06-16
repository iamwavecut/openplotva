//! Background worker that drives durable agent-loop runs on the `agent-qwen`
//! queue: claim or adopt a job, advance one engine step at a time while
//! checkpointing after each, then synthesize the final answer with the writer
//! provider and send it back to the chat.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use openplotva_agent::{
    AgentOrigin, AgentOutcome, AgentState, AgentTools, StepProgress, advance_one_step,
    render_evidence,
};
use openplotva_taskman::{
    AGENT_QWEN_QUEUE_NAME, AGENT_RUN_STATE_FORMAT, AGENT_STEP_STAGE, AgentJobParams,
    AgentRunStateBlob, InMemoryTaskQueue, TaskQueueAgentWorkItem, TaskQueueJobEvent, new_agent_job,
};
use time::OffsetDateTime;

use crate::agent_runtime::{
    AgentProviderRegistry, AifarmReasoner, SearchAgentSettings, now_unix_ms, synthesize_answer,
};
use crate::rich::RichSender;

/// Enqueue a search-agent run on the dedicated `agent-qwen` queue. This is the
/// integration seam a user-facing trigger (command or dialog tool) calls; the
/// live worker then drives and answers it asynchronously.
pub fn enqueue_search_agent_job(queue: &InMemoryTaskQueue, params: AgentJobParams) -> i64 {
    queue.assign(AGENT_QWEN_QUEUE_NAME, new_agent_job(params))
}

const AGENT_JOB_WORKER_ID: &str = "agent-job";
const AGENT_JOB_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const AGENT_FALLBACK_MESSAGE: &str =
    "Sorry, I couldn't complete that search right now. Please try again later.";

/// Cumulative agent worker run report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AgentJobWorkerRunReport {
    pub ticks: u64,
    pub processed: u64,
    pub completed: u64,
    pub failed: u64,
    pub resumed: u64,
}

/// Everything the agent worker needs, wired once at the composition root.
pub struct AgentJobWorker {
    queue: Arc<InMemoryTaskQueue>,
    registry: AgentProviderRegistry,
    tools: Arc<dyn AgentTools>,
    rich: Arc<dyn RichSender>,
    settings: SearchAgentSettings,
}

impl AgentJobWorker {
    #[must_use]
    pub fn new(
        queue: Arc<InMemoryTaskQueue>,
        registry: AgentProviderRegistry,
        tools: Arc<dyn AgentTools>,
        rich: Arc<dyn RichSender>,
        settings: SearchAgentSettings,
    ) -> Self {
        Self {
            queue,
            registry,
            tools,
            rich,
            settings,
        }
    }

    async fn process(&self, work: TaskQueueAgentWorkItem) -> AgentJobOutcome {
        let job_id = work.id;
        let resumed = work.resumed;
        let resume_count = work
            .agent_state
            .as_ref()
            .map_or(0, |blob| blob.resume_count);
        let agent = work.job.data.agent_data.clone().unwrap_or_default();
        let Some(telegram) = work.job.data.telegram_data.clone() else {
            self.fail(job_id, "agent job missing telegram routing data");
            return AgentJobOutcome::Failed;
        };
        let origin = AgentOrigin {
            chat_id: telegram.chat_id,
            message_id: telegram.message_id,
            user_id: telegram.user_id,
            thread_id: telegram.thread_message_id,
            user_full_name: telegram.user_full_name,
        };

        let reasoner_name =
            first_non_empty(&agent.reasoner_provider, &self.settings.reasoner_provider);
        let writer_name = first_non_empty(&agent.writer_provider, &self.settings.writer_provider);
        let Some(reasoner_provider) = self.registry.get(&reasoner_name) else {
            self.fail(
                job_id,
                &format!("unknown reasoner provider `{reasoner_name}`"),
            );
            return AgentJobOutcome::Failed;
        };
        let Some(writer_provider) = self.registry.get(&writer_name) else {
            self.fail(job_id, &format!("unknown writer provider `{writer_name}`"));
            return AgentJobOutcome::Failed;
        };

        let profile = self.settings.profile(
            reasoner_provider.model.clone(),
            writer_provider.model.clone(),
        );
        let reasoner = AifarmReasoner::new(Arc::clone(&reasoner_provider));

        let mut state = work
            .agent_state
            .as_ref()
            .and_then(|blob| serde_json::from_str::<AgentState>(&blob.state).ok())
            .unwrap_or_else(|| {
                AgentState::new(
                    agent.profile_id.clone(),
                    agent.goal.clone(),
                    origin.clone(),
                    now_unix_ms(),
                )
            });

        loop {
            if state.is_terminal() {
                break;
            }
            let now_ms = now_unix_ms();
            match advance_one_step(&profile, &reasoner, self.tools.as_ref(), state, now_ms).await {
                Ok(StepProgress::Continue(next)) => {
                    state = next;
                    self.checkpoint(job_id, &state, resume_count);
                }
                Ok(StepProgress::Terminal(next)) => {
                    state = next;
                    self.checkpoint(job_id, &state, resume_count);
                    break;
                }
                Err(error) => {
                    self.append_event(job_id, "agent_error", 0, Some(&error.to_string()));
                    self.fail(job_id, &error.to_string());
                    return AgentJobOutcome::Failed;
                }
            }
        }

        let answer = self
            .finalize_answer(&profile, writer_provider.as_ref(), &state)
            .await;
        self.send_answer(&origin, &answer).await;
        self.record_completion(job_id, &profile, &state, resume_count);
        let now = OffsetDateTime::now_utc();
        let _ = self.queue.complete(job_id, now);
        if resumed {
            AgentJobOutcome::CompletedResumed
        } else {
            AgentJobOutcome::Completed
        }
    }

    async fn finalize_answer(
        &self,
        profile: &openplotva_agent::AgentProfile,
        writer_provider: &crate::agent_runtime::AgentProviderClient,
        state: &AgentState,
    ) -> String {
        let evidence = render_evidence(state);
        let (draft, limited) = match &state.outcome {
            Some(AgentOutcome::Completed { answer }) => (answer.clone(), false),
            Some(AgentOutcome::Stopped { partial, .. }) => (partial.clone(), true),
            Some(AgentOutcome::Failed { reason }) => {
                tracing::warn!(%reason, "agent run failed before synthesis");
                return AGENT_FALLBACK_MESSAGE.to_owned();
            }
            None => (String::new(), false),
        };

        if evidence.trim().is_empty() {
            if draft.trim().is_empty() {
                return AGENT_FALLBACK_MESSAGE.to_owned();
            }
            return draft;
        }

        let note = if limited {
            "\n\nThe research hit a configured limit; answer from the evidence gathered so far."
        } else {
            ""
        };
        let user_content = format!(
            "User request:\n{}\n\nGathered evidence:\n{evidence}\n\nReasoner draft answer:\n{draft}{note}",
            state.goal
        );
        match synthesize_answer(
            writer_provider,
            &profile.writer_model,
            profile.writer_max_tokens,
            &self.settings.synthesis_prompt,
            &user_content,
        )
        .await
        {
            Ok(text) if !text.trim().is_empty() => text,
            Ok(_) | Err(_) if !draft.trim().is_empty() => draft,
            Ok(_) | Err(_) => AGENT_FALLBACK_MESSAGE.to_owned(),
        }
    }

    fn checkpoint(&self, job_id: i64, state: &AgentState, resume_count: i32) {
        if let Ok(serialized) = serde_json::to_string(state) {
            let blob = AgentRunStateBlob {
                format: AGENT_RUN_STATE_FORMAT.to_owned(),
                committed_step: state.committed_step(),
                state: serialized,
                resume_count,
            };
            if let Err(error) = self.queue.checkpoint_agent_state(job_id, blob) {
                tracing::warn!(%error, job_id, "agent checkpoint failed");
            }
        }
        self.append_event(
            job_id,
            AGENT_STEP_STAGE,
            i32::try_from(state.step_index).unwrap_or(i32::MAX),
            None,
        );
    }

    /// Emit a durable per-run summary event, visible through the existing taskman
    /// job diagnostics. Provides agent metrics without a separate metrics store.
    fn record_completion(
        &self,
        job_id: i64,
        profile: &openplotva_agent::AgentProfile,
        state: &AgentState,
        resume_count: i32,
    ) {
        let finish_reason = match &state.outcome {
            Some(AgentOutcome::Completed { .. }) => "completed".to_owned(),
            Some(AgentOutcome::Stopped { reason, .. }) => format!("stopped:{}", reason.as_str()),
            Some(AgentOutcome::Failed { .. }) => "failed".to_owned(),
            None => "incomplete".to_owned(),
        };
        let wall_ms = now_unix_ms().saturating_sub(state.started_at_unix_ms);
        let mut data = BTreeMap::new();
        data.insert("profile".to_owned(), profile.id.clone());
        data.insert("finish_reason".to_owned(), finish_reason);
        data.insert("steps".to_owned(), state.step_index.to_string());
        data.insert("tool_calls".to_owned(), state.tool_calls_made.to_string());
        data.insert("tokens".to_owned(), state.tokens_spent.to_string());
        data.insert("wall_ms".to_owned(), wall_ms.to_string());
        data.insert("resume_count".to_owned(), resume_count.to_string());
        let event = TaskQueueJobEvent {
            stage: "agent_complete".to_owned(),
            attempt: i32::try_from(state.step_index).unwrap_or(i32::MAX),
            data,
            ..TaskQueueJobEvent::default()
        };
        let _ = self
            .queue
            .append_job_event(job_id, event, OffsetDateTime::now_utc());
    }

    fn append_event(&self, job_id: i64, stage: &str, attempt: i32, error: Option<&str>) {
        let mut data = BTreeMap::new();
        data.insert("at_ms".to_owned(), now_unix_ms().to_string());
        let event = TaskQueueJobEvent {
            stage: stage.to_owned(),
            attempt,
            error: error.unwrap_or_default().to_owned(),
            data,
            ..TaskQueueJobEvent::default()
        };
        let _ = self
            .queue
            .append_job_event(job_id, event, OffsetDateTime::now_utc());
    }

    fn fail(&self, job_id: i64, reason: &str) {
        tracing::warn!(job_id, reason, "agent job failed");
        let _ = self.queue.fail(job_id, reason, OffsetDateTime::now_utc());
    }

    async fn send_answer(&self, origin: &AgentOrigin, answer: &str) {
        let options = openplotva_telegram::RichSendOptions {
            message_thread_id: origin.thread_id.map(i64::from),
            reply_to_message_id: Some(i64::from(origin.message_id)),
            allow_sending_without_reply: true,
            disable_notification: false,
            reply_markup: None,
        };
        // Sanitize to Telegram rich HTML exactly like the dialog flow, so the
        // answer renders properly instead of showing raw markdown.
        let prepared = crate::dialog_jobs::prepare_dialog_chat_response(answer);
        if let Err(error) = self
            .rich
            .send_rich(origin.chat_id, &prepared, &options)
            .await
        {
            tracing::warn!(%error, chat_id = origin.chat_id, "agent answer send failed");
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AgentJobOutcome {
    Completed,
    CompletedResumed,
    Failed,
}

fn first_non_empty(primary: &str, fallback: &str) -> String {
    if primary.trim().is_empty() {
        fallback.to_owned()
    } else {
        primary.to_owned()
    }
}

/// Run the agent worker until `stop` resolves. Cancellation is checked between
/// jobs; an in-flight job runs to its next checkpoint before the loop yields.
pub async fn run_agent_job_worker_until<Stop>(
    worker: &AgentJobWorker,
    stop: Stop,
) -> AgentJobWorkerRunReport
where
    Stop: Future<Output = ()>,
{
    let mut report = AgentJobWorkerRunReport::default();
    let mut ticker = tokio::time::interval(AGENT_JOB_POLL_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                let claimed = worker.queue.dequeue_or_adopt_agent(
                    AGENT_QWEN_QUEUE_NAME,
                    AGENT_JOB_WORKER_ID,
                    OffsetDateTime::now_utc(),
                );
                let Some(work) = claimed else { continue; };
                report.processed += 1;
                match worker.process(work).await {
                    AgentJobOutcome::Completed => report.completed += 1,
                    AgentJobOutcome::CompletedResumed => {
                        report.completed += 1;
                        report.resumed += 1;
                    }
                    AgentJobOutcome::Failed => report.failed += 1,
                }
            }
        }
    }
    report
}
