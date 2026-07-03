//! Runtime API virtual dialogs backed by the normal dialog execution path.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use openplotva_core::{ChatMessageMeta, ChatState, SENDER_TYPE_USER, ToolCall, UserState};
use openplotva_dialog::{
    DialogToolbox, DrawRequest, HistorySearchRequest, HistorySummaryRequest, ROLE_MODEL, ROLE_USER,
    RatesRequest, SongRequest, TOOL_RESULT_STATUS_EXECUTED, TOOL_RESULT_STATUS_OK,
    TOOL_RESULT_STATUS_QUEUED, ToolResult, ToolSideEffect, ToolboxFuture, VisionRequest,
};
use openplotva_llm::ChatProviderHandle;
use openplotva_server::{
    RuntimeTaskmanJobsFilter, RuntimeVirtualDialogData, RuntimeVirtualDialogDeleteResultData,
    RuntimeVirtualDialogFuture, RuntimeVirtualDialogManager, RuntimeVirtualDialogMessageData,
    RuntimeVirtualDialogSendRequest, RuntimeVirtualDialogStartRequest,
    RuntimeVirtualDialogToolMode,
};
use openplotva_storage::{
    HistoryEntryUpsert, PostgresHistoryStore, PostgresRuntimeVirtualDialogStore,
    PostgresVirtualMessageStore, RuntimeVirtualDialogDeleteReport,
    RuntimeVirtualDialogMessageRecord, RuntimeVirtualDialogRecord,
};
use openplotva_taskman::DialogJobParams;
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::watch;

use crate::{
    dialog_jobs::{
        self, DialogBotIdentity, DialogInputMaterializer, PostgresDialogInputMaterializer,
    },
    runtime_llm::RuntimeLlmTraceBuffer,
    runtime_taskman::RuntimeTaskmanInspectorHandle,
};

pub(crate) const RUNTIME_VIRTUAL_DIALOG_CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RUNTIME_VIRTUAL_DIALOG_MESSAGES_QUERY_LIMIT: i64 = 1_000;

#[derive(Clone, Default)]
pub(crate) struct RuntimeVirtualDialogManagerHandle {
    executor: Arc<Mutex<Option<Arc<RuntimeVirtualDialogExecutor>>>>,
}

impl RuntimeVirtualDialogManagerHandle {
    pub(crate) fn set_executor(&self, executor: Arc<RuntimeVirtualDialogExecutor>) {
        *self
            .executor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(executor);
    }

    fn executor(&self) -> Result<Arc<RuntimeVirtualDialogExecutor>, String> {
        self.executor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .ok_or_else(|| "runtime virtual dialog manager is not configured".to_owned())
    }
}

impl RuntimeVirtualDialogManager for RuntimeVirtualDialogManagerHandle {
    fn virtual_dialog<'a>(
        &'a self,
        session_id: &'a str,
    ) -> RuntimeVirtualDialogFuture<'a, Option<RuntimeVirtualDialogData>> {
        Box::pin(async move { self.executor()?.virtual_dialog(session_id).await })
    }

    fn start_virtual_dialog<'a>(
        &'a self,
        request: RuntimeVirtualDialogStartRequest,
    ) -> RuntimeVirtualDialogFuture<'a, RuntimeVirtualDialogData> {
        Box::pin(async move { self.executor()?.start_virtual_dialog(request).await })
    }

    fn send_virtual_dialog_message<'a>(
        &'a self,
        request: RuntimeVirtualDialogSendRequest,
    ) -> RuntimeVirtualDialogFuture<'a, RuntimeVirtualDialogMessageData> {
        Box::pin(async move { self.executor()?.send_virtual_dialog_message(request).await })
    }

    fn delete_virtual_dialog<'a>(
        &'a self,
        session_id: &'a str,
    ) -> RuntimeVirtualDialogFuture<'a, RuntimeVirtualDialogDeleteResultData> {
        Box::pin(async move { self.executor()?.delete_virtual_dialog(session_id).await })
    }
}

#[derive(Clone)]
pub(crate) struct RuntimeVirtualDialogExecutor {
    store: PostgresRuntimeVirtualDialogStore,
    identity: PostgresVirtualMessageStore,
    history: PostgresHistoryStore,
    materializer: PostgresDialogInputMaterializer,
    safe_provider: ChatProviderHandle,
    real_provider: ChatProviderHandle,
    safe_toolbox: std::sync::Arc<dyn openplotva_dialog::DialogToolbox>,
    real_toolbox: std::sync::Arc<dyn openplotva_dialog::DialogToolbox>,
    taskman: RuntimeTaskmanInspectorHandle,
    llm_trace_buffer: RuntimeLlmTraceBuffer,
    bot: DialogBotIdentity,
}

impl RuntimeVirtualDialogExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: PostgresRuntimeVirtualDialogStore,
        identity: PostgresVirtualMessageStore,
        history: PostgresHistoryStore,
        materializer: PostgresDialogInputMaterializer,
        safe_provider: ChatProviderHandle,
        real_provider: ChatProviderHandle,
        safe_toolbox: std::sync::Arc<dyn openplotva_dialog::DialogToolbox>,
        real_toolbox: std::sync::Arc<dyn openplotva_dialog::DialogToolbox>,
        taskman: RuntimeTaskmanInspectorHandle,
        llm_trace_buffer: RuntimeLlmTraceBuffer,
        bot: DialogBotIdentity,
    ) -> Self {
        Self {
            store,
            identity,
            history,
            materializer,
            safe_provider,
            real_provider,
            safe_toolbox,
            real_toolbox,
            taskman,
            llm_trace_buffer,
            bot,
        }
    }

    async fn virtual_dialog(
        &self,
        session_id: &str,
    ) -> Result<Option<RuntimeVirtualDialogData>, String> {
        let now = OffsetDateTime::now_utc();
        let Some(record) = self
            .active_session_after_lazy_expiration(session_id, now)
            .await?
        else {
            return Ok(None);
        };
        self.dialog_data_from_record(record).await.map(Some)
    }

    async fn start_virtual_dialog(
        &self,
        request: RuntimeVirtualDialogStartRequest,
    ) -> Result<RuntimeVirtualDialogData, String> {
        let now = OffsetDateTime::now_utc();
        let existing = self
            .active_session_after_lazy_expiration(&request.session_id, now)
            .await?;
        if request.replace_existing
            && let Some(record) = existing.as_ref()
        {
            self.cleanup_live_artifacts(record);
        }

        let record = self
            .store
            .start_session(&request.session_id, request.replace_existing, now)
            .await
            .map_err(|error| error.to_string())?;
        self.upsert_virtual_identity(&record).await?;
        self.history
            .invalidate_chat_history_cache(record.chat_id)
            .await
            .map_err(|error| error.to_string())?;
        self.dialog_data_from_record(record).await
    }

    async fn send_virtual_dialog_message(
        &self,
        request: RuntimeVirtualDialogSendRequest,
    ) -> Result<RuntimeVirtualDialogMessageData, String> {
        let now = OffsetDateTime::now_utc();
        let Some(record) = self
            .active_session_after_lazy_expiration(&request.session_id, now)
            .await?
        else {
            return Err(format!(
                "runtime virtual dialog session not found: {}",
                request.session_id
            ));
        };
        self.upsert_virtual_identity(&record).await?;

        let (user_message_id, model_message_id) = self
            .store
            .reserve_message_pair(&request.session_id, now)
            .await
            .map_err(|error| error.to_string())?;
        let params = DialogJobParams {
            chat_id: record.chat_id,
            message_id: user_message_id,
            user_id: record.user_id,
            user_full_name: virtual_user_name(&record.session_id),
            message_text: request.text.clone(),
            original_text: request.text.clone(),
            meta: json!(virtual_user_meta(record.user_id, &record.session_id)),
            max_output_tokens: 0,
            thread_id: None,
        };
        self.persist_user_message(
            &record,
            user_message_id,
            &request.text,
            request.tool_mode,
            now,
        )
        .await?;

        let input = self
            .materializer
            .materialize_dialog_input(&params, now)
            .await;
        // SAFE/REAL is a toolbox choice per message, not a provider property:
        // the console drives the same captured session loop the dialog worker
        // uses, with the SAFE toolbox faking generation side effects. The
        // legacy provider pair remains only for providers without the
        // chat-step seam (e.g. a WhiteCircle-wrapped SAFE chain).
        let provider = match request.tool_mode {
            RuntimeVirtualDialogToolMode::Safe => &self.safe_provider,
            RuntimeVirtualDialogToolMode::Real => &self.real_provider,
        };
        let (answer, session_tool_calls, output_provider) =
            if let Some(step_provider) = self.real_provider.as_chat_step() {
                let toolbox = match request.tool_mode {
                    RuntimeVirtualDialogToolMode::Safe => &self.safe_toolbox,
                    RuntimeVirtualDialogToolMode::Real => &self.real_toolbox,
                };
                let captured = match crate::dialog_turn::run_captured_session(
                    step_provider,
                    toolbox.as_ref(),
                    input,
                    8,
                )
                .await
                {
                    Ok(captured) => captured,
                    Err(error) => {
                        let _ = self
                            .history
                            .delete_message_entries(record.chat_id, user_message_id)
                            .await;
                        return Err(error);
                    }
                };
                (
                    captured.messages.join("\n\n"),
                    captured.tool_calls,
                    captured.provider,
                )
            } else {
                let output = match provider.run_dialog(input).await {
                    Ok(output) => output,
                    Err(error) => {
                        let _ = self
                            .history
                            .delete_message_entries(record.chat_id, user_message_id)
                            .await;
                        return Err(error.to_string());
                    }
                };
                let raw_answer = dialog_jobs::dialog_job_answer(&output);
                (
                    dialog_jobs::prepare_dialog_chat_response(&raw_answer),
                    output.tool_calls,
                    output.provider,
                )
            };
        self.history
            .upsert_tool_call_history(record.chat_id, user_message_id, &session_tool_calls)
            .await
            .map_err(|error| error.to_string())?;
        self.persist_model_message(
            &record,
            model_message_id,
            &answer,
            &output_provider,
            request.tool_mode,
            now,
        )
        .await?;
        self.store
            .touch_session(&request.session_id, now)
            .await
            .map_err(|error| error.to_string())?;

        Ok(RuntimeVirtualDialogMessageData {
            message_id: model_message_id,
            role: ROLE_MODEL.to_owned(),
            text: answer,
            at: format_time(now),
            provider: Some(output_provider),
            tool_mode: Some(request.tool_mode),
            tool_calls: tool_calls_value(&session_tool_calls),
        })
    }

    async fn delete_virtual_dialog(
        &self,
        session_id: &str,
    ) -> Result<RuntimeVirtualDialogDeleteResultData, String> {
        let now = OffsetDateTime::now_utc();
        let existing = self
            .store
            .session_record(session_id)
            .await
            .map_err(|error| error.to_string())?;
        let mut live_taskman_deleted = 0;
        let mut live_llm_traces_deleted = 0;
        if let Some(record) = existing.as_ref() {
            let live_cleanup = self.cleanup_live_artifacts(record);
            live_taskman_deleted = live_cleanup.taskman_deleted;
            live_llm_traces_deleted = live_cleanup.llm_traces_deleted;
        }
        let mut report = self
            .store
            .delete_session(session_id, now)
            .await
            .map_err(|error| error.to_string())?;
        report.taskman_deleted = report.taskman_deleted.saturating_add(live_taskman_deleted);
        report.llm_traces_deleted = report
            .llm_traces_deleted
            .saturating_add(live_llm_traces_deleted);
        if let Some(record) = existing.as_ref() {
            self.history
                .invalidate_chat_history_cache(record.chat_id)
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(delete_result_data(report))
    }

    async fn active_session_after_lazy_expiration(
        &self,
        session_id: &str,
        now: OffsetDateTime,
    ) -> Result<Option<RuntimeVirtualDialogRecord>, String> {
        let Some(record) = self
            .store
            .session_record(session_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        if record.expires_at > now {
            return Ok(Some(record));
        }
        self.cleanup_live_artifacts(&record);
        let _ = self
            .store
            .delete_session(session_id, now)
            .await
            .map_err(|error| error.to_string())?;
        self.history
            .invalidate_chat_history_cache(record.chat_id)
            .await
            .map_err(|error| error.to_string())?;
        Ok(None)
    }

    async fn dialog_data_from_record(
        &self,
        record: RuntimeVirtualDialogRecord,
    ) -> Result<RuntimeVirtualDialogData, String> {
        let messages = self
            .store
            .dialog_messages(record.chat_id, RUNTIME_VIRTUAL_DIALOG_MESSAGES_QUERY_LIMIT)
            .await
            .map_err(|error| error.to_string())
            .and_then(|records| dialog_message_data_from_records(records, self.bot.id))?;
        Ok(RuntimeVirtualDialogData {
            session_id: record.session_id,
            chat_id: record.chat_id,
            user_id: record.user_id,
            next_message_id: record.next_message_id,
            message_count: i32::try_from(messages.len()).unwrap_or(i32::MAX),
            last_activity_at: Some(format_time(record.last_activity_at)),
            expires_at: Some(format_time(record.expires_at)),
            messages,
        })
    }

    async fn upsert_virtual_identity(
        &self,
        record: &RuntimeVirtualDialogRecord,
    ) -> Result<(), String> {
        self.identity
            .upsert_chat_state(&ChatState::new(
                record.chat_id,
                "private",
                None,
                None,
                Some(virtual_user_name(&record.session_id)),
                None,
                Some(false),
            ))
            .await
            .map_err(|error| error.to_string())?;
        self.identity
            .upsert_user_state(&UserState::new(
                record.user_id,
                virtual_user_name(&record.session_id),
                None,
                None,
                Some("en".to_owned()),
                Some(false),
            ))
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn persist_user_message(
        &self,
        record: &RuntimeVirtualDialogRecord,
        message_id: i32,
        text: &str,
        tool_mode: RuntimeVirtualDialogToolMode,
        at: OffsetDateTime,
    ) -> Result<(), String> {
        let meta = virtual_user_meta(record.user_id, &record.session_id);
        let sender_name = virtual_user_name(&record.session_id);
        let payload = history_text_payload(HistoryPayloadInput {
            record,
            message_id,
            role: ROLE_USER,
            sender_id: record.user_id,
            sender_name: &sender_name,
            text,
            at,
            meta,
            provider: None,
            tool_mode: Some(tool_mode),
        });
        self.persist_history_payload(record, message_id, ROLE_USER, record.user_id, at, payload)
            .await
    }

    async fn persist_model_message(
        &self,
        record: &RuntimeVirtualDialogRecord,
        message_id: i32,
        text: &str,
        provider: &str,
        tool_mode: RuntimeVirtualDialogToolMode,
        at: OffsetDateTime,
    ) -> Result<(), String> {
        let mut meta = ChatMessageMeta {
            sender_type: SENDER_TYPE_USER.to_owned(),
            sender_id: self.bot.id,
            sender_name: self.bot.name.clone(),
            ..ChatMessageMeta::default()
        };
        if self.bot.id == 0 {
            meta.sender_id = record.chat_id;
        }
        let payload = history_text_payload(HistoryPayloadInput {
            record,
            message_id,
            role: ROLE_MODEL,
            sender_id: meta.sender_id,
            sender_name: &self.bot.name,
            text,
            at,
            meta,
            provider: Some(provider),
            tool_mode: Some(tool_mode),
        });
        self.persist_history_payload(record, message_id, ROLE_MODEL, self.bot.id, at, payload)
            .await
    }

    async fn persist_history_payload(
        &self,
        record: &RuntimeVirtualDialogRecord,
        message_id: i32,
        role: &str,
        sender_id: i64,
        at: OffsetDateTime,
        payload: Value,
    ) -> Result<(), String> {
        let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
        let entry_id = format!("msg:{message_id}");
        self.history
            .upsert_history_entry(HistoryEntryUpsert {
                bucket_day: at.date(),
                chat_id: record.chat_id,
                thread_id: 0,
                message_id,
                entry_id: &entry_id,
                kind: "text",
                role,
                occurred_at: at,
                sender_id,
                payload: &payload,
            })
            .await
            .map_err(|error| error.to_string())
    }

    fn cleanup_live_artifacts(&self, record: &RuntimeVirtualDialogRecord) -> LiveCleanupReport {
        cleanup_runtime_virtual_live_artifacts(record, &self.taskman, &self.llm_trace_buffer)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct LiveCleanupReport {
    taskman_deleted: i32,
    llm_traces_deleted: i32,
}

#[derive(Clone)]
pub(crate) struct RuntimeVirtualSafeToolbox {
    inner: Arc<dyn DialogToolbox>,
}

impl RuntimeVirtualSafeToolbox {
    pub(crate) fn new(inner: Arc<dyn DialogToolbox>) -> Self {
        Self { inner }
    }
}

impl DialogToolbox for RuntimeVirtualSafeToolbox {
    fn currency_rates<'a>(&'a self, req: RatesRequest) -> ToolboxFuture<'a> {
        self.inner.currency_rates(req)
    }

    fn draw_image<'a>(&'a self, req: DrawRequest) -> ToolboxFuture<'a> {
        Box::pin(async move {
            Ok(synthetic_side_effect_result(
                "image_generation_job",
                req.context.chat_id,
                req.context.message_id,
                json!({
                    "prompt": req.prompt,
                    "negative_prompt": req.negative_prompt,
                    "aspect_ratio": req.aspect_ratio,
                    "seed": req.seed,
                    "tool_mode": "SAFE",
                }),
            ))
        })
    }

    fn generate_song<'a>(&'a self, req: SongRequest) -> ToolboxFuture<'a> {
        Box::pin(async move {
            Ok(synthetic_side_effect_result(
                "music_generation_job",
                req.context.chat_id,
                req.context.message_id,
                json!({
                    "topic": req.topic,
                    "tool_mode": "SAFE",
                }),
            ))
        })
    }

    fn vision_image<'a>(&'a self, req: VisionRequest) -> ToolboxFuture<'a> {
        self.inner.vision_image(req)
    }

    fn web_search<'a>(&'a self, query: String) -> ToolboxFuture<'a> {
        self.inner.web_search(query)
    }

    fn crawl_url<'a>(&'a self, url: String) -> ToolboxFuture<'a> {
        self.inner.crawl_url(url)
    }

    fn youtube_summary<'a>(&'a self, video: String) -> ToolboxFuture<'a> {
        self.inner.youtube_summary(video)
    }

    fn queue_status<'a>(&'a self, user_id: i64) -> ToolboxFuture<'a> {
        Box::pin(async move {
            Ok(ToolResult {
                status: TOOL_RESULT_STATUS_OK.to_owned(),
                message: "SAFE mode queue status is synthetic for runtime virtual dialogs."
                    .to_owned(),
                data: Some(json!({
                    "user_id": user_id,
                    "queues": [],
                    "tool_mode": "SAFE",
                })),
                ..ToolResult::default()
            })
        })
    }

    fn cancel_drawing<'a>(&'a self, user_id: i64, chat_id: i64) -> ToolboxFuture<'a> {
        Box::pin(async move {
            Ok(ToolResult {
                status: TOOL_RESULT_STATUS_EXECUTED.to_owned(),
                message: "SAFE mode cancel_drawing accepted without touching real queues."
                    .to_owned(),
                data: Some(json!({
                    "user_id": user_id,
                    "chat_id": chat_id,
                    "tool_mode": "SAFE",
                })),
                ..ToolResult::default()
            })
        })
    }

    fn translate_text<'a>(&'a self, text: String, target_lang: String) -> ToolboxFuture<'a> {
        self.inner.translate_text(text, target_lang)
    }

    fn chat_history_summary<'a>(&'a self, req: HistorySummaryRequest) -> ToolboxFuture<'a> {
        self.inner.chat_history_summary(req)
    }

    fn history_search<'a>(&'a self, req: HistorySearchRequest) -> ToolboxFuture<'a> {
        self.inner.history_search(req)
    }
}

pub(crate) async fn run_runtime_virtual_dialog_cleanup_worker_until(
    store: PostgresRuntimeVirtualDialogStore,
    history: PostgresHistoryStore,
    taskman: RuntimeTaskmanInspectorHandle,
    llm_trace_buffer: RuntimeLlmTraceBuffer,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
) -> i32 {
    let mut deleted = 0i32;
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = OffsetDateTime::now_utc();
                match store.expired_sessions(now).await {
                    Ok(records) => {
                        for record in &records {
                            let _ = cleanup_runtime_virtual_live_artifacts(record, &taskman, &llm_trace_buffer);
                            if let Err(error) = history.invalidate_chat_history_cache(record.chat_id).await {
                                tracing::debug!(%error, chat_id = record.chat_id, "failed to invalidate expired runtime virtual dialog history cache");
                            }
                        }
                    }
                    Err(error) => tracing::warn!(%error, "failed to load expired runtime virtual dialogs for live cleanup"),
                }
                match store.delete_expired_sessions(now).await {
                    Ok(count) => deleted = deleted.saturating_add(count),
                    Err(error) => tracing::warn!(%error, "failed to clean expired runtime virtual dialogs"),
                }
            }
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
        }
    }
    deleted
}

fn cleanup_runtime_virtual_live_artifacts(
    record: &RuntimeVirtualDialogRecord,
    taskman: &RuntimeTaskmanInspectorHandle,
    llm_trace_buffer: &RuntimeLlmTraceBuffer,
) -> LiveCleanupReport {
    let taskman_deleted = taskman
        .delete_jobs(RuntimeTaskmanJobsFilter {
            chat_id: Some(record.chat_id),
            user_id: Some(record.user_id),
            limit: 1_000,
            ..RuntimeTaskmanJobsFilter::default()
        })
        .map(|report| report.deleted)
        .unwrap_or_default();
    let llm_traces_deleted = llm_trace_buffer.prune_chat(record.chat_id);
    LiveCleanupReport {
        taskman_deleted,
        llm_traces_deleted,
    }
}

fn dialog_message_data_from_records(
    records: Vec<RuntimeVirtualDialogMessageRecord>,
    bot_id: i64,
) -> Result<Vec<RuntimeVirtualDialogMessageData>, String> {
    records
        .into_iter()
        .map(|record| {
            let role = virtual_message_role(&record.payload, &record.role, bot_id);
            Ok(RuntimeVirtualDialogMessageData {
                message_id: record.message_id,
                role,
                text: string_value(&record.payload, "text"),
                at: format_time(record.occurred_at),
                provider: record
                    .payload
                    .get("provider")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                tool_mode: record
                    .payload
                    .get("tool_mode")
                    .and_then(Value::as_str)
                    .and_then(parse_tool_mode),
                tool_calls: payload_tool_calls(&record.payload),
            })
        })
        .collect()
}

fn virtual_message_role(value: &Value, stored_role: &str, bot_id: i64) -> String {
    let sender_id = value
        .get("meta")
        .and_then(|meta| meta.get("sender_id"))
        .and_then(Value::as_i64)
        .or_else(|| {
            value
                .get("from")
                .and_then(|from| from.get("id"))
                .and_then(Value::as_i64)
        })
        .unwrap_or_default();
    if stored_role == ROLE_MODEL || (bot_id != 0 && sender_id == bot_id) {
        ROLE_MODEL.to_owned()
    } else {
        ROLE_USER.to_owned()
    }
}

struct HistoryPayloadInput<'a> {
    record: &'a RuntimeVirtualDialogRecord,
    message_id: i32,
    role: &'a str,
    sender_id: i64,
    sender_name: &'a str,
    text: &'a str,
    at: OffsetDateTime,
    meta: ChatMessageMeta,
    provider: Option<&'a str>,
    tool_mode: Option<RuntimeVirtualDialogToolMode>,
}

fn history_text_payload(input: HistoryPayloadInput<'_>) -> Value {
    let record = input.record;
    let message_id = input.message_id;
    let mut payload = json!({
        "entry_id": format!("msg:{message_id}"),
        "role": input.role,
        "kind": "text",
        "timestamp": format_time(input.at),
        "message_id": message_id,
        "date": input.at.unix_timestamp(),
        "chat": {
            "id": record.chat_id,
            "type": "private",
            "first_name": virtual_user_name(&record.session_id),
        },
        "from": {
            "id": input.sender_id,
            "first_name": input.sender_name,
        },
        "text": input.text,
        "meta": input.meta,
    });
    if let Some(provider) = input.provider {
        payload["provider"] = json!(provider);
    }
    if let Some(tool_mode) = input.tool_mode {
        payload["tool_mode"] = json!(tool_mode_label(tool_mode));
    }
    payload
}

fn virtual_user_meta(user_id: i64, session_id: &str) -> ChatMessageMeta {
    ChatMessageMeta {
        sender_type: SENDER_TYPE_USER.to_owned(),
        sender_id: user_id,
        sender_name: virtual_user_name(session_id),
        ..ChatMessageMeta::default()
    }
}

fn virtual_user_name(session_id: &str) -> String {
    format!("Runtime Debug ({session_id})")
}

fn string_value(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn payload_tool_calls(value: &Value) -> Option<Value> {
    value
        .get("meta")
        .and_then(|meta| meta.get("tool_calls"))
        .filter(|value| value.as_array().is_some_and(|items| !items.is_empty()))
        .cloned()
        .or_else(|| value.get("tool_call").cloned())
}

fn parse_tool_mode(value: &str) -> Option<RuntimeVirtualDialogToolMode> {
    if value.eq_ignore_ascii_case("SAFE") {
        Some(RuntimeVirtualDialogToolMode::Safe)
    } else if value.eq_ignore_ascii_case("REAL") {
        Some(RuntimeVirtualDialogToolMode::Real)
    } else {
        None
    }
}

fn tool_mode_label(value: RuntimeVirtualDialogToolMode) -> &'static str {
    match value {
        RuntimeVirtualDialogToolMode::Safe => "SAFE",
        RuntimeVirtualDialogToolMode::Real => "REAL",
    }
}

fn tool_calls_value(calls: &[ToolCall]) -> Option<Value> {
    if calls.is_empty() {
        None
    } else {
        serde_json::to_value(calls).ok()
    }
}

fn delete_result_data(
    report: RuntimeVirtualDialogDeleteReport,
) -> RuntimeVirtualDialogDeleteResultData {
    RuntimeVirtualDialogDeleteResultData {
        found: report.found,
        deleted: report.deleted,
        history_deleted: report.history_deleted,
        taskman_deleted: report.taskman_deleted,
        llm_traces_deleted: report.llm_traces_deleted,
    }
}

fn synthetic_side_effect_result(
    kind: &str,
    chat_id: i64,
    message_id: i32,
    data: Value,
) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_QUEUED.to_owned(),
        message: "SAFE mode accepted the request without starting external side effects."
            .to_owned(),
        no_reply: false,
        side_effect: Some(ToolSideEffect {
            kind: kind.to_owned(),
            ticket_id: format!("runtime-virtual:{chat_id}:{message_id}:{kind}"),
            eta: String::new(),
            state: "queued".to_owned(),
        }),
        data: Some(data),
        error: None,
    }
}

fn format_time(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).unwrap_or_else(|_| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct ToolboxStub;

    impl DialogToolbox for ToolboxStub {}

    #[tokio::test]
    async fn runtime_virtual_dialog_safe_toolbox_returns_synthetic_side_effects() {
        let toolbox = RuntimeVirtualSafeToolbox::new(Arc::new(ToolboxStub));
        let result = toolbox
            .draw_image(DrawRequest {
                context: openplotva_dialog::ToolContext {
                    chat_id: -91,
                    message_id: 7,
                    ..openplotva_dialog::ToolContext::default()
                },
                prompt: "draw a fish".to_owned(),
                ..DrawRequest::default()
            })
            .await
            .expect("safe draw result");

        assert_eq!(result.status, TOOL_RESULT_STATUS_QUEUED);
        assert_eq!(
            result
                .side_effect
                .as_ref()
                .map(|effect| effect.kind.as_str()),
            Some("image_generation_job")
        );
        assert_eq!(
            result.data.as_ref().and_then(|data| data.get("tool_mode")),
            Some(&json!("SAFE"))
        );
    }

    #[tokio::test]
    async fn runtime_virtual_dialog_handle_reports_unconfigured_executor() {
        let handle = RuntimeVirtualDialogManagerHandle::default();
        let error = handle
            .virtual_dialog("session")
            .await
            .expect_err("unconfigured handle should error");

        assert_eq!(error, "runtime virtual dialog manager is not configured");
    }

    #[test]
    fn runtime_virtual_dialog_message_records_keep_empty_model_turns() {
        let at = OffsetDateTime::UNIX_EPOCH;
        let dialog = RuntimeVirtualDialogRecord {
            session_id: "empty-model-turn".to_owned(),
            chat_id: -9_100_000_000_001,
            user_id: -9_200_000_000_001,
            next_message_id: 3,
            last_activity_at: at,
            expires_at: at + time::Duration::hours(24),
            deleted_at: None,
            created_at: at,
            updated_at: at,
        };
        let payload = history_text_payload(HistoryPayloadInput {
            record: &dialog,
            message_id: 2,
            role: ROLE_MODEL,
            sender_id: 42,
            sender_name: "Plotva",
            text: "",
            at,
            meta: ChatMessageMeta {
                sender_type: SENDER_TYPE_USER.to_owned(),
                sender_id: 42,
                sender_name: "Plotva".to_owned(),
                ..ChatMessageMeta::default()
            },
            provider: Some("test-provider"),
            tool_mode: Some(RuntimeVirtualDialogToolMode::Safe),
        });
        let messages = dialog_message_data_from_records(
            vec![RuntimeVirtualDialogMessageRecord {
                message_id: 2,
                role: ROLE_MODEL.to_owned(),
                occurred_at: at,
                payload,
            }],
            42,
        )
        .expect("empty model message should decode");

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message_id, 2);
        assert_eq!(messages[0].role, ROLE_MODEL);
        assert_eq!(messages[0].text, "");
        assert_eq!(messages[0].provider.as_deref(), Some("test-provider"));
    }

    #[test]
    fn runtime_virtual_dialog_ttl_is_one_day() {
        assert_eq!(
            openplotva_storage::RUNTIME_VIRTUAL_DIALOG_TTL,
            time::Duration::hours(24)
        );
    }
}
