//! Runtime API LLM trace buffer and dialog provider recorder.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Instant,
};

use openplotva_dialog::{DialogInput, DialogOutput, DialogTraceArtifacts, DialogTraceError};
use openplotva_llm::{ChatProvider, ChatProviderFuture, ChatProviderHandle};
use openplotva_server::{
    RuntimeLlmGenConfigData, RuntimeLlmRequestChatData, RuntimeLlmRequestData,
    RuntimeLlmRequestMessageData, RuntimeLlmRequestResultData, RuntimeLlmRequestUserData,
    RuntimeLlmRequestsFilter, RuntimeLlmTraceInspector,
};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const GO_LLM_TRACE_BUFFER_CAPACITY: usize = 1_000;
const GO_RESPONSE_PREVIEW_SIZE: usize = 500;

#[derive(Clone, Debug)]
pub struct RuntimeLlmTraceBuffer {
    inner: Arc<Mutex<RuntimeLlmTraceBufferInner>>,
    next_id: Arc<AtomicI64>,
}

#[derive(Debug)]
struct RuntimeLlmTraceBufferInner {
    ring: Vec<Option<RuntimeLlmRequestData>>,
    write: usize,
    count: usize,
}

impl Default for RuntimeLlmTraceBuffer {
    fn default() -> Self {
        Self::new(GO_LLM_TRACE_BUFFER_CAPACITY)
    }
}

impl RuntimeLlmTraceBuffer {
    pub fn new(capacity: usize) -> Self {
        let capacity = if capacity == 0 {
            GO_LLM_TRACE_BUFFER_CAPACITY
        } else {
            capacity
        };
        Self {
            inner: Arc::new(Mutex::new(RuntimeLlmTraceBufferInner {
                ring: vec![None; capacity],
                write: 0,
                count: 0,
            })),
            next_id: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn record(&self, mut trace: RuntimeLlmRequestData) {
        trace.id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.ring.is_empty() {
            return;
        }
        let write = inner.write;
        inner.ring[write] = Some(trace);
        inner.write = (write + 1) % inner.ring.len();
        inner.count = inner.count.saturating_add(1).min(inner.ring.len());
    }

    pub fn clear(&self) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.ring.fill(None);
        inner.write = 0;
        inner.count = 0;
    }

    fn list(&self) -> Vec<RuntimeLlmRequestData> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.count == 0 || inner.ring.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::with_capacity(inner.count);
        let mut idx = inner.write.checked_sub(1).unwrap_or(inner.ring.len() - 1);
        for _ in 0..inner.count {
            if let Some(trace) = inner.ring[idx].clone() {
                out.push(trace);
            }
            idx = idx.checked_sub(1).unwrap_or(inner.ring.len() - 1);
        }
        out
    }
}

impl RuntimeLlmTraceInspector for RuntimeLlmTraceBuffer {
    fn llm_requests(
        &self,
        filter: RuntimeLlmRequestsFilter,
    ) -> Result<Vec<RuntimeLlmRequestData>, String> {
        let limit = filter.limit.max(1) as usize;
        let mut out = Vec::with_capacity(limit.min(GO_LLM_TRACE_BUFFER_CAPACITY));
        for trace in self.list() {
            if llm_trace_matches_filter(&trace, &filter) {
                out.push(trace);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }
}

pub struct TracingChatProvider {
    inner: ChatProviderHandle,
    traces: RuntimeLlmTraceBuffer,
}

impl TracingChatProvider {
    /// Wrap a dialog provider with runtime trace capture.
    pub fn new(inner: ChatProviderHandle, traces: RuntimeLlmTraceBuffer) -> Self {
        Self { inner, traces }
    }
}

impl ChatProvider for TracingChatProvider {
    fn provider_name(&self) -> &str {
        self.inner.provider_name()
    }

    fn run_dialog<'a>(&'a self, input: DialogInput) -> ChatProviderFuture<'a> {
        Box::pin(async move {
            let started = Instant::now();
            let trace_input = input.clone();
            match self.inner.run_dialog(input).await {
                Ok(output) => {
                    record_dialog_success_traces(
                        &self.traces,
                        self.inner.provider_name(),
                        &trace_input,
                        &output,
                        elapsed_ms(started),
                    );
                    Ok(output)
                }
                Err(error) => {
                    let message = error.to_string();
                    record_dialog_error_traces(
                        &self.traces,
                        self.inner.provider_name(),
                        &trace_input,
                        error.as_ref(),
                        &message,
                        elapsed_ms(started),
                    );
                    Err(error)
                }
            }
        })
    }
}

fn record_dialog_error_traces(
    traces: &RuntimeLlmTraceBuffer,
    provider_name: &str,
    input: &DialogInput,
    error: &(dyn Error + 'static),
    message: &str,
    duration_ms: i32,
) {
    if let Some(events) = dialog_trace_events_from_error(error).filter(|events| !events.is_empty())
    {
        for artifact in events {
            traces.record(trace_from_dialog_error_with_artifact(
                provider_name,
                input,
                message,
                Some(artifact),
                duration_ms,
            ));
        }
        return;
    }
    traces.record(trace_from_dialog_error_with_artifact(
        provider_name,
        input,
        message,
        None,
        duration_ms,
    ));
}

fn dialog_trace_events_from_error<'a>(
    error: &'a (dyn Error + 'static),
) -> Option<&'a [DialogTraceArtifacts]> {
    if let Some(error) = error.downcast_ref::<DialogTraceError>() {
        return Some(error.trace_events());
    }
    error.source().and_then(dialog_trace_events_from_error)
}

fn record_dialog_success_traces(
    traces: &RuntimeLlmTraceBuffer,
    provider_name: &str,
    input: &DialogInput,
    output: &DialogOutput,
    duration_ms: i32,
) {
    if output.trace_events.is_empty() {
        traces.record(trace_from_dialog_success(
            provider_name,
            input,
            output,
            duration_ms,
        ));
        return;
    }
    for artifact in &output.trace_events {
        traces.record(trace_from_dialog_success_with_artifact(
            provider_name,
            input,
            output,
            Some(artifact),
            duration_ms,
        ));
    }
}

fn trace_from_dialog_success(
    provider_name: &str,
    input: &DialogInput,
    output: &DialogOutput,
    duration_ms: i32,
) -> RuntimeLlmRequestData {
    trace_from_dialog_success_with_artifact(
        provider_name,
        input,
        output,
        output.trace.as_ref(),
        duration_ms,
    )
}

fn trace_from_dialog_success_with_artifact(
    provider_name: &str,
    input: &DialogInput,
    output: &DialogOutput,
    artifact: Option<&DialogTraceArtifacts>,
    duration_ms: i32,
) -> RuntimeLlmRequestData {
    let provider = non_empty(&output.provider).unwrap_or_else(|| provider_name.to_owned());
    let preview = trace_response_preview(output, artifact);
    let mut trace = trace_from_dialog_base(input, duration_ms);
    trace.provider = Some(provider);
    trace.raw_response = artifact
        .and_then(|artifact| artifact.raw_response.clone())
        .or_else(|| to_json_value(output));
    if let Some(artifact) = artifact {
        apply_dialog_trace_artifact(&mut trace, artifact);
    }
    trace.result = RuntimeLlmRequestResultData {
        duration_ms,
        response_text_preview: non_empty(&preview),
        ..RuntimeLlmRequestResultData::default()
    };
    trace
}

fn apply_dialog_trace_artifact(trace: &mut RuntimeLlmRequestData, artifact: &DialogTraceArtifacts) {
    if !artifact.provider.trim().is_empty() {
        trace.provider = Some(artifact.provider.trim().to_owned());
    }
    if !artifact.request_kind.trim().is_empty() {
        trace.request_kind = Some(artifact.request_kind.trim().to_owned());
    }
    if !artifact.source.trim().is_empty() {
        trace.source = artifact.source.trim().to_owned();
    }
    if !artifact.mode.trim().is_empty() {
        trace.mode = Some(artifact.mode.trim().to_owned());
    }
    if !artifact.flow.trim().is_empty() {
        trace.flow = Some(artifact.flow.trim().to_owned());
    }
    if artifact.iteration > 0 {
        trace.iteration = artifact.iteration;
    }
    if !artifact.model.trim().is_empty() {
        trace.model = Some(artifact.model.trim().to_owned());
    }
    if let Some(raw_request) = artifact.raw_request.clone() {
        trace.raw_request = Some(raw_request);
    }
    if let Some(raw_response) = artifact.raw_response.clone() {
        trace.raw_response = Some(raw_response);
    }
    if let Some(resolved_cache_content) = artifact.resolved_cache_content.clone() {
        trace.resolved_cache_content = Some(resolved_cache_content);
    }
    if let Some(transport) = artifact.transport.clone() {
        trace.transport = Some(transport);
    }
    if let Some(inference_params) = artifact.inference_params.clone() {
        trace.inference_params = Some(inference_params);
    }
    trace.usage = artifact
        .usage
        .as_ref()
        .and_then(|usage| serde_json::to_value(usage).ok());
    if let Some(timings) = artifact.timings.clone() {
        trace.timings = Some(timings);
    }
    if artifact.prompt_chars > 0 {
        trace.prompt_chars = artifact.prompt_chars;
    }
    if artifact.prompt_messages > 0 {
        trace.prompt_messages = artifact.prompt_messages;
    }
    if artifact.docs_chars > 0 {
        trace.docs_chars = artifact.docs_chars;
    }
}

fn trace_from_dialog_error_with_artifact(
    provider_name: &str,
    input: &DialogInput,
    error: &str,
    artifact: Option<&DialogTraceArtifacts>,
    duration_ms: i32,
) -> RuntimeLlmRequestData {
    let mut trace = trace_from_dialog_base(input, duration_ms);
    trace.provider = Some(provider_name.to_owned());
    if let Some(artifact) = artifact {
        apply_dialog_trace_artifact(&mut trace, artifact);
    }
    trace.result = RuntimeLlmRequestResultData {
        duration_ms,
        error: non_empty(
            artifact
                .map(|artifact| artifact.error.as_str())
                .filter(|error| !error.trim().is_empty())
                .unwrap_or(error),
        ),
        response_text_preview: artifact
            .and_then(|artifact| artifact.raw_response.as_ref())
            .and_then(trace_response_text)
            .map(|text| compact_preview(&text, GO_RESPONSE_PREVIEW_SIZE))
            .and_then(|text| non_empty(&text)),
    };
    trace
}

fn trace_from_dialog_base(input: &DialogInput, duration_ms: i32) -> RuntimeLlmRequestData {
    let raw_request = to_json_value(input);
    let docs_chars = input
        .reference_context
        .iter()
        .map(String::len)
        .sum::<usize>()
        .saturating_add(input.shield_context.len())
        .min(i32::MAX as usize) as i32;
    RuntimeLlmRequestData {
        at: format_ts(OffsetDateTime::now_utc()),
        request_kind: Some("dialog".to_owned()),
        source: "dialog".to_owned(),
        mode: Some(if input.guest_mode { "guest" } else { "chat" }.to_owned()),
        flow: Some("dialog".to_owned()),
        iteration: 1,
        model: non_empty(&input.model),
        chat: RuntimeLlmRequestChatData {
            chat_id: input.context.chat_id,
            thread_id: input.context.thread_id,
            chat_title: non_empty(&input.context.chat_title),
        },
        user: RuntimeLlmRequestUserData {
            user_id: input.user.id,
            full_name: non_empty(&input.user.full_name),
        },
        message: RuntimeLlmRequestMessageData {
            message_id: input.message.id,
        },
        gen_config: RuntimeLlmGenConfigData {
            max_output_tokens: input.max_output_tokens,
            ..RuntimeLlmGenConfigData::default()
        },
        docs: (!input.reference_context.is_empty()).then(|| json!(input.reference_context)),
        messages: (!input.history.is_empty()).then(|| json!(input.history)),
        raw_request,
        prompt_chars: raw_request_chars(input),
        prompt_messages: input.history.len().saturating_add(1).min(i32::MAX as usize) as i32,
        docs_chars,
        duration_ms,
        ..RuntimeLlmRequestData::default()
    }
}

fn llm_trace_matches_filter(
    trace: &RuntimeLlmRequestData,
    filter: &RuntimeLlmRequestsFilter,
) -> bool {
    (filter.source.is_empty()
        || trace.source == filter.source
        || trace.provider.as_deref() == Some(filter.source.as_str()))
        && (filter.model.is_empty() || trace.model.as_deref() == Some(filter.model.as_str()))
        && filter
            .chat_id
            .is_none_or(|chat_id| trace.chat.chat_id == chat_id)
        && filter
            .user_id
            .is_none_or(|user_id| trace.user.user_id == user_id)
        && (filter.q.is_empty() || llm_trace_matches_query(trace, &filter.q))
}

fn llm_trace_matches_query(trace: &RuntimeLlmRequestData, q: &str) -> bool {
    [
        trace.provider.as_deref().unwrap_or_default(),
        trace.request_kind.as_deref().unwrap_or_default(),
        &trace.source,
        trace.mode.as_deref().unwrap_or_default(),
        trace.flow.as_deref().unwrap_or_default(),
        trace.model.as_deref().unwrap_or_default(),
        trace.chat.chat_title.as_deref().unwrap_or_default(),
        trace.user.full_name.as_deref().unwrap_or_default(),
        trace
            .result
            .response_text_preview
            .as_deref()
            .unwrap_or_default(),
    ]
    .into_iter()
    .any(|field| contains_fold(field, q))
        || trace
            .raw_request
            .as_ref()
            .is_some_and(|value| contains_fold(&value.to_string(), q))
        || trace
            .raw_response
            .as_ref()
            .is_some_and(|value| contains_fold(&value.to_string(), q))
}

fn to_json_value<T: serde::Serialize>(value: &T) -> Option<Value> {
    serde_json::to_value(value).ok()
}

fn raw_request_chars(input: &DialogInput) -> i32 {
    serde_json::to_vec(input)
        .map(|bytes| bytes.len().min(i32::MAX as usize) as i32)
        .unwrap_or_default()
}

fn response_preview(output: &DialogOutput) -> String {
    compact_preview(
        if output.answer.trim().is_empty() {
            &output.response
        } else {
            &output.answer
        },
        GO_RESPONSE_PREVIEW_SIZE,
    )
}

fn trace_response_preview(
    output: &DialogOutput,
    artifact: Option<&DialogTraceArtifacts>,
) -> String {
    artifact
        .and_then(|artifact| artifact.raw_response.as_ref())
        .and_then(trace_response_text)
        .map(|text| compact_preview(&text, GO_RESPONSE_PREVIEW_SIZE))
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| response_preview(output))
}

fn trace_response_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => non_empty(text),
        Value::Array(items) => items.iter().find_map(trace_response_text),
        Value::Object(map) => {
            for key in ["content", "text", "reasoning"] {
                if let Some(text) = map.get(key).and_then(Value::as_str).and_then(non_empty) {
                    return Some(text);
                }
            }
            for key in [
                "choices",
                "candidates",
                "message",
                "delta",
                "parts",
                "output",
                "response",
                "result",
                "data",
            ] {
                if let Some(text) = map.get(key).and_then(trace_response_text) {
                    return Some(text);
                }
            }
            None
        }
        _ => None,
    }
}

fn compact_preview(value: &str, limit: usize) -> String {
    let mut out = String::new();
    for part in value.split_whitespace() {
        if out.is_empty() {
            out.push_str(part);
        } else {
            out.push(' ');
            out.push_str(part);
        }
        if out.chars().count() >= limit {
            return out.chars().take(limit).collect();
        }
    }
    out
}

fn elapsed_ms(started: Instant) -> i32 {
    started.elapsed().as_millis().min(i32::MAX as u128) as i32
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn contains_fold(value: &str, needle: &str) -> bool {
    value.to_lowercase().contains(&needle.to_lowercase())
}

fn format_ts(ts: OffsetDateTime) -> String {
    ts.format(&Rfc3339).unwrap_or_else(|_| ts.to_string())
}

#[cfg(test)]
mod tests {
    use openplotva_dialog::{DialogContext, DialogMessage, DialogUser};

    use super::*;

    #[test]
    fn runtime_llm_trace_buffer_lists_newest_and_filters_like_go() {
        let buffer = RuntimeLlmTraceBuffer::new(2);
        buffer.record(RuntimeLlmRequestData {
            provider: Some("aifarm".to_owned()),
            source: "dialog".to_owned(),
            model: Some("old".to_owned()),
            chat: RuntimeLlmRequestChatData {
                chat_id: 1,
                ..RuntimeLlmRequestChatData::default()
            },
            user: RuntimeLlmRequestUserData {
                user_id: 7,
                ..RuntimeLlmRequestUserData::default()
            },
            result: RuntimeLlmRequestResultData {
                response_text_preview: Some("old answer".to_owned()),
                ..RuntimeLlmRequestResultData::default()
            },
            ..RuntimeLlmRequestData::default()
        });
        buffer.record(RuntimeLlmRequestData {
            provider: Some("aifarm".to_owned()),
            source: "dialog".to_owned(),
            model: Some("new".to_owned()),
            raw_response: Some(json!({"content": "needle"})),
            chat: RuntimeLlmRequestChatData {
                chat_id: 2,
                ..RuntimeLlmRequestChatData::default()
            },
            user: RuntimeLlmRequestUserData {
                user_id: 7,
                ..RuntimeLlmRequestUserData::default()
            },
            ..RuntimeLlmRequestData::default()
        });

        let traces = buffer
            .llm_requests(RuntimeLlmRequestsFilter {
                q: "needle".to_owned(),
                source: "aifarm".to_owned(),
                user_id: Some(7),
                limit: 100,
                ..RuntimeLlmRequestsFilter::default()
            })
            .expect("trace list");

        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].model.as_deref(), Some("new"));
        assert_eq!(traces[0].id, 2);

        buffer.clear();
        assert!(
            buffer
                .llm_requests(RuntimeLlmRequestsFilter {
                    limit: 100,
                    ..RuntimeLlmRequestsFilter::default()
                })
                .expect("trace list after clear")
                .is_empty()
        );

        buffer.record(RuntimeLlmRequestData {
            source: "dialog".to_owned(),
            ..RuntimeLlmRequestData::default()
        });
        let traces = buffer
            .llm_requests(RuntimeLlmRequestsFilter {
                limit: 100,
                ..RuntimeLlmRequestsFilter::default()
            })
            .expect("trace list after rerecord");
        assert_eq!(traces[0].id, 3);
    }

    #[test]
    fn dialog_trace_shape_preserves_runtime_api_contract() {
        let input = DialogInput {
            context: DialogContext {
                chat_id: -100,
                thread_id: Some(5),
                chat_title: "Plotva Lab".to_owned(),
                ..DialogContext::default()
            },
            user: DialogUser {
                id: 7,
                full_name: "Alice".to_owned(),
            },
            message: DialogMessage {
                id: 77,
                text: "hello".to_owned(),
                ..DialogMessage::default()
            },
            model: "model-a".to_owned(),
            max_output_tokens: 512,
            ..DialogInput::default()
        };
        let output = DialogOutput {
            provider: "aifarm".to_owned(),
            answer: " hello   world ".to_owned(),
            trace: Some(openplotva_dialog::DialogTraceArtifacts {
                request_kind: "gemini.generateContent".to_owned(),
                raw_request: Some(json!({"contents": [{"role": "user"}]})),
                raw_response: Some(json!({"candidates": [{"content": "needle"}]})),
                resolved_cache_content: Some(json!({
                    "use_case": "chat_core_multi_turn",
                    "name": "cachedContents/chat-core-1",
                    "status": "hit"
                })),
                transport: Some(json!({"job_id": "job-1"})),
                inference_params: Some(json!({"tool_mode": "none"})),
                usage: Some(openplotva_dialog::DialogTraceUsage {
                    input_tokens: 11,
                    output_tokens: 7,
                    total_tokens: 18,
                    cached_tokens: 3,
                    thoughts_tokens: 2,
                    tool_use_prompt_tokens: 1,
                    traffic_type: "ON_DEMAND".to_owned(),
                }),
                timings: Some(json!({"generation_tps": 40.0})),
                ..openplotva_dialog::DialogTraceArtifacts::default()
            }),
            ..DialogOutput::default()
        };

        let trace = trace_from_dialog_success("aifarm", &input, &output, 42);

        assert_eq!(trace.provider.as_deref(), Some("aifarm"));
        assert_eq!(trace.source, "dialog");
        assert_eq!(trace.chat.chat_id, -100);
        assert_eq!(trace.user.user_id, 7);
        assert_eq!(trace.message.message_id, 77);
        assert_eq!(trace.gen_config.max_output_tokens, 512);
        assert_eq!(
            trace.request_kind.as_deref(),
            Some("gemini.generateContent")
        );
        assert_eq!(
            trace.result.response_text_preview.as_deref(),
            Some("needle")
        );
        assert_eq!(
            trace.raw_request,
            Some(json!({"contents": [{"role": "user"}]}))
        );
        assert_eq!(
            trace.raw_response,
            Some(json!({"candidates": [{"content": "needle"}]}))
        );
        assert_eq!(
            trace.resolved_cache_content,
            Some(json!({
                "use_case": "chat_core_multi_turn",
                "name": "cachedContents/chat-core-1",
                "status": "hit"
            }))
        );
        assert_eq!(trace.transport, Some(json!({"job_id": "job-1"})));
        assert_eq!(trace.inference_params, Some(json!({"tool_mode": "none"})));
        assert_eq!(trace.timings, Some(json!({"generation_tps": 40.0})));
        assert_eq!(
            trace.usage,
            Some(json!({
                "input_tokens": 11,
                "output_tokens": 7,
                "total_tokens": 18,
                "cached_tokens": 3,
                "thoughts_tokens": 2,
                "tool_use_prompt_tokens": 1,
                "traffic_type": "ON_DEMAND"
            }))
        );
    }

    #[test]
    fn dialog_trace_events_record_each_provider_request_like_go() {
        let buffer = RuntimeLlmTraceBuffer::new(8);
        let input = DialogInput {
            context: DialogContext {
                chat_id: -100,
                thread_id: Some(5),
                chat_title: "Plotva Lab".to_owned(),
                ..DialogContext::default()
            },
            user: DialogUser {
                id: 7,
                full_name: "Alice".to_owned(),
            },
            message: DialogMessage {
                id: 77,
                text: "hello".to_owned(),
                ..DialogMessage::default()
            },
            model: "fallback-model".to_owned(),
            max_output_tokens: 512,
            ..DialogInput::default()
        };
        let output = DialogOutput {
            provider: "aifarm".to_owned(),
            answer: "final answer".to_owned(),
            trace_events: vec![
                openplotva_dialog::DialogTraceArtifacts {
                    provider: "aifarm".to_owned(),
                    request_kind: "openai.chat.completions".to_owned(),
                    source: "aifarm".to_owned(),
                    mode: "tools".to_owned(),
                    flow: "dialog".to_owned(),
                    iteration: 1,
                    model: "model-a".to_owned(),
                    raw_response: Some(
                        json!({"choices": [{"message": {"content": "tool please"}}]}),
                    ),
                    prompt_chars: 111,
                    prompt_messages: 3,
                    docs_chars: 9,
                    ..openplotva_dialog::DialogTraceArtifacts::default()
                },
                openplotva_dialog::DialogTraceArtifacts {
                    provider: "aifarm".to_owned(),
                    request_kind: "openai.chat.completions".to_owned(),
                    source: "aifarm".to_owned(),
                    mode: "tools".to_owned(),
                    flow: "dialog".to_owned(),
                    iteration: 2,
                    model: "model-a".to_owned(),
                    raw_response: Some(
                        json!({"choices": [{"message": {"content": "final answer"}}]}),
                    ),
                    prompt_chars: 222,
                    prompt_messages: 5,
                    docs_chars: 9,
                    ..openplotva_dialog::DialogTraceArtifacts::default()
                },
            ],
            ..DialogOutput::default()
        };

        record_dialog_success_traces(&buffer, "aifarm", &input, &output, 99);

        let traces = buffer
            .llm_requests(RuntimeLlmRequestsFilter {
                limit: 10,
                ..RuntimeLlmRequestsFilter::default()
            })
            .expect("trace list");
        assert_eq!(traces.len(), 2);
        assert_eq!(traces[0].iteration, 2);
        assert_eq!(traces[0].source, "aifarm");
        assert_eq!(traces[0].mode.as_deref(), Some("tools"));
        assert_eq!(traces[0].model.as_deref(), Some("model-a"));
        assert_eq!(traces[0].prompt_chars, 222);
        assert_eq!(traces[0].prompt_messages, 5);
        assert_eq!(traces[0].docs_chars, 9);
        assert_eq!(
            traces[0].result.response_text_preview.as_deref(),
            Some("final answer")
        );
        assert_eq!(traces[1].iteration, 1);
        assert_eq!(
            traces[1].result.response_text_preview.as_deref(),
            Some("tool please")
        );
    }

    #[test]
    fn dialog_trace_errors_record_provider_artifacts_and_error_text() {
        let buffer = RuntimeLlmTraceBuffer::new(8);
        let input = DialogInput {
            context: DialogContext {
                chat_id: -100,
                thread_id: Some(5),
                chat_title: "Plotva Lab".to_owned(),
                ..DialogContext::default()
            },
            user: DialogUser {
                id: 7,
                full_name: "Alice".to_owned(),
            },
            message: DialogMessage {
                id: 77,
                text: "hello".to_owned(),
                ..DialogMessage::default()
            },
            model: "fallback-model".to_owned(),
            max_output_tokens: 512,
            ..DialogInput::default()
        };
        let error = openplotva_dialog::DialogTraceError::new(
            Box::new(std::io::Error::other("provider failed")),
            vec![openplotva_dialog::DialogTraceArtifacts {
                provider: "aifarm".to_owned(),
                request_kind: "openai.chat.completions".to_owned(),
                source: "aifarm".to_owned(),
                mode: "tools".to_owned(),
                flow: "dialog".to_owned(),
                iteration: 3,
                model: "model-a".to_owned(),
                raw_request: Some(json!({"messages": [{"role": "user"}]})),
                raw_response: Some(json!({"error": {"message": "capacity"}})),
                error: "aifarm provider unavailable: capacity".to_owned(),
                prompt_chars: 333,
                prompt_messages: 7,
                docs_chars: 11,
                ..openplotva_dialog::DialogTraceArtifacts::default()
            }],
        );

        record_dialog_error_traces(&buffer, "aifarm", &input, &error, &error.to_string(), 77);

        let traces = buffer
            .llm_requests(RuntimeLlmRequestsFilter {
                limit: 10,
                ..RuntimeLlmRequestsFilter::default()
            })
            .expect("trace list");
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].provider.as_deref(), Some("aifarm"));
        assert_eq!(
            traces[0].request_kind.as_deref(),
            Some("openai.chat.completions")
        );
        assert_eq!(traces[0].source, "aifarm");
        assert_eq!(traces[0].mode.as_deref(), Some("tools"));
        assert_eq!(traces[0].iteration, 3);
        assert_eq!(traces[0].model.as_deref(), Some("model-a"));
        assert_eq!(
            traces[0].raw_request,
            Some(json!({"messages": [{"role": "user"}]}))
        );
        assert_eq!(
            traces[0].raw_response,
            Some(json!({"error": {"message": "capacity"}}))
        );
        assert_eq!(
            traces[0].result.error.as_deref(),
            Some("aifarm provider unavailable: capacity")
        );
        assert_eq!(traces[0].prompt_chars, 333);
        assert_eq!(traces[0].prompt_messages, 7);
        assert_eq!(traces[0].docs_chars, 11);
    }
}
