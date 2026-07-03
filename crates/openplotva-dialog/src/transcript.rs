//! In-session transcript types for the dialog session engine.
//!
//! One dialog turn is becoming an agent session that issues several
//! single-shot chat steps. The transcript carries what happened inside the
//! session so far — assistant messages with their tool calls, tool results
//! keyed by call id, and chat messages injected mid-session — in a
//! provider-neutral shape. Providers map it onto their wire format (native
//! assistant/tool roles for OpenAI-compatible endpoints, plain-text context
//! blocks for tool-less echelons).

use serde_json::Value;

use crate::{DialogInput, DialogTraceArtifacts, ToolStep};

/// One tool call as recorded in the session transcript. `arguments` is the
/// JSON value the model produced (object or encoded string) so the call
/// round-trips into the next request byte-comparably.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// One in-session message. Ordering in the transcript is chronological.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionMessage {
    /// A completed assistant step: optional visible text plus the tool calls
    /// it issued (empty for a text-only intermediate recorded mid-session).
    Assistant {
        text: String,
        tool_calls: Vec<SessionToolCall>,
    },
    /// The result of one executed tool call, linked by `tool_call_id`.
    ToolResult {
        tool_call_id: String,
        name: String,
        /// JSON-encoded `ToolResult` body the model reads.
        content: String,
    },
    /// A chat message injected into the running session, pre-rendered with the
    /// same body format as history user turns (sender attribution included).
    InjectedUser { rendered: String },
}

/// Tool exposure for one chat step.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ToolsMode {
    /// Full native tool calling: pre-encoded OpenAI tool definitions.
    Native(Vec<Value>),
    /// The session is tool-capable but THIS step must produce text only
    /// (forced-final pass, tool-less fallback echelon). The tool-aware system
    /// prompt stays unchanged — swapping it would invalidate the prompt-cache
    /// prefix — only the request-level tool list is omitted.
    FinalOnly,
    /// No tools at all for the whole session (guest mode, `disable_tools`);
    /// the system prompt renders without the tool contract.
    #[default]
    Disabled,
}

/// Inputs of one single-shot chat step.
#[derive(Clone, Debug)]
pub struct ChatStepRequest {
    /// Materialized dialog input (context, persona, history, shield, memory).
    pub input: DialogInput,
    /// In-session messages so far; empty on the first iteration.
    pub transcript: Vec<SessionMessage>,
    pub tools: ToolsMode,
    /// 1-based session iteration, used for trace/telemetry tagging.
    pub iteration: usize,
}

/// One tool call decoded from a step response, ready for dispatch: the parsed
/// [`ToolStep`] plus the wire-level id that links its result back into the
/// transcript. `salvaged` marks calls recovered from assistant text by the
/// content parsers rather than the native `tool_calls` array.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChatStepToolCall {
    pub id: String,
    pub step: ToolStep,
    pub salvaged: bool,
}

/// Output of one single-shot chat step.
#[derive(Clone, Debug, Default)]
pub struct ChatStepOutput {
    pub provider: String,
    pub model: String,
    /// Assistant text content; may be empty when the step only calls tools.
    pub text: String,
    pub tool_calls: Vec<ChatStepToolCall>,
    pub trace: Option<DialogTraceArtifacts>,
}
