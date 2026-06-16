//! Composition-root wiring for the agent-loop engine: a registry of named
//! single-completion LLM clients, the `Reasoner`/`AgentTools` adapters over the
//! real AIFarm client and dialog tool box, and the search-agent profile.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openplotva_agent::{
    AgentBudgets, AgentError, AgentMessage, AgentProfile, AgentRole, AgentTools, Reasoner,
    ReasonerCall, ReasonerFuture, ReasonerReply, ToolDispatchFuture,
};
use openplotva_config::AppConfig;
use openplotva_dialog::{
    DialogToolbox, NativeToolCall, STEP_CRAWL_URL, STEP_WEB_SEARCH, ToolContext, ToolResult,
    ToolStep,
};
use openplotva_llm::aifarm::{
    AifarmHttpClient, ChatCompletionRequest, ChatMessage, CompletionResult, StatusUpdate,
};
use serde_json::{Value, json};

use crate::media::{agent_client_config_from_named_provider, aifarm_dialog_config_from_app_config};

/// The implicit provider name that always maps to the primary dialog config.
pub const CONVERSATIONAL_PROVIDER: &str = "conversational";

/// Default Discovery service the auto-registered `qwen-reasoner` provider targets.
pub const DEFAULT_QWEN_SERVICE_NAME: &str = "llm-openai-qwen35b-gguf";
/// Default model label sent to the qwen llama.cpp server (it serves one model and
/// is lenient about this field; override via `LLM_PROVIDERS_MODELS`).
pub const DEFAULT_QWEN_MODEL: &str = "qwen3.6-35b-a3b";

/// Reasoner orchestration prompt for the search agent.
pub const SEARCH_SYSTEM_PROMPT: &str =
    include_str!("../../../prompts/agentic/search_system.prompt");
/// Writer synthesis prompt for the search agent.
pub const SEARCH_SYNTHESIS_PROMPT: &str =
    include_str!("../../../prompts/agentic/search_synthesis.prompt");

/// A single-completion LLM client plus the request defaults for one provider.
#[derive(Clone)]
pub struct AgentProviderClient {
    pub client: AifarmHttpClient,
    pub model: String,
    pub include_reasoning: Option<bool>,
    pub enable_thinking: Option<bool>,
    pub temperature: Option<f64>,
    pub max_tokens: i32,
}

/// Name-keyed registry of providers selectable per agent profile.
#[derive(Clone, Default)]
pub struct AgentProviderRegistry {
    by_name: HashMap<String, Arc<AgentProviderClient>>,
}

impl AgentProviderRegistry {
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<AgentProviderClient>> {
        self.by_name.get(&normalize_name(name)).cloned()
    }

    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.by_name.contains_key(&normalize_name(name))
    }
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

/// Build the provider registry from config: always the `conversational` entry
/// (primary dialog config) plus one entry per `LLM_PROVIDERS_*` spec.
#[must_use]
pub fn build_agent_provider_registry(config: &AppConfig) -> AgentProviderRegistry {
    let mut by_name = HashMap::new();

    let dialog = aifarm_dialog_config_from_app_config(config);
    by_name.insert(
        CONVERSATIONAL_PROVIDER.to_owned(),
        Arc::new(AgentProviderClient {
            client: AifarmHttpClient::new(dialog.client),
            model: dialog.model,
            include_reasoning: dialog.include_reasoning,
            enable_thinking: dialog.enable_thinking,
            temperature: dialog.temperature,
            max_tokens: dialog.max_tokens,
        }),
    );

    for spec in &config.llm.providers {
        let client_config = agent_client_config_from_named_provider(config, spec);
        by_name.insert(
            normalize_name(&spec.name),
            Arc::new(AgentProviderClient {
                client: AifarmHttpClient::new(client_config),
                model: spec.model.clone(),
                include_reasoning: spec.include_reasoning,
                enable_thinking: spec.enable_thinking,
                temperature: spec.temperature,
                max_tokens: spec.max_tokens,
            }),
        );
    }

    // Auto-register the default qwen reasoner so the search agent works out of the
    // box; an explicit `LLM_PROVIDERS_*` entry of the same name takes precedence.
    let default_reasoner =
        normalize_name(openplotva_config::DEFAULT_AGENTIC_SEARCH_REASONER_PROVIDER);
    if !by_name.contains_key(&default_reasoner) {
        let spec = openplotva_config::NamedProviderConfig {
            name: openplotva_config::DEFAULT_AGENTIC_SEARCH_REASONER_PROVIDER.to_owned(),
            kind: openplotva_config::DEFAULT_LLM_PROVIDER_KIND.to_owned(),
            discovery_service_name: DEFAULT_QWEN_SERVICE_NAME.to_owned(),
            discovery_endpoint_name: config.llm.dialog.discovery_endpoint_name.clone(),
            model: DEFAULT_QWEN_MODEL.to_owned(),
            base_url: String::new(),
            url: String::new(),
            api_key: String::new(),
            include_reasoning: Some(false),
            enable_thinking: Some(false),
            max_tokens: openplotva_config::DEFAULT_LLM_PROVIDER_MAX_TOKENS,
            temperature: None,
            task_timeout_seconds: openplotva_config::DEFAULT_LLM_PROVIDER_TASK_TIMEOUT_SECONDS,
        };
        let client_config = agent_client_config_from_named_provider(config, &spec);
        by_name.insert(
            default_reasoner,
            Arc::new(AgentProviderClient {
                client: AifarmHttpClient::new(client_config),
                model: spec.model.clone(),
                include_reasoning: spec.include_reasoning,
                enable_thinking: spec.enable_thinking,
                temperature: spec.temperature,
                max_tokens: spec.max_tokens,
            }),
        );
    }

    AgentProviderRegistry { by_name }
}

/// Resolved search-agent settings (prompts + budgets + default providers), built
/// once from config so the worker needs no `AppConfig` reference.
#[derive(Clone)]
pub struct SearchAgentSettings {
    pub enabled: bool,
    pub system_prompt: String,
    pub synthesis_prompt: String,
    pub reasoner_provider: String,
    pub writer_provider: String,
    pub budgets: AgentBudgets,
    pub reasoner_max_tokens: i32,
    pub writer_max_tokens: i32,
}

impl SearchAgentSettings {
    #[must_use]
    pub fn from_app_config(
        config: &AppConfig,
        system_prompt: String,
        synthesis_prompt: String,
    ) -> Self {
        let search = &config.llm.agentic.search;
        let max_tool_calls = search.max_searches.saturating_add(search.max_crawls).max(1);
        let reasoner_provider = if search.reasoner_provider.trim().is_empty() {
            openplotva_config::DEFAULT_AGENTIC_SEARCH_REASONER_PROVIDER.to_owned()
        } else {
            search.reasoner_provider.clone()
        };
        let writer_provider = if search.writer_provider.trim().is_empty() {
            CONVERSATIONAL_PROVIDER.to_owned()
        } else {
            search.writer_provider.clone()
        };
        Self {
            enabled: search.enabled,
            system_prompt,
            synthesis_prompt,
            reasoner_provider,
            writer_provider,
            budgets: AgentBudgets {
                max_steps: u32::try_from(search.max_steps.max(1)).unwrap_or(8),
                max_total_tokens: u64::try_from(search.max_total_tokens.max(0)).unwrap_or(0),
                max_wall_ms: u64::try_from(search.wall_timeout_seconds.max(0))
                    .unwrap_or(0)
                    .saturating_mul(1000),
                max_tool_calls: u32::try_from(max_tool_calls).unwrap_or(7),
                max_tool_errors: 3,
            },
            reasoner_max_tokens: 2048,
            writer_max_tokens: 4096,
        }
    }

    /// Build the engine profile for one run with the resolved provider models.
    #[must_use]
    pub fn profile(&self, reasoner_model: String, writer_model: String) -> AgentProfile {
        AgentProfile {
            id: "search".to_owned(),
            system_prompt: self.system_prompt.clone(),
            allowed_tools: vec![STEP_WEB_SEARCH.to_owned(), STEP_CRAWL_URL.to_owned()],
            reasoner_model,
            writer_model,
            budgets: self.budgets,
            reasoner_max_tokens: self.reasoner_max_tokens,
            writer_max_tokens: self.writer_max_tokens,
        }
    }
}

/// `Reasoner` adapter that performs one chat round-trip via the AIFarm client.
pub struct AifarmReasoner {
    provider: Arc<AgentProviderClient>,
}

impl AifarmReasoner {
    #[must_use]
    pub fn new(provider: Arc<AgentProviderClient>) -> Self {
        Self { provider }
    }
}

impl Reasoner for AifarmReasoner {
    fn complete<'a>(&'a self, call: ReasonerCall) -> ReasonerFuture<'a> {
        Box::pin(async move {
            let request = build_request(&self.provider, &call, true);
            let mut sink = |_status: StatusUpdate| {};
            let result = self
                .provider
                .client
                .complete(request, &mut sink)
                .await
                .map_err(|error| AgentError::Reasoner(error.to_string()))?;
            parse_reply(&result)
        })
    }
}

/// Run a single no-tools completion on a writer provider to synthesize the final
/// answer from the gathered evidence.
pub async fn synthesize_answer(
    provider: &AgentProviderClient,
    model: &str,
    max_tokens: i32,
    system_prompt: &str,
    user_content: &str,
) -> Result<String, AgentError> {
    let call = ReasonerCall {
        model: model.to_owned(),
        max_tokens,
        messages: vec![
            AgentMessage::new(AgentRole::System, system_prompt.to_owned()),
            AgentMessage::new(AgentRole::User, user_content.to_owned()),
        ],
        tools: Vec::new(),
    };
    let request = build_request(provider, &call, false);
    let mut sink = |_status: StatusUpdate| {};
    let result = provider
        .client
        .complete(request, &mut sink)
        .await
        .map_err(|error| AgentError::Reasoner(error.to_string()))?;
    let reply = parse_reply(&result)?;
    Ok(reply.text)
}

/// `AgentTools` adapter over the shared dialog tool box. Tool/transport failures
/// become recoverable failed `ToolResult`s so the loop can react and continue.
pub struct AppAgentTools {
    toolbox: Arc<dyn DialogToolbox>,
}

impl AppAgentTools {
    #[must_use]
    pub fn new(toolbox: Arc<dyn DialogToolbox>) -> Self {
        Self { toolbox }
    }
}

impl AgentTools for AppAgentTools {
    fn dispatch<'a>(&'a self, _ctx: ToolContext, step: ToolStep) -> ToolDispatchFuture<'a> {
        Box::pin(async move {
            let outcome = match step.step.as_str() {
                STEP_WEB_SEARCH => self.toolbox.web_search(step.query.clone()).await,
                STEP_CRAWL_URL => self.toolbox.crawl_url(step.url.clone()).await,
                other => {
                    return Ok(ToolResult::failed(
                        "tool_unsupported",
                        format!("agent tool `{other}` is not supported"),
                    ));
                }
            };
            Ok(outcome.unwrap_or_else(|error| ToolResult::failed("tool_error", error.to_string())))
        })
    }
}

/// Current unix time in milliseconds for budget accounting.
#[must_use]
pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn build_request(
    provider: &AgentProviderClient,
    call: &ReasonerCall,
    with_tools: bool,
) -> ChatCompletionRequest {
    let messages = call.messages.iter().map(to_chat_message).collect();
    let max_tokens = if call.max_tokens > 0 {
        call.max_tokens
    } else {
        provider.max_tokens
    };
    let mut request = ChatCompletionRequest {
        model: call.model.clone(),
        messages,
        max_tokens,
        temperature: provider.temperature,
        include_reasoning: provider.include_reasoning,
        ..ChatCompletionRequest::default()
    };
    if with_tools {
        let tools: Vec<Value> = call
            .tools
            .iter()
            .filter_map(|tool| serde_json::to_value(tool).ok())
            .collect();
        if !tools.is_empty() {
            request.tools = tools;
            request.tool_choice = Some(json!("auto"));
            request.parallel_tool_calls = Some(false);
        }
    }
    if let Some(enable) = provider.enable_thinking {
        request.chat_template_kwargs = Some(json!({ "enable_thinking": enable }));
    }
    request
}

fn to_chat_message(message: &AgentMessage) -> ChatMessage {
    match message.role {
        AgentRole::Tool => {
            let name = message.tool_name.as_deref().unwrap_or("tool");
            ChatMessage {
                role: "user".to_owned(),
                content: format!("Observation from tool `{name}`:\n{}", message.content),
                ..ChatMessage::default()
            }
        }
        role => ChatMessage {
            role: chat_role(role).to_owned(),
            content: message.content.clone(),
            ..ChatMessage::default()
        },
    }
}

fn chat_role(role: AgentRole) -> &'static str {
    match role {
        AgentRole::System => "system",
        AgentRole::User => "user",
        AgentRole::Assistant | AgentRole::Tool => "assistant",
    }
}

fn parse_reply(result: &CompletionResult) -> Result<ReasonerReply, AgentError> {
    let Some(response) = &result.response else {
        return Err(AgentError::Reasoner(format!(
            "empty response body (status {})",
            result.status_code
        )));
    };
    let message = response
        .get("choices")
        .and_then(|choices| choices.get(0))
        .and_then(|choice| choice.get("message"));
    let text = message
        .and_then(|message| message.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let tool_calls = message
        .and_then(|message| message.get("tool_calls"))
        .and_then(Value::as_array)
        .map(|calls| {
            calls
                .iter()
                .filter_map(|call| serde_json::from_value::<NativeToolCall>(call.clone()).ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let usage = response.get("usage");
    let prompt_tokens = usage
        .and_then(|usage| usage.get("prompt_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completion_tokens = usage
        .and_then(|usage| usage.get("completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let (prompt_tokens, completion_tokens) = if prompt_tokens == 0 && completion_tokens == 0 {
        // Fallback estimate when the backend omits usage, so budgets still trip.
        (0, u64::try_from(text.len() / 4).unwrap_or(0))
    } else {
        (prompt_tokens, completion_tokens)
    };

    Ok(ReasonerReply {
        text,
        tool_calls,
        prompt_tokens,
        completion_tokens,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completion(response: Value) -> CompletionResult {
        CompletionResult {
            job_id: "j".to_owned(),
            status_code: 200,
            raw_body: String::new(),
            response: Some(response),
        }
    }

    #[test]
    fn parses_tool_call_and_usage() {
        let result = completion(json!({
            "choices": [{
                "message": {
                    "content": "",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": { "name": "web_search", "arguments": "{\"query\":\"rust\"}" }
                    }]
                }
            }],
            "usage": { "prompt_tokens": 12, "completion_tokens": 5 }
        }));
        let reply = parse_reply(&result).expect("reply");
        assert_eq!(reply.tool_calls.len(), 1);
        assert_eq!(reply.tool_calls[0].function.name, "web_search");
        assert_eq!(reply.prompt_tokens, 12);
        assert_eq!(reply.completion_tokens, 5);
    }

    #[test]
    fn parses_final_text_and_estimates_tokens_without_usage() {
        let result = completion(json!({
            "choices": [{ "message": { "content": "final answer text" } }]
        }));
        let reply = parse_reply(&result).expect("reply");
        assert!(reply.tool_calls.is_empty());
        assert_eq!(reply.text, "final answer text");
        assert!(reply.completion_tokens > 0);
    }

    #[test]
    fn tool_role_is_rendered_as_user_observation() {
        let message = AgentMessage {
            role: AgentRole::Tool,
            content: "results".to_owned(),
            tool_name: Some("web_search".to_owned()),
        };
        let chat = to_chat_message(&message);
        assert_eq!(chat.role, "user");
        assert!(chat.content.contains("web_search"));
        assert!(chat.content.contains("results"));
    }

    #[test]
    fn registry_auto_registers_qwen_reasoner_by_default() {
        let config =
            openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig::default())
                .expect("default config");
        let registry = build_agent_provider_registry(&config);
        assert!(registry.contains(CONVERSATIONAL_PROVIDER));
        assert!(registry.contains(openplotva_config::DEFAULT_AGENTIC_SEARCH_REASONER_PROVIDER));
    }
}
