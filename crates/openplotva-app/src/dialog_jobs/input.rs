//! Dialog input materialization: job params, chat settings, persona, history
//! merge, memory/shield context, and tool-call history persistence.

use std::{cmp::Ordering, collections::HashSet, fmt, sync::Arc};

use openplotva_core::{
    ChatMessageMeta, ChatSettings, SENDER_TYPE_CHANNEL, SENDER_TYPE_SAME_CHAT, SENDER_TYPE_USER,
    ToolCall, filter_non_terminator_tool_calls,
};
use openplotva_dialog::{
    CapturedMemory, DialogContext, DialogInput, DialogMessage, DialogUser, HistoryMessage,
    MESSAGE_KIND_TEXT, MESSAGE_KIND_TOOL_REQUEST, MESSAGE_KIND_TOOL_RESPONSE, Persona,
    PersonaSnapshot, ROLE_MODEL, ROLE_TOOL, ROLE_USER, SettingKv, TurnContextArtifact,
    daily_persona_for_unix_timestamp, filter_dialog_tool_calls_for_history,
    is_dialog_history_noise_tool_call_name,
};
use openplotva_memory::{CARD_STATUS_COMPETING, format_context as format_memory_context};
use openplotva_shield::{Options as ShieldOptions, SearchRequest as ShieldSearchRequest};
use openplotva_storage::{
    DialogMemoryChatMeta, PostgresChatSettingsStore, PostgresHistoryStore, PostgresMemoryStore,
    PostgresShieldStore, PostgresVirtualMessageStore,
};
use openplotva_taskman::DialogJobParams;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    asr::DialogAsrInputMaterializer,
    dialog_context::{
        build_dialog_shield_query_text, dialog_memory_retrieval_request,
        dialog_reference_context_from_memory,
    },
    memory_runtime::{EmbeddingProvider, memory_retrieval_query_task},
    vision::DialogVisionInputMaterializer,
};

use super::{
    DIALOG_HISTORY_FETCH_LIMIT, DIALOG_HISTORY_TTL, DialogInputMaterializerFuture,
    DialogToolCallHistoryFuture,
};

pub trait DialogToolCallHistoryStore {
    /// Error returned by concrete storage.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Upsert filtered dialog tool calls for one base text message.
    fn upsert_dialog_tool_calls<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        tool_calls: &'a [ToolCall],
    ) -> DialogToolCallHistoryFuture<'a, Self::Error>;
}

/// No-op tool history store used by narrow tests and unconnected wrappers.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopDialogToolCallHistoryStore;

pub trait DialogInputMaterializer {
    /// Materialize one provider input. Errors should be fail-open inside the implementation,
    fn materialize_dialog_input<'a>(
        &'a self,
        params: &'a DialogJobParams,
        now: OffsetDateTime,
    ) -> DialogInputMaterializerFuture<'a>;
}

/// Current-message-only fallback used by narrow tests and unconnected slices.
#[derive(Clone, Copy, Debug, Default)]
pub struct BasicDialogInputMaterializer;

impl DialogInputMaterializer for BasicDialogInputMaterializer {
    fn materialize_dialog_input<'a>(
        &'a self,
        params: &'a DialogJobParams,
        now: OffsetDateTime,
    ) -> DialogInputMaterializerFuture<'a> {
        Box::pin(async move { dialog_input_from_job_params_at(params, now) })
    }
}

/// Telegram bot identity used in dialog context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogBotIdentity {
    /// Bot user ID.
    pub id: i64,
    /// Bot first/display name.
    pub name: String,
}

impl DialogBotIdentity {
    #[must_use]
    pub fn new(id: i64, name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            id,
            name: non_blank_or(name.trim(), "Plotva"),
        }
    }
}

impl Default for DialogBotIdentity {
    fn default() -> Self {
        Self {
            id: 0,
            name: "Plotva".to_owned(),
        }
    }
}

/// Storage-backed dialog input materializer for the runtime worker.
#[derive(Clone, Debug)]
pub struct PostgresDialogInputMaterializer {
    settings: PostgresChatSettingsStore,
    identity: PostgresVirtualMessageStore,
    history: PostgresHistoryStore,
    memory: Option<PostgresMemoryStore>,
    memory_embedder: Option<Arc<dyn EmbeddingProvider>>,
    memory_embedding_dim: i32,
    shield: Option<PostgresShieldStore>,
    shield_embedder: Option<Arc<dyn EmbeddingProvider>>,
    shield_options: ShieldOptions,
    shield_history_tail_messages: usize,
    asr: Option<Arc<dyn DialogAsrInputMaterializer>>,
    vision: Option<Arc<dyn DialogVisionInputMaterializer>>,
    bot: DialogBotIdentity,
}

impl PostgresDialogInputMaterializer {
    /// Build the concrete runtime materializer.
    #[must_use]
    pub fn new(
        settings: PostgresChatSettingsStore,
        identity: PostgresVirtualMessageStore,
        history: PostgresHistoryStore,
        bot: DialogBotIdentity,
    ) -> Self {
        Self {
            settings,
            identity,
            history,
            memory: None,
            memory_embedder: None,
            memory_embedding_dim: 0,
            shield: None,
            shield_embedder: None,
            shield_options: ShieldOptions::default(),
            shield_history_tail_messages: 0,
            asr: None,
            vision: None,
            bot,
        }
    }

    #[must_use]
    pub fn with_memory_store(mut self, memory: PostgresMemoryStore) -> Self {
        self.memory = Some(memory);
        self
    }

    #[must_use]
    pub fn with_memory_embedder(
        mut self,
        embedder: Arc<dyn EmbeddingProvider>,
        embedding_dim: i32,
    ) -> Self {
        self.memory_embedder = Some(embedder);
        self.memory_embedding_dim = embedding_dim;
        self
    }

    #[must_use]
    pub fn with_shield_store(
        mut self,
        shield: PostgresShieldStore,
        options: ShieldOptions,
        history_tail_messages: usize,
    ) -> Self {
        self.shield = Some(shield);
        self.shield_options = options.with_defaults();
        self.shield_history_tail_messages = history_tail_messages;
        self
    }

    #[must_use]
    pub fn with_shield_embedder(mut self, embedder: Arc<dyn EmbeddingProvider>) -> Self {
        self.shield_embedder = Some(embedder);
        self
    }

    #[must_use]
    pub fn with_vision_materializer(
        mut self,
        vision: Arc<dyn DialogVisionInputMaterializer>,
    ) -> Self {
        self.vision = Some(vision);
        self
    }

    #[must_use]
    pub fn with_asr_materializer(mut self, asr: Arc<dyn DialogAsrInputMaterializer>) -> Self {
        self.asr = Some(asr);
        self
    }
}

impl DialogInputMaterializer for PostgresDialogInputMaterializer {
    fn materialize_dialog_input<'a>(
        &'a self,
        params: &'a DialogJobParams,
        now: OffsetDateTime,
    ) -> DialogInputMaterializerFuture<'a> {
        Box::pin(async move { self.materialize(params, now).await })
    }
}

impl DialogToolCallHistoryStore for NoopDialogToolCallHistoryStore {
    type Error = std::convert::Infallible;

    fn upsert_dialog_tool_calls<'a>(
        &'a self,
        _chat_id: i64,
        _message_id: i32,
        _tool_calls: &'a [ToolCall],
    ) -> DialogToolCallHistoryFuture<'a, Self::Error> {
        Box::pin(async { Ok(false) })
    }
}

impl DialogToolCallHistoryStore for PostgresHistoryStore {
    type Error = openplotva_storage::StorageError;

    fn upsert_dialog_tool_calls<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        tool_calls: &'a [ToolCall],
    ) -> DialogToolCallHistoryFuture<'a, Self::Error> {
        Box::pin(async move {
            self.upsert_tool_call_history(chat_id, message_id, tool_calls)
                .await
        })
    }
}

#[must_use]
pub fn dialog_job_has_payload(params: &DialogJobParams) -> bool {
    let meta = dialog_meta_from_value(&params.meta);
    !params.message_text.trim().is_empty()
        || !params.original_text.trim().is_empty()
        || !meta.message_type.trim().is_empty()
        || !meta.annotation.trim().is_empty()
        || !meta.vision_description.trim().is_empty()
        || !meta.attachments.is_empty()
}

#[must_use]
pub fn dialog_input_from_job_params_at(
    params: &DialogJobParams,
    now: OffsetDateTime,
) -> DialogInput {
    DialogInput {
        context: DialogContext {
            chat_id: params.chat_id,
            thread_id: params.thread_id,
            bot_name: "Plotva".to_owned(),
            ..DialogContext::default()
        },
        user: DialogUser {
            id: params.user_id,
            full_name: params.user_full_name.clone(),
        },
        message: DialogMessage {
            id: params.message_id,
            text: params.message_text.clone(),
            normalized: params.message_text.clone(),
            original_text: params.original_text.clone(),
            timestamp: Some(now),
            meta: dialog_meta_from_value(&params.meta),
            ..DialogMessage::default()
        },
        timestamp: Some(now),
        max_output_tokens: params.max_output_tokens,
        ..DialogInput::default()
    }
}

impl PostgresDialogInputMaterializer {
    async fn materialize(&self, params: &DialogJobParams, now: OffsetDateTime) -> DialogInput {
        let settings = self.load_settings(params.chat_id).await;
        let chat = self.load_chat(params.chat_id).await;
        let user = self.load_user(params.user_id).await;
        let history = self.load_history(params, now).await;
        let (reference_context, captured_memories) = self.load_reference_context(params).await;
        let shield_context = self.load_shield_context(params, &history).await;
        let mut input = dialog_input_from_materialized_context(
            params,
            now,
            &self.bot,
            Some(&settings),
            chat,
            user,
            history,
        );
        input.reference_context = reference_context;
        input.shield_context = shield_context;
        if let Some(asr) = self.asr.as_ref() {
            input = asr.materialize_dialog_asr_input(input, now).await;
        }
        if let Some(vision) = self.vision.as_ref() {
            input = vision.materialize_dialog_vision_input(input, now).await;
        }
        input.context_capture = Some(turn_context_artifact(&input, &settings, captured_memories));
        input
    }

    async fn load_settings(&self, chat_id: i64) -> ChatSettings {
        match self.settings.get_chat_settings(chat_id).await {
            Ok(Some(settings)) => settings,
            Ok(None) => ChatSettings::defaults(chat_id),
            Err(error) => {
                tracing::debug!(
                    %error,
                    chat_id,
                    "failed to load chat settings for dialog input; using defaults"
                );
                ChatSettings::defaults(chat_id)
            }
        }
    }

    async fn load_chat(&self, chat_id: i64) -> Option<openplotva_core::ChatState> {
        match self.settings.get_chat_state(chat_id).await {
            Ok(chat) => chat,
            Err(error) => {
                tracing::debug!(
                    %error,
                    chat_id,
                    "failed to load chat state for dialog input"
                );
                None
            }
        }
    }

    async fn load_memory_chat_meta(&self, chat_id: i64) -> DialogMemoryChatMeta {
        if chat_id == 0 {
            return DialogMemoryChatMeta::default();
        }
        match self.settings.get_dialog_memory_chat_meta(chat_id).await {
            Ok(Some(meta)) => meta,
            Ok(None) => DialogMemoryChatMeta::default(),
            Err(error) => {
                tracing::debug!(
                    %error,
                    chat_id,
                    "failed to load chat memory metadata for dialog input"
                );
                DialogMemoryChatMeta::default()
            }
        }
    }

    async fn load_user(&self, user_id: i64) -> Option<openplotva_core::UserState> {
        if user_id == 0 {
            return None;
        }
        match self.identity.get_user_state(user_id).await {
            Ok(user) => user,
            Err(error) => {
                tracing::debug!(
                    %error,
                    user_id,
                    "failed to load user state for dialog input"
                );
                None
            }
        }
    }

    async fn load_history(
        &self,
        params: &DialogJobParams,
        now: OffsetDateTime,
    ) -> Vec<HistoryMessage> {
        if params.chat_id == 0 {
            return Vec::new();
        }

        let thread_id = params.thread_id.unwrap_or_default();
        let chat_reset_at = self
            .history_reset_at(params.chat_id, 0)
            .await
            .unwrap_or(None)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let thread_reset_at = if thread_id == 0 {
            OffsetDateTime::UNIX_EPOCH
        } else {
            self.history_reset_at(params.chat_id, thread_id)
                .await
                .unwrap_or(None)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        };
        let ttl_cutoff = now - DIALOG_HISTORY_TTL;
        let chat_cutoff = max_offset_datetime(ttl_cutoff, chat_reset_at);
        let thread_cutoff = max_offset_datetime(ttl_cutoff, thread_reset_at);

        let chat_payloads = match self
            .history
            .recent_chat_history_payloads(
                params.chat_id,
                chat_cutoff,
                thread_id,
                thread_reset_at,
                DIALOG_HISTORY_FETCH_LIMIT,
            )
            .await
        {
            Ok(payloads) => payloads,
            Err(error) => {
                tracing::debug!(
                    %error,
                    chat_id = params.chat_id,
                    "failed to load chat history payloads for dialog input"
                );
                Vec::new()
            }
        };
        let thread_payloads = if thread_id == 0 {
            Vec::new()
        } else {
            match self
                .history
                .recent_thread_history_payloads(
                    params.chat_id,
                    thread_id,
                    thread_cutoff,
                    DIALOG_HISTORY_FETCH_LIMIT,
                )
                .await
            {
                Ok(payloads) => payloads,
                Err(error) => {
                    tracing::debug!(
                        %error,
                        chat_id = params.chat_id,
                        thread_id,
                        "failed to load thread history payloads for dialog input"
                    );
                    Vec::new()
                }
            }
        };

        merge_dialog_history_payloads(&chat_payloads, &thread_payloads, self.bot.id)
    }

    async fn load_reference_context(
        &self,
        params: &DialogJobParams,
    ) -> (Vec<String>, Vec<CapturedMemory>) {
        let Some(memory) = self.memory.as_ref() else {
            return (Vec::new(), Vec::new());
        };
        if params.message_text.trim().is_empty() {
            return (Vec::new(), Vec::new());
        }

        let meta = self.load_memory_chat_meta(params.chat_id).await;
        let Some(request) = dialog_memory_retrieval_request(
            params,
            meta.chat_type,
            meta.username,
            meta.active_usernames,
        ) else {
            return (Vec::new(), Vec::new());
        };

        let query_embedding = self.memory_query_embedding(&request.query, params).await;
        match memory
            .retrieve_with_vector(&request, query_embedding.as_ref())
            .await
        {
            Ok(memory) => {
                let captured = captured_memories_from_retrieval(&memory);
                (
                    dialog_reference_context_from_memory(&format_memory_context(&memory)),
                    captured,
                )
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    chat_id = params.chat_id,
                    user_id = params.user_id,
                    "memory retrieval failed for dialog input"
                );
                (Vec::new(), Vec::new())
            }
        }
    }

    async fn memory_query_embedding(
        &self,
        query: &str,
        params: &DialogJobParams,
    ) -> Option<openplotva_storage::PgEmbeddingVector> {
        let embedder = self.memory_embedder.as_ref()?;
        match embedder
            .embed_one(
                query,
                self.memory_embedding_dim,
                memory_retrieval_query_task(),
            )
            .await
        {
            Ok(embedding) => embedding,
            Err(error) => {
                tracing::warn!(
                    %error,
                    chat_id = params.chat_id,
                    user_id = params.user_id,
                    "memory query embedding failed; using lexical-only retrieval"
                );
                None
            }
        }
    }

    async fn load_shield_context(
        &self,
        params: &DialogJobParams,
        history: &[HistoryMessage],
    ) -> String {
        let Some(shield) = self.shield.as_ref() else {
            return String::new();
        };
        let query = build_dialog_shield_query_text(
            params,
            history,
            self.shield_options.query_max_chars,
            self.shield_history_tail_messages,
        );
        if query.trim().is_empty() {
            return String::new();
        }

        let query_embedding = self.shield_query_embedding(&query, params).await;
        match shield
            .search_with_vector(
                &ShieldSearchRequest {
                    query,
                    max_matches: self.shield_options.max_matches,
                    include_candidates: false,
                },
                &self.shield_options,
                query_embedding.as_ref(),
            )
            .await
        {
            Ok(result) => {
                if !result.matches.is_empty() {
                    tracing::info!(
                        chat_id = params.chat_id,
                        user_id = params.user_id,
                        message_id = params.message_id,
                        lexical_only = result.lexical_only,
                        matches = result.matches.len(),
                        "shield retrieval matched"
                    );
                }
                result.context
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    chat_id = params.chat_id,
                    user_id = params.user_id,
                    "shield retrieval failed for dialog input"
                );
                String::new()
            }
        }
    }

    async fn shield_query_embedding(
        &self,
        query: &str,
        params: &DialogJobParams,
    ) -> Option<openplotva_storage::PgEmbeddingVector> {
        let embedder = self.shield_embedder.as_ref()?;
        match embedder
            .embed_one(
                query,
                self.shield_options.embedding_dim,
                openplotva_shield::QUERY_EMBEDDING_TASK,
            )
            .await
        {
            Ok(embedding) => embedding,
            Err(error) => {
                tracing::warn!(
                    %error,
                    chat_id = params.chat_id,
                    user_id = params.user_id,
                    "shield query embedding failed; using lexical-only retrieval"
                );
                None
            }
        }
    }

    async fn history_reset_at(
        &self,
        chat_id: i64,
        thread_id: i32,
    ) -> Result<Option<OffsetDateTime>, openplotva_storage::StorageError> {
        self.history.history_reset_at(chat_id, thread_id).await
    }
}

#[must_use]
pub fn dialog_input_from_materialized_context(
    params: &DialogJobParams,
    now: OffsetDateTime,
    bot: &DialogBotIdentity,
    settings: Option<&ChatSettings>,
    chat: Option<openplotva_core::ChatState>,
    user: Option<openplotva_core::UserState>,
    history: Vec<HistoryMessage>,
) -> DialogInput {
    let mut input = dialog_input_from_job_params_at(params, now);
    input.context.bot_name = bot.name.clone();
    input.context.locale = dialog_locale(user.as_ref());
    input.context.chat_title = chat
        .and_then(|chat| chat.title)
        .map(|title| title.trim().to_owned())
        .filter(|title| !title.is_empty())
        .unwrap_or_default();
    input.persona = dialog_persona(params.chat_id, now, settings, &bot.name);
    input.history = history;
    let (reply_to_id, reply_to_name) =
        current_message_reply_context(&input.history, params.message_id);
    input.message.reply_to_id = reply_to_id;
    input.message.reply_to_name = reply_to_name;
    input.message.meta = dialog_message_meta(params, input.message.meta);
    input
}

pub(crate) async fn persist_dialog_tool_calls<Store>(
    store: &Store,
    params: &DialogJobParams,
    tool_calls: &[ToolCall],
) -> Result<bool, Store::Error>
where
    Store: DialogToolCallHistoryStore + Sync + ?Sized,
{
    if params.chat_id == 0 || params.message_id == 0 || tool_calls.is_empty() {
        return Ok(false);
    }
    let filtered = filter_dialog_tool_calls_for_history(tool_calls);
    if filtered.is_empty() {
        return Ok(false);
    }
    store
        .upsert_dialog_tool_calls(params.chat_id, params.message_id, &filtered)
        .await
}

fn dialog_meta_from_value(value: &serde_json::Value) -> ChatMessageMeta {
    if value.is_null() {
        return ChatMessageMeta::default();
    }
    serde_json::from_value(value.clone()).unwrap_or_default()
}

/// Map a recall result into the light, scored memory skeleton kept for the admin
/// Context X-ray (capped; previews trimmed). Full card bodies are not retained.
fn captured_memories_from_retrieval(
    memory: &openplotva_memory::RetrievedMemory,
) -> Vec<CapturedMemory> {
    memory
        .cards
        .iter()
        .take(40)
        .map(|card| CapturedMemory {
            card_id: card.id,
            salience: card.salience,
            confidence: card.confidence,
            card_type: card.card_type.trim().to_lowercase(),
            competing: card.status == CARD_STATUS_COMPETING,
            preview: card.fact_text.chars().take(120).collect(),
        })
        .collect()
}

fn chat_settings_context_kv(settings: &ChatSettings) -> Vec<SettingKv> {
    let yesno = |value: bool| if value { "yes" } else { "no" }.to_owned();
    let mut kv = Vec::new();
    if let Some(mood) = settings.mood_alignment.as_deref()
        && !mood.trim().is_empty()
    {
        kv.push(SettingKv {
            label: "mood".to_owned(),
            value: mood.to_owned(),
        });
    }
    kv.push(SettingKv {
        label: "custom persona".to_owned(),
        value: yesno(
            settings
                .custom_persona
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()),
        ),
    });
    kv.push(SettingKv {
        label: "reactivity".to_owned(),
        value: format!("{}%", settings.reactivity_percentage),
    });
    kv.push(SettingKv {
        label: "proactivity".to_owned(),
        value: format!("{}%", settings.proactivity_percentage),
    });
    kv.push(SettingKv {
        label: "text reply".to_owned(),
        value: yesno(settings.enable_global_text_reply),
    });
    kv.push(SettingKv {
        label: "draw reply".to_owned(),
        value: yesno(settings.enable_global_draw_reply),
    });
    kv.push(SettingKv {
        label: "profanity".to_owned(),
        value: yesno(settings.enable_profanity),
    });
    kv.push(SettingKv {
        label: "obscenifier".to_owned(),
        value: yesno(settings.enable_obscenifier),
    });
    kv
}

fn turn_context_artifact(
    input: &DialogInput,
    settings: &ChatSettings,
    memories: Vec<CapturedMemory>,
) -> TurnContextArtifact {
    let persona = PersonaSnapshot {
        name: input
            .persona
            .persona
            .as_ref()
            .map(|daily| daily.name.clone())
            .unwrap_or_default(),
        mood: input.persona.mood.clone(),
        custom: !input.persona.custom_persona.trim().is_empty(),
        profanity: input.persona.profanity,
        obscenifier: input.persona.obscenifier,
    };
    let reference_context_chars = input
        .reference_context
        .iter()
        .map(|line| line.chars().count())
        .sum::<usize>()
        .min(i32::MAX as usize) as i32;
    TurnContextArtifact {
        memories,
        persona: Some(persona),
        settings: chat_settings_context_kv(settings),
        history_len: i32::try_from(input.history.len()).unwrap_or(i32::MAX),
        tools_offered: !input.disable_tools,
        shield_on: !input.shield_context.trim().is_empty(),
        reference_context_chars,
    }
}

fn dialog_locale(user: Option<&openplotva_core::UserState>) -> String {
    user.and_then(|user| user.language_code.as_ref())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("ru")
        .to_owned()
}

pub(crate) fn dialog_persona(
    chat_id: i64,
    now: OffsetDateTime,
    settings: Option<&ChatSettings>,
    bot_name: &str,
) -> Persona {
    let daily_persona = || {
        let mut persona = daily_persona_for_unix_timestamp(chat_id, now.unix_timestamp());
        if persona.name.trim().is_empty() {
            persona.name = non_blank_or(bot_name.trim(), "Plotva");
        }
        persona
    };
    let Some(settings) = settings else {
        return Persona {
            mood: "neutral".to_owned(),
            persona: Some(daily_persona()),
            ..Persona::default()
        };
    };
    let custom_persona = settings
        .custom_persona
        .as_ref()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_default();
    Persona {
        mood: settings
            .mood_alignment
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("neutral")
            .to_owned(),
        persona: custom_persona.is_empty().then(daily_persona),
        custom_persona,
        profanity: settings.enable_profanity,
        obscenifier: settings.enable_obscenifier,
        reactivity: settings.reactivity_percentage,
        proactivity: settings.proactivity_percentage,
    }
}

fn dialog_message_meta(params: &DialogJobParams, mut meta: ChatMessageMeta) -> ChatMessageMeta {
    meta.tool_calls = filter_non_terminator_tool_calls(&meta.tool_calls);
    if meta.message_type.trim().is_empty() {
        meta.message_type = detect_dialog_message_type(&params.message_text, &meta);
    }
    if meta.sender_type.trim().is_empty() {
        meta.sender_type = SENDER_TYPE_USER.to_owned();
    }
    if meta.sender_id == 0 {
        meta.sender_id = params.user_id;
    }
    if meta.sender_name.trim().is_empty() {
        meta.sender_name = params.user_full_name.trim().to_owned();
    }
    meta
}

fn detect_dialog_message_type(message_text: &str, meta: &ChatMessageMeta) -> String {
    for attachment in &meta.attachments {
        if attachment.source.trim() != "message" {
            continue;
        }
        if !attachment.kind.trim().is_empty() {
            return attachment.kind.trim().to_owned();
        }
    }
    if !message_text.trim().is_empty() {
        return "text".to_owned();
    }
    "text".to_owned()
}

pub(crate) fn merge_dialog_history_payloads(
    chat_payloads: &[Vec<u8>],
    thread_payloads: &[Vec<u8>],
    bot_id: i64,
) -> Vec<HistoryMessage> {
    let mut merged = Vec::with_capacity(chat_payloads.len() + thread_payloads.len());
    let mut seen = HashSet::<String>::with_capacity(merged.capacity());
    append_unique_history_payloads(&mut merged, &mut seen, chat_payloads, true, bot_id);
    append_unique_history_payloads(&mut merged, &mut seen, thread_payloads, false, bot_id);
    merged.sort_by(history_message_newer);
    merged
}

fn append_unique_history_payloads(
    out: &mut Vec<HistoryMessage>,
    seen: &mut HashSet<String>,
    payloads: &[Vec<u8>],
    include_empty_key: bool,
    bot_id: i64,
) {
    for payload in payloads {
        let Some(message) = history_message_from_payload(payload, bot_id) else {
            continue;
        };
        let key = history_message_key(&message);
        if key.is_empty() && !include_empty_key {
            continue;
        }
        if !seen.insert(key) {
            continue;
        }
        out.push(message);
    }
}

fn history_message_from_payload(payload: &[u8], bot_id: i64) -> Option<HistoryMessage> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let object = value.as_object()?;
    let mut meta = object
        .get("meta")
        .and_then(|value| serde_json::from_value::<ChatMessageMeta>(value.clone()).ok())
        .unwrap_or_default();
    meta.tool_calls = filter_non_terminator_tool_calls(&meta.tool_calls);

    let tool_call = object
        .get("tool_call")
        .and_then(|value| serde_json::from_value::<ToolCall>(value.clone()).ok());
    if tool_call
        .as_ref()
        .is_some_and(|call| is_dialog_history_noise_tool_call_name(&call.name))
    {
        return None;
    }

    let kind = normalized_history_kind(
        string_field(object, "kind"),
        &tool_call,
        string_field(object, "role"),
    );
    let sender_id = non_zero_i64(meta.sender_id)
        .or_else(|| nested_i64(object, "from", "id"))
        .or_else(|| nested_i64(object, "sender_chat", "id"))
        .unwrap_or_default();
    if sender_id == 0 && tool_call.is_none() {
        return None;
    }
    if meta.sender_id == 0 {
        meta.sender_id = sender_id;
    }
    if meta.sender_type.trim().is_empty() {
        meta.sender_type = if object.contains_key("sender_chat") {
            history_payload_sender_chat_type(object)
        } else {
            SENDER_TYPE_USER.to_owned()
        };
    }
    if meta.sender_name.trim().is_empty() {
        meta.sender_name = history_payload_sender_name(object);
    }
    if meta.sender_username.trim().is_empty() {
        meta.sender_username = nested_string(object, "from", "username")
            .or_else(|| nested_string(object, "sender_chat", "username"))
            .unwrap_or_default();
    }

    let role = history_payload_role(&kind, string_field(object, "role"), sender_id, bot_id);
    let (reply_to_id, reply_to_name) = history_payload_reply_context(object);
    let name = if meta.sender_name.trim().is_empty() && role == ROLE_TOOL {
        tool_call
            .as_ref()
            .map(|call| call.name.trim().to_owned())
            .unwrap_or_default()
    } else {
        meta.sender_name.trim().to_owned()
    };

    Some(HistoryMessage {
        entry_id: string_field(object, "entry_id").trim().to_owned(),
        role,
        kind,
        name,
        text: string_field(object, "text"),
        original_text: string_field(object, "original_text"),
        timestamp: history_payload_timestamp(object),
        message_id: i32_field(object, "message_id"),
        thread_id: i32_field(object, "message_thread_id"),
        user_id: sender_id,
        reply_to_id,
        reply_to_name,
        meta,
        tool_call,
    })
}

fn normalized_history_kind(kind: String, tool_call: &Option<ToolCall>, role: String) -> String {
    if !kind.trim().is_empty() {
        return kind;
    }
    if tool_call.is_some() {
        if role == ROLE_TOOL {
            return MESSAGE_KIND_TOOL_RESPONSE.to_owned();
        }
        return MESSAGE_KIND_TOOL_REQUEST.to_owned();
    }
    MESSAGE_KIND_TEXT.to_owned()
}

fn history_payload_role(kind: &str, stored_role: String, sender_id: i64, bot_id: i64) -> String {
    match kind {
        MESSAGE_KIND_TOOL_RESPONSE => ROLE_TOOL.to_owned(),
        MESSAGE_KIND_TOOL_REQUEST => ROLE_MODEL.to_owned(),
        MESSAGE_KIND_TEXT => {
            if bot_id != 0 && sender_id == bot_id {
                ROLE_MODEL.to_owned()
            } else {
                ROLE_USER.to_owned()
            }
        }
        _ if stored_role == ROLE_TOOL => ROLE_TOOL.to_owned(),
        _ if bot_id != 0 && sender_id == bot_id => ROLE_MODEL.to_owned(),
        _ => ROLE_USER.to_owned(),
    }
}

fn history_payload_timestamp(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<OffsetDateTime> {
    object
        .get("timestamp")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
        .or_else(|| {
            object
                .get("date")
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| OffsetDateTime::from_unix_timestamp(value).ok())
        })
}

fn current_message_reply_context(history: &[HistoryMessage], message_id: i32) -> (i32, String) {
    if message_id == 0 {
        return (0, String::new());
    }
    history
        .iter()
        .find(|item| item.message_id == message_id && item.kind == MESSAGE_KIND_TEXT)
        .map(|item| (item.reply_to_id, item.reply_to_name.clone()))
        .unwrap_or_default()
}

fn history_payload_reply_context(
    object: &serde_json::Map<String, serde_json::Value>,
) -> (i32, String) {
    let Some(reply) = object
        .get("reply_to_message")
        .and_then(serde_json::Value::as_object)
    else {
        return (0, String::new());
    };
    (
        i32_field(reply, "message_id"),
        history_payload_sender_name(reply),
    )
}

fn history_payload_sender_name(object: &serde_json::Map<String, serde_json::Value>) -> String {
    nested_display_name(object, "from")
        .or_else(|| nested_string(object, "sender_chat", "title"))
        .unwrap_or_default()
}

fn history_payload_sender_chat_type(object: &serde_json::Map<String, serde_json::Value>) -> String {
    let sender_chat_id = nested_i64(object, "sender_chat", "id");
    let chat_id = nested_i64(object, "chat", "id");
    if sender_chat_id.is_some() && sender_chat_id == chat_id {
        SENDER_TYPE_SAME_CHAT.to_owned()
    } else {
        SENDER_TYPE_CHANNEL.to_owned()
    }
}

fn nested_display_name(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<String> {
    let first = nested_string(object, key, "first_name").unwrap_or_default();
    let last = nested_string(object, key, "last_name").unwrap_or_default();
    let name = format!("{} {}", first.trim(), last.trim())
        .trim()
        .to_owned();
    (!name.is_empty()).then_some(name)
}

fn history_message_newer(left: &HistoryMessage, right: &HistoryMessage) -> Ordering {
    match (left.timestamp, right.timestamp) {
        (Some(left), Some(right)) if left != right => return right.cmp(&left),
        (Some(_), None) => return Ordering::Less,
        (None, Some(_)) => return Ordering::Greater,
        _ => {}
    }
    right
        .message_id
        .cmp(&left.message_id)
        .then_with(|| history_kind_order(right).cmp(&history_kind_order(left)))
}

fn history_kind_order(message: &HistoryMessage) -> i32 {
    match message.kind.as_str() {
        MESSAGE_KIND_TOOL_RESPONSE => 3,
        MESSAGE_KIND_TOOL_REQUEST => 2,
        MESSAGE_KIND_TEXT => 1,
        _ => 0,
    }
}

fn history_message_key(message: &HistoryMessage) -> String {
    if !message.entry_id.trim().is_empty() {
        return message.entry_id.trim().to_owned();
    }
    if message.message_id != 0 {
        return message.message_id.to_string();
    }
    String::new()
}

fn max_offset_datetime(left: OffsetDateTime, right: OffsetDateTime) -> OffsetDateTime {
    left.max(right)
}

fn non_blank_or(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_owned()
    } else {
        value.trim().to_owned()
    }
}

fn string_field(object: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    object
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn i32_field(object: &serde_json::Map<String, serde_json::Value>, key: &str) -> i32 {
    object
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
}

fn nested_string(
    object: &serde_json::Map<String, serde_json::Value>,
    parent: &str,
    key: &str,
) -> Option<String> {
    object
        .get(parent)
        .and_then(serde_json::Value::as_object)
        .and_then(|nested| nested.get(key))
        .and_then(serde_json::Value::as_str)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn nested_i64(
    object: &serde_json::Map<String, serde_json::Value>,
    parent: &str,
    key: &str,
) -> Option<i64> {
    object
        .get(parent)
        .and_then(serde_json::Value::as_object)
        .and_then(|nested| nested.get(key))
        .and_then(serde_json::Value::as_i64)
}

fn non_zero_i64(value: i64) -> Option<i64> {
    (value != 0).then_some(value)
}

#[cfg(test)]
mod context_artifact_tests {
    use super::*;
    use openplotva_memory::{Card, RetrievedMemory};

    fn card(id: i64, salience: f64, competing: bool, fact: &str) -> Card {
        Card {
            id,
            salience,
            confidence: 0.7,
            card_type: "fact".to_owned(),
            status: if competing {
                CARD_STATUS_COMPETING.to_owned()
            } else {
                "active".to_owned()
            },
            fact_text: fact.to_owned(),
            ..Card::default()
        }
    }

    #[test]
    fn captured_memories_map_score_competing_and_trim_preview() {
        let long = "a".repeat(300);
        let memory = RetrievedMemory {
            cards: vec![
                card(1, 0.9, false, "ada likes tea"),
                card(2, 0.4, true, &long),
            ],
            ..RetrievedMemory::default()
        };
        let captured = captured_memories_from_retrieval(&memory);
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].card_id, 1);
        assert_eq!(captured[0].salience, 0.9);
        assert!(!captured[0].competing);
        assert_eq!(captured[0].preview, "ada likes tea");
        assert!(captured[1].competing);
        assert_eq!(captured[1].preview.chars().count(), 120);
    }

    #[test]
    fn captured_memories_cap_at_forty() {
        let cards = (0..60).map(|i| card(i, 0.5, false, "f")).collect();
        let memory = RetrievedMemory {
            cards,
            ..RetrievedMemory::default()
        };
        assert_eq!(captured_memories_from_retrieval(&memory).len(), 40);
    }

    #[test]
    fn artifact_snapshots_persona_settings_and_counts() {
        let mut input = DialogInput::default();
        input.persona.mood = "playful".to_owned();
        input.persona.custom_persona = "be terse".to_owned();
        input.persona.profanity = true;
        input.reference_context = vec!["mem a".to_owned(), "mem b".to_owned()];
        input.disable_tools = false;
        input.shield_context = "flag".to_owned();
        let settings = ChatSettings {
            reactivity_percentage: 30,
            enable_profanity: true,
            ..ChatSettings::defaults(-1)
        };
        let artifact = turn_context_artifact(&input, &settings, Vec::new());
        let persona = artifact.persona.expect("persona snapshot");
        assert_eq!(persona.mood, "playful");
        assert!(persona.custom);
        assert!(persona.profanity);
        assert!(artifact.tools_offered);
        assert!(artifact.shield_on);
        assert_eq!(artifact.reference_context_chars, 10);
        assert!(
            artifact
                .settings
                .iter()
                .any(|kv| kv.label == "reactivity" && kv.value == "30%")
        );
        assert!(
            artifact
                .settings
                .iter()
                .any(|kv| kv.label == "profanity" && kv.value == "yes")
        );
    }
}
