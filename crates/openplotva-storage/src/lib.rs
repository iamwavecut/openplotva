//! Storage boundary for Postgres, pgvector, SQLx, Redis, and Dragonfly.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use openplotva_config::{PostgresConfig, RedisConfig};
use openplotva_core::{
    ChatMessageMeta, ChatSettings, ChatSettingsUpdate, ChatState, MessageIdMapping, PendingOp,
    ToolCall, UserSettings, UserState,
};
use openplotva_history::{
    PrepareStoredSummaryError, StoredSummary, SummaryDocument, SummaryInput, SummaryMessageEntry,
    SummaryScope, decode_summary_message_entry_payload, parse_summary_event_time,
    prepare_stored_summary, summary_events_for_storage, summary_message_entry_timestamp,
    summary_source_id_for_storage,
};
use openplotva_shield::{Options as ShieldOptions, SearchRequest as ShieldSearchRequest};
use pgvector::Vector;
use redis::{
    Client as RedisClient, ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo,
    aio::ConnectionManager,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{
    PgPool, Row,
    migrate::{MigrateError, Migration, Migrator},
    postgres::{PgPoolOptions, PgRow},
};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::OnceCell;

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "storage";

const POSTGRES_MAX_CONNECTIONS: u32 = 50;
const POSTGRES_MIN_CONNECTIONS: u32 = 10;
const POSTGRES_MAX_CONNECTION_LIFETIME: Duration = Duration::from_secs(45 * 60);

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryCardUpsertParams {
    /// Memory visibility.
    pub visibility: String,
    /// Card type.
    pub card_type: String,
    /// Subject.
    pub subject: String,
    /// Predicate.
    pub predicate: String,
    /// Object.
    pub object: String,
    /// Fact text.
    pub fact_text: String,
    /// Deduplication hash.
    pub dedup_hash: String,
    /// Confidence.
    pub confidence: f64,
    /// Salience.
    pub salience: f64,
    /// Origin chat ID.
    pub origin_chat_id: i64,
    /// Origin thread ID.
    pub origin_thread_id: i32,
    /// Origin user ID.
    pub origin_user_id: i64,
    /// Scoped chat ID.
    pub chat_id: i64,
    /// Scoped thread ID.
    pub thread_id: i32,
    /// Scoped user ID.
    pub user_id: i64,
    /// Last observed timestamp.
    pub last_observed_at: OffsetDateTime,
}

/// Rust storage-side wrapper for pgvector embeddings.
///
/// `pgvector` currently tracks SQLx 0.8 for direct binds while this workspace
/// uses SQLx 0.9 alpha, so storage binds text literals through explicit
/// `$n::vector` SQL casts. The public boundary still uses the pgvector value
/// type and keeps vector formatting contained in this crate.
pub type PgEmbeddingVector = Vector;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DialogMemoryChatMeta {
    /// Telegram chat type.
    pub chat_type: String,
    /// Telegram public chat username.
    pub username: String,
    pub active_usernames: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemorySourceBatchParams {
    pub card_id: Option<i64>,
    /// Source chat ID.
    pub chat_id: i64,
    /// Source thread ID.
    pub thread_id: i32,
    /// Source entry IDs.
    pub entry_ids: Vec<String>,
    /// Source Telegram message IDs.
    pub message_ids: Vec<i32>,
    /// Occurrence timestamp.
    pub occurred_at: OffsetDateTime,
    /// Confidence.
    pub confidence: f64,
}

impl Default for MemorySourceBatchParams {
    fn default() -> Self {
        Self {
            card_id: None,
            chat_id: 0,
            thread_id: 0,
            entry_ids: Vec::new(),
            message_ids: Vec::new(),
            occurred_at: openplotva_memory::memory_zero_time(),
            confidence: 0.0,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryLinkBatchParams {
    /// Source card IDs.
    pub from_card_ids: Vec<i64>,
    /// Target card IDs.
    pub to_card_ids: Vec<i64>,
    /// Link relations.
    pub relations: Vec<String>,
    /// Link confidences.
    pub confidences: Vec<f64>,
}

/// Memory card plus retrieval score.
#[derive(Clone, Debug, PartialEq)]
pub struct ScoredMemoryCard {
    /// Card.
    pub card: openplotva_memory::Card,
    /// Score.
    pub score: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryRetrievalLimits {
    /// Card limit.
    pub cards: i32,
    /// Episode limit.
    pub episodes: i32,
}

#[must_use]
pub fn memory_retrieval_limits(req: &openplotva_memory::RetrievalRequest) -> MemoryRetrievalLimits {
    MemoryRetrievalLimits {
        cards: positive_or_default(req.card_limit, 12),
        episodes: positive_or_default(req.episode_limit, 2),
    }
}

#[must_use]
pub fn memory_card_upsert_params(
    card: openplotva_memory::CardInput,
) -> Option<MemoryCardUpsertParams> {
    memory_card_upsert_params_at(card, OffsetDateTime::now_utc())
}

#[must_use]
pub fn memory_card_upsert_params_at(
    card: openplotva_memory::CardInput,
    fallback_observed_at: OffsetDateTime,
) -> Option<MemoryCardUpsertParams> {
    let card = normalize_memory_card_input(card)?;
    let visibility = openplotva_memory::build_visibility_for_observation(&card.observation_scope);
    let (chat_id, thread_id, user_id) =
        memory_card_scope_keys(&card.observation_scope, &visibility);
    let last_observed_at = observed_memory_time(card.observed_at, fallback_observed_at);
    Some(MemoryCardUpsertParams {
        visibility: visibility.clone(),
        card_type: card.card_type.clone(),
        subject: card.subject.clone(),
        predicate: card.predicate.clone(),
        object: card.object.clone(),
        fact_text: card.fact_text.clone(),
        dedup_hash: memory_card_dedup_hash(&visibility, chat_id, thread_id, user_id, &card),
        confidence: card.confidence,
        salience: card.salience,
        origin_chat_id: card.observation_scope.chat_id,
        origin_thread_id: card.observation_scope.thread_id,
        origin_user_id: card.observation_scope.user_id,
        chat_id,
        thread_id,
        user_id,
        last_observed_at,
    })
}

#[must_use]
pub fn normalize_memory_card_input(
    mut input: openplotva_memory::CardInput,
) -> Option<openplotva_memory::CardInput> {
    input.fact_text = input.fact_text.trim().to_owned();
    if input.fact_text.is_empty() {
        return None;
    }
    if input.card_type.is_empty() {
        input.card_type = openplotva_memory::CARD_TYPE_PREFERENCE.to_owned();
    }
    input.subject = compact_memory_field(&input.subject);
    input.predicate = compact_memory_field(&input.predicate);
    input.object = compact_memory_field(&input.object);
    input.confidence = clamp01(input.confidence);
    input.salience = clamp01(input.salience);
    if input.confidence == 0.0 {
        input.confidence = 0.5;
    }
    if input.salience == 0.0 {
        input.salience = 0.5;
    }
    if input.observation_scope.kind.is_empty() {
        input.observation_scope.kind = openplotva_memory::CARD_KIND_CHAT.to_owned();
    }
    Some(input)
}

#[must_use]
pub fn memory_source_batch_params(
    card_id: i64,
    chat_id: i64,
    thread_id: i32,
    card: &openplotva_memory::CardInput,
) -> (MemorySourceBatchParams, bool) {
    memory_source_batch_params_at(card_id, chat_id, thread_id, card, OffsetDateTime::now_utc())
}

#[must_use]
pub fn memory_source_batch_params_at(
    card_id: i64,
    chat_id: i64,
    thread_id: i32,
    card: &openplotva_memory::CardInput,
    fallback_observed_at: OffsetDateTime,
) -> (MemorySourceBatchParams, bool) {
    let source_count = card
        .source_entry_ids
        .len()
        .max(card.source_message_ids.len());
    let mut params = MemorySourceBatchParams {
        card_id: Some(card_id),
        chat_id,
        thread_id,
        entry_ids: Vec::with_capacity(source_count),
        message_ids: Vec::with_capacity(source_count),
        occurred_at: observed_memory_time(card.observed_at, fallback_observed_at),
        confidence: card.confidence,
    };
    let mut seen = HashMap::<MemorySourceKey, ()>::with_capacity(source_count);
    for index in 0..source_count {
        let key = memory_source_key_at(&card.source_entry_ids, &card.source_message_ids, index);
        if key == MemorySourceKey::default() || seen.contains_key(&key) {
            continue;
        }
        seen.insert(key.clone(), ());
        params.entry_ids.push(key.entry_id);
        params.message_ids.push(key.message_id);
    }
    let ok = !params.entry_ids.is_empty();
    (params, ok)
}

#[must_use]
pub fn memory_link_batch_params(
    links: &[openplotva_memory::LinkInput],
) -> Option<MemoryLinkBatchParams> {
    if links.is_empty() {
        return None;
    }
    let mut index_by_key = HashMap::<MemoryLinkKey, usize>::with_capacity(links.len());
    let mut params = MemoryLinkBatchParams {
        from_card_ids: Vec::with_capacity(links.len()),
        to_card_ids: Vec::with_capacity(links.len()),
        relations: Vec::with_capacity(links.len()),
        confidences: Vec::with_capacity(links.len()),
    };
    for link in links {
        if link.from_card_id == 0 || link.to_card_id == 0 || link.from_card_id == link.to_card_id {
            continue;
        }
        let key = MemoryLinkKey {
            from_card_id: link.from_card_id,
            to_card_id: link.to_card_id,
            relation: link.relation.trim().to_owned(),
        };
        let confidence = clamp01(link.confidence);
        if let Some(index) = index_by_key.get(&key) {
            params.confidences[*index] = params.confidences[*index].max(confidence);
            continue;
        }
        index_by_key.insert(key.clone(), params.from_card_ids.len());
        params.from_card_ids.push(key.from_card_id);
        params.to_card_ids.push(key.to_card_id);
        params.relations.push(key.relation);
        params.confidences.push(confidence);
    }
    (!params.from_card_ids.is_empty()).then_some(params)
}

#[must_use]
pub fn rank_retrieved_memory_cards(
    limit: usize,
    groups: &[Vec<ScoredMemoryCard>],
) -> Vec<openplotva_memory::Card> {
    let mut cards_by_id = HashMap::<i64, ScoredMemoryCard>::new();
    for group in groups {
        for item in group {
            if cards_by_id
                .get(&item.card.id)
                .is_some_and(|existing| existing.score >= item.score)
            {
                continue;
            }
            cards_by_id.insert(item.card.id, item.clone());
        }
    }
    let mut ranked = cards_by_id.into_values().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                memory_card_updated_at(&right.card).cmp(&memory_card_updated_at(&left.card))
            })
    });
    ranked
        .into_iter()
        .take(limit)
        .map(|item| item.card)
        .collect()
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct MemorySourceKey {
    entry_id: String,
    message_id: i32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MemoryLinkKey {
    from_card_id: i64,
    to_card_id: i64,
    relation: String,
}

fn positive_or_default(value: i32, default: i32) -> i32 {
    if value > 0 { value } else { default }
}

fn optional_trimmed_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

/// Build a pgvector embedding value from raw model output.
#[must_use]
pub fn pg_embedding_vector(values: Vec<f32>) -> PgEmbeddingVector {
    PgEmbeddingVector::from(values)
}

fn pgvector_literal(vector: Option<&PgEmbeddingVector>) -> Option<String> {
    let vector = vector?;
    let values = vector.as_slice();
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let mut out = String::with_capacity(values.len() * 10 + 2);
    out.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&value.to_string());
    }
    out.push(']');
    Some(out)
}

fn memory_card_scope_keys(
    scope: &openplotva_memory::ObservationScope,
    visibility: &str,
) -> (i64, i32, i64) {
    match visibility {
        openplotva_memory::VISIBILITY_PUBLIC_USER => (0, 0, scope.user_id),
        openplotva_memory::VISIBILITY_PRIVATE_CHAT | openplotva_memory::VISIBILITY_CHAT_USER => {
            (scope.chat_id, 0, scope.user_id)
        }
        openplotva_memory::VISIBILITY_THREAD => (scope.chat_id, scope.thread_id, 0),
        _ => (scope.chat_id, 0, 0),
    }
}

fn memory_card_dedup_hash(
    visibility: &str,
    chat_id: i64,
    thread_id: i32,
    user_id: i64,
    card: &openplotva_memory::CardInput,
) -> String {
    let parts = [
        visibility.to_owned(),
        chat_id.to_string(),
        thread_id.to_string(),
        user_id.to_string(),
        card.subject.to_lowercase(),
        card.predicate.to_lowercase(),
        card.object.to_lowercase(),
        card.fact_text.to_lowercase(),
    ];
    let mut hasher = Sha256::new();
    hasher.update(parts.join("\0"));
    hex::encode(hasher.finalize())
}

fn memory_source_key_at(
    entry_ids: &[String],
    message_ids: &[i32],
    index: usize,
) -> MemorySourceKey {
    MemorySourceKey {
        entry_id: entry_ids
            .get(index)
            .map(|entry_id| entry_id.trim().to_owned())
            .unwrap_or_default(),
        message_id: message_ids.get(index).copied().unwrap_or_default(),
    }
}

fn compact_memory_field(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut pending_space = false;
    for ch in value.trim().chars() {
        if ch.is_whitespace() {
            if !out.is_empty() {
                pending_space = true;
            }
            continue;
        }
        if pending_space {
            out.push(' ');
            pending_space = false;
        }
        out.push(ch);
    }
    out
}

fn clamp01(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

fn observed_memory_time(
    value: OffsetDateTime,
    fallback_observed_at: OffsetDateTime,
) -> OffsetDateTime {
    if openplotva_memory::is_memory_zero_time(value) {
        fallback_observed_at
    } else {
        value
    }
}

fn memory_card_updated_at(card: &openplotva_memory::Card) -> OffsetDateTime {
    card.updated_at
        .unwrap_or_else(openplotva_memory::memory_zero_time)
}

fn memory_cursor_after_at(value: OffsetDateTime) -> OffsetDateTime {
    if openplotva_memory::is_memory_zero_time(value) {
        OffsetDateTime::UNIX_EPOCH
    } else {
        value
    }
}

pub const SQL_INSERT_VIRTUAL_MESSAGE: &str =
    "INSERT INTO message_id_map (vmsg_id, chat_id, thread_id) VALUES ($1, $2, $3)";

pub const SQL_UPSERT_USER: &str = "INSERT INTO users (id, first_name, last_name, username, language_code, is_premium, settings) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb) ON CONFLICT (id) DO UPDATE SET first_name = COALESCE(EXCLUDED.first_name, users.first_name), last_name = COALESCE(EXCLUDED.last_name, users.last_name), username = COALESCE(EXCLUDED.username, users.username), language_code = COALESCE(EXCLUDED.language_code, users.language_code), is_premium = COALESCE(EXCLUDED.is_premium, users.is_premium), settings = COALESCE(EXCLUDED.settings, users.settings), updated = CURRENT_TIMESTAMP";

pub const SQL_GET_USER: &str = "SELECT id, first_name, last_name, username, language_code, is_premium FROM users WHERE id = $1";

pub const SQL_LIST_USERS_BY_IDS: &str = "SELECT id, first_name, last_name, username, language_code, is_premium FROM users WHERE id = ANY($1::bigint[])";

pub const SQL_SEARCH_CHAT_MEMBER_CANDIDATES: &str = "SELECT u.id, u.first_name, u.last_name, u.username, cm.status, cm.last_message_at, cm.updated_at FROM chat_members cm JOIN users u ON u.id = cm.user_id WHERE cm.chat_id = $1 AND cm.status IN ('creator', 'administrator', 'member') AND ($2 = '' OR LOWER(COALESCE(u.username, '')) LIKE '%' || LOWER($2) || '%' OR LOWER(u.first_name) LIKE '%' || LOWER($2) || '%' OR LOWER(COALESCE(u.last_name, '')) LIKE '%' || LOWER($2) || '%') ORDER BY cm.last_message_at DESC NULLS LAST, cm.updated_at DESC, u.id LIMIT $3";

pub const SQL_GET_USER_ID_BY_USERNAME: &str = "SELECT id FROM users WHERE username = $1 LIMIT 1";

pub const SQL_CREATE_RUNTIME_API_TOKEN: &str =
    "INSERT INTO runtime_api_tokens (id, token_hash) VALUES ($1, $2) RETURNING *";

pub const SQL_GET_RUNTIME_API_TOKEN: &str = "SELECT * FROM runtime_api_tokens WHERE id = $1";

pub const SQL_LIST_RUNTIME_API_TOKENS_CREATED_SINCE: &str =
    "SELECT * FROM runtime_api_tokens WHERE created_at >= $1 ORDER BY created_at DESC, id ASC";

pub const SQL_DELETE_RUNTIME_API_TOKENS_OLDER_THAN: &str =
    "DELETE FROM runtime_api_tokens WHERE created_at < $1";

pub const SQL_UPSERT_CHAT: &str = "INSERT INTO chats (id, type, title, username, first_name, last_name, is_forum, active_usernames, available_reactions, bio, has_private_forwards, has_restricted_voice_and_video_messages, join_to_send_messages, join_by_request, description, invite_link, pinned_message, permissions, slow_mode_delay, message_auto_delete_time, has_aggressive_anti_spam_enabled, has_hidden_members, has_protected_content, has_visible_history, sticker_set_name, can_set_sticker_set, linked_chat_id, location) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11, $12, $13, $14, $15, $16, $17::jsonb, $18::jsonb, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28::jsonb) ON CONFLICT (id) DO UPDATE SET type = COALESCE(EXCLUDED.type, chats.type), title = COALESCE(EXCLUDED.title, chats.title), username = COALESCE(EXCLUDED.username, chats.username), first_name = COALESCE(EXCLUDED.first_name, chats.first_name), last_name = COALESCE(EXCLUDED.last_name, chats.last_name), is_forum = COALESCE(EXCLUDED.is_forum, chats.is_forum), active_usernames = COALESCE(EXCLUDED.active_usernames, chats.active_usernames), available_reactions = COALESCE(EXCLUDED.available_reactions, chats.available_reactions), bio = COALESCE(EXCLUDED.bio, chats.bio), has_private_forwards = COALESCE(EXCLUDED.has_private_forwards, chats.has_private_forwards), has_restricted_voice_and_video_messages = COALESCE(EXCLUDED.has_restricted_voice_and_video_messages, chats.has_restricted_voice_and_video_messages), join_to_send_messages = COALESCE(EXCLUDED.join_to_send_messages, chats.join_to_send_messages), join_by_request = COALESCE(EXCLUDED.join_by_request, chats.join_by_request), description = COALESCE(EXCLUDED.description, chats.description), invite_link = COALESCE(EXCLUDED.invite_link, chats.invite_link), pinned_message = COALESCE(EXCLUDED.pinned_message, chats.pinned_message), permissions = COALESCE(EXCLUDED.permissions, chats.permissions), slow_mode_delay = COALESCE(EXCLUDED.slow_mode_delay, chats.slow_mode_delay), message_auto_delete_time = COALESCE(EXCLUDED.message_auto_delete_time, chats.message_auto_delete_time), has_aggressive_anti_spam_enabled = COALESCE(EXCLUDED.has_aggressive_anti_spam_enabled, chats.has_aggressive_anti_spam_enabled), has_hidden_members = COALESCE(EXCLUDED.has_hidden_members, chats.has_hidden_members), has_protected_content = COALESCE(EXCLUDED.has_protected_content, chats.has_protected_content), has_visible_history = COALESCE(EXCLUDED.has_visible_history, chats.has_visible_history), sticker_set_name = COALESCE(EXCLUDED.sticker_set_name, chats.sticker_set_name), can_set_sticker_set = COALESCE(EXCLUDED.can_set_sticker_set, chats.can_set_sticker_set), linked_chat_id = COALESCE(EXCLUDED.linked_chat_id, chats.linked_chat_id), location = COALESCE(EXCLUDED.location, chats.location), updated = CURRENT_TIMESTAMP";

pub const SQL_RESOLVE_VIRTUAL_MESSAGE: &str = "UPDATE message_id_map SET real_message_id = $1, resolved_at = COALESCE($2, NOW()) WHERE vmsg_id = $3";

pub const SQL_GET_MAPPING_BY_VIRTUAL: &str =
    "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE vmsg_id = $1";

pub const SQL_LIST_MAPPINGS_BY_VIRTUAL_IDS: &str = "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE vmsg_id = ANY($1::text[])";

pub const SQL_GET_MAPPING_BY_REAL: &str = "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE chat_id = $1 AND real_message_id = $2";

pub const SQL_DELETE_MAPPING_BY_VIRTUAL: &str = "DELETE FROM message_id_map WHERE vmsg_id = $1";

pub const SQL_ENQUEUE_MESSAGE_OP: &str = "INSERT INTO message_ops_queue (vmsg_id, chat_id, op, payload) VALUES ($1, $2, $3, $4::jsonb) RETURNING id";

pub const SQL_LIST_PENDING_OPS: &str = "SELECT id, vmsg_id, chat_id, op, COALESCE(payload::text, '') AS payload, attempts FROM message_ops_queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT $1";

pub const SQL_MARK_OP_DONE: &str =
    "UPDATE message_ops_queue SET status = 'done', executed_at = NOW() WHERE id = $1";

pub const SQL_MARK_OP_FAILED: &str =
    "UPDATE message_ops_queue SET attempts = attempts + 1, last_error = $2 WHERE id = $1";

pub const SQL_GET_CHAT_TYPE: &str = "SELECT type FROM chats WHERE id = $1";

pub const SQL_GET_CHAT_STATE: &str =
    "SELECT id, type, title, username, first_name, last_name, is_forum FROM chats WHERE id = $1";

pub const SQL_LIST_USER_CHATS: &str = "SELECT c.id, c.type, c.title, c.username, c.first_name, c.last_name, c.is_forum FROM chats c JOIN chat_members cm ON c.id = cm.chat_id WHERE cm.user_id = $1";

pub const SQL_GET_DIALOG_MEMORY_CHAT_META: &str = "SELECT type, username, COALESCE(active_usernames::text, '') AS active_usernames FROM chats WHERE id = $1";

pub const SQL_GET_CHAT_MEMBER: &str =
    "SELECT * FROM chat_members WHERE chat_id = $1 AND user_id = $2";

pub const SQL_LIST_CHAT_MEMBERS: &str = "SELECT * FROM chat_members WHERE chat_id = $1";

pub const SQL_LIST_CHAT_MEMBERS_BY_USER_IDS: &str =
    "SELECT * FROM chat_members WHERE chat_id = $1 AND user_id = ANY($2::bigint[])";

pub const SQL_LIST_USER_CHAT_MEMBERSHIPS: &str = "SELECT * FROM chat_members WHERE user_id = $1";

pub const SQL_LIST_CHAT_DEPUTY_IDS: &str =
    "SELECT user_id FROM chat_deputies WHERE chat_id = $1 ORDER BY user_id";

pub const SQL_LIST_USER_DEPUTY_CHAT_IDS: &str =
    "SELECT chat_id FROM chat_deputies WHERE user_id = $1 ORDER BY chat_id";

pub const SQL_DELETE_ALL_CHAT_DEPUTIES: &str = "DELETE FROM chat_deputies WHERE chat_id = $1";

pub const SQL_UPSERT_CHAT_DEPUTIES: &str = "INSERT INTO chat_deputies (chat_id, user_id) SELECT $1, unnest($2::bigint[]) ON CONFLICT (chat_id, user_id) DO NOTHING";

pub const SQL_DELETE_CHAT_MEMBER: &str =
    "DELETE FROM chat_members WHERE chat_id = $1 AND user_id = $2";

pub const SQL_UPSERT_CHAT_MEMBER: &str = "INSERT INTO chat_members (chat_id, user_id, status, is_anonymous, custom_title, can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats, can_restrict_members, can_promote_members, can_change_info, can_invite_users, can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics, can_send_messages, can_send_media_messages, can_send_polls, can_send_other_messages, can_add_web_page_previews, until_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23) ON CONFLICT (chat_id, user_id) DO UPDATE SET status = COALESCE(EXCLUDED.status, chat_members.status), is_anonymous = COALESCE(EXCLUDED.is_anonymous, chat_members.is_anonymous), custom_title = COALESCE(EXCLUDED.custom_title, chat_members.custom_title), can_be_edited = COALESCE(EXCLUDED.can_be_edited, chat_members.can_be_edited), can_manage_chat = COALESCE(EXCLUDED.can_manage_chat, chat_members.can_manage_chat), can_delete_messages = COALESCE(EXCLUDED.can_delete_messages, chat_members.can_delete_messages), can_manage_video_chats = COALESCE(EXCLUDED.can_manage_video_chats, chat_members.can_manage_video_chats), can_restrict_members = COALESCE(EXCLUDED.can_restrict_members, chat_members.can_restrict_members), can_promote_members = COALESCE(EXCLUDED.can_promote_members, chat_members.can_promote_members), can_change_info = COALESCE(EXCLUDED.can_change_info, chat_members.can_change_info), can_invite_users = COALESCE(EXCLUDED.can_invite_users, chat_members.can_invite_users), can_post_messages = COALESCE(EXCLUDED.can_post_messages, chat_members.can_post_messages), can_edit_messages = COALESCE(EXCLUDED.can_edit_messages, chat_members.can_edit_messages), can_pin_messages = COALESCE(EXCLUDED.can_pin_messages, chat_members.can_pin_messages), can_manage_topics = COALESCE(EXCLUDED.can_manage_topics, chat_members.can_manage_topics), can_send_messages = COALESCE(EXCLUDED.can_send_messages, chat_members.can_send_messages), can_send_media_messages = COALESCE(EXCLUDED.can_send_media_messages, chat_members.can_send_media_messages), can_send_polls = COALESCE(EXCLUDED.can_send_polls, chat_members.can_send_polls), can_send_other_messages = COALESCE(EXCLUDED.can_send_other_messages, chat_members.can_send_other_messages), can_add_web_page_previews = COALESCE(EXCLUDED.can_add_web_page_previews, chat_members.can_add_web_page_previews), until_date = COALESCE(EXCLUDED.until_date, chat_members.until_date), updated_at = CURRENT_TIMESTAMP";

pub const SQL_UPDATE_MEMBER_LAST_MESSAGE: &str = "UPDATE chat_members SET last_message_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $1 AND user_id = $2";

pub const SQL_UPDATE_MEMBER_LAST_MESSAGES: &str = "WITH input AS (SELECT * FROM unnest($1::bigint[], $2::bigint[]) AS input(chat_id, user_id)) UPDATE chat_members AS member SET last_message_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP FROM input WHERE member.chat_id = input.chat_id AND member.user_id = input.user_id";

pub const SQL_UPSERT_CHAT_ACTIVE_USER: &str = "INSERT INTO chat_active_users (chat_id, user_id, last_active_at) VALUES ($1, $2, CURRENT_TIMESTAMP) ON CONFLICT (chat_id, user_id) DO UPDATE SET last_active_at = CURRENT_TIMESTAMP";

pub const SQL_UPSERT_CHAT_ACTIVE_USERS: &str = "INSERT INTO chat_active_users (chat_id, user_id, last_active_at) SELECT input.chat_id, input.user_id, CURRENT_TIMESTAMP FROM unnest($1::bigint[], $2::bigint[]) AS input(chat_id, user_id) ON CONFLICT (chat_id, user_id) DO UPDATE SET last_active_at = CURRENT_TIMESTAMP";

pub const SQL_LIST_ACTIVE_PARTICIPANTS: &str = "SELECT user_id FROM chat_members WHERE chat_id = $1 AND status IN ('administrator', 'member', 'creator') AND last_message_at IS NOT NULL AND last_message_at >= (CURRENT_TIMESTAMP - INTERVAL '24 hours') ORDER BY last_message_at DESC LIMIT $2";

pub const SQL_LIST_ACTIVE_PARTICIPANTS_FROM_TABLE: &str = "SELECT user_id FROM chat_active_users WHERE chat_id = $1 AND last_active_at >= (CURRENT_TIMESTAMP - INTERVAL '24 hours') ORDER BY last_active_at DESC LIMIT $2";

pub const SQL_UPSERT_TELEGRAM_FILE_METADATA: &str = "INSERT INTO telegram_files (file_unique_id, latest_file_id, media_kind, mime_type, width, height, file_size, first_seen_chat_id, first_seen_message_id, last_seen_chat_id, last_seen_message_id, last_seen_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP) ON CONFLICT (file_unique_id) DO UPDATE SET latest_file_id = EXCLUDED.latest_file_id, media_kind = EXCLUDED.media_kind, mime_type = COALESCE(EXCLUDED.mime_type, telegram_files.mime_type), width = COALESCE(EXCLUDED.width, telegram_files.width), height = COALESCE(EXCLUDED.height, telegram_files.height), file_size = COALESCE(EXCLUDED.file_size, telegram_files.file_size), last_seen_chat_id = EXCLUDED.last_seen_chat_id, last_seen_message_id = EXCLUDED.last_seen_message_id, last_seen_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP RETURNING file_unique_id, latest_file_id, media_kind, mime_type, width, height, file_size, first_seen_chat_id, first_seen_message_id, last_seen_chat_id, last_seen_message_id, last_seen_at, vision_status, vision_caption, vision_model, vision_latency_ms, recognition_requested_at, recognition_completed_at, COALESCE(extra::text, '{}') AS extra, created_at, updated_at";

pub const SQL_GET_TELEGRAM_FILE: &str = "SELECT file_unique_id, latest_file_id, media_kind, mime_type, width, height, file_size, first_seen_chat_id, first_seen_message_id, last_seen_chat_id, last_seen_message_id, last_seen_at, vision_status, vision_caption, vision_model, vision_latency_ms, recognition_requested_at, recognition_completed_at, COALESCE(extra::text, '{}') AS extra, created_at, updated_at FROM telegram_files WHERE file_unique_id = $1 LIMIT 1";

pub const SQL_LIST_TELEGRAM_FILES_BY_UNIQUE_IDS: &str = "SELECT file_unique_id, latest_file_id, media_kind, mime_type, width, height, file_size, first_seen_chat_id, first_seen_message_id, last_seen_chat_id, last_seen_message_id, last_seen_at, vision_status, vision_caption, vision_model, vision_latency_ms, recognition_requested_at, recognition_completed_at, COALESCE(extra::text, '{}') AS extra, created_at, updated_at FROM telegram_files WHERE file_unique_id = ANY($1::text[])";

pub const SQL_GET_TELEGRAM_FILE_BY_LATEST_FILE_ID: &str = "SELECT file_unique_id, latest_file_id, media_kind, mime_type, width, height, file_size, first_seen_chat_id, first_seen_message_id, last_seen_chat_id, last_seen_message_id, last_seen_at, vision_status, vision_caption, vision_model, vision_latency_ms, recognition_requested_at, recognition_completed_at, COALESCE(extra::text, '{}') AS extra, created_at, updated_at FROM telegram_files WHERE latest_file_id = $1 ORDER BY last_seen_at DESC LIMIT 1";

pub const SQL_UPDATE_TELEGRAM_FILE_VISION: &str = "UPDATE telegram_files SET vision_status = $2, vision_caption = COALESCE($3, vision_caption), vision_model = COALESCE($4, vision_model), vision_latency_ms = COALESCE($5, vision_latency_ms), recognition_requested_at = COALESCE($6, recognition_requested_at), recognition_completed_at = COALESCE($7, recognition_completed_at), updated_at = CURRENT_TIMESTAMP WHERE file_unique_id = $1 RETURNING file_unique_id, latest_file_id, media_kind, mime_type, width, height, file_size, first_seen_chat_id, first_seen_message_id, last_seen_chat_id, last_seen_message_id, last_seen_at, vision_status, vision_caption, vision_model, vision_latency_ms, recognition_requested_at, recognition_completed_at, COALESCE(extra::text, '{}') AS extra, created_at, updated_at";

pub const SQL_GET_CHAT_DISCOVERED: &str = "SELECT discovered FROM chats WHERE id = $1";

pub const SQL_RECORD_CHAT_DAILY_WINNER: &str = "INSERT INTO chat_game_results (chat_id, user_id, theme) VALUES ($1, $2, $3) RETURNING id, chat_id, user_id, theme, won_at, won_on_date";

pub const SQL_GET_TODAY_CHAT_WINNER: &str = "SELECT id, chat_id, user_id, theme, won_at, won_on_date FROM chat_game_results WHERE chat_id = $1 AND won_at::date = CURRENT_DATE ORDER BY won_at DESC LIMIT 1";

pub const SQL_INCREMENT_CHAT_GAME_WIN: &str = "INSERT INTO chat_game_stats (chat_id, user_id, wins_count, last_win_at) VALUES ($1, $2, 1, CURRENT_TIMESTAMP) ON CONFLICT (chat_id, user_id) DO UPDATE SET wins_count = chat_game_stats.wins_count + 1, last_win_at = CURRENT_TIMESTAMP";

pub const SQL_GET_YEARLY_TOP: &str = "SELECT u.id, u.first_name, u.last_name, u.username, u.language_code, u.is_premium, COUNT(*)::int AS wins_count, MAX(r.won_at) AS last_win_at FROM chat_game_results r JOIN users u ON u.id = r.user_id WHERE r.chat_id = $1 AND r.won_at >= date_trunc('year', CURRENT_DATE) GROUP BY u.id, u.first_name, u.last_name, u.username, u.language_code, u.is_premium ORDER BY wins_count DESC, last_win_at DESC LIMIT $2";

pub const SQL_GET_CHAT_SETTINGS: &str = "SELECT chat_id, mood_alignment, custom_persona, reactivity_percentage, proactivity_percentage, enable_global_text_reply, enable_global_draw_reply, enable_obscenifier, enable_profanity, enable_greet_joiners, enable_daily_game, daily_game_theme, greeting_html FROM chat_settings WHERE chat_id = $1";

pub const SQL_GET_USER_SETTINGS: &str = "SELECT user_id, disable_random_reactivity, updated, hide_original_draw_prompt FROM user_settings WHERE user_id = $1";

pub const SQL_UPSERT_USER_SETTINGS: &str = "INSERT INTO user_settings (user_id, disable_random_reactivity, hide_original_draw_prompt, updated) VALUES ($1, $2, $3, CURRENT_TIMESTAMP) ON CONFLICT (user_id) DO UPDATE SET disable_random_reactivity = EXCLUDED.disable_random_reactivity, hide_original_draw_prompt = EXCLUDED.hide_original_draw_prompt, updated = CURRENT_TIMESTAMP";

pub const SQL_UPSERT_CHAT_SETTINGS: &str = "WITH ensure_chat AS (INSERT INTO chats (id, type) VALUES ($1, COALESCE(NULLIF($14::text, ''), 'private')) ON CONFLICT (id) DO NOTHING) INSERT INTO chat_settings (chat_id, mood_alignment, custom_persona, reactivity_percentage, proactivity_percentage, enable_obscenifier, enable_profanity, enable_greet_joiners, enable_global_text_reply, enable_global_draw_reply, enable_daily_game, daily_game_theme, updated, greeting_html) VALUES ($1, $2, $3, $4, $5, COALESCE($6, TRUE)::boolean, COALESCE($7, TRUE)::boolean, COALESCE($8, FALSE)::boolean, COALESCE($9, TRUE)::boolean, COALESCE($10, TRUE)::boolean, COALESCE($11, TRUE)::boolean, COALESCE($12, 'auto')::text, CURRENT_TIMESTAMP, $13) ON CONFLICT (chat_id) DO UPDATE SET mood_alignment = EXCLUDED.mood_alignment, custom_persona = EXCLUDED.custom_persona, reactivity_percentage = COALESCE(EXCLUDED.reactivity_percentage, chat_settings.reactivity_percentage), proactivity_percentage = COALESCE(EXCLUDED.proactivity_percentage, chat_settings.proactivity_percentage), enable_obscenifier = COALESCE(EXCLUDED.enable_obscenifier, chat_settings.enable_obscenifier), enable_profanity = COALESCE(EXCLUDED.enable_profanity, chat_settings.enable_profanity), enable_greet_joiners = COALESCE(EXCLUDED.enable_greet_joiners, chat_settings.enable_greet_joiners), enable_global_text_reply = COALESCE(EXCLUDED.enable_global_text_reply, chat_settings.enable_global_text_reply), enable_global_draw_reply = COALESCE(EXCLUDED.enable_global_draw_reply, chat_settings.enable_global_draw_reply), enable_daily_game = COALESCE(EXCLUDED.enable_daily_game, chat_settings.enable_daily_game), daily_game_theme = EXCLUDED.daily_game_theme, greeting_html = EXCLUDED.greeting_html, updated = CURRENT_TIMESTAMP";

pub const SQL_SELECT_TEXT_HISTORY_ENTRY: &str = "SELECT bucket_day, entry_id, payload::text AS payload FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2 AND kind = 'text' ORDER BY occurred_at DESC LIMIT 1";

pub const SQL_UPDATE_HISTORY_ENTRY_PAYLOAD: &str = "UPDATE chat_history_entries SET payload = $4::jsonb, updated_at = CURRENT_TIMESTAMP WHERE bucket_day = $1 AND chat_id = $2 AND entry_id = $3";

pub const SQL_DELETE_HISTORY_MESSAGE_ENTRIES: &str =
    "DELETE FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2";

pub const SQL_DELETE_HISTORY_TOOL_ENTRIES: &str =
    "DELETE FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2 AND kind <> 'text'";

pub const SQL_UPSERT_CHAT_HISTORY_RESET: &str = "INSERT INTO chat_history_resets (chat_id, thread_id, reset_at) VALUES ($1, $2, $3) ON CONFLICT (chat_id, thread_id) DO UPDATE SET reset_at = GREATEST(chat_history_resets.reset_at, EXCLUDED.reset_at)";

pub const SQL_GET_CHAT_HISTORY_RESET_AT: &str =
    "SELECT reset_at FROM chat_history_resets WHERE chat_id = $1 AND thread_id = $2";

pub const SQL_SELECT_RECENT_CHAT_HISTORY_ENTRY_PAYLOADS: &str = "SELECT payload::text AS payload FROM chat_history_entries WHERE chat_id = $1 AND occurred_at > $2 AND ($3::integer = 0 OR thread_id <> $3 OR occurred_at > $4) ORDER BY occurred_at DESC, message_id DESC, CASE kind WHEN 'text' THEN 1 WHEN 'tool_request' THEN 2 WHEN 'tool_response' THEN 3 ELSE 4 END DESC, entry_id DESC LIMIT $5";

pub const SQL_SELECT_RECENT_THREAD_HISTORY_ENTRY_PAYLOADS: &str = "SELECT payload::text AS payload FROM chat_history_entries WHERE chat_id = $1 AND thread_id = $2 AND occurred_at > $3 ORDER BY occurred_at DESC, message_id DESC, CASE kind WHEN 'text' THEN 1 WHEN 'tool_request' THEN 2 WHEN 'tool_response' THEN 3 ELSE 4 END DESC, entry_id DESC LIMIT $4";

pub const SQL_ENSURE_CHAT_HISTORY_PARTITION: &str =
    "SELECT ensure_chat_history_partition($1::date)";

pub const SQL_UPSERT_HISTORY_ENTRY: &str = "INSERT INTO chat_history_entries (bucket_day, chat_id, thread_id, message_id, entry_id, kind, role, occurred_at, sender_id, payload) VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb) ON CONFLICT (bucket_day, chat_id, entry_id) DO UPDATE SET thread_id = EXCLUDED.thread_id, message_id = EXCLUDED.message_id, kind = EXCLUDED.kind, role = EXCLUDED.role, occurred_at = EXCLUDED.occurred_at, sender_id = EXCLUDED.sender_id, payload = EXCLUDED.payload, updated_at = CURRENT_TIMESTAMP";

pub const SQL_SELECT_CHAT_SUMMARY_ENTRY_PAYLOADS: &str = "SELECT payload::text AS payload FROM chat_history_entries WHERE chat_id = $1 AND occurred_at > $2 AND occurred_at <= $3 AND kind = 'text' ORDER BY occurred_at ASC, message_id ASC, entry_id ASC";

pub const SQL_SELECT_THREAD_SUMMARY_ENTRY_PAYLOADS: &str = "SELECT payload::text AS payload FROM chat_history_entries WHERE chat_id = $1 AND thread_id = $2 AND occurred_at > $3 AND occurred_at <= $4 AND kind = 'text' ORDER BY occurred_at ASC, message_id ASC, entry_id ASC";

pub const SQL_SELECT_REUSABLE_HISTORY_SUMMARIES: &str = "SELECT id, chat_id, thread_id, scope, requested_by_user_id, range_start_at, range_end_at, first_message_id, last_message_id, first_entry_id, last_entry_id, raw_message_count, covered_message_count, source_summary_ids, summary_json::text AS summary_json, summary_html, model, prompt_version, input_hash, prompt_hash, input_token_estimate, output_token_estimate, cascade_depth, quality_score, quality_notes, created_at FROM chat_history_summaries WHERE chat_id = $1 AND thread_id = $2 AND scope = $3 AND range_start_at >= $4 AND range_end_at <= $5 AND created_at > $6 ORDER BY range_start_at ASC, range_end_at ASC, created_at DESC";

pub const SQL_INSERT_HISTORY_SUMMARY: &str = "INSERT INTO chat_history_summaries (chat_id, thread_id, scope, requested_by_user_id, range_start_at, range_end_at, first_message_id, last_message_id, first_entry_id, last_entry_id, raw_message_count, covered_message_count, source_summary_ids, summary_json, summary_html, model, prompt_version, input_hash, prompt_hash, input_token_estimate, output_token_estimate, cascade_depth, quality_score, quality_notes) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24) RETURNING id, created_at";

pub const SQL_INSERT_HISTORY_SUMMARY_SOURCE: &str = "INSERT INTO chat_history_summary_sources (summary_id, source_order, source_type, source_summary_id, range_start_at, range_end_at, first_message_id, last_message_id, first_entry_id, last_entry_id, raw_message_count, covered_message_count) VALUES ($1, $2, $3, $4::bigint, $5, $6, $7, $8, $9, $10, $11, $12)";

pub const SQL_INSERT_CHAT_HISTORY_EVENT: &str = "INSERT INTO chat_history_events (summary_id, chat_id, thread_id, scope, event_order, title, description, actors, occurred_at, range_start_at, range_end_at, source_summary_ids, confidence) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::timestamptz, $10, $11, $12, $13)";

pub const SQL_UPSERT_MEMORY_CARD_LEXICAL: &str = "INSERT INTO memory_cards (visibility, card_type, subject, predicate, object, fact_text, dedup_hash, confidence, salience, origin_chat_id, origin_thread_id, origin_user_id, chat_id, thread_id, user_id, last_observed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) ON CONFLICT (visibility, user_id, chat_id, thread_id, dedup_hash) WHERE status = 'active' DO UPDATE SET confidence = GREATEST(memory_cards.confidence, EXCLUDED.confidence), salience = GREATEST(memory_cards.salience, EXCLUDED.salience), observation_count = memory_cards.observation_count + 1, last_observed_at = GREATEST(memory_cards.last_observed_at, EXCLUDED.last_observed_at), updated_at = CURRENT_TIMESTAMP RETURNING id, (xmax = 0) AS inserted";

pub const SQL_UPSERT_MEMORY_CARD_WITH_EMBEDDING: &str = "INSERT INTO memory_cards (visibility, card_type, subject, predicate, object, fact_text, dedup_hash, confidence, salience, origin_chat_id, origin_thread_id, origin_user_id, chat_id, thread_id, user_id, last_observed_at, embedding) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17::vector) ON CONFLICT (visibility, user_id, chat_id, thread_id, dedup_hash) WHERE status = 'active' DO UPDATE SET confidence = GREATEST(memory_cards.confidence, EXCLUDED.confidence), salience = GREATEST(memory_cards.salience, EXCLUDED.salience), observation_count = memory_cards.observation_count + 1, last_observed_at = GREATEST(memory_cards.last_observed_at, EXCLUDED.last_observed_at), embedding = COALESCE(EXCLUDED.embedding, memory_cards.embedding), updated_at = CURRENT_TIMESTAMP RETURNING id, (xmax = 0) AS inserted";

pub const SQL_INSERT_MEMORY_SOURCES: &str = "WITH input AS (SELECT unnest($4::text[]) AS entry_id, unnest($5::integer[]) AS message_id) INSERT INTO memory_sources (card_id, chat_id, thread_id, entry_id, message_id, occurred_at, confidence) SELECT $1, $2, $3, input.entry_id, input.message_id, $6, $7 FROM input WHERE NOT EXISTS (SELECT 1 FROM memory_sources WHERE card_id = $1 AND entry_id = input.entry_id AND message_id = input.message_id)";

pub const SQL_INSERT_MEMORY_EPISODE_LEXICAL: &str = "INSERT INTO memory_episodes (visibility, chat_id, thread_id, range_start_at, range_end_at, message_count, summary_text, topics, participants, model, prompt_version, cursor_after_at, cursor_after_message_id, cursor_after_entry_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) ON CONFLICT (chat_id, thread_id, range_start_at, range_end_at, prompt_version, cursor_after_at, cursor_after_message_id, cursor_after_entry_id) DO UPDATE SET summary_text = EXCLUDED.summary_text, topics = EXCLUDED.topics, participants = EXCLUDED.participants, model = EXCLUDED.model, updated_at = CURRENT_TIMESTAMP RETURNING id, (xmax = 0) AS inserted";

pub const SQL_INSERT_MEMORY_EPISODE_WITH_EMBEDDING: &str = "INSERT INTO memory_episodes (visibility, chat_id, thread_id, range_start_at, range_end_at, message_count, summary_text, topics, participants, model, prompt_version, cursor_after_at, cursor_after_message_id, cursor_after_entry_id, embedding) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::vector) ON CONFLICT (chat_id, thread_id, range_start_at, range_end_at, prompt_version, cursor_after_at, cursor_after_message_id, cursor_after_entry_id) DO UPDATE SET summary_text = EXCLUDED.summary_text, topics = EXCLUDED.topics, participants = EXCLUDED.participants, model = EXCLUDED.model, embedding = COALESCE(EXCLUDED.embedding, memory_episodes.embedding), updated_at = CURRENT_TIMESTAMP RETURNING id, (xmax = 0) AS inserted";

pub const SQL_UPSERT_MEMORY_LINKS: &str = "INSERT INTO memory_links (from_card_id, to_card_id, relation, confidence) SELECT unnest($1::bigint[]) AS from_card_id, unnest($2::bigint[]) AS to_card_id, unnest($3::text[]) AS relation, unnest($4::double precision[]) AS confidence ON CONFLICT (from_card_id, to_card_id, relation) DO UPDATE SET confidence = GREATEST(memory_links.confidence, EXCLUDED.confidence)";

pub const SQL_SUPERSEDE_MEMORY_CARD: &str = "UPDATE memory_cards SET status = 'superseded', valid_until = CURRENT_TIMESTAMP, superseded_by = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND status = 'active'";

pub const SQL_MARK_EXHAUSTED_MEMORY_RUNS: &str = r#"UPDATE memory_runs
SET status = 'failed',
    lease_owner = '',
    leased_until = NULL,
    error = CASE
        WHEN error <> '' THEN error
        ELSE 'memory run exhausted after 5 attempts without a captured failure'
    END,
    error_log = CASE
        WHEN jsonb_array_length(error_log) > 0 THEN error_log
        ELSE error_log || jsonb_build_array(jsonb_build_object(
            'attempt', attempts,
            'failed_at', CURRENT_TIMESTAMP,
            'error', CASE
                WHEN error <> '' THEN left(error, 4000)
                ELSE 'memory run exhausted after 5 attempts without a captured failure'
            END
        ))
    END,
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE status = 'processing'
  AND attempts >= 5
  AND (leased_until IS NULL OR leased_until < CURRENT_TIMESTAMP)"#;

pub const SQL_CLAIM_MEMORY_RUN: &str = r#"WITH current_processing AS (
    SELECT id, 0 AS priority
    FROM (
        SELECT id
        FROM memory_runs
        WHERE prompt_version = $1
          AND status = 'processing'
          AND leased_until < CURRENT_TIMESTAMP
          AND attempts < 5
        ORDER BY range_end_at DESC,
                 range_start_at DESC,
                 cursor_after_at ASC,
                 cursor_after_message_id ASC,
                 id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ) current_processing
),
current_failed AS (
    SELECT id, 1 AS priority
    FROM (
        SELECT id
        FROM memory_runs
        WHERE prompt_version = $1
          AND status = 'failed'
          AND attempts < 5
          AND (leased_until IS NULL OR leased_until < CURRENT_TIMESTAMP)
          AND NOT EXISTS (SELECT 1 FROM current_processing)
        ORDER BY range_end_at DESC,
                 range_start_at DESC,
                 cursor_after_at ASC,
                 cursor_after_message_id ASC,
                 id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ) current_failed
),
current_queued AS (
    SELECT id, 2 AS priority
    FROM (
        SELECT id
        FROM memory_runs
        WHERE prompt_version = $1
          AND status = 'queued'
          AND (leased_until IS NULL OR leased_until < CURRENT_TIMESTAMP)
          AND NOT EXISTS (SELECT 1 FROM current_processing)
          AND NOT EXISTS (SELECT 1 FROM current_failed)
        ORDER BY range_end_at DESC,
                 range_start_at DESC,
                 cursor_after_at ASC,
                 cursor_after_message_id ASC,
                 id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ) current_queued
),
current_candidate AS (
    SELECT id, priority FROM current_processing
    UNION ALL
    SELECT id, priority FROM current_failed
    UNION ALL
    SELECT id, priority FROM current_queued
),
legacy_processing AS (
    SELECT id, 3 AS priority
    FROM (
        SELECT id
        FROM memory_runs
        WHERE prompt_version <> $1
          AND status = 'processing'
          AND leased_until < CURRENT_TIMESTAMP
          AND attempts < 5
          AND NOT EXISTS (SELECT 1 FROM current_candidate)
        ORDER BY range_end_at DESC,
                 range_start_at DESC,
                 cursor_after_at ASC,
                 cursor_after_message_id ASC,
                 id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ) legacy_processing
),
legacy_failed AS (
    SELECT id, 4 AS priority
    FROM (
        SELECT id
        FROM memory_runs
        WHERE prompt_version <> $1
          AND status = 'failed'
          AND attempts < 5
          AND (leased_until IS NULL OR leased_until < CURRENT_TIMESTAMP)
          AND NOT EXISTS (SELECT 1 FROM current_candidate)
          AND NOT EXISTS (SELECT 1 FROM legacy_processing)
        ORDER BY range_end_at DESC,
                 range_start_at DESC,
                 cursor_after_at ASC,
                 cursor_after_message_id ASC,
                 id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ) legacy_failed
),
legacy_queued AS (
    SELECT id, 5 AS priority
    FROM (
        SELECT id
        FROM memory_runs
        WHERE prompt_version <> $1
          AND status = 'queued'
          AND (leased_until IS NULL OR leased_until < CURRENT_TIMESTAMP)
          AND NOT EXISTS (SELECT 1 FROM current_candidate)
          AND NOT EXISTS (SELECT 1 FROM legacy_processing)
          AND NOT EXISTS (SELECT 1 FROM legacy_failed)
        ORDER BY range_end_at DESC,
                 range_start_at DESC,
                 cursor_after_at ASC,
                 cursor_after_message_id ASC,
                 id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ) legacy_queued
),
candidate AS (
    SELECT id
    FROM (
        SELECT id, priority
        FROM current_candidate
        UNION ALL
        SELECT id, priority
        FROM legacy_processing
        UNION ALL
        SELECT id, priority
        FROM legacy_failed
        UNION ALL
        SELECT id, priority
        FROM legacy_queued
    ) candidates
    ORDER BY priority ASC
    LIMIT 1
)
UPDATE memory_runs AS r
SET status = 'processing',
    lease_owner = $2,
    leased_until = $3,
    attempts = attempts + 1,
    started_at = CURRENT_TIMESTAMP,
    completed_at = NULL,
    error = '',
    updated_at = CURRENT_TIMESTAMP
FROM candidate
WHERE r.id = candidate.id
RETURNING r.id, r.chat_id, r.thread_id, r.range_start_at, r.range_end_at, r.prompt_version, r.cursor_after_at, r.cursor_after_message_id, r.cursor_after_entry_id, r.attempts, r.message_count"#;

pub const SQL_COMPLETE_MEMORY_RUN: &str = "UPDATE memory_runs SET status = 'completed', lease_owner = '', leased_until = NULL, cards_inserted = $2, cards_updated = $3, cards_superseded = $4, episodes_inserted = $5, input_token_estimate = $6, output_token_estimate = $7, error = '', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1";

pub const SQL_FAIL_MEMORY_RUN: &str = r#"UPDATE memory_runs
SET status = 'failed',
    lease_owner = '',
    leased_until = CASE
        WHEN attempts >= 5 THEN NULL
        ELSE CURRENT_TIMESTAMP + make_interval(secs => LEAST(3600, GREATEST(60, attempts * attempts * 60))::double precision)
    END,
    error = left($2, 4000),
    error_log = error_log || jsonb_build_array(jsonb_build_object(
        'attempt', attempts,
        'failed_at', CURRENT_TIMESTAMP,
        'error', left($2, 4000)
    )),
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1"#;

pub const SQL_ENQUEUE_MEMORY_RUN_CONTINUATION: &str = "INSERT INTO memory_runs (chat_id, thread_id, range_start_at, range_end_at, prompt_version, cursor_after_at, cursor_after_message_id, cursor_after_entry_id, message_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (chat_id, thread_id, range_start_at, range_end_at, prompt_version, cursor_after_at, cursor_after_message_id, cursor_after_entry_id) DO NOTHING";

pub const SQL_RETRY_MEMORY_RUN: &str = "UPDATE memory_runs SET status = 'queued', lease_owner = '', leased_until = NULL, attempts = 0, error = '', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status = 'failed'";

pub const SQL_RETRY_FAILED_MEMORY_RUNS: &str = "UPDATE memory_runs SET status = 'queued', lease_owner = '', leased_until = NULL, attempts = 0, error = '', updated_at = CURRENT_TIMESTAMP WHERE status = 'failed'";

pub const SQL_ENSURE_DAILY_MEMORY_RUNS: &str = r#"WITH grouped_windows AS (
    SELECT
        h.chat_id,
        h.thread_id,
        date_trunc('hour', h.occurred_at) AS range_start_at,
        count(*)::int AS message_count
    FROM chat_history_entries h
    WHERE h.bucket_day >= GREATEST($2::timestamptz, $4::timestamptz)::date
      AND h.bucket_day < $3::timestamptz::date
      AND h.occurred_at >= GREATEST($2::timestamptz, $4::timestamptz)
      AND h.occurred_at < $3::timestamptz
      AND h.kind = 'text'
    GROUP BY h.chat_id, h.thread_id, date_trunc('hour', h.occurred_at)
    HAVING count(*) > 0
),
active_windows AS (
    SELECT
        chat_id,
        thread_id,
        range_start_at,
        LEAST(range_start_at + interval '1 hour', $3::timestamptz) AS range_end_at,
        message_count
    FROM grouped_windows
)
INSERT INTO memory_runs (
    chat_id,
    thread_id,
    range_start_at,
    range_end_at,
    prompt_version,
    message_count
)
SELECT chat_id, thread_id, range_start_at, range_end_at, $1, message_count
FROM active_windows
ON CONFLICT (chat_id, thread_id, range_start_at, range_end_at, prompt_version, cursor_after_at, cursor_after_message_id, cursor_after_entry_id) DO NOTHING"#;

pub const SQL_SKIP_SUPERSEDED_MEMORY_RUNS: &str = "UPDATE memory_runs SET status = 'skipped', lease_owner = '', leased_until = NULL, error = '', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE prompt_version <> $1 AND (status IN ('queued', 'failed') OR (status = 'processing' AND leased_until < CURRENT_TIMESTAMP))";

pub const SQL_SELECT_MEMORY_RUN_MESSAGES: &str = r#"SELECT payload::text AS payload
FROM chat_history_entries AS e
WHERE e.chat_id = $1
  AND e.thread_id = $2
  AND e.occurred_at >= $3
  AND e.occurred_at < $4
  AND e.kind = 'text'
  AND (
    e.occurred_at > $5
    OR (e.occurred_at = $5 AND e.message_id > $6)
    OR (e.occurred_at = $5 AND e.message_id = $6 AND e.entry_id > $7)
  )
  AND e.occurred_at > COALESCE((
    SELECT r.reset_at
    FROM chat_history_resets AS r
    WHERE r.chat_id = $1 AND r.thread_id = $2
  ), '-infinity'::timestamptz)
ORDER BY e.occurred_at ASC, e.message_id ASC, e.entry_id ASC
LIMIT $8"#;

pub const SQL_LIST_VISIBLE_MEMORY_CARDS: &str = "SELECT id, visibility, card_type, status, subject, predicate, object, fact_text, confidence, salience, observation_count, origin_chat_id, origin_user_id, chat_id, thread_id, user_id, valid_from, valid_until, last_observed_at, last_used_at, use_count, created_at, updated_at FROM memory_cards WHERE status = 'active' AND valid_until IS NULL AND ((visibility = 'chat' AND chat_id = $1 AND thread_id = 0) OR (visibility = 'thread' AND chat_id = $1 AND thread_id = $2 AND $2 <> 0) OR (visibility = 'chat_user' AND chat_id = $1 AND user_id = $3) OR (visibility = 'private_chat' AND chat_id = $1 AND user_id = $3) OR (visibility = 'public_user' AND user_id = $3 AND $4::bool)) ORDER BY updated_at DESC LIMIT $5";

pub const SQL_LIST_MEMORY_CARDS: &str = "SELECT id, visibility, card_type, status, subject, predicate, object, fact_text, confidence, salience, observation_count, origin_chat_id, origin_user_id, chat_id, thread_id, user_id, valid_from, valid_until, last_observed_at, last_used_at, use_count, created_at, updated_at FROM memory_cards WHERE ($1::bigint = 0 OR chat_id = $1 OR origin_chat_id = $1) AND ($2::integer IS NULL OR thread_id = $2 OR origin_thread_id = $2) AND ($3::bigint = 0 OR user_id = $3 OR origin_user_id = $3) AND ($4::text = '' OR status = $4) ORDER BY updated_at DESC, id DESC LIMIT $5";

pub const SQL_LIST_MEMORY_RUNS: &str = "SELECT id, chat_id, thread_id, range_start_at, range_end_at, prompt_version, status, attempts, message_count, cards_inserted, cards_updated, cards_superseded, episodes_inserted, input_token_estimate, output_token_estimate, error, error_log::text AS error_log, created_at, updated_at FROM memory_runs ORDER BY range_start_at DESC, id DESC LIMIT $1";

pub const SQL_LIST_MEMORY_RUN_ANALYTICS: &str = r#"SELECT
    status,
    count(*)::int AS run_count,
    COALESCE(sum(message_count), 0)::int AS message_count,
    COALESCE(avg(input_token_estimate), 0)::int AS avg_input_tokens,
    COALESCE(max(input_token_estimate), 0)::int AS max_input_tokens,
    COALESCE(avg(output_token_estimate), 0)::int AS avg_output_tokens,
    COALESCE(max(output_token_estimate), 0)::int AS max_output_tokens,
    COALESCE(avg(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000) FILTER (WHERE completed_at IS NOT NULL AND started_at IS NOT NULL AND completed_at >= started_at AND completed_at - started_at < interval '1 day'), 0)::int AS avg_duration_ms,
    COALESCE(max(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000) FILTER (WHERE completed_at IS NOT NULL AND started_at IS NOT NULL AND completed_at >= started_at AND completed_at - started_at < interval '1 day'), 0)::int AS max_duration_ms,
    max(updated_at)::timestamptz AS latest_updated_at
FROM memory_runs
GROUP BY status
ORDER BY status"#;

pub const SQL_LIST_MEMORY_RUN_ERROR_ANALYTICS: &str = r#"SELECT
    left(error, 300)::text AS error,
    count(*)::int AS run_count,
    max(updated_at)::timestamptz AS latest_updated_at
FROM memory_runs
WHERE updated_at >= $1
  AND error <> ''
GROUP BY left(error, 300)
ORDER BY max(updated_at) DESC
LIMIT 10"#;

pub const SQL_GET_MEMORY_RUN_ANALYTICS_META: &str = r#"SELECT
    count(*) FILTER (WHERE status = 'processing' AND leased_until < CURRENT_TIMESTAMP)::int AS stale_processing_count,
    max(completed_at)::timestamptz AS latest_completed_at,
    max(updated_at)::timestamptz AS latest_updated_at,
    (max(updated_at) FILTER (WHERE input_token_estimate > 0 OR output_token_estimate > 0))::timestamptz AS latest_token_stats_at
FROM memory_runs"#;

pub const SQL_SOFT_DELETE_MEMORY_CARD: &str = "UPDATE memory_cards SET status = 'deleted', deleted_at = CURRENT_TIMESTAMP, deleted_by_user_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND status <> 'deleted'";

pub const SQL_SOFT_DELETE_VISIBLE_MEMORY_CARD: &str = "UPDATE memory_cards SET status = 'deleted', deleted_at = CURRENT_TIMESTAMP, deleted_by_user_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND status <> 'deleted' AND ((visibility = 'chat' AND chat_id = $3 AND thread_id = 0) OR (visibility = 'thread' AND chat_id = $3 AND thread_id = $4 AND $4 <> 0) OR (visibility = 'chat_user' AND chat_id = $3 AND user_id = $5) OR (visibility = 'private_chat' AND chat_id = $3 AND user_id = $5) OR (visibility = 'public_user' AND user_id = $5 AND $6::bool))";

pub const SQL_RETRIEVE_MEMORY_CARDS_LEXICAL: &str = "WITH q AS (SELECT websearch_to_tsquery('simple', $1) AS tsq) SELECT id, visibility, card_type, status, subject, predicate, object, fact_text, confidence, salience, observation_count, origin_chat_id, origin_user_id, chat_id, thread_id, user_id, valid_from, valid_until, last_observed_at, last_used_at, use_count, created_at, updated_at, (0.45 * ts_rank_cd(text_search, q.tsq) + 0.20 * salience + 0.20 * confidence + 0.15 * CASE WHEN updated_at > now() - interval '1 day' THEN 1 WHEN updated_at > now() - interval '7 days' THEN 0.75 WHEN updated_at > now() - interval '30 days' THEN 0.45 ELSE 0.2 END - 0.10 * decay_score)::double precision AS score FROM memory_cards, q WHERE status = 'active' AND valid_until IS NULL AND text_search @@ q.tsq AND ((visibility = 'chat' AND chat_id = $2 AND thread_id = 0) OR (visibility = 'thread' AND chat_id = $2 AND thread_id = $3 AND $3 <> 0) OR (visibility = 'chat_user' AND chat_id = $2 AND user_id = $4) OR (visibility = 'private_chat' AND chat_id = $2 AND user_id = $4) OR (visibility = 'public_user' AND user_id = $4 AND $5::bool)) ORDER BY score DESC, updated_at DESC LIMIT $6";

pub const SQL_RETRIEVE_MEMORY_CARDS_VECTOR: &str = "WITH q AS (SELECT $1::vector AS embedding) SELECT id, visibility, card_type, status, subject, predicate, object, fact_text, confidence, salience, observation_count, origin_chat_id, origin_user_id, chat_id, thread_id, user_id, valid_from, valid_until, last_observed_at, last_used_at, use_count, created_at, updated_at, (0.50 * (1 - (memory_cards.embedding <=> q.embedding)) + 0.20 * salience + 0.20 * confidence + 0.10 * CASE WHEN updated_at > now() - interval '1 day' THEN 1 WHEN updated_at > now() - interval '7 days' THEN 0.75 WHEN updated_at > now() - interval '30 days' THEN 0.45 ELSE 0.2 END - 0.10 * decay_score)::double precision AS score FROM memory_cards, q WHERE status = 'active' AND valid_until IS NULL AND memory_cards.embedding IS NOT NULL AND ((visibility = 'chat' AND chat_id = $2 AND thread_id = 0) OR (visibility = 'thread' AND chat_id = $2 AND thread_id = $3 AND $3 <> 0) OR (visibility = 'chat_user' AND chat_id = $2 AND user_id = $4) OR (visibility = 'private_chat' AND chat_id = $2 AND user_id = $4) OR (visibility = 'public_user' AND user_id = $4 AND $5::bool)) ORDER BY memory_cards.embedding <=> q.embedding LIMIT $6";

pub const SQL_RETRIEVE_MEMORY_EPISODES: &str = "WITH q AS (SELECT websearch_to_tsquery('simple', $1) AS tsq) SELECT id, visibility, chat_id, thread_id, range_start_at, range_end_at, message_count, summary_text, topics, participants, created_at FROM memory_episodes, q WHERE chat_id = $2 AND (thread_id = 0 OR thread_id = $3) AND text_search @@ q.tsq ORDER BY ts_rank_cd(text_search, q.tsq) DESC, range_end_at DESC LIMIT $4";

pub const SQL_CREATE_SHIELD_DOCUMENT: &str = "INSERT INTO shield_documents (slug, title, body, category, enabled, priority, embedding) VALUES ($1, $2, $3, $4, $5, $6, $7::vector) RETURNING id, slug, title, body, category, enabled, priority, created_at, updated_at";

pub const SQL_UPDATE_SHIELD_DOCUMENT: &str = "UPDATE shield_documents SET slug = $1, title = $2, body = $3, category = $4, enabled = $5, priority = $6, embedding = CASE WHEN $7::boolean THEN $8::vector ELSE embedding END, updated_at = CURRENT_TIMESTAMP WHERE id = $9 RETURNING id, slug, title, body, category, enabled, priority, created_at, updated_at";

pub const SQL_DELETE_SHIELD_DOCUMENT: &str = "DELETE FROM shield_documents WHERE id = $1";

pub const SQL_LIST_SHIELD_DOCUMENTS: &str = "SELECT id, slug, title, body, category, enabled, priority, created_at, updated_at FROM shield_documents WHERE ($1::boolean OR enabled) AND ($2::text IS NULL OR LOWER(slug) LIKE '%' || LOWER($2::text) || '%' OR LOWER(title) LIKE '%' || LOWER($2::text) || '%' OR LOWER(category) LIKE '%' || LOWER($2::text) || '%') ORDER BY priority DESC, updated_at DESC, id DESC LIMIT $3 OFFSET $4";

pub const SQL_SEARCH_SHIELD_DOCUMENTS_LEXICAL: &str = "WITH raw_terms AS (SELECT DISTINCT term FROM unnest(tsvector_to_array(to_tsvector('russian', $2::text))) AS t(term) WHERE char_length(term) >= 3 OR term ~ '^[0-9]{2,}$'), q AS (SELECT CASE WHEN count(*) = 0 THEN NULL::tsquery ELSE string_agg(quote_literal(term), ' | ' ORDER BY term)::tsquery END AS tsq FROM raw_terms) SELECT id, slug, title, body, category, enabled, priority, created_at, updated_at, ts_rank_cd(title_search, q.tsq)::double precision AS lexical_score FROM shield_documents, q WHERE enabled AND q.tsq IS NOT NULL AND title_search @@ q.tsq ORDER BY lexical_score DESC, priority DESC, updated_at DESC LIMIT $1";

pub const SQL_SEARCH_SHIELD_DOCUMENTS_VECTOR: &str = "WITH q AS (SELECT $1::vector AS embedding) SELECT id, slug, title, body, category, enabled, priority, created_at, updated_at, (1 - (shield_documents.embedding <=> q.embedding))::double precision AS vector_score FROM shield_documents, q WHERE enabled AND shield_documents.embedding IS NOT NULL ORDER BY shield_documents.embedding <=> q.embedding, priority DESC, updated_at DESC LIMIT $2";

pub const SQL_GET_SHIELD_DOCUMENTS_WITHOUT_EMBEDDINGS: &str = "SELECT id, slug, title, body, category, enabled, priority, created_at, updated_at FROM shield_documents WHERE embedding IS NULL ORDER BY priority DESC, id ASC LIMIT $1";

pub const SQL_UPDATE_SHIELD_DOCUMENT_EMBEDDING: &str = "UPDATE shield_documents SET embedding = $1::vector, updated_at = CURRENT_TIMESTAMP WHERE id = $2";

pub const SQL_CREATE_SUBSCRIPTION: &str = "INSERT INTO subscriptions (user_id, telegram_payment_charge_id, provider_payment_charge_id, expires_at) VALUES ($1, $2, $3, $4) ON CONFLICT (telegram_payment_charge_id) DO UPDATE SET expires_at = COALESCE(EXCLUDED.expires_at, subscriptions.expires_at), updated_at = CURRENT_TIMESTAMP RETURNING *";

pub const SQL_GET_ACTIVE_SUBSCRIPTION: &str = "SELECT * FROM subscriptions WHERE user_id = $1 AND expires_at > NOW() AND canceled_at IS NULL AND refunded_at IS NULL AND telegram_payment_charge_id NOT LIKE 'admin_grant_%' ORDER BY created_at DESC, id DESC LIMIT 1";

pub const SQL_LIST_SUBSCRIPTIONS_BY_USER: &str =
    "SELECT * FROM subscriptions WHERE user_id = $1 ORDER BY created_at DESC, id DESC";

pub const SQL_GET_SUBSCRIPTION_BY_TELEGRAM_PAYMENT_CHARGE_ID: &str =
    "SELECT * FROM subscriptions WHERE telegram_payment_charge_id = $1 LIMIT 1";

pub const SQL_DELETE_SUBSCRIPTION: &str = "DELETE FROM subscriptions WHERE id = $1 RETURNING *";

pub const SQL_EXPIRE_SUBSCRIPTION: &str = "UPDATE subscriptions SET expires_at = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *";

pub const SQL_MARK_SUBSCRIPTION_CANCELED: &str = "UPDATE subscriptions SET canceled_at = COALESCE(canceled_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *";

pub const SQL_MARK_SUBSCRIPTION_REFUNDED: &str = "UPDATE subscriptions SET refunded_at = COALESCE(refunded_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *";

pub const SQL_CREATE_DONATION: &str = "INSERT INTO donations (user_id, telegram_payment_charge_id, provider_payment_charge_id, amount_stars) VALUES ($1, $2, $3, $4) ON CONFLICT (telegram_payment_charge_id) DO NOTHING RETURNING *";

pub const SQL_GET_DONATION_BY_TELEGRAM_PAYMENT_CHARGE_ID: &str =
    "SELECT * FROM donations WHERE telegram_payment_charge_id = $1 LIMIT 1";

pub const SQL_DELETE_DONATION: &str = "DELETE FROM donations WHERE id = $1 RETURNING *";

pub const SQL_UPSERT_VIP_CACHE: &str = "INSERT INTO vip_cache (user_id, is_vip, expires_at) VALUES ($1, $2, $3) ON CONFLICT (user_id) DO UPDATE SET is_vip = COALESCE(EXCLUDED.is_vip, vip_cache.is_vip), expires_at = COALESCE(EXCLUDED.expires_at, vip_cache.expires_at), updated_at = CURRENT_TIMESTAMP";

pub const SQL_CREATE_VIP_EVENT: &str = "SELECT id, user_id, event_type, delta_seconds, effective_expires_at, subscription_id, actor_user_id, reason, created_at FROM vip_create_event($1, $2, $3, $4, $5, $6)";

pub const SQL_GET_VIP_SUMMARY_BY_USER: &str = "SELECT id AS latest_event_id, user_id, event_type AS latest_event_type, delta_seconds AS latest_delta_seconds, effective_expires_at, effective_expires_at > CURRENT_TIMESTAMP AS is_active, CASE WHEN effective_expires_at > CURRENT_TIMESTAMP THEN FLOOR(EXTRACT(EPOCH FROM (effective_expires_at - CURRENT_TIMESTAMP)))::bigint ELSE 0::bigint END AS remaining_seconds, subscription_id AS latest_subscription_id, actor_user_id AS latest_actor_user_id, reason AS latest_reason, created_at AS latest_created_at FROM vip_events WHERE user_id = $1 ORDER BY id DESC LIMIT 1";

pub const SQL_LIST_VIP_EVENTS_BY_USER: &str = "SELECT ve.id, ve.user_id, ve.event_type, ve.delta_seconds, ve.effective_expires_at, ve.subscription_id, ve.actor_user_id, actor.username AS actor_username, actor.first_name AS actor_first_name, ve.reason, ve.created_at, s.telegram_payment_charge_id, s.provider_payment_charge_id, s.expires_at AS subscription_expires_at, s.canceled_at AS subscription_canceled_at, s.refunded_at AS subscription_refunded_at FROM vip_events ve LEFT JOIN users actor ON actor.id = ve.actor_user_id LEFT JOIN subscriptions s ON s.id = ve.subscription_id WHERE ve.user_id = $1 ORDER BY ve.id DESC";

pub const SQL_GET_VIP_CACHE: &str =
    "SELECT * FROM vip_cache WHERE user_id = $1 AND expires_at > CURRENT_TIMESTAMP LIMIT 1";

pub const SQL_DELETE_VIP_CACHE: &str = "DELETE FROM vip_cache WHERE user_id = $1";

pub const SQL_CLEANUP_EXPIRED_VIP_CACHE: &str =
    "DELETE FROM vip_cache WHERE expires_at <= CURRENT_TIMESTAMP RETURNING user_id";

pub const RATE_LIMITED_CHAT_KEY_PREFIX: &str = "plotva:rate_limited_chat:";

pub const DRAW_RATE_LIMIT_KEY_PREFIX: &str = "plotva:rate_limit:";
pub const DRAW_RATE_LIMIT_TTL: Duration = Duration::from_secs(30 * 60);

pub const CHAT_ADMINS_KEY_PREFIX: &str = "chat:";

pub const CHAT_ADMINS_KEY_SUFFIX: &str = ":admins";

pub const EPHEMERAL_MESSAGE_KEY_PREFIX: &str = "ephemeral_messages:";

pub const QUEUED_STICKER_KEY_PREFIX: &str = "queued_sticker:";

pub const QUEUED_STICKER_TTL: Duration = Duration::from_secs(60 * 60);

pub const LAST_GENERATION_KEY_PREFIX: &str = "last_gen:";

pub const LAST_GENERATION_TTL: Duration = Duration::from_secs(24 * 60 * 60);

pub const TRANSLATION_CACHE_KEY_PREFIX: &str = "plotva:t8:";

pub const TRANSLATION_CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

pub const BLOCKED_CHAT_KEY_PREFIX: &str = "plotva:blocked_chat:";

pub const BLOCKED_CHAT_TTL: Duration = Duration::from_secs(10 * 60);

pub const JOIN_GREETING_USERS_TTL: Duration = Duration::from_secs(10 * 60);

pub const JOIN_GREETING_DEBOUNCE_TTL: Duration = Duration::from_secs(30);

pub const JOIN_GREETING_MESSAGE_TTL: Duration = Duration::from_secs(10 * 60);

pub const EPHEMERAL_MESSAGE_PATTERN: &str = "ephemeral_messages:*";

pub const EPHEMERAL_CLEANUP_BATCH_SIZE: usize = 10;

pub const EPHEMERAL_DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(15);

pub const CHAT_HISTORY_CACHE_KEY_PREFIX: &str = "plotva:chat_history_cache:v2:";

pub const CHAT_MEMBER_STATUS_MEMBER: &str = "member";

pub const CHAT_MEMBER_STATUS_ADMINISTRATOR: &str = "administrator";

pub const CHAT_MEMBER_STATUS_CREATOR: &str = "creator";

pub const CHAT_MEMBER_STATUS_LEFT: &str = "left";

pub const CHAT_MEMBER_STATUS_KICKED: &str = "kicked";

pub static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

const LEGACY_MIGRATION_TABLE_EXISTS_SQL: &str = "SELECT to_regclass('gorp_migrations') IS NOT NULL";
const SQL_LIST_LEGACY_MIGRATION_IDS: &str =
    "SELECT id FROM gorp_migrations ORDER BY applied_at ASC, id ASC";
const SQL_ENSURE_SQLX_MIGRATIONS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS _sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    success BOOLEAN NOT NULL,
    checksum BYTEA NOT NULL,
    execution_time BIGINT NOT NULL
);
"#;
const SQL_INSERT_BRIDGED_SQLX_MIGRATION: &str = r#"
INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum, execution_time)
SELECT $1, $2, COALESCE((SELECT applied_at FROM gorp_migrations WHERE id = $3), now()), TRUE, $4, 0
ON CONFLICT (version) DO NOTHING
"#;
#[derive(Clone, Copy, Debug)]
struct LegacyMigrationBridgeEntry<'a> {
    legacy_id: &'a str,
    migration: &'a Migration,
}

/// Connected service clients kept alive by the application shell.
#[derive(Clone, Debug)]
pub struct ServiceClients {
    /// SQLx Postgres pool.
    pub postgres: PgPool,
    /// Redis client that has passed a startup ping.
    pub redis: RedisStore,
    /// Whether SQLx migrations were applied during startup.
    pub migrations_applied: bool,
}

/// Redis client wrapper.
#[derive(Clone, Debug)]
pub struct RedisStore {
    connections: RedisConnectionPool,
}

#[derive(Clone, Debug)]
struct RedisConnectionPool {
    client: RedisClient,
    manager: Arc<OnceCell<ConnectionManager>>,
}

impl RedisConnectionPool {
    fn new(client: RedisClient) -> Self {
        Self {
            client,
            manager: Arc::new(OnceCell::new()),
        }
    }

    fn client(&self) -> &RedisClient {
        &self.client
    }

    async fn connection(&self) -> redis::RedisResult<ConnectionManager> {
        self.manager
            .get_or_try_init(|| async { self.client.get_connection_manager().await })
            .await
            .cloned()
    }

    #[cfg(test)]
    fn shares_manager_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.manager, &other.manager)
    }
}

/// Persisted ephemeral Telegram message lifecycle record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EphemeralMessage {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram message ID.
    pub message_id: i64,
    /// Instant after which cleanup should try deleting the message.
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LastGenerationRecord {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Generated Telegram message IDs in frame order.
    pub message_ids: Vec<i64>,
    /// Generation caption.
    pub caption: String,
    /// Unix creation timestamp in seconds.
    pub created_at: i64,
}

pub const TELEGRAM_FILE_VISION_STATUS_PENDING: &str = "pending";

pub const TELEGRAM_FILE_VISION_STATUS_PROCESSING: &str = "processing";

pub const TELEGRAM_FILE_VISION_STATUS_COMPLETED: &str = "completed";

pub const TELEGRAM_FILE_VISION_STATUS_FAILED: &str = "failed";

pub const TELEGRAM_FILE_VISION_REQUEST_TIMEOUT: Duration = Duration::from_secs(2 * 60);

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramFileMetadataUpsert {
    /// Telegram stable unique file ID.
    pub file_unique_id: String,
    /// Latest downloadable Telegram file ID.
    pub latest_file_id: String,
    pub media_kind: String,
    /// MIME type when Telegram provides it.
    pub mime_type: Option<String>,
    /// Image width.
    pub width: Option<i32>,
    /// Image height.
    pub height: Option<i32>,
    /// File size in bytes.
    pub file_size: Option<i64>,
    /// First seen chat ID.
    pub first_seen_chat_id: Option<i64>,
    /// First seen message ID.
    pub first_seen_message_id: Option<i64>,
    /// Last seen chat ID.
    pub last_seen_chat_id: Option<i64>,
    /// Last seen message ID.
    pub last_seen_message_id: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TelegramFileVisionUpdate {
    /// Telegram stable unique file ID.
    pub file_unique_id: String,
    /// New vision status.
    pub vision_status: String,
    pub vision_caption: Option<String>,
    pub vision_model: Option<String>,
    pub vision_latency_ms: Option<i32>,
    pub recognition_requested_at: Option<OffsetDateTime>,
    pub recognition_completed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TelegramFileRecord {
    /// Telegram stable unique file ID.
    pub file_unique_id: String,
    /// Latest downloadable Telegram file ID.
    pub latest_file_id: String,
    pub media_kind: String,
    /// MIME type.
    pub mime_type: Option<String>,
    /// Image width.
    pub width: Option<i32>,
    /// Image height.
    pub height: Option<i32>,
    /// File size in bytes.
    pub file_size: Option<i64>,
    /// First seen chat ID.
    pub first_seen_chat_id: Option<i64>,
    /// First seen message ID.
    pub first_seen_message_id: Option<i64>,
    /// Last seen chat ID.
    pub last_seen_chat_id: Option<i64>,
    /// Last seen message ID.
    pub last_seen_message_id: Option<i64>,
    /// Last observed timestamp.
    pub last_seen_at: OffsetDateTime,
    /// Vision status.
    pub vision_status: String,
    /// Vision caption.
    pub vision_caption: Option<String>,
    /// Vision model.
    pub vision_model: Option<String>,
    /// Vision latency in milliseconds.
    pub vision_latency_ms: Option<i32>,
    /// Recognition request timestamp.
    pub recognition_requested_at: Option<OffsetDateTime>,
    /// Recognition completion timestamp.
    pub recognition_completed_at: Option<OffsetDateTime>,
    /// Extra JSONB payload.
    pub extra: serde_json::Value,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
    /// Update timestamp.
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramStoredFileRef {
    /// Latest downloadable Telegram file ID.
    pub latest_file_id: String,
    /// Telegram stable unique file ID.
    pub file_unique_id: String,
    pub media_kind: String,
    /// MIME type.
    pub mime_type: String,
    /// Image width.
    pub width: i32,
    /// Image height.
    pub height: i32,
    /// File size in bytes.
    pub file_size: i64,
    /// Last seen chat ID.
    pub chat_id: i64,
    /// Last seen message ID.
    pub message_id: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VisionDescriptionUpdate {
    /// Telegram stable unique file ID.
    pub file_unique_id: String,
    /// Generated caption.
    pub caption: String,
}

#[must_use]
pub fn telegram_file_completed_caption(record: Option<&TelegramFileRecord>) -> String {
    let Some(record) = record else {
        return String::new();
    };
    if record.vision_status != TELEGRAM_FILE_VISION_STATUS_COMPLETED {
        return String::new();
    }
    record
        .vision_caption
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_owned()
}

#[must_use]
pub fn telegram_file_vision_caption_pending_at(
    record: Option<&TelegramFileRecord>,
    now: OffsetDateTime,
) -> bool {
    let Some(record) = record else {
        return false;
    };
    if record.vision_status != TELEGRAM_FILE_VISION_STATUS_PROCESSING {
        return false;
    }
    record
        .recognition_requested_at
        .is_some_and(|requested_at| requested_at + TELEGRAM_FILE_VISION_REQUEST_TIMEOUT > now)
}

#[must_use]
pub fn telegram_file_ref_from_record(record: Option<&TelegramFileRecord>) -> TelegramStoredFileRef {
    let Some(record) = record else {
        return TelegramStoredFileRef::default();
    };
    TelegramStoredFileRef {
        latest_file_id: record.latest_file_id.trim().to_owned(),
        file_unique_id: record.file_unique_id.trim().to_owned(),
        media_kind: record.media_kind.trim().to_owned(),
        mime_type: record
            .mime_type
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_owned(),
        width: record.width.unwrap_or_default(),
        height: record.height.unwrap_or_default(),
        file_size: record.file_size.unwrap_or_default(),
        chat_id: record.last_seen_chat_id.unwrap_or_default(),
        message_id: record.last_seen_message_id.unwrap_or_default(),
    }
}

impl RedisStore {
    pub fn from_client(client: RedisClient) -> Self {
        Self {
            connections: RedisConnectionPool::new(client),
        }
    }

    /// Access the underlying Redis client.
    pub fn client(&self) -> &RedisClient {
        self.connections.client()
    }

    /// Build the Redis-backed rate-limit store over this client.
    pub fn rate_limit_store(&self) -> RedisRateLimitStore {
        RedisRateLimitStore::with_connection_pool(
            self.connections.clone(),
            RATE_LIMITED_CHAT_KEY_PREFIX,
        )
    }

    /// Build the Redis-backed draw rate-limit store over this client.
    pub fn draw_rate_limit_store(&self) -> RedisDrawRateLimitStore {
        RedisDrawRateLimitStore::with_connection_pool(
            self.connections.clone(),
            DRAW_RATE_LIMIT_KEY_PREFIX,
        )
    }

    /// Build the Redis-backed chat-admin cache store over this client.
    pub fn chat_admin_cache_store(&self) -> RedisChatAdminCacheStore {
        RedisChatAdminCacheStore::with_connection_pool(
            self.connections.clone(),
            CHAT_ADMINS_KEY_PREFIX,
        )
    }

    /// Build the Redis-backed ephemeral-message store over this client.
    pub fn ephemeral_message_store(&self) -> RedisEphemeralMessageStore {
        RedisEphemeralMessageStore::with_connection_pool(
            self.connections.clone(),
            EPHEMERAL_MESSAGE_KEY_PREFIX,
        )
    }

    /// Build the Redis-backed queued-sticker store over this client.
    pub fn queued_sticker_store(&self) -> RedisQueuedStickerStore {
        RedisQueuedStickerStore::with_connection_pool(
            self.connections.clone(),
            QUEUED_STICKER_KEY_PREFIX,
        )
    }

    /// Build the Redis-backed last-generation store over this client.
    pub fn last_generation_store(&self) -> RedisLastGenerationStore {
        RedisLastGenerationStore::with_connection_pool(
            self.connections.clone(),
            LAST_GENERATION_KEY_PREFIX,
        )
    }

    /// Build the Redis-backed translation cache store over this client.
    pub fn translation_cache_store(&self) -> RedisTranslationCacheStore {
        RedisTranslationCacheStore::with_connection_pool(
            self.connections.clone(),
            TRANSLATION_CACHE_KEY_PREFIX,
        )
    }

    /// Build the Redis-backed blocked-chat store over this client.
    pub fn blocked_chat_store(&self) -> RedisBlockedChatStore {
        RedisBlockedChatStore::with_connection_pool(
            self.connections.clone(),
            BLOCKED_CHAT_KEY_PREFIX,
        )
    }

    /// Build the Redis-backed join-greeting store over this client.
    pub fn join_greeting_store(&self) -> RedisJoinGreetingStore {
        RedisJoinGreetingStore::with_connection_pool(self.connections.clone())
    }
}

#[derive(Clone, Debug)]
pub struct RedisRateLimitStore {
    connections: RedisConnectionPool,
    key_prefix: String,
}

impl RedisRateLimitStore {
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, RATE_LIMITED_CHAT_KEY_PREFIX)
    }

    /// Build a rate-limit store with an explicit key prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self::with_connection_pool(RedisConnectionPool::new(client), key_prefix)
    }

    fn with_connection_pool(
        connections: RedisConnectionPool,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self {
            connections,
            key_prefix: key_prefix.into(),
        }
    }

    /// Return the Redis key this store uses for one chat.
    pub fn key_for_chat(&self, chat_id: i64) -> String {
        format!("{}{chat_id}", self.key_prefix)
    }

    pub async fn set_chat_rate_limit(
        &self,
        chat_id: i64,
        expiry: OffsetDateTime,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let value = rate_limit_expiry_redis_value(expiry)?;
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command.arg(self.key_for_chat(chat_id)).arg(value);
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }

    /// Load one chat's persisted rate-limit expiry.
    pub async fn chat_rate_limit_expiry(
        &self,
        chat_id: i64,
    ) -> Result<Option<OffsetDateTime>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key_for_chat(chat_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        value
            .as_deref()
            .map(rate_limit_expiry_from_redis_value)
            .transpose()
    }

    /// Return whether a chat is still rate-limited at `now`.
    pub async fn chat_is_rate_limited_at(
        &self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> Result<bool, StorageError> {
        let expiry = self.chat_rate_limit_expiry(chat_id).await?;
        Ok(rate_limit_is_active_at(expiry, now))
    }
}

#[derive(Clone, Debug)]
pub struct RedisDrawRateLimitStore {
    connections: RedisConnectionPool,
    key_prefix: String,
}

impl RedisDrawRateLimitStore {
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, DRAW_RATE_LIMIT_KEY_PREFIX)
    }

    /// Build a draw rate-limit store with an explicit key prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self::with_connection_pool(RedisConnectionPool::new(client), key_prefix)
    }

    fn with_connection_pool(
        connections: RedisConnectionPool,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self {
            connections,
            key_prefix: key_prefix.into(),
        }
    }

    /// Return the Redis key this store uses for one user.
    pub fn key_for_user(&self, user_id: i64) -> String {
        format!("{}{user_id}", self.key_prefix)
    }

    /// Load one user's draw-generation timestamps.
    pub async fn draw_rate_limit_timestamps(
        &self,
        user_id: i64,
    ) -> Result<Vec<OffsetDateTime>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key_for_user(user_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        match value {
            Some(value) => draw_rate_limit_timestamps_from_redis_value(&value),
            None => Ok(Vec::new()),
        }
    }

    pub async fn set_draw_rate_limit_timestamps_default_ttl(
        &self,
        user_id: i64,
        timestamps: &[OffsetDateTime],
    ) -> Result<(), StorageError> {
        self.set_draw_rate_limit_timestamps(user_id, timestamps, DRAW_RATE_LIMIT_TTL)
            .await
    }

    /// Persist one user's draw-generation timestamps with an explicit TTL.
    pub async fn set_draw_rate_limit_timestamps(
        &self,
        user_id: i64,
        timestamps: &[OffsetDateTime],
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let value = draw_rate_limit_timestamps_redis_value(timestamps)?;
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command.arg(self.key_for_user(user_id)).arg(value);
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RedisChatAdminCacheStore {
    connections: RedisConnectionPool,
    key_prefix: String,
    key_suffix: String,
}

impl RedisChatAdminCacheStore {
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, CHAT_ADMINS_KEY_PREFIX)
    }

    /// Build a chat-admin cache store with an explicit prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self::with_connection_pool(RedisConnectionPool::new(client), key_prefix)
    }

    fn with_connection_pool(
        connections: RedisConnectionPool,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self {
            connections,
            key_prefix: key_prefix.into(),
            key_suffix: CHAT_ADMINS_KEY_SUFFIX.to_owned(),
        }
    }

    /// Return the Redis key this store uses for one chat.
    pub fn key_for_chat(&self, chat_id: i64) -> String {
        format!("{}{chat_id}{}", self.key_prefix, self.key_suffix)
    }

    pub async fn set_chat_admin_ids(
        &self,
        chat_id: i64,
        admin_ids: &[i64],
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let value = chat_admin_ids_redis_value(admin_ids)?;
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command.arg(self.key_for_chat(chat_id)).arg(value);
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }

    /// Load cached Telegram admin IDs for one chat.
    pub async fn chat_admin_ids(&self, chat_id: i64) -> Result<Option<Vec<i64>>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key_for_chat(chat_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        value
            .as_deref()
            .map(chat_admin_ids_from_redis_value)
            .transpose()
    }
}

#[derive(Clone, Debug)]
pub struct RedisQueuedStickerStore {
    connections: RedisConnectionPool,
    key_prefix: String,
}

impl RedisQueuedStickerStore {
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, QUEUED_STICKER_KEY_PREFIX)
    }

    /// Build a queued-sticker store with an explicit prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self::with_connection_pool(RedisConnectionPool::new(client), key_prefix)
    }

    fn with_connection_pool(
        connections: RedisConnectionPool,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self {
            connections,
            key_prefix: key_prefix.into(),
        }
    }

    /// Return the Redis key this store uses for one source message.
    pub fn key_for_message(&self, chat_id: i64, message_id: i64) -> String {
        queued_sticker_key_with_prefix(&self.key_prefix, chat_id, message_id)
    }

    pub async fn set_queued_sticker_message_id(
        &self,
        chat_id: i64,
        message_id: i64,
        sticker_message_id: i64,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command
            .arg(self.key_for_message(chat_id, message_id))
            .arg(sticker_message_id.to_string());
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }

    /// Load one queued sticker ID.
    pub async fn queued_sticker_message_id(
        &self,
        chat_id: i64,
        message_id: i64,
    ) -> Result<Option<i64>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<String> = redis::cmd("GET")
            .arg(self.key_for_message(chat_id, message_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(queued_sticker_message_id_from_redis_value(value))
    }

    /// Delete one queued sticker record.
    pub async fn delete_queued_sticker(
        &self,
        chat_id: i64,
        message_id: i64,
    ) -> Result<(), StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let _: i64 = redis::cmd("DEL")
            .arg(self.key_for_message(chat_id, message_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RedisEphemeralMessageStore {
    connections: RedisConnectionPool,
    key_prefix: String,
}

impl RedisEphemeralMessageStore {
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, EPHEMERAL_MESSAGE_KEY_PREFIX)
    }

    /// Build an ephemeral-message store with an explicit prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self::with_connection_pool(RedisConnectionPool::new(client), key_prefix)
    }

    fn with_connection_pool(
        connections: RedisConnectionPool,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self {
            connections,
            key_prefix: key_prefix.into(),
        }
    }

    /// Return the Redis key this store uses for one Telegram message.
    pub fn key_for_message(&self, chat_id: i64, message_id: i64) -> String {
        format!("{}{chat_id}:{message_id}", self.key_prefix)
    }

    pub async fn set_ephemeral_message(
        &self,
        message: &EphemeralMessage,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let value = ephemeral_message_redis_value(message)?;
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command
            .arg(self.key_for_message(message.chat_id, message.message_id))
            .arg(value);
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }

    /// Load one persisted ephemeral message.
    pub async fn ephemeral_message(
        &self,
        chat_id: i64,
        message_id: i64,
    ) -> Result<Option<EphemeralMessage>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key_for_message(chat_id, message_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        value
            .as_deref()
            .map(ephemeral_message_from_redis_value)
            .transpose()
    }

    pub async fn ephemeral_messages(&self) -> Result<Vec<EphemeralMessage>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut cursor = 0_u64;
        let mut messages = Vec::new();
        let pattern = format!("{}*", self.key_prefix);

        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100_u64)
                .query_async(&mut connection)
                .await
                .map_err(|source| StorageError::Redis { source })?;

            if !keys.is_empty() {
                let values: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
                    .arg(&keys)
                    .query_async(&mut connection)
                    .await
                    .map_err(|source| StorageError::Redis { source })?;
                for value in values.into_iter().flatten() {
                    messages.push(ephemeral_message_from_redis_value(&value)?);
                }
            }

            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(messages)
    }

    /// Delete persisted ephemeral-message records after Telegram delete attempts.
    pub async fn delete_ephemeral_messages(
        &self,
        messages: &[EphemeralMessage],
    ) -> Result<(), StorageError> {
        if messages.is_empty() {
            return Ok(());
        }
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let keys = messages
            .iter()
            .map(|message| self.key_for_message(message.chat_id, message.message_id));
        let _: i64 = redis::cmd("DEL")
            .arg(keys.collect::<Vec<_>>())
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RedisLastGenerationStore {
    connections: RedisConnectionPool,
    key_prefix: String,
}

impl RedisLastGenerationStore {
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, LAST_GENERATION_KEY_PREFIX)
    }

    /// Build a last-generation store with an explicit key prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self::with_connection_pool(RedisConnectionPool::new(client), key_prefix)
    }

    fn with_connection_pool(
        connections: RedisConnectionPool,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self {
            connections,
            key_prefix: key_prefix.into(),
        }
    }

    /// Return the Redis key this store uses for one chat/user pair.
    pub fn key_for_generation(&self, chat_id: i64, user_id: i64) -> String {
        format!("{}{chat_id}:{user_id}", self.key_prefix)
    }

    pub async fn set_last_generation(
        &self,
        generation: &LastGenerationRecord,
    ) -> Result<(), StorageError> {
        self.set_last_generation_with_ttl(generation, LAST_GENERATION_TTL)
            .await
    }

    /// Persist the last generation with an explicit TTL.
    pub async fn set_last_generation_with_ttl(
        &self,
        generation: &LastGenerationRecord,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let value = last_generation_redis_value(generation)?;
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command
            .arg(self.key_for_generation(generation.chat_id, generation.user_id))
            .arg(value);
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }

    /// Load the last generation for one chat/user pair.
    pub async fn last_generation(
        &self,
        chat_id: i64,
        user_id: i64,
    ) -> Result<Option<LastGenerationRecord>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key_for_generation(chat_id, user_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        value
            .as_deref()
            .map(last_generation_from_redis_value)
            .transpose()
    }

    /// Delete the last-generation value for one chat/user pair.
    pub async fn delete_last_generation(
        &self,
        chat_id: i64,
        user_id: i64,
    ) -> Result<(), StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let _: i64 = redis::cmd("DEL")
            .arg(self.key_for_generation(chat_id, user_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RedisTranslationCacheStore {
    connections: RedisConnectionPool,
    key_prefix: String,
}

impl RedisTranslationCacheStore {
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, TRANSLATION_CACHE_KEY_PREFIX)
    }

    /// Build a translation cache store with an explicit prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self::with_connection_pool(RedisConnectionPool::new(client), key_prefix)
    }

    fn with_connection_pool(
        connections: RedisConnectionPool,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self {
            connections,
            key_prefix: key_prefix.into(),
        }
    }

    /// Return the Redis key this store uses for one target/text pair.
    pub fn key_for_translation(&self, target_lang: &str, text: &str) -> String {
        translation_cache_key_with_prefix(&self.key_prefix, target_lang, text)
    }

    /// Load one cached translation.
    pub async fn cached_translation(
        &self,
        target_lang: &str,
        text: &str,
    ) -> Result<Option<String>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key_for_translation(target_lang, text))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        value
            .as_deref()
            .map(translation_cache_from_redis_value)
            .transpose()
    }

    pub async fn set_cached_translation(
        &self,
        target_lang: &str,
        text: &str,
        translation: &str,
    ) -> Result<(), StorageError> {
        self.set_cached_translation_with_ttl(target_lang, text, translation, TRANSLATION_CACHE_TTL)
            .await
    }

    /// Persist one cached translation with an explicit TTL.
    pub async fn set_cached_translation_with_ttl(
        &self,
        target_lang: &str,
        text: &str,
        translation: &str,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let value = translation_cache_redis_value(translation)?;
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command
            .arg(self.key_for_translation(target_lang, text))
            .arg(value);
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RedisBlockedChatStore {
    connections: RedisConnectionPool,
    key_prefix: String,
}

impl RedisBlockedChatStore {
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, BLOCKED_CHAT_KEY_PREFIX)
    }

    /// Build a blocked-chat store with an explicit prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self::with_connection_pool(RedisConnectionPool::new(client), key_prefix)
    }

    fn with_connection_pool(
        connections: RedisConnectionPool,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self {
            connections,
            key_prefix: key_prefix.into(),
        }
    }

    /// Return the Redis key this store uses for one chat.
    pub fn key_for_chat(&self, chat_id: i64) -> String {
        format!("{}{chat_id}", self.key_prefix)
    }

    pub async fn block_chat_until(
        &self,
        chat_id: i64,
        unblock_at: OffsetDateTime,
    ) -> Result<(), StorageError> {
        self.block_chat_until_with_ttl(chat_id, unblock_at, BLOCKED_CHAT_TTL)
            .await
    }

    /// Persist one blocked-chat window with an explicit TTL.
    pub async fn block_chat_until_with_ttl(
        &self,
        chat_id: i64,
        unblock_at: OffsetDateTime,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let value = blocked_chat_redis_value(unblock_at)?;
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command.arg(self.key_for_chat(chat_id)).arg(value);
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }

    /// Load the stored unblock time for one chat.
    pub async fn blocked_until(
        &self,
        chat_id: i64,
    ) -> Result<Option<OffsetDateTime>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key_for_chat(chat_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        value
            .as_deref()
            .map(blocked_chat_from_redis_value)
            .transpose()
    }

    pub async fn is_chat_blocked_at(
        &self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> Result<bool, StorageError> {
        Ok(blocked_chat_is_active_at(
            self.blocked_until(chat_id).await?,
            now,
        ))
    }
}

#[derive(Clone, Debug)]
pub struct RedisJoinGreetingStore {
    connections: RedisConnectionPool,
}

impl RedisJoinGreetingStore {
    pub fn new(client: RedisClient) -> Self {
        Self::with_connection_pool(RedisConnectionPool::new(client))
    }

    fn with_connection_pool(connections: RedisConnectionPool) -> Self {
        Self { connections }
    }

    pub fn users_key(chat_id: i64) -> String {
        join_greeting_users_key(chat_id)
    }

    pub fn message_key(chat_id: i64) -> String {
        join_greeting_message_key(chat_id)
    }

    pub fn debounce_key(chat_id: i64) -> String {
        join_greeting_debounce_key(chat_id)
    }

    pub async fn record_join_member_ids(
        &self,
        chat_id: i64,
        user_ids: &[i64],
        score: i64,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        if user_ids.is_empty() {
            return Ok(());
        }
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let key = Self::users_key(chat_id);
        let mut zadd = redis::cmd("ZADD");
        zadd.arg(&key);
        for user_id in user_ids {
            zadd.arg(score).arg(user_id.to_string());
        }
        let _: i64 = zadd
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        if !ttl.is_zero() {
            let _: bool = redis::cmd("EXPIRE")
                .arg(&key)
                .arg(redis_ttl_seconds(ttl))
                .query_async(&mut connection)
                .await
                .map_err(|source| StorageError::Redis { source })?;
        }
        Ok(())
    }

    pub async fn start_debounce(&self, chat_id: i64, ttl: Duration) -> Result<bool, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command.arg(Self::debounce_key(chat_id)).arg("1").arg("NX");
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let value: Option<String> = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(value.is_some())
    }

    /// Load user ID strings whose join score is strictly newer than `min_score`.
    pub async fn recent_join_member_ids(
        &self,
        chat_id: i64,
        min_score: i64,
    ) -> Result<Vec<String>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        redis::cmd("ZRANGEBYSCORE")
            .arg(Self::users_key(chat_id))
            .arg(format!("({min_score}"))
            .arg("+inf")
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })
    }

    pub async fn previous_greeting_message_id(
        &self,
        chat_id: i64,
    ) -> Result<Option<i32>, StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<String> = redis::cmd("GET")
            .arg(Self::message_key(chat_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(value.and_then(|value| value.parse::<i32>().ok()))
    }

    pub async fn set_previous_greeting_message_id(
        &self,
        chat_id: i64,
        message_id: i32,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command
            .arg(Self::message_key(chat_id))
            .arg(message_id.to_string());
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PostgresRuntimeTokenStore {
    pool: PgPool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeApiTokenRecord {
    /// Token ID, without the `prt_` prefix.
    pub id: String,
    /// SHA-256 token-secret hash.
    pub token_hash: Vec<u8>,
    /// Token creation timestamp.
    pub created_at: OffsetDateTime,
}

impl PostgresRuntimeTokenStore {
    /// Build a runtime-token store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn create_runtime_api_token(
        &self,
        id: &str,
        token_hash: &[u8],
    ) -> Result<RuntimeApiTokenRecord, StorageError> {
        let row = sqlx::query(SQL_CREATE_RUNTIME_API_TOKEN)
            .bind(id)
            .bind(token_hash)
            .fetch_one(&self.pool)
            .await?;
        runtime_api_token_from_row(row).map_err(StorageError::from)
    }

    /// Load a runtime API token row by ID.
    pub async fn get_runtime_api_token(
        &self,
        id: &str,
    ) -> Result<Option<RuntimeApiTokenRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_RUNTIME_API_TOKEN)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(runtime_api_token_from_row).transpose()?)
    }

    pub async fn list_runtime_api_tokens_created_since(
        &self,
        cutoff: OffsetDateTime,
    ) -> Result<Vec<RuntimeApiTokenRecord>, StorageError> {
        let rows = sqlx::query(SQL_LIST_RUNTIME_API_TOKENS_CREATED_SINCE)
            .bind(cutoff)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(runtime_api_token_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    /// Delete expired runtime API tokens and return the affected row count.
    pub async fn delete_runtime_api_tokens_older_than(
        &self,
        cutoff: OffsetDateTime,
    ) -> Result<u64, StorageError> {
        let result = sqlx::query(SQL_DELETE_RUNTIME_API_TOKENS_OLDER_THAN)
            .bind(cutoff)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Clone, Debug)]
pub struct PostgresVirtualMessageStore {
    pool: PgPool,
}

impl PostgresVirtualMessageStore {
    /// Build a virtual-message store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn upsert_user_state(&self, user: &UserState) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_USER)
            .bind(user.id)
            .bind(&user.first_name)
            .bind(user.last_name.as_deref())
            .bind(user.username.as_deref())
            .bind(user.language_code.as_deref())
            .bind(user.is_premium)
            .bind(Option::<&str>::None)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_user_id_by_username(
        &self,
        username: &str,
    ) -> Result<Option<i64>, StorageError> {
        let user_id = sqlx::query_scalar::<_, i64>(SQL_GET_USER_ID_BY_USERNAME)
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        Ok(user_id)
    }

    pub async fn get_user_state(&self, user_id: i64) -> Result<Option<UserState>, StorageError> {
        let row = sqlx::query(SQL_GET_USER)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(user_state_from_row).transpose()?)
    }

    pub async fn upsert_chat_state(&self, chat: &ChatState) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_CHAT)
            .bind(chat.id)
            .bind(&chat.chat_type)
            .bind(chat.title.as_deref())
            .bind(chat.username.as_deref())
            .bind(chat.first_name.as_deref())
            .bind(chat.last_name.as_deref())
            .bind(chat.is_forum)
            .bind(Option::<&str>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<i64>::None)
            .bind(Option::<i64>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<i64>::None)
            .bind(Option::<&str>::None)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn insert_virtual_message(
        &self,
        vmsg_id: &str,
        chat_id: i64,
        thread_id: Option<i32>,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_INSERT_VIRTUAL_MESSAGE)
            .bind(vmsg_id)
            .bind(chat_id)
            .bind(thread_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Resolve a virtual message to its real Telegram message ID.
    pub async fn resolve_virtual_message(
        &self,
        vmsg_id: &str,
        real_message_id: i32,
        resolved_at: Option<OffsetDateTime>,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_RESOLVE_VIRTUAL_MESSAGE)
            .bind(real_message_id)
            .bind(resolved_at)
            .bind(vmsg_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Get a virtual-message mapping by virtual ID.
    pub async fn get_mapping_by_virtual(
        &self,
        vmsg_id: &str,
    ) -> Result<Option<MessageIdMapping>, StorageError> {
        let row = sqlx::query(SQL_GET_MAPPING_BY_VIRTUAL)
            .bind(vmsg_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(message_id_mapping_from_row).transpose()?)
    }

    /// List virtual-message mappings by virtual IDs.
    pub async fn list_mappings_by_virtual_ids(
        &self,
        vmsg_ids: &[String],
    ) -> Result<Vec<MessageIdMapping>, StorageError> {
        let rows = sqlx::query(SQL_LIST_MAPPINGS_BY_VIRTUAL_IDS)
            .bind(vmsg_ids)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(message_id_mapping_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    /// Get a virtual-message mapping by real Telegram message ID.
    pub async fn get_mapping_by_real(
        &self,
        chat_id: i64,
        real_message_id: i32,
    ) -> Result<Option<MessageIdMapping>, StorageError> {
        let row = sqlx::query(SQL_GET_MAPPING_BY_REAL)
            .bind(chat_id)
            .bind(real_message_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(message_id_mapping_from_row).transpose()?)
    }

    /// Delete a virtual-message mapping by virtual ID.
    pub async fn delete_mapping_by_virtual(&self, vmsg_id: &str) -> Result<(), StorageError> {
        sqlx::query(SQL_DELETE_MAPPING_BY_VIRTUAL)
            .bind(vmsg_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Enqueue a pending operation for a virtual message.
    pub async fn enqueue_message_op(
        &self,
        vmsg_id: &str,
        chat_id: i64,
        op: &str,
        payload_json: Option<&str>,
    ) -> Result<i64, StorageError> {
        let row = sqlx::query(SQL_ENQUEUE_MESSAGE_OP)
            .bind(vmsg_id)
            .bind(chat_id)
            .bind(op)
            .bind(payload_json)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get::<i64, _>("id")?)
    }

    pub async fn list_pending_ops(&self, limit: i32) -> Result<Vec<PendingOp>, StorageError> {
        let rows = sqlx::query(SQL_LIST_PENDING_OPS)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(pending_op_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    /// Mark a pending operation as done.
    pub async fn mark_op_done(&self, id: i64) -> Result<(), StorageError> {
        sqlx::query(SQL_MARK_OP_DONE)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Record a failed pending operation attempt.
    pub async fn mark_op_failed(&self, id: i64, message: &str) -> Result<(), StorageError> {
        sqlx::query(SQL_MARK_OP_FAILED)
            .bind(id)
            .bind(message)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PostgresChatSettingsStore {
    pool: PgPool,
}

impl PostgresChatSettingsStore {
    /// Build a chat-settings store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn get_chat_type(&self, chat_id: i64) -> Result<Option<String>, StorageError> {
        let chat_type = sqlx::query_scalar::<_, String>(SQL_GET_CHAT_TYPE)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(chat_type)
    }

    pub async fn get_chat_settings(
        &self,
        chat_id: i64,
    ) -> Result<Option<ChatSettings>, StorageError> {
        let row = sqlx::query(SQL_GET_CHAT_SETTINGS)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(chat_settings_from_row).transpose()?)
    }

    pub async fn get_chat_state(&self, chat_id: i64) -> Result<Option<ChatState>, StorageError> {
        let row = sqlx::query(SQL_GET_CHAT_STATE)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(chat_state_from_row).transpose()?)
    }

    pub async fn list_user_chats(&self, user_id: i64) -> Result<Vec<ChatState>, StorageError> {
        let rows = sqlx::query(SQL_LIST_USER_CHATS)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(chat_state_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub async fn get_dialog_memory_chat_meta(
        &self,
        chat_id: i64,
    ) -> Result<Option<DialogMemoryChatMeta>, StorageError> {
        let row = sqlx::query(SQL_GET_DIALOG_MEMORY_CHAT_META)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(dialog_memory_chat_meta_from_row).transpose()?)
    }

    pub async fn get_user_settings(
        &self,
        user_id: i64,
    ) -> Result<Option<UserSettings>, StorageError> {
        let row = sqlx::query(SQL_GET_USER_SETTINGS)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(user_settings_from_row).transpose()?)
    }

    pub async fn upsert_user_settings(
        &self,
        user_id: i64,
        disable_random_reactivity: bool,
        hide_original_draw_prompt: bool,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_USER_SETTINGS)
            .bind(user_id)
            .bind(disable_random_reactivity)
            .bind(hide_original_draw_prompt)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert_chat_settings(
        &self,
        update: &ChatSettingsUpdate,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_CHAT_SETTINGS)
            .bind(update.chat_id)
            .bind(update.mood_alignment.as_deref())
            .bind(update.custom_persona.as_deref())
            .bind(update.reactivity_percentage)
            .bind(update.proactivity_percentage)
            .bind(update.enable_obscenifier)
            .bind(update.enable_profanity)
            .bind(update.enable_greet_joiners)
            .bind(update.enable_global_text_reply)
            .bind(update.enable_global_draw_reply)
            .bind(update.enable_daily_game)
            .bind(update.daily_game_theme.as_str())
            .bind(update.greeting_html.as_deref())
            .bind(update.chat_type.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PostgresChatMemberStore {
    pool: PgPool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChatMemberRecord {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram chat-member status string.
    pub status: String,
    /// Whether the member is anonymous.
    pub is_anonymous: Option<bool>,
    /// Custom admin/creator title.
    pub custom_title: Option<String>,
    /// Whether the bot can edit this member.
    pub can_be_edited: Option<bool>,
    /// Whether the member can manage chat metadata.
    pub can_manage_chat: Option<bool>,
    /// Whether the member can delete messages.
    pub can_delete_messages: Option<bool>,
    /// Whether the member can manage video chats.
    pub can_manage_video_chats: Option<bool>,
    /// Whether the member can restrict users.
    pub can_restrict_members: Option<bool>,
    /// Whether the member can promote users.
    pub can_promote_members: Option<bool>,
    /// Whether the member can change chat info.
    pub can_change_info: Option<bool>,
    /// Whether the member can invite users.
    pub can_invite_users: Option<bool>,
    /// Whether the member can post messages.
    pub can_post_messages: Option<bool>,
    /// Whether the member can edit messages.
    pub can_edit_messages: Option<bool>,
    /// Whether the member can pin messages.
    pub can_pin_messages: Option<bool>,
    /// Whether the member can manage forum topics.
    pub can_manage_topics: Option<bool>,
    /// Whether the member can send text messages.
    pub can_send_messages: Option<bool>,
    pub can_send_media_messages: Option<bool>,
    /// Whether the member can send polls.
    pub can_send_polls: Option<bool>,
    /// Whether the member can send other messages.
    pub can_send_other_messages: Option<bool>,
    /// Whether the member can add web page previews.
    pub can_add_web_page_previews: Option<bool>,
    /// Optional restricted/kicked expiration.
    pub until_date: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChatMemberCandidate {
    /// Telegram user ID.
    pub id: i64,
    /// Telegram first name.
    pub first_name: String,
    /// Telegram last name.
    pub last_name: Option<String>,
    /// Telegram username.
    pub username: Option<String>,
    /// Current chat-member status.
    pub status: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChatMemberUpsert {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram chat-member status string.
    pub status: String,
    /// Whether the member is anonymous.
    pub is_anonymous: Option<bool>,
    /// Custom admin/creator title.
    pub custom_title: Option<String>,
    /// Whether the bot can edit this member.
    pub can_be_edited: Option<bool>,
    /// Whether the member can manage chat metadata.
    pub can_manage_chat: Option<bool>,
    /// Whether the member can delete messages.
    pub can_delete_messages: Option<bool>,
    /// Whether the member can manage video chats.
    pub can_manage_video_chats: Option<bool>,
    /// Whether the member can restrict users.
    pub can_restrict_members: Option<bool>,
    /// Whether the member can promote users.
    pub can_promote_members: Option<bool>,
    /// Whether the member can change chat info.
    pub can_change_info: Option<bool>,
    /// Whether the member can invite users.
    pub can_invite_users: Option<bool>,
    /// Whether the member can post messages.
    pub can_post_messages: Option<bool>,
    /// Whether the member can edit messages.
    pub can_edit_messages: Option<bool>,
    /// Whether the member can pin messages.
    pub can_pin_messages: Option<bool>,
    /// Whether the member can manage forum topics.
    pub can_manage_topics: Option<bool>,
    /// Whether the member can send text messages.
    pub can_send_messages: Option<bool>,
    pub can_send_media_messages: Option<bool>,
    /// Whether the member can send polls.
    pub can_send_polls: Option<bool>,
    /// Whether the member can send other messages.
    pub can_send_other_messages: Option<bool>,
    /// Whether the member can add web page previews.
    pub can_add_web_page_previews: Option<bool>,
    /// Optional restricted/kicked expiration.
    pub until_date: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoredAdminChatMember {
    /// Telegram user ID.
    pub user_id: i64,
    /// Stored admin/creator status.
    pub status: String,
    /// Whether the admin is anonymous.
    pub is_anonymous: Option<bool>,
    /// Custom admin title.
    pub custom_title: Option<String>,
    /// Whether the admin can delete messages.
    pub can_delete_messages: Option<bool>,
    /// Whether the admin can manage video chats.
    pub can_manage_video_chats: Option<bool>,
    /// Whether the admin can restrict members.
    pub can_restrict_members: Option<bool>,
    /// Whether the admin can promote members.
    pub can_promote_members: Option<bool>,
    /// Whether the admin can change chat info.
    pub can_change_info: Option<bool>,
    /// Whether the admin can invite users.
    pub can_invite_users: Option<bool>,
    /// Whether the admin can post messages.
    pub can_post_messages: Option<bool>,
    /// Whether the admin can edit messages.
    pub can_edit_messages: Option<bool>,
    /// Whether the admin can pin messages.
    pub can_pin_messages: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChatGameResult {
    /// Result primary key.
    pub id: i64,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Winner Telegram user ID.
    pub user_id: i64,
    /// Daily-game theme key.
    pub theme: String,
    /// Timestamp when the winner was recorded.
    pub won_at: OffsetDateTime,
    pub won_on_date: time::Date,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChatGameTopRow {
    /// Winner user row.
    pub user: UserState,
    /// Number of wins in the queried period.
    pub wins_count: i32,
    /// Most recent win timestamp.
    pub last_win_at: Option<OffsetDateTime>,
}

impl PostgresChatMemberStore {
    /// Build a chat-member store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn get_chat_member(
        &self,
        chat_id: i64,
        user_id: i64,
    ) -> Result<Option<ChatMemberRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_CHAT_MEMBER)
            .bind(chat_id)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(chat_member_from_row).transpose()?)
    }

    pub async fn list_chat_members(
        &self,
        chat_id: i64,
    ) -> Result<Vec<ChatMemberRecord>, StorageError> {
        let rows = sqlx::query(SQL_LIST_CHAT_MEMBERS)
            .bind(chat_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(chat_member_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub async fn upsert_chat_member(&self, member: &ChatMemberUpsert) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_CHAT_MEMBER)
            .bind(member.chat_id)
            .bind(member.user_id)
            .bind(&member.status)
            .bind(member.is_anonymous)
            .bind(member.custom_title.as_deref())
            .bind(member.can_be_edited)
            .bind(member.can_manage_chat)
            .bind(member.can_delete_messages)
            .bind(member.can_manage_video_chats)
            .bind(member.can_restrict_members)
            .bind(member.can_promote_members)
            .bind(member.can_change_info)
            .bind(member.can_invite_users)
            .bind(member.can_post_messages)
            .bind(member.can_edit_messages)
            .bind(member.can_pin_messages)
            .bind(member.can_manage_topics)
            .bind(member.can_send_messages)
            .bind(member.can_send_media_messages)
            .bind(member.can_send_polls)
            .bind(member.can_send_other_messages)
            .bind(member.can_add_web_page_previews)
            .bind(member.until_date)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_chat_member(&self, chat_id: i64, user_id: i64) -> Result<(), StorageError> {
        sqlx::query(SQL_DELETE_CHAT_MEMBER)
            .bind(chat_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_member_last_message(
        &self,
        chat_id: i64,
        user_id: i64,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_UPDATE_MEMBER_LAST_MESSAGE)
            .bind(chat_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_member_last_messages(
        &self,
        pairs: &[(i64, i64)],
    ) -> Result<(), StorageError> {
        if pairs.is_empty() {
            return Ok(());
        }
        let (chat_ids, user_ids) = chat_user_pair_arrays(pairs);
        sqlx::query(SQL_UPDATE_MEMBER_LAST_MESSAGES)
            .bind(&chat_ids)
            .bind(&user_ids)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert_chat_active_user(
        &self,
        chat_id: i64,
        user_id: i64,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_CHAT_ACTIVE_USER)
            .bind(chat_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert_chat_active_users(&self, pairs: &[(i64, i64)]) -> Result<(), StorageError> {
        if pairs.is_empty() {
            return Ok(());
        }
        let (chat_ids, user_ids) = chat_user_pair_arrays(pairs);
        sqlx::query(SQL_UPSERT_CHAT_ACTIVE_USERS)
            .bind(&chat_ids)
            .bind(&user_ids)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_active_participants(
        &self,
        chat_id: i64,
        limit_count: i32,
    ) -> Result<Vec<i64>, StorageError> {
        let rows = sqlx::query_scalar::<_, i64>(SQL_LIST_ACTIVE_PARTICIPANTS)
            .bind(chat_id)
            .bind(limit_count)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    pub async fn list_active_participants_from_table(
        &self,
        chat_id: i64,
        limit_count: i32,
    ) -> Result<Vec<i64>, StorageError> {
        let rows = sqlx::query_scalar::<_, i64>(SQL_LIST_ACTIVE_PARTICIPANTS_FROM_TABLE)
            .bind(chat_id)
            .bind(limit_count)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    /// Upsert user state for admin-sync user persistence.
    pub async fn upsert_user_state(&self, user: &UserState) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_USER)
            .bind(user.id)
            .bind(&user.first_name)
            .bind(user.last_name.as_deref())
            .bind(user.username.as_deref())
            .bind(user.language_code.as_deref())
            .bind(user.is_premium)
            .bind(Option::<&str>::None)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_user_state(&self, user_id: i64) -> Result<Option<UserState>, StorageError> {
        let row = sqlx::query(SQL_GET_USER)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(user_state_from_row).transpose()?)
    }

    pub async fn list_user_states_by_ids(
        &self,
        user_ids: &[i64],
    ) -> Result<Vec<UserState>, StorageError> {
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(SQL_LIST_USERS_BY_IDS)
            .bind(user_ids)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(user_state_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub async fn list_chat_members_by_user_ids(
        &self,
        chat_id: i64,
        user_ids: &[i64],
    ) -> Result<Vec<ChatMemberRecord>, StorageError> {
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(SQL_LIST_CHAT_MEMBERS_BY_USER_IDS)
            .bind(chat_id)
            .bind(user_ids)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(chat_member_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub async fn list_user_chat_memberships(
        &self,
        user_id: i64,
    ) -> Result<Vec<ChatMemberRecord>, StorageError> {
        let rows = sqlx::query(SQL_LIST_USER_CHAT_MEMBERSHIPS)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(chat_member_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub async fn list_chat_deputy_ids(&self, chat_id: i64) -> Result<Vec<i64>, StorageError> {
        sqlx::query_scalar::<_, i64>(SQL_LIST_CHAT_DEPUTY_IDS)
            .bind(chat_id)
            .fetch_all(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    pub async fn list_user_deputy_chat_ids(&self, user_id: i64) -> Result<Vec<i64>, StorageError> {
        sqlx::query_scalar::<_, i64>(SQL_LIST_USER_DEPUTY_CHAT_IDS)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    pub async fn replace_chat_deputies(
        &self,
        chat_id: i64,
        user_ids: &[i64],
    ) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(SQL_DELETE_ALL_CHAT_DEPUTIES)
            .bind(chat_id)
            .execute(&mut *tx)
            .await?;
        if !user_ids.is_empty() {
            sqlx::query(SQL_UPSERT_CHAT_DEPUTIES)
                .bind(chat_id)
                .bind(user_ids)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn search_chat_member_candidates(
        &self,
        chat_id: i64,
        query: &str,
        limit_count: i32,
    ) -> Result<Vec<ChatMemberCandidate>, StorageError> {
        let rows = sqlx::query(SQL_SEARCH_CHAT_MEMBER_CANDIDATES)
            .bind(chat_id)
            .bind(query)
            .bind(limit_count)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(chat_member_candidate_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub async fn get_chat_discovered(
        &self,
        chat_id: i64,
    ) -> Result<Option<OffsetDateTime>, StorageError> {
        let discovered = sqlx::query_scalar::<_, OffsetDateTime>(SQL_GET_CHAT_DISCOVERED)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(discovered)
    }

    pub async fn record_chat_daily_winner(
        &self,
        chat_id: i64,
        user_id: i64,
        theme: &str,
    ) -> Result<ChatGameResult, StorageError> {
        let row = sqlx::query(SQL_RECORD_CHAT_DAILY_WINNER)
            .bind(chat_id)
            .bind(user_id)
            .bind(theme)
            .fetch_one(&self.pool)
            .await?;
        Ok(chat_game_result_from_row(row)?)
    }

    pub async fn get_today_chat_winner(
        &self,
        chat_id: i64,
    ) -> Result<Option<ChatGameResult>, StorageError> {
        let row = sqlx::query(SQL_GET_TODAY_CHAT_WINNER)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(chat_game_result_from_row).transpose()?)
    }

    pub async fn increment_chat_game_win(
        &self,
        chat_id: i64,
        user_id: i64,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_INCREMENT_CHAT_GAME_WIN)
            .bind(chat_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_yearly_top(
        &self,
        chat_id: i64,
        limit_count: i32,
    ) -> Result<Vec<ChatGameTopRow>, StorageError> {
        let rows = sqlx::query(SQL_GET_YEARLY_TOP)
            .bind(chat_id)
            .bind(limit_count)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(chat_game_top_row_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }
}

fn chat_user_pair_arrays(pairs: &[(i64, i64)]) -> (Vec<i64>, Vec<i64>) {
    let mut chat_ids = Vec::with_capacity(pairs.len());
    let mut user_ids = Vec::with_capacity(pairs.len());
    for &(chat_id, user_id) in pairs {
        chat_ids.push(chat_id);
        user_ids.push(user_id);
    }
    (chat_ids, user_ids)
}

#[derive(Clone, Debug)]
pub struct PostgresTelegramFileStore {
    pool: PgPool,
}

impl PostgresTelegramFileStore {
    /// Build a Telegram file metadata store over an existing pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn upsert_metadata(
        &self,
        params: &TelegramFileMetadataUpsert,
    ) -> Result<TelegramFileRecord, StorageError> {
        let row = sqlx::query(SQL_UPSERT_TELEGRAM_FILE_METADATA)
            .bind(&params.file_unique_id)
            .bind(&params.latest_file_id)
            .bind(&params.media_kind)
            .bind(params.mime_type.as_deref())
            .bind(params.width)
            .bind(params.height)
            .bind(params.file_size)
            .bind(params.first_seen_chat_id)
            .bind(params.first_seen_message_id)
            .bind(params.last_seen_chat_id)
            .bind(params.last_seen_message_id)
            .fetch_one(&self.pool)
            .await?;
        telegram_file_from_row(row)
    }

    pub async fn get_file(
        &self,
        file_unique_id: &str,
    ) -> Result<Option<TelegramFileRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_TELEGRAM_FILE)
            .bind(file_unique_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(telegram_file_from_row).transpose()
    }

    pub async fn list_files_by_unique_ids(
        &self,
        file_unique_ids: &[String],
    ) -> Result<Vec<TelegramFileRecord>, StorageError> {
        if file_unique_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(SQL_LIST_TELEGRAM_FILES_BY_UNIQUE_IDS)
            .bind(file_unique_ids)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(telegram_file_from_row).collect()
    }

    pub async fn get_file_by_latest_file_id(
        &self,
        latest_file_id: &str,
    ) -> Result<Option<TelegramFileRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_TELEGRAM_FILE_BY_LATEST_FILE_ID)
            .bind(latest_file_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(telegram_file_from_row).transpose()
    }

    pub async fn update_vision(
        &self,
        params: &TelegramFileVisionUpdate,
    ) -> Result<TelegramFileRecord, StorageError> {
        let row = sqlx::query(SQL_UPDATE_TELEGRAM_FILE_VISION)
            .bind(&params.file_unique_id)
            .bind(&params.vision_status)
            .bind(params.vision_caption.as_deref())
            .bind(params.vision_model.as_deref())
            .bind(params.vision_latency_ms)
            .bind(params.recognition_requested_at)
            .bind(params.recognition_completed_at)
            .fetch_one(&self.pool)
            .await?;
        telegram_file_from_row(row)
    }
}

#[must_use]
pub fn stored_member_can_open_group_settings(member: Option<&ChatMemberRecord>) -> bool {
    member.is_some_and(|member| {
        member.status == CHAT_MEMBER_STATUS_CREATOR
            || member.status == CHAT_MEMBER_STATUS_ADMINISTRATOR
                && member.can_promote_members == Some(true)
    })
}

#[must_use]
pub fn is_active_chat_member_status(status: &str) -> bool {
    matches!(
        status,
        CHAT_MEMBER_STATUS_CREATOR | CHAT_MEMBER_STATUS_ADMINISTRATOR | CHAT_MEMBER_STATUS_MEMBER
    )
}

#[must_use]
pub fn stored_admin_chat_member(member: &ChatMemberRecord) -> Option<StoredAdminChatMember> {
    if member.status != CHAT_MEMBER_STATUS_ADMINISTRATOR
        && member.status != CHAT_MEMBER_STATUS_CREATOR
    {
        return None;
    }

    Some(StoredAdminChatMember {
        user_id: member.user_id,
        status: member.status.clone(),
        is_anonymous: member.is_anonymous,
        custom_title: member.custom_title.clone(),
        can_delete_messages: member.can_delete_messages,
        can_manage_video_chats: member.can_manage_video_chats,
        can_restrict_members: member.can_restrict_members,
        can_promote_members: member.can_promote_members,
        can_change_info: member.can_change_info,
        can_invite_users: member.can_invite_users,
        can_post_messages: member.can_post_messages,
        can_edit_messages: member.can_edit_messages,
        can_pin_messages: member.can_pin_messages,
    })
}

#[derive(Clone, Debug)]
pub struct PostgresHistoryStore {
    pool: PgPool,
    redis: Option<RedisConnectionPool>,
}

#[derive(Clone, Copy, Debug)]
pub struct HistoryEntryUpsert<'payload> {
    /// UTC bucket day partition.
    pub bucket_day: time::Date,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram thread/topic ID.
    pub thread_id: i32,
    /// Telegram message ID.
    pub message_id: i32,
    /// Stable history entry ID, such as `msg:123`.
    pub entry_id: &'payload str,
    /// History message kind.
    pub kind: &'payload str,
    /// Dialog role.
    pub role: &'payload str,
    /// Message timestamp.
    pub occurred_at: OffsetDateTime,
    /// Sender ID.
    pub sender_id: i64,
    pub payload: &'payload [u8],
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistoryToolEntryUpsert {
    /// UTC bucket day partition.
    pub bucket_day: time::Date,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram thread/topic ID.
    pub thread_id: i32,
    /// Telegram message ID.
    pub message_id: i32,
    /// Stable tool history entry ID.
    pub entry_id: String,
    /// History message kind.
    pub kind: String,
    /// Dialog role.
    pub role: String,
    /// Message timestamp.
    pub occurred_at: OffsetDateTime,
    pub sender_id: i64,
    pub payload: Vec<u8>,
}

impl PostgresHistoryStore {
    /// Build a history store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool, redis: None }
    }

    pub fn with_redis_client(mut self, redis: RedisClient) -> Self {
        self.redis = Some(RedisConnectionPool::new(redis));
        self
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn upsert_history_entry(
        &self,
        entry: HistoryEntryUpsert<'_>,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_ENSURE_CHAT_HISTORY_PARTITION)
            .bind(entry.bucket_day)
            .execute(&self.pool)
            .await?;

        let payload = serde_json::from_slice::<serde_json::Value>(entry.payload)
            .map_err(|source| StorageError::HistoryPayloadCodec { source })?;

        sqlx::query(SQL_UPSERT_HISTORY_ENTRY)
            .bind(entry.bucket_day)
            .bind(entry.chat_id)
            .bind(entry.thread_id)
            .bind(entry.message_id)
            .bind(entry.entry_id)
            .bind(entry.kind)
            .bind(entry.role)
            .bind(entry.occurred_at)
            .bind(entry.sender_id)
            .bind(sqlx::types::Json(payload))
            .execute(&self.pool)
            .await?;

        self.invalidate_history_cache(entry.chat_id).await?;
        Ok(())
    }

    /// Update the stored text for an existing inbound history row.
    /// required IDs or no text history row exists.
    pub async fn update_text_entry(
        &self,
        chat_id: i64,
        message_id: i32,
        new_text: &str,
    ) -> Result<bool, StorageError> {
        if chat_id == 0 || message_id == 0 {
            return Ok(false);
        }

        let row = sqlx::query(SQL_SELECT_TEXT_HISTORY_ENTRY)
            .bind(chat_id)
            .bind(message_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(false);
        };

        let bucket_day: time::Date = row.try_get("bucket_day")?;
        let entry_id: String = row.try_get("entry_id")?;
        let payload: String = row.try_get("payload")?;
        let updated_payload = history_text_payload_with_text(&payload, new_text)?;

        sqlx::query(SQL_UPDATE_HISTORY_ENTRY_PAYLOAD)
            .bind(bucket_day)
            .bind(chat_id)
            .bind(&entry_id)
            .bind(&updated_payload)
            .execute(&self.pool)
            .await?;
        self.invalidate_history_cache(chat_id).await?;
        Ok(true)
    }

    /// Update the stored message text, original text, and metadata for an edited inbound message.
    ///
    /// required IDs or no text history row exists.
    pub async fn update_message_entry(
        &self,
        chat_id: i64,
        message_id: i32,
        new_text: &str,
        original_text: &str,
        meta: &ChatMessageMeta,
    ) -> Result<bool, StorageError> {
        if chat_id == 0 || message_id == 0 {
            return Ok(false);
        }

        let row = sqlx::query(SQL_SELECT_TEXT_HISTORY_ENTRY)
            .bind(chat_id)
            .bind(message_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(false);
        };

        let bucket_day: time::Date = row.try_get("bucket_day")?;
        let entry_id: String = row.try_get("entry_id")?;
        let payload: String = row.try_get("payload")?;
        let updated_payload = history_text_payload_with_message_update(
            &payload,
            new_text,
            original_text,
            meta.clone(),
        )?;

        sqlx::query(SQL_UPDATE_HISTORY_ENTRY_PAYLOAD)
            .bind(bucket_day)
            .bind(chat_id)
            .bind(&entry_id)
            .bind(&updated_payload)
            .execute(&self.pool)
            .await?;
        self.invalidate_history_cache(chat_id).await?;
        Ok(true)
    }

    /// Upsert vision descriptions into stored message metadata.
    /// updates are empty, no text row exists, or the stored metadata is already equivalent.
    pub async fn upsert_vision_descriptions(
        &self,
        chat_id: i64,
        message_id: i32,
        updates: &[VisionDescriptionUpdate],
    ) -> Result<bool, StorageError> {
        if chat_id == 0 || message_id == 0 {
            return Ok(false);
        }
        let updates = normalize_vision_description_updates(updates);
        if updates.is_empty() {
            return Ok(false);
        }

        let row = sqlx::query(SQL_SELECT_TEXT_HISTORY_ENTRY)
            .bind(chat_id)
            .bind(message_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(false);
        };

        let bucket_day: time::Date = row.try_get("bucket_day")?;
        let entry_id: String = row.try_get("entry_id")?;
        let payload: String = row.try_get("payload")?;
        let Some(updated_payload) =
            history_text_payload_with_vision_descriptions(&payload, &updates)?
        else {
            return Ok(false);
        };

        sqlx::query(SQL_UPDATE_HISTORY_ENTRY_PAYLOAD)
            .bind(bucket_day)
            .bind(chat_id)
            .bind(&entry_id)
            .bind(&updated_payload)
            .execute(&self.pool)
            .await?;
        self.invalidate_history_cache(chat_id).await?;
        Ok(true)
    }

    /// Delete stored history entries for one Telegram message ID.
    pub async fn delete_message_entries(
        &self,
        chat_id: i64,
        message_id: i32,
    ) -> Result<u64, StorageError> {
        if chat_id == 0 || message_id == 0 {
            return Ok(0);
        }

        let result = sqlx::query(SQL_DELETE_HISTORY_MESSAGE_ENTRIES)
            .bind(chat_id)
            .bind(message_id)
            .execute(&self.pool)
            .await?;
        self.invalidate_history_cache(chat_id).await?;
        Ok(result.rows_affected())
    }

    /// Upsert tool-call history into stored message metadata.
    /// terminator filtering, or the base text history row is absent.
    pub async fn upsert_tool_call_history(
        &self,
        chat_id: i64,
        message_id: i32,
        tool_calls: &[ToolCall],
    ) -> Result<bool, StorageError> {
        if chat_id == 0 || message_id == 0 || tool_calls.is_empty() {
            return Ok(false);
        }
        let tool_calls = openplotva_core::filter_non_terminator_tool_calls(tool_calls);
        if tool_calls.is_empty() {
            return Ok(false);
        }

        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(SQL_SELECT_TEXT_HISTORY_ENTRY)
            .bind(chat_id)
            .bind(message_id)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let payload: String = row.try_get("payload")?;
        let entries = history_tool_call_entries_from_base_payload(
            chat_id,
            message_id,
            &payload,
            &tool_calls,
        )?;

        sqlx::query(SQL_DELETE_HISTORY_TOOL_ENTRIES)
            .bind(chat_id)
            .bind(message_id)
            .execute(&mut *tx)
            .await?;

        for entry in &entries {
            let payload = serde_json::from_slice::<serde_json::Value>(&entry.payload)
                .map_err(|source| StorageError::HistoryPayloadCodec { source })?;
            sqlx::query(SQL_ENSURE_CHAT_HISTORY_PARTITION)
                .bind(entry.bucket_day)
                .execute(&mut *tx)
                .await?;
            sqlx::query(SQL_UPSERT_HISTORY_ENTRY)
                .bind(entry.bucket_day)
                .bind(entry.chat_id)
                .bind(entry.thread_id)
                .bind(entry.message_id)
                .bind(&entry.entry_id)
                .bind(&entry.kind)
                .bind(&entry.role)
                .bind(entry.occurred_at)
                .bind(entry.sender_id)
                .bind(sqlx::types::Json(payload))
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        self.invalidate_history_cache(chat_id).await?;
        Ok(true)
    }

    /// Mark one chat/thread history reset point.
    pub async fn reset_history_at(
        &self,
        chat_id: i64,
        thread_id: i32,
        reset_at: OffsetDateTime,
    ) -> Result<bool, StorageError> {
        if chat_id == 0 {
            return Ok(false);
        }

        sqlx::query(SQL_UPSERT_CHAT_HISTORY_RESET)
            .bind(chat_id)
            .bind(thread_id)
            .bind(reset_at)
            .execute(&self.pool)
            .await?;
        self.invalidate_history_cache(chat_id).await?;
        Ok(true)
    }

    pub async fn history_reset_at(
        &self,
        chat_id: i64,
        thread_id: i32,
    ) -> Result<Option<OffsetDateTime>, StorageError> {
        if chat_id == 0 {
            return Ok(None);
        }
        sqlx::query_scalar(SQL_GET_CHAT_HISTORY_RESET_AT)
            .bind(chat_id)
            .bind(thread_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    pub async fn recent_chat_history_payloads(
        &self,
        chat_id: i64,
        cutoff: OffsetDateTime,
        thread_id: i32,
        thread_reset_at: OffsetDateTime,
        limit_count: i32,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        if chat_id == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(SQL_SELECT_RECENT_CHAT_HISTORY_ENTRY_PAYLOADS)
            .bind(chat_id)
            .bind(cutoff)
            .bind(thread_id)
            .bind(thread_reset_at)
            .bind(limit_count.max(1))
            .fetch_all(&self.pool)
            .await?;
        summary_payload_rows_to_bytes(rows)
    }

    pub async fn recent_thread_history_payloads(
        &self,
        chat_id: i64,
        thread_id: i32,
        cutoff: OffsetDateTime,
        limit_count: i32,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        if chat_id == 0 || thread_id == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(SQL_SELECT_RECENT_THREAD_HISTORY_ENTRY_PAYLOADS)
            .bind(chat_id)
            .bind(thread_id)
            .bind(cutoff)
            .bind(limit_count.max(1))
            .fetch_all(&self.pool)
            .await?;
        summary_payload_rows_to_bytes(rows)
    }

    pub async fn chat_summary_entry_payloads(
        &self,
        chat_id: i64,
        range_start_at: OffsetDateTime,
        range_end_at: OffsetDateTime,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        summary_payload_rows_to_bytes(
            sqlx::query(SQL_SELECT_CHAT_SUMMARY_ENTRY_PAYLOADS)
                .bind(chat_id)
                .bind(range_start_at)
                .bind(range_end_at)
                .fetch_all(&self.pool)
                .await?,
        )
    }

    pub async fn thread_summary_entry_payloads(
        &self,
        chat_id: i64,
        thread_id: i32,
        range_start_at: OffsetDateTime,
        range_end_at: OffsetDateTime,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        summary_payload_rows_to_bytes(
            sqlx::query(SQL_SELECT_THREAD_SUMMARY_ENTRY_PAYLOADS)
                .bind(chat_id)
                .bind(thread_id)
                .bind(range_start_at)
                .bind(range_end_at)
                .fetch_all(&self.pool)
                .await?,
        )
    }

    pub async fn summary_entry_payloads(
        &self,
        chat_id: i64,
        thread_id: i32,
        scope: SummaryScope,
        range_start_at: OffsetDateTime,
        range_end_at: OffsetDateTime,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        if scope == SummaryScope::Thread && thread_id != 0 {
            self.thread_summary_entry_payloads(chat_id, thread_id, range_start_at, range_end_at)
                .await
        } else {
            self.chat_summary_entry_payloads(chat_id, range_start_at, range_end_at)
                .await
        }
    }

    pub async fn save_summary(
        &self,
        input: &SummaryInput,
        doc: &SummaryDocument,
    ) -> Result<StoredSummary, StorageError> {
        let prepared = prepare_stored_summary(input, doc)
            .map_err(|source| StorageError::HistorySummaryPrepare { source })?;
        let summary_json = String::from_utf8_lossy(&prepared.summary_json);
        let mut tx = self.pool.begin().await?;
        let mut stored = prepared.stored;

        let row = sqlx::query(SQL_INSERT_HISTORY_SUMMARY)
            .bind(stored.chat_id)
            .bind(stored.thread_id)
            .bind(stored.scope.as_str())
            .bind(stored.requested_by_user_id)
            .bind(stored.range_start_at)
            .bind(stored.range_end_at)
            .bind(stored.first_message_id)
            .bind(stored.last_message_id)
            .bind(&stored.first_entry_id)
            .bind(&stored.last_entry_id)
            .bind(stored.raw_message_count)
            .bind(stored.covered_message_count)
            .bind(&stored.source_summary_ids)
            .bind(summary_json.as_ref())
            .bind(&stored.summary_html)
            .bind(&stored.model)
            .bind(&stored.prompt_version)
            .bind(&stored.input_hash)
            .bind(&stored.prompt_hash)
            .bind(stored.input_token_estimate)
            .bind(stored.output_token_estimate)
            .bind(stored.cascade_depth)
            .bind(stored.quality_score)
            .bind(&stored.quality_notes)
            .fetch_one(&mut *tx)
            .await?;
        stored.id = row.try_get("id")?;
        stored.created_at = row.try_get("created_at")?;

        for source in &prepared.sources {
            sqlx::query(SQL_INSERT_HISTORY_SUMMARY_SOURCE)
                .bind(stored.id)
                .bind(source.source_order)
                .bind(source.source_type.as_str())
                .bind(summary_source_id_for_storage(source))
                .bind(source.range_start_at)
                .bind(source.range_end_at)
                .bind(source.first_message_id)
                .bind(source.last_message_id)
                .bind(&source.first_entry_id)
                .bind(&source.last_entry_id)
                .bind(source.raw_message_count)
                .bind(source.covered_message_count)
                .execute(&mut *tx)
                .await?;
        }

        for event in summary_events_for_storage(&stored.summary_json) {
            let occurred_at = parse_summary_event_time(&event.event.occurred_at);
            sqlx::query(SQL_INSERT_CHAT_HISTORY_EVENT)
                .bind(stored.id)
                .bind(stored.chat_id)
                .bind(stored.thread_id)
                .bind(stored.scope.as_str())
                .bind(event.source_order)
                .bind(&event.event.title)
                .bind(&event.event.description)
                .bind(&event.event.actors)
                .bind(occurred_at)
                .bind(stored.range_start_at)
                .bind(stored.range_end_at)
                .bind(&stored.source_summary_ids)
                .bind(event.event.confidence)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(stored)
    }

    pub async fn reusable_history_summaries(
        &self,
        chat_id: i64,
        thread_id: i32,
        scope: SummaryScope,
        range_start_at: OffsetDateTime,
        range_end_at: OffsetDateTime,
        reset_at: OffsetDateTime,
    ) -> Result<Vec<StoredSummary>, StorageError> {
        let rows = sqlx::query(SQL_SELECT_REUSABLE_HISTORY_SUMMARIES)
            .bind(chat_id)
            .bind(thread_id)
            .bind(scope.as_str())
            .bind(range_start_at)
            .bind(range_end_at)
            .bind(reset_at)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(stored_summary_from_row).collect()
    }

    async fn invalidate_history_cache(&self, chat_id: i64) -> Result<(), StorageError> {
        let Some(redis) = &self.redis else {
            return Ok(());
        };
        let mut connection = redis
            .connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let _: i64 = redis::cmd("DEL")
            .arg(history_cache_key(chat_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }
}

/// SQLx-backed long-term memory store.
#[derive(Clone, Debug)]
pub struct PostgresMemoryStore {
    pool: PgPool,
}

impl PostgresMemoryStore {
    /// Build a memory store over an existing pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_dialog_memory_chat_meta(
        &self,
        chat_id: i64,
    ) -> Result<Option<DialogMemoryChatMeta>, StorageError> {
        let row = sqlx::query(SQL_GET_DIALOG_MEMORY_CHAT_META)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(dialog_memory_chat_meta_from_row).transpose()?)
    }

    pub async fn upsert_cards_lexical(
        &self,
        cards: &[openplotva_memory::CardInput],
    ) -> Result<(openplotva_memory::RunStats, Vec<i64>), StorageError> {
        self.upsert_cards_with_embeddings(cards, &[]).await
    }

    /// Upsert memory cards, card sources, and optional pgvector embeddings.
    pub async fn upsert_cards_with_embeddings(
        &self,
        cards: &[openplotva_memory::CardInput],
        embeddings: &[Option<PgEmbeddingVector>],
    ) -> Result<(openplotva_memory::RunStats, Vec<i64>), StorageError> {
        let mut stats = openplotva_memory::RunStats::default();
        if cards.is_empty() {
            return Ok((stats, Vec::new()));
        }
        let mut tx = self.pool.begin().await?;
        let fallback_observed_at = OffsetDateTime::now_utc();
        let mut ids = Vec::with_capacity(cards.len());

        for (index, raw) in cards.iter().enumerate() {
            let Some(params) = memory_card_upsert_params_at(raw.clone(), fallback_observed_at)
            else {
                continue;
            };
            let embedding = embeddings
                .get(index)
                .and_then(Option::as_ref)
                .and_then(|vector| pgvector_literal(Some(vector)));
            let row = sqlx::query(SQL_UPSERT_MEMORY_CARD_WITH_EMBEDDING)
                .bind(&params.visibility)
                .bind(&params.card_type)
                .bind(&params.subject)
                .bind(&params.predicate)
                .bind(&params.object)
                .bind(&params.fact_text)
                .bind(&params.dedup_hash)
                .bind(params.confidence)
                .bind(params.salience)
                .bind(params.origin_chat_id)
                .bind(params.origin_thread_id)
                .bind(params.origin_user_id)
                .bind(params.chat_id)
                .bind(params.thread_id)
                .bind(params.user_id)
                .bind(params.last_observed_at)
                .bind(embedding)
                .fetch_one(&mut *tx)
                .await?;
            let id: i64 = row.try_get("id")?;
            if row.try_get::<bool, _>("inserted")? {
                stats.cards_inserted += 1;
            } else {
                stats.cards_updated += 1;
            }
            let normalized = normalize_memory_card_input(raw.clone()).expect("params normalized");
            self.insert_memory_sources_on(
                &mut tx,
                id,
                normalized.observation_scope.chat_id,
                normalized.observation_scope.thread_id,
                &normalized,
                fallback_observed_at,
            )
            .await?;
            ids.push(id);
        }

        tx.commit().await?;
        Ok((stats, ids))
    }

    pub async fn insert_episode_lexical(
        &self,
        episode: openplotva_memory::Episode,
        model: &str,
        prompt_version: &str,
    ) -> Result<(i64, bool), StorageError> {
        self.insert_episode_with_embedding(episode, model, prompt_version, None)
            .await
    }

    /// Insert one memory episode with an optional pgvector embedding.
    pub async fn insert_episode_with_embedding(
        &self,
        mut episode: openplotva_memory::Episode,
        model: &str,
        prompt_version: &str,
        embedding: Option<&PgEmbeddingVector>,
    ) -> Result<(i64, bool), StorageError> {
        let summary_text = episode.summary_text.trim().to_owned();
        if summary_text.is_empty() || episode.chat_id == 0 {
            return Ok((0, false));
        }
        if episode.visibility.is_empty() {
            episode.visibility = openplotva_memory::chat_visibility(
                openplotva_memory::CARD_KIND_CHAT,
                episode.thread_id,
            );
        }
        let embedding = pgvector_literal(embedding);
        let row = sqlx::query(SQL_INSERT_MEMORY_EPISODE_WITH_EMBEDDING)
            .bind(&episode.visibility)
            .bind(episode.chat_id)
            .bind(episode.thread_id)
            .bind(episode.range_start_at)
            .bind(episode.range_end_at)
            .bind(episode.message_count)
            .bind(&summary_text)
            .bind(&episode.topics)
            .bind(&episode.participants)
            .bind(model.trim())
            .bind(prompt_version.trim())
            .bind(memory_cursor_after_at(episode.cursor_after_at))
            .bind(episode.cursor_message_id)
            .bind(episode.cursor_entry_id.trim())
            .bind(embedding)
            .fetch_one(&self.pool)
            .await?;
        Ok((row.try_get("id")?, row.try_get("inserted")?))
    }

    pub async fn insert_links(
        &self,
        links: &[openplotva_memory::LinkInput],
    ) -> Result<(), StorageError> {
        let Some(params) = memory_link_batch_params(links) else {
            return Ok(());
        };
        sqlx::query(SQL_UPSERT_MEMORY_LINKS)
            .bind(&params.from_card_ids)
            .bind(&params.to_card_ids)
            .bind(&params.relations)
            .bind(&params.confidences)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Mark one active memory card as superseded by another card.
    pub async fn supersede_card(&self, old_id: i64, new_id: i64) -> Result<(), StorageError> {
        if old_id == 0 || new_id == 0 || old_id == new_id {
            return Ok(());
        }
        sqlx::query(SQL_SUPERSEDE_MEMORY_CARD)
            .bind(new_id)
            .bind(old_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Mark exhausted processing memory runs before claiming fresh work.
    pub async fn mark_exhausted_runs(&self) -> Result<u64, StorageError> {
        Ok(sqlx::query(SQL_MARK_EXHAUSTED_MEMORY_RUNS)
            .execute(&self.pool)
            .await?
            .rows_affected())
    }

    /// Claim one queued, retryable failed, or stale processing memory run.
    pub async fn claim_run(
        &self,
        owner: &str,
        lease: Duration,
    ) -> Result<Option<openplotva_memory::Run>, StorageError> {
        self.mark_exhausted_runs().await?;
        let lease = if lease.is_zero() {
            Duration::from_secs(15 * 60)
        } else {
            lease
        };
        let row = sqlx::query(SQL_CLAIM_MEMORY_RUN)
            .bind(openplotva_memory::PROMPT_VERSION)
            .bind(owner.trim())
            .bind(OffsetDateTime::now_utc() + duration_to_time(lease))
            .fetch_optional(&self.pool)
            .await?;
        row.map(memory_run_from_claim_row).transpose()
    }

    pub async fn complete_run(
        &self,
        run_id: i64,
        stats: openplotva_memory::RunStats,
    ) -> Result<(), StorageError> {
        if run_id == 0 {
            return Ok(());
        }
        sqlx::query(SQL_COMPLETE_MEMORY_RUN)
            .bind(run_id)
            .bind(stats.cards_inserted)
            .bind(stats.cards_updated)
            .bind(stats.cards_superseded)
            .bind(stats.episodes_inserted)
            .bind(stats.input_tokens)
            .bind(stats.output_tokens)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn fail_run(&self, run_id: i64, cause: &str) -> Result<(), StorageError> {
        if run_id == 0 {
            return Ok(());
        }
        sqlx::query(SQL_FAIL_MEMORY_RUN)
            .bind(run_id)
            .bind(cause)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Queue continuation for a run when the selected input was truncated.
    pub async fn enqueue_run_continuation(
        &self,
        run: &openplotva_memory::Run,
        after: &openplotva_memory::Message,
        remaining_messages: i32,
    ) -> Result<(), StorageError> {
        if openplotva_memory::is_memory_zero_time(after.occurred_at) {
            return Ok(());
        }
        sqlx::query(SQL_ENQUEUE_MEMORY_RUN_CONTINUATION)
            .bind(run.chat_id)
            .bind(run.thread_id)
            .bind(run.range_start_at)
            .bind(run.range_end_at)
            .bind(run.prompt_version.trim())
            .bind(after.occurred_at)
            .bind(after.message_id)
            .bind(after.entry_id.trim())
            .bind(remaining_messages.max(0))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Retry one failed memory run.
    pub async fn retry_run(&self, run_id: i64) -> Result<(), StorageError> {
        if run_id == 0 {
            return Ok(());
        }
        sqlx::query(SQL_RETRY_MEMORY_RUN)
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Retry all failed memory runs.
    pub async fn retry_failed_runs(&self) -> Result<u64, StorageError> {
        Ok(sqlx::query(SQL_RETRY_FAILED_MEMORY_RUNS)
            .execute(&self.pool)
            .await?
            .rows_affected())
    }

    pub async fn ensure_daily_runs(
        &self,
        now: OffsetDateTime,
        retention: Duration,
    ) -> Result<u64, StorageError> {
        let retention = if retention.is_zero() {
            Duration::from_secs(7 * 24 * 60 * 60)
        } else {
            retention
        };
        let now = now.to_offset(time::UtcOffset::UTC);
        let day_end = now.date().midnight().assume_utc();
        let earliest = now - duration_to_time(retention);
        let mut day_start = earliest.date().midnight().assume_utc();
        let mut total = 0;
        while day_start < day_end {
            let next_day = day_start + time::Duration::days(1);
            total += self
                .ensure_daily_run_window(day_start, next_day, earliest)
                .await?;
            day_start = next_day;
        }
        self.skip_superseded_runs().await?;
        Ok(total)
    }

    async fn ensure_daily_run_window(
        &self,
        window_start: OffsetDateTime,
        window_end: OffsetDateTime,
        earliest: OffsetDateTime,
    ) -> Result<u64, StorageError> {
        Ok(sqlx::query(SQL_ENSURE_DAILY_MEMORY_RUNS)
            .bind(openplotva_memory::PROMPT_VERSION)
            .bind(window_start)
            .bind(window_end)
            .bind(earliest)
            .execute(&self.pool)
            .await?
            .rows_affected())
    }

    /// Skip queued/stale runs from older prompt versions.
    pub async fn skip_superseded_runs(&self) -> Result<u64, StorageError> {
        Ok(sqlx::query(SQL_SKIP_SUPERSEDED_MEMORY_RUNS)
            .bind(openplotva_memory::PROMPT_VERSION)
            .execute(&self.pool)
            .await?
            .rows_affected())
    }

    pub async fn load_run_messages(
        &self,
        run: &openplotva_memory::Run,
        max_messages_per_run: i32,
    ) -> Result<Vec<openplotva_memory::Message>, StorageError> {
        let limit = if max_messages_per_run <= 1 {
            201
        } else {
            max_messages_per_run + 1
        };
        let cursor_at = memory_cursor_after_at(run.cursor_after_at);
        let rows = sqlx::query(SQL_SELECT_MEMORY_RUN_MESSAGES)
            .bind(run.chat_id)
            .bind(run.thread_id)
            .bind(run.range_start_at)
            .bind(run.range_end_at)
            .bind(cursor_at)
            .bind(run.cursor_message_id)
            .bind(run.cursor_entry_id.trim())
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let payload: String = row.try_get("payload")?;
            let entry =
                decode_summary_message_entry_payload(payload.as_bytes()).map_err(|source| {
                    match source {
                        openplotva_history::SummaryEntryDecodeError::Json(source) => {
                            StorageError::HistoryPayloadCodec { source }
                        }
                    }
                })?;
            let message = memory_message_from_history_entry(&entry);
            if !message.text.trim().is_empty() {
                out.push(message);
            }
        }
        Ok(out)
    }

    pub async fn list_visible_cards(
        &self,
        scope: &openplotva_memory::RetrievalScope,
        limit: i32,
    ) -> Result<Vec<openplotva_memory::Card>, StorageError> {
        let limit = positive_or_default(limit, 100);
        let rows = sqlx::query(SQL_LIST_VISIBLE_MEMORY_CARDS)
            .bind(scope.chat_id)
            .bind(scope.thread_id)
            .bind(scope.user_id)
            .bind(openplotva_memory::include_public_user_memory(scope))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(memory_card_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn soft_delete_visible_card(
        &self,
        id: i64,
        deleted_by_user_id: i64,
        scope: &openplotva_memory::RetrievalScope,
    ) -> Result<bool, StorageError> {
        if id == 0 {
            return Ok(false);
        }
        let result = sqlx::query(SQL_SOFT_DELETE_VISIBLE_MEMORY_CARD)
            .bind(deleted_by_user_id)
            .bind(id)
            .bind(scope.chat_id)
            .bind(scope.thread_id)
            .bind(scope.user_id)
            .bind(openplotva_memory::include_public_user_memory(scope))
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// List memory cards for admin/runtime diagnostics.
    pub async fn list_cards(
        &self,
        filter: &openplotva_memory::CardFilter,
    ) -> Result<Vec<openplotva_memory::Card>, StorageError> {
        let limit = if filter.limit <= 0 || filter.limit > 500 {
            100
        } else {
            filter.limit
        };
        let rows = sqlx::query(SQL_LIST_MEMORY_CARDS)
            .bind(filter.chat_id)
            .bind(filter.thread_id)
            .bind(filter.user_id)
            .bind(filter.status.trim())
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(memory_card_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    /// List memory runs for admin diagnostics.
    pub async fn list_runs(
        &self,
        limit: i32,
    ) -> Result<Vec<openplotva_memory::RunRecord>, StorageError> {
        let limit = positive_or_default(limit, 100).min(500);
        let rows = sqlx::query(SQL_LIST_MEMORY_RUNS)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(memory_run_record_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn run_analytics(
        &self,
        since: OffsetDateTime,
    ) -> Result<openplotva_memory::RunAnalytics, StorageError> {
        let mut analytics = openplotva_memory::RunAnalytics {
            since,
            ..openplotva_memory::RunAnalytics::default()
        };
        let rows = sqlx::query(SQL_LIST_MEMORY_RUN_ANALYTICS)
            .fetch_all(&self.pool)
            .await?;
        for row in rows {
            let stat = memory_run_status_stat_from_row(&row)?;
            match stat.status.as_str() {
                "queued" => analytics.queued_count = stat.count,
                "processing" => analytics.processing_count = stat.count,
                "completed" => analytics.completed_count = stat.count,
                "failed" => analytics.failed_count = stat.count,
                _ => {}
            }
            analytics.statuses.push(stat);
        }

        let rows = sqlx::query(SQL_LIST_MEMORY_RUN_ERROR_ANALYTICS)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        analytics.recent_errors = rows
            .iter()
            .map(memory_run_error_stat_from_row)
            .collect::<Result<Vec<_>, _>>()?;

        let row = sqlx::query(SQL_GET_MEMORY_RUN_ANALYTICS_META)
            .fetch_one(&self.pool)
            .await?;
        analytics.stale_processing_count = row.try_get("stale_processing_count")?;
        analytics.latest_completed_at = row.try_get("latest_completed_at")?;
        analytics.latest_updated_at = row.try_get("latest_updated_at")?;
        analytics.latest_run_with_token_stats = row.try_get("latest_token_stats_at")?;
        Ok(analytics)
    }

    pub async fn soft_delete_card(
        &self,
        id: i64,
        deleted_by_user_id: i64,
    ) -> Result<(), StorageError> {
        if id == 0 {
            return Ok(());
        }
        sqlx::query(SQL_SOFT_DELETE_MEMORY_CARD)
            .bind(deleted_by_user_id)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn retrieve_lexical(
        &self,
        req: &openplotva_memory::RetrievalRequest,
    ) -> Result<openplotva_memory::RetrievedMemory, StorageError> {
        self.retrieve_with_vector(req, None).await
    }

    pub async fn retrieve_with_vector(
        &self,
        req: &openplotva_memory::RetrievalRequest,
        query_embedding: Option<&PgEmbeddingVector>,
    ) -> Result<openplotva_memory::RetrievedMemory, StorageError> {
        let query = req.query.trim();
        if query.is_empty() {
            return Ok(openplotva_memory::RetrievedMemory::default());
        }
        let limits = memory_retrieval_limits(req);
        let lexical_cards = self
            .retrieve_cards_lexical(&req.scope, query, limits.cards)
            .await?;
        let vector_cards = if let Some(query_embedding) = query_embedding {
            self.retrieve_cards_vector(&req.scope, query_embedding, limits.cards)
                .await?
        } else {
            Vec::new()
        };
        let cards =
            rank_retrieved_memory_cards(limits.cards as usize, &[lexical_cards, vector_cards]);
        let episodes = self
            .retrieve_episodes(&req.scope, query, limits.episodes)
            .await?;
        Ok(openplotva_memory::RetrievedMemory { cards, episodes })
    }

    async fn retrieve_cards_lexical(
        &self,
        scope: &openplotva_memory::RetrievalScope,
        query: &str,
        limit: i32,
    ) -> Result<Vec<ScoredMemoryCard>, StorageError> {
        let rows = sqlx::query(SQL_RETRIEVE_MEMORY_CARDS_LEXICAL)
            .bind(query)
            .bind(scope.chat_id)
            .bind(scope.thread_id)
            .bind(scope.user_id)
            .bind(openplotva_memory::include_public_user_memory(scope))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                let score = row.try_get("score")?;
                let card = memory_card_from_row(row)?;
                Ok(ScoredMemoryCard { card, score })
            })
            .collect::<Result<Vec<_>, StorageError>>()
    }

    async fn retrieve_cards_vector(
        &self,
        scope: &openplotva_memory::RetrievalScope,
        query_embedding: &PgEmbeddingVector,
        limit: i32,
    ) -> Result<Vec<ScoredMemoryCard>, StorageError> {
        let Some(query_embedding) = pgvector_literal(Some(query_embedding)) else {
            return Ok(Vec::new());
        };
        let rows = sqlx::query(SQL_RETRIEVE_MEMORY_CARDS_VECTOR)
            .bind(query_embedding)
            .bind(scope.chat_id)
            .bind(scope.thread_id)
            .bind(scope.user_id)
            .bind(openplotva_memory::include_public_user_memory(scope))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                let score = row.try_get("score")?;
                let card = memory_card_from_row(row)?;
                Ok(ScoredMemoryCard { card, score })
            })
            .collect::<Result<Vec<_>, StorageError>>()
    }

    async fn retrieve_episodes(
        &self,
        scope: &openplotva_memory::RetrievalScope,
        query: &str,
        limit: i32,
    ) -> Result<Vec<openplotva_memory::Episode>, StorageError> {
        let rows = sqlx::query(SQL_RETRIEVE_MEMORY_EPISODES)
            .bind(query)
            .bind(scope.chat_id)
            .bind(scope.thread_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(memory_episode_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn insert_memory_sources_on(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        card_id: i64,
        chat_id: i64,
        thread_id: i32,
        card: &openplotva_memory::CardInput,
        fallback_observed_at: OffsetDateTime,
    ) -> Result<(), StorageError> {
        let (params, ok) =
            memory_source_batch_params_at(card_id, chat_id, thread_id, card, fallback_observed_at);
        if !ok {
            return Ok(());
        }
        sqlx::query(SQL_INSERT_MEMORY_SOURCES)
            .bind(params.card_id)
            .bind(params.chat_id)
            .bind(params.thread_id)
            .bind(&params.entry_ids)
            .bind(&params.message_ids)
            .bind(params.occurred_at)
            .bind(params.confidence)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}

/// SQLx-backed Shield document retrieval store.
#[derive(Clone, Debug)]
pub struct PostgresShieldStore {
    pool: PgPool,
}

impl PostgresShieldStore {
    /// Build a Shield store over an existing pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_document(
        &self,
        input: openplotva_shield::DocumentInput,
        embedding: Option<&PgEmbeddingVector>,
    ) -> Result<openplotva_shield::Document, StorageError> {
        let embedding = pgvector_literal(embedding);
        let row = sqlx::query(SQL_CREATE_SHIELD_DOCUMENT)
            .bind(&input.slug)
            .bind(&input.title)
            .bind(&input.body)
            .bind(&input.category)
            .bind(input.enabled)
            .bind(input.priority)
            .bind(embedding)
            .fetch_one(&self.pool)
            .await?;
        shield_document_from_row(row)
    }

    pub async fn update_document(
        &self,
        id: i64,
        input: openplotva_shield::DocumentInput,
        embedding: Option<&PgEmbeddingVector>,
    ) -> Result<openplotva_shield::Document, StorageError> {
        let embedding = pgvector_literal(embedding);
        let row = sqlx::query(SQL_UPDATE_SHIELD_DOCUMENT)
            .bind(&input.slug)
            .bind(&input.title)
            .bind(&input.body)
            .bind(&input.category)
            .bind(input.enabled)
            .bind(input.priority)
            .bind(true)
            .bind(embedding)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        shield_document_from_row(row)
    }

    /// Delete a Shield document by ID.
    pub async fn delete_document(&self, id: i64) -> Result<(), StorageError> {
        sqlx::query(SQL_DELETE_SHIELD_DOCUMENT)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// List Shield documents for the admin API.
    pub async fn list_documents(
        &self,
        filter: &openplotva_shield::ListFilter,
    ) -> Result<Vec<openplotva_shield::Document>, StorageError> {
        let limit = positive_or_default(filter.limit, 100);
        let search_query = optional_trimmed_string(&filter.query);
        let rows = sqlx::query(SQL_LIST_SHIELD_DOCUMENTS)
            .bind(filter.include_disabled)
            .bind(search_query)
            .bind(limit)
            .bind(filter.offset.max(0))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(shield_document_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    /// List Shield documents that need title embedding rebuild.
    pub async fn documents_without_embeddings(
        &self,
        limit: i32,
    ) -> Result<Vec<openplotva_shield::Document>, StorageError> {
        let rows = sqlx::query(SQL_GET_SHIELD_DOCUMENTS_WITHOUT_EMBEDDINGS)
            .bind(positive_or_default(limit, 100))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(shield_document_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Update a Shield document embedding.
    pub async fn update_embedding(
        &self,
        id: i64,
        embedding: Option<&PgEmbeddingVector>,
    ) -> Result<(), StorageError> {
        let embedding = pgvector_literal(embedding);
        sqlx::query(SQL_UPDATE_SHIELD_DOCUMENT_EMBEDDING)
            .bind(embedding)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn search_lexical(
        &self,
        request: &ShieldSearchRequest,
        options: &ShieldOptions,
    ) -> Result<openplotva_shield::SearchResult, StorageError> {
        self.search_with_vector(request, options, None).await
    }

    pub async fn search_with_vector(
        &self,
        request: &ShieldSearchRequest,
        options: &ShieldOptions,
        query_embedding: Option<&PgEmbeddingVector>,
    ) -> Result<openplotva_shield::SearchResult, StorageError> {
        let options = options.clone().with_defaults();
        let query = openplotva_shield::normalize_query(&request.query, options.query_max_chars);
        if query.trim().is_empty() {
            return Ok(openplotva_shield::SearchResult::default());
        }

        let max_matches = openplotva_shield::search_max_matches(request.max_matches, &options);
        let candidate_limit = openplotva_shield::candidate_limit(max_matches);
        let lexical_rows = self.search_lexical_rows(&query, candidate_limit).await?;
        let vector_attempted = query_embedding
            .and_then(|vector| pgvector_literal(Some(vector)))
            .is_some();
        let vector_rows = if vector_attempted {
            let query_embedding = query_embedding.expect("vector_attempted requires embedding");
            self.search_vector_rows(query_embedding, candidate_limit)
                .await?
        } else {
            Vec::new()
        };
        let matches =
            openplotva_shield::merge_matches(&lexical_rows, &vector_rows, &options, max_matches);
        let mut result = openplotva_shield::SearchResult {
            query,
            context: openplotva_shield::format_context(&matches, max_matches),
            matches,
            lexical_only: !vector_attempted,
            ..openplotva_shield::SearchResult::default()
        };
        if request.include_candidates {
            result.candidates = openplotva_shield::build_candidates(
                &lexical_rows,
                &vector_rows,
                &result.matches,
                &options,
            );
            result.debug = Some(openplotva_shield::SearchDebug {
                max_matches,
                candidate_limit,
                lexical_min_score: options.lexical_min_score,
                vector_min_score: options.vector_min_score,
            });
        }
        Ok(result)
    }

    async fn search_lexical_rows(
        &self,
        query: &str,
        limit: i32,
    ) -> Result<Vec<openplotva_shield::ScoredDocument>, StorageError> {
        let rows = sqlx::query(SQL_SEARCH_SHIELD_DOCUMENTS_LEXICAL)
            .bind(limit)
            .bind(query)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(shield_scored_document_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn search_vector_rows(
        &self,
        query_embedding: &PgEmbeddingVector,
        limit: i32,
    ) -> Result<Vec<openplotva_shield::ScoredDocument>, StorageError> {
        let Some(query_embedding) = pgvector_literal(Some(query_embedding)) else {
            return Ok(Vec::new());
        };
        let rows = sqlx::query(SQL_SEARCH_SHIELD_DOCUMENTS_VECTOR)
            .bind(query_embedding)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(shield_vector_scored_document_from_row)
            .collect::<Result<Vec<_>, _>>()
    }
}

#[derive(Clone, Debug)]
pub struct PostgresPaymentStore {
    pool: PgPool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionCreate<'value> {
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram Stars payment charge ID.
    pub telegram_payment_charge_id: &'value str,
    /// Provider-side payment charge ID, often empty for Stars.
    pub provider_payment_charge_id: &'value str,
    /// Subscription expiry recorded at payment processing time.
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionRecord {
    /// Database primary key.
    pub id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram Stars payment charge ID.
    pub telegram_payment_charge_id: String,
    /// Provider-side payment charge ID, often empty for Stars.
    pub provider_payment_charge_id: String,
    /// Subscription expiry.
    pub expires_at: OffsetDateTime,
    /// Row creation time.
    pub created_at: OffsetDateTime,
    /// Row update time.
    pub updated_at: OffsetDateTime,
    /// Telegram-side cancellation timestamp, when recorded.
    pub canceled_at: Option<OffsetDateTime>,
    /// Refund timestamp, when recorded.
    pub refunded_at: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DonationCreate<'value> {
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram Stars payment charge ID.
    pub telegram_payment_charge_id: &'value str,
    /// Provider-side payment charge ID, often empty for Stars.
    pub provider_payment_charge_id: &'value str,
    /// Donation amount in Telegram Stars.
    pub amount_stars: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DonationRecord {
    /// Database primary key.
    pub id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram Stars payment charge ID.
    pub telegram_payment_charge_id: String,
    /// Provider-side payment charge ID, often empty for Stars.
    pub provider_payment_charge_id: String,
    /// Donation amount in Telegram Stars.
    pub amount_stars: i64,
    /// Row creation time.
    pub created_at: OffsetDateTime,
}

impl PostgresPaymentStore {
    /// Build a payment store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn create_subscription(
        &self,
        subscription: SubscriptionCreate<'_>,
    ) -> Result<SubscriptionRecord, StorageError> {
        let row = sqlx::query(SQL_CREATE_SUBSCRIPTION)
            .bind(subscription.user_id)
            .bind(subscription.telegram_payment_charge_id)
            .bind(subscription.provider_payment_charge_id)
            .bind(subscription.expires_at)
            .fetch_one(&self.pool)
            .await?;
        subscription_from_row(row).map_err(StorageError::from)
    }

    /// Load the current active non-admin, non-canceled, non-refunded subscription for a user.
    pub async fn get_active_subscription(
        &self,
        user_id: i64,
    ) -> Result<Option<SubscriptionRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_ACTIVE_SUBSCRIPTION)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(subscription_from_row).transpose()?)
    }

    pub async fn list_subscriptions_by_user(
        &self,
        user_id: i64,
    ) -> Result<Vec<SubscriptionRecord>, StorageError> {
        let rows = sqlx::query(SQL_LIST_SUBSCRIPTIONS_BY_USER)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(subscription_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    /// Load a subscription by Telegram Stars payment charge ID.
    pub async fn get_subscription_by_telegram_payment_charge_id(
        &self,
        telegram_payment_charge_id: &str,
    ) -> Result<Option<SubscriptionRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_SUBSCRIPTION_BY_TELEGRAM_PAYMENT_CHARGE_ID)
            .bind(telegram_payment_charge_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(subscription_from_row).transpose()?)
    }

    /// Delete a subscription row and return the removed row.
    pub async fn delete_subscription(&self, id: i64) -> Result<SubscriptionRecord, StorageError> {
        let row = sqlx::query(SQL_DELETE_SUBSCRIPTION)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        subscription_from_row(row).map_err(StorageError::from)
    }

    /// Set a subscription expiry and return the updated row.
    pub async fn expire_subscription(
        &self,
        id: i64,
        expires_at: OffsetDateTime,
    ) -> Result<SubscriptionRecord, StorageError> {
        let row = sqlx::query(SQL_EXPIRE_SUBSCRIPTION)
            .bind(id)
            .bind(expires_at)
            .fetch_one(&self.pool)
            .await?;
        subscription_from_row(row).map_err(StorageError::from)
    }

    pub async fn mark_subscription_canceled(
        &self,
        id: i64,
    ) -> Result<SubscriptionRecord, StorageError> {
        let row = sqlx::query(SQL_MARK_SUBSCRIPTION_CANCELED)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        subscription_from_row(row).map_err(StorageError::from)
    }

    pub async fn mark_subscription_refunded(
        &self,
        id: i64,
    ) -> Result<SubscriptionRecord, StorageError> {
        let row = sqlx::query(SQL_MARK_SUBSCRIPTION_REFUNDED)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        subscription_from_row(row).map_err(StorageError::from)
    }

    /// Insert a donation payment row.
    ///
    /// Duplicate Telegram charge IDs return `sqlx::Error::RowNotFound`, matching
    pub async fn create_donation(
        &self,
        donation: DonationCreate<'_>,
    ) -> Result<DonationRecord, StorageError> {
        let row = sqlx::query(SQL_CREATE_DONATION)
            .bind(donation.user_id)
            .bind(donation.telegram_payment_charge_id)
            .bind(donation.provider_payment_charge_id)
            .bind(donation.amount_stars)
            .fetch_one(&self.pool)
            .await?;
        donation_from_row(row).map_err(StorageError::from)
    }

    /// Load a donation by Telegram Stars payment charge ID.
    pub async fn get_donation_by_telegram_payment_charge_id(
        &self,
        telegram_payment_charge_id: &str,
    ) -> Result<Option<DonationRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_DONATION_BY_TELEGRAM_PAYMENT_CHARGE_ID)
            .bind(telegram_payment_charge_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(donation_from_row).transpose()?)
    }

    /// Delete a donation row and return the removed row.
    pub async fn delete_donation(&self, id: i64) -> Result<DonationRecord, StorageError> {
        let row = sqlx::query(SQL_DELETE_DONATION)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        donation_from_row(row).map_err(StorageError::from)
    }
}

#[derive(Clone, Debug)]
pub struct PostgresVipStore {
    pool: PgPool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VipCacheUpsert {
    /// Telegram user ID.
    pub user_id: i64,
    /// Whether the user has VIP according to the external Telegram check cache.
    pub is_vip: bool,
    /// Cached VIP expiry timestamp.
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VipCacheRecord {
    /// Telegram user ID.
    pub user_id: i64,
    /// Whether the user has VIP according to the external Telegram check cache.
    pub is_vip: bool,
    /// Cached VIP expiry timestamp.
    pub expires_at: OffsetDateTime,
    /// Row creation time.
    pub created_at: OffsetDateTime,
    /// Row update time.
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VipEventCreate<'value> {
    /// Telegram user ID.
    pub user_id: i64,
    /// VIP event type.
    pub event_type: &'value str,
    /// Delta in VIP seconds.
    pub delta_seconds: i64,
    /// Related subscription row, when applicable.
    pub subscription_id: Option<i64>,
    /// Admin or actor user ID, when applicable.
    pub actor_user_id: Option<i64>,
    pub reason: Option<&'value str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VipEventRecord {
    /// Database primary key.
    pub id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// VIP event type.
    pub event_type: String,
    /// Delta in VIP seconds.
    pub delta_seconds: i64,
    /// Effective expiry after applying this event.
    pub effective_expires_at: OffsetDateTime,
    /// Related subscription row, when applicable.
    pub subscription_id: Option<i64>,
    /// Admin or actor user ID, when applicable.
    pub actor_user_id: Option<i64>,
    /// Human-readable reason.
    pub reason: String,
    /// Row creation time.
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VipSummaryRecord {
    /// Latest VIP event ID.
    pub latest_event_id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Latest VIP event type.
    pub latest_event_type: String,
    /// Latest VIP delta in seconds.
    pub latest_delta_seconds: i64,
    /// Current effective expiry.
    pub effective_expires_at: OffsetDateTime,
    /// Whether the effective expiry is still in the future at query time.
    pub is_active: bool,
    pub remaining_seconds: i64,
    /// Related latest subscription row, when applicable.
    pub latest_subscription_id: Option<i64>,
    /// Related latest actor user, when applicable.
    pub latest_actor_user_id: Option<i64>,
    /// Latest event reason.
    pub latest_reason: String,
    /// Latest event creation time.
    pub latest_created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VipEventListRecord {
    /// Database primary key.
    pub id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// VIP event type.
    pub event_type: String,
    /// Delta in VIP seconds.
    pub delta_seconds: i64,
    /// Effective expiry after applying this event.
    pub effective_expires_at: OffsetDateTime,
    /// Related subscription row, when applicable.
    pub subscription_id: Option<i64>,
    /// Admin or actor user ID, when applicable.
    pub actor_user_id: Option<i64>,
    /// Joined actor username.
    pub actor_username: Option<String>,
    /// Joined actor first name.
    pub actor_first_name: Option<String>,
    /// Human-readable reason.
    pub reason: String,
    /// Row creation time.
    pub created_at: OffsetDateTime,
    /// Joined subscription Telegram payment charge ID.
    pub telegram_payment_charge_id: Option<String>,
    /// Joined subscription provider payment charge ID.
    pub provider_payment_charge_id: Option<String>,
    /// Joined subscription expiry.
    pub subscription_expires_at: Option<OffsetDateTime>,
    /// Joined subscription cancellation timestamp.
    pub subscription_canceled_at: Option<OffsetDateTime>,
    /// Joined subscription refund timestamp.
    pub subscription_refunded_at: Option<OffsetDateTime>,
}

impl PostgresVipStore {
    /// Build a VIP store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Upsert the legacy external VIP cache row.
    pub async fn upsert_vip_cache(&self, cache: VipCacheUpsert) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_VIP_CACHE)
            .bind(cache.user_id)
            .bind(cache.is_vip)
            .bind(cache.expires_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Load a non-expired VIP cache row by user ID.
    pub async fn get_vip_cache(
        &self,
        user_id: i64,
    ) -> Result<Option<VipCacheRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_VIP_CACHE)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(vip_cache_from_row).transpose()?)
    }

    /// Delete a VIP cache row by user ID.
    pub async fn delete_vip_cache(&self, user_id: i64) -> Result<(), StorageError> {
        sqlx::query(SQL_DELETE_VIP_CACHE)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete expired VIP cache rows and return affected user IDs.
    pub async fn cleanup_expired_vip_cache(&self) -> Result<Vec<i64>, StorageError> {
        let rows = sqlx::query_scalar::<_, i64>(SQL_CLEANUP_EXPIRED_VIP_CACHE)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    pub async fn create_vip_event(
        &self,
        event: VipEventCreate<'_>,
    ) -> Result<VipEventRecord, StorageError> {
        let row = sqlx::query(SQL_CREATE_VIP_EVENT)
            .bind(event.user_id)
            .bind(event.event_type)
            .bind(event.delta_seconds)
            .bind(event.subscription_id)
            .bind(event.actor_user_id)
            .bind(event.reason)
            .fetch_one(&self.pool)
            .await?;
        vip_event_from_row(row).map_err(StorageError::from)
    }

    /// Load the latest VIP ledger summary for a user.
    pub async fn get_vip_summary_by_user(
        &self,
        user_id: i64,
    ) -> Result<Option<VipSummaryRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_VIP_SUMMARY_BY_USER)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(vip_summary_from_row).transpose()?)
    }

    pub async fn list_vip_events_by_user(
        &self,
        user_id: i64,
    ) -> Result<Vec<VipEventListRecord>, StorageError> {
        let rows = sqlx::query(SQL_LIST_VIP_EVENTS_BY_USER)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(vip_event_list_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }
}

/// Storage connection failures.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Postgres connection or ping failed.
    #[error("failed to connect to Postgres: {source}")]
    Postgres {
        /// SQLx error.
        #[from]
        source: sqlx::Error,
    },
    /// Redis client configuration failed.
    #[error("invalid Redis configuration: {source}")]
    RedisConfig {
        /// Redis configuration error.
        source: redis::RedisError,
    },
    /// Redis connection or ping failed.
    #[error("failed to connect to Redis: {source}")]
    Redis {
        /// Redis connection error.
        source: redis::RedisError,
    },
    #[error("failed to apply SQL migrations: {source}")]
    Migrate {
        #[from]
        source: MigrateError,
    },
    #[error("unknown existing migration ids: {ids:?}")]
    UnknownLegacyMigrations { ids: Vec<String> },
    /// Rate-limit expiry JSON codec failed.
    #[error("decode rate limit expiry: {source}")]
    RateLimitCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Draw rate-limit timestamp JSON codec failed.
    #[error("decode draw rate limit timestamps: {source}")]
    DrawRateLimitCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Chat-admin ID list JSON codec failed.
    #[error("decode chat admin ids: {source}")]
    ChatAdminIdsCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Ephemeral message JSON codec failed.
    #[error("decode ephemeral message: {source}")]
    EphemeralMessageCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Last-generation JSON codec failed.
    #[error("decode last generation: {source}")]
    LastGenerationCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Translation cache JSON codec failed.
    #[error("decode translation cache: {source}")]
    TranslationCacheCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Blocked-chat timestamp JSON codec failed.
    #[error("decode blocked chat: {source}")]
    BlockedChatCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Rate-limit expiry timestamp could not be represented.
    #[error("invalid rate limit expiry timestamp: {source}")]
    RateLimitTimestamp {
        /// Timestamp range error.
        source: time::error::ComponentRange,
    },
    /// Draw rate-limit timestamp could not be represented.
    #[error("invalid draw rate limit timestamp: {source}")]
    DrawRateLimitTimestamp {
        /// Timestamp range error.
        source: time::error::ComponentRange,
    },
    /// Ephemeral message expiry timestamp could not be represented.
    #[error("invalid ephemeral message expiry timestamp: {source}")]
    EphemeralMessageTimestamp {
        /// Timestamp range error.
        source: time::error::ComponentRange,
    },
    /// Blocked-chat timestamp could not be represented.
    #[error("invalid blocked chat timestamp: {source}")]
    BlockedChatTimestamp {
        /// Timestamp range error.
        source: time::error::ComponentRange,
    },
    /// Chat history payload JSON codec failed.
    #[error("decode chat history payload: {source}")]
    HistoryPayloadCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    #[error("chat history payload is not a JSON object")]
    HistoryPayloadShape,
    /// Chat history summary preparation failed before SQL persistence.
    #[error("prepare chat history summary: {source}")]
    HistorySummaryPrepare {
        /// Summary preparation error.
        source: PrepareStoredSummaryError,
    },
    /// Chat history summary JSON codec failed.
    #[error("decode chat history summary: {source}")]
    HistorySummaryCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Telegram file metadata JSON codec failed.
    #[error("decode telegram file metadata: {source}")]
    TelegramFileCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
}

impl StorageError {
    #[must_use]
    pub fn is_row_not_found(&self) -> bool {
        matches!(
            self,
            Self::Postgres {
                source: sqlx::Error::RowNotFound
            }
        )
    }
}

pub fn rate_limited_chat_key(chat_id: i64) -> String {
    format!("{RATE_LIMITED_CHAT_KEY_PREFIX}{chat_id}")
}

pub fn chat_admins_key(chat_id: i64) -> String {
    format!("{CHAT_ADMINS_KEY_PREFIX}{chat_id}{CHAT_ADMINS_KEY_SUFFIX}")
}

pub fn queued_sticker_key(chat_id: i64, message_id: i64) -> String {
    queued_sticker_key_with_prefix(QUEUED_STICKER_KEY_PREFIX, chat_id, message_id)
}

pub fn ephemeral_message_key(chat_id: i64, message_id: i64) -> String {
    format!("{EPHEMERAL_MESSAGE_KEY_PREFIX}{chat_id}:{message_id}")
}

pub fn last_generation_key(chat_id: i64, user_id: i64) -> String {
    format!("{LAST_GENERATION_KEY_PREFIX}{chat_id}:{user_id}")
}

pub fn translation_cache_key(target_lang: &str, text: &str) -> String {
    translation_cache_key_with_prefix(TRANSLATION_CACHE_KEY_PREFIX, target_lang, text)
}

pub fn blocked_chat_key(chat_id: i64) -> String {
    format!("{BLOCKED_CHAT_KEY_PREFIX}{chat_id}")
}

pub fn join_greeting_users_key(chat_id: i64) -> String {
    format!("join_greet:users:{chat_id}")
}

pub fn join_greeting_message_key(chat_id: i64) -> String {
    format!("join_greet:msg:{chat_id}")
}

pub fn join_greeting_debounce_key(chat_id: i64) -> String {
    format!("join_greet:debounce:{chat_id}")
}

pub fn translation_cache_hash_key(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn translation_cache_key_with_prefix(key_prefix: &str, target_lang: &str, text: &str) -> String {
    format!(
        "{}{}:{}",
        key_prefix,
        target_lang,
        translation_cache_hash_key(text)
    )
}

fn queued_sticker_key_with_prefix(key_prefix: &str, chat_id: i64, message_id: i64) -> String {
    format!("{key_prefix}{chat_id}:{message_id}")
}

fn queued_sticker_message_id_from_redis_value(value: Option<String>) -> Option<i64> {
    value.map(|value| value.trim().parse::<i64>().unwrap_or_default())
}

pub fn ephemeral_message_keys(messages: &[EphemeralMessage]) -> Vec<String> {
    messages
        .iter()
        .map(|message| ephemeral_message_key(message.chat_id, message.message_id))
        .collect()
}

pub fn history_cache_key(chat_id: i64) -> String {
    format!("{CHAT_HISTORY_CACHE_KEY_PREFIX}{chat_id}")
}

pub fn history_text_payload_with_text(
    payload: impl AsRef<[u8]>,
    new_text: &str,
) -> Result<String, StorageError> {
    let mut payload: serde_json::Value = serde_json::from_slice(payload.as_ref())
        .map_err(|source| StorageError::HistoryPayloadCodec { source })?;
    let object = payload
        .as_object_mut()
        .ok_or(StorageError::HistoryPayloadShape)?;
    if new_text.is_empty() {
        object.remove("text");
    } else {
        object.insert(
            "text".to_owned(),
            serde_json::Value::String(new_text.to_owned()),
        );
    }
    serde_json::to_string(&payload).map_err(|source| StorageError::HistoryPayloadCodec { source })
}

pub fn history_text_payload_with_message_update(
    payload: impl AsRef<[u8]>,
    new_text: &str,
    original_text: &str,
    meta: ChatMessageMeta,
) -> Result<String, StorageError> {
    let mut payload: serde_json::Value = serde_json::from_slice(payload.as_ref())
        .map_err(|source| StorageError::HistoryPayloadCodec { source })?;
    let object = payload
        .as_object_mut()
        .ok_or(StorageError::HistoryPayloadShape)?;
    let (text, original_text, meta) =
        normalize_history_message_update(new_text, original_text, meta);

    if text.is_empty() {
        object.remove("text");
    } else {
        object.insert("text".to_owned(), serde_json::Value::String(text));
    }
    if original_text.is_empty() {
        object.remove("original_text");
    } else {
        object.insert(
            "original_text".to_owned(),
            serde_json::Value::String(original_text),
        );
    }
    object.insert(
        "meta".to_owned(),
        serde_json::to_value(meta)
            .map_err(|source| StorageError::HistoryPayloadCodec { source })?,
    );

    serde_json::to_string(&payload).map_err(|source| StorageError::HistoryPayloadCodec { source })
}

pub fn history_text_payload_with_vision_descriptions(
    payload: impl AsRef<[u8]>,
    updates: &[VisionDescriptionUpdate],
) -> Result<Option<String>, StorageError> {
    let updates = normalize_vision_description_updates(updates);
    if updates.is_empty() {
        return Ok(None);
    }

    let mut payload: serde_json::Value = serde_json::from_slice(payload.as_ref())
        .map_err(|source| StorageError::HistoryPayloadCodec { source })?;
    let object = payload
        .as_object_mut()
        .ok_or(StorageError::HistoryPayloadShape)?;
    let meta_value = object
        .get("meta")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let meta = serde_json::from_value(meta_value)
        .map_err(|source| StorageError::HistoryPayloadCodec { source })?;
    let (meta, updated) = apply_vision_description_updates(meta, &updates);
    if !updated {
        return Ok(None);
    }

    object.insert(
        "meta".to_owned(),
        serde_json::to_value(meta)
            .map_err(|source| StorageError::HistoryPayloadCodec { source })?,
    );
    serde_json::to_string(&payload)
        .map(Some)
        .map_err(|source| StorageError::HistoryPayloadCodec { source })
}

#[must_use]
pub fn normalize_vision_description_updates(
    updates: &[VisionDescriptionUpdate],
) -> Vec<VisionDescriptionUpdate> {
    let mut out = Vec::with_capacity(updates.len());
    let mut seen = std::collections::HashSet::with_capacity(updates.len());
    for item in updates {
        let file_unique_id = item.file_unique_id.trim();
        let caption = item.caption.trim();
        if file_unique_id.is_empty() || caption.is_empty() {
            continue;
        }
        if !seen.insert(file_unique_id.to_owned()) {
            continue;
        }
        out.push(VisionDescriptionUpdate {
            file_unique_id: file_unique_id.to_owned(),
            caption: caption.to_owned(),
        });
    }
    out
}

#[must_use]
pub fn format_vision_description_updates(updates: &[VisionDescriptionUpdate]) -> String {
    if let [single] = updates {
        return single.caption.clone();
    }
    let mut out = String::new();
    for (idx, item) in updates.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        out.push_str(&format!("image_{}: {}", idx + 1, item.caption));
    }
    out.trim().to_owned()
}

fn apply_vision_description_updates(
    mut meta: ChatMessageMeta,
    updates: &[VisionDescriptionUpdate],
) -> (ChatMessageMeta, bool) {
    let mut updated = false;
    let vision_description = format_vision_description_updates(updates);
    if meta.vision_description.trim() != vision_description {
        meta.vision_description = vision_description;
        updated = true;
    }

    for item in updates {
        let Some(idx) = vision_attachment_index(&meta, &item.file_unique_id) else {
            meta.attachments.push(openplotva_core::ChatAttachment {
                kind: "image".to_owned(),
                source: "message".to_owned(),
                file_unique_id: item.file_unique_id.clone(),
                caption: item.caption.clone(),
                ..openplotva_core::ChatAttachment::default()
            });
            updated = true;
            continue;
        };
        if apply_vision_description_attachment_update(&mut meta.attachments[idx], item) {
            updated = true;
        }
    }
    if meta.message_type.trim().is_empty() {
        meta.message_type = "image".to_owned();
        updated = true;
    }
    (meta, updated)
}

fn vision_attachment_index(meta: &ChatMessageMeta, file_unique_id: &str) -> Option<usize> {
    let file_unique_id = file_unique_id.trim();
    if file_unique_id.is_empty() {
        return None;
    }
    meta.attachments.iter().position(|attachment| {
        attachment.file_unique_id.trim() == file_unique_id
            && matches!(attachment.source.trim(), "" | "message")
    })
}

fn apply_vision_description_attachment_update(
    attachment: &mut openplotva_core::ChatAttachment,
    update: &VisionDescriptionUpdate,
) -> bool {
    let mut updated = false;
    if !attachment.content.trim().is_empty() {
        attachment.content.clear();
        updated = true;
    }
    if attachment.caption.trim() != update.caption {
        attachment.caption = update.caption.clone();
        updated = true;
    }
    if attachment.kind.trim().is_empty() {
        attachment.kind = "image".to_owned();
        updated = true;
    }
    if attachment.source.trim() != "message" {
        attachment.source = "message".to_owned();
        updated = true;
    }
    updated
}

pub fn history_tool_call_entries_from_base_payload(
    chat_id: i64,
    message_id: i32,
    base_payload: impl AsRef<[u8]>,
    tool_calls: &[ToolCall],
) -> Result<Vec<HistoryToolEntryUpsert>, StorageError> {
    let base: serde_json::Value = serde_json::from_slice(base_payload.as_ref())
        .map_err(|source| StorageError::HistoryPayloadCodec { source })?;
    let object = base.as_object().ok_or(StorageError::HistoryPayloadShape)?;
    let base_time = history_payload_time(object).unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let chat = object
        .get("chat")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({ "id": chat_id }));
    let thread_id = history_payload_i32(object, "message_thread_id");

    let tool_calls = openplotva_core::filter_non_terminator_tool_calls(tool_calls);
    let mut entries = Vec::with_capacity(tool_calls.len() * 2);
    for (idx, call) in tool_calls.into_iter().enumerate() {
        let call_at = call
            .at
            .as_deref()
            .and_then(parse_history_payload_time_string);
        let (request_time, response_time) = if let Some(at) = call_at {
            (at, at)
        } else {
            (
                base_time + time::Duration::milliseconds((idx * 2 + 1) as i64),
                base_time + time::Duration::milliseconds((idx * 2 + 2) as i64),
            )
        };
        entries.push(history_tool_entry(
            "tool_request",
            "model",
            HistoryToolEntryBuild {
                chat_id,
                thread_id,
                message_id,
                idx,
                call: &call,
                at: request_time,
                chat: &chat,
                include_output: false,
            },
        )?);
        entries.push(history_tool_entry(
            "tool_response",
            "tool",
            HistoryToolEntryBuild {
                chat_id,
                thread_id,
                message_id,
                idx,
                call: &call,
                at: response_time,
                chat: &chat,
                include_output: true,
            },
        )?);
    }
    Ok(entries)
}

fn normalize_history_message_update(
    text: &str,
    original_text: &str,
    mut meta: ChatMessageMeta,
) -> (String, String, ChatMessageMeta) {
    let text = text.trim().to_owned();
    let mut original_text = original_text.trim().to_owned();
    if original_text == text {
        original_text.clear();
    }

    if !text.is_empty() {
        for attachment in &mut meta.attachments {
            if attachment.source.trim() != "message" {
                continue;
            }
            if !attachment.caption.trim().is_empty() {
                attachment.caption.clear();
            }
            if attachment.content.trim() == text {
                attachment.content.clear();
            }
        }
    }

    (text, original_text, meta)
}

struct HistoryToolEntryBuild<'a> {
    chat_id: i64,
    thread_id: i32,
    message_id: i32,
    idx: usize,
    call: &'a ToolCall,
    at: OffsetDateTime,
    chat: &'a serde_json::Value,
    include_output: bool,
}

fn history_tool_entry(
    kind: &str,
    role: &str,
    build: HistoryToolEntryBuild<'_>,
) -> Result<HistoryToolEntryUpsert, StorageError> {
    let entry_id = history_tool_entry_id(kind, build.message_id, build.idx, build.call);
    let timestamp = format_history_payload_time(build.at);
    let mut tool_call = build.call.clone();
    tool_call.at = Some(timestamp.clone());
    if !build.include_output {
        tool_call.output = None;
    }

    let mut payload = serde_json::Map::new();
    payload.insert(
        "entry_id".to_owned(),
        serde_json::Value::String(entry_id.clone()),
    );
    payload.insert(
        "role".to_owned(),
        serde_json::Value::String(role.to_owned()),
    );
    payload.insert(
        "kind".to_owned(),
        serde_json::Value::String(kind.to_owned()),
    );
    payload.insert("timestamp".to_owned(), serde_json::Value::String(timestamp));
    payload.insert(
        "message_id".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(build.message_id)),
    );
    payload.insert("chat".to_owned(), build.chat.clone());
    payload.insert(
        "date".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(build.at.unix_timestamp())),
    );
    if build.thread_id != 0 {
        payload.insert(
            "message_thread_id".to_owned(),
            serde_json::Value::Number(serde_json::Number::from(build.thread_id)),
        );
    }
    payload.insert(
        "meta".to_owned(),
        serde_json::to_value(ChatMessageMeta::default())
            .map_err(|source| StorageError::HistoryPayloadCodec { source })?,
    );
    payload.insert(
        "tool_call".to_owned(),
        serde_json::to_value(tool_call)
            .map_err(|source| StorageError::HistoryPayloadCodec { source })?,
    );

    let payload = serde_json::to_vec(&serde_json::Value::Object(payload))
        .map_err(|source| StorageError::HistoryPayloadCodec { source })?;
    Ok(HistoryToolEntryUpsert {
        bucket_day: build.at.date(),
        chat_id: build.chat_id,
        thread_id: build.thread_id,
        message_id: build.message_id,
        entry_id,
        kind: kind.to_owned(),
        role: role.to_owned(),
        occurred_at: build.at,
        sender_id: 0,
        payload,
    })
}

fn history_tool_entry_id(kind: &str, message_id: i32, idx: usize, call: &ToolCall) -> String {
    let name = call.name.trim();
    let r#ref = call.r#ref.trim();
    if r#ref.is_empty() {
        format!("{kind}:{message_id}:{name}:{idx}")
    } else {
        format!("{kind}:{message_id}:{name}:{ref}", ref = r#ref)
    }
}

fn history_payload_time(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<OffsetDateTime> {
    object
        .get("timestamp")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_history_payload_time_string)
        .or_else(|| {
            object
                .get("date")
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| OffsetDateTime::from_unix_timestamp(value).ok())
        })
}

fn parse_history_payload_time_string(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()
}

fn format_history_payload_time(value: OffsetDateTime) -> String {
    value
        .to_offset(time::UtcOffset::UTC)
        .format(&time::format_description::well_known::Rfc3339)
        .expect("Rfc3339 formatting should be infallible for valid OffsetDateTime")
}

fn history_payload_i32(object: &serde_json::Map<String, serde_json::Value>, key: &str) -> i32 {
    object
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
}

/// Encode a rate-limit expiry as the approved Rust-native Redis JSON value.
pub fn rate_limit_expiry_redis_value(expiry: OffsetDateTime) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(&RateLimitExpiryValue {
        unix_timestamp_nanos: expiry.unix_timestamp_nanos(),
    })
    .map_err(|source| StorageError::RateLimitCodec { source })
}

/// Decode a Rust-native rate-limit expiry Redis value.
pub fn rate_limit_expiry_from_redis_value(value: &[u8]) -> Result<OffsetDateTime, StorageError> {
    let value: RateLimitExpiryValue =
        serde_json::from_slice(value).map_err(|source| StorageError::RateLimitCodec { source })?;
    OffsetDateTime::from_unix_timestamp_nanos(value.unix_timestamp_nanos)
        .map_err(|source| StorageError::RateLimitTimestamp { source })
}

/// Encode draw-generation timestamps as the approved Rust-native Redis JSON value.
pub fn draw_rate_limit_timestamps_redis_value(
    timestamps: &[OffsetDateTime],
) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(&DrawRateLimitValue {
        unix_timestamp_nanos: timestamps
            .iter()
            .map(|timestamp| timestamp.unix_timestamp_nanos())
            .collect(),
    })
    .map_err(|source| StorageError::DrawRateLimitCodec { source })
}

/// Decode Rust-native draw-generation timestamp Redis values.
pub fn draw_rate_limit_timestamps_from_redis_value(
    value: &[u8],
) -> Result<Vec<OffsetDateTime>, StorageError> {
    let value: DrawRateLimitValue = serde_json::from_slice(value)
        .map_err(|source| StorageError::DrawRateLimitCodec { source })?;
    value
        .unix_timestamp_nanos
        .into_iter()
        .map(|timestamp| {
            OffsetDateTime::from_unix_timestamp_nanos(timestamp)
                .map_err(|source| StorageError::DrawRateLimitTimestamp { source })
        })
        .collect()
}

/// Encode a blocked-chat unblock timestamp as the approved Rust-native Redis JSON value.
pub fn blocked_chat_redis_value(unblock_at: OffsetDateTime) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(&BlockedChatValue {
        unblock_at_unix_timestamp_nanos: unblock_at.unix_timestamp_nanos(),
    })
    .map_err(|source| StorageError::BlockedChatCodec { source })
}

/// Decode a Rust-native blocked-chat unblock timestamp.
pub fn blocked_chat_from_redis_value(value: &[u8]) -> Result<OffsetDateTime, StorageError> {
    let value: BlockedChatValue = serde_json::from_slice(value)
        .map_err(|source| StorageError::BlockedChatCodec { source })?;
    OffsetDateTime::from_unix_timestamp_nanos(value.unblock_at_unix_timestamp_nanos)
        .map_err(|source| StorageError::BlockedChatTimestamp { source })
}

pub fn blocked_chat_is_active_at(unblock_at: Option<OffsetDateTime>, now: OffsetDateTime) -> bool {
    unblock_at.is_some_and(|unblock_at| now < unblock_at)
}

/// Encode cached chat administrator IDs as the approved Rust-native Redis JSON value.
pub fn chat_admin_ids_redis_value(admin_ids: &[i64]) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(admin_ids).map_err(|source| StorageError::ChatAdminIdsCodec { source })
}

/// Decode cached chat administrator IDs from the Rust-native Redis JSON value.
pub fn chat_admin_ids_from_redis_value(value: &[u8]) -> Result<Vec<i64>, StorageError> {
    serde_json::from_slice(value).map_err(|source| StorageError::ChatAdminIdsCodec { source })
}

/// Encode an ephemeral message as the approved Rust-native Redis JSON value.
pub fn ephemeral_message_redis_value(message: &EphemeralMessage) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(&EphemeralMessageValue {
        chat_id: message.chat_id,
        message_id: message.message_id,
        expires_at_unix_timestamp_nanos: message.expires_at.unix_timestamp_nanos(),
    })
    .map_err(|source| StorageError::EphemeralMessageCodec { source })
}

/// Decode an ephemeral message from the Rust-native Redis JSON value.
pub fn ephemeral_message_from_redis_value(value: &[u8]) -> Result<EphemeralMessage, StorageError> {
    let value: EphemeralMessageValue = serde_json::from_slice(value)
        .map_err(|source| StorageError::EphemeralMessageCodec { source })?;
    let expires_at =
        OffsetDateTime::from_unix_timestamp_nanos(value.expires_at_unix_timestamp_nanos)
            .map_err(|source| StorageError::EphemeralMessageTimestamp { source })?;
    Ok(EphemeralMessage {
        chat_id: value.chat_id,
        message_id: value.message_id,
        expires_at,
    })
}

/// Encode a last-generation record as the approved Rust-native Redis JSON value.
pub fn last_generation_redis_value(
    generation: &LastGenerationRecord,
) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(&LastGenerationValue {
        chat_id: generation.chat_id,
        user_id: generation.user_id,
        message_ids: generation.message_ids.clone(),
        caption: generation.caption.clone(),
        created_at: generation.created_at,
    })
    .map_err(|source| StorageError::LastGenerationCodec { source })
}

/// Decode a last-generation record from the Rust-native Redis JSON value.
pub fn last_generation_from_redis_value(
    value: &[u8],
) -> Result<LastGenerationRecord, StorageError> {
    let value: LastGenerationValue = serde_json::from_slice(value)
        .map_err(|source| StorageError::LastGenerationCodec { source })?;
    Ok(LastGenerationRecord {
        chat_id: value.chat_id,
        user_id: value.user_id,
        message_ids: value.message_ids,
        caption: value.caption,
        created_at: value.created_at,
    })
}

/// Encode a translation cache value as the approved Rust-native Redis JSON value.
pub fn translation_cache_redis_value(translation: &str) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(translation).map_err(|source| StorageError::TranslationCacheCodec { source })
}

/// Decode a translation cache value from the Rust-native Redis JSON value.
pub fn translation_cache_from_redis_value(value: &[u8]) -> Result<String, StorageError> {
    serde_json::from_slice(value).map_err(|source| StorageError::TranslationCacheCodec { source })
}

pub fn ephemeral_redis_ttl(duration: Duration, cleanup_interval: Duration) -> Duration {
    duration
        .saturating_add(cleanup_interval)
        .saturating_add(Duration::from_secs(1))
}

pub fn expired_ephemeral_messages_at(
    messages: &[EphemeralMessage],
    now: OffsetDateTime,
) -> Vec<EphemeralMessage> {
    messages
        .iter()
        .filter(|message| now > message.expires_at)
        .cloned()
        .collect()
}

pub fn rate_limit_is_active_at(expiry: Option<OffsetDateTime>, now: OffsetDateTime) -> bool {
    expiry.is_some_and(|expiry| now < expiry)
}

#[derive(Debug, Deserialize, Serialize)]
struct RateLimitExpiryValue {
    unix_timestamp_nanos: i128,
}

#[derive(Debug, Deserialize, Serialize)]
struct DrawRateLimitValue {
    unix_timestamp_nanos: Vec<i128>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BlockedChatValue {
    unblock_at_unix_timestamp_nanos: i128,
}

#[derive(Debug, Deserialize, Serialize)]
struct EphemeralMessageValue {
    chat_id: i64,
    message_id: i64,
    expires_at_unix_timestamp_nanos: i128,
}

#[derive(Debug, Deserialize, Serialize)]
struct LastGenerationValue {
    chat_id: i64,
    user_id: i64,
    message_ids: Vec<i64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    caption: String,
    created_at: i64,
}

fn memory_run_from_claim_row(row: PgRow) -> Result<openplotva_memory::Run, StorageError> {
    Ok(openplotva_memory::Run {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        range_start_at: row.try_get("range_start_at")?,
        range_end_at: row.try_get("range_end_at")?,
        prompt_version: row.try_get("prompt_version")?,
        cursor_after_at: row.try_get("cursor_after_at")?,
        cursor_message_id: row.try_get("cursor_after_message_id")?,
        cursor_entry_id: row.try_get("cursor_after_entry_id")?,
        attempts: row.try_get("attempts")?,
        message_count: row.try_get("message_count")?,
    })
}

fn memory_message_from_history_entry(entry: &SummaryMessageEntry) -> openplotva_memory::Message {
    let sender = memory_sender_from_history_entry(entry);
    let mut text = entry.original_text.trim();
    if text.is_empty() {
        text = entry.text.trim();
    }
    if text.is_empty() {
        text = entry.caption.trim();
    }
    let sender_type = if entry.meta.sender_type.trim().is_empty() {
        sender.sender_type.clone()
    } else {
        entry.meta.sender_type.trim().to_owned()
    };
    openplotva_memory::Message {
        entry_id: entry.entry_id.trim().to_owned(),
        message_id: entry.message_id,
        thread_id: entry.message_thread_id,
        user_id: sender.id,
        sender_name: sender.display_name(),
        sender_username: sender.username.trim().to_owned(),
        sender_type,
        sender_is_bot: sender.is_bot || entry.from.as_ref().is_some_and(|from| from.is_bot),
        is_forwarded: entry.forward_origin.is_some() || entry.is_automatic_forward,
        is_automatic_forward: entry.is_automatic_forward,
        forward_origin_type: entry
            .forward_origin
            .as_ref()
            .map(|origin| origin.origin_type.trim().to_owned())
            .unwrap_or_default(),
        via_bot_username: entry
            .via_bot
            .as_ref()
            .map(|bot| bot.username.trim().to_owned())
            .unwrap_or_default(),
        text: text.to_owned(),
        occurred_at: summary_message_entry_timestamp(entry),
    }
}

fn memory_sender_from_history_entry(entry: &SummaryMessageEntry) -> openplotva_core::MessageSender {
    if let Some(sender_chat) = &entry.sender_chat {
        let sender_type = if entry
            .chat
            .as_ref()
            .is_some_and(|chat| chat.id == sender_chat.id)
        {
            openplotva_core::SENDER_TYPE_SAME_CHAT
        } else {
            openplotva_core::SENDER_TYPE_CHANNEL
        };
        let full_name = memory_chat_full_name(sender_chat);
        return openplotva_core::MessageSender {
            sender_type: sender_type.to_owned(),
            id: sender_chat.id,
            full_name: if full_name.is_empty() {
                String::new()
            } else {
                format!("📣 {full_name}")
            },
            username: sender_chat.username.trim().to_owned(),
            is_bot: false,
        };
    }
    if let Some(from) = &entry.from {
        return openplotva_core::MessageSender {
            sender_type: openplotva_core::SENDER_TYPE_USER.to_owned(),
            id: from.id,
            full_name: memory_user_full_name(from),
            username: from.username.trim().to_owned(),
            is_bot: from.is_bot,
        };
    }
    openplotva_core::MessageSender::system()
}

fn memory_user_full_name(user: &openplotva_history::SummaryTelegramUser) -> String {
    let name = format!("{} {}", user.first_name, user.last_name)
        .trim()
        .to_owned();
    if name.is_empty() {
        user.username.clone()
    } else {
        name
    }
}

fn memory_chat_full_name(chat: &openplotva_history::SummaryTelegramChat) -> String {
    if !chat.title.is_empty() {
        return chat.title.trim().to_owned();
    }
    let name = format!("{} {}", chat.first_name, chat.last_name)
        .trim()
        .to_owned();
    if name.is_empty() {
        chat.username.clone()
    } else {
        name
    }
}

fn duration_to_time(duration: Duration) -> time::Duration {
    time::Duration::seconds(duration.as_secs().min(i64::MAX as u64) as i64)
}

fn redis_ttl_millis(ttl: Duration) -> u64 {
    let millis = ttl.as_millis();
    if millis == 0 {
        return 1;
    }
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn redis_ttl_seconds(ttl: Duration) -> u64 {
    let seconds = ttl.as_secs();
    if seconds == 0 {
        return 1;
    }
    seconds
}

fn legacy_migration_bridge_entries() -> Vec<LegacyMigrationBridgeEntry<'static>> {
    MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .filter_map(|migration| {
            legacy_migration_id_for_version(migration.version).map(|legacy_id| {
                LegacyMigrationBridgeEntry {
                    legacy_id,
                    migration,
                }
            })
        })
        .collect()
}

fn legacy_migration_id_for_version(version: i64) -> Option<&'static str> {
    match version {
        0 => Some("0_init.sql"),
        100 => Some("100_memory_pipeline.sql"),
        101 => Some("101_fix_memory_run_attempt_timestamps.sql"),
        102 => Some("102_chat_history_partition_helpers.sql"),
        103 => Some("103_shield_documents.sql"),
        104 => Some("104_tune_shield_retrieval.sql"),
        105 => Some("105_reduce_shield_title_overlap.sql"),
        106 => Some("106_add_shield_violence_kill_fallback.sql"),
        107 => Some("107_enable_daily_game_for_all_chats.sql"),
        108 => Some("108_memory_run_error_log_and_retry_cap.sql"),
        109 => Some("109_backfill_memory_run_error_log.sql"),
        110 => Some("110_query_performance_indexes.sql"),
        10 => Some("10_daily_game.sql"),
        11 => Some("11_member_activity.sql"),
        12 => Some("12_enable_daily_game_by_default.sql"),
        13 => Some("13_auto_theme_default.sql"),
        14 => Some("14_greeting_html.sql"),
        15 => Some("15_message_virtual_id.sql"),
        16 => Some("16_job_queue_indexes.sql"),
        17 => Some("17_job_queue_expr_indexes.sql"),
        18 => Some("18_job_queue_unlogged_tablespace.sql"),
        19 => Some("19_drop_job_queue_artifacts.sql"),
        1 => Some("1_ensure_feature_flags_not_null.sql"),
        20 => Some("20_app_settings.sql"),
        21 => Some("21_chat_performance_indexes.sql"),
        22 => Some("22_telegram_files.sql"),
        23 => Some("23_chat_active_users.sql"),
        24 => Some("24_add_search_indices.sql"),
        25 => Some("25_llm_events.sql"),
        26 => Some("26_llm_events_drop_fks.sql"),
        27 => Some("27_user_settings.sql"),
        28 => Some("28_telegram_files_latest_file_id_index.sql"),
        29 => Some("29_whitecircle_checks.sql"),
        2 => Some("2_ensure_settings_fields_not_null.sql"),
        30 => Some("30_whitecircle_checks_external_session.sql"),
        31 => Some("31_user_settings_hide_original_draw_prompt.sql"),
        32 => Some("32_chat_deputies.sql"),
        33 => Some("33_runtime_api_tokens.sql"),
        34 => Some("34_chat_history.sql"),
        35 => Some("35_chat_history_summaries.sql"),
        36 => Some("36_llm_events_inference_metrics.sql"),
        3 => Some("3_add_payments_tables.sql"),
        4 => Some("4_memory.sql"),
        5 => Some("5_add_job_queue.sql"),
        6 => Some("6_remove_retry_fields.sql"),
        7 => Some("7_optimize_job_messages_placeholders.sql"),
        8 => Some("8_replace_memory_with_documents.sql"),
        99 => Some("99_vip_ledger.sql"),
        9 => Some("9_simplify_documents_table.sql"),
        _ => None,
    }
}

/// Connect to Postgres and Redis using the current service-spine settings.
pub async fn connect_service_clients(
    postgres: &PostgresConfig,
    redis: &RedisConfig,
    run_migrations: bool,
) -> Result<ServiceClients, StorageError> {
    let postgres = connect_postgres(postgres).await?;
    if run_migrations {
        run_migrations_on(&postgres).await?;
    }
    let redis = connect_redis(redis).await?;

    Ok(ServiceClients {
        postgres,
        redis,
        migrations_applied: run_migrations,
    })
}

pub async fn connect_postgres(config: &PostgresConfig) -> Result<PgPool, StorageError> {
    PgPoolOptions::new()
        .max_connections(POSTGRES_MAX_CONNECTIONS)
        .min_connections(POSTGRES_MIN_CONNECTIONS)
        .max_lifetime(POSTGRES_MAX_CONNECTION_LIFETIME)
        .connect(&config.startup_dsn())
        .await
        .map_err(StorageError::from)
}

/// Apply all pending SQLx migrations to an already connected Postgres pool.
pub async fn run_migrations_on(pool: &PgPool) -> Result<(), StorageError> {
    bridge_existing_migration_history(pool).await?;
    MIGRATOR.run(pool).await.map_err(StorageError::from)
}

pub async fn bridge_existing_migration_history(pool: &PgPool) -> Result<usize, StorageError> {
    let has_go_table: bool = sqlx::query_scalar(LEGACY_MIGRATION_TABLE_EXISTS_SQL)
        .fetch_one(pool)
        .await?;
    if !has_go_table {
        return Ok(0);
    }

    let legacy_ids: Vec<String> = sqlx::query_scalar(SQL_LIST_LEGACY_MIGRATION_IDS)
        .fetch_all(pool)
        .await?;
    if legacy_ids.is_empty() {
        return Ok(0);
    }

    let bridge_entries = legacy_migration_bridge_entries();
    let known_legacy_ids = bridge_entries
        .iter()
        .map(|entry| entry.legacy_id)
        .collect::<HashSet<_>>();
    let unknown_ids = legacy_ids
        .iter()
        .filter(|id| !known_legacy_ids.contains(id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown_ids.is_empty() {
        return Err(StorageError::UnknownLegacyMigrations { ids: unknown_ids });
    }

    sqlx::query(SQL_ENSURE_SQLX_MIGRATIONS_TABLE)
        .execute(pool)
        .await?;

    let legacy_id_set = legacy_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let mut inserted = 0usize;
    for entry in bridge_entries {
        if !legacy_id_set.contains(entry.legacy_id) {
            continue;
        }
        let result = sqlx::query(SQL_INSERT_BRIDGED_SQLX_MIGRATION)
            .bind(entry.migration.version)
            .bind(&*entry.migration.description)
            .bind(entry.legacy_id)
            .bind(&*entry.migration.checksum)
            .execute(pool)
            .await?;
        inserted += usize::try_from(result.rows_affected()).unwrap_or(usize::MAX);
    }

    Ok(inserted)
}

/// Connect to Redis/Dragonfly and verify the selected DB with `PING`.
pub async fn connect_redis(config: &RedisConfig) -> Result<RedisStore, StorageError> {
    let client = RedisClient::open(
        redis_connection_info(config).map_err(|source| StorageError::RedisConfig { source })?,
    )
    .map_err(|source| StorageError::RedisConfig { source })?;
    let store = RedisStore::from_client(client);
    let mut connection = store
        .connections
        .connection()
        .await
        .map_err(|source| StorageError::Redis { source })?;
    let _: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .map_err(|source| StorageError::Redis { source })?;

    Ok(store)
}

fn redis_connection_info(config: &RedisConfig) -> redis::RedisResult<ConnectionInfo> {
    let redis_settings = if config.password.is_empty() {
        RedisConnectionInfo::default().set_db(config.db)
    } else {
        RedisConnectionInfo::default()
            .set_db(config.db)
            .set_password(&config.password)
    };

    ConnectionAddr::Tcp(config.host.clone(), config.port)
        .into_connection_info()
        .map(|info| info.set_redis_settings(redis_settings))
}

fn message_id_mapping_from_row(row: PgRow) -> Result<MessageIdMapping, sqlx::Error> {
    Ok(MessageIdMapping {
        vmsg_id: row.try_get("vmsg_id")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        real_message_id: row.try_get("real_message_id")?,
    })
}

fn telegram_file_from_row(row: PgRow) -> Result<TelegramFileRecord, StorageError> {
    let extra: String = row.try_get("extra")?;
    let extra = serde_json::from_str(&extra)
        .map_err(|source| StorageError::TelegramFileCodec { source })?;
    Ok(TelegramFileRecord {
        file_unique_id: row.try_get("file_unique_id")?,
        latest_file_id: row.try_get("latest_file_id")?,
        media_kind: row.try_get("media_kind")?,
        mime_type: row.try_get("mime_type")?,
        width: row.try_get("width")?,
        height: row.try_get("height")?,
        file_size: row.try_get("file_size")?,
        first_seen_chat_id: row.try_get("first_seen_chat_id")?,
        first_seen_message_id: row.try_get("first_seen_message_id")?,
        last_seen_chat_id: row.try_get("last_seen_chat_id")?,
        last_seen_message_id: row.try_get("last_seen_message_id")?,
        last_seen_at: row.try_get("last_seen_at")?,
        vision_status: row.try_get("vision_status")?,
        vision_caption: row.try_get("vision_caption")?,
        vision_model: row.try_get("vision_model")?,
        vision_latency_ms: row.try_get("vision_latency_ms")?,
        recognition_requested_at: row.try_get("recognition_requested_at")?,
        recognition_completed_at: row.try_get("recognition_completed_at")?,
        extra,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn pending_op_from_row(row: PgRow) -> Result<PendingOp, sqlx::Error> {
    let payload: String = row.try_get("payload")?;
    Ok(PendingOp {
        id: row.try_get("id")?,
        vmsg_id: row.try_get("vmsg_id")?,
        chat_id: row.try_get("chat_id")?,
        op: row.try_get("op")?,
        payload: payload.into_bytes(),
        attempts: row.try_get("attempts")?,
    })
}

fn runtime_api_token_from_row(row: PgRow) -> Result<RuntimeApiTokenRecord, sqlx::Error> {
    Ok(RuntimeApiTokenRecord {
        id: row.try_get("id")?,
        token_hash: row.try_get("token_hash")?,
        created_at: row.try_get("created_at")?,
    })
}

fn stored_summary_from_row(row: PgRow) -> Result<StoredSummary, StorageError> {
    let scope: String = row.try_get("scope")?;
    let summary_json: String = row.try_get("summary_json")?;
    let summary_json = if summary_json.is_empty() {
        openplotva_history::SummaryContent::default()
    } else {
        serde_json::from_str(&summary_json)
            .map_err(|source| StorageError::HistorySummaryCodec { source })?
    };
    Ok(StoredSummary {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        scope: summary_scope_from_db(&scope),
        requested_by_user_id: row.try_get("requested_by_user_id")?,
        range_start_at: row.try_get("range_start_at")?,
        range_end_at: row.try_get("range_end_at")?,
        first_message_id: row.try_get("first_message_id")?,
        last_message_id: row.try_get("last_message_id")?,
        first_entry_id: row.try_get("first_entry_id")?,
        last_entry_id: row.try_get("last_entry_id")?,
        raw_message_count: row.try_get("raw_message_count")?,
        covered_message_count: row.try_get("covered_message_count")?,
        source_summary_ids: row.try_get("source_summary_ids")?,
        summary_json,
        summary_html: row.try_get("summary_html")?,
        model: row.try_get("model")?,
        prompt_version: row.try_get("prompt_version")?,
        input_hash: row.try_get("input_hash")?,
        prompt_hash: row.try_get("prompt_hash")?,
        input_token_estimate: row.try_get("input_token_estimate")?,
        output_token_estimate: row.try_get("output_token_estimate")?,
        cascade_depth: row.try_get("cascade_depth")?,
        quality_score: row.try_get("quality_score")?,
        quality_notes: row.try_get("quality_notes")?,
        created_at: row.try_get("created_at")?,
    })
}

fn summary_payload_rows_to_bytes(rows: Vec<PgRow>) -> Result<Vec<Vec<u8>>, StorageError> {
    rows.into_iter()
        .map(|row| {
            let payload: String = row.try_get("payload")?;
            Ok(payload.into_bytes())
        })
        .collect()
}

fn memory_card_from_row(row: PgRow) -> Result<openplotva_memory::Card, StorageError> {
    Ok(openplotva_memory::Card {
        id: row.try_get("id")?,
        visibility: row.try_get("visibility")?,
        card_type: row.try_get("card_type")?,
        status: row.try_get("status")?,
        subject: row.try_get("subject")?,
        predicate: row.try_get("predicate")?,
        object: row.try_get("object")?,
        fact_text: row.try_get("fact_text")?,
        confidence: row.try_get("confidence")?,
        salience: row.try_get("salience")?,
        observation_count: row.try_get("observation_count")?,
        origin_chat_id: row.try_get("origin_chat_id")?,
        origin_user_id: row.try_get("origin_user_id")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        user_id: row.try_get("user_id")?,
        valid_from: row.try_get("valid_from")?,
        valid_until: row.try_get("valid_until")?,
        last_observed_at: row.try_get("last_observed_at")?,
        last_used_at: row.try_get("last_used_at")?,
        use_count: row.try_get("use_count")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn memory_run_record_from_row(row: PgRow) -> Result<openplotva_memory::RunRecord, StorageError> {
    let error_log: String = row.try_get("error_log")?;
    Ok(openplotva_memory::RunRecord {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        range_start_at: row.try_get("range_start_at")?,
        range_end_at: row.try_get("range_end_at")?,
        prompt_version: row.try_get("prompt_version")?,
        status: row.try_get("status")?,
        attempts: row.try_get("attempts")?,
        message_count: row.try_get("message_count")?,
        cards_inserted: row.try_get("cards_inserted")?,
        cards_updated: row.try_get("cards_updated")?,
        cards_superseded: row.try_get("cards_superseded")?,
        episodes_inserted: row.try_get("episodes_inserted")?,
        input_tokens: row.try_get("input_token_estimate")?,
        output_tokens: row.try_get("output_token_estimate")?,
        error: row.try_get("error")?,
        errors: memory_run_errors(&error_log),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn memory_run_errors(raw: &str) -> Vec<openplotva_memory::RunErrorEntry> {
    let raw = raw.trim();
    if raw.is_empty() || raw == "null" {
        return Vec::new();
    }
    serde_json::from_str(raw).unwrap_or_default()
}

fn memory_run_status_stat_from_row(
    row: &PgRow,
) -> Result<openplotva_memory::RunStatusStat, StorageError> {
    Ok(openplotva_memory::RunStatusStat {
        status: row.try_get("status")?,
        count: row.try_get("run_count")?,
        message_count: row.try_get("message_count")?,
        avg_input_tokens: row.try_get("avg_input_tokens")?,
        max_input_tokens: row.try_get("max_input_tokens")?,
        avg_output_tokens: row.try_get("avg_output_tokens")?,
        max_output_tokens: row.try_get("max_output_tokens")?,
        avg_duration_ms: row.try_get("avg_duration_ms")?,
        max_duration_ms: row.try_get("max_duration_ms")?,
        latest_updated_at: row.try_get("latest_updated_at")?,
    })
}

fn memory_run_error_stat_from_row(
    row: &PgRow,
) -> Result<openplotva_memory::RunErrorStat, StorageError> {
    Ok(openplotva_memory::RunErrorStat {
        error: row.try_get("error")?,
        count: row.try_get("run_count")?,
        latest_updated_at: row.try_get("latest_updated_at")?,
    })
}

fn memory_episode_from_row(row: PgRow) -> Result<openplotva_memory::Episode, StorageError> {
    Ok(openplotva_memory::Episode {
        id: row.try_get("id")?,
        visibility: row.try_get("visibility")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        range_start_at: row.try_get("range_start_at")?,
        range_end_at: row.try_get("range_end_at")?,
        message_count: row.try_get("message_count")?,
        summary_text: row.try_get("summary_text")?,
        topics: row.try_get("topics")?,
        participants: row.try_get("participants")?,
        created_at: row.try_get("created_at")?,
        ..openplotva_memory::Episode::default()
    })
}

fn shield_scored_document_from_row(
    row: PgRow,
) -> Result<openplotva_shield::ScoredDocument, StorageError> {
    let lexical_score = row.try_get("lexical_score")?;
    Ok(openplotva_shield::ScoredDocument {
        document: shield_document_from_row(row)?,
        lexical_score,
        vector_score: 0.0,
    })
}

fn shield_vector_scored_document_from_row(
    row: PgRow,
) -> Result<openplotva_shield::ScoredDocument, StorageError> {
    let vector_score = row.try_get("vector_score")?;
    Ok(openplotva_shield::ScoredDocument {
        document: shield_document_from_row(row)?,
        lexical_score: 0.0,
        vector_score,
    })
}

fn shield_document_from_row(row: PgRow) -> Result<openplotva_shield::Document, StorageError> {
    Ok(openplotva_shield::Document {
        id: row.try_get("id")?,
        slug: row.try_get("slug")?,
        title: row.try_get("title")?,
        body: row.try_get("body")?,
        category: row.try_get("category")?,
        enabled: row.try_get("enabled")?,
        priority: row.try_get("priority")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn summary_scope_from_db(scope: &str) -> SummaryScope {
    match scope {
        "chat" => SummaryScope::Chat,
        "thread" => SummaryScope::Thread,
        _ => SummaryScope::Unknown,
    }
}

fn user_state_from_row(row: PgRow) -> Result<UserState, sqlx::Error> {
    Ok(UserState {
        id: row.try_get("id")?,
        first_name: row.try_get("first_name")?,
        last_name: row.try_get("last_name")?,
        username: row.try_get("username")?,
        language_code: row.try_get("language_code")?,
        is_premium: row.try_get("is_premium")?,
    })
}

fn chat_state_from_row(row: PgRow) -> Result<ChatState, sqlx::Error> {
    Ok(ChatState::new(
        row.try_get::<i64, _>("id")?,
        row.try_get::<String, _>("type")?,
        row.try_get("title")?,
        row.try_get("username")?,
        row.try_get("first_name")?,
        row.try_get("last_name")?,
        row.try_get("is_forum")?,
    ))
}

fn dialog_memory_chat_meta_from_row(row: PgRow) -> Result<DialogMemoryChatMeta, sqlx::Error> {
    Ok(DialogMemoryChatMeta {
        chat_type: row
            .try_get::<Option<String>, _>("type")?
            .unwrap_or_default()
            .trim()
            .to_owned(),
        username: row
            .try_get::<Option<String>, _>("username")?
            .unwrap_or_default()
            .trim()
            .to_owned(),
        active_usernames: parse_active_usernames_json(
            &row.try_get::<Option<String>, _>("active_usernames")?
                .unwrap_or_default(),
        ),
    })
}

fn parse_active_usernames_json(raw: &str) -> Vec<String> {
    if raw.is_empty() {
        return Vec::new();
    }
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

fn chat_settings_from_row(row: PgRow) -> Result<ChatSettings, sqlx::Error> {
    Ok(ChatSettings {
        chat_id: row.try_get("chat_id")?,
        mood_alignment: row.try_get("mood_alignment")?,
        custom_persona: row.try_get("custom_persona")?,
        reactivity_percentage: row.try_get("reactivity_percentage")?,
        proactivity_percentage: row.try_get("proactivity_percentage")?,
        enable_global_text_reply: row.try_get("enable_global_text_reply")?,
        enable_global_draw_reply: row.try_get("enable_global_draw_reply")?,
        enable_obscenifier: row.try_get("enable_obscenifier")?,
        enable_profanity: row.try_get("enable_profanity")?,
        enable_greet_joiners: row.try_get("enable_greet_joiners")?,
        enable_daily_game: row.try_get("enable_daily_game")?,
        daily_game_theme: row.try_get("daily_game_theme")?,
        greeting_html: row.try_get("greeting_html")?,
    })
}

fn user_settings_from_row(row: PgRow) -> Result<UserSettings, sqlx::Error> {
    Ok(UserSettings {
        user_id: row.try_get("user_id")?,
        disable_random_reactivity: row.try_get("disable_random_reactivity")?,
        updated: row.try_get("updated")?,
        hide_original_draw_prompt: row.try_get("hide_original_draw_prompt")?,
    })
}

fn chat_member_from_row(row: PgRow) -> Result<ChatMemberRecord, sqlx::Error> {
    Ok(ChatMemberRecord {
        chat_id: row.try_get("chat_id")?,
        user_id: row.try_get("user_id")?,
        status: row.try_get("status")?,
        is_anonymous: row.try_get("is_anonymous")?,
        custom_title: row.try_get("custom_title")?,
        can_be_edited: row.try_get("can_be_edited")?,
        can_manage_chat: row.try_get("can_manage_chat")?,
        can_delete_messages: row.try_get("can_delete_messages")?,
        can_manage_video_chats: row.try_get("can_manage_video_chats")?,
        can_restrict_members: row.try_get("can_restrict_members")?,
        can_promote_members: row.try_get("can_promote_members")?,
        can_change_info: row.try_get("can_change_info")?,
        can_invite_users: row.try_get("can_invite_users")?,
        can_post_messages: row.try_get("can_post_messages")?,
        can_edit_messages: row.try_get("can_edit_messages")?,
        can_pin_messages: row.try_get("can_pin_messages")?,
        can_manage_topics: row.try_get("can_manage_topics")?,
        can_send_messages: row.try_get("can_send_messages")?,
        can_send_media_messages: row.try_get("can_send_media_messages")?,
        can_send_polls: row.try_get("can_send_polls")?,
        can_send_other_messages: row.try_get("can_send_other_messages")?,
        can_add_web_page_previews: row.try_get("can_add_web_page_previews")?,
        until_date: row.try_get("until_date")?,
    })
}

fn chat_member_candidate_from_row(row: PgRow) -> Result<ChatMemberCandidate, sqlx::Error> {
    Ok(ChatMemberCandidate {
        id: row.try_get("id")?,
        first_name: row.try_get("first_name")?,
        last_name: row.try_get("last_name")?,
        username: row.try_get("username")?,
        status: row.try_get("status")?,
    })
}

fn chat_game_result_from_row(row: PgRow) -> Result<ChatGameResult, sqlx::Error> {
    Ok(ChatGameResult {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        user_id: row.try_get("user_id")?,
        theme: row.try_get("theme")?,
        won_at: row.try_get("won_at")?,
        won_on_date: row.try_get("won_on_date")?,
    })
}

fn chat_game_top_row_from_row(row: PgRow) -> Result<ChatGameTopRow, sqlx::Error> {
    Ok(ChatGameTopRow {
        user: UserState {
            id: row.try_get("id")?,
            first_name: row.try_get("first_name")?,
            last_name: row.try_get("last_name")?,
            username: row.try_get("username")?,
            language_code: row.try_get("language_code")?,
            is_premium: row.try_get("is_premium")?,
        },
        wins_count: row.try_get("wins_count")?,
        last_win_at: row.try_get("last_win_at")?,
    })
}

fn subscription_from_row(row: PgRow) -> Result<SubscriptionRecord, sqlx::Error> {
    Ok(SubscriptionRecord {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        telegram_payment_charge_id: row.try_get("telegram_payment_charge_id")?,
        provider_payment_charge_id: row.try_get("provider_payment_charge_id")?,
        expires_at: row.try_get("expires_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        canceled_at: row.try_get("canceled_at")?,
        refunded_at: row.try_get("refunded_at")?,
    })
}

fn donation_from_row(row: PgRow) -> Result<DonationRecord, sqlx::Error> {
    Ok(DonationRecord {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        telegram_payment_charge_id: row.try_get("telegram_payment_charge_id")?,
        provider_payment_charge_id: row.try_get("provider_payment_charge_id")?,
        amount_stars: row.try_get("amount_stars")?,
        created_at: row.try_get("created_at")?,
    })
}

fn vip_cache_from_row(row: PgRow) -> Result<VipCacheRecord, sqlx::Error> {
    Ok(VipCacheRecord {
        user_id: row.try_get("user_id")?,
        is_vip: row.try_get("is_vip")?,
        expires_at: row.try_get("expires_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn vip_event_from_row(row: PgRow) -> Result<VipEventRecord, sqlx::Error> {
    Ok(VipEventRecord {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        event_type: row.try_get("event_type")?,
        delta_seconds: row.try_get("delta_seconds")?,
        effective_expires_at: row.try_get("effective_expires_at")?,
        subscription_id: row.try_get("subscription_id")?,
        actor_user_id: row.try_get("actor_user_id")?,
        reason: row.try_get("reason")?,
        created_at: row.try_get("created_at")?,
    })
}

fn vip_summary_from_row(row: PgRow) -> Result<VipSummaryRecord, sqlx::Error> {
    Ok(VipSummaryRecord {
        latest_event_id: row.try_get("latest_event_id")?,
        user_id: row.try_get("user_id")?,
        latest_event_type: row.try_get("latest_event_type")?,
        latest_delta_seconds: row.try_get("latest_delta_seconds")?,
        effective_expires_at: row.try_get("effective_expires_at")?,
        is_active: row.try_get("is_active")?,
        remaining_seconds: row.try_get("remaining_seconds")?,
        latest_subscription_id: row.try_get("latest_subscription_id")?,
        latest_actor_user_id: row.try_get("latest_actor_user_id")?,
        latest_reason: row.try_get("latest_reason")?,
        latest_created_at: row.try_get("latest_created_at")?,
    })
}

fn vip_event_list_from_row(row: PgRow) -> Result<VipEventListRecord, sqlx::Error> {
    Ok(VipEventListRecord {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        event_type: row.try_get("event_type")?,
        delta_seconds: row.try_get("delta_seconds")?,
        effective_expires_at: row.try_get("effective_expires_at")?,
        subscription_id: row.try_get("subscription_id")?,
        actor_user_id: row.try_get("actor_user_id")?,
        actor_username: row.try_get("actor_username")?,
        actor_first_name: row.try_get("actor_first_name")?,
        reason: row.try_get("reason")?,
        created_at: row.try_get("created_at")?,
        telegram_payment_charge_id: row.try_get("telegram_payment_charge_id")?,
        provider_payment_charge_id: row.try_get("provider_payment_charge_id")?,
        subscription_expires_at: row.try_get("subscription_expires_at")?,
        subscription_canceled_at: row.try_get("subscription_canceled_at")?,
        subscription_refunded_at: row.try_get("subscription_refunded_at")?,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        error::Error,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use openplotva_config::{DEFAULT_REDIS_DB, RedisConfig};
    use redis::ConnectionAddr;
    use sqlx::postgres::PgPoolOptions;

    #[test]
    fn redis_store_derived_stores_share_connection_pool() -> Result<(), Box<dyn Error>> {
        let client = redis::Client::open("redis://127.0.0.1/0")?;
        let store = super::RedisStore::from_client(client);

        let rate_limits = store.rate_limit_store();
        let ephemeral = store.ephemeral_message_store();
        let blocked = store.blocked_chat_store();

        assert!(
            rate_limits
                .connections
                .shares_manager_with(&ephemeral.connections)
        );
        assert!(
            ephemeral
                .connections
                .shares_manager_with(&blocked.connections)
        );
        Ok(())
    }

    #[test]
    fn concurrent_index_migrations_are_single_statement_no_tx_files() {
        for migration in super::MIGRATOR
            .iter()
            .filter(|migration| migration.sql.as_str().contains("CONCURRENTLY"))
        {
            assert!(
                migration.no_tx,
                "migration {} creates a concurrent index without no_tx",
                migration.version
            );
            let statement_count = migration
                .sql
                .as_str()
                .lines()
                .filter(|line| {
                    let trimmed = line.trim();
                    !trimmed.is_empty() && !trimmed.starts_with("--")
                })
                .collect::<Vec<_>>()
                .join("\n")
                .matches(';')
                .count();
            assert_eq!(
                statement_count, 1,
                "migration {} must contain exactly one statement because SQLx executes each no_tx file as one query",
                migration.version
            );
        }
    }

    #[test]
    fn runtime_api_token_sql_uses_stable_query_shapes() {
        assert_eq!(
            super::SQL_CREATE_RUNTIME_API_TOKEN,
            "INSERT INTO runtime_api_tokens (id, token_hash) VALUES ($1, $2) RETURNING *"
        );
        assert_eq!(
            super::SQL_LIST_RUNTIME_API_TOKENS_CREATED_SINCE,
            "SELECT * FROM runtime_api_tokens WHERE created_at >= $1 ORDER BY created_at DESC, id ASC"
        );
        assert_eq!(
            super::SQL_DELETE_RUNTIME_API_TOKENS_OLDER_THAN,
            "DELETE FROM runtime_api_tokens WHERE created_at < $1"
        );
    }

    #[test]
    fn memory_card_upsert_params_match_scope_hash_and_defaults() -> Result<(), Box<dyn Error>> {
        let observed = time::OffsetDateTime::from_unix_timestamp(1_770_000_000)?;
        let card = openplotva_memory::CardInput {
            observation_scope: openplotva_memory::ObservationScope {
                chat_id: -100,
                thread_id: 7,
                user_id: 42,
                chat_type: "supergroup".to_owned(),
                username: "plotva".to_owned(),
                kind: openplotva_memory::CARD_KIND_USER.to_owned(),
                ..openplotva_memory::ObservationScope::default()
            },
            subject: " Alice\n ".to_owned(),
            predicate: " likes\t".to_owned(),
            object: " Rust ".to_owned(),
            fact_text: " Alice likes Rust ".to_owned(),
            confidence: 0.0,
            salience: 2.0,
            observed_at: openplotva_memory::memory_zero_time(),
            ..openplotva_memory::CardInput::default()
        };

        let Some(params) = super::memory_card_upsert_params_at(card, observed) else {
            panic!("card should normalize");
        };

        assert_eq!(params.visibility, openplotva_memory::VISIBILITY_PUBLIC_USER);
        assert_eq!(params.card_type, openplotva_memory::CARD_TYPE_PREFERENCE);
        assert_eq!(params.subject, "Alice");
        assert_eq!(params.predicate, "likes");
        assert_eq!(params.object, "Rust");
        assert_eq!(params.fact_text, "Alice likes Rust");
        assert_eq!(params.confidence, 0.5);
        assert_eq!(params.salience, 1.0);
        assert_eq!(params.origin_chat_id, -100);
        assert_eq!(params.origin_thread_id, 7);
        assert_eq!(params.origin_user_id, 42);
        assert_eq!(params.chat_id, 0);
        assert_eq!(params.thread_id, 0);
        assert_eq!(params.user_id, 42);
        assert_eq!(params.last_observed_at, observed);
        assert_eq!(
            params.dedup_hash,
            "300cda06b5cdaaa5aab26c8d7070d4899b312ae876407edadd3f62903f613d71"
        );

        Ok(())
    }

    #[test]
    fn memory_source_batch_params_preserve_go_dedupe_and_empty_shape() -> Result<(), Box<dyn Error>>
    {
        let observed = time::OffsetDateTime::from_unix_timestamp(1_770_000_000)?;
        let card = openplotva_memory::CardInput {
            source_entry_ids: vec![
                " entry-a ".to_owned(),
                String::new(),
                "entry-a".to_owned(),
                "entry-b".to_owned(),
            ],
            source_message_ids: vec![10, 0, 10, 0, 99],
            observed_at: observed,
            confidence: 0.73,
            ..openplotva_memory::CardInput::default()
        };

        let (params, ok) = super::memory_source_batch_params(42, -100, 7, &card);

        assert!(ok);
        assert_eq!(params.card_id, Some(42));
        assert_eq!(params.chat_id, -100);
        assert_eq!(params.thread_id, 7);
        assert_eq!(params.entry_ids, vec!["entry-a", "entry-b", ""]);
        assert_eq!(params.message_ids, vec![10, 0, 99]);
        assert_eq!(params.occurred_at, observed);
        assert_eq!(params.confidence, 0.73);

        let (blank, blank_ok) = super::memory_source_batch_params(
            42,
            -100,
            7,
            &openplotva_memory::CardInput {
                source_entry_ids: vec![" ".to_owned(), String::new()],
                source_message_ids: vec![0, 0],
                ..openplotva_memory::CardInput::default()
            },
        );
        assert!(!blank_ok);
        assert_eq!(blank.entry_ids, Vec::<String>::new());
        assert_eq!(blank.message_ids, Vec::<i32>::new());

        Ok(())
    }

    #[test]
    fn memory_link_batch_params_preserve_go_bulk_upsert_semantics() {
        let Some(params) = super::memory_link_batch_params(&[
            openplotva_memory::LinkInput {
                from_card_id: 0,
                to_card_id: 20,
                relation: "supports".to_owned(),
                confidence: 0.9,
            },
            openplotva_memory::LinkInput {
                from_card_id: 10,
                to_card_id: 10,
                relation: "supports".to_owned(),
                confidence: 0.9,
            },
            openplotva_memory::LinkInput {
                from_card_id: 10,
                to_card_id: 20,
                relation: " supports ".to_owned(),
                confidence: 0.25,
            },
            openplotva_memory::LinkInput {
                from_card_id: 10,
                to_card_id: 20,
                relation: "supports".to_owned(),
                confidence: 0.9,
            },
            openplotva_memory::LinkInput {
                from_card_id: 11,
                to_card_id: 21,
                relation: String::new(),
                confidence: 2.0,
            },
            openplotva_memory::LinkInput {
                from_card_id: 12,
                to_card_id: 22,
                relation: "contradicts".to_owned(),
                confidence: -1.0,
            },
        ]) else {
            panic!("link batch should normalize");
        };

        assert_eq!(params.from_card_ids, vec![10, 11, 12]);
        assert_eq!(params.to_card_ids, vec![20, 21, 22]);
        assert_eq!(params.relations, vec!["supports", "", "contradicts"]);
        assert_eq!(params.confidences, vec![0.9, 1.0, 0.0]);
    }

    #[test]
    fn rank_retrieved_memory_cards_use_best_score_then_updated_at() -> Result<(), Box<dyn Error>> {
        let now = time::OffsetDateTime::from_unix_timestamp(1_770_000_000)?;
        let ranked = super::rank_retrieved_memory_cards(
            2,
            &[
                vec![
                    super::ScoredMemoryCard {
                        card: openplotva_memory::Card {
                            id: 1,
                            updated_at: Some(now - time::Duration::hours(1)),
                            ..openplotva_memory::Card::default()
                        },
                        score: 0.4,
                    },
                    super::ScoredMemoryCard {
                        card: openplotva_memory::Card {
                            id: 2,
                            updated_at: Some(now - time::Duration::hours(2)),
                            ..openplotva_memory::Card::default()
                        },
                        score: 0.8,
                    },
                ],
                vec![
                    super::ScoredMemoryCard {
                        card: openplotva_memory::Card {
                            id: 1,
                            updated_at: Some(now),
                            ..openplotva_memory::Card::default()
                        },
                        score: 0.9,
                    },
                    super::ScoredMemoryCard {
                        card: openplotva_memory::Card {
                            id: 3,
                            updated_at: Some(now + time::Duration::hours(1)),
                            ..openplotva_memory::Card::default()
                        },
                        score: 0.8,
                    },
                ],
            ],
        );

        assert_eq!(
            ranked.iter().map(|card| card.id).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            super::memory_retrieval_limits(&openplotva_memory::RetrievalRequest::default()),
            super::MemoryRetrievalLimits {
                cards: 12,
                episodes: 2
            }
        );

        Ok(())
    }

    #[test]
    fn memory_storage_sql_matches_go_query_contracts() {
        assert!(super::SQL_UPSERT_MEMORY_CARD_LEXICAL.contains("ON CONFLICT (visibility, user_id, chat_id, thread_id, dedup_hash) WHERE status = 'active'"));
        assert!(
            super::SQL_UPSERT_MEMORY_CARD_LEXICAL
                .contains("observation_count = memory_cards.observation_count + 1")
        );
        assert!(
            super::SQL_UPSERT_MEMORY_CARD_WITH_EMBEDDING
                .contains("embedding = COALESCE(EXCLUDED.embedding, memory_cards.embedding)")
        );
        assert!(super::SQL_INSERT_MEMORY_SOURCES.contains("unnest($4::text[])"));
        assert!(super::SQL_INSERT_MEMORY_SOURCES.contains("WHERE NOT EXISTS"));
        assert!(super::SQL_INSERT_MEMORY_EPISODE_LEXICAL.contains("ON CONFLICT (chat_id, thread_id, range_start_at, range_end_at, prompt_version, cursor_after_at, cursor_after_message_id, cursor_after_entry_id)"));
        assert!(
            super::SQL_INSERT_MEMORY_EPISODE_WITH_EMBEDDING
                .contains("embedding = COALESCE(EXCLUDED.embedding, memory_episodes.embedding)")
        );
        assert!(
            super::SQL_UPSERT_MEMORY_LINKS
                .contains("ON CONFLICT (from_card_id, to_card_id, relation)")
        );
        assert!(
            super::SQL_SUPERSEDE_MEMORY_CARD
                .contains("SET status = 'superseded', valid_until = CURRENT_TIMESTAMP")
        );
        assert!(
            super::SQL_SOFT_DELETE_VISIBLE_MEMORY_CARD
                .contains("status <> 'deleted' AND ((visibility = 'chat'")
        );
        assert!(
            super::SQL_SOFT_DELETE_MEMORY_CARD.contains("WHERE id = $2 AND status <> 'deleted'")
        );
        assert!(super::SQL_MARK_EXHAUSTED_MEMORY_RUNS.contains("attempts >= 5"));
        assert!(
            super::SQL_CLAIM_MEMORY_RUN
                .contains("WHERE prompt_version = $1\n          AND status = 'queued'")
        );
        assert!(super::SQL_CLAIM_MEMORY_RUN.contains("FOR UPDATE SKIP LOCKED"));
        assert!(super::SQL_COMPLETE_MEMORY_RUN.contains("cards_superseded = $4"));
        assert!(super::SQL_FAIL_MEMORY_RUN.contains("make_interval"));
        assert!(super::SQL_ENQUEUE_MEMORY_RUN_CONTINUATION.contains("cursor_after_message_id"));
        assert!(super::SQL_RETRY_FAILED_MEMORY_RUNS.contains("WHERE status = 'failed'"));
        assert!(super::SQL_LIST_MEMORY_RUNS.contains("error_log::text AS error_log"));
        assert!(super::SQL_LIST_MEMORY_RUNS.contains("ORDER BY range_start_at DESC, id DESC"));
        assert!(super::SQL_ENSURE_DAILY_MEMORY_RUNS.contains("date_trunc('hour', h.occurred_at)"));
        assert!(super::SQL_ENSURE_DAILY_MEMORY_RUNS.contains("h.bucket_day >="));
        assert!(super::SQL_SKIP_SUPERSEDED_MEMORY_RUNS.contains("prompt_version <> $1"));
        assert!(
            super::SQL_SELECT_MEMORY_RUN_MESSAGES
                .contains("ORDER BY e.occurred_at ASC, e.message_id ASC, e.entry_id ASC")
        );
        assert!(super::SQL_LIST_VISIBLE_MEMORY_CARDS.contains("visibility = 'private_chat'"));
        assert!(super::SQL_RETRIEVE_MEMORY_CARDS_LEXICAL.contains("0.45 * ts_rank_cd"));
        assert!(
            super::SQL_RETRIEVE_MEMORY_CARDS_VECTOR
                .contains("0.50 * (1 - (memory_cards.embedding <=> q.embedding))")
        );
        assert!(super::SQL_RETRIEVE_MEMORY_CARDS_VECTOR.contains("embedding IS NOT NULL"));
        assert!(
            super::SQL_RETRIEVE_MEMORY_CARDS_VECTOR
                .contains("ORDER BY memory_cards.embedding <=> q.embedding")
        );
        assert!(super::SQL_RETRIEVE_MEMORY_EPISODES.contains("websearch_to_tsquery('simple'"));
    }

    #[test]
    fn pgvector_literals_are_storage_local_and_strict() {
        let vector = super::pg_embedding_vector(vec![1.0, -0.5, 3.25]);
        assert_eq!(
            super::pgvector_literal(Some(&vector)).as_deref(),
            Some("[1,-0.5,3.25]")
        );
        assert!(super::pgvector_literal(Some(&super::pg_embedding_vector(Vec::new()))).is_none());
        assert!(
            super::pgvector_literal(Some(&super::pg_embedding_vector(vec![f32::NAN]))).is_none()
        );
        assert!(super::pgvector_literal(None).is_none());
    }

    #[test]
    fn dialog_memory_chat_meta_sql_and_parser_match_go_shape() {
        assert!(super::SQL_GET_DIALOG_MEMORY_CHAT_META.contains("active_usernames::text"));
        assert_eq!(
            super::parse_active_usernames_json(r#"["ada","bob",""]"#),
            vec!["ada".to_owned(), "bob".to_owned(), String::new()]
        );
        assert!(super::parse_active_usernames_json("").is_empty());
        assert!(super::parse_active_usernames_json("{bad json").is_empty());
    }

    #[test]
    fn memory_message_from_history_entry_matches_go_extraction_input_mapping()
    -> Result<(), Box<dyn Error>> {
        let entry = openplotva_history::decode_summary_message_entry_payload(
            br#"{
                "entry_id": " msg:7 ",
                "timestamp": "2026-05-20T10:00:00+02:00",
                "message_id": 7,
                "message_thread_id": 3,
                "text": " fallback text ",
                "original_text": " original text ",
                "from": {"id": 42, "first_name": " Alice ", "last_name": " Wave ", "username": "alice", "is_bot": true},
                "chat": {"id": 100, "type": "supergroup"},
                "forward_origin": {"type": "channel"},
                "via_bot": {"username": "helper_bot"},
                "is_automatic_forward": true,
                "meta": {"sender_type": " user "}
            }"#,
        )?;

        let message = super::memory_message_from_history_entry(&entry);

        assert_eq!(message.entry_id, "msg:7");
        assert_eq!(message.message_id, 7);
        assert_eq!(message.thread_id, 3);
        assert_eq!(message.user_id, 42);
        assert_eq!(message.sender_name, "Alice   Wave");
        assert_eq!(message.sender_username, "alice");
        assert_eq!(message.sender_type, "user");
        assert!(message.sender_is_bot);
        assert!(message.is_forwarded);
        assert!(message.is_automatic_forward);
        assert_eq!(message.forward_origin_type, "channel");
        assert_eq!(message.via_bot_username, "helper_bot");
        assert_eq!(message.text, "original text");
        assert_eq!(
            message.occurred_at,
            time::OffsetDateTime::parse(
                "2026-05-20T08:00:00Z",
                &time::format_description::well_known::Rfc3339
            )?
        );
        Ok(())
    }

    #[test]
    fn shield_storage_sql_matches_go_lexical_query_contract() {
        assert!(super::SQL_CREATE_SHIELD_DOCUMENT.contains("INSERT INTO shield_documents"));
        assert!(
            super::SQL_UPDATE_SHIELD_DOCUMENT
                .contains("embedding = CASE WHEN $7::boolean THEN $8::vector ELSE embedding END")
        );
        assert!(super::SQL_DELETE_SHIELD_DOCUMENT.contains("DELETE FROM shield_documents"));
        assert!(
            super::SQL_LIST_SHIELD_DOCUMENTS
                .contains("ORDER BY priority DESC, updated_at DESC, id DESC")
        );
        assert!(super::SQL_SEARCH_SHIELD_DOCUMENTS_LEXICAL.contains("to_tsvector('russian'"));
        assert!(super::SQL_SEARCH_SHIELD_DOCUMENTS_LEXICAL.contains("quote_literal(term)"));
        assert!(super::SQL_SEARCH_SHIELD_DOCUMENTS_LEXICAL.contains("title_search @@ q.tsq"));
        assert!(
            super::SQL_SEARCH_SHIELD_DOCUMENTS_LEXICAL
                .contains("ORDER BY lexical_score DESC, priority DESC, updated_at DESC")
        );
        assert!(super::SQL_SEARCH_SHIELD_DOCUMENTS_VECTOR.contains(
            "(1 - (shield_documents.embedding <=> q.embedding))::double precision AS vector_score"
        ));
        assert!(super::SQL_SEARCH_SHIELD_DOCUMENTS_VECTOR.contains("embedding IS NOT NULL"));
        assert!(super::SQL_SEARCH_SHIELD_DOCUMENTS_VECTOR.contains(
            "ORDER BY shield_documents.embedding <=> q.embedding, priority DESC, updated_at DESC"
        ));
        assert!(
            super::SQL_GET_SHIELD_DOCUMENTS_WITHOUT_EMBEDDINGS.contains("WHERE embedding IS NULL")
        );
        assert!(super::SQL_UPDATE_SHIELD_DOCUMENT_EMBEDDING.contains("SET embedding = $1::vector"));
    }

    #[test]
    fn telegram_file_storage_sql_matches_go_contract() {
        assert!(
            super::SQL_UPSERT_TELEGRAM_FILE_METADATA
                .contains("ON CONFLICT (file_unique_id) DO UPDATE SET")
        );
        assert!(
            super::SQL_UPSERT_TELEGRAM_FILE_METADATA
                .contains("mime_type = COALESCE(EXCLUDED.mime_type, telegram_files.mime_type)")
        );
        assert!(
            super::SQL_GET_TELEGRAM_FILE_BY_LATEST_FILE_ID
                .contains("ORDER BY last_seen_at DESC LIMIT 1")
        );
        assert!(
            super::SQL_UPDATE_TELEGRAM_FILE_VISION
                .contains("vision_caption = COALESCE($3, vision_caption)")
        );
    }

    #[test]
    fn telegram_file_metadata_helpers_match_go_caption_pending_and_ref_rules() {
        let requested_at =
            time::OffsetDateTime::from_unix_timestamp(1_770_000_000).expect("valid test timestamp");
        let base = super::TelegramFileRecord {
            file_unique_id: " unique ".to_owned(),
            latest_file_id: " latest ".to_owned(),
            media_kind: " photo ".to_owned(),
            mime_type: Some(" image/jpeg ".to_owned()),
            width: Some(320),
            height: Some(240),
            file_size: Some(123),
            first_seen_chat_id: Some(-1),
            first_seen_message_id: Some(10),
            last_seen_chat_id: Some(-2),
            last_seen_message_id: Some(20),
            last_seen_at: requested_at,
            vision_status: super::TELEGRAM_FILE_VISION_STATUS_COMPLETED.to_owned(),
            vision_caption: Some(" caption ".to_owned()),
            vision_model: Some("model".to_owned()),
            vision_latency_ms: Some(12),
            recognition_requested_at: Some(requested_at),
            recognition_completed_at: Some(requested_at + time::Duration::seconds(1)),
            extra: serde_json::json!({}),
            created_at: requested_at,
            updated_at: requested_at,
        };

        assert_eq!(
            super::telegram_file_completed_caption(Some(&base)),
            "caption"
        );
        let mut processing = base.clone();
        processing.vision_status = super::TELEGRAM_FILE_VISION_STATUS_PROCESSING.to_owned();
        processing.vision_caption = Some(" ignored ".to_owned());
        assert!(super::telegram_file_completed_caption(Some(&processing)).is_empty());
        assert!(super::telegram_file_vision_caption_pending_at(
            Some(&processing),
            requested_at + time::Duration::seconds(119)
        ));
        assert!(!super::telegram_file_vision_caption_pending_at(
            Some(&processing),
            requested_at + time::Duration::seconds(120)
        ));

        let r#ref = super::telegram_file_ref_from_record(Some(&base));
        assert_eq!(r#ref.latest_file_id, "latest");
        assert_eq!(r#ref.file_unique_id, "unique");
        assert_eq!(r#ref.media_kind, "photo");
        assert_eq!(r#ref.mime_type, "image/jpeg");
        assert_eq!(r#ref.width, 320);
        assert_eq!(r#ref.height, 240);
        assert_eq!(r#ref.file_size, 123);
        assert_eq!(r#ref.chat_id, -2);
        assert_eq!(r#ref.message_id, 20);
    }

    #[tokio::test]
    async fn live_memory_store_round_trips_lexical_cards_links_and_episodes_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await?;
        let store = super::PostgresMemoryStore::new(pool.clone());
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i64;
        let chat_id = -930_000_000 - (suffix % 1_000_000);
        let user_id = 930_000_000 + (suffix % 1_000_000);
        let observed = time::OffsetDateTime::from_unix_timestamp(1_770_000_000)?;
        let scope = openplotva_memory::ObservationScope {
            chat_id,
            user_id,
            kind: openplotva_memory::CARD_KIND_CHAT.to_owned(),
            ..openplotva_memory::ObservationScope::default()
        };

        let result: Result<(), Box<dyn Error>> = async {
            let cards = vec![
                openplotva_memory::CardInput {
                    observation_scope: scope.clone(),
                    card_type: openplotva_memory::CARD_TYPE_TECHNICAL_FACT.to_owned(),
                    subject: "OpenPlotva".to_owned(),
                    predicate: "ports".to_owned(),
                    object: "memory".to_owned(),
                    fact_text: "OpenPlotva ports Rust memory storage.".to_owned(),
                    confidence: 0.7,
                    salience: 0.8,
                    source_entry_ids: vec!["msg:a".to_owned()],
                    source_message_ids: vec![10],
                    observed_at: observed,
                },
                openplotva_memory::CardInput {
                    observation_scope: scope.clone(),
                    card_type: openplotva_memory::CARD_TYPE_DECISION.to_owned(),
                    subject: "Memory".to_owned(),
                    predicate: "uses".to_owned(),
                    object: "lexical fallback".to_owned(),
                    fact_text: "Memory retrieval keeps lexical fallback behavior.".to_owned(),
                    confidence: 0.6,
                    salience: 0.9,
                    source_entry_ids: vec!["msg:b".to_owned()],
                    source_message_ids: vec![11],
                    observed_at: observed,
                },
            ];
            let (stats, ids) = store.upsert_cards_lexical(&cards).await?;
            assert_eq!(stats.cards_inserted, 2);
            assert_eq!(ids.len(), 2);

            let (updated, updated_ids) = store.upsert_cards_lexical(&cards[..1]).await?;
            assert_eq!(updated.cards_updated, 1);
            assert_eq!(updated_ids[0], ids[0]);

            store
                .insert_links(&[openplotva_memory::LinkInput {
                    from_card_id: ids[0],
                    to_card_id: ids[1],
                    relation: "supports".to_owned(),
                    confidence: 0.85,
                }])
                .await?;

            let (episode_id, episode_inserted) = store
                .insert_episode_lexical(
                    openplotva_memory::Episode {
                        chat_id,
                        range_start_at: observed - time::Duration::hours(1),
                        range_end_at: observed,
                        message_count: 2,
                        summary_text: "Rust memory storage lexical fallback was discussed."
                            .to_owned(),
                        topics: vec!["memory".to_owned()],
                        participants: vec!["OpenPlotva".to_owned()],
                        ..openplotva_memory::Episode::default()
                    },
                    "model",
                    "prompt",
                )
                .await?;
            assert!(episode_id > 0);
            assert!(episode_inserted);

            let retrieval_scope = openplotva_memory::RetrievalScope {
                chat_id,
                user_id,
                ..openplotva_memory::RetrievalScope::default()
            };
            let visible = store.list_visible_cards(&retrieval_scope, 0).await?;
            assert_eq!(visible.len(), 2);
            let retrieved = store
                .retrieve_lexical(&openplotva_memory::RetrievalRequest {
                    scope: retrieval_scope.clone(),
                    query: "Rust memory storage".to_owned(),
                    card_limit: 4,
                    episode_limit: 2,
                })
                .await?;
            assert!(!retrieved.cards.is_empty());
            assert_eq!(retrieved.episodes.len(), 1);
            assert_eq!(retrieved.episodes[0].id, episode_id);

            let listed = store
                .list_cards(&openplotva_memory::CardFilter {
                    chat_id,
                    limit: 10,
                    ..openplotva_memory::CardFilter::default()
                })
                .await?;
            assert_eq!(listed.len(), 2);
            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM memory_episodes WHERE chat_id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM memory_cards WHERE chat_id = $1 OR origin_chat_id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await;
        result
    }

    #[tokio::test]
    async fn live_shield_store_searches_lexical_context_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await?;
        let store = super::PostgresShieldStore::new(pool.clone());
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let slug = format!("openplotva-test-{suffix}");

        let result: Result<(), Box<dyn Error>> = async {
            sqlx::query(
                "INSERT INTO shield_documents (slug, title, body, category, enabled, priority) VALUES ($1, 'OpenPlotva lexical shield safety', 'stay safe', 'test', true, 99)",
            )
            .bind(&slug)
            .execute(&pool)
            .await?;

            let result = store
                .search_lexical(
                    &openplotva_shield::SearchRequest {
                        query: "OpenPlotva safety".to_owned(),
                        include_candidates: true,
                        ..openplotva_shield::SearchRequest::default()
                    },
                    &openplotva_shield::Options {
                        lexical_min_score: 0.0,
                        ..openplotva_shield::Options::default()
                    },
                )
                .await?;

            assert!(result.lexical_only);
            assert_eq!(result.query, "OpenPlotva safety");
            assert!(result.matches.iter().any(|item| item.document.slug == slug));
            assert!(result.context.contains("<shield_context>"));
            assert!(result.debug.is_some());
            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM shield_documents WHERE slug = $1")
            .bind(&slug)
            .execute(&pool)
            .await;
        result
    }

    #[test]
    fn draw_rate_limit_timestamps_codec_preserves_key_and_ttl_contract()
    -> Result<(), Box<dyn Error>> {
        let first = time::OffsetDateTime::from_unix_timestamp(1_770_000_000)?;
        let second = first + time::Duration::minutes(1);
        let value = super::draw_rate_limit_timestamps_redis_value(&[first, second])?;

        assert_eq!(
            super::draw_rate_limit_timestamps_from_redis_value(&value)?,
            vec![first, second]
        );
        assert_eq!(super::DRAW_RATE_LIMIT_KEY_PREFIX, "plotva:rate_limit:");
        assert_eq!(super::DRAW_RATE_LIMIT_TTL, Duration::from_secs(30 * 60));
        Ok(())
    }

    #[tokio::test]
    async fn live_telegram_file_store_round_trips_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await?;
        let store = super::PostgresTelegramFileStore::new(pool.clone());
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let unique_id = format!("openplotva-file-{suffix}");
        let requested_at = time::OffsetDateTime::from_unix_timestamp(1_770_000_001)?;
        let completed_at = time::OffsetDateTime::from_unix_timestamp(1_770_000_003)?;

        let result: Result<(), Box<dyn Error>> = async {
            let inserted = store
                .upsert_metadata(&super::TelegramFileMetadataUpsert {
                    file_unique_id: unique_id.clone(),
                    latest_file_id: "file-old".to_owned(),
                    media_kind: "image".to_owned(),
                    mime_type: Some("image/jpeg".to_owned()),
                    width: Some(320),
                    height: Some(240),
                    file_size: Some(12_345),
                    first_seen_chat_id: Some(-100),
                    first_seen_message_id: Some(10),
                    last_seen_chat_id: Some(-100),
                    last_seen_message_id: Some(10),
                })
                .await?;
            assert_eq!(inserted.file_unique_id, unique_id);
            assert_eq!(
                inserted.vision_status,
                super::TELEGRAM_FILE_VISION_STATUS_PENDING
            );
            assert_eq!(inserted.mime_type.as_deref(), Some("image/jpeg"));

            let updated = store
                .upsert_metadata(&super::TelegramFileMetadataUpsert {
                    file_unique_id: unique_id.clone(),
                    latest_file_id: "file-new".to_owned(),
                    media_kind: "image".to_owned(),
                    last_seen_chat_id: Some(-200),
                    last_seen_message_id: Some(20),
                    ..super::TelegramFileMetadataUpsert::default()
                })
                .await?;
            assert_eq!(updated.latest_file_id, "file-new");
            assert_eq!(updated.mime_type.as_deref(), Some("image/jpeg"));
            assert_eq!(updated.width, Some(320));
            assert_eq!(updated.first_seen_chat_id, Some(-100));
            assert_eq!(updated.last_seen_chat_id, Some(-200));

            let by_unique = store.get_file(&unique_id).await?.expect("inserted row");
            assert_eq!(by_unique.latest_file_id, "file-new");
            let listed = store
                .list_files_by_unique_ids(std::slice::from_ref(&unique_id))
                .await?;
            assert_eq!(listed.len(), 1);
            let by_latest = store
                .get_file_by_latest_file_id("file-new")
                .await?
                .expect("latest file row");
            assert_eq!(by_latest.file_unique_id, unique_id);

            let vision = store
                .update_vision(&super::TelegramFileVisionUpdate {
                    file_unique_id: unique_id.clone(),
                    vision_status: super::TELEGRAM_FILE_VISION_STATUS_COMPLETED.to_owned(),
                    vision_caption: Some("caption".to_owned()),
                    vision_model: Some("vision-model".to_owned()),
                    vision_latency_ms: Some(2000),
                    recognition_requested_at: Some(requested_at),
                    recognition_completed_at: Some(completed_at),
                })
                .await?;
            assert_eq!(
                vision.vision_status,
                super::TELEGRAM_FILE_VISION_STATUS_COMPLETED
            );
            assert_eq!(vision.vision_caption.as_deref(), Some("caption"));
            assert_eq!(vision.vision_model.as_deref(), Some("vision-model"));
            assert_eq!(vision.vision_latency_ms, Some(2000));
            assert_eq!(vision.recognition_requested_at, Some(requested_at));
            assert_eq!(vision.recognition_completed_at, Some(completed_at));
            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM telegram_files WHERE file_unique_id = $1")
            .bind(&unique_id)
            .execute(&pool)
            .await;
        result
    }

    #[test]
    fn redis_connection_info_matches_go_cache_defaults() -> Result<(), redis::RedisError> {
        let config = RedisConfig {
            host: "127.0.0.1".to_owned(),
            port: 6379,
            password: String::new(),
            db: DEFAULT_REDIS_DB,
        };

        let info = super::redis_connection_info(&config)?;

        assert_eq!(
            info.addr(),
            &ConnectionAddr::Tcp("127.0.0.1".to_owned(), 6379)
        );
        assert_eq!(info.redis_settings().db(), 0);
        assert_eq!(info.redis_settings().password(), None);

        Ok(())
    }

    #[test]
    fn rate_limit_storage_keys_and_codec_preserve_go_policy_contract() -> Result<(), Box<dyn Error>>
    {
        let expiry = time::OffsetDateTime::from_unix_timestamp_nanos(1_710_000_000_123_456_789)?;

        assert_eq!(
            super::rate_limited_chat_key(42),
            "plotva:rate_limited_chat:42"
        );
        assert_eq!(
            super::rate_limit_expiry_redis_value(expiry)?,
            br#"{"unix_timestamp_nanos":1710000000123456789}"#
        );
        assert_eq!(
            super::rate_limit_expiry_from_redis_value(
                br#"{"unix_timestamp_nanos":1710000000123456789}"#
            )?,
            expiry
        );

        Ok(())
    }

    #[test]
    fn rate_limit_active_check_uses_go_strict_before_boundary() -> Result<(), Box<dyn Error>> {
        let now = time::OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let future = now + time::Duration::seconds(1);
        let past = now - time::Duration::seconds(1);

        assert!(super::rate_limit_is_active_at(Some(future), now));
        assert!(!super::rate_limit_is_active_at(Some(now), now));
        assert!(!super::rate_limit_is_active_at(Some(past), now));
        assert!(!super::rate_limit_is_active_at(None, now));

        Ok(())
    }

    #[test]
    fn rate_limit_codec_rejects_legacy_gob_wrapped_values() {
        let error =
            super::rate_limit_expiry_from_redis_value(&[0xff, 0xb4, 0x0a, 0x00, 0xff, 0xb0])
                .expect_err("legacy gob values should be rejected after the approved cutover");

        assert!(error.to_string().contains("decode rate limit expiry"));
    }

    #[test]
    fn chat_admin_cache_key_and_codec_use_rust_native_json() -> Result<(), Box<dyn Error>> {
        assert_eq!(super::chat_admins_key(-10042), "chat:-10042:admins");

        let value = super::chat_admin_ids_redis_value(&[42, 43])?;
        assert_eq!(serde_json::from_slice::<Vec<i64>>(&value)?, vec![42, 43]);
        assert_eq!(
            super::chat_admin_ids_from_redis_value(&value)?,
            vec![42, 43]
        );

        let error = super::chat_admin_ids_from_redis_value(&[0xff, 0xb4, 0x0a, 0x00, 0xff, 0xb0])
            .expect_err("legacy gob values should be rejected after the approved cutover");
        assert!(error.to_string().contains("decode chat admin ids"));
        Ok(())
    }

    #[test]
    fn queued_sticker_key_and_ttl_preserve_go_contract() {
        assert_eq!(
            super::queued_sticker_key(-10042, 77),
            "queued_sticker:-10042:77"
        );
        assert_eq!(super::QUEUED_STICKER_TTL, Duration::from_secs(60 * 60));
        assert_eq!(
            super::queued_sticker_message_id_from_redis_value(Some(" 444 ".to_owned())),
            Some(444)
        );
        assert_eq!(
            super::queued_sticker_message_id_from_redis_value(Some("bad-id".to_owned())),
            Some(0)
        );
        assert_eq!(
            super::queued_sticker_message_id_from_redis_value(None),
            None
        );
    }

    #[test]
    fn ephemeral_message_keys_ttl_and_codec_preserve_go_lifecycle_contract()
    -> Result<(), Box<dyn Error>> {
        let expires_at =
            time::OffsetDateTime::from_unix_timestamp_nanos(1_710_000_000_123_456_789)?;
        let message = super::EphemeralMessage {
            chat_id: -10042,
            message_id: 77,
            expires_at,
        };

        assert_eq!(
            super::ephemeral_message_key(-10042, 77),
            "ephemeral_messages:-10042:77"
        );
        assert_eq!(
            super::ephemeral_message_keys(std::slice::from_ref(&message)),
            vec!["ephemeral_messages:-10042:77"]
        );
        assert_eq!(super::EPHEMERAL_CLEANUP_BATCH_SIZE, 10);
        assert_eq!(
            super::ephemeral_redis_ttl(Duration::from_secs(60), Duration::from_secs(15)),
            Duration::from_secs(76)
        );

        let value = super::ephemeral_message_redis_value(&message)?;
        assert_eq!(
            value,
            br#"{"chat_id":-10042,"message_id":77,"expires_at_unix_timestamp_nanos":1710000000123456789}"#
        );
        assert_eq!(super::ephemeral_message_from_redis_value(&value)?, message);

        let error =
            super::ephemeral_message_from_redis_value(&[0xff, 0xb4, 0x0a, 0x00, 0xff, 0xb0])
                .expect_err("legacy gob values should be rejected after the approved cutover");
        assert!(error.to_string().contains("decode ephemeral message"));
        Ok(())
    }

    #[test]
    fn last_generation_key_ttl_and_codec_use_go_shape_with_rust_native_json()
    -> Result<(), Box<dyn Error>> {
        let generation = super::LastGenerationRecord {
            chat_id: -10042,
            user_id: 77,
            message_ids: vec![101, 102],
            caption: "caption".to_owned(),
            created_at: 1_710_000_000,
        };

        assert_eq!(super::last_generation_key(-10042, 77), "last_gen:-10042:77");
        assert_eq!(
            super::LAST_GENERATION_TTL,
            Duration::from_secs(24 * 60 * 60)
        );

        let value = super::last_generation_redis_value(&generation)?;
        assert_eq!(
            value,
            br#"{"chat_id":-10042,"user_id":77,"message_ids":[101,102],"caption":"caption","created_at":1710000000}"#
        );
        assert_eq!(super::last_generation_from_redis_value(&value)?, generation);

        let without_caption = super::LastGenerationRecord {
            caption: String::new(),
            ..generation
        };
        assert_eq!(
            super::last_generation_redis_value(&without_caption)?,
            br#"{"chat_id":-10042,"user_id":77,"message_ids":[101,102],"created_at":1710000000}"#
        );

        let error = super::last_generation_from_redis_value(&[0xff, 0xb4, 0x0a, 0x00, 0xff, 0xb0])
            .expect_err("legacy gob values should be rejected after the approved cutover");
        assert!(error.to_string().contains("decode last generation"));
        Ok(())
    }

    #[test]
    fn translation_cache_key_ttl_and_codec_use_go_key_with_rust_native_json()
    -> Result<(), Box<dyn Error>> {
        assert_eq!(
            super::translation_cache_hash_key("hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            super::translation_cache_key("ru", "hello"),
            "plotva:t8:ru:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            super::TRANSLATION_CACHE_TTL,
            Duration::from_secs(24 * 60 * 60)
        );

        let value = super::translation_cache_redis_value("привет")?;
        assert_eq!(value, "\"привет\"".as_bytes());
        assert_eq!(super::translation_cache_from_redis_value(&value)?, "привет");

        let error =
            super::translation_cache_from_redis_value(&[0xff, 0xb4, 0x0a, 0x00, 0xff, 0xb0])
                .expect_err("legacy gob values should be rejected after the approved cutover");
        assert!(error.to_string().contains("decode translation cache"));
        Ok(())
    }

    #[test]
    fn blocked_chat_keys_ttl_and_codec_use_rust_native_json() -> Result<(), Box<dyn Error>> {
        let unblock_at =
            time::OffsetDateTime::from_unix_timestamp_nanos(1_710_000_000_123_456_789)?;

        assert_eq!(
            super::blocked_chat_key(-10042),
            "plotva:blocked_chat:-10042"
        );
        assert_eq!(super::BLOCKED_CHAT_TTL, Duration::from_secs(10 * 60));
        assert_eq!(
            super::blocked_chat_redis_value(unblock_at)?,
            br#"{"unblock_at_unix_timestamp_nanos":1710000000123456789}"#
        );
        assert_eq!(
            super::blocked_chat_from_redis_value(
                br#"{"unblock_at_unix_timestamp_nanos":1710000000123456789}"#
            )?,
            unblock_at
        );
        assert!(super::blocked_chat_is_active_at(
            Some(unblock_at),
            unblock_at - time::Duration::nanoseconds(1)
        ));
        assert!(!super::blocked_chat_is_active_at(
            Some(unblock_at),
            unblock_at
        ));

        let error = super::blocked_chat_from_redis_value(&[0xff, 0xb4, 0x0a, 0x00, 0xff, 0xb0])
            .expect_err("legacy gob values should be rejected after the approved cutover");
        assert!(error.to_string().contains("decode blocked chat"));
        Ok(())
    }

    #[test]
    fn join_greeting_keys_and_ttls_match_go() {
        assert_eq!(
            super::join_greeting_users_key(-10042),
            "join_greet:users:-10042"
        );
        assert_eq!(
            super::join_greeting_message_key(-10042),
            "join_greet:msg:-10042"
        );
        assert_eq!(
            super::join_greeting_debounce_key(-10042),
            "join_greet:debounce:-10042"
        );
        assert_eq!(super::JOIN_GREETING_USERS_TTL, Duration::from_secs(10 * 60));
        assert_eq!(super::JOIN_GREETING_DEBOUNCE_TTL, Duration::from_secs(30));
        assert_eq!(
            super::JOIN_GREETING_MESSAGE_TTL,
            Duration::from_secs(10 * 60)
        );
    }

    #[test]
    fn expired_ephemeral_messages_use_go_strict_after_boundary() -> Result<(), Box<dyn Error>> {
        let now = time::OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let messages = vec![
            super::EphemeralMessage {
                chat_id: 1,
                message_id: 10,
                expires_at: now - time::Duration::seconds(1),
            },
            super::EphemeralMessage {
                chat_id: 2,
                message_id: 20,
                expires_at: now,
            },
            super::EphemeralMessage {
                chat_id: 3,
                message_id: 30,
                expires_at: now + time::Duration::seconds(1),
            },
        ];

        assert_eq!(
            super::expired_ephemeral_messages_at(&messages, now),
            vec![messages[0].clone()]
        );
        Ok(())
    }

    #[test]
    fn history_tool_call_entries_expand_like_go_service() -> Result<(), Box<dyn Error>> {
        let base = serde_json::json!({
            "entry_id": "msg:77",
            "role": "user",
            "kind": "text",
            "timestamp": "2026-05-20T00:00:00Z",
            "message_id": 77,
            "message_thread_id": 9,
            "date": 1_768_867_200,
            "chat": {"id": 42, "type": "private"},
            "text": "draw cat",
            "meta": {}
        });
        let entries = super::history_tool_call_entries_from_base_payload(
            42,
            77,
            base.to_string(),
            &[
                openplotva_core::ToolCall {
                    name: " draw_image ".to_owned(),
                    r#ref: " req-1 ".to_owned(),
                    input: Some(serde_json::json!({"prompt":"cat"})),
                    output: Some(serde_json::json!({"status":"queued"})),
                    ..openplotva_core::ToolCall::default()
                },
                openplotva_core::ToolCall {
                    name: "final_response".to_owned(),
                    ..openplotva_core::ToolCall::default()
                },
            ],
        )?;

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry_id, "tool_request:77:draw_image:req-1");
        assert_eq!(entries[0].kind, "tool_request");
        assert_eq!(entries[0].role, "model");
        assert_eq!(entries[0].thread_id, 9);
        assert_eq!(entries[0].sender_id, 0);
        assert_eq!(entries[1].entry_id, "tool_response:77:draw_image:req-1");
        assert!(entries[1].occurred_at > entries[0].occurred_at);

        let request: serde_json::Value = serde_json::from_slice(&entries[0].payload)?;
        let response: serde_json::Value = serde_json::from_slice(&entries[1].payload)?;
        assert_eq!(request["chat"]["id"], 42);
        assert_eq!(request["message_thread_id"], 9);
        assert_eq!(request["tool_call"]["name"], " draw_image ");
        assert_eq!(request["tool_call"]["ref"], " req-1 ");
        assert_eq!(request["tool_call"]["input"]["prompt"], "cat");
        assert!(request["tool_call"].get("output").is_none());
        assert_eq!(response["tool_call"]["output"]["status"], "queued");
        assert_eq!(request["timestamp"], "2026-05-20T00:00:00.001Z");
        assert_eq!(response["timestamp"], "2026-05-20T00:00:00.002Z");
        assert_eq!(request["tool_call"]["at"], request["timestamp"]);
        assert_eq!(response["tool_call"]["at"], response["timestamp"]);
        Ok(())
    }

    #[test]
    fn history_text_payload_with_message_update_matches_go_normalization()
    -> Result<(), Box<dyn Error>> {
        let payload = serde_json::json!({
            "entry_id": "msg:77",
            "role": "user",
            "kind": "text",
            "timestamp": "2026-05-20T00:00:00Z",
            "message_id": 77,
            "date": 1_710_000_000,
            "chat": {"id": 42, "type": "private"},
            "text": "old text",
            "original_text": "old original",
            "meta": {}
        });
        let meta = openplotva_core::ChatMessageMeta {
            sender_id: 99,
            attachments: vec![
                openplotva_core::ChatAttachment {
                    kind: "image".to_owned(),
                    source: "message".to_owned(),
                    caption: " edited text ".to_owned(),
                    content: "edited text".to_owned(),
                    ..openplotva_core::ChatAttachment::default()
                },
                openplotva_core::ChatAttachment {
                    kind: "image".to_owned(),
                    source: "upload".to_owned(),
                    caption: " keep ".to_owned(),
                    content: "edited text".to_owned(),
                    ..openplotva_core::ChatAttachment::default()
                },
            ],
            ..openplotva_core::ChatMessageMeta::default()
        };

        let updated = super::history_text_payload_with_message_update(
            payload.to_string(),
            " edited text ",
            " edited text ",
            meta,
        )?;
        let updated: serde_json::Value = serde_json::from_str(&updated)?;

        assert_eq!(updated["text"], "edited text");
        assert!(updated.get("original_text").is_none());
        assert_eq!(updated["meta"]["sender_id"], 99);
        assert_eq!(updated["meta"]["attachments"][0]["source"], "message");
        assert!(updated["meta"]["attachments"][0].get("caption").is_none());
        assert!(updated["meta"]["attachments"][0].get("content").is_none());
        assert_eq!(updated["meta"]["attachments"][1]["source"], "upload");
        assert_eq!(updated["meta"]["attachments"][1]["caption"], " keep ");
        assert_eq!(updated["meta"]["attachments"][1]["content"], "edited text");
        Ok(())
    }

    #[test]
    fn history_text_payload_with_vision_descriptions_matches_go_meta_updates()
    -> Result<(), Box<dyn Error>> {
        let payload = serde_json::json!({
            "entry_id": "msg:77",
            "role": "user",
            "kind": "text",
            "timestamp": "2026-05-20T00:00:00Z",
            "text": "photo",
            "meta": {
                "attachments": [
                    {
                        "kind": "",
                        "source": "",
                        "content": "old",
                        "file_unique_id": "file-1"
                    },
                    {
                        "kind": "image",
                        "source": "quoted",
                        "file_unique_id": "file-2",
                        "caption": "keep"
                    }
                ]
            }
        });

        let updated = super::history_text_payload_with_vision_descriptions(
            payload.to_string(),
            &[
                super::VisionDescriptionUpdate {
                    file_unique_id: " file-1 ".to_owned(),
                    caption: " cat ".to_owned(),
                },
                super::VisionDescriptionUpdate {
                    file_unique_id: "file-2".to_owned(),
                    caption: "dog".to_owned(),
                },
                super::VisionDescriptionUpdate {
                    file_unique_id: "file-1".to_owned(),
                    caption: "duplicate ignored".to_owned(),
                },
            ],
        )?
        .expect("payload should change");
        let updated: serde_json::Value = serde_json::from_str(&updated)?;

        assert_eq!(
            updated["meta"]["vision_description"],
            "image_1: cat\nimage_2: dog"
        );
        assert_eq!(updated["meta"]["type"], "image");
        assert_eq!(updated["meta"]["attachments"][0]["kind"], "image");
        assert_eq!(updated["meta"]["attachments"][0]["source"], "message");
        assert_eq!(updated["meta"]["attachments"][0]["caption"], "cat");
        assert!(updated["meta"]["attachments"][0].get("content").is_none());
        assert_eq!(updated["meta"]["attachments"][1]["source"], "quoted");
        assert_eq!(
            updated["meta"]["attachments"][2]["file_unique_id"],
            "file-2"
        );
        assert_eq!(updated["meta"]["attachments"][2]["source"], "message");
        assert_eq!(updated["meta"]["attachments"][2]["caption"], "dog");

        assert!(
            super::history_text_payload_with_vision_descriptions(
                updated.to_string(),
                &[
                    super::VisionDescriptionUpdate {
                        file_unique_id: "file-1".to_owned(),
                        caption: "cat".to_owned(),
                    },
                    super::VisionDescriptionUpdate {
                        file_unique_id: "file-2".to_owned(),
                        caption: "dog".to_owned(),
                    },
                ],
            )?
            .is_none()
        );
        Ok(())
    }

    #[test]
    fn virtual_message_sql_matches_go_query_contracts() {
        let _mapping = openplotva_core::MessageIdMapping::resolved("v1", 42, 77);

        assert_eq!(
            super::SQL_INSERT_VIRTUAL_MESSAGE,
            "INSERT INTO message_id_map (vmsg_id, chat_id, thread_id) VALUES ($1, $2, $3)"
        );
        assert_eq!(
            super::SQL_RESOLVE_VIRTUAL_MESSAGE,
            "UPDATE message_id_map SET real_message_id = $1, resolved_at = COALESCE($2, NOW()) WHERE vmsg_id = $3"
        );
        assert_eq!(
            super::SQL_GET_MAPPING_BY_VIRTUAL,
            "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE vmsg_id = $1"
        );
        assert_eq!(
            super::SQL_LIST_MAPPINGS_BY_VIRTUAL_IDS,
            "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE vmsg_id = ANY($1::text[])"
        );
        assert_eq!(
            super::SQL_GET_MAPPING_BY_REAL,
            "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE chat_id = $1 AND real_message_id = $2"
        );
        assert_eq!(
            super::SQL_DELETE_MAPPING_BY_VIRTUAL,
            "DELETE FROM message_id_map WHERE vmsg_id = $1"
        );
    }

    #[test]
    fn pending_message_op_sql_matches_go_query_contracts() {
        let _op = openplotva_core::PendingOp::new(1, "v1", 42, "edit");

        assert_eq!(
            super::SQL_ENQUEUE_MESSAGE_OP,
            "INSERT INTO message_ops_queue (vmsg_id, chat_id, op, payload) VALUES ($1, $2, $3, $4::jsonb) RETURNING id"
        );
        assert_eq!(
            super::SQL_LIST_PENDING_OPS,
            "SELECT id, vmsg_id, chat_id, op, COALESCE(payload::text, '') AS payload, attempts FROM message_ops_queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT $1"
        );
        assert_eq!(
            super::SQL_MARK_OP_DONE,
            "UPDATE message_ops_queue SET status = 'done', executed_at = NOW() WHERE id = $1"
        );
        assert_eq!(
            super::SQL_MARK_OP_FAILED,
            "UPDATE message_ops_queue SET attempts = attempts + 1, last_error = $2 WHERE id = $1"
        );
    }

    #[test]
    fn chat_member_sql_matches_go_query_contracts() {
        assert_eq!(
            super::SQL_GET_CHAT_MEMBER,
            "SELECT * FROM chat_members WHERE chat_id = $1 AND user_id = $2"
        );
        assert_eq!(
            super::SQL_LIST_CHAT_MEMBERS,
            "SELECT * FROM chat_members WHERE chat_id = $1"
        );
        assert_eq!(
            super::SQL_DELETE_CHAT_MEMBER,
            "DELETE FROM chat_members WHERE chat_id = $1 AND user_id = $2"
        );
        assert_eq!(
            super::SQL_UPSERT_CHAT_MEMBER,
            "INSERT INTO chat_members (chat_id, user_id, status, is_anonymous, custom_title, can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats, can_restrict_members, can_promote_members, can_change_info, can_invite_users, can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics, can_send_messages, can_send_media_messages, can_send_polls, can_send_other_messages, can_add_web_page_previews, until_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23) ON CONFLICT (chat_id, user_id) DO UPDATE SET status = COALESCE(EXCLUDED.status, chat_members.status), is_anonymous = COALESCE(EXCLUDED.is_anonymous, chat_members.is_anonymous), custom_title = COALESCE(EXCLUDED.custom_title, chat_members.custom_title), can_be_edited = COALESCE(EXCLUDED.can_be_edited, chat_members.can_be_edited), can_manage_chat = COALESCE(EXCLUDED.can_manage_chat, chat_members.can_manage_chat), can_delete_messages = COALESCE(EXCLUDED.can_delete_messages, chat_members.can_delete_messages), can_manage_video_chats = COALESCE(EXCLUDED.can_manage_video_chats, chat_members.can_manage_video_chats), can_restrict_members = COALESCE(EXCLUDED.can_restrict_members, chat_members.can_restrict_members), can_promote_members = COALESCE(EXCLUDED.can_promote_members, chat_members.can_promote_members), can_change_info = COALESCE(EXCLUDED.can_change_info, chat_members.can_change_info), can_invite_users = COALESCE(EXCLUDED.can_invite_users, chat_members.can_invite_users), can_post_messages = COALESCE(EXCLUDED.can_post_messages, chat_members.can_post_messages), can_edit_messages = COALESCE(EXCLUDED.can_edit_messages, chat_members.can_edit_messages), can_pin_messages = COALESCE(EXCLUDED.can_pin_messages, chat_members.can_pin_messages), can_manage_topics = COALESCE(EXCLUDED.can_manage_topics, chat_members.can_manage_topics), can_send_messages = COALESCE(EXCLUDED.can_send_messages, chat_members.can_send_messages), can_send_media_messages = COALESCE(EXCLUDED.can_send_media_messages, chat_members.can_send_media_messages), can_send_polls = COALESCE(EXCLUDED.can_send_polls, chat_members.can_send_polls), can_send_other_messages = COALESCE(EXCLUDED.can_send_other_messages, chat_members.can_send_other_messages), can_add_web_page_previews = COALESCE(EXCLUDED.can_add_web_page_previews, chat_members.can_add_web_page_previews), until_date = COALESCE(EXCLUDED.until_date, chat_members.until_date), updated_at = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_UPDATE_MEMBER_LAST_MESSAGE,
            "UPDATE chat_members SET last_message_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $1 AND user_id = $2"
        );
        assert_eq!(
            super::SQL_UPDATE_MEMBER_LAST_MESSAGES,
            "WITH input AS (SELECT * FROM unnest($1::bigint[], $2::bigint[]) AS input(chat_id, user_id)) UPDATE chat_members AS member SET last_message_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP FROM input WHERE member.chat_id = input.chat_id AND member.user_id = input.user_id"
        );
        assert_eq!(
            super::SQL_UPSERT_CHAT_ACTIVE_USER,
            "INSERT INTO chat_active_users (chat_id, user_id, last_active_at) VALUES ($1, $2, CURRENT_TIMESTAMP) ON CONFLICT (chat_id, user_id) DO UPDATE SET last_active_at = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_UPSERT_CHAT_ACTIVE_USERS,
            "INSERT INTO chat_active_users (chat_id, user_id, last_active_at) SELECT input.chat_id, input.user_id, CURRENT_TIMESTAMP FROM unnest($1::bigint[], $2::bigint[]) AS input(chat_id, user_id) ON CONFLICT (chat_id, user_id) DO UPDATE SET last_active_at = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_LIST_ACTIVE_PARTICIPANTS,
            "SELECT user_id FROM chat_members WHERE chat_id = $1 AND status IN ('administrator', 'member', 'creator') AND last_message_at IS NOT NULL AND last_message_at >= (CURRENT_TIMESTAMP - INTERVAL '24 hours') ORDER BY last_message_at DESC LIMIT $2"
        );
        assert_eq!(
            super::SQL_LIST_ACTIVE_PARTICIPANTS_FROM_TABLE,
            "SELECT user_id FROM chat_active_users WHERE chat_id = $1 AND last_active_at >= (CURRENT_TIMESTAMP - INTERVAL '24 hours') ORDER BY last_active_at DESC LIMIT $2"
        );
        assert_eq!(
            super::SQL_GET_CHAT_DISCOVERED,
            "SELECT discovered FROM chats WHERE id = $1"
        );
    }

    #[test]
    fn chat_game_sql_matches_go_query_contracts() {
        let _result_type = std::mem::size_of::<super::ChatGameResult>();
        let _top_type = std::mem::size_of::<super::ChatGameTopRow>();

        assert_eq!(
            super::SQL_RECORD_CHAT_DAILY_WINNER,
            "INSERT INTO chat_game_results (chat_id, user_id, theme) VALUES ($1, $2, $3) RETURNING id, chat_id, user_id, theme, won_at, won_on_date"
        );
        assert_eq!(
            super::SQL_GET_TODAY_CHAT_WINNER,
            "SELECT id, chat_id, user_id, theme, won_at, won_on_date FROM chat_game_results WHERE chat_id = $1 AND won_at::date = CURRENT_DATE ORDER BY won_at DESC LIMIT 1"
        );
        assert_eq!(
            super::SQL_INCREMENT_CHAT_GAME_WIN,
            "INSERT INTO chat_game_stats (chat_id, user_id, wins_count, last_win_at) VALUES ($1, $2, 1, CURRENT_TIMESTAMP) ON CONFLICT (chat_id, user_id) DO UPDATE SET wins_count = chat_game_stats.wins_count + 1, last_win_at = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_GET_YEARLY_TOP,
            "SELECT u.id, u.first_name, u.last_name, u.username, u.language_code, u.is_premium, COUNT(*)::int AS wins_count, MAX(r.won_at) AS last_win_at FROM chat_game_results r JOIN users u ON u.id = r.user_id WHERE r.chat_id = $1 AND r.won_at >= date_trunc('year', CURRENT_DATE) GROUP BY u.id, u.first_name, u.last_name, u.username, u.language_code, u.is_premium ORDER BY wins_count DESC, last_win_at DESC LIMIT $2"
        );
    }

    #[test]
    fn stored_chat_member_permission_matches_go_group_settings_rule() {
        let creator = super::ChatMemberRecord {
            chat_id: -10042,
            user_id: 42,
            status: super::CHAT_MEMBER_STATUS_CREATOR.to_owned(),
            ..super::ChatMemberRecord::default()
        };
        let promoting_admin = super::ChatMemberRecord {
            chat_id: -10042,
            user_id: 43,
            status: super::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
            can_promote_members: Some(true),
            ..super::ChatMemberRecord::default()
        };
        let non_promoting_admin = super::ChatMemberRecord {
            chat_id: -10042,
            user_id: 44,
            status: super::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
            can_promote_members: Some(false),
            ..super::ChatMemberRecord::default()
        };

        assert!(super::stored_member_can_open_group_settings(Some(&creator)));
        assert!(super::stored_member_can_open_group_settings(Some(
            &promoting_admin
        )));
        assert!(!super::stored_member_can_open_group_settings(Some(
            &non_promoting_admin
        )));
        assert!(!super::stored_member_can_open_group_settings(None));
        assert!(super::is_active_chat_member_status(
            super::CHAT_MEMBER_STATUS_CREATOR
        ));
        assert!(super::is_active_chat_member_status(
            super::CHAT_MEMBER_STATUS_ADMINISTRATOR
        ));
        assert!(super::is_active_chat_member_status(
            super::CHAT_MEMBER_STATUS_MEMBER
        ));
        assert!(!super::is_active_chat_member_status(
            super::CHAT_MEMBER_STATUS_LEFT
        ));
    }

    #[test]
    fn stored_admin_chat_member_maps_go_admin_permissions() {
        let admin = super::ChatMemberRecord {
            chat_id: -10042,
            user_id: 42,
            status: super::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
            is_anonymous: Some(true),
            custom_title: Some("Boss".to_owned()),
            can_delete_messages: Some(true),
            can_manage_video_chats: Some(true),
            can_restrict_members: Some(true),
            can_promote_members: Some(true),
            can_change_info: Some(true),
            can_invite_users: Some(true),
            can_post_messages: Some(true),
            can_edit_messages: Some(true),
            can_pin_messages: Some(true),
            ..super::ChatMemberRecord::default()
        };
        let regular_member = super::ChatMemberRecord {
            chat_id: -10042,
            user_id: 43,
            status: super::CHAT_MEMBER_STATUS_MEMBER.to_owned(),
            ..super::ChatMemberRecord::default()
        };

        let stored_admin =
            super::stored_admin_chat_member(&admin).expect("admin should map from stored row");

        assert_eq!(stored_admin.user_id, 42);
        assert_eq!(stored_admin.status, super::CHAT_MEMBER_STATUS_ADMINISTRATOR);
        assert_eq!(stored_admin.is_anonymous, Some(true));
        assert_eq!(stored_admin.custom_title.as_deref(), Some("Boss"));
        assert_eq!(stored_admin.can_delete_messages, Some(true));
        assert_eq!(stored_admin.can_manage_video_chats, Some(true));
        assert_eq!(stored_admin.can_promote_members, Some(true));
        assert!(super::stored_admin_chat_member(&regular_member).is_none());
    }

    #[test]
    fn history_edit_delete_storage_contract_matches_go_side_effects() -> Result<(), Box<dyn Error>>
    {
        let updated_payload = super::history_text_payload_with_text(
            br#"{"entry_id":"msg:77","message_id":77,"text":"old","meta":{}}"#,
            "new text",
        )?;
        let empty_text_payload = super::history_text_payload_with_text(
            br#"{"entry_id":"msg:77","message_id":77,"text":"old","meta":{}}"#,
            "",
        )?;

        assert_eq!(
            super::SQL_SELECT_TEXT_HISTORY_ENTRY,
            "SELECT bucket_day, entry_id, payload::text AS payload FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2 AND kind = 'text' ORDER BY occurred_at DESC LIMIT 1"
        );
        assert_eq!(
            super::SQL_UPDATE_HISTORY_ENTRY_PAYLOAD,
            "UPDATE chat_history_entries SET payload = $4::jsonb, updated_at = CURRENT_TIMESTAMP WHERE bucket_day = $1 AND chat_id = $2 AND entry_id = $3"
        );
        assert_eq!(
            super::SQL_DELETE_HISTORY_MESSAGE_ENTRIES,
            "DELETE FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2"
        );
        assert_eq!(
            super::SQL_DELETE_HISTORY_TOOL_ENTRIES,
            "DELETE FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2 AND kind <> 'text'"
        );
        assert_eq!(
            super::SQL_UPSERT_CHAT_HISTORY_RESET,
            "INSERT INTO chat_history_resets (chat_id, thread_id, reset_at) VALUES ($1, $2, $3) ON CONFLICT (chat_id, thread_id) DO UPDATE SET reset_at = GREATEST(chat_history_resets.reset_at, EXCLUDED.reset_at)"
        );
        assert_eq!(
            super::SQL_GET_CHAT_HISTORY_RESET_AT,
            "SELECT reset_at FROM chat_history_resets WHERE chat_id = $1 AND thread_id = $2"
        );
        assert_eq!(
            super::SQL_SELECT_CHAT_SUMMARY_ENTRY_PAYLOADS,
            "SELECT payload::text AS payload FROM chat_history_entries WHERE chat_id = $1 AND occurred_at > $2 AND occurred_at <= $3 AND kind = 'text' ORDER BY occurred_at ASC, message_id ASC, entry_id ASC"
        );
        assert_eq!(
            super::SQL_SELECT_THREAD_SUMMARY_ENTRY_PAYLOADS,
            "SELECT payload::text AS payload FROM chat_history_entries WHERE chat_id = $1 AND thread_id = $2 AND occurred_at > $3 AND occurred_at <= $4 AND kind = 'text' ORDER BY occurred_at ASC, message_id ASC, entry_id ASC"
        );
        assert_eq!(
            super::history_cache_key(42),
            "plotva:chat_history_cache:v2:42"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&updated_payload)?["text"],
            "new text"
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(&empty_text_payload)?
                .get("text")
                .is_none(),
            "Empty edit payloads omit the JSON text field"
        );

        Ok(())
    }

    #[test]
    fn history_upsert_storage_contract_matches_go_service() {
        assert_eq!(
            super::SQL_UPSERT_HISTORY_ENTRY,
            "INSERT INTO chat_history_entries (bucket_day, chat_id, thread_id, message_id, entry_id, kind, role, occurred_at, sender_id, payload) VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb) ON CONFLICT (bucket_day, chat_id, entry_id) DO UPDATE SET thread_id = EXCLUDED.thread_id, message_id = EXCLUDED.message_id, kind = EXCLUDED.kind, role = EXCLUDED.role, occurred_at = EXCLUDED.occurred_at, sender_id = EXCLUDED.sender_id, payload = EXCLUDED.payload, updated_at = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_ENSURE_CHAT_HISTORY_PARTITION,
            "SELECT ensure_chat_history_partition($1::date)"
        );
    }

    #[test]
    fn history_summary_storage_sql_matches_go_query_contracts() {
        assert_eq!(
            super::SQL_SELECT_REUSABLE_HISTORY_SUMMARIES,
            "SELECT id, chat_id, thread_id, scope, requested_by_user_id, range_start_at, range_end_at, first_message_id, last_message_id, first_entry_id, last_entry_id, raw_message_count, covered_message_count, source_summary_ids, summary_json::text AS summary_json, summary_html, model, prompt_version, input_hash, prompt_hash, input_token_estimate, output_token_estimate, cascade_depth, quality_score, quality_notes, created_at FROM chat_history_summaries WHERE chat_id = $1 AND thread_id = $2 AND scope = $3 AND range_start_at >= $4 AND range_end_at <= $5 AND created_at > $6 ORDER BY range_start_at ASC, range_end_at ASC, created_at DESC"
        );
        assert_eq!(
            super::SQL_INSERT_HISTORY_SUMMARY,
            "INSERT INTO chat_history_summaries (chat_id, thread_id, scope, requested_by_user_id, range_start_at, range_end_at, first_message_id, last_message_id, first_entry_id, last_entry_id, raw_message_count, covered_message_count, source_summary_ids, summary_json, summary_html, model, prompt_version, input_hash, prompt_hash, input_token_estimate, output_token_estimate, cascade_depth, quality_score, quality_notes) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24) RETURNING id, created_at"
        );
        assert_eq!(
            super::SQL_INSERT_HISTORY_SUMMARY_SOURCE,
            "INSERT INTO chat_history_summary_sources (summary_id, source_order, source_type, source_summary_id, range_start_at, range_end_at, first_message_id, last_message_id, first_entry_id, last_entry_id, raw_message_count, covered_message_count) VALUES ($1, $2, $3, $4::bigint, $5, $6, $7, $8, $9, $10, $11, $12)"
        );
        assert_eq!(
            super::SQL_INSERT_CHAT_HISTORY_EVENT,
            "INSERT INTO chat_history_events (summary_id, chat_id, thread_id, scope, event_order, title, description, actors, occurred_at, range_start_at, range_end_at, source_summary_ids, confidence) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::timestamptz, $10, $11, $12, $13)"
        );
    }

    #[test]
    fn user_and_chat_state_sql_matches_go_query_contracts() {
        let _user = openplotva_core::UserState {
            id: 500,
            first_name: "Carol".to_owned(),
            last_name: Some(String::new()),
            username: Some("carol".to_owned()),
            language_code: Some(" ru ".to_owned()),
            is_premium: Some(true),
        };
        let _chat = openplotva_core::ChatState {
            id: -300,
            chat_type: "private".to_owned(),
            title: Some(" Team ".to_owned()),
            username: None,
            first_name: Some("Alice".to_owned()),
            last_name: None,
            is_forum: Some(true),
        };

        assert_eq!(
            super::SQL_UPSERT_USER,
            "INSERT INTO users (id, first_name, last_name, username, language_code, is_premium, settings) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb) ON CONFLICT (id) DO UPDATE SET first_name = COALESCE(EXCLUDED.first_name, users.first_name), last_name = COALESCE(EXCLUDED.last_name, users.last_name), username = COALESCE(EXCLUDED.username, users.username), language_code = COALESCE(EXCLUDED.language_code, users.language_code), is_premium = COALESCE(EXCLUDED.is_premium, users.is_premium), settings = COALESCE(EXCLUDED.settings, users.settings), updated = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_GET_USER_ID_BY_USERNAME,
            "SELECT id FROM users WHERE username = $1 LIMIT 1"
        );
        assert_eq!(
            super::SQL_UPSERT_CHAT,
            "INSERT INTO chats (id, type, title, username, first_name, last_name, is_forum, active_usernames, available_reactions, bio, has_private_forwards, has_restricted_voice_and_video_messages, join_to_send_messages, join_by_request, description, invite_link, pinned_message, permissions, slow_mode_delay, message_auto_delete_time, has_aggressive_anti_spam_enabled, has_hidden_members, has_protected_content, has_visible_history, sticker_set_name, can_set_sticker_set, linked_chat_id, location) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11, $12, $13, $14, $15, $16, $17::jsonb, $18::jsonb, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28::jsonb) ON CONFLICT (id) DO UPDATE SET type = COALESCE(EXCLUDED.type, chats.type), title = COALESCE(EXCLUDED.title, chats.title), username = COALESCE(EXCLUDED.username, chats.username), first_name = COALESCE(EXCLUDED.first_name, chats.first_name), last_name = COALESCE(EXCLUDED.last_name, chats.last_name), is_forum = COALESCE(EXCLUDED.is_forum, chats.is_forum), active_usernames = COALESCE(EXCLUDED.active_usernames, chats.active_usernames), available_reactions = COALESCE(EXCLUDED.available_reactions, chats.available_reactions), bio = COALESCE(EXCLUDED.bio, chats.bio), has_private_forwards = COALESCE(EXCLUDED.has_private_forwards, chats.has_private_forwards), has_restricted_voice_and_video_messages = COALESCE(EXCLUDED.has_restricted_voice_and_video_messages, chats.has_restricted_voice_and_video_messages), join_to_send_messages = COALESCE(EXCLUDED.join_to_send_messages, chats.join_to_send_messages), join_by_request = COALESCE(EXCLUDED.join_by_request, chats.join_by_request), description = COALESCE(EXCLUDED.description, chats.description), invite_link = COALESCE(EXCLUDED.invite_link, chats.invite_link), pinned_message = COALESCE(EXCLUDED.pinned_message, chats.pinned_message), permissions = COALESCE(EXCLUDED.permissions, chats.permissions), slow_mode_delay = COALESCE(EXCLUDED.slow_mode_delay, chats.slow_mode_delay), message_auto_delete_time = COALESCE(EXCLUDED.message_auto_delete_time, chats.message_auto_delete_time), has_aggressive_anti_spam_enabled = COALESCE(EXCLUDED.has_aggressive_anti_spam_enabled, chats.has_aggressive_anti_spam_enabled), has_hidden_members = COALESCE(EXCLUDED.has_hidden_members, chats.has_hidden_members), has_protected_content = COALESCE(EXCLUDED.has_protected_content, chats.has_protected_content), has_visible_history = COALESCE(EXCLUDED.has_visible_history, chats.has_visible_history), sticker_set_name = COALESCE(EXCLUDED.sticker_set_name, chats.sticker_set_name), can_set_sticker_set = COALESCE(EXCLUDED.can_set_sticker_set, chats.can_set_sticker_set), linked_chat_id = COALESCE(EXCLUDED.linked_chat_id, chats.linked_chat_id), location = COALESCE(EXCLUDED.location, chats.location), updated = CURRENT_TIMESTAMP"
        );
    }

    #[test]
    fn chat_settings_sql_matches_go_permission_contracts() {
        let _settings = openplotva_core::ChatSettings::defaults(42);
        assert_eq!(
            super::SQL_GET_CHAT_TYPE,
            "SELECT type FROM chats WHERE id = $1"
        );
        assert_eq!(
            super::SQL_GET_CHAT_SETTINGS,
            "SELECT chat_id, mood_alignment, custom_persona, reactivity_percentage, proactivity_percentage, enable_global_text_reply, enable_global_draw_reply, enable_obscenifier, enable_profanity, enable_greet_joiners, enable_daily_game, daily_game_theme, greeting_html FROM chat_settings WHERE chat_id = $1"
        );
        assert_eq!(
            super::SQL_GET_USER_SETTINGS,
            "SELECT user_id, disable_random_reactivity, updated, hide_original_draw_prompt FROM user_settings WHERE user_id = $1"
        );
        assert_eq!(
            super::SQL_UPSERT_USER_SETTINGS,
            "INSERT INTO user_settings (user_id, disable_random_reactivity, hide_original_draw_prompt, updated) VALUES ($1, $2, $3, CURRENT_TIMESTAMP) ON CONFLICT (user_id) DO UPDATE SET disable_random_reactivity = EXCLUDED.disable_random_reactivity, hide_original_draw_prompt = EXCLUDED.hide_original_draw_prompt, updated = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_UPSERT_CHAT_SETTINGS,
            "WITH ensure_chat AS (INSERT INTO chats (id, type) VALUES ($1, COALESCE(NULLIF($14::text, ''), 'private')) ON CONFLICT (id) DO NOTHING) INSERT INTO chat_settings (chat_id, mood_alignment, custom_persona, reactivity_percentage, proactivity_percentage, enable_obscenifier, enable_profanity, enable_greet_joiners, enable_global_text_reply, enable_global_draw_reply, enable_daily_game, daily_game_theme, updated, greeting_html) VALUES ($1, $2, $3, $4, $5, COALESCE($6, TRUE)::boolean, COALESCE($7, TRUE)::boolean, COALESCE($8, FALSE)::boolean, COALESCE($9, TRUE)::boolean, COALESCE($10, TRUE)::boolean, COALESCE($11, TRUE)::boolean, COALESCE($12, 'auto')::text, CURRENT_TIMESTAMP, $13) ON CONFLICT (chat_id) DO UPDATE SET mood_alignment = EXCLUDED.mood_alignment, custom_persona = EXCLUDED.custom_persona, reactivity_percentage = COALESCE(EXCLUDED.reactivity_percentage, chat_settings.reactivity_percentage), proactivity_percentage = COALESCE(EXCLUDED.proactivity_percentage, chat_settings.proactivity_percentage), enable_obscenifier = COALESCE(EXCLUDED.enable_obscenifier, chat_settings.enable_obscenifier), enable_profanity = COALESCE(EXCLUDED.enable_profanity, chat_settings.enable_profanity), enable_greet_joiners = COALESCE(EXCLUDED.enable_greet_joiners, chat_settings.enable_greet_joiners), enable_global_text_reply = COALESCE(EXCLUDED.enable_global_text_reply, chat_settings.enable_global_text_reply), enable_global_draw_reply = COALESCE(EXCLUDED.enable_global_draw_reply, chat_settings.enable_global_draw_reply), enable_daily_game = COALESCE(EXCLUDED.enable_daily_game, chat_settings.enable_daily_game), daily_game_theme = EXCLUDED.daily_game_theme, greeting_html = EXCLUDED.greeting_html, updated = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_LIST_USER_CHATS,
            "SELECT c.id, c.type, c.title, c.username, c.first_name, c.last_name, c.is_forum FROM chats c JOIN chat_members cm ON c.id = cm.chat_id WHERE cm.user_id = $1"
        );
        assert_eq!(
            super::SQL_LIST_CHAT_DEPUTY_IDS,
            "SELECT user_id FROM chat_deputies WHERE chat_id = $1 ORDER BY user_id"
        );
        assert_eq!(
            super::SQL_LIST_USER_DEPUTY_CHAT_IDS,
            "SELECT chat_id FROM chat_deputies WHERE user_id = $1 ORDER BY chat_id"
        );
        assert!(
            super::SQL_SEARCH_CHAT_MEMBER_CANDIDATES
                .contains("ORDER BY cm.last_message_at DESC NULLS LAST")
        );
        assert!(
            super::SQL_UPSERT_CHAT_DEPUTIES.contains("ON CONFLICT (chat_id, user_id) DO NOTHING")
        );
    }

    #[test]
    fn payment_storage_sql_matches_go_query_contracts() {
        assert_eq!(
            super::SQL_CREATE_SUBSCRIPTION,
            "INSERT INTO subscriptions (user_id, telegram_payment_charge_id, provider_payment_charge_id, expires_at) VALUES ($1, $2, $3, $4) ON CONFLICT (telegram_payment_charge_id) DO UPDATE SET expires_at = COALESCE(EXCLUDED.expires_at, subscriptions.expires_at), updated_at = CURRENT_TIMESTAMP RETURNING *"
        );
        assert_eq!(
            super::SQL_GET_ACTIVE_SUBSCRIPTION,
            "SELECT * FROM subscriptions WHERE user_id = $1 AND expires_at > NOW() AND canceled_at IS NULL AND refunded_at IS NULL AND telegram_payment_charge_id NOT LIKE 'admin_grant_%' ORDER BY created_at DESC, id DESC LIMIT 1"
        );
        assert_eq!(
            super::SQL_LIST_SUBSCRIPTIONS_BY_USER,
            "SELECT * FROM subscriptions WHERE user_id = $1 ORDER BY created_at DESC, id DESC"
        );
        assert_eq!(
            super::SQL_GET_SUBSCRIPTION_BY_TELEGRAM_PAYMENT_CHARGE_ID,
            "SELECT * FROM subscriptions WHERE telegram_payment_charge_id = $1 LIMIT 1"
        );
        assert_eq!(
            super::SQL_DELETE_SUBSCRIPTION,
            "DELETE FROM subscriptions WHERE id = $1 RETURNING *"
        );
        assert_eq!(
            super::SQL_EXPIRE_SUBSCRIPTION,
            "UPDATE subscriptions SET expires_at = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *"
        );
        assert_eq!(
            super::SQL_MARK_SUBSCRIPTION_CANCELED,
            "UPDATE subscriptions SET canceled_at = COALESCE(canceled_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *"
        );
        assert_eq!(
            super::SQL_MARK_SUBSCRIPTION_REFUNDED,
            "UPDATE subscriptions SET refunded_at = COALESCE(refunded_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *"
        );
        assert_eq!(
            super::SQL_CREATE_DONATION,
            "INSERT INTO donations (user_id, telegram_payment_charge_id, provider_payment_charge_id, amount_stars) VALUES ($1, $2, $3, $4) ON CONFLICT (telegram_payment_charge_id) DO NOTHING RETURNING *"
        );
        assert_eq!(
            super::SQL_GET_DONATION_BY_TELEGRAM_PAYMENT_CHARGE_ID,
            "SELECT * FROM donations WHERE telegram_payment_charge_id = $1 LIMIT 1"
        );
        assert_eq!(
            super::SQL_DELETE_DONATION,
            "DELETE FROM donations WHERE id = $1 RETURNING *"
        );
    }

    #[tokio::test]
    async fn live_payment_store_round_trips_when_postgres_dsn_is_set() -> Result<(), Box<dyn Error>>
    {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let user_id = 9_002_000_000_000_i64 + i64::try_from(suffix % 1_000_000)?;
        let charge_id = format!("test_subscription_{suffix}");
        let duplicate_provider_id = format!("provider_updated_{suffix}");
        let donation_charge_id = format!("test_donation_{suffix}");
        let admin_charge_id = format!("admin_grant_test_{suffix}");
        let store = super::PostgresPaymentStore::new(pool.clone());
        let identity_store = super::PostgresVirtualMessageStore::new(pool.clone());

        identity_store
            .upsert_user_state(&openplotva_core::UserState::new(
                user_id,
                "Payment Tester",
                None,
                None,
                None,
                None,
            ))
            .await?;

        let first_expiry = time::OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
        let second_expiry = first_expiry + time::Duration::days(30);

        let result: Result<(), Box<dyn Error>> = async {
            let subscription = store
                .create_subscription(super::SubscriptionCreate {
                    user_id,
                    telegram_payment_charge_id: &charge_id,
                    provider_payment_charge_id: "provider-original",
                    expires_at: first_expiry,
                })
                .await?;
            assert_eq!(subscription.user_id, user_id);
            assert_eq!(subscription.telegram_payment_charge_id, charge_id);
            assert_eq!(subscription.provider_payment_charge_id, "provider-original");
            assert_eq!(subscription.expires_at, first_expiry);

            let duplicate = store
                .create_subscription(super::SubscriptionCreate {
                    user_id,
                    telegram_payment_charge_id: &charge_id,
                    provider_payment_charge_id: &duplicate_provider_id,
                    expires_at: second_expiry,
                })
                .await?;
            assert_eq!(duplicate.id, subscription.id);
            assert_eq!(
                duplicate.provider_payment_charge_id, "provider-original",
                "Duplicate Telegram charge IDs only refresh expires_at"
            );
            assert_eq!(duplicate.expires_at, second_expiry);

            let loaded_subscription = store
                .get_subscription_by_telegram_payment_charge_id(&charge_id)
                .await?
                .ok_or_else(|| std::io::Error::other("subscription should be readable"))?;
            assert_eq!(loaded_subscription.id, subscription.id);

            let expired = store
                .expire_subscription(subscription.id, first_expiry)
                .await?;
            assert_eq!(expired.expires_at, first_expiry);

            let active = store
                .get_active_subscription(user_id)
                .await?
                .ok_or_else(|| std::io::Error::other("subscription should be active"))?;
            assert_eq!(active.id, subscription.id);

            let canceled = store.mark_subscription_canceled(subscription.id).await?;
            assert!(canceled.canceled_at.is_some());
            assert!(store.get_active_subscription(user_id).await?.is_none());

            let admin_grant = store
                .create_subscription(super::SubscriptionCreate {
                    user_id,
                    telegram_payment_charge_id: &admin_charge_id,
                    provider_payment_charge_id: "",
                    expires_at: second_expiry,
                })
                .await?;
            assert!(store.get_active_subscription(user_id).await?.is_none());

            let refunded = store.mark_subscription_refunded(admin_grant.id).await?;
            assert!(refunded.refunded_at.is_some());

            let subscriptions = store.list_subscriptions_by_user(user_id).await?;
            assert_eq!(subscriptions.len(), 2);
            assert_eq!(subscriptions[0].id, admin_grant.id);
            assert_eq!(subscriptions[1].id, subscription.id);

            let donation = store
                .create_donation(super::DonationCreate {
                    user_id,
                    telegram_payment_charge_id: &donation_charge_id,
                    provider_payment_charge_id: "provider-donation",
                    amount_stars: 123,
                })
                .await?;
            assert_eq!(donation.user_id, user_id);
            assert_eq!(donation.amount_stars, 123);
            assert!(matches!(
                store
                    .create_donation(super::DonationCreate {
                        user_id,
                        telegram_payment_charge_id: &donation_charge_id,
                        provider_payment_charge_id: "provider-donation-duplicate",
                        amount_stars: 456,
                    })
                    .await,
                Err(super::StorageError::Postgres {
                    source: sqlx::Error::RowNotFound
                })
            ));

            let loaded_donation = store
                .get_donation_by_telegram_payment_charge_id(&donation_charge_id)
                .await?
                .ok_or_else(|| std::io::Error::other("donation should be readable"))?;
            assert_eq!(loaded_donation.id, donation.id);

            let deleted = store.delete_donation(donation.id).await?;
            assert_eq!(deleted.id, donation.id);
            assert!(
                store
                    .get_donation_by_telegram_payment_charge_id(&donation_charge_id)
                    .await?
                    .is_none()
            );

            let deleted_admin_grant = store.delete_subscription(admin_grant.id).await?;
            assert_eq!(deleted_admin_grant.id, admin_grant.id);
            let deleted_subscription = store.delete_subscription(subscription.id).await?;
            assert_eq!(deleted_subscription.id, subscription.id);

            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM donations WHERE telegram_payment_charge_id = $1")
            .bind(&donation_charge_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM subscriptions WHERE telegram_payment_charge_id = ANY($1)")
            .bind([charge_id.as_str(), admin_charge_id.as_str()])
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await;
        result
    }

    #[test]
    fn vip_storage_sql_matches_go_query_contracts() {
        assert_eq!(
            super::SQL_UPSERT_VIP_CACHE,
            "INSERT INTO vip_cache (user_id, is_vip, expires_at) VALUES ($1, $2, $3) ON CONFLICT (user_id) DO UPDATE SET is_vip = COALESCE(EXCLUDED.is_vip, vip_cache.is_vip), expires_at = COALESCE(EXCLUDED.expires_at, vip_cache.expires_at), updated_at = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_CREATE_VIP_EVENT,
            "SELECT id, user_id, event_type, delta_seconds, effective_expires_at, subscription_id, actor_user_id, reason, created_at FROM vip_create_event($1, $2, $3, $4, $5, $6)"
        );
        assert_eq!(
            super::SQL_GET_VIP_SUMMARY_BY_USER,
            "SELECT id AS latest_event_id, user_id, event_type AS latest_event_type, delta_seconds AS latest_delta_seconds, effective_expires_at, effective_expires_at > CURRENT_TIMESTAMP AS is_active, CASE WHEN effective_expires_at > CURRENT_TIMESTAMP THEN FLOOR(EXTRACT(EPOCH FROM (effective_expires_at - CURRENT_TIMESTAMP)))::bigint ELSE 0::bigint END AS remaining_seconds, subscription_id AS latest_subscription_id, actor_user_id AS latest_actor_user_id, reason AS latest_reason, created_at AS latest_created_at FROM vip_events WHERE user_id = $1 ORDER BY id DESC LIMIT 1"
        );
        assert_eq!(
            super::SQL_LIST_VIP_EVENTS_BY_USER,
            "SELECT ve.id, ve.user_id, ve.event_type, ve.delta_seconds, ve.effective_expires_at, ve.subscription_id, ve.actor_user_id, actor.username AS actor_username, actor.first_name AS actor_first_name, ve.reason, ve.created_at, s.telegram_payment_charge_id, s.provider_payment_charge_id, s.expires_at AS subscription_expires_at, s.canceled_at AS subscription_canceled_at, s.refunded_at AS subscription_refunded_at FROM vip_events ve LEFT JOIN users actor ON actor.id = ve.actor_user_id LEFT JOIN subscriptions s ON s.id = ve.subscription_id WHERE ve.user_id = $1 ORDER BY ve.id DESC"
        );
        assert_eq!(
            super::SQL_GET_VIP_CACHE,
            "SELECT * FROM vip_cache WHERE user_id = $1 AND expires_at > CURRENT_TIMESTAMP LIMIT 1"
        );
        assert_eq!(
            super::SQL_DELETE_VIP_CACHE,
            "DELETE FROM vip_cache WHERE user_id = $1"
        );
        assert_eq!(
            super::SQL_CLEANUP_EXPIRED_VIP_CACHE,
            "DELETE FROM vip_cache WHERE expires_at <= CURRENT_TIMESTAMP RETURNING user_id"
        );
    }

    #[tokio::test]
    async fn live_vip_store_round_trips_when_postgres_dsn_is_set() -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let user_id = 9_003_000_000_000_i64 + i64::try_from(suffix % 1_000_000)?;
        let actor_id = user_id + 1;
        let charge_id = format!("test_vip_subscription_{suffix}");
        let identity_store = super::PostgresVirtualMessageStore::new(pool.clone());
        let payment_store = super::PostgresPaymentStore::new(pool.clone());
        let vip_store = super::PostgresVipStore::new(pool.clone());
        let future_expiry = time::OffsetDateTime::from_unix_timestamp(1_900_000_000)?;
        let past_expiry = time::OffsetDateTime::from_unix_timestamp(1_600_000_000)?;

        identity_store
            .upsert_user_state(&openplotva_core::UserState::new(
                user_id,
                "VIP Tester",
                None,
                None,
                None,
                None,
            ))
            .await?;
        identity_store
            .upsert_user_state(&openplotva_core::UserState::new(
                actor_id,
                "Admin Actor",
                None,
                Some("admin_actor".to_owned()),
                None,
                None,
            ))
            .await?;

        let result: Result<(), Box<dyn Error>> = async {
            vip_store
                .upsert_vip_cache(super::VipCacheUpsert {
                    user_id,
                    is_vip: true,
                    expires_at: future_expiry,
                })
                .await?;
            let cache = vip_store
                .get_vip_cache(user_id)
                .await?
                .ok_or_else(|| std::io::Error::other("future VIP cache should be readable"))?;
            assert_eq!(cache.user_id, user_id);
            assert!(cache.is_vip);
            assert_eq!(cache.expires_at, future_expiry);

            vip_store.delete_vip_cache(user_id).await?;
            assert!(vip_store.get_vip_cache(user_id).await?.is_none());
            vip_store
                .upsert_vip_cache(super::VipCacheUpsert {
                    user_id: actor_id,
                    is_vip: true,
                    expires_at: past_expiry,
                })
                .await?;
            assert!(vip_store.get_vip_cache(actor_id).await?.is_none());
            assert!(
                vip_store
                    .cleanup_expired_vip_cache()
                    .await?
                    .contains(&actor_id)
            );

            let subscription = payment_store
                .create_subscription(super::SubscriptionCreate {
                    user_id,
                    telegram_payment_charge_id: &charge_id,
                    provider_payment_charge_id: "provider-vip",
                    expires_at: future_expiry,
                })
                .await?;
            let payment_event = vip_store
                .create_vip_event(super::VipEventCreate {
                    user_id,
                    event_type: openplotva_core::VIP_EVENT_TYPE_PAYMENT,
                    delta_seconds: openplotva_core::vip_days_to_seconds(30),
                    subscription_id: Some(subscription.id),
                    actor_user_id: None,
                    reason: Some("payment charge"),
                })
                .await?;
            let duplicate_payment_event = vip_store
                .create_vip_event(super::VipEventCreate {
                    user_id,
                    event_type: openplotva_core::VIP_EVENT_TYPE_PAYMENT,
                    delta_seconds: openplotva_core::vip_days_to_seconds(30),
                    subscription_id: Some(subscription.id),
                    actor_user_id: None,
                    reason: Some("payment duplicate"),
                })
                .await?;
            assert_eq!(
                duplicate_payment_event.id, payment_event.id,
                "VIP event creation returns the existing subscription-scoped event on conflicts"
            );

            let adjustment = vip_store
                .create_vip_event(super::VipEventCreate {
                    user_id,
                    event_type: openplotva_core::VIP_EVENT_TYPE_ADMIN_ADJUSTMENT,
                    delta_seconds: -3_600,
                    subscription_id: None,
                    actor_user_id: Some(actor_id),
                    reason: Some("admin correction"),
                })
                .await?;

            let summary = vip_store
                .get_vip_summary_by_user(user_id)
                .await?
                .ok_or_else(|| std::io::Error::other("VIP summary should be readable"))?;
            assert_eq!(summary.latest_event_id, adjustment.id);
            assert_eq!(
                summary.latest_event_type,
                openplotva_core::VIP_EVENT_TYPE_ADMIN_ADJUSTMENT
            );
            assert_eq!(summary.latest_actor_user_id, Some(actor_id));
            assert_eq!(summary.latest_reason, "admin correction");

            let events = vip_store.list_vip_events_by_user(user_id).await?;
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].id, adjustment.id);
            assert_eq!(events[0].actor_username.as_deref(), Some("admin_actor"));
            assert_eq!(events[0].actor_first_name.as_deref(), Some("Admin Actor"));
            assert_eq!(events[1].id, payment_event.id);
            assert_eq!(events[1].subscription_id, Some(subscription.id));
            assert_eq!(
                events[1].telegram_payment_charge_id.as_deref(),
                Some(charge_id.as_str())
            );
            assert_eq!(
                events[1].provider_payment_charge_id.as_deref(),
                Some("provider-vip")
            );
            assert_eq!(events[1].subscription_expires_at, Some(future_expiry));

            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM vip_events WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM vip_cache WHERE user_id = ANY($1)")
            .bind([user_id, actor_id])
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM subscriptions WHERE telegram_payment_charge_id = $1")
            .bind(&charge_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = ANY($1)")
            .bind([user_id, actor_id])
            .execute(&pool)
            .await;
        result
    }

    #[tokio::test]
    async fn live_virtual_message_store_round_trips_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let store = super::PostgresVirtualMessageStore::new(pool);
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let vmsg_id = format!("t{suffix}");
        let chat_id = -9_001_234_567_890_i64;
        let real_message_id = 321_987_i32;
        let _ = store.delete_mapping_by_virtual(&vmsg_id).await;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .insert_virtual_message(&vmsg_id, chat_id, Some(77))
                .await?;
            let mapping = store
                .get_mapping_by_virtual(&vmsg_id)
                .await?
                .ok_or_else(|| std::io::Error::other("inserted mapping was not readable"))?;
            assert_eq!(mapping.vmsg_id, vmsg_id);
            assert_eq!(mapping.chat_id, chat_id);
            assert_eq!(mapping.thread_id, Some(77));
            assert_eq!(mapping.real_message_id, None);

            let op_id = store
                .enqueue_message_op(
                    &vmsg_id,
                    chat_id,
                    "edit",
                    Some(r#"{"text":"edited","parse_mode":"HTML"}"#),
                )
                .await?;
            let pending = store.list_pending_ops(1_000).await?;
            let op = pending
                .iter()
                .find(|row| row.id == op_id)
                .ok_or_else(|| std::io::Error::other("enqueued op was not listed as pending"))?;
            assert_eq!(op.vmsg_id, vmsg_id);
            assert_eq!(op.chat_id, chat_id);
            assert_eq!(op.op, "edit");
            let payload: serde_json::Value = serde_json::from_slice(&op.payload)?;
            assert_eq!(
                payload,
                serde_json::json!({"text": "edited", "parse_mode": "HTML"})
            );
            assert_eq!(op.attempts, 0);

            store.mark_op_failed(op_id, "temporary failure").await?;
            let pending = store.list_pending_ops(1_000).await?;
            let op = pending
                .iter()
                .find(|row| row.id == op_id)
                .ok_or_else(|| std::io::Error::other("failed op was not still pending"))?;
            assert_eq!(op.attempts, 1);

            store
                .resolve_virtual_message(&vmsg_id, real_message_id, None)
                .await?;
            let mapping = store
                .get_mapping_by_real(chat_id, real_message_id)
                .await?
                .ok_or_else(|| std::io::Error::other("resolved mapping was not readable"))?;
            assert_eq!(mapping.vmsg_id, vmsg_id);
            assert_eq!(mapping.real_message_id, Some(real_message_id));

            store.mark_op_done(op_id).await?;
            let pending = store.list_pending_ops(1_000).await?;
            assert!(pending.iter().all(|row| row.id != op_id));

            Ok(())
        }
        .await;

        let cleanup = store.delete_mapping_by_virtual(&vmsg_id).await;
        result?;
        cleanup?;
        assert!(store.get_mapping_by_virtual(&vmsg_id).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn live_history_store_updates_and_deletes_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let store = super::PostgresHistoryStore::new(pool.clone());
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let chat_id = -9_001_222_333_444_i64;
        let message_id = i32::try_from(suffix % 1_000_000_000)?;
        let entry_id = format!("msg:{message_id}");
        let occurred_at = time::OffsetDateTime::now_utc();
        let bucket_day = occurred_at.date();
        let payload = serde_json::json!({
            "entry_id": entry_id,
            "role": "user",
            "kind": "text",
            "timestamp": "2026-05-20T00:00:00Z",
            "message_id": message_id,
            "date": occurred_at.unix_timestamp(),
            "chat": {"id": chat_id, "type": "private"},
            "text": "old text",
            "meta": {}
        })
        .to_string();

        sqlx::query("SELECT ensure_chat_history_partition($1::date)")
            .bind(bucket_day)
            .execute(&pool)
            .await?;
        let _ = store.delete_message_entries(chat_id, message_id).await;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .upsert_history_entry(super::HistoryEntryUpsert {
                    bucket_day,
                    chat_id,
                    thread_id: 77,
                    message_id,
                    entry_id: &entry_id,
                    kind: "text",
                    role: "user",
                    occurred_at,
                    sender_id: 100,
                    payload: payload.as_bytes(),
                })
                .await?;

            let range_start = occurred_at - time::Duration::seconds(1);
            let range_end = occurred_at + time::Duration::seconds(1);
            let chat_payloads = store
                .summary_entry_payloads(
                    chat_id,
                    77,
                    openplotva_history::SummaryScope::Chat,
                    range_start,
                    range_end,
                )
                .await?;
            assert_eq!(chat_payloads.len(), 1);
            let thread_payloads = store
                .summary_entry_payloads(
                    chat_id,
                    77,
                    openplotva_history::SummaryScope::Thread,
                    range_start,
                    range_end,
                )
                .await?;
            assert_eq!(thread_payloads.len(), 1);
            let wrong_thread_payloads = store
                .summary_entry_payloads(
                    chat_id,
                    78,
                    openplotva_history::SummaryScope::Thread,
                    range_start,
                    range_end,
                )
                .await?;
            assert!(wrong_thread_payloads.is_empty());

            assert!(store
                .update_text_entry(chat_id, message_id, "new text")
                .await?);
            let updated_payload: String = sqlx::query_scalar(
                "SELECT payload::text FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2 AND kind = 'text'",
            )
            .bind(chat_id)
            .bind(message_id)
            .fetch_one(&pool)
            .await?;
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&updated_payload)?["text"],
                "new text"
            );

            assert_eq!(store.delete_message_entries(chat_id, message_id).await?, 1);
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2",
            )
            .bind(chat_id)
            .bind(message_id)
            .fetch_one(&pool)
            .await?;
            assert_eq!(count, 0);

            let reset_at = occurred_at + time::Duration::seconds(2);
            assert!(store.reset_history_at(chat_id, 77, reset_at).await?);
            assert_eq!(store.history_reset_at(chat_id, 77).await?, Some(reset_at));
            assert_eq!(store.history_reset_at(chat_id, 78).await?, None);

            Ok(())
        }
        .await;

        let _ = store.delete_message_entries(chat_id, message_id).await;
        result
    }

    #[tokio::test]
    async fn live_history_summary_store_round_trips_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let store = super::PostgresHistoryStore::new(pool.clone());
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let chat_id = -9_001_333_444_555_i64 - i64::try_from(suffix % 1_000_000)?;
        let start = time::OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
        let end = start + time::Duration::minutes(15);
        let event_at = start + time::Duration::minutes(5);
        let event_at_text = event_at.format(&time::format_description::well_known::Rfc3339)?;
        let input = openplotva_history::SummaryInput {
            chat_id,
            thread_id: 12,
            scope: openplotva_history::SummaryScope::Thread,
            range_start_at: start,
            range_end_at: end,
            first_message_id: 100,
            last_message_id: 105,
            first_entry_id: "msg:100".to_owned(),
            last_entry_id: "msg:105".to_owned(),
            raw_message_count: 6,
            covered_message_count: 6,
            requested_by_user_id: 77,
            input_hash: "input-hash".to_owned(),
            input_token_estimate: 123,
            ..openplotva_history::SummaryInput::default()
        };
        let doc = openplotva_history::SummaryDocument {
            content: openplotva_history::SummaryContent {
                event_details: vec![openplotva_history::SummaryEvent {
                    title: " shipped ".to_owned(),
                    description: "release".to_owned(),
                    actors: vec!["Ada".to_owned()],
                    occurred_at: event_at_text,
                    confidence: 0.0,
                }],
                recap: "done".to_owned(),
                quality_score: 0.8,
                ..openplotva_history::SummaryContent::default()
            },
            html: "<b>done</b>".to_owned(),
            model: "aifarm/test".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            ..openplotva_history::SummaryDocument::default()
        };

        let result: Result<(), Box<dyn Error>> = async {
            let stored = store.save_summary(&input, &doc).await?;
            assert!(stored.id > 0);
            assert_eq!(stored.chat_id, chat_id);
            assert_eq!(stored.scope, openplotva_history::SummaryScope::Thread);
            assert_eq!(stored.summary_json.events, vec!["shipped"]);
            assert_eq!(stored.quality_score, 0.8);

            let loaded = store
                .reusable_history_summaries(
                    chat_id,
                    12,
                    openplotva_history::SummaryScope::Thread,
                    start - time::Duration::seconds(1),
                    end + time::Duration::seconds(1),
                    start - time::Duration::days(1),
                )
                .await?;
            let loaded = loaded
                .iter()
                .find(|summary| summary.id == stored.id)
                .ok_or_else(|| std::io::Error::other("stored summary was not reusable"))?;
            assert_eq!(loaded.summary_html, "<b>done</b>");
            assert_eq!(loaded.source_summary_ids, Vec::<i64>::new());
            assert_eq!(loaded.summary_json.event_details[0].title, "shipped");

            let source_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM chat_history_summary_sources WHERE summary_id = $1 AND source_type = 'message_range'",
            )
            .bind(stored.id)
            .fetch_one(&pool)
            .await?;
            assert_eq!(source_count, 1);

            let (title, confidence): (String, f64) = sqlx::query_as(
                "SELECT title, confidence FROM chat_history_events WHERE summary_id = $1 ORDER BY event_order ASC LIMIT 1",
            )
            .bind(stored.id)
            .fetch_one(&pool)
            .await?;
            assert_eq!(title, "shipped");
            assert_eq!(confidence, 0.8);

            sqlx::query("DELETE FROM chat_history_summaries WHERE id = $1")
                .bind(stored.id)
                .execute(&pool)
                .await?;
            Ok(())
        }
        .await;

        if let Err(error) = &result {
            let _ = sqlx::query("DELETE FROM chat_history_summaries WHERE chat_id = $1")
                .bind(chat_id)
                .execute(&pool)
                .await;
            return Err(error.to_string().into());
        }
        result
    }

    #[tokio::test]
    async fn live_chat_settings_store_round_trips_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let store = super::PostgresChatSettingsStore::new(pool.clone());
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let chat_id = -9_001_444_555_666_i64 - i64::try_from(suffix % 1_000_000)?;
        let mut settings = openplotva_core::ChatSettings::defaults(chat_id);
        settings.enable_global_draw_reply = false;
        let update = openplotva_core::ChatSettingsUpdate {
            chat_id,
            chat_type: "supergroup".to_owned(),
            mood_alignment: settings.mood_alignment.clone(),
            custom_persona: settings.custom_persona.clone(),
            reactivity_percentage: settings.reactivity_percentage,
            proactivity_percentage: settings.proactivity_percentage,
            enable_global_text_reply: settings.enable_global_text_reply,
            enable_global_draw_reply: settings.enable_global_draw_reply,
            enable_obscenifier: settings.enable_obscenifier,
            enable_profanity: settings.enable_profanity,
            enable_greet_joiners: settings.enable_greet_joiners,
            enable_daily_game: settings.enable_daily_game.unwrap_or(true),
            daily_game_theme: settings.daily_game_theme.clone().unwrap_or_default(),
            greeting_html: None,
        };

        let result: Result<(), Box<dyn Error>> = async {
            store.upsert_chat_settings(&update).await?;
            let loaded = store
                .get_chat_settings(chat_id)
                .await?
                .ok_or_else(|| std::io::Error::other("inserted settings were not readable"))?;

            assert_eq!(loaded.chat_id, chat_id);
            assert!(loaded.enable_global_text_reply);
            assert!(!loaded.enable_global_draw_reply);
            assert_eq!(loaded.daily_game_theme.as_deref(), Some("auto"));

            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM chat_settings WHERE chat_id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM chats WHERE id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await;
        result
    }

    #[tokio::test]
    async fn live_chat_member_store_round_trips_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let identity_store = super::PostgresVirtualMessageStore::new(pool.clone());
        let store = super::PostgresChatMemberStore::new(pool.clone());
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let chat_id = -9_001_555_666_777_i64 - i64::try_from(suffix % 1_000_000)?;
        let user_id = 9_001_000_000_i64 + i64::try_from(suffix % 1_000_000)?;

        let result: Result<(), Box<dyn Error>> = async {
            identity_store
                .upsert_chat_state(&openplotva_core::ChatState::new(
                    chat_id,
                    "supergroup",
                    Some("member test".to_owned()),
                    None,
                    None,
                    None,
                    None,
                ))
                .await?;
            identity_store
                .upsert_user_state(&openplotva_core::UserState::new(
                    user_id, "Ada", None, None, None, None,
                ))
                .await?;
            store
                .upsert_chat_member(&super::ChatMemberUpsert {
                    chat_id,
                    user_id,
                    status: super::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
                    can_promote_members: Some(true),
                    can_delete_messages: Some(true),
                    ..super::ChatMemberUpsert::default()
                })
                .await?;
            store
                .upsert_chat_member(&super::ChatMemberUpsert {
                    chat_id,
                    user_id,
                    status: super::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
                    can_delete_messages: Some(false),
                    ..super::ChatMemberUpsert::default()
                })
                .await?;

            let member = store
                .get_chat_member(chat_id, user_id)
                .await?
                .ok_or_else(|| std::io::Error::other("inserted member was not readable"))?;

            assert_eq!(member.chat_id, chat_id);
            assert_eq!(member.user_id, user_id);
            assert_eq!(member.status, super::CHAT_MEMBER_STATUS_ADMINISTRATOR);
            assert_eq!(
                member.can_promote_members,
                Some(true),
                "COALESCE upsert preserves nullable permissions when later writes omit them"
            );
            assert_eq!(member.can_delete_messages, Some(false));
            let members = store.list_chat_members(chat_id).await?;
            assert_eq!(members.len(), 1);
            assert_eq!(members[0].user_id, user_id);

            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM chats WHERE id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await;
        result
    }

    #[tokio::test]
    async fn live_redis_rate_limit_store_round_trips_when_url_is_set() -> Result<(), Box<dyn Error>>
    {
        let Ok(url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let prefix = format!("openplotva:test:rate_limited_chat:{suffix}:");
        let store = super::RedisRateLimitStore::with_key_prefix(client.clone(), prefix);
        let chat_id = -900_123_i64;
        let now = time::OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let expiry = now + time::Duration::seconds(30);
        let mut connection = client.get_multiplexed_async_connection().await?;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .set_chat_rate_limit(chat_id, expiry, Duration::from_secs(30))
                .await?;

            assert_eq!(store.chat_rate_limit_expiry(chat_id).await?, Some(expiry));
            assert!(store.chat_is_rate_limited_at(chat_id, now).await?);
            assert!(!store.chat_is_rate_limited_at(chat_id, expiry).await?);

            Ok(())
        }
        .await;

        let _: i64 = redis::cmd("DEL")
            .arg(store.key_for_chat(chat_id))
            .query_async(&mut connection)
            .await?;
        result
    }

    #[tokio::test]
    async fn live_redis_chat_admin_cache_store_round_trips_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let store = super::RedisChatAdminCacheStore::with_key_prefix(
            client.clone(),
            format!("openplotva:test:chat_admins:{suffix}:"),
        );
        let chat_id = -900_124_i64;
        let mut connection = client.get_multiplexed_async_connection().await?;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .set_chat_admin_ids(chat_id, &[42, 43], Duration::from_secs(30 * 60))
                .await?;

            assert_eq!(store.chat_admin_ids(chat_id).await?, Some(vec![42, 43]));
            Ok(())
        }
        .await;

        let _: i64 = redis::cmd("DEL")
            .arg(store.key_for_chat(chat_id))
            .query_async(&mut connection)
            .await?;
        result
    }

    #[tokio::test]
    async fn live_redis_ephemeral_message_store_round_trips_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let store = super::RedisEphemeralMessageStore::with_key_prefix(
            client.clone(),
            format!("openplotva:test:ephemeral_messages:{suffix}:"),
        );
        let expires_at =
            time::OffsetDateTime::from_unix_timestamp_nanos(1_710_000_000_123_456_789)?;
        let message = super::EphemeralMessage {
            chat_id: -900_125,
            message_id: 77,
            expires_at,
        };
        let mut connection = client.get_multiplexed_async_connection().await?;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .set_ephemeral_message(
                    &message,
                    super::ephemeral_redis_ttl(
                        Duration::from_secs(60),
                        super::EPHEMERAL_DEFAULT_CLEANUP_INTERVAL,
                    ),
                )
                .await?;

            assert_eq!(
                store
                    .ephemeral_message(message.chat_id, message.message_id)
                    .await?,
                Some(message.clone())
            );
            assert!(
                store.ephemeral_messages().await?.contains(&message),
                "SCAN/MGET should load the same Rust-native ephemeral record"
            );
            store
                .delete_ephemeral_messages(std::slice::from_ref(&message))
                .await?;
            assert_eq!(
                store
                    .ephemeral_message(message.chat_id, message.message_id)
                    .await?,
                None
            );
            Ok(())
        }
        .await;

        let _: i64 = redis::cmd("DEL")
            .arg(store.key_for_message(message.chat_id, message.message_id))
            .query_async(&mut connection)
            .await?;
        result
    }

    #[tokio::test]
    async fn live_redis_last_generation_store_round_trips_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let store = super::RedisLastGenerationStore::with_key_prefix(
            client.clone(),
            format!("openplotva:test:last_gen:{suffix}:"),
        );
        let generation = super::LastGenerationRecord {
            chat_id: -900_126,
            user_id: 88,
            message_ids: vec![501, 502],
            caption: "caption".to_owned(),
            created_at: 1_710_000_000,
        };
        let mut connection = client.get_multiplexed_async_connection().await?;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .set_last_generation_with_ttl(&generation, Duration::from_secs(30))
                .await?;

            assert_eq!(
                store
                    .last_generation(generation.chat_id, generation.user_id)
                    .await?,
                Some(generation.clone())
            );
            store
                .delete_last_generation(generation.chat_id, generation.user_id)
                .await?;
            assert_eq!(
                store
                    .last_generation(generation.chat_id, generation.user_id)
                    .await?,
                None
            );
            Ok(())
        }
        .await;

        let _: i64 = redis::cmd("DEL")
            .arg(store.key_for_generation(generation.chat_id, generation.user_id))
            .query_async(&mut connection)
            .await?;
        result
    }
}
