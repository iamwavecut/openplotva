//! Dialog tools scoped to the authenticated requester and current chat.
use openplotva_dialog::{
    DialogToolbox, ToolContext, ToolResult, ToolStep, ToolboxFuture, sanitize_tool_text,
};
use openplotva_memory::{CardInput, ObservationScope, RetrievalScope};
use openplotva_storage::{PostgresHistoryStore, PostgresMemoryStore};
use serde_json::{Value, json};
use std::sync::Arc;
use time::OffsetDateTime;

pub struct AgentContextTools {
    pub direct_draw: Arc<dyn crate::dialog_messages::DirectDrawApiEffects>,
    pub history: PostgresHistoryStore,
    pub memory: PostgresMemoryStore,
    pub vip: Arc<dyn crate::payments::VipStatusChecker + Send + Sync>,
}

fn ok(data: Value) -> ToolResult {
    ToolResult {
        status: "ok".into(),
        data: Some(data),
        ..ToolResult::default()
    }
}

impl DialogToolbox for AgentContextTools {
    fn agent_tool<'a>(&'a self, ctx: ToolContext, step: ToolStep) -> ToolboxFuture<'a> {
        Box::pin(async move {
            let scope = RetrievalScope {
                chat_id: ctx.chat_id,
                thread_id: ctx.thread_id.unwrap_or(0),
                user_id: ctx.user_id,
                chat_type: if ctx.chat_id == ctx.user_id {
                    "private"
                } else {
                    "supergroup"
                }
                .into(),
                ..RetrievalScope::default()
            };
            match step.step.as_str() {
                "draw_api" => {
                    let result = self
                        .direct_draw
                        .send_direct_draw_api(crate::dialog_messages::DirectDrawApiRequest {
                            chat_id: ctx.chat_id,
                            message_id: ctx.message_id,
                            user_id: ctx.user_id,
                            user_full_name: ctx.user_full_name,
                            prompt: step.prompt,
                            caption: sanitize_tool_text(&step.caption),
                            thread_id: ctx.thread_id,
                            is_forum: ctx.thread_id.is_some(),
                        })
                        .await;
                    Ok(if result.sent {
                        ok(json!({"delivered":true}))
                    } else {
                        ToolResult::failed(
                            "draw_api_failed",
                            result
                                .error
                                .unwrap_or_else(|| "Image was not delivered".into()),
                        )
                    })
                }

                "get_user_status" => match self
                    .vip
                    .verified_is_vip_at(ctx.user_id, OffsetDateTime::now_utc())
                    .await
                {
                    Ok(vip) => Ok(ok(
                        json!({"user_id":ctx.user_id, "status":"verified", "vip":vip, "entitlements":{"generate_song":vip,"edit_image":vip,"draw_image":true},"subject_to_chat_policy_and_rate_limits":true}),
                    )),
                    Err(_) => Ok(ToolResult::failed(
                        "status_unknown",
                        "VIP verification is unavailable. Do not treat this as a confirmed non-VIP status.",
                    )),
                },
                "get_messages" | "history_search" => {
                    if step.message_ids.len() > 20 {
                        return Ok(ToolResult::failed(
                            "too_many_ids",
                            "Read at most 20 messages per call",
                        ));
                    }
                    let messages = self
                        .history
                        .agent_messages(
                            ctx.chat_id,
                            scope.thread_id,
                            &step.message_ids,
                            &step.query,
                            step.author_id,
                        )
                        .await?;
                    let nearby = if step.step == "get_messages" && !messages.is_empty() {
                        let ids = messages
                            .iter()
                            .filter_map(|message| message["message_id"].as_i64())
                            .filter_map(|id| i32::try_from(id).ok())
                            .collect::<Vec<_>>();
                        self.history
                            .agent_message_neighbors(ctx.chat_id, scope.thread_id, &ids)
                            .await?
                    } else {
                        Vec::new()
                    };
                    Ok(ok(
                        json!({"messages": messages, "nearby":nearby, "limit":40}),
                    ))
                }
                "get_job_status" => Ok(ok(
                    json!({"job":self.history.agent_job_status(ctx.chat_id, ctx.user_id, step.job_id).await?}),
                )),
                "memory_search" => {
                    let cards = if step.memory_scope == "self_global" {
                        if ctx.chat_id != ctx.user_id {
                            return Ok(ToolResult::failed(
                                "scope_denied",
                                "Global recall is available only in your private chat",
                            ));
                        }
                        self.memory.agent_own_memory(ctx.user_id).await?
                    } else {
                        self.memory.list_visible_cards(&scope, 100).await?.into_iter().map(|card| json!({"id":card.id,"text":card.fact_text,"scope":card.visibility,"user_id":card.user_id})).collect()
                    };
                    let terms = step.query.to_lowercase();
                    let cards = cards
                        .into_iter()
                        .filter(|card| {
                            terms.is_empty()
                                || terms.split_whitespace().any(|term| {
                                    card["text"]
                                        .as_str()
                                        .unwrap_or_default()
                                        .to_lowercase()
                                        .contains(term)
                                })
                        })
                        .take(20)
                        .collect::<Vec<_>>();
                    Ok(ok(json!({"cards": cards})))
                }
                "memory_manage" => {
                    let global = step.memory_scope == "self_global";
                    if !matches!(step.memory_scope.as_str(), "self" | "chat" | "self_global")
                        || (global && (ctx.chat_id != ctx.user_id || step.action != "forget"))
                    {
                        return Ok(ToolResult::failed(
                            "scope_denied",
                            "Use self or chat. Global forgetting requires an explicit request in the requester's private chat.",
                        ));
                    }
                    let text = step.text.trim();
                    if step.action != "forget" && (text.is_empty() || text.chars().count() > 2000) {
                        return Ok(ToolResult::failed(
                            "invalid_fact",
                            "Supply a fact of 1 to 2000 characters",
                        ));
                    }
                    if step.action == "remember" {
                        let personal = step.memory_scope == "self";
                        let card = CardInput {
                            observation_scope: ObservationScope {
                                chat_id: ctx.chat_id,
                                thread_id: scope.thread_id,
                                user_id: ctx.user_id,
                                chat_type: scope.chat_type,
                                kind: if personal { "user" } else { "chat" }.into(),
                                ..ObservationScope::default()
                            },
                            card_type: "technical_fact".into(),
                            subject: if personal {
                                format!("user:{}", ctx.user_id)
                            } else {
                                format!("chat:{}", ctx.chat_id)
                            },
                            predicate: "fact".into(),
                            object: text.into(),
                            fact_text: text.into(),
                            confidence: 0.9,
                            salience: 0.7,
                            source_message_ids: vec![ctx.message_id],
                            observed_at: OffsetDateTime::now_utc(),
                            valid_from: OffsetDateTime::now_utc(),
                            ..CardInput::default()
                        };
                        let (_, ids) = self.memory.upsert_cards_lexical(&[card]).await?;
                        return Ok(ok(json!({"card_ids":ids})));
                    }
                    if !matches!(step.action.as_str(), "update" | "forget") {
                        return Ok(ToolResult::failed(
                            "invalid_action",
                            "Use remember, update, or forget",
                        ));
                    }
                    let count = self
                        .memory
                        .agent_change_memory(
                            &scope,
                            step.card_id,
                            &step.action,
                            if step.action == "forget" { "" } else { text },
                            &step.memory_scope,
                        )
                        .await?;
                    if count == 0 {
                        return Ok(ToolResult::failed(
                            "not_found_or_denied",
                            "No permitted active memory card matches this request",
                        ));
                    }
                    Ok(ok(json!({"changed":count})))
                }
                _ => Ok(ToolResult::failed("unknown_tool", "Unknown context tool")),
            }
        })
    }
}

/// One text-only continuation for a failed media delivery, with the original context.
pub struct MediaFailureExplainer {
    pub provider: openplotva_llm::ChatProviderHandle,
    pub materializer: crate::dialog_jobs::PostgresDialogInputMaterializer,
    pub history: PostgresHistoryStore,
}
impl MediaFailureExplainer {
    pub async fn explain(
        &self,
        chat_id: i64,
        thread_id: Option<i32>,
        message_id: i32,
        failure: &str,
    ) -> Option<String> {
        use crate::dialog_jobs::DialogInputMaterializer;
        let messages = self
            .history
            .agent_messages(chat_id, thread_id.unwrap_or(0), &[message_id], "", 0)
            .await
            .ok()?;
        let source = messages.first()?;
        let payload = source.get("message")?.to_string();
        let entry =
            openplotva_history::decode_summary_message_entry_payload(payload.as_bytes()).ok()?;
        let params = openplotva_taskman::DialogJobParams {
            chat_id,
            thread_id,
            message_id,
            user_id: source.get("user_id")?.as_i64()?,
            user_full_name: entry
                .from
                .as_ref()
                .map(|user| user.first_name.clone())
                .unwrap_or_default(),
            message_text: entry.text,
            original_text: entry.original_text,
            meta: serde_json::to_value(entry.meta).ok()?,
            max_output_tokens: 512,
        };
        let mut input = self
            .materializer
            .materialize_dialog_input(&params, OffsetDateTime::now_utc())
            .await
            .ok()?;
        input.disable_tools = true;
        input.reference_context.push(format!("Media delivery has ended with a problem: {failure}. Explain this once in your usual voice. Do not invent a cause or claim an undelivered result. Do not restart generation."));
        struct NoTools;
        impl DialogToolbox for NoTools {}
        let result = crate::dialog_turn::run_captured_session(
            self.provider.as_chat_step()?,
            &NoTools,
            input,
            1,
        )
        .await
        .ok()?;
        result.messages.into_iter().last()
    }
}
