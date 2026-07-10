//! Runtime GraphQL adapter for the durable Telegram inbox and outbox.

use openplotva_server::{
    RuntimeTelegramDeliveryFuture, RuntimeTelegramDeliveryInspector,
    RuntimeTelegramDeliveryListFilter, RuntimeTelegramOutboxAttemptData,
    RuntimeTelegramOutboxItemData, RuntimeTelegramOutboxMutationResultData,
    RuntimeTelegramOutboxRetryRequest, RuntimeTelegramOutboxStatsData,
    RuntimeTelegramUpdateAttemptData, RuntimeTelegramUpdateInboxItemData,
    RuntimeTelegramUpdateInboxStatsData,
};
use openplotva_storage::{
    PostgresTelegramDeliveryStore, PostgresTelegramOutboxStore, TelegramOutboxAttempt,
    TelegramOutboxItem, TelegramOutboxStats, TelegramUpdateAttempt, TelegramUpdateInboxItem,
    TelegramUpdateInboxStats,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Debug)]
pub struct PostgresRuntimeTelegramDeliveryInspector {
    inbox: PostgresTelegramDeliveryStore,
    outbox: PostgresTelegramOutboxStore,
}

impl PostgresRuntimeTelegramDeliveryInspector {
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            inbox: PostgresTelegramDeliveryStore::new(pool.clone()),
            outbox: PostgresTelegramOutboxStore::new(pool),
        }
    }
}

impl RuntimeTelegramDeliveryInspector for PostgresRuntimeTelegramDeliveryInspector {
    fn update_inbox_stats<'a>(
        &'a self,
    ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramUpdateInboxStatsData> {
        Box::pin(async move {
            self.inbox
                .stats()
                .await
                .map(runtime_inbox_stats)
                .map_err(|error| error.to_string())
        })
    }

    fn update_inbox_item<'a>(
        &'a self,
        id: i64,
    ) -> RuntimeTelegramDeliveryFuture<'a, Option<RuntimeTelegramUpdateInboxItemData>> {
        Box::pin(async move {
            self.inbox
                .item(id)
                .await
                .map(|item| item.map(runtime_inbox_item))
                .map_err(|error| error.to_string())
        })
    }

    fn update_inbox_items<'a>(
        &'a self,
        filter: RuntimeTelegramDeliveryListFilter,
    ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramUpdateInboxItemData>> {
        Box::pin(async move {
            self.inbox
                .list_items(
                    positive_limit(filter.limit),
                    filter.before_id,
                    filter.state.as_deref(),
                )
                .await
                .map(|items| items.into_iter().map(runtime_inbox_item).collect())
                .map_err(|error| error.to_string())
        })
    }

    fn update_inbox_attempts<'a>(
        &'a self,
        inbox_id: i64,
        limit: i32,
    ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramUpdateAttemptData>> {
        Box::pin(async move {
            self.inbox
                .attempts(inbox_id, positive_limit(limit))
                .await
                .map(|items| items.into_iter().map(runtime_update_attempt).collect())
                .map_err(|error| error.to_string())
        })
    }

    fn outbox_stats<'a>(
        &'a self,
    ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramOutboxStatsData> {
        Box::pin(async move {
            self.outbox
                .stats()
                .await
                .map(runtime_outbox_stats)
                .map_err(|error| error.to_string())
        })
    }

    fn outbox_item<'a>(
        &'a self,
        operation_id: &'a str,
    ) -> RuntimeTelegramDeliveryFuture<'a, Option<RuntimeTelegramOutboxItemData>> {
        Box::pin(async move {
            self.outbox
                .item(operation_id)
                .await
                .map(|item| item.map(runtime_outbox_item))
                .map_err(|error| error.to_string())
        })
    }

    fn outbox_items<'a>(
        &'a self,
        filter: RuntimeTelegramDeliveryListFilter,
    ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramOutboxItemData>> {
        Box::pin(async move {
            self.outbox
                .list_items(
                    positive_limit(filter.limit),
                    filter.before_id,
                    filter.state.as_deref(),
                )
                .await
                .map(|items| items.into_iter().map(runtime_outbox_item).collect())
                .map_err(|error| error.to_string())
        })
    }

    fn outbox_attempts<'a>(
        &'a self,
        outbox_id: i64,
        limit: i32,
    ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramOutboxAttemptData>> {
        Box::pin(async move {
            self.outbox
                .attempts(outbox_id, positive_limit(limit))
                .await
                .map(|items| items.into_iter().map(runtime_outbox_attempt).collect())
                .map_err(|error| error.to_string())
        })
    }

    fn retry_outbox<'a>(
        &'a self,
        request: RuntimeTelegramOutboxRetryRequest,
    ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramOutboxMutationResultData> {
        Box::pin(async move {
            self.outbox
                .retry_operation_manually(&request.operation_id, request.accept_duplicate_risk)
                .await
                .map_err(|error| error.to_string())?;
            let state = self
                .outbox
                .item(&request.operation_id)
                .await
                .map_err(|error| error.to_string())?
                .map(|item| item.state);
            Ok(RuntimeTelegramOutboxMutationResultData {
                operation_id: request.operation_id,
                changed: true,
                state,
            })
        })
    }

    fn cancel_outbox<'a>(
        &'a self,
        operation_id: String,
    ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramOutboxMutationResultData> {
        Box::pin(async move {
            let changed = self
                .outbox
                .cancel_operation(&operation_id, "cancelled through runtime API")
                .await
                .map_err(|error| error.to_string())?;
            let state = self
                .outbox
                .item(&operation_id)
                .await
                .map_err(|error| error.to_string())?
                .map(|item| item.state);
            Ok(RuntimeTelegramOutboxMutationResultData {
                operation_id,
                changed,
                state,
            })
        })
    }
}

fn runtime_inbox_stats(stats: TelegramUpdateInboxStats) -> RuntimeTelegramUpdateInboxStatsData {
    RuntimeTelegramUpdateInboxStatsData {
        pending: stats.pending,
        processing: stats.processing,
        retry_wait: stats.retry_wait,
        completed: stats.completed,
        ignored: stats.ignored,
        dead_letter: stats.dead_letter,
        payload_conflicts: stats.payload_conflicts,
        quarantined: stats.quarantined,
        total_deliveries: stats.total_deliveries,
        oldest_pending_at: format_optional_time(stats.oldest_pending_at),
        oldest_retry_at: format_optional_time(stats.oldest_retry_at),
        oldest_lease_expiry: format_optional_time(stats.oldest_lease_expiry),
    }
}

fn runtime_inbox_item(item: TelegramUpdateInboxItem) -> RuntimeTelegramUpdateInboxItemData {
    RuntimeTelegramUpdateInboxItemData {
        id: item.id,
        bot_id: item.bot_id,
        update_id: item.update_id,
        schema_version: i32::from(item.schema_version),
        source: item.source,
        stream_ms: item.stream_ms,
        stream_seq: item.stream_seq,
        last_stream_ms: item.last_stream_ms,
        last_stream_seq: item.last_stream_seq,
        payload_size_bytes: item.payload_size_bytes,
        payload_sha256: hex::encode(item.payload_sha256),
        payload_conflict: item.payload_conflict,
        update_type: item.update_type,
        telegram_event_at: format_optional_time(item.telegram_event_at),
        first_received_at: format_time(item.first_received_at),
        last_received_at: format_time(item.last_received_at),
        materialized_at: format_time(item.materialized_at),
        delivery_count: item.delivery_count,
        ordering_key: item.ordering_key,
        priority: item.priority,
        chat_id: item.chat_id,
        thread_id: item.thread_id,
        user_id: item.user_id,
        status: item.status,
        available_at: format_time(item.available_at),
        attempt_count: item.attempt_count,
        lease_owner: item.lease_owner,
        leased_until: format_optional_time(item.leased_until),
        processing_started_at: format_optional_time(item.processing_started_at),
        state_applied_at: format_optional_time(item.state_applied_at),
        handler_completed_at: format_optional_time(item.handler_completed_at),
        completed_at: format_optional_time(item.completed_at),
        outcome: item.outcome,
        ignored_reason: item.ignored_reason,
        last_error_class: item.last_error_class,
        last_error: item.last_error,
        created_at: format_time(item.created_at),
        updated_at: format_time(item.updated_at),
    }
}

fn runtime_update_attempt(item: TelegramUpdateAttempt) -> RuntimeTelegramUpdateAttemptData {
    RuntimeTelegramUpdateAttemptData {
        attempt: item.attempt,
        lease_token: item.lease_token,
        worker_id: item.worker_id,
        claimed_at: format_time(item.claimed_at),
        state_started_at: format_optional_time(item.state_started_at),
        state_completed_at: format_optional_time(item.state_completed_at),
        handler_started_at: format_optional_time(item.handler_started_at),
        handler_completed_at: format_optional_time(item.handler_completed_at),
        finished_at: format_optional_time(item.finished_at),
        outcome: item.outcome,
        error_class: item.error_class,
        error: item.error,
    }
}

fn runtime_outbox_stats(stats: TelegramOutboxStats) -> RuntimeTelegramOutboxStatsData {
    RuntimeTelegramOutboxStatsData {
        pending: stats.pending,
        leased: stats.leased,
        retry_wait: stats.retry_wait,
        delivered: stats.delivered,
        ambiguous: stats.ambiguous,
        dead_letter: stats.dead_letter,
        expired: stats.expired,
        cancelled: stats.cancelled,
        protected_unresolved: stats.protected_unresolved,
        oldest_pending_at: format_optional_time(stats.oldest_pending_at),
        oldest_lease_expiry: format_optional_time(stats.oldest_lease_expiry),
    }
}

fn runtime_outbox_item(item: TelegramOutboxItem) -> RuntimeTelegramOutboxItemData {
    RuntimeTelegramOutboxItemData {
        id: item.id,
        operation_id: item.operation_id,
        batch_id: item.batch_id,
        part_index: item.part_index,
        bot_id: item.bot_id,
        chat_id: item.chat_id,
        thread_id: item.thread_id,
        ordering_key: item.ordering_key,
        causation_update_id: item.causation_update_id,
        dialog_job_id: item.dialog_job_id,
        trigger_message_id: item.trigger_message_id,
        method_kind: item.method_kind,
        delivery_policy: item.delivery_policy,
        protected: item.protected,
        priority: item.priority,
        state: item.state,
        available_at: format_time(item.available_at),
        expires_at: format_optional_time(item.expires_at),
        attempt_count: item.attempt_count,
        lease_owner: item.lease_owner,
        leased_until: format_optional_time(item.leased_until),
        last_error_class: item.last_error_class,
        last_error: item.last_error,
        response_kind: item.response_kind,
        telegram_message_ids: item.telegram_message_ids,
        has_receipt: item.receipt.is_some(),
        confirmed_at: format_optional_time(item.confirmed_at),
        created_at: format_time(item.created_at),
        updated_at: format_time(item.updated_at),
    }
}

fn runtime_outbox_attempt(item: TelegramOutboxAttempt) -> RuntimeTelegramOutboxAttemptData {
    RuntimeTelegramOutboxAttemptData {
        attempt: item.attempt,
        lease_token: item.lease_token,
        worker_id: item.worker_id,
        claimed_at: format_time(item.claimed_at),
        request_started_at: format_optional_time(item.request_started_at),
        response_received_at: format_optional_time(item.response_received_at),
        finished_at: format_optional_time(item.finished_at),
        outcome: item.outcome,
        http_status: item.http_status,
        latency_ms: item.latency_ms,
        error_class: item.error_class,
        error: item.error,
    }
}

fn positive_limit(value: i32) -> usize {
    usize::try_from(value.max(1)).unwrap_or(1)
}

fn format_time(value: OffsetDateTime) -> String {
    value
        .format(&Rfc3339)
        .unwrap_or_else(|_| value.unix_timestamp().to_string())
}

fn format_optional_time(value: Option<OffsetDateTime>) -> Option<String> {
    value.map(format_time)
}
