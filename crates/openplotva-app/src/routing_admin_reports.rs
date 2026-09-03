use std::cmp::Ordering;

use openplotva_storage::llm_routing::{
    RoutingAdminIncidentGroup, RoutingAdminIncidentSample, RoutingAdminIncidentSnapshot,
    RoutingAdminReportState,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};

pub const MAX_TELEGRAM_REPORT_BYTES: usize = 3_900;
const MAX_RENDERED_GROUPS: usize = 5;
const REPORT_WINDOW: Duration = Duration::seconds(60 * 60);
const ACTIVE_FAILURE_AGE: Duration = Duration::seconds(5 * 60);
const DELIVERY_RETRY_FLOOR: Duration = Duration::seconds(5 * 60);
const STALE_PENDING_AGE: Duration = Duration::seconds(10 * 60);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormattedIncidentDigest {
    pub text: String,
    pub fingerprint: String,
    pub latest_occurrence: Option<OffsetDateTime>,
    pub has_incidents: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminReportDeliveryPlan {
    None,
    Send,
    Edit { message_id: i64 },
}

#[must_use]
pub fn format_incident_digest(
    snapshot: &RoutingAdminIncidentSnapshot,
    now: OffsetDateTime,
) -> FormattedIncidentDigest {
    if snapshot.groups.is_empty() {
        return formatted_digest(
            "🟢 LLM: за последние 60 минут сбоев нет\n\
             Отчёт обновлён после восстановления. Новых сообщений не будет до следующего сбоя."
                .to_owned(),
            None,
            false,
        );
    }

    let mut groups = snapshot.groups.clone();
    groups.sort_by(compare_incident_groups);
    let latest_occurrence = groups.iter().map(|group| group.last_seen).max();
    let user_impact = groups.iter().any(user_facing_group_has_context);
    let status = match latest_occurrence {
        Some(last_seen) if now - last_seen <= ACTIVE_FAILURE_AGE && user_impact => {
            "🔴 LLM: сбои затрагивают пользователей"
        }
        Some(last_seen) if now - last_seen <= ACTIVE_FAILURE_AGE => {
            "🟠 LLM: деградируют фоновые пайплайны"
        }
        _ => "🟡 LLM: новых сбоев нет, наблюдаем",
    };

    let mut text = format!(
        "{status}\nЗа 60 мин · события: {} · пользователи: {} · чаты: {} · задачи: {}",
        grouped_number(snapshot.total_occurrences),
        grouped_number(snapshot.affected_users),
        grouped_number(snapshot.affected_chats),
        grouped_number(snapshot.affected_jobs),
    );
    if let Some(last_seen) = latest_occurrence {
        text.push_str(&format!("\nПоследний сбой: {} UTC", time_of_day(last_seen)));
    }

    let total_groups = snapshot.total_groups.max(groups.len() as i64).max(0) as usize;
    let mut rendered = 0usize;
    for group in groups.iter().take(MAX_RENDERED_GROUPS) {
        let section = format_incident_group(group, rendered + 1);
        if text.len().saturating_add(section.len()).saturating_add(160) > MAX_TELEGRAM_REPORT_BYTES
        {
            break;
        }
        text.push_str("\n\n");
        text.push_str(&section);
        rendered += 1;
    }

    let omitted = total_groups.saturating_sub(rendered);
    if omitted > 0 {
        let footer = format!(
            "\n\nЕщё {} {} — Runtime API → routingEvents",
            grouped_number(omitted as i64),
            russian_count_word(omitted as i64, "группа", "группы", "групп")
        );
        push_within(&mut text, &footer, MAX_TELEGRAM_REPORT_BYTES);
    }

    formatted_digest(text, latest_occurrence, true)
}

#[must_use]
pub fn plan_admin_report_delivery(
    state: &RoutingAdminReportState,
    digest: &FormattedIncidentDigest,
    now: OffsetDateTime,
) -> AdminReportDeliveryPlan {
    if !digest.has_incidents && state.telegram_message_id.is_none() {
        return AdminReportDeliveryPlan::None;
    }
    if state
        .pending_started_at
        .is_some_and(|started| started > now - STALE_PENDING_AGE)
    {
        return AdminReportDeliveryPlan::None;
    }
    if state.last_rendered_fingerprint.as_deref() == Some(digest.fingerprint.as_str()) {
        return AdminReportDeliveryPlan::None;
    }
    if state.last_delivery_error_class.is_some()
        && state
            .last_delivery_attempt_at
            .is_some_and(|attempted| attempted > now - DELIVERY_RETRY_FLOOR)
    {
        return AdminReportDeliveryPlan::None;
    }

    let Some(message_id) = state.telegram_message_id else {
        if !digest.has_incidents
            || state
                .last_new_message_at
                .is_some_and(|sent| sent > now - REPORT_WINDOW)
        {
            return AdminReportDeliveryPlan::None;
        }
        return AdminReportDeliveryPlan::Send;
    };

    if digest.has_incidents
        && let (Some(last_new), Some(latest)) =
            (state.last_new_message_at, digest.latest_occurrence)
        && now >= last_new + REPORT_WINDOW
        && latest >= last_new + REPORT_WINDOW
    {
        return AdminReportDeliveryPlan::Send;
    }
    AdminReportDeliveryPlan::Edit { message_id }
}

fn formatted_digest(
    text: String,
    latest_occurrence: Option<OffsetDateTime>,
    has_incidents: bool,
) -> FormattedIncidentDigest {
    let fingerprint = hex::encode(Sha256::digest(text.as_bytes()));
    FormattedIncidentDigest {
        text,
        fingerprint,
        latest_occurrence,
        has_incidents,
    }
}

fn compare_incident_groups(
    left: &RoutingAdminIncidentGroup,
    right: &RoutingAdminIncidentGroup,
) -> Ordering {
    user_facing_group_has_context(right)
        .cmp(&user_facing_group_has_context(left))
        .then_with(|| severity_rank(&right.severity).cmp(&severity_rank(&left.severity)))
        .then_with(|| right.occurrences.cmp(&left.occurrences))
        .then_with(|| right.last_seen.cmp(&left.last_seen))
        .then_with(|| left.dedupe_key.cmp(&right.dedupe_key))
}

fn user_facing_group_has_context(group: &RoutingAdminIncidentGroup) -> bool {
    is_user_facing_workflow(&group.workflow_key)
        && (group.affected_users > 0 || group.affected_chats > 0 || group.affected_jobs > 0)
}

fn is_user_facing_workflow(workflow: &str) -> bool {
    workflow == "dialog"
        || workflow == "vision"
        || workflow == "asr"
        || workflow == "youtube_summary"
        || workflow == "music_generation"
        || workflow.starts_with("image_generation")
        || workflow.starts_with("image_edit")
        || workflow.starts_with("agentic_")
}

fn severity_rank(severity: &str) -> u8 {
    match severity {
        "critical" => 5,
        "error" => 4,
        "warn" => 3,
        "info" => 2,
        _ => 1,
    }
}

fn format_incident_group(group: &RoutingAdminIncidentGroup, index: usize) -> String {
    let workflow = bounded_dynamic(&group.workflow_key, 80);
    let mut lines = vec![format!(
        "{index}. {} · {workflow} · {}",
        operation_label(group),
        grouped_number(group.occurrences)
    )];

    let reasons = formatted_reasons(&group.reason_counts);
    if !reasons.is_empty() {
        lines.push(format!("Причина: {reasons}"));
    }
    if let Some(route) = formatted_route(group) {
        lines.push(format!("Маршрут: {route}"));
    }
    if group.affected_users > 0 || group.affected_chats > 0 || group.affected_jobs > 0 {
        let label = if is_user_facing_workflow(&group.workflow_key) {
            "Затронуто"
        } else {
            "Связано"
        };
        lines.push(format!(
            "{label}: {} польз. · {} чатов · {} задач",
            grouped_number(group.affected_users),
            grouped_number(group.affected_chats),
            grouped_number(group.affected_jobs)
        ));
    }
    let samples = group
        .samples
        .iter()
        .filter_map(format_context_sample)
        .take(3)
        .collect::<Vec<_>>();
    if !samples.is_empty() {
        lines.push(format!("Контекст: {}", samples.join("; ")));
    }
    lines.push(format!(
        "Период: {}–{} UTC",
        time_of_day(group.first_seen),
        time_of_day(group.last_seen)
    ));
    lines.join("\n")
}

fn operation_label(group: &RoutingAdminIncidentGroup) -> &'static str {
    match group.event_type.as_str() {
        "route_unavailable" => "Маршрут не настроен",
        "no_candidates" => "Нет подходящей модели",
        "circuit_open_exhaustion" => "Все маршруты в cooldown",
        "capacity_unavailable" => "Нет свободной мощности",
        "router_reload_failed" => "Перезагрузка маршрутов",
        "routing_backfill_failed" => "Синхронизация маршрутов",
        "all_attempts_exhausted" => match group.workflow_key.as_str() {
            "dialog" => "Ответ в диалоге",
            "memory_extraction" | "memory_subject_merge" => "Обработка памяти",
            "vision" => "Распознавание изображения",
            "asr" => "Распознавание речи",
            "music_generation" => "Генерация музыки",
            workflow if workflow.starts_with("image_generation") => "Генерация изображения",
            workflow if workflow.starts_with("image_edit") => "Редактирование изображения",
            workflow if workflow.starts_with("agentic_") => "AI-планирование",
            _ => "Все попытки исчерпаны",
        },
        _ => "Сбой LLM",
    }
}

fn formatted_reasons(value: &Value) -> String {
    let Some(object) = value.as_object() else {
        return String::new();
    };
    let mut reasons = object
        .iter()
        .map(|(reason, count)| (reason.as_str(), count.as_i64().unwrap_or_default().max(0)))
        .collect::<Vec<_>>();
    reasons.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    let total = reasons.len();
    let mut rendered = reasons
        .into_iter()
        .take(2)
        .map(|(reason, count)| {
            let safe_reason = bounded_dynamic(reason, 180);
            let label = reason_label(reason);
            if label == reason {
                format!("{safe_reason} · {}", grouped_number(count))
            } else {
                format!("{label} ({safe_reason}) · {}", grouped_number(count))
            }
        })
        .collect::<Vec<_>>();
    if total > rendered.len() {
        rendered.push(format!("ещё {} причин", total - rendered.len()));
    }
    rendered.join("; ")
}

fn reason_label(reason: &str) -> &str {
    match reason {
        "provider_unavailable" => "провайдер недоступен",
        "provider_overloaded" => "провайдер перегружен",
        "capacity_unavailable" => "нет свободной мощности",
        "provider_protocol_error" => "некорректный ответ API",
        "rate_limited" | "provider_rate_limited" => "лимит запросов провайдера",
        "attempt_deadline_exceeded" | "deadline_exceeded" => "истёк срок запроса",
        "missing_route" => "маршрут отсутствует",
        "zero_selected_attempts" => "нет подходящего маршрута",
        _ => reason,
    }
}

fn formatted_route(group: &RoutingAdminIncidentGroup) -> Option<String> {
    let provider = group
        .provider_name
        .as_deref()
        .map(|value| bounded_dynamic(value, 100))
        .or_else(|| group.provider_id.map(|id| format!("provider #{id}")));
    let model = group
        .model_name
        .as_deref()
        .map(|value| bounded_dynamic(value, 120))
        .or_else(|| group.model_id.map(|id| format!("model #{id}")));
    match (provider, model) {
        (Some(provider), Some(model)) => Some(format!("{provider} → {model}")),
        (Some(provider), None) => Some(provider),
        (None, Some(model)) => Some(model),
        (None, None) => None,
    }
}

fn format_context_sample(sample: &RoutingAdminIncidentSample) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(user_id) = sample.user_id {
        let user = sample
            .user_username
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|username| format!("@{}", bounded_dynamic(username, 64)))
            .or_else(|| {
                sample
                    .user_name
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(|name| bounded_dynamic(name, 80))
            })
            .unwrap_or_else(|| "user".to_owned());
        parts.push(format!("{user} ({user_id})"));
    }
    if let Some(chat_id) = sample.chat_id {
        let chat = sample
            .chat_name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|name| format!("«{}»", bounded_dynamic(name, 80)))
            .or_else(|| {
                sample
                    .chat_username
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(|username| format!("@{}", bounded_dynamic(username, 64)))
            })
            .unwrap_or_else(|| "chat".to_owned());
        parts.push(format!("{chat} ({chat_id})"));
    }
    if let Some(job_id) = sample.job_id {
        parts.push(format!("job {job_id}"));
    }
    if let Some(thread_id) = sample.thread_id {
        parts.push(format!("topic {thread_id}"));
    }
    if let Some(message_id) = sample.message_id {
        parts.push(format!("msg {message_id}"));
    }
    (!parts.is_empty()).then(|| parts.join(", "))
}

fn bounded_dynamic(value: &str, max_bytes: usize) -> String {
    let redacted = openplotva_observability::secrets::redact(value);
    let compact = redacted.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() <= max_bytes {
        return compact;
    }
    let mut end = max_bytes.saturating_sub('…'.len_utf8()).min(compact.len());
    while !compact.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}…", &compact[..end])
}

fn push_within(text: &mut String, suffix: &str, max_bytes: usize) {
    let remaining = max_bytes.saturating_sub(text.len());
    if suffix.len() <= remaining {
        text.push_str(suffix);
        return;
    }
    let mut end = remaining.saturating_sub('…'.len_utf8()).min(suffix.len());
    while !suffix.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    text.push_str(&suffix[..end]);
    if remaining >= '…'.len_utf8() {
        text.push('…');
    }
}

fn time_of_day(value: OffsetDateTime) -> String {
    format!("{:02}:{:02}", value.hour(), value.minute())
}

fn grouped_number(value: i64) -> String {
    let value = value.max(0).to_string();
    let mut out = String::with_capacity(value.len() + value.len() / 3);
    for (index, ch) in value.chars().enumerate() {
        if index > 0 && (value.len() - index).is_multiple_of(3) {
            out.push(' ');
        }
        out.push(ch);
    }
    out
}

fn russian_count_word<'a>(value: i64, one: &'a str, few: &'a str, many: &'a str) -> &'a str {
    let value = value.unsigned_abs();
    if value % 100 / 10 == 1 {
        return many;
    }
    match value % 10 {
        1 => one,
        2..=4 => few,
        _ => many,
    }
}

#[cfg(test)]
mod tests {
    use openplotva_storage::llm_routing::{
        RoutingAdminIncidentGroup, RoutingAdminIncidentSample, RoutingAdminIncidentSnapshot,
        RoutingAdminReportOperationKind, RoutingAdminReportState,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::*;

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("timestamp")
    }

    fn group(
        dedupe_key: &str,
        workflow_key: &str,
        occurrences: i64,
        affected_users: i64,
        first_seen: i64,
        last_seen: i64,
    ) -> RoutingAdminIncidentGroup {
        RoutingAdminIncidentGroup {
            dedupe_key: dedupe_key.to_owned(),
            severity: "error".to_owned(),
            event_type: "all_attempts_exhausted".to_owned(),
            workflow_key: workflow_key.to_owned(),
            provider_id: Some(8),
            provider_name: Some("vram-cloud".to_owned()),
            model_id: Some(9),
            model_name: Some("vram.cloud/qwen3.6-27b".to_owned()),
            queue_name: Some("dialog".to_owned()),
            summary: "raw_prompt=must-not-leak".to_owned(),
            reason_counts: json!({"provider_overloaded": occurrences}),
            occurrences,
            affected_users,
            affected_chats: i64::from(affected_users > 0),
            affected_jobs: i64::from(affected_users > 0),
            first_seen: at(first_seen),
            last_seen: at(last_seen),
            samples: if affected_users > 0 {
                vec![RoutingAdminIncidentSample {
                    user_id: Some(42),
                    user_name: Some("Alice Example".to_owned()),
                    user_username: Some("alice".to_owned()),
                    chat_id: Some(-100123),
                    chat_name: Some("Plotva Lab".to_owned()),
                    chat_username: None,
                    job_id: Some(3501765),
                    thread_id: Some(9),
                    message_id: Some(77),
                }]
            } else {
                Vec::new()
            },
        }
    }

    fn snapshot(groups: Vec<RoutingAdminIncidentGroup>) -> RoutingAdminIncidentSnapshot {
        RoutingAdminIncidentSnapshot {
            total_occurrences: groups.iter().map(|group| group.occurrences).sum(),
            affected_users: groups.iter().map(|group| group.affected_users).sum(),
            affected_chats: groups.iter().map(|group| group.affected_chats).sum(),
            affected_jobs: groups.iter().map(|group| group.affected_jobs).sum(),
            total_groups: groups.len() as i64,
            groups,
        }
    }

    #[test]
    fn digest_prioritizes_user_impact_and_explains_the_failure() {
        let digest = format_incident_digest(
            &snapshot(vec![
                group("memory", "memory_extraction", 1_000, 0, 900, 995),
                group("dialog", "dialog", 12, 8, 930, 998),
            ]),
            at(1_000),
        );

        assert!(
            digest
                .text
                .starts_with("🔴 LLM: сбои затрагивают пользователей")
        );
        assert!(digest.text.contains("события: 1 012"));
        assert!(digest.text.contains("1. Ответ в диалоге · dialog · 12"));
        assert!(
            digest
                .text
                .contains("провайдер перегружен (provider_overloaded)")
        );
        assert!(digest.text.contains("vram-cloud → vram.cloud/qwen3.6-27b"));
        assert!(digest.text.contains("@alice (42)"));
        assert!(digest.text.contains("«Plotva Lab» (-100123)"));
        assert!(digest.text.contains("job 3501765"));
        assert!(
            digest
                .text
                .contains("2. Обработка памяти · memory_extraction · 1 000")
        );
        assert!(!digest.text.contains("raw_prompt"));
        assert_eq!(digest.latest_occurrence, Some(at(998)));
        assert!(digest.has_incidents);
    }

    #[test]
    fn digest_is_bounded_and_reports_omitted_groups() {
        let mut groups = Vec::new();
        for index in 0..30 {
            let mut item = group(
                &format!("group-{index}"),
                &format!("workflow_{index}_{}", "x".repeat(200)),
                100 - index,
                1,
                900,
                999,
            );
            item.reason_counts = json!({format!("reason-{index}-{}", "y".repeat(300)): 1});
            groups.push(item);
        }

        let digest = format_incident_digest(&snapshot(groups), at(1_000));

        assert!(digest.text.len() <= MAX_TELEGRAM_REPORT_BYTES);
        assert!(digest.text.contains("Ещё "));
        assert!(digest.text.contains("Runtime API → routingEvents"));
    }

    #[test]
    fn empty_snapshot_formats_recovered_state_without_incident_timestamp() {
        let digest = format_incident_digest(&RoutingAdminIncidentSnapshot::default(), at(1_000));

        assert_eq!(
            digest.text,
            "🟢 LLM: за последние 60 минут сбоев нет\n\
             Отчёт обновлён после восстановления. Новых сообщений не будет до следующего сбоя."
        );
        assert!(!digest.has_incidents);
        assert_eq!(digest.latest_occurrence, None);
    }

    #[test]
    fn first_active_digest_sends_once() {
        let digest = format_incident_digest(
            &snapshot(vec![group("dialog", "dialog", 1, 1, 999, 999)]),
            at(1_000),
        );
        let state = RoutingAdminReportState {
            admin_id: 42,
            ..RoutingAdminReportState::default()
        };

        assert_eq!(
            plan_admin_report_delivery(&state, &digest, at(1_000)),
            AdminReportDeliveryPlan::Send
        );
    }

    #[test]
    fn matching_fingerprint_is_a_noop() {
        let digest = format_incident_digest(
            &snapshot(vec![group("dialog", "dialog", 1, 1, 999, 999)]),
            at(1_000),
        );
        let state = RoutingAdminReportState {
            admin_id: 42,
            telegram_message_id: Some(77),
            last_new_message_at: Some(at(900)),
            last_rendered_fingerprint: Some(digest.fingerprint.clone()),
            ..RoutingAdminReportState::default()
        };

        assert_eq!(
            plan_admin_report_delivery(&state, &digest, at(1_000)),
            AdminReportDeliveryPlan::None
        );
    }

    #[test]
    fn changed_current_message_inside_the_hour_edits_it() {
        let digest = format_incident_digest(
            &snapshot(vec![group("dialog", "dialog", 2, 1, 999, 999)]),
            at(1_000),
        );
        let state = RoutingAdminReportState {
            admin_id: 42,
            telegram_message_id: Some(77),
            last_new_message_at: Some(at(900)),
            last_rendered_fingerprint: Some("old".to_owned()),
            ..RoutingAdminReportState::default()
        };

        assert_eq!(
            plan_admin_report_delivery(&state, &digest, at(1_000)),
            AdminReportDeliveryPlan::Edit { message_id: 77 }
        );
    }

    #[test]
    fn active_digest_rotates_only_after_a_newer_hour_of_incidents() {
        let state = RoutingAdminReportState {
            admin_id: 42,
            telegram_message_id: Some(77),
            last_new_message_at: Some(at(1_000)),
            last_rendered_fingerprint: Some("old".to_owned()),
            ..RoutingAdminReportState::default()
        };
        let before = format_incident_digest(
            &snapshot(vec![group("dialog", "dialog", 2, 1, 999, 4_599)]),
            at(4_600),
        );
        let boundary = format_incident_digest(
            &snapshot(vec![group("dialog", "dialog", 3, 1, 999, 4_600)]),
            at(4_600),
        );

        assert_eq!(
            plan_admin_report_delivery(&state, &before, at(4_600)),
            AdminReportDeliveryPlan::Edit { message_id: 77 }
        );
        assert_eq!(
            plan_admin_report_delivery(&state, &boundary, at(4_600)),
            AdminReportDeliveryPlan::Send
        );
    }

    #[test]
    fn lost_edit_target_never_bypasses_the_hourly_send_gate() {
        let digest = format_incident_digest(
            &snapshot(vec![group("dialog", "dialog", 1, 1, 999, 999)]),
            at(1_000),
        );
        let state = RoutingAdminReportState {
            admin_id: 42,
            telegram_message_id: None,
            last_new_message_at: Some(at(900)),
            last_rendered_fingerprint: Some("old".to_owned()),
            ..RoutingAdminReportState::default()
        };

        assert_eq!(
            plan_admin_report_delivery(&state, &digest, at(1_000)),
            AdminReportDeliveryPlan::None
        );
    }

    #[test]
    fn recent_failure_and_live_pending_operation_suppress_retries() {
        let digest = format_incident_digest(
            &snapshot(vec![group("dialog", "dialog", 1, 1, 999, 999)]),
            at(1_000),
        );
        let failed = RoutingAdminReportState {
            admin_id: 42,
            last_delivery_attempt_at: Some(at(900)),
            last_delivery_error_class: Some("retryable_transient".to_owned()),
            ..RoutingAdminReportState::default()
        };
        let pending = RoutingAdminReportState {
            admin_id: 42,
            pending_virtual_id: Some("routing-admin-report:send:42:1".to_owned()),
            pending_kind: Some(RoutingAdminReportOperationKind::Send),
            pending_fingerprint: Some("pending".to_owned()),
            pending_started_at: Some(at(900)),
            ..RoutingAdminReportState::default()
        };

        assert_eq!(
            plan_admin_report_delivery(&failed, &digest, at(1_000)),
            AdminReportDeliveryPlan::None
        );
        assert_eq!(
            plan_admin_report_delivery(&pending, &digest, at(1_000)),
            AdminReportDeliveryPlan::None
        );
    }

    #[test]
    fn stale_pending_operation_and_elapsed_retry_floor_can_recover() {
        let digest = format_incident_digest(
            &snapshot(vec![group("dialog", "dialog", 1, 1, 1_599, 1_599)]),
            at(1_600),
        );
        let state = RoutingAdminReportState {
            admin_id: 42,
            pending_virtual_id: Some("routing-admin-report:send:42:1".to_owned()),
            pending_kind: Some(RoutingAdminReportOperationKind::Send),
            pending_fingerprint: Some("pending".to_owned()),
            pending_started_at: Some(at(999)),
            last_delivery_attempt_at: Some(at(1_299)),
            last_delivery_error_class: Some("retryable_transient".to_owned()),
            ..RoutingAdminReportState::default()
        };

        assert_eq!(
            plan_admin_report_delivery(&state, &digest, at(1_600)),
            AdminReportDeliveryPlan::Send
        );
    }

    #[test]
    fn recovered_digest_edits_existing_message_but_never_creates_one() {
        let digest = format_incident_digest(&RoutingAdminIncidentSnapshot::default(), at(1_000));
        let existing = RoutingAdminReportState {
            admin_id: 42,
            telegram_message_id: Some(77),
            last_new_message_at: Some(at(900)),
            last_rendered_fingerprint: Some("active".to_owned()),
            ..RoutingAdminReportState::default()
        };
        let absent = RoutingAdminReportState {
            admin_id: 42,
            ..RoutingAdminReportState::default()
        };

        assert_eq!(
            plan_admin_report_delivery(&existing, &digest, at(1_000)),
            AdminReportDeliveryPlan::Edit { message_id: 77 }
        );
        assert_eq!(
            plan_admin_report_delivery(&absent, &digest, at(1_000)),
            AdminReportDeliveryPlan::None
        );
    }
}
