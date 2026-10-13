//! Filtered, paged reads of persisted taskman jobs for operator surfaces.
//!
//! The in-memory queue only holds active jobs and a short tail of finished ones,
//! so listings read `taskman_jobs`, which keeps finished jobs until retention.

use std::collections::BTreeMap;

use sqlx::{Postgres, QueryBuilder, Row};
use time::OffsetDateTime;

use crate::{PostgresTaskQueueStore, StorageError, task_queue_record_from_row};
use openplotva_taskman::TaskQueueRecord;

/// Statuses whose jobs the in-memory queue always holds.
pub const TASK_QUEUE_ACTIVE_STATUSES: [&str; 3] = ["pending", "processing", "waiting_delivery"];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TaskQueueTimeColumn {
    #[default]
    CreatedAt,
    StartedAt,
    CompletedAt,
}

impl TaskQueueTimeColumn {
    const fn column(self) -> &'static str {
        match self {
            Self::CreatedAt => "created_at",
            Self::StartedAt => "started_at",
            Self::CompletedAt => "completed_at",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TaskQueueSortColumn {
    Id,
    Priority,
    #[default]
    CreatedAt,
    StartedAt,
    CompletedAt,
}

impl TaskQueueSortColumn {
    const fn column(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Priority => "priority",
            Self::CreatedAt => "created_at",
            Self::StartedAt => "started_at",
            Self::CompletedAt => "completed_at",
        }
    }
}

/// Filter over persisted jobs. Empty lists and `None` mean "no constraint".
#[derive(Clone, Debug, Default)]
pub struct TaskQueueJobFilter {
    pub statuses: Vec<String>,
    pub queues: Vec<String>,
    pub user_id: Option<i64>,
    pub chat_id: Option<i64>,
    pub time_column: TaskQueueTimeColumn,
    pub from: Option<OffsetDateTime>,
    pub to: Option<OffsetDateTime>,
    /// Lowercase substring matched against queue, type, status, title, user name,
    /// and the payload text fields that job previews are built from.
    pub search: String,
}

#[derive(Clone, Debug, Default)]
pub struct TaskQueueJobPage {
    pub total: i64,
    pub by_status: BTreeMap<String, i64>,
    pub by_queue: BTreeMap<String, i64>,
    /// Matching records without their event log.
    pub records: Vec<TaskQueueRecord>,
}

const SEARCH_HAYSTACK: &str = "lower(concat_ws(' ', queue_name, job_type, status, \
     record #>> '{job,title}', \
     record #>> '{job,data,telegram_data,user_full_name}', \
     record #>> '{job,data,control_data,text}', \
     record #>> '{job,data,asr_data,file_unique_id}', \
     record #>> '{job,data,image_data,prompt}', \
     record #>> '{job,data,image_data,original_text}', \
     record #>> '{job,data,music_data,topic}', \
     record #>> '{job,data,music_data,lyrics}', \
     record #>> '{job,data,dialog_data,message_text}', \
     record #>> '{job,data,dialog_data,original_text}', \
     record #>> '{job,data,agent_data,goal}', \
     record #>> '{job,data,agent_data,profile_id}'))";

fn push_filter(builder: &mut QueryBuilder<Postgres>, filter: &TaskQueueJobFilter) {
    builder.push(" WHERE deleted_at IS NULL");
    if !filter.statuses.is_empty() {
        builder
            .push(" AND status = ANY(")
            .push_bind(filter.statuses.clone())
            .push(")");
    }
    if !filter.queues.is_empty() {
        builder
            .push(" AND queue_name = ANY(")
            .push_bind(filter.queues.clone())
            .push(")");
    }
    if let Some(user_id) = filter.user_id {
        builder
            .push(" AND COALESCE(user_id, 0) = ")
            .push_bind(user_id);
    }
    if let Some(chat_id) = filter.chat_id {
        builder
            .push(" AND COALESCE(chat_id, 0) = ")
            .push_bind(chat_id);
    }
    let time_column = filter.time_column.column();
    if let Some(from) = filter.from {
        builder
            .push(format_args!(" AND {time_column} >= "))
            .push_bind(from);
    }
    if let Some(to) = filter.to {
        builder
            .push(format_args!(" AND {time_column} <= "))
            .push_bind(to);
    }
    if !filter.search.is_empty() {
        builder
            .push(format_args!(" AND strpos({SEARCH_HAYSTACK}, "))
            .push_bind(filter.search.clone())
            .push(") > 0");
    }
}

impl PostgresTaskQueueStore {
    /// Page through persisted jobs with per-status and per-queue counts of all
    /// matches. Rows with an equal sort value are ordered by id in the same
    /// direction; missing timestamps sort last.
    pub async fn list_task_queue_jobs(
        &self,
        filter: &TaskQueueJobFilter,
        sort: TaskQueueSortColumn,
        descending: bool,
        offset: i64,
        limit: i64,
    ) -> Result<TaskQueueJobPage, StorageError> {
        let mut counts = QueryBuilder::<Postgres>::new(
            "SELECT status, queue_name, count(*) AS jobs FROM taskman_jobs",
        );
        push_filter(&mut counts, filter);
        counts.push(" GROUP BY status, queue_name");
        let mut page = TaskQueueJobPage::default();
        for row in counts.build().fetch_all(&self.pool).await? {
            let status: String = row.try_get("status")?;
            let queue_name: String = row.try_get("queue_name")?;
            let jobs: i64 = row.try_get("jobs")?;
            page.total += jobs;
            *page.by_status.entry(status).or_default() += jobs;
            *page.by_queue.entry(queue_name).or_default() += jobs;
        }
        if page.total == 0 || offset >= page.total {
            return Ok(page);
        }

        let direction = if descending { "DESC" } else { "ASC" };
        let mut items = QueryBuilder::<Postgres>::new(
            "SELECT id, record - 'events' AS record FROM taskman_jobs",
        );
        push_filter(&mut items, filter);
        items
            .push(format_args!(
                " ORDER BY {} {direction} NULLS LAST, id {direction} OFFSET ",
                sort.column()
            ))
            .push_bind(offset.max(0))
            .push(" LIMIT ")
            .push_bind(limit.max(0));
        for row in items.build().fetch_all(&self.pool).await? {
            let id: i64 = row.try_get("id")?;
            match task_queue_record_from_row(row) {
                Ok(record) => page.records.push(record),
                Err(error) => {
                    tracing::warn!(job_id = id, %error, "skipping undeserializable taskman job row");
                }
            }
        }
        Ok(page)
    }

    /// Soft-delete persisted finished jobs that match `filter`, except the ids the
    /// caller already deleted through the in-memory queue. Active jobs are never
    /// touched here because the in-memory queue owns them.
    pub async fn delete_finished_task_queue_jobs(
        &self,
        filter: &TaskQueueJobFilter,
        skip_ids: &[i64],
    ) -> Result<u64, StorageError> {
        let mut builder = QueryBuilder::<Postgres>::new(
            "WITH deleted AS (UPDATE taskman_jobs SET deleted_at = now(), updated_at = now()",
        );
        push_filter(&mut builder, filter);
        builder
            .push(" AND status <> ALL(")
            .push_bind(TASK_QUEUE_ACTIVE_STATUSES.map(str::to_owned).to_vec())
            .push(") AND id <> ALL(")
            .push_bind(skip_ids.to_vec())
            .push(
                ") RETURNING id, record) \
                 INSERT INTO taskman_job_history (job_id, op, record) \
                 SELECT id, ",
            )
            .push_bind(openplotva_taskman::TASK_QUEUE_WAL_DELETE_JOB)
            .push(", record FROM deleted");
        Ok(builder.build().execute(&self.pool).await?.rows_affected())
    }
}
