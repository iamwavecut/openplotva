use std::time::{Duration, Instant};

use futures_util::TryStreamExt as _;
use openplotva_server::{
    RuntimeSqlReadRequest, RuntimeSqlReadResult, RuntimeSqlReader, RuntimeSqlReaderFuture,
};
use serde_json::Value;
use sqlx::{
    AssertSqlSafe, Column as _, Executor as _, PgPool, Postgres, Row as _, SqlSafeStr as _,
    TypeInfo as _, ValueRef as _, query::Query, types::Json,
};

type PgQuery<'q> = Query<'q, Postgres, <Postgres as sqlx::Database>::Arguments>;

#[derive(Clone)]
pub struct PostgresRuntimeSqlReader {
    pool: PgPool,
}

impl PostgresRuntimeSqlReader {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn read_inner(
        &self,
        request: RuntimeSqlReadRequest,
    ) -> Result<RuntimeSqlReadResult, RuntimeSqlReadError> {
        let sql = validate_runtime_sql_read(&request.sql)?;
        let timeout_ms = positive_or_default(request.timeout_ms, 5_000);
        let row_limit = positive_or_default(request.row_limit, 200);
        let result_bytes_limit = positive_or_default(request.result_bytes_limit, 256 * 1024);
        let started = Instant::now();

        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "SET LOCAL statement_timeout = {timeout_ms}"
        )))
        .execute(&mut *tx)
        .await?;

        let columns = describe_columns(&mut tx, &sql).await.unwrap_or_default();
        let mut result = if is_explain_sql(&sql) {
            execute_direct_sql(&mut tx, &sql, &request.args, row_limit, result_bytes_limit).await?
        } else {
            execute_wrapped_select_sql(&mut tx, &sql, &request.args, row_limit, result_bytes_limit)
                .await?
        };
        if !columns.is_empty() {
            result.columns = columns;
        }
        tx.commit().await?;
        result.elapsed_ms = elapsed_millis_i32(started);
        Ok(result)
    }
}

impl RuntimeSqlReader for PostgresRuntimeSqlReader {
    fn read<'a>(&'a self, request: RuntimeSqlReadRequest) -> RuntimeSqlReaderFuture<'a> {
        Box::pin(async move {
            let timeout_ms = positive_or_default(request.timeout_ms, 5_000) as u64;
            match tokio::time::timeout(Duration::from_millis(timeout_ms), self.read_inner(request))
                .await
            {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(error)) => Err(error.to_string()),
                Err(_) => Err("sql read timed out".to_owned()),
            }
        })
    }
}

async fn describe_columns(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    sql: &str,
) -> Result<Vec<String>, sqlx::Error> {
    Ok((&mut **tx)
        .describe(AssertSqlSafe(sql.to_owned()).into_sql_str())
        .await?
        .columns()
        .iter()
        .map(|column| column.name().to_owned())
        .collect())
}

async fn execute_wrapped_select_sql(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    sql: &str,
    args: &[Value],
    row_limit: i32,
    result_bytes_limit: i32,
) -> Result<RuntimeSqlReadResult, RuntimeSqlReadError> {
    let scan_limit = row_limit.saturating_add(1).max(1);
    let wrapped_sql = format!(
        "SELECT row_to_json(openplotva_runtime_sql_row)::text AS __openplotva_row \
         FROM ({sql}) AS openplotva_runtime_sql_row LIMIT {scan_limit}"
    );
    let query = bind_runtime_sql_args(sqlx::query(AssertSqlSafe(wrapped_sql)), args);
    let mut rows = query.fetch(&mut **tx);
    let mut result = RuntimeSqlReadResult {
        columns: Vec::new(),
        rows: Vec::new(),
        row_count: 0,
        elapsed_ms: 0,
        truncated: false,
    };
    let mut total_bytes = 0_usize;

    while let Some(row) = rows.try_next().await? {
        let row_json: String = row.try_get("__openplotva_row")?;
        let row_value = serde_json::from_str::<Value>(&row_json)
            .map_err(|source| RuntimeSqlReadError::RowJson { source })?;
        if result.columns.is_empty() {
            result.columns = row_columns_from_value(&row_value);
        }
        let row_bytes = serde_json::to_vec(&row_value)
            .map_err(|source| RuntimeSqlReadError::RowJson { source })?
            .len();
        if result.rows.len() >= row_limit as usize
            || total_bytes.saturating_add(row_bytes) > result_bytes_limit as usize
        {
            result.truncated = true;
            break;
        }
        result.rows.push(row_value);
        total_bytes += row_bytes;
    }

    result.row_count = result.rows.len() as i32;
    Ok(result)
}

async fn execute_direct_sql(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    sql: &str,
    args: &[Value],
    row_limit: i32,
    result_bytes_limit: i32,
) -> Result<RuntimeSqlReadResult, RuntimeSqlReadError> {
    let query = bind_runtime_sql_args(sqlx::query(AssertSqlSafe(sql.to_owned())), args);
    let mut rows = query.fetch(&mut **tx);
    let mut result = RuntimeSqlReadResult {
        columns: Vec::new(),
        rows: Vec::new(),
        row_count: 0,
        elapsed_ms: 0,
        truncated: false,
    };
    let mut total_bytes = 0_usize;

    while let Some(row) = rows.try_next().await? {
        if result.columns.is_empty() {
            result.columns = row
                .columns()
                .iter()
                .map(|column| column.name().to_owned())
                .collect();
        }
        let mut row_map = serde_json::Map::new();
        for (index, column) in row.columns().iter().enumerate() {
            row_map.insert(
                column.name().to_owned(),
                postgres_value_to_json(&row, index, column.type_info().name())?,
            );
        }
        let row_value = Value::Object(row_map);
        let row_bytes = serde_json::to_vec(&row_value)
            .map_err(|source| RuntimeSqlReadError::RowJson { source })?
            .len();
        if result.rows.len() >= row_limit as usize
            || total_bytes.saturating_add(row_bytes) > result_bytes_limit as usize
        {
            result.truncated = true;
            break;
        }
        result.rows.push(row_value);
        total_bytes += row_bytes;
    }

    result.row_count = result.rows.len() as i32;
    Ok(result)
}

fn bind_runtime_sql_args<'q>(mut query: PgQuery<'q>, args: &'q [Value]) -> PgQuery<'q> {
    for arg in args {
        query = match arg {
            Value::Null => query.bind(None::<String>),
            Value::Bool(value) => query.bind(*value),
            Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    query.bind(value)
                } else if let Some(value) =
                    value.as_u64().and_then(|value| i64::try_from(value).ok())
                {
                    query.bind(value)
                } else if let Some(value) = value.as_f64() {
                    query.bind(value)
                } else {
                    query.bind(value.to_string())
                }
            }
            Value::String(value) => query.bind(value),
            Value::Array(_) | Value::Object(_) => query.bind(arg.to_string()),
        };
    }
    query
}

fn postgres_value_to_json(
    row: &sqlx::postgres::PgRow,
    index: usize,
    type_name: &str,
) -> Result<Value, RuntimeSqlReadError> {
    if row.try_get_raw(index)?.is_null() {
        return Ok(Value::Null);
    }

    match type_name {
        "BOOL" => Ok(Value::Bool(row.try_get(index)?)),
        "INT2" => Ok(Value::from(row.try_get::<i16, _>(index)?)),
        "INT4" => Ok(Value::from(row.try_get::<i32, _>(index)?)),
        "INT8" => Ok(Value::from(row.try_get::<i64, _>(index)?)),
        "FLOAT4" => Ok(Value::from(row.try_get::<f32, _>(index)?)),
        "FLOAT8" => Ok(Value::from(row.try_get::<f64, _>(index)?)),
        "JSON" | "JSONB" => Ok(row.try_get::<Json<Value>, _>(index)?.0),
        "BYTEA" => {
            let bytes = row.try_get::<Vec<u8>, _>(index)?;
            if let Ok(decoded) = serde_json::from_slice::<Value>(&bytes) {
                Ok(decoded)
            } else {
                Ok(Value::String(String::from_utf8_lossy(&bytes).into_owned()))
            }
        }
        _ => Ok(Value::String(row.try_get::<String, _>(index)?)),
    }
}

fn row_columns_from_value(value: &Value) -> Vec<String> {
    value
        .as_object()
        .map(|row| row.keys().cloned().collect())
        .unwrap_or_default()
}

fn elapsed_millis_i32(started: Instant) -> i32 {
    i32::try_from(started.elapsed().as_millis()).unwrap_or(i32::MAX)
}

fn positive_or_default(value: i32, default: i32) -> i32 {
    if value > 0 { value } else { default }
}

fn is_explain_sql(sql: &str) -> bool {
    first_runtime_sql_token(sql).as_deref() == Some("explain")
}

fn validate_runtime_sql_read(sql: &str) -> Result<String, RuntimeSqlReadError> {
    let sql = single_runtime_sql_statement(sql)?;
    let tokens = runtime_sql_tokens(&sql);
    let Some(first) = tokens.first().map(String::as_str) else {
        return Err(RuntimeSqlReadError::SqlRequired);
    };

    match first {
        "select" => validate_select_tokens(&tokens)?,
        "with" => validate_with_tokens(&tokens)?,
        "explain" => validate_explain_tokens(&tokens)?,
        _ => return Err(RuntimeSqlReadError::OnlyReadOnly),
    }
    Ok(sql)
}

fn validate_select_tokens(tokens: &[String]) -> Result<(), RuntimeSqlReadError> {
    if let Some(into_index) = tokens.iter().position(|token| token == "into") {
        let from_index = tokens.iter().position(|token| token == "from");
        if from_index.is_none_or(|from_index| into_index < from_index) {
            return Err(RuntimeSqlReadError::SelectInto);
        }
    }
    Ok(())
}

fn validate_with_tokens(tokens: &[String]) -> Result<(), RuntimeSqlReadError> {
    let mut index = 1;
    if tokens.get(index).map(String::as_str) == Some("recursive") {
        index += 1;
    }

    while let Some(name) = tokens.get(index) {
        let cte_name = name.clone();
        index += 1;
        if tokens.get(index).map(String::as_str) == Some("(") {
            while tokens.get(index).map(String::as_str) != Some(")") {
                index += 1;
                if index >= tokens.len() {
                    return Err(RuntimeSqlReadError::OnlyReadOnly);
                }
            }
            index += 1;
        }
        if tokens.get(index).map(String::as_str) != Some("as") {
            return Err(RuntimeSqlReadError::OnlyReadOnly);
        }
        index += 1;
        if tokens.get(index).map(String::as_str) != Some("(") {
            return Err(RuntimeSqlReadError::OnlyReadOnly);
        }
        index += 1;
        let cte_first = tokens
            .get(index)
            .map(String::as_str)
            .ok_or(RuntimeSqlReadError::OnlyReadOnly)?;
        if !matches!(cte_first, "select" | "with" | "explain") {
            return Err(RuntimeSqlReadError::UnsafeCte {
                name: cte_name,
                reason: "only SELECT, WITH ... SELECT, and EXPLAIN without ANALYZE are allowed"
                    .to_owned(),
            });
        }

        let mut depth = 1_i32;
        while depth > 0 {
            index += 1;
            match tokens.get(index).map(String::as_str) {
                Some("(") => depth += 1,
                Some(")") => depth -= 1,
                Some(_) => {}
                None => return Err(RuntimeSqlReadError::OnlyReadOnly),
            }
        }
        index += 1;
        if tokens.get(index).map(String::as_str) == Some(",") {
            index += 1;
            continue;
        }
        break;
    }

    match tokens.get(index).map(String::as_str) {
        Some("select") => validate_select_tokens(&tokens[index..]),
        _ => Err(RuntimeSqlReadError::OnlyReadOnly),
    }
}

fn validate_explain_tokens(tokens: &[String]) -> Result<(), RuntimeSqlReadError> {
    if tokens.iter().any(|token| token == "analyze") {
        return Err(RuntimeSqlReadError::ExplainAnalyze);
    }
    if !tokens
        .iter()
        .any(|token| matches!(token.as_str(), "select" | "with"))
    {
        return Err(RuntimeSqlReadError::OnlyReadOnly);
    }
    Ok(())
}

fn first_runtime_sql_token(sql: &str) -> Option<String> {
    runtime_sql_tokens(sql).into_iter().next()
}

fn single_runtime_sql_statement(sql: &str) -> Result<String, RuntimeSqlReadError> {
    let mut statement_end = None;
    let mut scanner = RuntimeSqlScanner::new(sql);
    while let Some(token) = scanner.next_token() {
        if token == ";" {
            statement_end = Some(scanner.position());
            break;
        }
    }

    if let Some(end) = statement_end {
        if !sql[end..].trim().is_empty() {
            return Err(RuntimeSqlReadError::MultipleStatements);
        }
        let trimmed = sql[..end.saturating_sub(1)].trim();
        if trimmed.is_empty() {
            return Err(RuntimeSqlReadError::SqlRequired);
        }
        return Ok(trimmed.to_owned());
    }

    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(RuntimeSqlReadError::SqlRequired);
    }
    Ok(trimmed.to_owned())
}

fn runtime_sql_tokens(sql: &str) -> Vec<String> {
    let mut scanner = RuntimeSqlScanner::new(sql);
    let mut tokens = Vec::new();
    while let Some(token) = scanner.next_token() {
        tokens.push(token);
    }
    tokens
}

struct RuntimeSqlScanner<'a> {
    sql: &'a str,
    bytes: &'a [u8],
    index: usize,
}

impl<'a> RuntimeSqlScanner<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            bytes: sql.as_bytes(),
            index: 0,
        }
    }

    fn position(&self) -> usize {
        self.index
    }

    fn next_token(&mut self) -> Option<String> {
        loop {
            self.skip_ws_and_comments();
            if self.index >= self.bytes.len() {
                return None;
            }

            let byte = self.bytes[self.index];
            match byte {
                b'\'' => {
                    self.skip_single_quoted();
                    continue;
                }
                b'"' => {
                    self.skip_double_quoted();
                    continue;
                }
                b'$' if self.skip_dollar_quoted() => continue,
                b'(' | b')' | b',' | b';' => {
                    self.index += 1;
                    return Some((byte as char).to_string());
                }
                _ if is_ident_start(byte) => {
                    let start = self.index;
                    self.index += 1;
                    while self
                        .bytes
                        .get(self.index)
                        .is_some_and(|byte| is_ident_continue(*byte))
                    {
                        self.index += 1;
                    }
                    return Some(self.sql[start..self.index].to_ascii_lowercase());
                }
                _ => self.index += 1,
            }
        }
    }

    fn skip_ws_and_comments(&mut self) {
        loop {
            while self
                .bytes
                .get(self.index)
                .is_some_and(|byte| byte.is_ascii_whitespace())
            {
                self.index += 1;
            }
            if self.bytes.get(self.index..self.index + 2) == Some(b"--") {
                self.index += 2;
                while self
                    .bytes
                    .get(self.index)
                    .is_some_and(|byte| *byte != b'\n')
                {
                    self.index += 1;
                }
                continue;
            }
            if self.bytes.get(self.index..self.index + 2) == Some(b"/*") {
                self.index += 2;
                while self.index + 1 < self.bytes.len()
                    && self.bytes.get(self.index..self.index + 2) != Some(b"*/")
                {
                    self.index += 1;
                }
                self.index = (self.index + 2).min(self.bytes.len());
                continue;
            }
            break;
        }
    }

    fn skip_single_quoted(&mut self) {
        self.index += 1;
        while self.index < self.bytes.len() {
            match self.bytes[self.index] {
                b'\'' if self.bytes.get(self.index + 1) == Some(&b'\'') => self.index += 2,
                b'\'' => {
                    self.index += 1;
                    break;
                }
                _ => self.index += 1,
            }
        }
    }

    fn skip_double_quoted(&mut self) {
        self.index += 1;
        while self.index < self.bytes.len() {
            match self.bytes[self.index] {
                b'"' if self.bytes.get(self.index + 1) == Some(&b'"') => self.index += 2,
                b'"' => {
                    self.index += 1;
                    break;
                }
                _ => self.index += 1,
            }
        }
    }

    fn skip_dollar_quoted(&mut self) -> bool {
        let Some(tag_end_offset) = self.bytes[self.index + 1..]
            .iter()
            .position(|byte| *byte == b'$')
        else {
            return false;
        };
        let tag_end = self.index + 1 + tag_end_offset;
        if !self.bytes[self.index + 1..tag_end]
            .iter()
            .all(|byte| is_ident_continue(*byte))
        {
            return false;
        }
        let tag = &self.sql[self.index..=tag_end];
        self.index = tag_end + 1;
        if let Some(close_offset) = self.sql[self.index..].find(tag) {
            self.index += close_offset + tag.len();
        } else {
            self.index = self.bytes.len();
        }
        true
    }
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[derive(Debug, thiserror::Error)]
enum RuntimeSqlReadError {
    #[error("sql is required")]
    SqlRequired,
    #[error("only a single statement is allowed")]
    MultipleStatements,
    #[error("only SELECT, WITH ... SELECT, and EXPLAIN without ANALYZE are allowed")]
    OnlyReadOnly,
    #[error("SELECT INTO is not allowed")]
    SelectInto,
    #[error("EXPLAIN ANALYZE is not allowed")]
    ExplainAnalyze,
    #[error("unsafe CTE {name:?}: {reason}")]
    UnsafeCte { name: String, reason: String },
    #[error("execute read-only sql: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("marshal sql row: {source}")]
    RowJson { source: serde_json::Error },
}

#[cfg(test)]
mod tests {
    use super::{runtime_sql_tokens, single_runtime_sql_statement, validate_runtime_sql_read};

    #[test]
    fn runtime_sql_validator_matches_go_allow_list() {
        for sql in [
            "select 1",
            "with items as (select 1 as id) select * from items",
            "select 1 union select 2",
            "explain select 1",
            "select ';' as semi;",
        ] {
            let result = validate_runtime_sql_read(sql)
                .unwrap_or_else(|error| panic!("expected {sql:?} to pass, got {error}"));
            assert!(!result.ends_with(';'));
        }
    }

    #[test]
    fn runtime_sql_validator_matches_go_rejections() {
        for (sql, want) in [
            (" ", "sql is required"),
            ("select 1; select 2", "only a single statement is allowed"),
            ("delete from users", "only SELECT"),
            ("select 1 into temp test", "SELECT INTO is not allowed"),
            ("explain analyze select 1", "EXPLAIN ANALYZE is not allowed"),
            (
                "with removed as (delete from users returning *) select * from removed",
                "unsafe CTE \"removed\"",
            ),
        ] {
            let error = validate_runtime_sql_read(sql)
                .expect_err("unsafe runtime SQL should be rejected")
                .to_string();
            assert!(
                error.contains(want),
                "validate_runtime_sql_read({sql:?}) = {error:?}, want {want:?}"
            );
        }
    }

    #[test]
    fn runtime_sql_tokenizer_ignores_comments_and_strings() {
        assert_eq!(
            runtime_sql_tokens("/* delete */ select '-- ;' as value"),
            vec!["select", "as", "value"]
        );
        assert_eq!(
            single_runtime_sql_statement("select $$;$$ as value;")
                .unwrap_or_else(|error| panic!("single statement failed: {error}")),
            "select $$;$$ as value"
        );
    }
}
