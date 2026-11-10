//! PostgreSQL-only SQLx API shared with runtime adapters.

pub use sqlx_core::{
    acquire::Acquire,
    column::Column,
    connection::Connection,
    database::Database,
    error::{self, Error},
    executor::{Execute, Executor},
    from_row::FromRow,
    migrate,
    query::{self, query},
    query_as::query_as,
    query_builder::QueryBuilder,
    query_scalar::query_scalar,
    raw_sql::raw_sql,
    row::Row,
    sql_str::{AssertSqlSafe, SqlSafeStr, SqlStr},
    transaction::Transaction,
    type_info::TypeInfo,
    types,
    value::ValueRef,
};
pub use sqlx_postgres::{self as postgres, PgConnection, PgPool, Postgres};
