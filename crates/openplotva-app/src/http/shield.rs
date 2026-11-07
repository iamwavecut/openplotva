use super::HttpRouterState;
use super::auth::{admin_auth_failure_response, require_admin_request};
use super::common::{
    admin_error_response, admin_json_response, admin_required_i64, admin_time_json,
    parse_admin_optional_bool, parse_admin_positive_i32, query_values as admin_auth_query_values,
};
use crate::memory_runtime;
use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use openplotva_storage::PostgresShieldStore;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ShieldHttpState {
    admin_ids: Arc<[i64]>,
    bot_token: Arc<str>,
    shield_embedder: Option<Arc<dyn memory_runtime::EmbeddingProvider>>,
    shield_options: openplotva_shield::Options,
    shield_store: Option<PostgresShieldStore>,
}

impl axum::extract::FromRef<Arc<HttpRouterState>> for ShieldHttpState {
    fn from_ref(routes: &Arc<HttpRouterState>) -> Self {
        Self {
            admin_ids: routes.admin_ids.clone(),
            bot_token: routes.bot_token.clone(),
            shield_embedder: routes.shield_embedder.clone(),
            shield_options: routes.shield_options.clone(),
            shield_store: routes.shield_store.clone(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_state(routes: &HttpRouterState) -> ShieldHttpState {
    use axum::extract::FromRef as _;
    ShieldHttpState::from_ref(&Arc::new(routes.clone()))
}

pub(crate) async fn admin_shield_documents(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<ShieldHttpState>,
    body: Bytes,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    let body = body.as_ref();
    match method {
        Method::GET => admin_shield_documents_list_response(routes, headers, raw_query).await,
        Method::POST => admin_shield_document_create_response(routes, headers, body).await,
        Method::PUT => {
            admin_shield_document_update_response(routes, headers, raw_query, body).await
        }
        Method::DELETE => admin_shield_document_delete_response(routes, headers, raw_query).await,
        _ => admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

pub(crate) async fn admin_shield_embeddings_rebuild(
    method: Method,
    headers: HeaderMap,
    State(routes): State<ShieldHttpState>,
) -> Response {
    let headers = &headers;
    let routes = &routes;
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes.shield_store.as_ref() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "shield disabled");
    };
    let Some(embedder) = routes.shield_embedder.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let documents = match store
        .documents_without_embeddings(routes.shield_options.rebuild_batch_size)
        .await
    {
        Ok(documents) => documents,
        Err(error) => {
            tracing::warn!(%error, "failed to list shield documents without embeddings");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    let mut processed = 0_i32;
    let mut failed = 0_i32;
    for document in documents {
        processed += 1;
        let embedding = match embedder
            .embed_one(
                document.title.trim(),
                routes.shield_options.embedding_dim,
                openplotva_shield::TITLE_EMBEDDING_TASK,
            )
            .await
        {
            Ok(embedding) => embedding,
            Err(error) => {
                failed += 1;
                tracing::warn!(
                    %error,
                    document_id = document.id,
                    "shield title embedding rebuild failed"
                );
                continue;
            }
        };
        if let Err(error) = store
            .update_embedding(document.id, embedding.as_ref())
            .await
        {
            failed += 1;
            tracing::warn!(
                %error,
                document_id = document.id,
                "shield embedding update failed"
            );
        }
    }
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({ "ok": true, "processed": processed, "failed": failed }),
    )
}

pub(crate) async fn admin_shield_test(
    method: Method,
    headers: HeaderMap,
    State(routes): State<ShieldHttpState>,
    body: Bytes,
) -> Response {
    let headers = &headers;
    let routes = &routes;
    let body = body.as_ref();
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes.shield_store.as_ref() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "shield disabled");
    };
    let Ok(req) = serde_json::from_slice::<AdminShieldTestRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "invalid json");
    };
    if req.query.trim().is_empty() {
        return admin_error_response(StatusCode::BAD_REQUEST, "query required");
    }
    let request = openplotva_shield::SearchRequest {
        query: req.query.clone(),
        max_matches: req.max_matches,
        include_candidates: req.debug,
    };
    let embedding = admin_shield_embed_query(routes, &req.query).await;
    match store
        .search_with_vector(&request, &routes.shield_options, embedding.as_ref())
        .await
    {
        Ok(result) => admin_json_response(
            StatusCode::OK,
            admin_shield_test_json(&result, &req.expected_category),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to test admin shield query");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

#[cfg(test)]
pub(crate) async fn admin_shield_documents_response(
    routes: &ShieldHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    admin_shield_documents(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
        Bytes::copy_from_slice(body),
    )
    .await
}

pub(crate) async fn admin_shield_documents_list_response(
    routes: &ShieldHttpState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes.shield_store.as_ref() else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "count": 0, "documents": [] }),
        );
    };
    let values = admin_auth_query_values(raw_query);
    let filter = openplotva_shield::ListFilter {
        query: values.get("q").cloned().unwrap_or_default(),
        include_disabled: parse_admin_optional_bool(
            values.get("include_disabled").map(String::as_str),
        )
        .unwrap_or(false),
        limit: parse_shield_limit(values.get("limit").map(String::as_str)),
        offset: 0,
    };
    match store.list_documents(&filter).await {
        Ok(documents) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "count": documents.len(),
                "documents": documents.iter().map(admin_shield_document_json).collect::<Vec<_>>(),
            }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to list admin shield documents");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_shield_document_create_response(
    routes: &ShieldHttpState,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes.shield_store.as_ref() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "shield disabled");
    };
    let Ok(req) = serde_json::from_slice::<AdminShieldDocumentRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "invalid json");
    };
    let input =
        match openplotva_shield::normalize_document_input(admin_shield_document_input(req, true)) {
            Ok(input) => input,
            Err(error) => return admin_error_response(StatusCode::BAD_REQUEST, &error.to_string()),
        };
    let embedding = admin_shield_embed_title(routes, &input.title).await;
    match store.create_document(input, embedding.as_ref()).await {
        Ok(document) => admin_json_response(StatusCode::OK, admin_shield_document_json(&document)),
        Err(error) => {
            tracing::warn!(%error, "failed to create admin shield document");
            admin_error_response(StatusCode::BAD_REQUEST, &error.to_string())
        }
    }
}

pub(crate) async fn admin_shield_document_update_response(
    routes: &ShieldHttpState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes.shield_store.as_ref() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "shield disabled");
    };
    let Ok(req) = serde_json::from_slice::<AdminShieldDocumentRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "invalid json");
    };
    let values = admin_auth_query_values(raw_query);
    let mut id = req.id;
    if let Some(raw_id) = values
        .get("id")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let Ok(parsed) = raw_id.parse::<i64>() else {
            return admin_error_response(StatusCode::BAD_REQUEST, "invalid id");
        };
        if parsed <= 0 {
            return admin_error_response(StatusCode::BAD_REQUEST, "invalid id");
        }
        id = parsed;
    }
    if id <= 0 {
        return admin_error_response(StatusCode::BAD_REQUEST, "id required");
    }
    let input = match openplotva_shield::normalize_document_input(admin_shield_document_input(
        req, false,
    )) {
        Ok(input) => input,
        Err(error) => return admin_error_response(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let embedding = admin_shield_embed_title(routes, &input.title).await;
    match store.update_document(id, input, embedding.as_ref()).await {
        Ok(document) => admin_json_response(StatusCode::OK, admin_shield_document_json(&document)),
        Err(error) => {
            tracing::warn!(%error, id, "failed to update admin shield document");
            admin_error_response(StatusCode::BAD_REQUEST, &error.to_string())
        }
    }
}

pub(crate) async fn admin_shield_document_delete_response(
    routes: &ShieldHttpState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let id = match admin_required_i64(&values, "id", "id required", "id required") {
        Ok(value) if value > 0 => value,
        Ok(_) => return admin_error_response(StatusCode::BAD_REQUEST, "id required"),
        Err(response) => return *response,
    };
    if let Some(store) = routes.shield_store.as_ref()
        && let Err(error) = store.delete_document(id).await
    {
        tracing::warn!(%error, id, "failed to delete admin shield document");
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }
    admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true }))
}

#[cfg(test)]
pub(crate) async fn admin_shield_embeddings_rebuild_response(
    routes: &ShieldHttpState,
    method: Method,
    headers: &HeaderMap,
) -> Response {
    admin_shield_embeddings_rebuild(method, headers.clone(), State(routes.clone())).await
}

#[cfg(test)]
pub(crate) async fn admin_shield_test_response(
    routes: &ShieldHttpState,
    method: Method,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    admin_shield_test(
        method,
        headers.clone(),
        State(routes.clone()),
        Bytes::copy_from_slice(body),
    )
    .await
}

pub(crate) fn admin_shield_document_input(
    req: AdminShieldDocumentRequest,
    default_enabled: bool,
) -> openplotva_shield::DocumentInput {
    openplotva_shield::DocumentInput {
        slug: req.slug,
        title: req.title,
        body: req.body,
        category: req.category,
        enabled: req.enabled.unwrap_or(default_enabled),
        priority: req.priority,
    }
}

pub(crate) async fn admin_shield_embed_title(
    routes: &ShieldHttpState,
    title: &str,
) -> Option<openplotva_storage::PgEmbeddingVector> {
    let embedder = routes.shield_embedder.as_ref()?;
    match embedder
        .embed_one(
            title.trim(),
            routes.shield_options.embedding_dim,
            openplotva_shield::TITLE_EMBEDDING_TASK,
        )
        .await
    {
        Ok(embedding) => embedding,
        Err(error) => {
            tracing::warn!(%error, "shield title embedding failed; storing lexical-only document");
            None
        }
    }
}

pub(crate) async fn admin_shield_embed_query(
    routes: &ShieldHttpState,
    query: &str,
) -> Option<openplotva_storage::PgEmbeddingVector> {
    let embedder = routes.shield_embedder.as_ref()?;
    match embedder
        .embed_one(
            query,
            routes.shield_options.embedding_dim,
            openplotva_shield::QUERY_EMBEDDING_TASK,
        )
        .await
    {
        Ok(embedding) => embedding,
        Err(error) => {
            tracing::warn!(%error, "shield embedding failed; using lexical-only retrieval");
            None
        }
    }
}

pub(crate) fn parse_shield_limit(raw: Option<&str>) -> i32 {
    parse_admin_positive_i32(raw, 100, 500)
}

pub(crate) fn admin_shield_category_matched(
    matches: &[openplotva_shield::Match],
    expected: &str,
) -> bool {
    let expected = expected.trim();
    if expected.is_empty() {
        return false;
    }
    matches
        .iter()
        .any(|item| item.document.category.trim().eq_ignore_ascii_case(expected))
}

pub(crate) fn admin_shield_document_json(
    document: &openplotva_shield::Document,
) -> serde_json::Value {
    serde_json::json!({
        "id": document.id,
        "slug": document.slug,
        "title": document.title,
        "body": document.body,
        "category": document.category,
        "enabled": document.enabled,
        "priority": document.priority,
        "created_at": admin_time_json(document.created_at),
        "updated_at": admin_time_json(document.updated_at),
    })
}

pub(crate) fn admin_shield_scored_document_json(
    item: &openplotva_shield::ScoredDocument,
) -> serde_json::Value {
    serde_json::json!({
        "document": admin_shield_document_json(&item.document),
        "lexical_score": item.lexical_score,
        "vector_score": item.vector_score,
    })
}

pub(crate) fn admin_shield_candidate_json(
    candidate: &openplotva_shield::Candidate,
) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    value.insert(
        "document".to_owned(),
        admin_shield_document_json(&candidate.document),
    );
    value.insert(
        "lexical_score".to_owned(),
        serde_json::json!(candidate.lexical_score),
    );
    value.insert(
        "vector_score".to_owned(),
        serde_json::json!(candidate.vector_score),
    );
    value.insert("matched".to_owned(), serde_json::json!(candidate.matched));
    if !candidate.signals.is_empty() {
        value.insert("signals".to_owned(), serde_json::json!(candidate.signals));
    }
    serde_json::Value::Object(value)
}

pub(crate) fn admin_shield_test_json(
    result: &openplotva_shield::SearchResult,
    expected_category: &str,
) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    value.insert("query".to_owned(), serde_json::json!(result.query));
    value.insert(
        "matches".to_owned(),
        serde_json::json!(
            result
                .matches
                .iter()
                .map(admin_shield_scored_document_json)
                .collect::<Vec<_>>()
        ),
    );
    value.insert("context".to_owned(), serde_json::json!(result.context));
    value.insert(
        "lexical_only".to_owned(),
        serde_json::json!(result.lexical_only),
    );
    if !result.candidates.is_empty() {
        value.insert(
            "candidates".to_owned(),
            serde_json::json!(
                result
                    .candidates
                    .iter()
                    .map(admin_shield_candidate_json)
                    .collect::<Vec<_>>()
            ),
        );
    }
    if let Some(debug) = result.debug.as_ref() {
        value.insert(
            "debug".to_owned(),
            serde_json::json!({
                "max_matches": debug.max_matches,
                "candidate_limit": debug.candidate_limit,
                "lexical_min_score": debug.lexical_min_score,
                "vector_min_score": debug.vector_min_score,
            }),
        );
    }
    let expected = expected_category.trim();
    if !expected.is_empty() {
        value.insert("expected_category".to_owned(), serde_json::json!(expected));
        value.insert(
            "category_matched".to_owned(),
            serde_json::json!(admin_shield_category_matched(&result.matches, expected)),
        );
    }
    serde_json::Value::Object(value)
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct AdminShieldDocumentRequest {
    #[serde(default)]
    pub(crate) id: i64,
    #[serde(default)]
    pub(crate) slug: String,
    #[serde(default)]
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) body: String,
    #[serde(default)]
    pub(crate) category: String,
    #[serde(default)]
    pub(crate) enabled: Option<bool>,
    #[serde(default)]
    pub(crate) priority: i32,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct AdminShieldTestRequest {
    #[serde(default)]
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) expected_category: String,
    #[serde(default)]
    pub(crate) max_matches: i32,
    #[serde(default)]
    pub(crate) debug: bool,
}
