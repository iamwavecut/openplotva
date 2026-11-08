//! Telegram webhook ingress and HTTP status mapping.
use axum::{
    body::Bytes,
    extract::Extension,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
#[derive(Clone)]
pub(crate) struct TelegramWebhookRoute {
    pub(crate) sender: openplotva_telegram::WebhookUpdateSender,
    pub(crate) secret_token: Arc<str>,
}

pub(crate) async fn telegram_webhook(
    Extension(route): Extension<TelegramWebhookRoute>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    telegram_webhook_response(&route.sender, method, headers, &route.secret_token, body).await
}

pub async fn telegram_webhook_response(
    sender: &openplotva_telegram::WebhookUpdateSender,
    method: Method,
    headers: HeaderMap,
    secret_token: &str,
    body: Bytes,
) -> Response {
    let provided_secret = headers
        .get(openplotva_telegram::TELEGRAM_WEBHOOK_SECRET_HEADER)
        .and_then(|value| value.to_str().ok());

    match sender
        .handle_webhook_request(method.as_str(), provided_secret, secret_token, &body)
        .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => {
            let status = StatusCode::from_u16(error.http_status())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            status.into_response()
        }
    }
}
