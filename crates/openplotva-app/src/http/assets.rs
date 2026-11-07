//! Embedded WebApp and admin assets exposed over HTTP.

use axum::{
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

pub(crate) fn static_web_asset_response(
    group: openplotva_web::StaticAssetGroup,
    path: &str,
) -> Response {
    let Some(asset) = openplotva_web::static_asset(group, path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, asset.content_type),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        asset.bytes,
    )
        .into_response()
}

pub(crate) fn moved_permanently(location: &'static str) -> Response {
    (
        StatusCode::MOVED_PERMANENTLY,
        [(header::LOCATION, location)],
    )
        .into_response()
}

pub(crate) fn found(location: &'static str) -> Response {
    (StatusCode::FOUND, [(header::LOCATION, location)]).into_response()
}
