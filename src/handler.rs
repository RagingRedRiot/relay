use std::net::SocketAddr;

use crate::{app::AppState, server::handle_socket};
use axum::{
    extract::{ConnectInfo, State, WebSocketUpgrade},
    http::{StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use axum_extra::{
    TypedHeader,
    headers::{self},
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "frontend/dist/"]
struct Assets;

pub(crate) async fn ws_handler(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    tracing::debug!(who = %addr, %user_agent, "connection accepted");

    let frame_cap = state.config.max_chunk_bytes + crate::attachment::CHUNK_HEADER_LEN;
    ws.max_message_size(frame_cap)
        .max_frame_size(frame_cap)
        .on_upgrade(move |socket| handle_socket(socket, state, addr))
}

pub(crate) async fn embedded_asset(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    serve_asset(path)
}

fn serve_asset(path: &str) -> Response {
    match Assets::get(path) {
        Some(file) => {
            let mime = file.metadata.mimetype();
            ([(header::CONTENT_TYPE, mime)], file.data.into_owned()).into_response()
        }
        None => match Assets::get("index.html") {
            Some(file) => {
                let mime = file.metadata.mimetype();
                ([(header::CONTENT_TYPE, mime)], file.data.into_owned()).into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        },
    }
}

pub(crate) async fn no_content() -> StatusCode {
    StatusCode::NO_CONTENT
}

pub(crate) async fn ok() -> StatusCode {
    StatusCode::OK
}
