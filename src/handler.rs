use std::net::SocketAddr;

use crate::{app::AppState, server::handle_socket};
use axum::{
    extract::{ConnectInfo, State, WebSocketUpgrade},
    http::{StatusCode, header},
    response::{Html, IntoResponse},
};
use axum_extra::{
    TypedHeader,
    headers::{self},
};

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

    // Pin both the message and frame size caps to the configured max chunk payload
    // (plus the frame header) so the limit a client learns from GetMaxChunkSize is
    // the real one -- not undercut by tungstenite's smaller default frame cap. A
    // chunk over this is dropped by the transport (connection closed), so a
    // well-behaved client queries the limit and stays under it.
    let frame_cap = state.config.max_chunk_bytes + crate::attachment::CHUNK_HEADER_LEN;
    ws.max_message_size(frame_cap)
        .max_frame_size(frame_cap)
        .on_upgrade(move |socket| handle_socket(socket, state, addr))
}

pub(crate) async fn index() -> Html<&'static str> {
    Html(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/assets/index.html"
    )))
}

pub(crate) async fn script() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/javascript")],
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/script.js")),
    )
}

pub(crate) async fn no_content() -> StatusCode {
    StatusCode::NO_CONTENT
}

pub(crate) async fn ok() -> StatusCode {
    StatusCode::OK
}
