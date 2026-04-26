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

pub(crate) struct RateLimitConfig {
    pub(crate) per_second: u64,
    pub(crate) burst: u32,
}

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
    // TODO tracing
    println!("{user_agent} at {addr} connected.");
    ws.on_upgrade(move |socket| handle_socket(socket, state, addr))
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
