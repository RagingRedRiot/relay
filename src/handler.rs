
use std::{net::SocketAddr, sync::Arc};

use axum::{Router, extract::{ConnectInfo, State, WebSocketUpgrade}, http::{StatusCode, header}, response::{Html, IntoResponse}, routing::{any, get}};
use axum_extra::{TypedHeader, headers};
use tokio_util::sync::CancellationToken;
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};
use crate::server::handle_socket;

#[derive(Clone)]
pub struct AppState {
    pub(crate) shutdown: CancellationToken
}

pub(crate) struct RateLimitConfig {
    pub(crate) per_second: u64,
    pub(crate) burst: u32
}

pub(crate) async fn app(token: CancellationToken, rl: RateLimitConfig) -> axum::Router {

    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(rl.per_second)
            .burst_size(rl.burst)
            .finish()
            .unwrap()
    );

    let state = AppState { shutdown: token.clone() };

    Router::new()
        .route("/", get(index))
        .route("/health", get(ok))
        .route("/script.js", get(script))
        .route("/favicon.ico", get(no_content))
        .route("/ws", any(ws_handler))
        .layer(GovernorLayer::new(governor_conf))
        .with_state(state)
}

async fn ws_handler(
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

async fn index() -> Html<&'static str> {                                         
    Html(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/index.html")))
}

async fn script() -> impl IntoResponse {                                         
    (           
        [(header::CONTENT_TYPE, "application/javascript")],
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/script.js")),  
    )                                                                            
}

async fn no_content() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn ok() -> StatusCode {
    StatusCode::OK
}