use std::sync::Arc;

use axum::{
    Router,
    routing::{any, get},
};
use tokio_util::sync::CancellationToken;
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};

use crate::{
    auth::AuthHandle,
    handler::{RateLimitConfig, index, no_content, ok, script, ws_handler},
};

#[derive(Clone)]
pub struct AppState {
    pub(crate) shutdown: CancellationToken,
    pub(crate) auth_handle: AuthHandle,
}

pub(crate) async fn app(token: CancellationToken, rl: RateLimitConfig) -> axum::Router {
    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(rl.per_second)
            .burst_size(rl.burst)
            .finish()
            .unwrap(),
    );

    let state = AppState {
        shutdown: token.clone(),
        auth_handle: crate::auth::spawn(token.clone()),
    };

    Router::new()
        .route("/", get(index))
        .route("/health", get(ok))
        .route("/script.js", get(script))
        .route("/favicon.ico", get(no_content))
        .route("/ws", any(ws_handler))
        .layer(GovernorLayer::new(governor_conf))
        .with_state(state)
}
