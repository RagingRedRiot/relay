use std::sync::Arc;

use axum::{
    Router,
    routing::{any, get},
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};

use crate::{
    auth::AuthHandle,
    config::Config,
    handler::{index, no_content, ok, script, ws_handler},
    user::{self, UserHandle},
};

#[derive(Clone)]
pub struct AppState {
    pub(crate) shutdown: CancellationToken,
    pub(crate) config: Arc<Config>,
    pub(crate) auth_handle: AuthHandle,
    pub(crate) user_handle: UserHandle,
}

pub async fn app(
    shutdown: CancellationToken,
    auth_handle: AuthHandle,
    config: Config,
    pool: PgPool,
) -> axum::Router {
    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(config.rate_limit_per_second)
            .burst_size(config.rate_limit_burst)
            .finish()
            .unwrap(),
    );

    // ACTOR SPAWN AND HANDLE
    // AuthHandle was spawned in calling function
    let user_handle = user::spawn(shutdown.clone(), pool).await;

    let state = AppState {
        shutdown: shutdown.clone(),
        config: Arc::new(config),
        auth_handle,
        user_handle,
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
