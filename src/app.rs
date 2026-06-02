use std::sync::Arc;

use axum::{
    Router,
    routing::{any, get},
};
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};

use crate::{
    auth::AuthHandle,
    config::Config,
    control::ServerControl,
    handler::{index, no_content, ok, script, ws_handler},
    hub::Hub,
    message::{self, MessageHandle},
    room::{self, RoomHandle},
    user::{self, UserHandle},
};

#[derive(Clone)]
pub struct AppState {
    pub(crate) shutdown: CancellationToken,
    pub(crate) config: Arc<Config>,
    pub(crate) auth_handle: AuthHandle,
    pub(crate) user_handle: UserHandle,
    pub(crate) room_handle: RoomHandle,
    pub(crate) message_handle: MessageHandle,
    pub(crate) pool: PgPool,
    pub(crate) write_semaphore: Arc<Semaphore>,
    pub(crate) download_semaphore: Arc<Semaphore>,
    pub(crate) hub: Hub,
    // Lifecycle control: lets an authorized in-app action (the admin RestartServer /
    // ShutdownServer commands) ask the supervisor in `main` to restart or shut the
    // process down. Reachable wherever AppState is.
    pub(crate) control: ServerControl,
}

pub async fn app(
    shutdown: CancellationToken,
    auth_handle: AuthHandle,
    config: Config,
    pool: PgPool,
    control: ServerControl,
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
    let hub = Hub::new();
    let user_handle = user::spawn(shutdown.clone(), pool.clone()).await;
    let room_handle = room::spawn(shutdown.clone(), pool.clone(), hub.clone()).await;
    let message_handle = message::spawn(shutdown.clone(), pool.clone(), hub.clone()).await;

    let write_semaphore = Arc::new(Semaphore::new(
        crate::attachment::MAX_CONCURRENT_CHUNK_WRITES,
    ));
    let download_semaphore = Arc::new(Semaphore::new(
        crate::attachment::MAX_CONCURRENT_CHUNK_READS,
    ));

    let state = AppState {
        shutdown: shutdown.clone(),
        config: Arc::new(config),
        auth_handle,
        user_handle,
        room_handle,
        message_handle,
        pool,
        write_semaphore,
        download_semaphore,
        hub,
        control,
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
