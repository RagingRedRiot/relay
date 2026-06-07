use std::sync::Arc;

use axum::{
    Router,
    routing::{any, get},
};
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};
use tower_http::services::{ServeDir, ServeFile};

use crate::{
    auth::AuthHandle,
    config::Config,
    control::ServerControl,
    handler::{embedded_asset, ok, ws_handler},
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

    // Extract before config is moved into Arc.
    let frontend_dir = config.frontend_dir.clone();

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

    let router = Router::new()
        .route("/health", get(ok))
        .route("/ws", any(ws_handler))
        .layer(GovernorLayer::new(governor_conf))
        .with_state(state);

    // When FRONTEND_DIR is set, serve from the filesystem (community or dev
    // override). Otherwise serve the embedded default frontend. Both paths fall
    // back to index.html for unmatched routes so client-side routing works.
    if let Some(dir) = frontend_dir {
        router
            .fallback_service(ServeDir::new(&dir).fallback(ServeFile::new(dir.join("index.html"))))
    } else {
        router.fallback(embedded_asset)
    }
}
