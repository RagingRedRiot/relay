use relay::auth;
use relay::model::{NewCredential, Password};
use relay::user::ensure_admin;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use relay::app::app;
use relay::config::Config;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let config = Config::from_env().expect("invalid config");

    // initialize tracing
    tracing_subscriber::fmt::init();

    let shutdown = CancellationToken::new();

    let pool = PgPool::connect(&config.database_url).await.unwrap();
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("database migrations failed");

    ensure_admin(
        pool.clone(),
        &config.admin_username,
        NewCredential {
            password: Password(config.admin_credential.to_owned()),
        },
    )
    .await
    .expect("failed to ensure default admin");

    let auth_handle = auth::spawn(shutdown.clone(), pool.clone()).await;

    relay::reaper::spawn(
        shutdown.clone(),
        pool.clone(),
        config.retention_days,
        Duration::from_secs(config.reap_interval_secs),
    );

    let listener = tokio::net::TcpListener::bind(&config.bind).await.unwrap();

    let app = app(shutdown.clone(), auth_handle, config, pool.clone()).await;

    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown_signal().await;
            shutdown.cancel();
        }
    });

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown({
        let shutdown = shutdown;
        async move { shutdown.cancelled().await }
    })
    .await
    .unwrap();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { println!("\nSHUTDOWN REQUESTED!") },
        _ = terminate => { println!("\nSHUTDOWN REQUESTED!") },
    }
}
