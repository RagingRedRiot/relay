use std::net::SocketAddr;
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
    let auth_handle = relay::auth::spawn(shutdown.clone());

    let app = app(shutdown.clone(), auth_handle, config).await;

    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown_signal().await;
            shutdown.cancel();
        }
    });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

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
