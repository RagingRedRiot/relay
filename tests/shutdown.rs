#[tokio::test]
async fn test_shutdown() {
    use futures_util::StreamExt;
    use std::time::Duration;
    use tokio::task::JoinSet;

    dotenvy::dotenv().ok();
    // Import the database URL
    let mut config = relay::config::Config::from_env().expect("invalid config");

    // Overwrite non-URL configurations for this test
    config.rate_limit_per_second = 10;
    config.rate_limit_burst = 15;

    // Build App - Run in Task
    let shutdown = tokio_util::sync::CancellationToken::new();
    let auth_handle = relay::auth::testing::spawn_test(
        shutdown.clone(),
        relay::auth::testing::default_test_users(),
    );

    let app = relay::app::app(shutdown.clone(), auth_handle, config).await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _ = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown({
                let shutdown = shutdown.clone();
                async move { shutdown.cancelled().await }
            })
            .await;
        }
    });

    let mut clients = JoinSet::new();

    for _ in 1..100 {
        let url = format!("ws://{addr}/ws");
        clients.spawn(async move {
            let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
            // Hold the connection open; exit when the server closes it.
            while let Some(msg) = ws.next().await {
                if msg.is_err() {
                    break;
                }
            }
        });
    }

    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(5), async {
        while clients.join_next().await.is_some() {}
    })
    .await
    .expect("clients did not shut down in time");
}
