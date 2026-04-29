#[tokio::test]
async fn test_rate_limit_rejects_over_burst() {
    dotenvy::dotenv().ok();
    // Import the database URL
    let mut config = relay::config::Config::from_env().expect("invalid config");

    // Overwrite non-URL configurations for this test
    config.rate_limit_per_second = 1;
    config.rate_limit_burst = 5;

    let shutdown = tokio_util::sync::CancellationToken::new();
    let auth_handle = relay::auth::testing::spawn_test(
        shutdown.clone(),
        relay::auth::testing::default_test_users(),
    );

    let app = relay::app::app(shutdown.clone(), auth_handle, config).await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/health");

    let mut statuses = Vec::new();
    for _ in 0..20 {
        let resp = client.get(&url).send().await.unwrap();
        statuses.push(resp.status().as_u16());
    }

    assert_eq!(
        statuses[0], 200,
        "first request blocked — limiter misconfigured"
    );
    assert!(
        statuses.iter().any(|&s| s == 429),
        "expected at least one 429, got {:?}",
        statuses
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_websocket_limiter() {
    use futures_util::{SinkExt, StreamExt};
    use relay::model::{ClientCommand, ServerEvent};
    use tokio::time::Duration;
    use tungstenite::Message;

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

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientCommand::Auth {
            username: "alice".to_owned(),
            password: "alicepass".to_owned(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    // Spam Flood
    for _ in 1..50 {
        let msg = tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientCommand::Echo {
                string: "SPAM".to_owned(),
            })
            .unwrap()
            .into(),
        );
        if ws.send(msg).await.is_err() {
            break;
        };
    }

    // Drain
    let mut received: Vec<Message> = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(result) = ws.next().await {
            match result {
                Ok(msg) => received.push(msg),
                Err(_) => break,
            }
        }
    })
    .await;

    // Parse JSON Responses
    let parsed: Vec<ServerEvent> = received
        .iter()
        .filter_map(|m| m.to_text().ok())
        .filter_map(|s| serde_json::from_str::<ServerEvent>(s).ok())
        .collect();

    // Count RateLimit responses
    let rate_limit_count = parsed
        .iter()
        .filter(|c| matches!(c, ServerEvent::RateLimit { .. }))
        .count();

    // Count Echos
    let echo_count = parsed
        .iter()
        .filter(|c| matches!(c, ServerEvent::Echo { .. }))
        .count();

    let close_frame = received.iter().any(|m| matches!(m, Message::Close(_)));

    assert_eq!(
        rate_limit_count, 3,
        "expected excatly 3 RATELIMIT warnings, got {}: {:?}",
        rate_limit_count, parsed
    );

    assert!(
        echo_count >= 15,
        "expected at least 15 echos before limited, got {}",
        echo_count
    );

    assert!(
        close_frame,
        "expected server to send close after violating rate limits"
    );

    shutdown.cancel();
}
