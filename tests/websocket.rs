#[tokio::test]
async fn test_websocket_communications() {
    use futures_util::{SinkExt, StreamExt};
    use relay::model::{ClientCommand, ServerEvent};

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

    let msg = ws.next().await.unwrap().unwrap();
    let event = serde_json::from_str::<ServerEvent>(&msg.to_text().unwrap()).unwrap();
    assert_eq!(event, ServerEvent::AuthOk);

    let binding = "a".repeat(10_000);
    let test_messages: Vec<&str> = vec![
        "TEST",
        r"¯\_(ツ)_/¯",
        "",
        "hello\nworld\ttab",
        "he said \"hello\"",
        "<script>alert('xss')</script>",
        "{\"nested\": true}",
        "a]b[c}d{e",
        " leading and trailing spaces ",
        "🦀🔥💀",
        &binding,
    ];

    for m in test_messages {
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientCommand::Echo {
                string: m.to_owned(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();

        let msg = ws.next().await.unwrap().unwrap();
        let event = serde_json::from_str::<ServerEvent>(&msg.to_text().unwrap()).unwrap();
        assert_eq!(
            event,
            ServerEvent::Echo {
                string: m.to_owned()
            }
        );
    }

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"BogusVariant":{}}"#.into(),
    ))
    .await
    .unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    let event = serde_json::from_str::<ServerEvent>(&msg.to_text().unwrap()).unwrap();
    assert_eq!(
        event,
        ServerEvent::Error {
            error: "invalid command".to_owned()
        }
    );

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"Echo":{"string":}}"#.into(),
    ))
    .await
    .unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    let event = serde_json::from_str::<ServerEvent>(&msg.to_text().unwrap()).unwrap();
    assert_eq!(
        event,
        ServerEvent::Error {
            error: "malformed JSON".to_owned()
        }
    );

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        "just a string".into(),
    ))
    .await
    .unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    let event = serde_json::from_str::<ServerEvent>(&msg.to_text().unwrap()).unwrap();
    assert_eq!(
        event,
        ServerEvent::Error {
            error: "malformed JSON".to_owned()
        }
    );

    shutdown.cancel();
}
