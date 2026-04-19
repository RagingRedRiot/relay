mod server;
mod handler;
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;

use crate::handler::{app, RateLimitConfig};

#[tokio::main]
async fn main() {
    // initialize tracing
    tracing_subscriber::fmt::init();
    
    let shutdown = CancellationToken::new();
    let app = app(shutdown.clone(), RateLimitConfig { per_second: 4, burst: 10 }).await;

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
    ).with_graceful_shutdown({
        let shutdown = shutdown;
        async move { shutdown.cancelled().await }
    }).await.unwrap();

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

#[tokio::test]
async fn test_shutdown() {
    use tokio::task::JoinSet;
    use futures_util::StreamExt;
    use std::time::Duration;

    // Build App - Run in Task
    let shutdown = CancellationToken::new();
    let app = app(shutdown.clone(), RateLimitConfig { per_second: 10, burst: 15 }).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _ = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { let _ = axum::serve(
        listener, 
        app.into_make_service_with_connect_info::<SocketAddr>(),
    ).with_graceful_shutdown({
        let shutdown = shutdown.clone();
        async move { shutdown.cancelled().await }
    }).await;}});

    let mut clients = JoinSet::new();

    for _ in 1..100 {
        let url = format!("ws://{addr}/ws");
        clients.spawn(async move {
          let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
          // Hold the connection open; exit when the server closes it.
          while let Some(msg) = ws.next().await {
              if msg.is_err() { break; }
          }  
        });
    }

    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(5), async {
        while clients.join_next().await.is_some() {}
    }).await.expect("clients did not shut down in time");
}

#[tokio::test]
async fn test_websocket_communications() {
    use futures_util::{SinkExt, StreamExt};
    use crate::server::{ACTION, ClientCommand, ServerEvent};

    // Build App - Run in Task
    let shutdown = CancellationToken::new();
    let app = app(shutdown.clone(), RateLimitConfig { per_second: 10, burst: 15 }).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    
    let _ = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { let _ = axum::serve(
        listener, 
        app.into_make_service_with_connect_info::<SocketAddr>(),
    ).with_graceful_shutdown({
        let shutdown = shutdown.clone();
        async move { shutdown.cancelled().await }
    }).await;}});

    let (mut ws, _) = tokio_tungstenite::connect_async(
        format!("ws://{addr}/ws")
    ).await.unwrap();

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

        ws.send(
            tokio_tungstenite::tungstenite::Message::Text(
                serde_json::to_string(
                    &ServerEvent { action: ACTION::ECHO, content: m.to_owned() }
                ).unwrap().into()
            )
        ).await.unwrap();

        let msg = ws.next().await.unwrap().unwrap();
        let cmd = serde_json::from_str::<ClientCommand>(&msg.to_text().unwrap()).unwrap();
        assert_eq!(cmd, ClientCommand { action: ACTION::ECHO, content: m.to_owned() });
    }

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"action":"INVALID","content":"test"}"#.into()
    )).await.unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    let cmd = serde_json::from_str::<ClientCommand>(&msg.to_text().unwrap()).unwrap();
    assert_eq!(cmd, ClientCommand { action: ACTION::ERROR, content: "invalid command".to_owned() });

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"action":"ECHO".; "content":"malformed JSON'\"}"#.into()
    )).await.unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    let cmd = serde_json::from_str::<ClientCommand>(&msg.to_text().unwrap()).unwrap();
    assert_eq!(cmd, ClientCommand { action: ACTION::ERROR, content: "malformed JSON".to_owned() });

     ws.send(tokio_tungstenite::tungstenite::Message::Text(
        "just a string".into()
    )).await.unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    let cmd = serde_json::from_str::<ClientCommand>(&msg.to_text().unwrap()).unwrap();
    assert_eq!(cmd, ClientCommand { action: ACTION::ERROR, content: "malformed JSON".to_owned() });

    shutdown.cancel();
}

#[tokio::test]
async fn test_rate_limit_rejects_over_burst() {
    let shutdown = CancellationToken::new();
    let app = app(shutdown.clone(), RateLimitConfig { burst: 5, per_second: 1 }).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        ).await;
    });

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/health");

    let mut statuses = Vec::new();
    for _ in 0..20 {
        let resp = client.get(&url).send().await.unwrap();
        statuses.push(resp.status().as_u16());
    }

    assert_eq!(statuses[0], 200, "first request blocked — limiter misconfigured");
    assert!(
        statuses.iter().any(|&s| s == 429),
        "expected at least one 429, got {:?}", statuses
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_websocket_limiter() {
    use futures_util::{SinkExt, StreamExt};
    use tungstenite::Message;
    use tokio::time::Duration;
    use crate::server::{ACTION, ServerEvent};

    // Build App - Run in Task
    let shutdown = CancellationToken::new();
    let app = app(shutdown.clone(), RateLimitConfig { per_second: 10, burst: 15 }).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    
    let _ = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { let _ = axum::serve(
        listener, 
        app.into_make_service_with_connect_info::<SocketAddr>(),
    ).with_graceful_shutdown({
        let shutdown = shutdown.clone();
        async move { shutdown.cancelled().await }
    }).await;}});

    let (mut ws, _) = tokio_tungstenite::connect_async(
        format!("ws://{addr}/ws")
    ).await.unwrap();

    // Spam Flood
    for _ in 1..50 {
        let msg = tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(
                &ServerEvent { action: ACTION::ECHO, content: "SPAM".to_owned() }
            ).unwrap().into()
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
    }).await;

    // Parse JSON Responses
    let parsed: Vec<ServerEvent> = received.iter()
        .filter_map(|m| m.to_text().ok())
        .filter_map(|s| serde_json::from_str::<ServerEvent>(s).ok())
        .collect();

    // Count RateLimit responses
    let rate_limit_count = parsed.iter()
        .filter(|c| c.action == ACTION::RATELIMIT)
        .count();

    // Count Echos
    let echo_count = parsed.iter()
        .filter(|c| c.action == ACTION::ECHO)
        .count();

    let close_frame = received.iter().any(
        |m| matches!(m, Message::Close(_)
    ));

    assert_eq!(
        rate_limit_count, 3,
        "expected excatly 3 RATELIMIT warnings, got {}: {:?}", rate_limit_count, parsed
    );

    assert!(
        echo_count >= 15, 
        "expected at least 15 echos before limited, got {}", echo_count
    );

    assert!(close_frame, "expected server to send close after violating rate limits");

    shutdown.cancel();
}