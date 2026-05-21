mod common;

use std::time::Duration;

use common::*;
use sqlx::PgPool;

// HTTP requests beyond the burst allowance should be rejected with 429.
#[sqlx::test]
async fn http_requests_over_burst_are_rejected(pool: PgPool) {
    let server = spawn_app(pool, |c| {
        c.rate_limit_per_second = 1;
        c.rate_limit_burst = 5;
    })
    .await;

    let client = reqwest::Client::new();
    let url = format!("http://{}/health", server.addr);

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
        "expected at least one 429, got {statuses:?}"
    );
}

// The per-connection message limiter (hardcoded in the ws handler) should
// emit RateLimit warnings and then close the socket once a client floods it.
#[sqlx::test]
async fn websocket_message_flood_is_rate_limited(pool: PgPool) {
    seed_user(&pool, "alice", "alicepass").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    send_cmd(
        &mut ws,
        &ClientCommand::Auth {
            username: "alice".to_owned(),
            password: Password("alicepass".to_owned()),
        },
    )
    .await;

    for _ in 1..50 {
        let spam = ClientCommand::Echo {
            string: "SPAM".to_owned(),
        };
        if !try_send_cmd(&mut ws, &spam).await {
            break;
        }
    }

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

    let parsed: Vec<ServerEvent> = received
        .iter()
        .filter_map(|m| m.to_text().ok())
        .filter_map(|s| serde_json::from_str::<ServerEvent>(s).ok())
        .collect();

    let rate_limit_count = parsed
        .iter()
        .filter(|c| matches!(c, ServerEvent::RateLimit { .. }))
        .count();

    let echo_count = parsed
        .iter()
        .filter(|c| matches!(c, ServerEvent::Echo { .. }))
        .count();

    let close_frame = received.iter().any(|m| matches!(m, Message::Close(_)));

    assert_eq!(
        rate_limit_count, 3,
        "expected exactly 3 RateLimit warnings, got {rate_limit_count}: {parsed:?}"
    );
    assert!(
        echo_count >= 15,
        "expected at least 15 echos before limited, got {echo_count}"
    );
    assert!(
        close_frame,
        "expected server to send close after violating rate limits"
    );
}
