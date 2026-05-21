mod common;

use common::*;
use sqlx::PgPool;

#[sqlx::test]
async fn websocket_echoes_arbitrary_payloads(pool: PgPool) {
    seed_user(&pool, "alice", "alicepass").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepass").await;

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
        send_cmd(
            &mut ws,
            &ClientCommand::Echo {
                string: m.to_owned(),
            },
        )
        .await;
        assert_eq!(
            next_event(&mut ws).await,
            ServerEvent::Echo {
                string: m.to_owned()
            }
        );
    }
}

#[sqlx::test]
async fn websocket_rejects_malformed_input(pool: PgPool) {
    seed_user(&pool, "alice", "alicepass").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepass").await;

    // Valid JSON, unknown command variant.
    send_text(&mut ws, r#"{"BogusVariant":{}}"#).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::Error {
            error: "invalid command".to_owned()
        }
    );

    // Syntactically broken JSON.
    send_text(&mut ws, r#"{"Echo":{"string":}}"#).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::Error {
            error: "malformed JSON".to_owned()
        }
    );

    // Not JSON at all.
    send_text(&mut ws, "just a string").await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::Error {
            error: "malformed JSON".to_owned()
        }
    );
}
