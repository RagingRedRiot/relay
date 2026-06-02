mod common;

use common::*;
use sqlx::PgPool;
use uuid::Uuid;

// Open a socket, authenticate, and post one message to `room_name`, returning its
// id so the test can react to it. Goes through the real SendMessage path rather
// than seeding SQL, so the message exists exactly as production would create it.
async fn post_message(
    server: &TestServer,
    username: &str,
    password: &str,
    room_name: &str,
) -> Uuid {
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, username, password).await;

    let room_id = room_id(&server.pool, room_name).await;
    send_cmd(
        &mut ws,
        &ClientCommand::SendMessage {
            room_id,
            content: "hello".to_owned(),
            attachments: vec![],
        },
    )
    .await;

    let message_id = match next_reply(&mut ws).await {
        ServerEvent::MessageCreated { message_id, .. } => message_id,
        other => panic!("expected MessageCreated, got {other:?}"),
    };
    close_socket(&mut ws).await;
    message_id
}

// Total reactions on a message.
async fn reaction_count(pool: &PgPool, message_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM message_reactions WHERE message_id = $1")
        .bind(message_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

// True if `username` has reacted to `message_id` with `emoji`.
async fn has_reaction(pool: &PgPool, message_id: Uuid, username: &str, emoji: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM message_reactions r
            JOIN users u ON u.user_id = r.user_id
            WHERE r.message_id = $1 AND u.username = $2 AND r.emoji = $3)",
    )
    .bind(message_id)
    .bind(username)
    .bind(emoji)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[sqlx::test]
async fn member_can_react_and_reaction_is_persisted(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let message_id = post_message(&server, "alice", "alicepw", "vault").await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::AddReaction {
            message_id,
            emoji: "👍".to_owned(),
        },
    )
    .await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Success);

    assert!(has_reaction(&pool, message_id, "alice", "👍").await);
    assert_eq!(reaction_count(&pool, message_id).await, 1);
}

#[sqlx::test]
async fn adding_the_same_emoji_twice_is_idempotent(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let message_id = post_message(&server, "alice", "alicepw", "vault").await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;
    for _ in 0..2 {
        send_cmd(
            &mut ws,
            &ClientCommand::AddReaction {
                message_id,
                emoji: "🎉".to_owned(),
            },
        )
        .await;
        assert_eq!(next_reply(&mut ws).await, ServerEvent::Success);
    }

    // Both adds succeeded, but the composite PK leaves a single row.
    assert_eq!(reaction_count(&pool, message_id).await, 1);
}

#[sqlx::test]
async fn distinct_emojis_and_users_each_get_a_row(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    seed_membership(&pool, "vault", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let message_id = post_message(&server, "alice", "alicepw", "vault").await;

    let mut alice = create_socket(server.addr).await;
    authenticate(&mut alice, "alice", "alicepw").await;
    for emoji in ["👍", "🎉"] {
        send_cmd(
            &mut alice,
            &ClientCommand::AddReaction {
                message_id,
                emoji: emoji.to_owned(),
            },
        )
        .await;
        assert_eq!(next_reply(&mut alice).await, ServerEvent::Success);
    }

    // A different user reacting with an emoji alice already used is its own row.
    let mut bob = create_socket(server.addr).await;
    authenticate(&mut bob, "bob", "bobpw").await;
    send_cmd(
        &mut bob,
        &ClientCommand::AddReaction {
            message_id,
            emoji: "👍".to_owned(),
        },
    )
    .await;
    assert_eq!(next_reply(&mut bob).await, ServerEvent::Success);

    assert_eq!(reaction_count(&pool, message_id).await, 3);
}

#[sqlx::test]
async fn remove_reaction_is_idempotent(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let message_id = post_message(&server, "alice", "alicepw", "vault").await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::AddReaction {
            message_id,
            emoji: "👍".to_owned(),
        },
    )
    .await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Success);

    // First remove deletes the row; the second is a no-op but still succeeds.
    for _ in 0..2 {
        send_cmd(
            &mut ws,
            &ClientCommand::RemoveReaction {
                message_id,
                emoji: "👍".to_owned(),
            },
        )
        .await;
        assert_eq!(next_reply(&mut ws).await, ServerEvent::Success);
    }

    assert_eq!(reaction_count(&pool, message_id).await, 0);
}

#[sqlx::test]
async fn non_member_cannot_react(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "mallory", "mallorypw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let message_id = post_message(&server, "alice", "alicepw", "vault").await;

    // mallory is a valid user but not a member of vault.
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mallory", "mallorypw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::AddReaction {
            message_id,
            emoji: "👍".to_owned(),
        },
    )
    .await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Failed);

    assert_eq!(reaction_count(&pool, message_id).await, 0);
}

#[sqlx::test]
async fn unknown_message_is_indistinguishable_from_forbidden(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    // A random id for a message that never existed yields the same generic Failed a
    // non-member gets, so existence doesn't leak. The session survives either way.
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;
    let nonexistent: Uuid = "00000000-0000-0000-0000-0000000000ff".parse().unwrap();
    send_cmd(
        &mut ws,
        &ClientCommand::AddReaction {
            message_id: nonexistent,
            emoji: "👍".to_owned(),
        },
    )
    .await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Failed);

    // Still responsive afterward.
    close_socket(&mut ws).await;
}
