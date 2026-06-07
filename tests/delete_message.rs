mod common;

use common::*;
use sqlx::PgPool;
use uuid::Uuid;

// Post a plain message to `room_name` and return its id. Skips the live echo so
// the MessageCreated ack is what we read.
async fn post(ws: &mut Ws, room_name: &str, content: &str) -> Uuid {
    send_cmd(
        ws,
        &ClientCommand::SendMessage {
            room_name: room_name.to_owned(),
            content: content.to_owned(),
            attachments: vec![],
        },
    )
    .await;
    match next_reply(ws).await {
        ServerEvent::MessageCreated { message_id, .. } => message_id,
        other => panic!("expected MessageCreated, got {other:?}"),
    }
}

async fn message_exists(pool: &PgPool, id: Uuid) -> bool {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages WHERE message_id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count messages")
        > 0
}

// Drive a DeleteMessage and read until the expected outcome. The deleter is
// subscribed to the room, so a MessageRemoved broadcast can interleave with the
// Success/Failed reply in either order; collect past it.
async fn delete_and_expect_success(ws: &mut Ws, message_id: Uuid) {
    send_cmd(ws, &ClientCommand::DeleteMessage { message_id }).await;
    loop {
        match next_reply(ws).await {
            ServerEvent::Success => return,
            ServerEvent::MessageRemoved { .. } => continue,
            other => panic!("expected Success, got {other:?}"),
        }
    }
}

// A sender can unsend their own message: the server acks Success and the row is
// deleted.
#[sqlx::test]
async fn sender_can_unsend_own_message(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let message_id = post(&mut ws, "general", "hello").await;
    assert!(message_exists(&pool, message_id).await);

    delete_and_expect_success(&mut ws, message_id).await;
    assert!(
        !message_exists(&pool, message_id).await,
        "the message should be deleted"
    );

    close_socket(&mut ws).await;
}

// An admin can remove anyone's message, even a room they don't belong to
// (moderation). Carol is an admin but not a member of `general`.
#[sqlx::test]
async fn admin_can_remove_any_message(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_admin(&pool, "carol", "carolpw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = create_socket(server.addr).await;
    authenticate(&mut alice, "alice", "alicepw").await;
    let message_id = post(&mut alice, "general", "hello").await;

    let mut carol = create_socket(server.addr).await;
    authenticate(&mut carol, "carol", "carolpw").await;
    delete_and_expect_success(&mut carol, message_id).await;

    assert!(
        !message_exists(&pool, message_id).await,
        "admin removal should delete the message"
    );

    // Alice, a subscribed member, sees the admin's removal live -- drain it (and
    // her own post echo) before closing so the close handshake is clean.
    loop {
        match next_event(&mut alice).await {
            ServerEvent::MessageRemoved { message_id: id, .. } => {
                assert_eq!(id, message_id);
                break;
            }
            ServerEvent::NewMessage { .. } | ServerEvent::Resync { .. } => continue,
            other => panic!("expected MessageRemoved, got {other:?}"),
        }
    }

    close_socket(&mut alice).await;
    close_socket(&mut carol).await;
}

// A plain member who is neither the author nor an admin cannot delete someone
// else's message: the request fails and the message survives.
#[sqlx::test]
async fn non_author_non_admin_cannot_delete(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    seed_membership(&pool, "general", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = create_socket(server.addr).await;
    authenticate(&mut alice, "alice", "alicepw").await;
    let message_id = post(&mut alice, "general", "hello").await;

    let mut bob = create_socket(server.addr).await;
    authenticate(&mut bob, "bob", "bobpw").await;
    send_cmd(&mut bob, &ClientCommand::DeleteMessage { message_id }).await;
    assert_eq!(next_reply(&mut bob).await, ServerEvent::Failed);

    assert!(
        message_exists(&pool, message_id).await,
        "the message must survive a forbidden delete"
    );

    close_socket(&mut alice).await;
    close_socket(&mut bob).await;
}

// Deleting a message fans a MessageRemoved out to the room's other live members,
// so it disappears for everyone, not just the deleter.
#[sqlx::test]
async fn deletion_is_broadcast_to_room_members(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    seed_membership(&pool, "general", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = create_socket(server.addr).await;
    authenticate(&mut alice, "alice", "alicepw").await;
    let mut bob = create_socket(server.addr).await;
    authenticate(&mut bob, "bob", "bobpw").await;

    let message_id = post(&mut alice, "general", "hello").await;
    // Bob sees the live post first.
    match next_event(&mut bob).await {
        ServerEvent::NewMessage { message, .. } => assert_eq!(message.message_id, message_id),
        other => panic!("expected NewMessage, got {other:?}"),
    }

    delete_and_expect_success(&mut alice, message_id).await;

    // Bob gets the removal live (skipping any other interleaved push).
    loop {
        match next_event(&mut bob).await {
            ServerEvent::MessageRemoved {
                room_name,
                message_id: id,
            } => {
                assert_eq!(room_name, "general");
                assert_eq!(id, message_id);
                break;
            }
            ServerEvent::NewMessage { .. } | ServerEvent::Resync { .. } => continue,
            other => panic!("expected MessageRemoved, got {other:?}"),
        }
    }

    close_socket(&mut alice).await;
    close_socket(&mut bob).await;
}
