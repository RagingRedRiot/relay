mod common;

use common::*;
use relay::model::MessageHistoryItem;
use sqlx::PgPool;
use tokio::time::{Duration, sleep, timeout};
use uuid::Uuid;

// Connect, authenticate, then issue an Echo round-trip as a barrier. A command
// reply is only produced after the receiver task has run subscribe_existing_rooms
// (it precedes the command loop), so by the time the Echo returns this session's
// room broadcast receivers exist -- any message posted afterward is captured.
async fn connect(server: &TestServer, username: &str, password: &str) -> Ws {
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, username, password).await;
    send_cmd(
        &mut ws,
        &ClientCommand::Echo {
            string: "sync".to_owned(),
        },
    )
    .await;
    assert_eq!(
        next_reply(&mut ws).await,
        ServerEvent::Echo {
            string: "sync".to_owned()
        }
    );
    ws
}

// Post a message and return its id, skipping the poster's own live echo to read
// the MessageCreated ack.
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

// The next live NewMessage on this socket.
async fn recv_new_message(ws: &mut Ws) -> (String, MessageHistoryItem) {
    match next_event(ws).await {
        ServerEvent::NewMessage { room_name, message } => (room_name, message),
        other => panic!("expected NewMessage, got {other:?}"),
    }
}

// Assert no frame arrives within `within`. Used for "this session must NOT be
// delivered" cases; a generous window so a real (buggy) delivery is caught.
async fn assert_no_event(ws: &mut Ws, within: Duration) {
    match timeout(within, ws.next()).await {
        Err(_) => {} // timed out -> nothing delivered
        Ok(Some(Ok(msg))) => panic!("unexpected event: {msg:?}"),
        Ok(_) => panic!("connection closed unexpectedly"),
    }
}

#[sqlx::test]
async fn member_receives_another_members_message_live(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    seed_membership(&pool, "vault", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = connect(&server, "alice", "alicepw").await;
    let mut bob = connect(&server, "bob", "bobpw").await;

    let message_id = post(&mut alice, "vault", "hello room").await;

    let (room_name, message) = recv_new_message(&mut bob).await;
    assert_eq!(room_name, "vault");
    assert_eq!(message.message_id, message_id);
    assert_eq!(message.content, "hello room");
    assert_eq!(message.sender_username, "alice");
}

#[sqlx::test]
async fn sender_receives_its_own_echo(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = connect(&server, "alice", "alicepw").await;

    send_cmd(
        &mut alice,
        &ClientCommand::SendMessage {
            room_name: "vault".to_owned(),
            content: "echo me".to_owned(),
            attachments: vec![],
        },
    )
    .await;

    // The sender gets both the ack and a live NewMessage for its own message (it
    // dedups by message_id). Order between them is unspecified, so collect both.
    let mut saw_ack = false;
    let mut saw_echo = false;
    while !(saw_ack && saw_echo) {
        match next_event(&mut alice).await {
            ServerEvent::MessageCreated { message, .. } => {
                assert_eq!(message.content, "echo me");
                saw_ack = true;
            }
            ServerEvent::NewMessage { message, .. } => {
                assert_eq!(message.content, "echo me");
                saw_echo = true;
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}

#[sqlx::test]
async fn non_member_receives_nothing(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "carl", "carlpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = connect(&server, "alice", "alicepw").await;
    // carl is a valid, connected user but not a member of vault.
    let mut carl = connect(&server, "carl", "carlpw").await;

    post(&mut alice, "vault", "members only").await;

    // carl never subscribed to vault, so nothing is delivered to him.
    assert_no_event(&mut carl, Duration::from_millis(500)).await;
}

#[sqlx::test]
async fn joining_subscribes_to_live_messages(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", true, false).await; // public so bob can join
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = connect(&server, "alice", "alicepw").await;
    let mut bob = connect(&server, "bob", "bobpw").await;

    // Before joining, bob isn't subscribed.
    post(&mut alice, "vault", "before join").await;
    assert_no_event(&mut bob, Duration::from_millis(300)).await;

    // Joining subscribes him: subscribe_room runs before the Success reply, so once
    // bob reads Success his receiver exists.
    send_cmd(
        &mut bob,
        &ClientCommand::JoinRoom {
            room_name: "vault".to_owned(),
        },
    )
    .await;
    assert_eq!(next_reply(&mut bob).await, ServerEvent::Success);

    let message_id = post(&mut alice, "vault", "after join").await;
    let (_, message) = recv_new_message(&mut bob).await;
    assert_eq!(message.message_id, message_id);
    assert_eq!(message.content, "after join");
}

#[sqlx::test]
async fn leaving_unsubscribes_from_live_messages(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", true, false).await;
    seed_membership(&pool, "vault", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = connect(&server, "alice", "alicepw").await;
    let mut bob = connect(&server, "bob", "bobpw").await;

    send_cmd(
        &mut bob,
        &ClientCommand::LeaveRoom {
            room_name: "vault".to_owned(),
        },
    )
    .await;
    assert_eq!(next_reply(&mut bob).await, ServerEvent::Success);

    // The unsubscribe (Subscription::Remove) is queued to bob's sender task ahead of
    // the post; give it a moment to drain so the StreamMap entry is really gone.
    sleep(Duration::from_millis(200)).await;

    post(&mut alice, "vault", "after bob left").await;
    assert_no_event(&mut bob, Duration::from_millis(500)).await;
}

#[sqlx::test]
async fn approval_subscribes_an_online_requester(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    // Private but discoverable: bob can request to join, the owner approves.
    seed_room(&pool, "vault", "alice", false, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = connect(&server, "alice", "alicepw").await;
    let mut bob = connect(&server, "bob", "bobpw").await;

    // bob requests to join -- not a member yet, so not subscribed.
    send_cmd(
        &mut bob,
        &ClientCommand::JoinRoom {
            room_name: "vault".to_owned(),
        },
    )
    .await;
    assert_eq!(next_reply(&mut bob).await, ServerEvent::JoinRequested);

    // alice approves. The approval runs on alice's session but subscribes bob's
    // open session (cross-session) before the Success reply comes back.
    send_cmd(
        &mut alice,
        &ClientCommand::ApproveJoinRequest {
            room_name: "vault".to_owned(),
            requester_username: "bob".to_owned(),
        },
    )
    .await;
    assert_eq!(next_reply(&mut alice).await, ServerEvent::Success);

    // bob gets live messages without reconnecting.
    let message_id = post(&mut alice, "vault", "welcome aboard").await;
    let (_, message) = recv_new_message(&mut bob).await;
    assert_eq!(message.message_id, message_id);
    assert_eq!(message.content, "welcome aboard");
}
