mod common;

use common::*;
use relay::model::RoomUnread;
use sqlx::PgPool;
use uuid::Uuid;

async fn login(server: &TestServer, username: &str, password: &str) -> Ws {
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, username, password).await;
    ws
}

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

async fn join(ws: &mut Ws, room_name: &str) {
    send_cmd(
        ws,
        &ClientCommand::JoinRoom {
            room_name: room_name.to_owned(),
        },
    )
    .await;
    assert_eq!(next_reply(ws).await, ServerEvent::Success);
}

async fn mark_read(ws: &mut Ws, room_name: &str, up_to: Uuid) {
    send_cmd(
        ws,
        &ClientCommand::MarkRead {
            room_name: room_name.to_owned(),
            up_to_message_id: up_to,
        },
    )
    .await;
    assert_eq!(next_reply(ws).await, ServerEvent::Success);
}

async fn summary(ws: &mut Ws) -> Vec<RoomUnread> {
    send_cmd(ws, &ClientCommand::GetUnreadSummary).await;
    match next_reply(ws).await {
        ServerEvent::UnreadSummary { rooms } => rooms,
        other => panic!("expected UnreadSummary, got {other:?}"),
    }
}

// Unread count for one room from the caller's summary; panics if the room isn't
// listed (every room the caller is in should appear).
async fn unread_for(ws: &mut Ws, room_name: &str) -> i64 {
    summary(ws)
        .await
        .into_iter()
        .find(|r| r.room_name == room_name)
        .unwrap_or_else(|| panic!("{room_name} not in summary"))
        .unread
}

#[sqlx::test]
async fn own_sends_are_not_unread_to_the_sender(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = login(&server, "alice", "alicepw").await;
    post(&mut alice, "vault", "one").await;
    post(&mut alice, "vault", "two").await;

    // Sending advances the sender's own watermark, so the room reads as caught up.
    assert_eq!(unread_for(&mut alice, "vault").await, 0);
}

#[sqlx::test]
async fn new_member_starts_caught_up_then_accrues_unread(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", true, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = login(&server, "alice", "alicepw").await;
    post(&mut alice, "vault", "before bob 1").await;
    post(&mut alice, "vault", "before bob 2").await;

    // Bob joins after the backlog exists: caught up, not a wall of unread.
    let mut bob = login(&server, "bob", "bobpw").await;
    join(&mut bob, "vault").await;
    assert_eq!(unread_for(&mut bob, "vault").await, 0);

    // Messages sent after he joined are unread to him.
    post(&mut alice, "vault", "after bob").await;
    assert_eq!(unread_for(&mut bob, "vault").await, 1);
}

#[sqlx::test]
async fn mark_read_clears_unread(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", true, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = login(&server, "alice", "alicepw").await;
    let mut bob = login(&server, "bob", "bobpw").await;
    join(&mut bob, "vault").await;

    post(&mut alice, "vault", "m1").await;
    let newest = post(&mut alice, "vault", "m2").await;
    assert_eq!(unread_for(&mut bob, "vault").await, 2);

    mark_read(&mut bob, "vault", newest).await;
    assert_eq!(unread_for(&mut bob, "vault").await, 0);
}

#[sqlx::test]
async fn mark_read_is_forward_only(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", true, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = login(&server, "alice", "alicepw").await;
    let mut bob = login(&server, "bob", "bobpw").await;
    join(&mut bob, "vault").await;

    let older = post(&mut alice, "vault", "m1").await;
    let newest = post(&mut alice, "vault", "m2").await;

    mark_read(&mut bob, "vault", newest).await;
    assert_eq!(unread_for(&mut bob, "vault").await, 0);

    // A stale MarkRead at an older message must not rewind the watermark (which
    // would resurrect m2 as unread). It succeeds but changes nothing.
    mark_read(&mut bob, "vault", older).await;
    assert_eq!(unread_for(&mut bob, "vault").await, 0);
}

#[sqlx::test]
async fn non_member_cannot_mark_read(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "mallory", "mallorypw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = login(&server, "alice", "alicepw").await;
    let message_id = post(&mut alice, "vault", "secret").await;

    let mut mallory = login(&server, "mallory", "mallorypw").await;
    send_cmd(
        &mut mallory,
        &ClientCommand::MarkRead {
            room_name: "vault".to_owned(),
            up_to_message_id: message_id,
        },
    )
    .await;
    assert_eq!(next_reply(&mut mallory).await, ServerEvent::Failed);
}

#[sqlx::test]
async fn summary_lists_every_room_including_zero_unread(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", true, false).await;
    seed_room(&pool, "lounge", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = login(&server, "alice", "alicepw").await;
    let mut bob = login(&server, "bob", "bobpw").await;
    join(&mut bob, "vault").await;

    // Bob posts in vault; alice never read or sent there, so it's unread to her.
    // alice's lounge has no messages -> still listed, at zero.
    post(&mut bob, "vault", "hi").await;
    post(&mut bob, "vault", "again").await;

    let rooms = summary(&mut alice).await;
    // Ordered by room_name: lounge before vault.
    assert_eq!(
        rooms,
        vec![
            RoomUnread {
                room_name: "lounge".to_owned(),
                unread: 0,
            },
            RoomUnread {
                room_name: "vault".to_owned(),
                unread: 2,
            },
        ]
    );
}
