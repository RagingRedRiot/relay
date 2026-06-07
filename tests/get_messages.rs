mod common;

use common::*;
use relay::model::{MessageHistoryItem, NewMessageAttachment};
use sqlx::PgPool;
use uuid::Uuid;

// Connect + authenticate, returning a live socket.
async fn login(server: &TestServer, username: &str, password: &str) -> Ws {
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, username, password).await;
    ws
}

// Post a message over the wire and return its id.
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

// Request a page of history and return it, asserting the room name echoes back.
async fn history(
    ws: &mut Ws,
    room_name: &str,
    before: Option<Uuid>,
    limit: Option<u32>,
) -> Vec<MessageHistoryItem> {
    send_cmd(
        ws,
        &ClientCommand::GetMessages {
            room_name: room_name.to_owned(),
            before,
            limit,
        },
    )
    .await;
    match next_reply(ws).await {
        ServerEvent::MessageHistory {
            room_name: got,
            messages,
        } => {
            assert_eq!(got, room_name);
            messages
        }
        other => panic!("expected MessageHistory, got {other:?}"),
    }
}

// The contents of a page, in the order returned (newest first).
fn contents(page: &[MessageHistoryItem]) -> Vec<&str> {
    page.iter().map(|m| m.content.as_str()).collect()
}

#[sqlx::test]
async fn returns_room_messages_newest_first(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = login(&server, "alice", "alicepw").await;
    post(&mut ws, "vault", "first").await;
    post(&mut ws, "vault", "second").await;
    post(&mut ws, "vault", "third").await;

    let page = history(&mut ws, "vault", None, None).await;
    assert_eq!(contents(&page), vec!["third", "second", "first"]);
    // Sender is the display username, never a raw id.
    assert!(page.iter().all(|m| m.sender_username == "alice"));
}

#[sqlx::test]
async fn paginates_backwards_with_before_cursor(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = login(&server, "alice", "alicepw").await;
    for n in 1..=5 {
        post(&mut ws, "vault", &format!("m{n}")).await;
    }

    let page1 = history(&mut ws, "vault", None, Some(2)).await;
    assert_eq!(contents(&page1), vec!["m5", "m4"]);

    let cursor = page1.last().unwrap().message_id;
    let page2 = history(&mut ws, "vault", Some(cursor), Some(2)).await;
    assert_eq!(contents(&page2), vec!["m3", "m2"]);

    let cursor = page2.last().unwrap().message_id;
    let page3 = history(&mut ws, "vault", Some(cursor), Some(2)).await;
    assert_eq!(contents(&page3), vec!["m1"]);

    // Past the start: an empty page, not an error.
    let cursor = page3.last().unwrap().message_id;
    let page4 = history(&mut ws, "vault", Some(cursor), Some(2)).await;
    assert!(page4.is_empty());
}

#[sqlx::test]
async fn limit_is_clamped(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = login(&server, "alice", "alicepw").await;
    for n in 1..=3 {
        post(&mut ws, "vault", &format!("m{n}")).await;
    }

    // limit 0 floors to 1 rather than returning nothing.
    let page = history(&mut ws, "vault", None, Some(0)).await;
    assert_eq!(contents(&page), vec!["m3"]);
}

#[sqlx::test]
async fn attachments_appear_in_history(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = login(&server, "alice", "alicepw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::SendMessage {
            room_name: "vault".to_owned(),
            content: "with file".to_owned(),
            attachments: vec![NewMessageAttachment {
                filename: "notes.txt".to_owned(),
                content_type: "text/plain".to_owned(),
                size_bytes: 12,
                chunk_count: 1,
                content_sha256: vec![0u8; 32],
            }],
        },
    )
    .await;
    let attachment_id = match next_reply(&mut ws).await {
        ServerEvent::MessageCreated { attachment_ids, .. } => attachment_ids[0],
        other => panic!("expected MessageCreated, got {other:?}"),
    };

    let page = history(&mut ws, "vault", None, None).await;
    assert_eq!(page.len(), 1);
    let att = &page[0].attachments;
    assert_eq!(att.len(), 1);
    assert_eq!(att[0].attachment_id, attachment_id);
    assert_eq!(att[0].filename, "notes.txt");
    assert_eq!(att[0].size_bytes, 12);
    // Declared but never uploaded -> still incomplete in history.
    assert!(!att[0].is_complete);
}

#[sqlx::test]
async fn reactions_are_summarized_with_caller_flag(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    seed_membership(&pool, "vault", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = login(&server, "alice", "alicepw").await;
    let message_id = post(&mut alice, "vault", "react to me").await;

    // alice 👍 ; bob 👍 and 🎉
    let mut bob = login(&server, "bob", "bobpw").await;
    async fn react(ws: &mut Ws, message_id: Uuid, emoji: &str) {
        send_cmd(
            ws,
            &ClientCommand::AddReaction {
                message_id,
                emoji: emoji.to_owned(),
            },
        )
        .await;
        assert_eq!(next_reply(ws).await, ServerEvent::Success);
    }
    react(&mut alice, message_id, "👍").await;
    react(&mut bob, message_id, "👍").await;
    react(&mut bob, message_id, "🎉").await;

    // Seen from alice: 👍 counts 2 and is hers; 🎉 counts 1 and is not.
    let page = history(&mut alice, "vault", None, None).await;
    let reactions = &page[0].reactions;
    let thumbs = reactions.iter().find(|r| r.emoji == "👍").unwrap();
    assert_eq!(thumbs.count, 2);
    assert!(thumbs.reacted_by_me);
    let party = reactions.iter().find(|r| r.emoji == "🎉").unwrap();
    assert_eq!(party.count, 1);
    assert!(!party.reacted_by_me);
}

#[sqlx::test]
async fn non_member_is_denied_and_room_existence_hidden(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "mallory", "mallorypw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = login(&server, "alice", "alicepw").await;
    post(&mut alice, "vault", "secret").await;

    let mut mallory = login(&server, "mallory", "mallorypw").await;

    // Non-member of a real room and a totally unknown room give the same answer.
    for name in ["vault", "no-such-room"] {
        send_cmd(
            &mut mallory,
            &ClientCommand::GetMessages {
                room_name: name.to_owned(),
                before: None,
                limit: None,
            },
        )
        .await;
        assert_eq!(next_reply(&mut mallory).await, ServerEvent::Failed);
    }
}

#[sqlx::test]
async fn deleted_sender_falls_back_to_snapshot(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    // A message whose sender is already gone: sender_id NULL, snapshot set -- the
    // exact state delete_user leaves behind for preserved messages.
    sqlx::query(
        "INSERT INTO messages (room_id, sender_id, sender_username_snapshot, content)
         SELECT r.room_id, NULL, 'ghost', 'from the void'
         FROM rooms r WHERE r.room_name = $1",
    )
    .bind("vault")
    .execute(&pool)
    .await
    .unwrap();

    let mut ws = login(&server, "alice", "alicepw").await;
    let page = history(&mut ws, "vault", None, None).await;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].sender_username, "ghost");
    assert_eq!(page[0].content, "from the void");
}
