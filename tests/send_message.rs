mod common;

use common::*;
use relay::model::NewMessageAttachment;
use sqlx::PgPool;
use uuid::Uuid;

fn send_message_cmd(
    room_name: &str,
    content: &str,
    attachments: Vec<NewMessageAttachment>,
) -> ClientCommand {
    ClientCommand::SendMessage {
        room_name: room_name.to_owned(),
        content: content.to_owned(),
        attachments,
    }
}

fn attachment(filename: &str, chunk_count: i32, size_bytes: i64) -> NewMessageAttachment {
    NewMessageAttachment {
        filename: filename.to_owned(),
        content_type: "application/octet-stream".to_owned(),
        size_bytes,
        chunk_count,
        content_sha256: vec![0u8; 32],
    }
}

#[sqlx::test]
async fn member_can_post_plain_message(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let rid = room_id(&pool, "general").await;
    send_cmd(&mut ws, &send_message_cmd("general", "hello world", vec![])).await;

    match next_reply(&mut ws).await {
        ServerEvent::MessageCreated {
            message_id,
            attachment_ids,
            ..
        } => {
            assert!(attachment_ids.is_empty());

            let (room, sender, content): (Uuid, Option<Uuid>, String) = sqlx::query_as(
                "SELECT room_id, sender_id, content FROM messages WHERE message_id = $1",
            )
            .bind(message_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(room, rid);
            assert_eq!(content, "hello world");
            // Sender is the authenticated user, recorded server-side.
            let alice: Uuid =
                sqlx::query_scalar("SELECT user_id FROM users WHERE username = 'alice'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(sender, Some(alice));
        }
        other => panic!("expected MessageCreated, got {other:?}"),
    }

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn attachments_are_created_incomplete_in_order(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let attachments = vec![
        attachment("first.bin", 2, 1024),
        attachment("second.bin", 5, 4096),
    ];
    send_cmd(
        &mut ws,
        &send_message_cmd("general", "see files", attachments),
    )
    .await;

    let (message_id, attachment_ids) = match next_reply(&mut ws).await {
        ServerEvent::MessageCreated {
            message_id,
            attachment_ids,
            ..
        } => (message_id, attachment_ids),
        other => panic!("expected MessageCreated, got {other:?}"),
    };

    // One id per declared attachment, in declaration order.
    assert_eq!(attachment_ids.len(), 2);

    let rows: Vec<(Uuid, String, i32, bool)> = sqlx::query_as(
        "SELECT attachment_id, filename, chunk_count, is_complete
         FROM message_attachments WHERE message_id = $1
         ORDER BY created_at",
    )
    .bind(message_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(rows.len(), 2);
    // Returned ids line up, in order, with the declared attachments.
    assert_eq!(rows[0].0, attachment_ids[0]);
    assert_eq!(rows[1].0, attachment_ids[1]);
    assert_eq!(rows[0].1, "first.bin");
    assert_eq!(rows[1].1, "second.bin");
    assert_eq!(rows[0].2, 2);
    assert_eq!(rows[1].2, 5);
    // Rows start incomplete; no chunks have arrived yet.
    assert!(!rows[0].3);
    assert!(!rows[1].3);

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn non_member_cannot_post(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "mallory", "mallorypw").await;
    // Private room owned by alice; mallory is not a member.
    seed_room(&pool, "secret", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mallory", "mallorypw").await;

    let rid = room_id(&pool, "secret").await;
    send_cmd(&mut ws, &send_message_cmd("secret", "let me in", vec![])).await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Failed);

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE room_id = $1")
        .bind(rid)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn unknown_room_is_indistinguishable_from_forbidden(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &send_message_cmd("missing", "anyone?", vec![])).await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn empty_content_is_rejected_and_session_survives(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    // Empty content violates the messages.content CHECK -> Failed.
    send_cmd(&mut ws, &send_message_cmd("general", "", vec![])).await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Failed);

    // The session is still usable afterward.
    send_cmd(&mut ws, &send_message_cmd("general", "recovered", vec![])).await;
    assert!(matches!(
        next_reply(&mut ws).await,
        ServerEvent::MessageCreated { .. }
    ));

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn malformed_attachment_rolls_back_the_message(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let rid = room_id(&pool, "general").await;
    // size_bytes = 0 violates the message_attachments CHECK; the whole insert
    // (message included) must roll back.
    let bad = vec![attachment("empty.bin", 1, 0)];
    send_cmd(&mut ws, &send_message_cmd("general", "has a bad file", bad)).await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Failed);

    let msg_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE room_id = $1")
        .bind(rid)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        msg_count, 0,
        "message must not persist when an attachment is invalid"
    );

    close_socket(&mut ws).await;
}
