mod common;

use common::*;
use relay::model::NewMessageAttachment;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

// A minimal xz stream header: the 6-byte magic (FD 37 7A 58 5A 00) is enough for
// magic-byte sniffing to recognize it as application/x-xz, which is not in the
// supported-binary allowlist and so must be rejected.
fn xz_bytes() -> Vec<u8> {
    vec![0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00, 0x01, 0x02, 0x03, 0x04]
}

// Declare a single-chunk attachment on a fresh message and return its id. The
// bytes are streamed separately; this only creates the (incomplete) row.
async fn declare(ws: &mut Ws, room_name: &str, declared_type: &str, payload: &[u8]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    send_cmd(
        ws,
        &ClientCommand::SendMessage {
            room_name: room_name.to_owned(),
            content: "file".to_owned(),
            attachments: vec![NewMessageAttachment {
                filename: "archive.xz".to_owned(),
                content_type: declared_type.to_owned(),
                size_bytes: payload.len() as i64,
                chunk_count: 1,
                content_sha256: hasher.finalize().to_vec(),
            }],
        },
    )
    .await;
    match next_reply(ws).await {
        ServerEvent::MessageCreated { attachment_ids, .. } => attachment_ids[0],
        other => panic!("expected MessageCreated, got {other:?}"),
    }
}

async fn attachment_exists(pool: &PgPool, id: Uuid) -> bool {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM message_attachments WHERE attachment_id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("count attachments")
        > 0
}

async fn chunk_count(pool: &PgPool, id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM message_attachment_chunks WHERE attachment_id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count chunks")
}

async fn expect_rejection_and_removal(ws: &mut Ws, room: &str, attachment_id: Uuid) -> String {
    let mut reason = None;
    let mut removed = false;
    while reason.is_none() || !removed {
        match next_reply(ws).await {
            ServerEvent::AttachmentRejected {
                attachment_id: id,
                reason: r,
            } => {
                assert_eq!(id, attachment_id);
                reason = Some(r);
            }
            ServerEvent::MessageRemoved { room_name, .. } => {
                assert_eq!(room_name, room);
                removed = true;
            }
            other => panic!("expected AttachmentRejected or MessageRemoved, got {other:?}"),
        }
    }
    reason.unwrap()
}

// A fully-uploaded but unsupported file (xz archive) is rejected by the
// content-type policy, and the rejection cancels the upload: the attachment row
// and all of its chunks are deleted from the database rather than left incomplete
// for the reaper. The parent message keeps its caption text.
#[sqlx::test]
async fn rejected_upload_is_deleted(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let payload = xz_bytes();
    let attachment_id = declare(&mut ws, "general", "application/x-xz", &payload).await;
    send_chunk_frame(&mut ws, attachment_id, 0, &payload).await;

    // The rejection and removal are live events on the uploader's room stream.
    let reason = expect_rejection_and_removal(&mut ws, "general", attachment_id).await;
    assert!(reason.contains("x-xz"), "reason should name the type");

    // The cancelled upload leaves nothing behind: the attachment row is gone, its
    // chunks cascaded away, and the file-only parent message (whose caption was
    // just the filename) is deleted rather than left as a bare filename for the room.
    assert!(
        !attachment_exists(&pool, attachment_id).await,
        "attachment row should be deleted"
    );
    assert_eq!(
        chunk_count(&pool, attachment_id).await,
        0,
        "chunks should be deleted"
    );
    let messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(&pool)
        .await
        .expect("count messages");
    assert_eq!(messages, 0, "the orphaned file-only message is deleted too");

    close_socket(&mut ws).await;
}

// Rejected attachment cleanup must reach other room members that already rendered
// the incomplete message. They need AttachmentRejected to drop the dead attachment
// and MessageRemoved when that rejection empties the message.
#[sqlx::test]
async fn rejected_upload_cleanup_is_broadcast_to_room_members(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    seed_membership(&pool, "general", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut alice = create_socket(server.addr).await;
    authenticate(&mut alice, "alice", "alicepw").await;

    let mut bob = create_socket(server.addr).await;
    authenticate(&mut bob, "bob", "bobpw").await;
    send_cmd(&mut bob, &ClientCommand::GetUnreadSummary).await;
    match next_reply(&mut bob).await {
        ServerEvent::UnreadSummary { .. } => {}
        other => panic!("expected UnreadSummary, got {other:?}"),
    }

    let payload = xz_bytes();
    let attachment_id = declare(&mut alice, "general", "application/x-xz", &payload).await;
    send_chunk_frame(&mut alice, attachment_id, 0, &payload).await;

    let mut saw_live_message = false;
    let mut saw_rejected = false;
    let mut saw_removed = false;
    while !saw_rejected || !saw_removed {
        match next_event(&mut bob).await {
            ServerEvent::NewMessage { room_name, message } => {
                assert_eq!(room_name, "general");
                assert!(
                    message
                        .attachments
                        .iter()
                        .any(|a| a.attachment_id == attachment_id)
                );
                saw_live_message = true;
            }
            ServerEvent::AttachmentRejected {
                attachment_id: id, ..
            } => {
                assert_eq!(id, attachment_id);
                saw_rejected = true;
            }
            ServerEvent::MessageRemoved { room_name, .. } => {
                assert_eq!(room_name, "general");
                saw_removed = true;
            }
            ServerEvent::Resync { .. } => {}
            other => panic!("expected live cleanup event, got {other:?}"),
        }
    }
    assert!(
        saw_live_message,
        "bob should have seen the message before cleanup"
    );

    let reason = expect_rejection_and_removal(&mut alice, "general", attachment_id).await;
    assert!(reason.contains("x-xz"), "reason should name the type");

    close_socket(&mut alice).await;
    close_socket(&mut bob).await;
}

// Smuggling: a binary file (xz) declared as text/plain is also rejected and
// cancelled. This path goes through the magicless/declared branch only when the
// bytes have no signature; xz does have one, so detection wins and the file is
// rejected as its true type regardless of the false text declaration.
#[sqlx::test]
async fn mislabeled_binary_is_deleted(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let payload = xz_bytes();
    let attachment_id = declare(&mut ws, "general", "text/plain", &payload).await;
    send_chunk_frame(&mut ws, attachment_id, 0, &payload).await;

    // Detection wins over the false text declaration: rejected as its true type.
    let mut rejected = false;
    let mut removed = false;
    while !rejected || !removed {
        match next_reply(&mut ws).await {
            ServerEvent::AttachmentRejected {
                attachment_id: id, ..
            } => {
                assert_eq!(id, attachment_id);
                rejected = true;
            }
            ServerEvent::MessageRemoved { .. } => removed = true,
            other => panic!("expected AttachmentRejected or MessageRemoved, got {other:?}"),
        }
    }

    assert!(
        !attachment_exists(&pool, attachment_id).await,
        "attachment row should be deleted"
    );
    assert_eq!(
        chunk_count(&pool, attachment_id).await,
        0,
        "chunks should be deleted"
    );

    close_socket(&mut ws).await;
}
