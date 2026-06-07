mod common;

use common::*;
use relay::model::NewMessageAttachment;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

// Seed a message + a *complete* attachment with the given chunks, returning the
// attachment_id. Inserts directly (bypassing the upload actor) so a download test
// doesn't depend on the upload path. content_sha256 is a placeholder: download
// trusts is_complete and never re-hashes, so the digest value is irrelevant here
// (only its 32-byte length matters for the CHECK).
async fn seed_complete_attachment(
    pool: &PgPool,
    room_name: &str,
    sender_username: &str,
    chunks: &[&[u8]],
) -> Uuid {
    seed_attachment(pool, room_name, sender_username, chunks, true).await
}

async fn seed_attachment(
    pool: &PgPool,
    room_name: &str,
    sender_username: &str,
    chunks: &[&[u8]],
    is_complete: bool,
) -> Uuid {
    let message_id: Uuid = sqlx::query_scalar(
        "INSERT INTO messages (room_id, sender_id, content)
         SELECT r.room_id, u.user_id, 'with file'
         FROM rooms r, users u
         WHERE r.room_name = $1 AND u.username = $2
         RETURNING message_id",
    )
    .bind(room_name)
    .bind(sender_username)
    .fetch_one(pool)
    .await
    .expect("seed message failed");

    let total: i64 = chunks.iter().map(|c| c.len() as i64).sum();
    let attachment_id: Uuid = sqlx::query_scalar(
        "INSERT INTO message_attachments
         (message_id, filename, content_type, size_bytes, chunk_count, content_sha256, is_complete)
         VALUES ($1, 'file.bin', 'application/octet-stream', $2, $3, $4, $5)
         RETURNING attachment_id",
    )
    .bind(message_id)
    .bind(total)
    .bind(chunks.len() as i32)
    .bind(vec![0u8; 32])
    .bind(is_complete)
    .fetch_one(pool)
    .await
    .expect("seed attachment failed");

    for (seq, data) in chunks.iter().enumerate() {
        sqlx::query(
            "INSERT INTO message_attachment_chunks (attachment_id, seq, data)
             VALUES ($1, $2, $3)",
        )
        .bind(attachment_id)
        .bind(seq as i32)
        .bind(*data)
        .execute(pool)
        .await
        .expect("seed chunk failed");
    }

    attachment_id
}

// Send DownloadAttachment and collect the streamed binary chunk frames until the
// terminal AttachmentEnd. Asserts every chunk is for `attachment_id` and that
// seqs arrive contiguous and ascending, then returns the reassembled bytes.
async fn download_and_reassemble(ws: &mut Ws, attachment_id: Uuid) -> Vec<u8> {
    send_cmd(ws, &ClientCommand::DownloadAttachment { attachment_id }).await;

    let mut chunks: Vec<(i32, Vec<u8>)> = Vec::new();
    loop {
        let msg = ws
            .next()
            .await
            .expect("connection closed mid-download")
            .expect("websocket error");

        if msg.is_binary() {
            let frame = msg.into_data();
            assert!(frame.len() > 20, "chunk frame missing header/payload");
            let id = Uuid::from_slice(&frame[0..16]).expect("bad attachment_id in frame");
            assert_eq!(id, attachment_id, "chunk for the wrong attachment");
            let seq = u32::from_be_bytes([frame[16], frame[17], frame[18], frame[19]]) as i32;
            chunks.push((seq, frame[20..].to_vec()));
        } else if msg.is_text() {
            let text = msg.to_text().expect("text frame").to_owned();
            match serde_json::from_str::<ServerEvent>(&text).expect("server event") {
                ServerEvent::AttachmentEnd { attachment_id: end } => {
                    assert_eq!(end, attachment_id);
                    break;
                }
                // Live push events can interleave on a subscribed socket; skip them.
                ServerEvent::NewMessage { .. } | ServerEvent::Resync { .. } => continue,
                other => panic!("unexpected event mid-download: {other:?}"),
            }
        } else {
            panic!("unexpected frame: {msg:?}");
        }
    }

    for (i, (seq, _)) in chunks.iter().enumerate() {
        assert_eq!(*seq, i as i32, "chunks must stream in seq order");
    }
    chunks.into_iter().flat_map(|(_, data)| data).collect()
}

#[sqlx::test]
async fn member_downloads_attachment_in_order(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let parts: [&[u8]; 3] = [b"hello ", b"chunked ", b"world"];
    let attachment_id = seed_complete_attachment(&pool, "general", "alice", &parts).await;

    let server = spawn_app(pool.clone(), |_| {}).await;
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let bytes = download_and_reassemble(&mut ws, attachment_id).await;
    assert_eq!(bytes, b"hello chunked world");

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn any_room_member_can_download_not_just_sender(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    seed_membership(&pool, "general", "bob").await;
    // alice is the sender; bob is a plain member.
    let parts: [&[u8]; 2] = [b"shared ", b"file"];
    let attachment_id = seed_complete_attachment(&pool, "general", "alice", &parts).await;

    let server = spawn_app(pool.clone(), |_| {}).await;
    let mut ws = create_socket(server.addr).await;
    // Download as bob -- read access is room membership, not sender-ship.
    authenticate(&mut ws, "bob", "bobpw").await;

    let bytes = download_and_reassemble(&mut ws, attachment_id).await;
    assert_eq!(bytes, b"shared file");

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn non_member_cannot_download(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "mallory", "mallorypw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let parts: [&[u8]; 1] = [b"secret bytes"];
    let attachment_id = seed_complete_attachment(&pool, "general", "alice", &parts).await;

    let server = spawn_app(pool.clone(), |_| {}).await;
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mallory", "mallorypw").await;

    // mallory is not a member: a generic error, indistinguishable from "no such
    // attachment", and no bytes.
    send_cmd(
        &mut ws,
        &ClientCommand::DownloadAttachment { attachment_id },
    )
    .await;
    assert!(matches!(
        next_reply(&mut ws).await,
        ServerEvent::Error { .. }
    ));

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn incomplete_attachment_is_not_downloadable(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    // Chunks present but the row is still incomplete -- must not be served.
    let parts: [&[u8]; 2] = [b"partial ", b"upload"];
    let attachment_id = seed_attachment(&pool, "general", "alice", &parts, false).await;

    let server = spawn_app(pool.clone(), |_| {}).await;
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::DownloadAttachment { attachment_id },
    )
    .await;
    assert!(matches!(
        next_reply(&mut ws).await,
        ServerEvent::Error { .. }
    ));

    close_socket(&mut ws).await;
}

// Full round trip through the real actors: declare an attachment on a message,
// stream its bytes up as binary chunk frames, wait for the upload actor to verify
// the SHA-256 and flip is_complete (AttachmentComplete), then download it back and
// assert the reassembled bytes are byte-for-byte identical. Unlike the other
// download tests, nothing is hand-seeded into the chunk table -- every byte makes
// the trip up through process_binary and the upload actor and back down the
// download stream, exercising the framing symmetrically on both sides.
#[sqlx::test]
async fn upload_then_download_round_trips_bytes(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;

    let server = spawn_app(pool.clone(), |_| {}).await;
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    // A non-trivial "file" split into uneven chunks: four full 1024B chunks and a
    // short final one, so the last frame differs in length from the rest. It opens
    // with a real PNG signature so the server's content-type policy detects and
    // accepts it as image/png (the filler bytes include NUL, so it can't pass as
    // text); the rest is arbitrary, exercising byte-exact round-tripping.
    let mut original: Vec<u8> = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    original.extend((0..4992u32).map(|i| (i % 251) as u8));
    let parts: Vec<&[u8]> = original.chunks(1024).collect();
    assert_eq!(parts.len(), 5);

    // The declared digest must match what the actor recomputes by streaming the
    // chunks in seq order, or verify() fails and is_complete never flips.
    let mut hasher = Sha256::new();
    hasher.update(&original);
    let content_sha256 = hasher.finalize().to_vec();

    send_cmd(
        &mut ws,
        &ClientCommand::SendMessage {
            room_name: "general".to_owned(),
            content: "with file".to_owned(),
            attachments: vec![NewMessageAttachment {
                filename: "roundtrip.png".to_owned(),
                content_type: "image/png".to_owned(),
                size_bytes: original.len() as i64,
                chunk_count: parts.len() as i32,
                content_sha256,
            }],
        },
    )
    .await;

    let attachment_id = match next_reply(&mut ws).await {
        ServerEvent::MessageCreated { attachment_ids, .. } => {
            assert_eq!(attachment_ids.len(), 1);
            attachment_ids[0]
        }
        other => panic!("expected MessageCreated, got {other:?}"),
    };

    // Stream the bytes up. The upload actor persists each chunk idempotently and,
    // once all are present, verifies size + hash before announcing completion.
    for (seq, part) in parts.iter().enumerate() {
        send_chunk_frame(&mut ws, attachment_id, seq as i32, part).await;
    }

    match next_reply(&mut ws).await {
        ServerEvent::AttachmentComplete { attachment_id: id } => assert_eq!(id, attachment_id),
        other => panic!("expected AttachmentComplete, got {other:?}"),
    }

    // Pull it back down and assert the bytes survived the round trip intact.
    let downloaded = download_and_reassemble(&mut ws, attachment_id).await;
    assert_eq!(downloaded, original);

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn unknown_attachment_yields_generic_error(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let missing = Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
    send_cmd(
        &mut ws,
        &ClientCommand::DownloadAttachment {
            attachment_id: missing,
        },
    )
    .await;
    assert!(matches!(
        next_reply(&mut ws).await,
        ServerEvent::Error { .. }
    ));

    close_socket(&mut ws).await;
}
