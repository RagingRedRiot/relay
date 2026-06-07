mod common;

use common::*;
use relay::model::NewMessageAttachment;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

// The framework's own default max message size, minus the 20-byte chunk header --
// what an unconfigured server should advertise as the max chunk payload.
fn framework_default_payload() -> usize {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size
        .expect("framework default is a concrete size")
        - 20
}

// Declare a single-chunk attachment on a fresh message in `room_id`, sized and
// hashed for `payload`, and return its attachment_id. The bytes still have to be
// streamed up separately; this just creates the (incomplete) row to stream into.
async fn declare_single_chunk(ws: &mut Ws, room_name: &str, payload: &[u8]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    send_cmd(
        ws,
        &ClientCommand::SendMessage {
            room_name: room_name.to_owned(),
            content: "file".to_owned(),
            attachments: vec![NewMessageAttachment {
                // These chunk-sizing tests use repeated 0x07 bytes, which have no
                // magic signature and no NUL, so the content-type policy accepts
                // them as the declared text type. Content is incidental here.
                filename: "f.txt".to_owned(),
                content_type: "text/plain".to_owned(),
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

#[sqlx::test]
async fn default_matches_framework_default(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    // An unconfigured server reports the framework default (minus header), so a
    // client gets out-of-the-box axum/tungstenite behavior with no surprises.
    send_cmd(&mut ws, &ClientCommand::GetMaxChunkSize).await;
    assert_eq!(
        next_reply(&mut ws).await,
        ServerEvent::MaxChunkSize {
            bytes: framework_default_payload()
        }
    );

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn configured_value_is_reported(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    // Operator pins a smaller cap; the query reflects exactly that.
    let server = spawn_app(pool.clone(), |c| c.max_chunk_bytes = 4096).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &ClientCommand::GetMaxChunkSize).await;
    assert_eq!(
        next_reply(&mut ws).await,
        ServerEvent::MaxChunkSize { bytes: 4096 }
    );

    close_socket(&mut ws).await;
}

// Boundary, accept side: a chunk whose payload is *exactly* the configured cap
// rides through (frame = 4096 payload + 20 header = 4116 = transport limit) and
// the upload completes. Paired with the reject test below, this pins down the
// header accounting -- an off-by-one (counting the header twice, or not at all)
// would break one side or the other.
#[sqlx::test]
async fn chunk_exactly_at_limit_completes(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |c| c.max_chunk_bytes = 4096).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let payload = vec![7u8; 4096];
    let attachment_id = declare_single_chunk(&mut ws, "general", &payload).await;
    send_chunk_frame(&mut ws, attachment_id, 0, &payload).await;

    match next_reply(&mut ws).await {
        ServerEvent::AttachmentComplete { attachment_id: id } => assert_eq!(id, attachment_id),
        other => panic!("expected AttachmentComplete, got {other:?}"),
    }

    close_socket(&mut ws).await;
}

// Boundary, reject side: one byte past the cap (frame = 4097 + 20 = 4117, over
// the 4116 transport limit) is dropped by the transport before our code sees it,
// so the connection closes rather than completing the upload.
#[sqlx::test]
async fn chunk_one_byte_over_limit_drops_the_connection(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |c| c.max_chunk_bytes = 4096).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let payload = vec![7u8; 4097];
    let attachment_id = declare_single_chunk(&mut ws, "general", &payload).await;
    send_chunk_frame(&mut ws, attachment_id, 0, &payload).await;

    // We never get a normal ServerEvent back -- the connection closes. The drop
    // surfaces as end of stream, a stream error, or a Close frame, depending on
    // timing; any of the three is the rejection we expect. A live NewMessage echo
    // of the message we just posted can arrive first, so skip those.
    loop {
        match ws.next().await {
            None | Some(Err(_)) => break,
            Some(Ok(msg)) if msg.is_close() => break,
            Some(Ok(msg)) if msg.is_text() => {
                let text = msg.to_text().expect("text frame");
                match serde_json::from_str::<ServerEvent>(text) {
                    Ok(ServerEvent::NewMessage { .. }) | Ok(ServerEvent::Resync { .. }) => continue,
                    other => panic!("expected the connection to drop, got {other:?}"),
                }
            }
            Some(Ok(msg)) => panic!("expected the connection to drop, got {msg:?}"),
        }
    }
}

// Going *larger* than the framework default actually works -- not just the
// advertised number, but the enforced transport cap. A 17 MiB payload exceeds
// tungstenite's 16 MiB default frame size, so under framework defaults this
// single-frame chunk would be dropped; configuring a bigger cap lets it complete.
#[sqlx::test]
async fn chunk_larger_than_framework_default_is_accepted_when_configured(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |c| c.max_chunk_bytes = 18 * 1024 * 1024).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    let payload = vec![7u8; 17 * 1024 * 1024];
    let attachment_id = declare_single_chunk(&mut ws, "general", &payload).await;
    send_chunk_frame(&mut ws, attachment_id, 0, &payload).await;

    match next_reply(&mut ws).await {
        ServerEvent::AttachmentComplete { attachment_id: id } => assert_eq!(id, attachment_id),
        other => panic!("expected AttachmentComplete, got {other:?}"),
    }

    close_socket(&mut ws).await;
}
