use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::hub::Hub;
use crate::model::ServerEvent;

// Max chunk writes in flight across all uploads, so an upload burst can't drain
// the shared PgPool and starve message/room/user queries.
pub const MAX_CONCURRENT_CHUNK_WRITES: usize = 16;

// Same bound for the read side: a download burst can't drain the pool either.
pub const MAX_CONCURRENT_CHUNK_READS: usize = 16;

// Fixed header on every chunk frame: [attachment_id 16B][seq u32 BE 4B]. The
// payload follows. Shared so the wire parser, the transport size cap, and the
// advertised max-chunk-size all agree on the overhead.
pub const CHUNK_HEADER_LEN: usize = 20;

// How long an upload may sit idle (no new chunk) before its actor gives up. The
// partial chunks persist in the DB for resume; the reaper is the slow backstop.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

// One chunk handed from the session to its upload actor. The actor already knows
// its attachment_id, so only the order index and bytes travel.
pub struct Chunk {
    pub seq: i32,
    pub data: Vec<u8>,
}

// #### Content-type policy ####
//
// A completed upload is checked against a magic-byte sniff of its own contents
// before it is published, so the stored content_type is trustworthy regardless of
// what the client declared. Detection is authoritative: a mislabeled but supported
// file is corrected to its real type, and an unsupported or smuggled file is
// rejected outright.
//
// Types whose bytes carry a recognizable magic signature. infer detects these; the
// detected type is stored verbatim, overriding any (possibly false) declaration.
const SUPPORTED_BINARY_TYPES: &[&str] = &[
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "application/pdf",
    "application/zip",
];

// Text-like types that have no magic signature (infer returns nothing), so the
// specific type can't be detected and the client's declaration is the only source.
// Allowed only when the declared type is one of these AND the bytes don't look
// binary, which keeps arbitrary binaries from being smuggled in as "text".
const SUPPORTED_TEXT_TYPES: &[&str] = &[
    "text/plain",
    "text/csv",
    "text/markdown",
    "application/json",
    "image/svg+xml",
];

// Resolve the content_type to store for a completed upload, sniffing its leading
// bytes. Ok(type) is the authoritative type to persist (the detected type for
// magic formats; the declared type for magicless text). Err carries a short,
// client-safe rejection reason.
fn resolve_content_type(declared: &str, header: &[u8]) -> Result<String, String> {
    // Normalize: drop any parameters (e.g. "; charset=utf-8") and lowercase.
    let declared = declared
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();

    match infer::get(header) {
        Some(kind) => {
            let detected = kind.mime_type();
            if !SUPPORTED_BINARY_TYPES.contains(&detected) {
                return Err(format!("unsupported file type ({detected})"));
            }
            // Detection wins: store the true type, correcting any mislabeling.
            Ok(detected.to_owned())
        }
        None => {
            // No magic signature -> only the declared text-like allowlist is valid,
            // and a NUL byte in the header means it isn't really text.
            if !SUPPORTED_TEXT_TYPES.contains(&declared.as_str()) {
                return Err(format!(
                    "unsupported or unrecognized file type ({declared})"
                ));
            }
            if header.contains(&0u8) {
                return Err("declared a text type but the content is binary".to_owned());
            }
            Ok(declared)
        }
    }
}

// Session-held handle to a single in-flight upload actor. Bound to one attachment:
// it can only ever feed chunks to that attachment's actor.
pub struct AttachmentHandle {
    sender: mpsc::Sender<Chunk>,
}

impl AttachmentHandle {
    // Forward a chunk to the actor. Err (with the chunk returned) means the actor
    // has exited (completed or timed out) and the caller should respawn to resume.
    pub async fn route(&self, chunk: Chunk) -> Result<(), mpsc::error::SendError<Chunk>> {
        self.sender.send(chunk).await
    }
}

// Spawn a per-upload actor bound to `attachment_id`. The caller (the session) must
// have already confirmed the user owns the attachment; this actor performs no auth
// and can only ever write chunks for this one attachment (capability confinement).
// It is disposable -- it exits on completion, idle timeout, the session dropping
// the handle, or global shutdown -- and resume just respawns it.
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    attachment_id: Uuid,
    chunk_count: i32,
    size_bytes: i64,
    content_sha256: Vec<u8>,
    content_type: String,
    pool: PgPool,
    write_semaphore: Arc<Semaphore>,
    user_tx: mpsc::Sender<ServerEvent>,
    hub: Hub,
    shutdown: CancellationToken,
) -> AttachmentHandle {
    let (tx, mut rx) = mpsc::channel::<Chunk>(8);

    tokio::spawn(async move {
        loop {
            let chunk = tokio::select! {
                _ = shutdown.cancelled() => break,
                res = timeout(IDLE_TIMEOUT, rx.recv()) => match res {
                    Ok(Some(chunk)) => chunk,
                    Ok(None) => break, // session dropped the handle
                    Err(_) => break,   // idle timeout -> abandon; partials persist
                },
            };

            // Drop out-of-range seqs so that "count == chunk_count" later implies a
            // contiguous 0..chunk_count-1 set. Bad/buggy client.
            if chunk.seq < 0 || chunk.seq >= chunk_count {
                continue;
            }

            // Persist under a write permit so uploads can't exhaust the pool. The
            // insert is idempotent: a re-sent seq is a no-op (keep-first).
            let permit = match write_semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => break, // semaphore closed -> shutting down
            };
            let inserted = sqlx::query(
                "INSERT INTO message_attachment_chunks (attachment_id, seq, data)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (attachment_id, seq) DO NOTHING",
            )
            .bind(attachment_id)
            .bind(chunk.seq)
            .bind(&chunk.data)
            .execute(&pool)
            .await;
            drop(permit);

            let inserted = match inserted {
                Ok(result) => result,
                Err(e) => {
                    // Often a benign FK-gone (the attachment was reaped mid-upload);
                    // could also be a real DB error. Either way, abandon this upload.
                    tracing::warn!(error = %e, %attachment_id, "upload: chunk insert failed (attachment reaped or DB error)");
                    break;
                }
            };

            // A duplicate re-send makes no progress -- don't bother re-counting.
            if inserted.rows_affected() == 0 {
                continue;
            }

            // All chunks present yet? Cheap count; the bytes stay out-of-line (TOAST).
            let count: i64 = match sqlx::query_scalar(
                "SELECT COUNT(*) FROM message_attachment_chunks WHERE attachment_id = $1",
            )
            .bind(attachment_id)
            .fetch_one(&pool)
            .await
            {
                Ok(count) => count,
                Err(e) => {
                    tracing::error!(error = %e, %attachment_id, "upload: chunk count query failed");
                    break;
                }
            };

            if count < chunk_count as i64 {
                continue;
            }

            // Every chunk is in. Verify by streaming them in seq order through the
            // hasher (no reassembly), checking total size and the declared digest.
            match verify(&pool, attachment_id, size_bytes, &content_sha256).await {
                Ok(true) => {
                    // Bytes match the declared hash/size; now resolve the file's
                    // true format. Magic detection only reads the header, so the
                    // leading bytes (chunk 0) are enough -- no full reassembly.
                    let header = match sniff_header(&pool, attachment_id).await {
                        Ok(header) => header,
                        Err(e) => {
                            tracing::error!(error = %e, %attachment_id, "upload: header sniff query failed");
                            break;
                        }
                    };

                    let final_type = match resolve_content_type(&content_type, &header) {
                        Ok(t) => t,
                        Err(reason) => {
                            // Unsupported format or a type-mismatch we won't accept.
                            // Cancel the upload: delete the attachment (chunks cascade)
                            // and, if that leaves its message with nothing attached,
                            // delete the message too and fan removal details out to the
                            // room -- otherwise it lingers as a bare filename or dead
                            // attachment for every member. Then tell the uploader exactly
                            // what failed and why.
                            tracing::warn!(%attachment_id, %reason, "upload: rejected by content-type policy");
                            let fanned_out =
                                cancel_rejected_upload(&pool, &hub, attachment_id, reason.clone())
                                    .await;
                            if !fanned_out {
                                let _ = user_tx
                                    .send(ServerEvent::AttachmentRejected {
                                        attachment_id,
                                        reason,
                                    })
                                    .await;
                            }
                            break;
                        }
                    };

                    // CAS flip: exactly one path wins the monotonic false->true and
                    // announces completion, correcting the stored type to the
                    // detected one in the same statement; a racing actor gets 0 rows
                    // and is silent.
                    let won = sqlx::query_scalar::<_, Uuid>(
                        "UPDATE message_attachments
                            SET is_complete = true, content_type = $2
                            WHERE attachment_id = $1 AND NOT is_complete
                            RETURNING attachment_id",
                    )
                    .bind(attachment_id)
                    .bind(&final_type)
                    .fetch_optional(&pool)
                    .await;

                    if let Ok(Some(_)) = won {
                        let _ = user_tx
                            .send(ServerEvent::AttachmentComplete { attachment_id })
                            .await;
                    }
                    break;
                }
                Ok(false) => {
                    // All chunks present but content doesn't match the declared
                    // hash/size. Keep-first means re-sending can't fix it; leave the
                    // row incomplete for the reaper and report a generic failure.
                    // Details stay server-side; the client only sees a generic error.
                    tracing::warn!(%attachment_id, "upload: content does not match declared hash/size");
                    let _ = user_tx
                        .send(ServerEvent::Error {
                            error: "attachment upload failed".to_owned(),
                        })
                        .await;
                    break;
                }
                Err(e) => {
                    tracing::error!(error = %e, %attachment_id, "upload: verification query failed");
                    break;
                }
            }
        }
    });

    AttachmentHandle { sender: tx }
}

// Spawn a disposable task that streams one attachment's bytes back to its
// requester. Unlike upload there is no actor, no registry, and no coordination:
// the chunk rows are immutable and read-only, so a download is just auth-check
// then stream. The task ends on completion, an auth/DB miss, or the session
// dropping the receiving end of `user_tx` -- no shutdown token is needed, since a
// dead session makes the next send fail and stops the task.
pub fn download(
    attachment_id: Uuid,
    requester_id: Uuid,
    pool: PgPool,
    read_semaphore: Arc<Semaphore>,
    user_tx: mpsc::Sender<ServerEvent>,
) {
    tokio::spawn(async move {
        // Auth + lookup in one shot: the requester must be a member of the
        // attachment's room, and the attachment must be complete (never serve a
        // partial upload). A miss on any of these is indistinguishable -- one
        // generic error -- so neither the attachment's existence nor the room's
        // membership leaks. This is a per-request JIT check ([[feedback_jit_authorization]])
        // because room membership is revocable, unlike the immutable sender-ship
        // the upload path checks.
        let chunk_count = match sqlx::query_scalar::<_, i32>(
            "SELECT a.chunk_count
                FROM message_attachments a
                JOIN messages m ON m.message_id = a.message_id
                WHERE a.attachment_id = $1
                  AND a.is_complete
                  AND EXISTS (SELECT 1 FROM memberships mb
                              WHERE mb.room_id = m.room_id AND mb.user_id = $2)",
        )
        .bind(attachment_id)
        .bind(requester_id)
        .fetch_optional(&pool)
        .await
        {
            Ok(Some(count)) => count,
            Ok(None) => {
                let _ = user_tx
                    .send(ServerEvent::Error {
                        error: "attachment unavailable".to_owned(),
                    })
                    .await;
                return;
            }
            Err(e) => {
                tracing::error!(error = %e, %attachment_id, "download: lookup query failed");
                let _ = user_tx.send(ServerEvent::Failed).await;
                return;
            }
        };

        // Stream chunks in seq order. Each chunk is fetched by its PK and the DB
        // connection released before the (possibly slow) send, so a slow client
        // never pins a pool connection across the wire -- the bounded user_tx
        // channel is the backpressure. Each fetch takes a read permit so a
        // download burst can't exhaust the pool, symmetric with the write path.
        for seq in 0..chunk_count {
            let permit = match read_semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => return, // semaphore closed -> shutting down
            };
            let data = sqlx::query_scalar::<_, Vec<u8>>(
                "SELECT data FROM message_attachment_chunks
                    WHERE attachment_id = $1 AND seq = $2",
            )
            .bind(attachment_id)
            .bind(seq)
            .fetch_optional(&pool)
            .await;
            drop(permit);

            let data = match data {
                Ok(Some(data)) => data,
                // A gap in a "complete" attachment shouldn't be possible; stop and
                // report a generic failure rather than send a truncated stream.
                Ok(None) => {
                    tracing::error!(%attachment_id, seq, "download: missing chunk in a complete attachment");
                    let _ = user_tx.send(ServerEvent::Failed).await;
                    return;
                }
                Err(e) => {
                    tracing::error!(error = %e, %attachment_id, seq, "download: chunk fetch failed");
                    let _ = user_tx.send(ServerEvent::Failed).await;
                    return;
                }
            };

            if user_tx
                .send(ServerEvent::AttachmentChunk {
                    attachment_id,
                    seq,
                    data,
                })
                .await
                .is_err()
            {
                return; // session gone
            }
        }

        let _ = user_tx
            .send(ServerEvent::AttachmentEnd { attachment_id })
            .await;
    });
}

// Read the attachment's leading bytes (chunk 0) for magic-byte sniffing. Magic
// detection only inspects a file's header, so the first chunk is always enough
// and the rest of the file never has to be reassembled in memory.
async fn sniff_header(pool: &PgPool, attachment_id: Uuid) -> Result<Vec<u8>, sqlx::Error> {
    let header: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT data FROM message_attachment_chunks
            WHERE attachment_id = $1 AND seq = 0",
    )
    .bind(attachment_id)
    .fetch_optional(pool)
    .await?;
    Ok(header.unwrap_or_default())
}

// Roll back an upload the content-type policy rejected. Delete the attachment row
// (its chunks cascade via ON DELETE CASCADE), and if that leaves the parent
// message with no attachments at all -- the common case, since a file post's
// caption defaults to the filename, so the message is nothing but the file --
// delete the message too and broadcast MessageRemoved so it doesn't sit in the
// room as a bare filename for every member. A message with surviving attachments
// is kept, but the rejected attachment is still fanned out to the room so live
// clients can remove that dead attachment from the message. Best-effort: a DB
// hiccup here at worst leaves an incomplete row for the reaper, never a served
// file (the row never reached is_complete).
async fn cancel_rejected_upload(
    pool: &PgPool,
    hub: &Hub,
    attachment_id: Uuid,
    reason: String,
) -> bool {
    // Resolve the parent message and room before the attachment row is gone, so we
    // can target the room broadcast after the deletes.
    let parent = sqlx::query_as::<_, (Uuid, Uuid, String)>(
        "SELECT m.message_id, m.room_id, r.room_name
            FROM message_attachments a
            JOIN messages m ON m.message_id = a.message_id
            JOIN rooms r ON r.room_id = m.room_id
            WHERE a.attachment_id = $1",
    )
    .bind(attachment_id)
    .fetch_optional(pool)
    .await;

    if let Err(e) = sqlx::query("DELETE FROM message_attachments WHERE attachment_id = $1")
        .bind(attachment_id)
        .execute(pool)
        .await
    {
        tracing::error!(error = %e, %attachment_id, "upload: failed to delete rejected attachment");
        return false;
    }

    let (message_id, room_id, room_name) = match parent {
        Ok(Some(row)) => row,
        // Already gone (reaped or a race): the attachment is deleted, nothing to fan out.
        Ok(None) => return false,
        Err(e) => {
            tracing::error!(error = %e, %attachment_id, "upload: parent message lookup failed");
            return false;
        }
    };

    hub.publish(
        room_id,
        ServerEvent::AttachmentRejected {
            attachment_id,
            reason,
        },
    );

    // Only orphan-delete the message when the rejected file was its only attachment.
    let remaining: i64 =
        match sqlx::query_scalar("SELECT COUNT(*) FROM message_attachments WHERE message_id = $1")
            .bind(message_id)
            .fetch_one(pool)
            .await
        {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(error = %e, %message_id, "upload: sibling-attachment count failed");
                return true;
            }
        };
    if remaining > 0 {
        return true;
    }

    if let Err(e) = sqlx::query("DELETE FROM messages WHERE message_id = $1")
        .bind(message_id)
        .execute(pool)
        .await
    {
        tracing::error!(error = %e, %message_id, "upload: failed to delete orphaned message");
        return true;
    }

    hub.publish(
        room_id,
        ServerEvent::MessageRemoved {
            room_name,
            message_id,
        },
    );
    true
}

// Stream the attachment's chunks in seq order, summing length and hashing, and
// compare against the declared size and SHA-256. Never holds the whole file in
// memory.
async fn verify(
    pool: &PgPool,
    attachment_id: Uuid,
    size_bytes: i64,
    content_sha256: &[u8],
) -> Result<bool, sqlx::Error> {
    let mut hasher = Sha256::new();
    let mut total: i64 = 0;

    let mut chunks = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT data FROM message_attachment_chunks
            WHERE attachment_id = $1 ORDER BY seq",
    )
    .bind(attachment_id)
    .fetch(pool);

    while let Some(data) = chunks.try_next().await? {
        total += data.len() as i64;
        hasher.update(data);
    }

    Ok(total == size_bytes && hasher.finalize().as_slice() == content_sha256)
}
