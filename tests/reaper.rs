mod common;

use common::*;
use sqlx::PgPool;
use uuid::Uuid;

// Insert an invite with an explicit age (days old) so reaping can be exercised
// deterministically.
async fn seed_aged_invite(pool: &PgPool, room: &str, invitee: &str, age_days: i64) {
    sqlx::query(
        "INSERT INTO room_invites (room_id, user_id, created_at)
         SELECT r.room_id, u.user_id, now() - make_interval(days => $3)
         FROM rooms r, users u
         WHERE r.room_name = $1 AND u.username = $2",
    )
    .bind(room)
    .bind(invitee)
    .bind(age_days as i32)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_aged_request(pool: &PgPool, room: &str, username: &str, age_days: i64) {
    sqlx::query(
        "INSERT INTO room_join_requests (room_id, user_id, created_at)
         SELECT r.room_id, u.user_id, now() - make_interval(days => $3)
         FROM rooms r, users u
         WHERE r.room_name = $1 AND u.username = $2",
    )
    .bind(room)
    .bind(username)
    .bind(age_days as i32)
    .execute(pool)
    .await
    .unwrap();
}

async fn invite_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM room_invites")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn request_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM room_join_requests")
        .fetch_one(pool)
        .await
        .unwrap()
}

// Invites and join requests older than the retention window are reaped; ones
// inside the window survive.
#[sqlx::test]
async fn reaps_stale_invites_and_requests(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "carl", "carlpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;

    // 40 days old (stale) vs 1 day old (fresh), for both tables.
    seed_aged_invite(&pool, "vault", "bob", 40).await;
    seed_aged_invite(&pool, "vault", "carl", 1).await;
    seed_aged_request(&pool, "vault", "bob", 40).await;
    seed_aged_request(&pool, "vault", "carl", 1).await;

    assert_eq!(invite_count(&pool).await, 2);
    assert_eq!(request_count(&pool).await, 2);

    relay::reaper::reap(&pool, 30).await.unwrap();

    // Only the fresh rows remain.
    assert_eq!(invite_count(&pool).await, 1);
    assert_eq!(request_count(&pool).await, 1);

    let surviving_invitee: String = sqlx::query_scalar(
        "SELECT u.username FROM room_invites ri JOIN users u ON u.user_id = ri.user_id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(surviving_invitee, "carl");
}

// Reaping with nothing stale is a no-op and leaves fresh rows intact.
#[sqlx::test]
async fn reap_keeps_everything_within_window(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;

    seed_aged_invite(&pool, "vault", "bob", 5).await;
    seed_aged_request(&pool, "vault", "bob", 5).await;

    relay::reaper::reap(&pool, 30).await.unwrap();

    assert_eq!(invite_count(&pool).await, 1);
    assert_eq!(request_count(&pool).await, 1);
}

// A message young enough to survive the retention window, returning its id so
// attachments can hang off it. timestamp drives the message reaper.
async fn seed_message(pool: &PgPool, room: &str, sender: &str, age_days: i64) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO messages (room_id, sender_id, content, timestamp)
         SELECT r.room_id, u.user_id, 'with file', now() - make_interval(days => $3)
         FROM rooms r, users u
         WHERE r.room_name = $1 AND u.username = $2
         RETURNING message_id",
    )
    .bind(room)
    .bind(sender)
    .bind(age_days as i32)
    .fetch_one(pool)
    .await
    .unwrap()
}

// One attachment on `message_id` plus a single chunk, aged via created_at (which
// the incomplete-upload sweep keys on). is_complete decides whether the sweep can
// touch it at all.
async fn seed_attachment(pool: &PgPool, message_id: Uuid, is_complete: bool, age_hours: i64) {
    let attachment_id: Uuid = sqlx::query_scalar(
        "INSERT INTO message_attachments
         (message_id, filename, content_type, size_bytes, chunk_count, content_sha256,
          is_complete, created_at)
         VALUES ($1, 'file.bin', 'application/octet-stream', 1, 1, $2, $3,
                 now() - make_interval(hours => $4))
         RETURNING attachment_id",
    )
    .bind(message_id)
    .bind(vec![0u8; 32])
    .bind(is_complete)
    .bind(age_hours as i32)
    .fetch_one(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO message_attachment_chunks (attachment_id, seq, data) VALUES ($1, 0, $2)",
    )
    .bind(attachment_id)
    .bind(vec![1u8])
    .execute(pool)
    .await
    .unwrap();
}

async fn attachment_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM message_attachments")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn chunk_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM message_attachment_chunks")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn message_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(pool)
        .await
        .unwrap()
}

// The sweep reclaims uploads abandoned mid-stream (incomplete, past the grace
// period) and their chunks, while leaving completed attachments and still-resumable
// recent partials alone -- and without touching the young parent message.
#[sqlx::test]
async fn reaps_abandoned_uploads_only(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "vault", "alice", false, false).await;

    // Parent message is well inside the retention window, so the message rule won't
    // take it (and cascade its attachments) -- the incomplete sweep must stand alone.
    let message_id = seed_message(&pool, "vault", "alice", 2).await;

    seed_attachment(&pool, message_id, false, 48).await; // abandoned, past grace -> reaped
    seed_attachment(&pool, message_id, false, 1).await; // recent partial -> still resumable
    seed_attachment(&pool, message_id, true, 48).await; // completed -> ages out with message

    assert_eq!(attachment_count(&pool).await, 3);
    assert_eq!(chunk_count(&pool).await, 3);

    relay::reaper::reap(&pool, 30).await.unwrap();

    // Only the aged incomplete row (and its chunk) is gone.
    assert_eq!(attachment_count(&pool).await, 2);
    assert_eq!(chunk_count(&pool).await, 2);
    // The message and its remaining (complete + recent) attachments survive.
    assert_eq!(message_count(&pool).await, 1);

    let surviving_incomplete: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM message_attachments WHERE NOT is_complete")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(surviving_incomplete, 1);
}
