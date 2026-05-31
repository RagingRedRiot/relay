mod common;

use common::*;
use sqlx::PgPool;

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
