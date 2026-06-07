mod common;

use common::*;
use sqlx::PgPool;

fn cancel_cmd(room: &str) -> ClientCommand {
    ClientCommand::CancelJoinRequest {
        room_name: room.to_owned(),
    }
}

// Seed a pending join request directly, bypassing the join protocol.
async fn seed_join_request(pool: &PgPool, room: &str, username: &str) {
    sqlx::query(
        "INSERT INTO room_join_requests (room_id, user_id)
         SELECT r.room_id, u.user_id FROM rooms r, users u
         WHERE r.room_name = $1 AND u.username = $2",
    )
    .bind(room)
    .bind(username)
    .execute(pool)
    .await
    .unwrap();
}

async fn request_count(pool: &PgPool, room: &str, username: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM room_join_requests jr
         JOIN rooms r ON r.room_id = jr.room_id
         JOIN users u ON u.user_id = jr.user_id
         WHERE r.room_name = $1 AND u.username = $2",
    )
    .bind(room)
    .bind(username)
    .fetch_one(pool)
    .await
    .unwrap()
}

// The requester withdraws their own pending request: it's gone.
#[sqlx::test]
async fn requester_cancels_own_request(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    seed_join_request(&pool, "club", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &cancel_cmd("club")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert_eq!(request_count(&pool, "club", "bob").await, 0);
    // Cancelling a request never makes the caller a member.
    assert!(!is_member(&pool, "club", "bob").await);

    close_socket(&mut ws).await;
}

// Cancelling with no pending request is a no-op.
#[sqlx::test]
async fn cancel_without_request_no_change(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &cancel_cmd("club")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);

    close_socket(&mut ws).await;
}

// A cancel only withdraws the caller's own request, not another user's.
#[sqlx::test]
async fn cancel_only_affects_own_request(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "carol", "carolpw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    seed_join_request(&pool, "club", "bob").await;
    seed_join_request(&pool, "club", "carol").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &cancel_cmd("club")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert_eq!(request_count(&pool, "club", "bob").await, 0);
    // carol's request is untouched.
    assert_eq!(request_count(&pool, "club", "carol").await, 1);

    close_socket(&mut ws).await;
}
