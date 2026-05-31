mod common;

use common::*;
use sqlx::PgPool;

fn join_cmd(room: &str) -> ClientCommand {
    ClientCommand::JoinRoom {
        room_name: room.to_owned(),
    }
}

fn leave_cmd(room: &str) -> ClientCommand {
    ClientCommand::LeaveRoom {
        room_name: room.to_owned(),
    }
}

async fn pending_request_count(pool: &PgPool, room: &str, username: &str) -> i64 {
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

// ##### Public: join now #####

// Joining a public room adds the caller as a plain (non-owner) member.
#[sqlx::test]
async fn join_public_room_adds_member(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &join_cmd("general")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert!(is_member(&pool, "general", "bob").await);
    assert!(!is_owner(&pool, "general", "bob").await);

    // Joining again is a no-op.
    send_cmd(&mut ws, &join_cmd("general")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);

    close_socket(&mut ws).await;
}

// ##### Private + discoverable: request #####

// Joining a discoverable private room queues a request rather than joining, and
// a duplicate attempt is a no-op.
#[sqlx::test]
async fn join_private_discoverable_queues_request(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &join_cmd("club")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::JoinRequested);
    assert_eq!(pending_request_count(&pool, "club", "bob").await, 1);
    assert!(!is_member(&pool, "club", "bob").await);

    // Re-requesting is a no-op.
    send_cmd(&mut ws, &join_cmd("club")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);
    assert_eq!(pending_request_count(&pool, "club", "bob").await, 1);

    close_socket(&mut ws).await;
}

// ##### Private + non-discoverable: invite-only, hidden #####

// A non-discoverable private room is invite-only: a join attempt reports
// NoRoomExists (hiding it) and queues no request.
#[sqlx::test]
async fn join_private_hidden_room_is_invite_only(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &join_cmd("vault")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoRoomExists);
    assert_eq!(pending_request_count(&pool, "vault", "bob").await, 0);

    // Indistinguishable from a room that doesn't exist.
    send_cmd(&mut ws, &join_cmd("ghost")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoRoomExists);

    close_socket(&mut ws).await;
}

// ##### Leave #####

// Leaving removes the caller's membership; leaving a room you're not in is a
// no-op.
#[sqlx::test]
async fn leave_removes_membership(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    seed_membership(&pool, "general", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &leave_cmd("general")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert!(!is_member(&pool, "general", "bob").await);

    // Leaving again is a no-op.
    send_cmd(&mut ws, &leave_cmd("general")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);

    close_socket(&mut ws).await;
}
