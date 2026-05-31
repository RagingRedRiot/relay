mod common;

use common::*;
use relay::model::JoinRequestInfo;
use sqlx::PgPool;

fn approve_cmd(room: &str, requester: &str) -> ClientCommand {
    ClientCommand::ApproveJoinRequest {
        room_name: room.to_owned(),
        requester_username: requester.to_owned(),
    }
}

fn reject_cmd(room: &str, requester: &str) -> ClientCommand {
    ClientCommand::RejectJoinRequest {
        room_name: room.to_owned(),
        requester_username: requester.to_owned(),
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

// ##### Approve #####

// An owner approves a pending request: the requester becomes a member and the
// request is consumed.
#[sqlx::test]
async fn owner_approves_request(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    seed_join_request(&pool, "club", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &approve_cmd("club", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert!(is_member(&pool, "club", "bob").await);
    assert_eq!(request_count(&pool, "club", "bob").await, 0);

    close_socket(&mut ws).await;
}

// Approving when there's no pending request is a no-op (and adds no member).
#[sqlx::test]
async fn approve_without_request_no_change(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &approve_cmd("club", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);
    assert!(!is_member(&pool, "club", "bob").await);

    close_socket(&mut ws).await;
}

// An admin can approve a request for a room they don't own.
#[sqlx::test]
async fn admin_can_approve(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    seed_join_request(&pool, "club", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(&mut ws, &approve_cmd("club", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert!(is_member(&pool, "club", "bob").await);

    close_socket(&mut ws).await;
}

// ##### Reject #####

// An owner rejects a request: it's removed and the requester does not join.
#[sqlx::test]
async fn owner_rejects_request(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    seed_join_request(&pool, "club", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &reject_cmd("club", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert_eq!(request_count(&pool, "club", "bob").await, 0);
    assert!(!is_member(&pool, "club", "bob").await);

    close_socket(&mut ws).await;
}

// ##### Authorization #####

// A non-owner, non-admin member cannot approve; the request survives.
#[sqlx::test]
async fn non_owner_cannot_approve(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "carl", "carlpw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    seed_membership(&pool, "club", "carl").await;
    seed_join_request(&pool, "club", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "carl", "carlpw").await;

    send_cmd(&mut ws, &approve_cmd("club", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    assert_eq!(request_count(&pool, "club", "bob").await, 1);
    assert!(!is_member(&pool, "club", "bob").await);

    close_socket(&mut ws).await;
}

// ##### Get: my requests #####

// GetMyJoinRequests lists the rooms the caller has requested.
#[sqlx::test]
async fn get_my_join_requests_lists_rooms(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    seed_room(&pool, "lounge", "alice", false, true).await;
    seed_join_request(&pool, "club", "bob").await;
    seed_join_request(&pool, "lounge", "bob").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &ClientCommand::GetMyJoinRequests).await;
    match next_event(&mut ws).await {
        ServerEvent::MyJoinRequests { mut rooms } => {
            rooms.sort();
            assert_eq!(rooms, vec!["club".to_owned(), "lounge".to_owned()]);
        }
        other => panic!("expected MyJoinRequests, got {other:?}"),
    }

    close_socket(&mut ws).await;
}

// ##### Get: incoming requests #####

// GetIncomingJoinRequests lists requests for rooms the caller owns (and not for
// rooms owned by others).
#[sqlx::test]
async fn get_incoming_lists_owned_room_requests(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "carl", "carlpw").await;
    seed_room(&pool, "club", "alice", false, true).await; // alice owns
    seed_room(&pool, "other", "carl", false, true).await; // carl owns
    seed_join_request(&pool, "club", "bob").await;
    seed_join_request(&pool, "other", "bob").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &ClientCommand::GetIncomingJoinRequests).await;
    match next_event(&mut ws).await {
        ServerEvent::IncomingJoinRequests { requests } => {
            // Only alice's room, not carl's.
            assert_eq!(
                requests,
                vec![JoinRequestInfo {
                    room_name: "club".to_owned(),
                    username: "bob".to_owned(),
                }]
            );
        }
        other => panic!("expected IncomingJoinRequests, got {other:?}"),
    }

    close_socket(&mut ws).await;
}
