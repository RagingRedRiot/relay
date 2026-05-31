mod common;

use common::*;
use sqlx::PgPool;

fn add_owner_cmd(room: &str, new_owner: &str) -> ClientCommand {
    ClientCommand::AddRoomOwner {
        room_name: room.to_owned(),
        new_owner_username: new_owner.to_owned(),
    }
}

// ##### Happy path #####

// An owner grants co-ownership to an existing member: Success, the member
// becomes an owner, and the granting owner stays an owner (additive grant).
#[sqlx::test]
async fn owner_grants_co_ownership(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "room", "alice", false, true).await;
    seed_membership(&pool, "room", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &add_owner_cmd("room", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    assert!(is_owner(&pool, "room", "bob").await);
    assert!(is_owner(&pool, "room", "alice").await); // additive

    close_socket(&mut ws).await;
}

// An admin who is neither owner nor member can still grant ownership.
#[sqlx::test]
async fn admin_can_grant(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "room", "alice", false, true).await;
    seed_membership(&pool, "room", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(&mut ws, &add_owner_cmd("room", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert!(is_owner(&pool, "room", "bob").await);

    close_socket(&mut ws).await;
}

// ##### Idempotency #####

// Granting ownership to someone who is already an owner is a no-op: NoChange,
// distinct from the "not a member" failure.
#[sqlx::test]
async fn grant_to_existing_owner_returns_no_change(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "room", "alice", false, true).await;
    seed_membership(&pool, "room", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    // First grant succeeds...
    send_cmd(&mut ws, &add_owner_cmd("room", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    // ...granting again is a no-op.
    send_cmd(&mut ws, &add_owner_cmd("room", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);

    close_socket(&mut ws).await;
}

// ##### Membership requirement #####

// The new owner must already be a member: granting to a non-member fails and
// creates no membership.
#[sqlx::test]
async fn grant_to_non_member_fails(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "room", "alice", false, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &add_owner_cmd("room", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    assert!(!is_member(&pool, "room", "bob").await);

    close_socket(&mut ws).await;
}

// ##### Authorization #####

// A plain member (non-owner, non-admin) cannot grant ownership.
#[sqlx::test]
async fn non_owner_member_cannot_grant(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "carl", "carlpw").await;
    seed_room(&pool, "room", "alice", false, true).await;
    seed_membership(&pool, "room", "bob").await;
    seed_membership(&pool, "room", "carl").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    // carl is a member but not an owner.
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "carl", "carlpw").await;

    send_cmd(&mut ws, &add_owner_cmd("room", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    assert!(!is_owner(&pool, "room", "bob").await);

    close_socket(&mut ws).await;
}

// A non-owner cannot probe room existence: a real room they don't own and a
// nonexistent room both return the same Failed.
#[sqlx::test]
async fn nonexistent_room_fails_like_unauthorized(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &add_owner_cmd("ghost", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}
