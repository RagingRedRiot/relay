mod common;

use common::*;
use sqlx::PgPool;

fn invite_cmd(room: &str, invitee: &str) -> ClientCommand {
    ClientCommand::InviteToRoom {
        room_name: room.to_owned(),
        invitee_username: invitee.to_owned(),
    }
}

fn accept_cmd(room: &str) -> ClientCommand {
    ClientCommand::AcceptInvite {
        room_name: room.to_owned(),
    }
}

fn decline_cmd(room: &str) -> ClientCommand {
    ClientCommand::DeclineInvite {
        room_name: room.to_owned(),
    }
}

async fn seed_invite(pool: &PgPool, room: &str, invitee: &str) {
    sqlx::query(
        "INSERT INTO room_invites (room_id, user_id)
         SELECT r.room_id, u.user_id FROM rooms r, users u
         WHERE r.room_name = $1 AND u.username = $2",
    )
    .bind(room)
    .bind(invitee)
    .execute(pool)
    .await
    .unwrap();
}

async fn invite_count(pool: &PgPool, room: &str, invitee: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM room_invites ri
         JOIN rooms r ON r.room_id = ri.room_id
         JOIN users u ON u.user_id = ri.user_id
         WHERE r.room_name = $1 AND u.username = $2",
    )
    .bind(room)
    .bind(invitee)
    .fetch_one(pool)
    .await
    .unwrap()
}

// ##### Inviting #####

// An owner invites a user; the invite is recorded and records the inviter.
#[sqlx::test]
async fn owner_invites_user(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &invite_cmd("vault", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert_eq!(invite_count(&pool, "vault", "bob").await, 1);

    let invited_by: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT ri.invited_by FROM room_invites ri
         JOIN rooms r ON r.room_id = ri.room_id
         JOIN users u ON u.user_id = ri.user_id
         WHERE r.room_name = $1 AND u.username = $2",
    )
    .bind("vault")
    .bind("bob")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(invited_by.is_some());

    // Re-inviting is a no-op.
    send_cmd(&mut ws, &invite_cmd("vault", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);

    close_socket(&mut ws).await;
}

// Inviting an existing member is a no-op.
#[sqlx::test]
async fn invite_existing_member_no_change(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    seed_membership(&pool, "vault", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &invite_cmd("vault", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);
    assert_eq!(invite_count(&pool, "vault", "bob").await, 0);

    close_socket(&mut ws).await;
}

// A non-owner cannot invite.
#[sqlx::test]
async fn non_owner_cannot_invite(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "carl", "carlpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    seed_membership(&pool, "vault", "carl").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "carl", "carlpw").await;

    send_cmd(&mut ws, &invite_cmd("vault", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    assert_eq!(invite_count(&pool, "vault", "bob").await, 0);

    close_socket(&mut ws).await;
}

// ##### Accept / decline #####

// Accepting an invite to a non-discoverable (invite-only) room joins it -- the
// whole point of the invite path. The invite is consumed.
#[sqlx::test]
async fn accept_invite_joins_invite_only_room(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    seed_invite(&pool, "vault", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &accept_cmd("vault")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert!(is_member(&pool, "vault", "bob").await);
    assert_eq!(invite_count(&pool, "vault", "bob").await, 0);

    close_socket(&mut ws).await;
}

// Accepting with no pending invite is a no-op and doesn't join.
#[sqlx::test]
async fn accept_without_invite_no_change(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &accept_cmd("vault")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);
    assert!(!is_member(&pool, "vault", "bob").await);

    close_socket(&mut ws).await;
}

// Declining removes the invite without joining.
#[sqlx::test]
async fn decline_removes_invite(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    seed_invite(&pool, "vault", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &decline_cmd("vault")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert_eq!(invite_count(&pool, "vault", "bob").await, 0);
    assert!(!is_member(&pool, "vault", "bob").await);

    close_socket(&mut ws).await;
}

// ##### Get: my invites #####

// GetMyInvites lists the rooms the caller has been invited to.
#[sqlx::test]
async fn get_my_invites_lists_rooms(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "vault", "alice", false, false).await;
    seed_room(&pool, "lab", "alice", false, false).await;
    seed_invite(&pool, "vault", "bob").await;
    seed_invite(&pool, "lab", "bob").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &ClientCommand::GetMyInvites).await;
    match next_event(&mut ws).await {
        ServerEvent::MyInvites { mut rooms } => {
            rooms.sort();
            assert_eq!(rooms, vec!["lab".to_owned(), "vault".to_owned()]);
        }
        other => panic!("expected MyInvites, got {other:?}"),
    }

    close_socket(&mut ws).await;
}
