mod common;

use common::*;
use sqlx::PgPool;

fn remove_cmd(room: &str, member: &str) -> ClientCommand {
    ClientCommand::RemoveRoomMember {
        room_name: room.to_owned(),
        member_username: member.to_owned(),
    }
}

// An owner removes another member: the membership is gone.
#[sqlx::test]
async fn owner_removes_member(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, false).await;
    seed_membership(&pool, "club", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    assert!(is_member(&pool, "club", "bob").await);
    send_cmd(&mut ws, &remove_cmd("club", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert!(!is_member(&pool, "club", "bob").await);

    close_socket(&mut ws).await;
}

// An admin can remove a member from a room they don't own.
#[sqlx::test]
async fn admin_removes_member(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, false).await;
    seed_membership(&pool, "club", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(&mut ws, &remove_cmd("club", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert!(!is_member(&pool, "club", "bob").await);

    close_socket(&mut ws).await;
}

// A non-owner member can't remove anyone; the target stays a member. "No such
// room" and "not authorized" are reported identically (Failed) so a non-owner
// can't probe a private room's existence.
#[sqlx::test]
async fn non_owner_cannot_remove(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "carol", "carolpw").await;
    seed_room(&pool, "club", "alice", false, false).await;
    seed_membership(&pool, "club", "bob").await;
    seed_membership(&pool, "club", "carol").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &remove_cmd("club", "carol")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    assert!(is_member(&pool, "club", "carol").await);

    close_socket(&mut ws).await;
}

// Removing someone who isn't a member is a no-op.
#[sqlx::test]
async fn removing_non_member_no_change(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &remove_cmd("club", "bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);

    close_socket(&mut ws).await;
}

// A removed member can no longer post to the room: the JIT membership check on
// send now fails.
#[sqlx::test]
async fn removed_member_cannot_post(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "club", "alice", true, false).await;
    seed_membership(&pool, "club", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    // Owner removes bob.
    let mut alice = create_socket(server.addr).await;
    authenticate(&mut alice, "alice", "alicepw").await;
    send_cmd(&mut alice, &remove_cmd("club", "bob")).await;
    assert_eq!(next_event(&mut alice).await, ServerEvent::Success);

    // bob, on his own session, can no longer post.
    let mut bob = create_socket(server.addr).await;
    authenticate(&mut bob, "bob", "bobpw").await;
    send_cmd(
        &mut bob,
        &ClientCommand::SendMessage {
            room_name: "club".to_owned(),
            content: "still here?".to_owned(),
            attachments: vec![],
        },
    )
    .await;
    assert_eq!(next_event(&mut bob).await, ServerEvent::Failed);

    close_socket(&mut alice).await;
    close_socket(&mut bob).await;
}
