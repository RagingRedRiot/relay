mod common;

use common::*;
use sqlx::PgPool;

fn rename_cmd(current: &str, new: &str) -> ClientCommand {
    ClientCommand::SetRoomName {
        current_name: current.to_owned(),
        new_name: new.to_owned(),
    }
}

async fn room_exists(pool: &PgPool, name: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM rooms WHERE room_name = $1)")
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap()
}

// ##### Happy path #####

// An owner renames their room; the new name is persisted (trimmed).
#[sqlx::test]
async fn owner_renames_room(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "old", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &rename_cmd("old", "new")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert!(room_exists(&pool, "new").await);
    assert!(!room_exists(&pool, "old").await);

    close_socket(&mut ws).await;
}

// An admin can rename a room they don't own.
#[sqlx::test]
async fn admin_can_rename(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "old", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(&mut ws, &rename_cmd("old", "new")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    assert!(room_exists(&pool, "new").await);

    close_socket(&mut ws).await;
}

// ##### Authorization #####

// A plain member cannot rename; the name is unchanged.
#[sqlx::test]
async fn non_owner_member_cannot_rename(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "old", "alice", true, true).await;
    seed_membership(&pool, "old", "bob").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &rename_cmd("old", "new")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    assert!(room_exists(&pool, "old").await);
    assert!(!room_exists(&pool, "new").await);

    close_socket(&mut ws).await;
}

// Renaming a nonexistent room fails the same way as unauthorized (no leak).
#[sqlx::test]
async fn rename_nonexistent_room_fails(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &rename_cmd("ghost", "new")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}
