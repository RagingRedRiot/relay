mod common;

use common::*;
use sqlx::PgPool;

fn new_room_cmd(
    name: &str,
    is_public: Option<bool>,
    is_discoverable: Option<bool>,
) -> ClientCommand {
    ClientCommand::NewRoom {
        room_name: name.to_owned(),
        is_public,
        is_discoverable,
    }
}

// ##### Happy path #####

// Creating a room returns Success, persists the requested visibility, and seeds
// the creator as the room's first owner-member.
#[sqlx::test]
async fn creates_room_with_visibility_and_owner(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &new_room_cmd("general", Some(true), Some(true))).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    let (is_public, is_discoverable): (bool, bool) =
        sqlx::query_as("SELECT is_public, is_discoverable FROM rooms WHERE room_name = $1")
            .bind("general")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(is_public);
    assert!(is_discoverable);

    assert!(is_owner(&pool, "general", "alice").await);

    close_socket(&mut ws).await;
}

// ##### Defaults #####

// Omitting both flags defaults the room to private and non-discoverable.
#[sqlx::test]
async fn omitted_flags_default_to_private_hidden(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &new_room_cmd("secret", None, None)).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    let (is_public, is_discoverable): (bool, bool) =
        sqlx::query_as("SELECT is_public, is_discoverable FROM rooms WHERE room_name = $1")
            .bind("secret")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!is_public);
    assert!(!is_discoverable);

    close_socket(&mut ws).await;
}

// ##### Uniqueness #####

// Room names are unique case-insensitively (matching the LOWER(room_name)
// index), so a clashing name fails.
#[sqlx::test]
async fn duplicate_name_is_rejected_case_insensitively(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &new_room_cmd("GENERAL", Some(true), Some(true))).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rooms WHERE LOWER(room_name) = 'general'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);

    close_socket(&mut ws).await;
}

// A failed create must not tear down the session.
#[sqlx::test]
async fn failed_create_keeps_session(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &new_room_cmd("general", None, None)).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // Same socket still serves a follow-up command.
    send_cmd(&mut ws, &new_room_cmd("another", None, None)).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    close_socket(&mut ws).await;
}
