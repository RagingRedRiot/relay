mod common;

use common::*;
use sqlx::PgPool;

fn get_room_cmd(room: &str) -> ClientCommand {
    ClientCommand::GetRoom {
        room_name: room.to_owned(),
    }
}

// ##### Public rooms #####

// A public room is visible to anyone authenticated, member or not.
#[sqlx::test]
async fn public_room_visible_to_non_member(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &get_room_cmd("general")).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::RoomInfo {
            room_name: "general".to_owned(),
            is_public: true,
            is_discoverable: true,
        }
    );

    close_socket(&mut ws).await;
}

// ##### Private rooms #####

// A private room is visible to its members.
#[sqlx::test]
async fn private_room_visible_to_member(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "secret", "alice", false, false).await;
    seed_membership(&pool, "secret", "bob").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &get_room_cmd("secret")).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::RoomInfo {
            room_name: "secret".to_owned(),
            is_public: false,
            is_discoverable: false,
        }
    );

    close_socket(&mut ws).await;
}

// A private room is visible to admins even when they aren't members.
#[sqlx::test]
async fn private_room_visible_to_admin(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "secret", "alice", false, false).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(&mut ws, &get_room_cmd("secret")).await;
    assert!(matches!(
        next_event(&mut ws).await,
        ServerEvent::RoomInfo { .. }
    ));

    close_socket(&mut ws).await;
}

// ##### Existence hiding #####

// A private room is hidden from a non-member: it reports NoRoomExists, exactly
// as a room that does not exist would — so existence isn't leaked.
#[sqlx::test]
async fn private_room_hidden_from_non_member(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "secret", "alice", false, false).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    // Real-but-private room...
    send_cmd(&mut ws, &get_room_cmd("secret")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoRoomExists);

    // ...is indistinguishable from a room that doesn't exist.
    send_cmd(&mut ws, &get_room_cmd("ghost")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoRoomExists);

    close_socket(&mut ws).await;
}
