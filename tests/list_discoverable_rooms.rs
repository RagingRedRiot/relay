mod common;

use common::*;
use relay::model::DiscoverableRoom;
use sqlx::PgPool;

fn list_cmd() -> ClientCommand {
    ClientCommand::ListDiscoverableRooms
}

// ##### Visibility rules #####

// An empty server returns an empty list.
#[sqlx::test]
async fn empty_when_no_rooms(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &list_cmd()).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::DiscoverableRooms { rooms: vec![] }
    );

    close_socket(&mut ws).await;
}

// Public rooms appear in the listing.
#[sqlx::test]
async fn public_room_appears(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, false).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &list_cmd()).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::DiscoverableRooms {
            rooms: vec![DiscoverableRoom {
                room_name: "general".to_owned(),
                is_public: true,
                member_count: 1,
            }]
        }
    );

    close_socket(&mut ws).await;
}

// Discoverable-but-private rooms appear in the listing with is_public = false.
#[sqlx::test]
async fn discoverable_private_room_appears(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "club", "alice", false, true).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &list_cmd()).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::DiscoverableRooms {
            rooms: vec![DiscoverableRoom {
                room_name: "club".to_owned(),
                is_public: false,
                member_count: 1,
            }]
        }
    );

    close_socket(&mut ws).await;
}

// Private non-discoverable rooms are absent — existence must not be leaked.
#[sqlx::test]
async fn private_non_discoverable_room_hidden(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "secret", "alice", false, false).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &list_cmd()).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::DiscoverableRooms { rooms: vec![] }
    );

    close_socket(&mut ws).await;
}

// Private non-discoverable rooms are absent even for their own members.
#[sqlx::test]
async fn private_non_discoverable_room_hidden_from_member(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "secret", "alice", false, false).await;
    seed_membership(&pool, "secret", "bob").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &list_cmd()).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::DiscoverableRooms { rooms: vec![] }
    );

    close_socket(&mut ws).await;
}

// ##### Member count #####

// member_count reflects actual membership, not just ownership.
#[sqlx::test]
async fn member_count_is_accurate(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "carol", "carolpw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    seed_membership(&pool, "general", "bob").await;
    seed_membership(&pool, "general", "carol").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &list_cmd()).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::DiscoverableRooms {
            rooms: vec![DiscoverableRoom {
                room_name: "general".to_owned(),
                is_public: true,
                member_count: 3,
            }]
        }
    );

    close_socket(&mut ws).await;
}

// ##### Ordering #####

// Rooms are returned ordered by member_count descending, then name ascending.
#[sqlx::test]
async fn ordered_by_member_count_then_name(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "carol", "carolpw").await;
    // "popular" has 3 members; "alpha" and "zeta" have 1 each (tied, sorted by name).
    seed_room(&pool, "popular", "alice", true, true).await;
    seed_membership(&pool, "popular", "bob").await;
    seed_membership(&pool, "popular", "carol").await;
    seed_room(&pool, "zeta", "alice", true, true).await;
    seed_room(&pool, "alpha", "alice", true, true).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &list_cmd()).await;
    let event = next_event(&mut ws).await;
    let names: Vec<&str> = match &event {
        ServerEvent::DiscoverableRooms { rooms } => {
            rooms.iter().map(|r| r.room_name.as_str()).collect()
        }
        other => panic!("expected DiscoverableRooms, got {other:?}"),
    };
    assert_eq!(names, vec!["popular", "alpha", "zeta"]);

    close_socket(&mut ws).await;
}

// ##### Mixed room types #####

// A mix of public, discoverable-private, and hidden rooms: only the first two
// appear, and member counts are independent.
#[sqlx::test]
async fn mixed_visibility_rooms(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "open", "alice", true, true).await;
    seed_membership(&pool, "open", "bob").await;
    seed_room(&pool, "listed", "alice", false, true).await;
    seed_room(&pool, "secret", "alice", false, false).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &list_cmd()).await;
    let event = next_event(&mut ws).await;
    let rooms = match event {
        ServerEvent::DiscoverableRooms { rooms } => rooms,
        other => panic!("expected DiscoverableRooms, got {other:?}"),
    };

    let names: Vec<&str> = rooms.iter().map(|r| r.room_name.as_str()).collect();
    assert!(names.contains(&"open"), "public room should be listed");
    assert!(
        names.contains(&"listed"),
        "discoverable room should be listed"
    );
    assert!(!names.contains(&"secret"), "hidden room must not appear");
    assert_eq!(rooms.len(), 2);

    let open = rooms.iter().find(|r| r.room_name == "open").unwrap();
    assert!(open.is_public);
    assert_eq!(open.member_count, 2);

    let listed = rooms.iter().find(|r| r.room_name == "listed").unwrap();
    assert!(!listed.is_public);
    assert_eq!(listed.member_count, 1);

    close_socket(&mut ws).await;
}
