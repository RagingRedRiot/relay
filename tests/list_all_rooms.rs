mod common;

use common::*;
use sqlx::PgPool;

fn list_all_cmd() -> ClientCommand {
    ClientCommand::ListAllRooms
}

// An admin sees every room, including private non-discoverable ones that the
// public discover listing hides.
#[sqlx::test]
async fn admin_lists_all_rooms_including_private(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    // A public room, a discoverable private room, and a fully private one.
    seed_room(&pool, "general", "alice", true, false).await;
    seed_room(&pool, "staff", "alice", false, true).await;
    seed_room(&pool, "vault", "alice", false, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(&mut ws, &list_all_cmd()).await;
    let names = match next_event(&mut ws).await {
        ServerEvent::AllRooms { rooms } => {
            rooms.into_iter().map(|r| r.room_name).collect::<Vec<_>>()
        }
        other => panic!("expected AllRooms, got {other:?}"),
    };
    // All three present, including the private non-discoverable "vault".
    assert!(names.contains(&"general".to_owned()));
    assert!(names.contains(&"staff".to_owned()));
    assert!(names.contains(&"vault".to_owned()));

    close_socket(&mut ws).await;
}

// A non-admin is rejected: listing every room is a moderation-only capability.
#[sqlx::test]
async fn non_admin_is_rejected(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, false).await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &list_all_cmd()).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}
