mod common;

use common::*;
use relay::model::RoomMember;
use sqlx::PgPool;

fn get_members_cmd(room: &str) -> ClientCommand {
    ClientCommand::GetRoomMembership {
        room_name: room.to_owned(),
    }
}

fn usernames(members: &[RoomMember]) -> Vec<&str> {
    members.iter().map(|m| m.username.as_str()).collect()
}

// ##### Listing #####

// Members are listed owners-first; a public room's membership is readable by a
// non-member.
#[sqlx::test]
async fn lists_members_owner_first(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "carl", "carlpw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    seed_membership(&pool, "general", "bob").await;
    let server = spawn_app(pool, |_| {}).await;

    // carl is not a member, but the room is public.
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "carl", "carlpw").await;

    send_cmd(&mut ws, &get_members_cmd("general")).await;
    match next_event(&mut ws).await {
        ServerEvent::RoomMembers { members } => {
            // alice is the owner, so she sorts first.
            assert_eq!(usernames(&members), vec!["alice", "bob"]);
            // Ownership is reflected per member.
            assert!(members[0].is_owner, "alice should be an owner");
            assert!(!members[1].is_owner, "bob should not be an owner");
        }
        other => panic!("expected RoomMembers, got {other:?}"),
    }

    close_socket(&mut ws).await;
}

// ##### No user_id leak #####

// The membership payload must not carry the internal user_id over the wire.
#[sqlx::test]
async fn membership_payload_omits_user_id(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_room(&pool, "general", "alice", true, true).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;

    send_cmd(&mut ws, &get_members_cmd("general")).await;

    // Inspect the raw frame rather than the parsed event.
    let msg = ws.next().await.expect("closed").expect("ws error");
    let text = msg.to_text().expect("text frame");
    assert!(
        text.contains("\"username\""),
        "expected a member username in {text:?}"
    );
    assert!(
        !text.contains("user_id"),
        "membership payload leaked user_id: {text:?}"
    );

    close_socket(&mut ws).await;
}

// ##### Existence hiding #####

// A non-member can't list a private room's membership; it reports NoRoomExists.
#[sqlx::test]
async fn private_membership_hidden_from_non_member(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_room(&pool, "secret", "alice", false, false).await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(&mut ws, &get_members_cmd("secret")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoRoomExists);

    close_socket(&mut ws).await;
}
