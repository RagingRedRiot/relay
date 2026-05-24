mod common;

use common::*;
use sqlx::PgPool;

// ##### Self-edits #####

#[sqlx::test]
async fn user_edits_own_data(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "bob".to_owned(),
            username: Some("bobby".to_owned()),
            first_name: Some("Bobby".to_owned()),
            last_name: Some("Joel".to_owned()),
            alias: Some("BobbyJ".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    assert_eq!(
        fetch_user_info(&mut ws, "bobby").await,
        UserInfoFields {
            first_name: Some("Bobby".to_owned()),
            last_name: Some("Joel".to_owned()),
            alias: Some("BobbyJ".to_owned()),
            username: "bobby".to_owned(),
        }
    );

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn admin_edits_own_data(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "admin".to_owned(),
            username: None,
            first_name: Some("Ada".to_owned()),
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    assert_eq!(
        fetch_user_info(&mut ws, "admin").await,
        UserInfoFields {
            first_name: Some("Ada".to_owned()),
            last_name: None,
            alias: None,
            username: "admin".to_owned(),
        }
    );

    close_socket(&mut ws).await;
}

// ##### Admin edits other users #####

#[sqlx::test]
async fn admin_edits_other_user(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "bob".to_owned(),
            username: Some("bobby".to_owned()),
            first_name: Some("Bobby".to_owned()),
            last_name: Some("Joel".to_owned()),
            alias: Some("BobbyJ".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    assert_eq!(
        fetch_user_info(&mut ws, "bobby").await,
        UserInfoFields {
            first_name: Some("Bobby".to_owned()),
            last_name: Some("Joel".to_owned()),
            alias: Some("BobbyJ".to_owned()),
            username: "bobby".to_owned(),
        }
    );

    close_socket(&mut ws).await;
}

// ##### Authorization #####

#[sqlx::test]
async fn non_admin_cannot_edit_other_user(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "alice".to_owned(),
            username: Some("evil".to_owned()),
            first_name: Some("Mallory".to_owned()),
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // alice's row must be untouched.
    assert_eq!(
        fetch_user_info(&mut ws, "alice").await,
        UserInfoFields {
            first_name: None,
            last_name: None,
            alias: None,
            username: "alice".to_owned(),
        }
    );

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn edit_nonexistent_user_fails(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "ghost".to_owned(),
            username: None,
            first_name: Some("Hi".to_owned()),
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}

// A Failed edit must not tear down the session — the client should be able
// to keep issuing commands and get responses.
#[sqlx::test]
async fn failed_edit_does_not_close_session(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    // bob isn't allowed to touch alice.
    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "alice".to_owned(),
            username: None,
            first_name: Some("Mallory".to_owned()),
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // Same socket must still serve a follow-up command.
    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "bob".to_owned(),
            username: None,
            first_name: Some("Bob".to_owned()),
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    close_socket(&mut ws).await;
}

// ##### Field semantics #####

// COALESCE: fields left as None should preserve their previous values.
#[sqlx::test]
async fn partial_edit_preserves_unchanged_fields(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    // First, populate every field.
    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "bob".to_owned(),
            username: None,
            first_name: Some("Robert".to_owned()),
            last_name: Some("Smith".to_owned()),
            alias: Some("Bobby".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    // Then change only first_name; last_name and alias must persist.
    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "bob".to_owned(),
            username: None,
            first_name: Some("Bob".to_owned()),
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    assert_eq!(
        fetch_user_info(&mut ws, "bob").await,
        UserInfoFields {
            first_name: Some("Bob".to_owned()),
            last_name: Some("Smith".to_owned()),
            alias: Some("Bobby".to_owned()),
            username: "bob".to_owned(),
        }
    );

    close_socket(&mut ws).await;
}

// An edit with every field None still matches a row, so the actor reports
// Success even though nothing actually changed.
#[sqlx::test]
async fn edit_with_no_changes_succeeds(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "bob".to_owned(),
            username: None,
            first_name: None,
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    close_socket(&mut ws).await;
}

// Empty strings are distinct from None: the actor does not normalize "" to
// NULL today, so they overwrite the existing value. This test locks in
// that behavior — if we later add validation that rejects "" on these
// fields, this assertion will be the canary.
#[sqlx::test]
async fn empty_string_fields_overwrite_existing_values(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "bob".to_owned(),
            username: None,
            first_name: Some("Robert".to_owned()),
            last_name: Some("Smith".to_owned()),
            alias: Some("Bobby".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "bob".to_owned(),
            username: None,
            first_name: Some(String::new()),
            last_name: Some(String::new()),
            alias: Some(String::new()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    assert_eq!(
        fetch_user_info(&mut ws, "bob").await,
        UserInfoFields {
            first_name: Some(String::new()),
            last_name: Some(String::new()),
            alias: Some(String::new()),
            username: "bob".to_owned(),
        }
    );

    close_socket(&mut ws).await;
}

// users.username is UNIQUE, so renaming to a taken username trips the
// constraint inside the UPDATE and the actor must surface Failed without
// disturbing either row.
#[sqlx::test]
async fn username_collision_fails(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "bob".to_owned(),
            username: Some("alice".to_owned()),
            first_name: None,
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    assert_eq!(fetch_user_info(&mut ws, "bob").await.username, "bob");
    assert_eq!(fetch_user_info(&mut ws, "alice").await.username, "alice");

    close_socket(&mut ws).await;
}

// Credentials are keyed by user_id, so a rename should: (1) make the old
// username unauthenticatable, and (2) leave the same password valid under
// the new username.
#[sqlx::test]
async fn rename_does_not_break_credentials(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target: "bob".to_owned(),
            username: Some("bobby".to_owned()),
            first_name: None,
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    close_socket(&mut ws).await;

    // The old username must no longer authenticate.
    let mut ws = create_socket(server.addr).await;
    send_cmd(
        &mut ws,
        &ClientCommand::Auth {
            username: "bob".to_owned(),
            password: Password("bobpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoAuth);
    expect_close(&mut ws).await;

    // The new username with the original password must succeed.
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bobby", "bobpw").await;
    close_socket(&mut ws).await;
}
