mod common;

use common::*;
use sqlx::PgPool;

// Tests for username/profile normalization and the empty-string guards:
//   * case-insensitive username uniqueness + lookups (LOWER() index)
//   * leading/trailing whitespace trimmed on write
//   * blank optional fields collapse to NULL
//   * empty/whitespace-only usernames are rejected (CHECK + NOT NULL)
//
// These live in their own file (a pure addition) rather than touching the
// existing per-command test files.

// Seed a default admin, start the app, and return an authenticated socket.
// Signups stay closed (the spawn_app default), so creation goes through the
// admin path. Returns the server too so it outlives the socket.
async fn admin_session(pool: PgPool) -> (TestServer, Ws) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;
    (server, ws)
}

fn new_user_full(
    username: &str,
    first_name: Option<&str>,
    last_name: Option<&str>,
    alias: Option<&str>,
) -> ClientCommand {
    ClientCommand::NewUser {
        username: username.to_owned(),
        password: Password("pw".to_owned()),
        first_name: first_name.map(|s| s.to_owned()),
        last_name: last_name.map(|s| s.to_owned()),
        alias: alias.map(|s| s.to_owned()),
    }
}

// ##### Case-insensitive uniqueness #####

#[sqlx::test]
async fn username_uniqueness_is_case_insensitive(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(&mut ws, &new_user_cmd("Alice", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    // Same name, different casing — both must collide with "Alice".
    send_cmd(&mut ws, &new_user_cmd("alice", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    send_cmd(&mut ws, &new_user_cmd("ALICE", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn auth_succeeds_with_different_username_casing(pool: PgPool) {
    // Stored as "Alice"; authentication must resolve regardless of casing.
    seed_user(&pool, "Alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "alicepw").await;
    close_socket(&mut ws).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "ALICE", "alicepw").await;
    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn get_user_by_username_is_case_insensitive_and_preserves_casing(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(&mut ws, &new_user_cmd("Alice", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    // Looked up with a different casing, but the stored casing comes back.
    let info = fetch_user_info(&mut ws, "alice").await;
    assert_eq!(info.username, "Alice");

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn edit_to_case_insensitive_duplicate_username_fails(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(&mut ws, &new_user_cmd("henry", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);
    send_cmd(&mut ws, &new_user_cmd("iris", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    // Renaming iris to a different-cased "henry" must collide.
    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target_username: "iris".to_owned(),
            username: Some("HENRY".to_owned()),
            first_name: None,
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // iris keeps her original name.
    assert_eq!(fetch_user_info(&mut ws, "iris").await.username, "iris");

    close_socket(&mut ws).await;
}

// ##### Trimming on create #####

#[sqlx::test]
async fn username_is_trimmed_on_create(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(&mut ws, &new_user_cmd("  spacey  ", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    // Stored without the surrounding whitespace.
    assert_eq!(fetch_user_info(&mut ws, "spacey").await.username, "spacey");

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn profile_fields_are_trimmed_on_create(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(
        &mut ws,
        &new_user_full(
            "carol",
            Some("  Carol  "),
            Some("  Jones  "),
            Some("  CJ  "),
        ),
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    assert_eq!(
        fetch_user_info(&mut ws, "carol").await,
        UserInfoFields {
            first_name: Some("Carol".to_owned()),
            last_name: Some("Jones".to_owned()),
            alias: Some("CJ".to_owned()),
            username: "carol".to_owned(),
        }
    );

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn blank_profile_fields_become_null_on_create(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    // Whitespace-only and empty optional fields normalize to NULL.
    send_cmd(
        &mut ws,
        &new_user_full("dave", Some("   "), Some(""), Some(" ")),
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    assert_eq!(
        fetch_user_info(&mut ws, "dave").await,
        UserInfoFields {
            first_name: None,
            last_name: None,
            alias: None,
            username: "dave".to_owned(),
        }
    );

    close_socket(&mut ws).await;
}

// ##### Empty-username rejection #####

#[sqlx::test]
async fn empty_username_rejected_on_create(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(&mut ws, &new_user_cmd("", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn whitespace_only_username_rejected_on_create(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    // Trims to empty, so the CHECK rejects it.
    send_cmd(&mut ws, &new_user_cmd("   ", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}

// ##### Trimming on edit #####

#[sqlx::test]
async fn profile_fields_are_trimmed_on_edit(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(&mut ws, &new_user_cmd("erin", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target_username: "erin".to_owned(),
            username: None,
            first_name: Some("  Erin  ".to_owned()),
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    assert_eq!(
        fetch_user_info(&mut ws, "erin").await.first_name,
        Some("Erin".to_owned())
    );

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn username_is_trimmed_on_edit(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(&mut ws, &new_user_cmd("grace", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target_username: "grace".to_owned(),
            username: Some("  graceful  ".to_owned()),
            first_name: None,
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    assert_eq!(
        fetch_user_info(&mut ws, "graceful").await.username,
        "graceful"
    );

    close_socket(&mut ws).await;
}

// On edit, a provided field is distinct from an absent one:
//   * Some("")/whitespace -> clear to NULL
//   * None               -> leave unchanged

#[sqlx::test]
async fn empty_string_edit_clears_optional_field_to_null(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(
        &mut ws,
        &new_user_full("frank", Some("Frank"), Some("Smith"), Some("Frankie")),
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target_username: "frank".to_owned(),
            username: None,
            first_name: Some(String::new()),
            last_name: Some(String::new()),
            alias: Some(String::new()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    assert_eq!(
        fetch_user_info(&mut ws, "frank").await,
        UserInfoFields {
            first_name: None,
            last_name: None,
            alias: None,
            username: "frank".to_owned(),
        }
    );

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn whitespace_edit_clears_optional_field_to_null(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(&mut ws, &new_user_full("gwen", Some("Gwen"), None, None)).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target_username: "gwen".to_owned(),
            username: None,
            first_name: Some("   ".to_owned()),
            last_name: None,
            alias: None,
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    assert_eq!(fetch_user_info(&mut ws, "gwen").await.first_name, None);

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn none_edit_leaves_optional_field_unchanged(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    send_cmd(&mut ws, &new_user_full("hank", Some("Hank"), None, None)).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    // first_name is absent (None) from this edit, so it must survive untouched
    // even as another field changes.
    send_cmd(
        &mut ws,
        &ClientCommand::EditUser {
            target_username: "hank".to_owned(),
            username: None,
            first_name: None,
            last_name: None,
            alias: Some("Hanky".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    let info = fetch_user_info(&mut ws, "hank").await;
    assert_eq!(info.first_name, Some("Hank".to_owned()));
    assert_eq!(info.alias, Some("Hanky".to_owned()));

    close_socket(&mut ws).await;
}

// ##### Whitespace-complete trimming (tabs/newlines, not just spaces) #####

#[sqlx::test]
async fn tabs_and_newlines_are_trimmed_on_create(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    // Wrapped in a mix of spaces, tabs, and newlines on both ends.
    send_cmd(
        &mut ws,
        &new_user_full("\t ned \n", Some("\nNed\t"), None, None),
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);

    let info = fetch_user_info(&mut ws, "ned").await;
    assert_eq!(info.username, "ned");
    assert_eq!(info.first_name, Some("Ned".to_owned()));

    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn tab_and_newline_only_username_rejected_on_create(pool: PgPool) {
    let (_server, mut ws) = admin_session(pool).await;

    // No spaces at all -- only tabs/newlines. Bare SQL TRIM would miss these;
    // trim_ws reduces them to '', so the CHECK rejects it.
    send_cmd(&mut ws, &new_user_cmd("\t\n", "pw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}
