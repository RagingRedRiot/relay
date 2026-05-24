mod common;

use common::*;
use sqlx::PgPool;

// ##### Self-deletes #####

// Self-delete: the actor reports UserDeleted { is_self: true }, which the
// server translates to Success followed by a server-initiated Close frame.
#[sqlx::test]
async fn user_deletes_self(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "bob".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    expect_close(&mut ws).await;
}

// After a self-delete, the credentials row was cascaded away, so the old
// username/password must no longer authenticate.
#[sqlx::test]
async fn self_deleted_user_cannot_authenticate(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "bob".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    expect_close(&mut ws).await;

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
}

// ##### Admin deletes other users #####

// Admin deleting someone else is not is_self, so the server sends Success
// and leaves the socket open.
#[sqlx::test]
async fn admin_deletes_other_user(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "bob".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    // The deleted user is no longer fetchable.
    send_cmd(&mut ws, &get_user_by_username_cmd("bob")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoUserExists);

    close_socket(&mut ws).await;
}

// After admin deletion, the victim's credentials are also gone (CASCADE), so
// they can no longer authenticate.
#[sqlx::test]
async fn admin_deleted_user_cannot_authenticate(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "bob".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    close_socket(&mut ws).await;

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
}

// ##### Authorization #####

// A non-admin trying to delete someone else is rejected, and the target row
// must remain intact.
#[sqlx::test]
async fn non_admin_cannot_delete_other_user(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "alice".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // alice's row must be untouched — fetchable on the same socket.
    assert_eq!(fetch_user_info(&mut ws, "alice").await.username, "alice");

    close_socket(&mut ws).await;
}

// Deleting a username that doesn't exist short-circuits before the auth
// check, so the response is Failed regardless of who's asking.
#[sqlx::test]
async fn delete_nonexistent_user_fails(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "ghost".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}

// ##### Default admin protection #####

// The default admin (is_default = true) cannot be deleted, even by itself.
// This is the load-bearing guard that prevents an empty-admins state.
#[sqlx::test]
async fn default_admin_cannot_self_delete(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "admin".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // Socket stays alive, and the admin row is still there.
    assert_eq!(fetch_user_info(&mut ws, "admin").await.username, "admin");

    close_socket(&mut ws).await;
}

// A non-default admin can delete themselves — the is_default guard only
// protects the default admin, not every admin.
#[sqlx::test]
async fn non_default_admin_can_self_delete(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mod", "modpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "mod".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    expect_close(&mut ws).await;

    // The default admin must still be intact and usable.
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;
    assert_eq!(fetch_user_info(&mut ws, "admin").await.username, "admin");
    close_socket(&mut ws).await;
}

// The is_default guard is on the *target*, not the source: even another
// admin cannot delete the default admin.
#[sqlx::test]
async fn non_default_admin_cannot_delete_default_admin(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mod", "modpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "admin".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    assert_eq!(fetch_user_info(&mut ws, "admin").await.username, "admin");

    close_socket(&mut ws).await;
}

// The default admin can delete other admins (only is_default targets are
// protected, not admins-in-general).
#[sqlx::test]
async fn default_admin_can_delete_other_admin(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "mod".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    send_cmd(&mut ws, &get_user_by_username_cmd("mod")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoUserExists);

    close_socket(&mut ws).await;
}

// ##### Session continuity #####

// A Failed delete must not tear down the session — the client should be able
// to keep issuing commands.
#[sqlx::test]
async fn failed_delete_does_not_close_session(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    // bob isn't allowed to delete alice.
    send_cmd(
        &mut ws,
        &ClientCommand::DeleteUser {
            target_username: "alice".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // Same socket must still serve a follow-up command.
    assert_eq!(fetch_user_info(&mut ws, "alice").await.username, "alice");

    close_socket(&mut ws).await;
}
