mod common;

use common::*;
use sqlx::PgPool;

// ##### Happy path #####

#[sqlx::test]
async fn user_changes_own_password(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::UpdatePassword {
            current_password: Password("bobpw".to_owned()),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    close_socket(&mut ws).await;

    // Old password must no longer authenticate, new one must.
    assert_password_fails(server.addr, "bob", "bobpw").await;
    assert_password_works(server.addr, "bob", "newpw").await;
}

// Non-default admins are ordinary users for credential rotation — the
// default-admin lockdown is scoped to the default admin only.
#[sqlx::test]
async fn non_default_admin_changes_own_password(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mod", "modpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::UpdatePassword {
            current_password: Password("modpw".to_owned()),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    close_socket(&mut ws).await;

    assert_password_fails(server.addr, "mod", "modpw").await;
    assert_password_works(server.addr, "mod", "newpw").await;
}

// ##### Authorization / proof-of-possession #####

// Wrong current password must fail and leave the existing credential intact.
#[sqlx::test]
async fn wrong_current_password_fails(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::UpdatePassword {
            current_password: Password("wrong".to_owned()),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    close_socket(&mut ws).await;

    // Original password still works; the attempted new value must not.
    assert_password_works(server.addr, "bob", "bobpw").await;
    assert_password_fails(server.addr, "bob", "newpw").await;
}

// ##### Default admin lockdown #####

// The default admin's credentials are config-managed (set at boot via
// ensure_admin); UpdatePassword must reject even when the default admin
// supplies the correct current password.
#[sqlx::test]
async fn default_admin_cannot_change_own_password(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::UpdatePassword {
            current_password: Password("adminpw".to_owned()),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    close_socket(&mut ws).await;

    // The original password still works; the proposed new one must not.
    assert_password_works(server.addr, "admin", "adminpw").await;
    assert_password_fails(server.addr, "admin", "newpw").await;
}

// ##### Session liveness #####

// A failed update must not tear down the session — a follow-up command
// on the same socket should succeed.
#[sqlx::test]
async fn failed_update_does_not_close_session(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::UpdatePassword {
            current_password: Password("wrong".to_owned()),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // Same socket must still serve a follow-up.
    send_cmd(
        &mut ws,
        &ClientCommand::UpdatePassword {
            current_password: Password("bobpw".to_owned()),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    close_socket(&mut ws).await;
}
