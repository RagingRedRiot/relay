mod common;

use common::*;
use sqlx::PgPool;

// ##### Happy paths #####

#[sqlx::test]
async fn default_admin_resets_user_password(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::ResetPassword {
            target_username: "bob".to_owned(),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    close_socket(&mut ws).await;

    assert_password_fails(server.addr, "bob", "bobpw").await;
    assert_password_works(server.addr, "bob", "newpw").await;
}

// Non-default admins can reset non-admin users — the only restriction on
// non-default admins is they can't reset *other* admins.
#[sqlx::test]
async fn non_default_admin_resets_user_password(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mod", "modpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::ResetPassword {
            target_username: "bob".to_owned(),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    close_socket(&mut ws).await;

    assert_password_works(server.addr, "bob", "newpw").await;
}

// The default admin is the only one allowed to reset another admin.
#[sqlx::test]
async fn default_admin_resets_non_default_admin(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::ResetPassword {
            target_username: "mod".to_owned(),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    close_socket(&mut ws).await;

    assert_password_fails(server.addr, "mod", "modpw").await;
    assert_password_works(server.addr, "mod", "newpw").await;
}

// ##### Authorization — non-admins #####

// A regular user cannot reset anyone, including themselves.
#[sqlx::test]
async fn non_admin_cannot_reset(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::ResetPassword {
            target_username: "alice".to_owned(),
            new_password: Password("evil".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    close_socket(&mut ws).await;

    assert_password_works(server.addr, "alice", "alicepw").await;
}

// ##### Self-target lockout — admins must use UpdatePassword for their own #####

// Non-default admin cannot use ResetPassword on themselves — the self-target
// guard fires regardless of admin status. Forces the proof-of-possession path.
#[sqlx::test]
async fn non_default_admin_cannot_reset_self(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mod", "modpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::ResetPassword {
            target_username: "mod".to_owned(),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    close_socket(&mut ws).await;

    assert_password_works(server.addr, "mod", "modpw").await;
}

// The default admin cannot reset themselves either — both the self-target
// guard and the target_is_default guard apply, but the visible outcome is
// the same: Failed.
#[sqlx::test]
async fn default_admin_cannot_reset_self(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::ResetPassword {
            target_username: "admin".to_owned(),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    close_socket(&mut ws).await;

    assert_password_works(server.addr, "admin", "adminpw").await;
}

// ##### Admin-on-admin protection #####

// Limits the blast radius of a non-default admin compromise: an attacker
// holding a non-default admin session cannot lock out another admin.
#[sqlx::test]
async fn non_default_admin_cannot_reset_another_admin(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod1", "mod1pw").await;
    seed_extra_admin(&pool, "mod2", "mod2pw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mod1", "mod1pw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::ResetPassword {
            target_username: "mod2".to_owned(),
            new_password: Password("evil".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    close_socket(&mut ws).await;

    assert_password_works(server.addr, "mod2", "mod2pw").await;
}

// A non-default admin must not be able to lock out the default admin —
// otherwise compromising any non-default admin would be a path to taking
// over the break-glass account.
#[sqlx::test]
async fn non_default_admin_cannot_reset_default_admin(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mod", "modpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::ResetPassword {
            target_username: "admin".to_owned(),
            new_password: Password("evil".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    close_socket(&mut ws).await;

    assert_password_works(server.addr, "admin", "adminpw").await;
}

// ##### Misc #####

#[sqlx::test]
async fn reset_nonexistent_user_fails(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::ResetPassword {
            target_username: "ghost".to_owned(),
            new_password: Password("newpw".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    close_socket(&mut ws).await;
}

// A Failed reset must not tear down the session.
#[sqlx::test]
async fn failed_reset_does_not_close_session(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    // bob isn't an admin — reset must fail.
    send_cmd(
        &mut ws,
        &ClientCommand::ResetPassword {
            target_username: "alice".to_owned(),
            new_password: Password("evil".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // Session still alive — a follow-up lookup must come back fine.
    assert_eq!(fetch_user_info(&mut ws, "alice").await.username, "alice");
    close_socket(&mut ws).await;
}
