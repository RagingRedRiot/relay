mod common;

use common::*;
use sqlx::PgPool;

// ##### Happy path #####

// Admin demotes another admin: Success, the admins row is gone for the
// target, the users row stays put (demote != delete), and the target can
// still authenticate as a regular user.
#[sqlx::test]
async fn admin_demotes_other_admin(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Demote {
            target_username: "mod".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    let admin_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM admins a JOIN users u ON u.user_id = a.user_id \
         WHERE u.username = $1",
    )
    .bind("mod")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(admin_count, 0);

    // The users row is untouched.
    assert_eq!(fetch_user_info(&mut ws, "mod").await.username, "mod");

    close_socket(&mut ws).await;

    // And the demoted user can still authenticate with the same credentials.
    assert_password_works(server.addr, "mod", "modpw").await;
}

// Non-default admin self-demoting: Success, and the admins row is gone.
#[sqlx::test]
async fn non_default_admin_can_self_demote(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mod", "modpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Demote {
            target_username: "mod".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    let admin_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM admins a JOIN users u ON u.user_id = a.user_id \
         WHERE u.username = $1",
    )
    .bind("mod")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(admin_count, 0);

    close_socket(&mut ws).await;
}

// JIT auth: after self-demote on the same socket, subsequent admin actions
// must be re-checked and fail — admin status is not cached for the session.
#[sqlx::test]
async fn self_demote_revokes_admin_in_same_session(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mod", "modpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Demote {
            target_username: "mod".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    // Same socket, no re-auth — mod is no longer admin so promoting alice
    // must be rejected.
    send_cmd(
        &mut ws,
        &ClientCommand::Promote {
            target_username: "alice".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}

// ##### Idempotency #####

// Demoting a user who isn't an admin is a no-op: NoChange, mirroring the
// promote vocabulary for "target already in the desired state".
#[sqlx::test]
async fn demote_non_admin_returns_no_change(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Demote {
            target_username: "bob".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);

    close_socket(&mut ws).await;
}

// ##### Default admin protection #####

// Another admin cannot demote the default admin. The default admin row must
// stay intact and is_default must still be true.
#[sqlx::test]
async fn non_default_admin_cannot_demote_default_admin(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "mod", "modpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Demote {
            target_username: "admin".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    let is_default: bool = sqlx::query_scalar(
        "SELECT is_default FROM admins a JOIN users u ON u.user_id = a.user_id \
         WHERE u.username = $1",
    )
    .bind("admin")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(is_default);

    close_socket(&mut ws).await;
}

// The default admin cannot demote itself either — the is_default guard is
// on the target, regardless of who is asking.
#[sqlx::test]
async fn default_admin_cannot_self_demote(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Demote {
            target_username: "admin".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    let is_default: bool = sqlx::query_scalar(
        "SELECT is_default FROM admins a JOIN users u ON u.user_id = a.user_id \
         WHERE u.username = $1",
    )
    .bind("admin")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(is_default);

    close_socket(&mut ws).await;
}

// ##### Authorization #####

// A non-admin attempting to demote anyone is rejected; the target's admin
// status (if any) is not affected.
#[sqlx::test]
async fn non_admin_cannot_demote(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Demote {
            target_username: "mod".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // mod is still an admin.
    let admin_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM admins a JOIN users u ON u.user_id = a.user_id \
         WHERE u.username = $1",
    )
    .bind("mod")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(admin_count, 1);

    close_socket(&mut ws).await;
}

// ##### Bad input #####

// Demoting a username that doesn't exist collapses to Failed at the
// boundary, per the generic client-error surface.
#[sqlx::test]
async fn demote_nonexistent_user_fails(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Demote {
            target_username: "ghost".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}

// ##### Session continuity #####

// A Failed demote must not tear down the session.
#[sqlx::test]
async fn failed_demote_does_not_close_session(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    // bob isn't allowed to demote anyone.
    send_cmd(
        &mut ws,
        &ClientCommand::Demote {
            target_username: "admin".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // Same socket must still serve a follow-up command.
    assert_eq!(fetch_user_info(&mut ws, "admin").await.username, "admin");

    close_socket(&mut ws).await;
}
