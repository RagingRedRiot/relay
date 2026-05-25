mod common;

use common::*;
use sqlx::PgPool;
use uuid::Uuid;

// ##### Happy path #####

// Admin promotes a non-admin user: protocol returns Success, and the new
// admins row records the granting admin's user_id and is_default = false.
#[sqlx::test]
async fn admin_promotes_user(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Promote {
            target_username: "bob".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);

    let admin_id: Uuid = sqlx::query_scalar("SELECT user_id FROM users WHERE username = $1")
        .bind("admin")
        .fetch_one(&pool)
        .await
        .unwrap();
    let (granted_by, is_default): (Option<Uuid>, bool) = sqlx::query_as(
        "SELECT a.granted_by, a.is_default \
         FROM admins a JOIN users u ON u.user_id = a.user_id \
         WHERE u.username = $1",
    )
    .bind("bob")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(granted_by, Some(admin_id));
    assert!(!is_default);

    close_socket(&mut ws).await;
}

// The promotion must actually grant admin authority — not just write a row.
// A freshly promoted user should be able to perform an admin-only action
// (here: promoting yet another user).
#[sqlx::test]
async fn promoted_user_has_admin_authority(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    // admin promotes bob.
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::Promote {
            target_username: "bob".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    close_socket(&mut ws).await;

    // bob, now an admin, promotes alice.
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;
    send_cmd(
        &mut ws,
        &ClientCommand::Promote {
            target_username: "alice".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Success);
    close_socket(&mut ws).await;
}

// ##### Idempotency #####

// Promoting someone who is already an admin returns NoChange — the
// ON CONFLICT DO NOTHING insert affects zero rows.
#[sqlx::test]
async fn promote_existing_admin_returns_no_change(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    seed_extra_admin(&pool, "mod", "modpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Promote {
            target_username: "mod".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);

    close_socket(&mut ws).await;
}

// Self-promote by an existing admin is just the already-admin case — the
// source check passes, then the insert conflicts on the PK.
#[sqlx::test]
async fn admin_self_promote_returns_no_change(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Promote {
            target_username: "admin".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoChange);

    close_socket(&mut ws).await;
}

// ##### Authorization #####

// A non-admin attempting to promote someone else is rejected and no admins
// row is created for the target.
#[sqlx::test]
async fn non_admin_cannot_promote(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Promote {
            target_username: "alice".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    let row_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM admins a JOIN users u ON u.user_id = a.user_id \
         WHERE u.username = $1",
    )
    .bind("alice")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row_count, 0);

    close_socket(&mut ws).await;
}

// There is no self-target carve-out: a non-admin trying to promote
// themselves is still rejected. The source-admin check runs first.
#[sqlx::test]
async fn non_admin_cannot_self_promote(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Promote {
            target_username: "bob".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    let row_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM admins a JOIN users u ON u.user_id = a.user_id \
         WHERE u.username = $1",
    )
    .bind("bob")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row_count, 0);

    close_socket(&mut ws).await;
}

// ##### Bad input #####

// Promoting a username that doesn't exist collapses to Failed at the
// boundary (per the generic client-error surface), even for an admin.
#[sqlx::test]
async fn promote_nonexistent_user_fails(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Promote {
            target_username: "ghost".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    close_socket(&mut ws).await;
}

// ##### Session continuity #####

// A Failed promote must not tear down the session — the client should keep
// issuing commands on the same socket.
#[sqlx::test]
async fn failed_promote_does_not_close_session(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    seed_user(&pool, "alice", "alicepw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;

    send_cmd(
        &mut ws,
        &ClientCommand::Promote {
            target_username: "alice".to_owned(),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);

    // Same socket must still serve a follow-up command.
    assert_eq!(fetch_user_info(&mut ws, "alice").await.username, "alice");

    close_socket(&mut ws).await;
}
