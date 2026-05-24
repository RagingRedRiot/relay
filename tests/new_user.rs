mod common;

use common::*;
use sqlx::PgPool;

// ##### Authentication #####

#[sqlx::test]
async fn auth_succeeds_with_valid_credentials(pool: PgPool) {
    seed_admin(&pool, "test", "test").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "test", "test").await;
    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn auth_fails_with_invalid_credentials(pool: PgPool) {
    // No users seeded — any credentials must be rejected.
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    send_cmd(
        &mut ws,
        &ClientCommand::Auth {
            username: "nobody".to_owned(),
            password: Password("wrong".to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoAuth);
    expect_close(&mut ws).await;
}

// ##### Unauthenticated user creation #####

#[sqlx::test]
async fn unauthenticated_signup_rejected_when_signups_closed(pool: PgPool) {
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    send_cmd(&mut ws, &new_user_cmd("newbie", "newbiepw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoAuth);
    expect_close(&mut ws).await;
}

#[sqlx::test]
async fn unauthenticated_signup_allowed_when_signups_open(pool: PgPool) {
    let server = spawn_app(pool, |c| c.open_signups = true).await;

    let mut ws = create_socket(server.addr).await;
    send_cmd(&mut ws, &new_user_cmd("newbie", "newbiepw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);
    expect_close(&mut ws).await;
}

// ##### Authenticated user creation #####

#[sqlx::test]
async fn admin_creates_user_when_signups_closed(pool: PgPool) {
    seed_admin(&pool, "admin", "adminpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "admin", "adminpw").await;
    send_cmd(&mut ws, &new_user_cmd("newbie", "newbiepw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);
    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn non_admin_denied_when_signups_closed(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;
    send_cmd(&mut ws, &new_user_cmd("newbie", "newbiepw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::Failed);
    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn non_admin_creates_user_when_signups_open(pool: PgPool) {
    seed_user(&pool, "bob", "bobpw").await;
    let server = spawn_app(pool, |c| c.open_signups = true).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "bobpw").await;
    send_cmd(&mut ws, &new_user_cmd("newbie", "newbiepw")).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::UserCreated);
    close_socket(&mut ws).await;
}
