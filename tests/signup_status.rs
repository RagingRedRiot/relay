mod common;

use common::*;
use sqlx::PgPool;

// A client can query signup status in the prelude, before authenticating, so a
// login screen knows whether to offer registration. Default config has signups
// closed.
#[sqlx::test]
async fn reports_signups_closed_pre_auth(pool: PgPool) {
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    send_cmd(&mut ws, &ClientCommand::GetSignupStatus).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::SignupStatus {
            open_signups: false
        }
    );
    // (no graceful close: pre-auth Close yields NoAuth; just drop the socket)
}

#[sqlx::test]
async fn reports_signups_open_pre_auth(pool: PgPool) {
    let server = spawn_app(pool, |c| c.open_signups = true).await;

    let mut ws = create_socket(server.addr).await;
    send_cmd(&mut ws, &ClientCommand::GetSignupStatus).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::SignupStatus { open_signups: true }
    );
}

// The query is non-terminal: after answering it, the prelude keeps waiting, so the
// same socket can still authenticate.
#[sqlx::test]
async fn query_does_not_consume_the_prelude(pool: PgPool) {
    seed_admin(&pool, "boss", "bosspw").await;
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    send_cmd(&mut ws, &ClientCommand::GetSignupStatus).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::SignupStatus {
            open_signups: false
        }
    );

    // Still in the prelude — authenticate on the same socket and proceed.
    authenticate(&mut ws, "boss", "bosspw").await;
    close_socket(&mut ws).await;
}

// A query keeps the prelude open, but any other (non-auth, non-signup) command
// then still ends the connection — an unauthenticated socket can't linger or do
// anything but query, auth, or sign up.
#[sqlx::test]
async fn non_prelude_command_still_closes(pool: PgPool) {
    let server = spawn_app(pool, |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    send_cmd(&mut ws, &ClientCommand::GetSignupStatus).await;
    assert_eq!(
        next_event(&mut ws).await,
        ServerEvent::SignupStatus {
            open_signups: false
        }
    );

    // A command that isn't Auth / NewUser / GetSignupStatus terminates the prelude.
    send_cmd(&mut ws, &ClientCommand::GetUnreadSummary).await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoAuth);
    expect_close(&mut ws).await;
}

// The same query also works after authentication (it's a plain command too).
#[sqlx::test]
async fn query_works_post_auth(pool: PgPool) {
    seed_admin(&pool, "boss", "bosspw").await;
    let server = spawn_app(pool, |c| c.open_signups = true).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "boss", "bosspw").await;

    send_cmd(&mut ws, &ClientCommand::GetSignupStatus).await;
    assert_eq!(
        next_reply(&mut ws).await,
        ServerEvent::SignupStatus { open_signups: true }
    );
    close_socket(&mut ws).await;
}
