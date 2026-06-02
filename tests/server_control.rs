mod common;

use common::*;
use relay::control::ControlSignal;
use sqlx::PgPool;
use tokio::sync::mpsc::error::TryRecvError;

async fn login(server: &TestServer, username: &str, password: &str) -> Ws {
    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, username, password).await;
    ws
}

#[sqlx::test]
async fn admin_can_restart(pool: PgPool) {
    seed_admin(&pool, "root", "rootpw").await;
    let mut server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = login(&server, "root", "rootpw").await;
    send_cmd(&mut ws, &ClientCommand::RestartServer).await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Success);

    // The command signaled the supervisor to restart.
    assert_eq!(server.control_rx.recv().await, Some(ControlSignal::Restart));
}

#[sqlx::test]
async fn admin_can_shutdown(pool: PgPool) {
    seed_admin(&pool, "root", "rootpw").await;
    let mut server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = login(&server, "root", "rootpw").await;
    send_cmd(&mut ws, &ClientCommand::ShutdownServer).await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Success);

    assert_eq!(
        server.control_rx.recv().await,
        Some(ControlSignal::Shutdown)
    );
}

#[sqlx::test]
async fn any_admin_can_restart_not_just_default(pool: PgPool) {
    // bob is a non-default admin: lifecycle commands require *admin*, not the
    // default admin specifically.
    seed_extra_admin(&pool, "bob", "bobpw").await;
    let mut server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = login(&server, "bob", "bobpw").await;
    send_cmd(&mut ws, &ClientCommand::RestartServer).await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Success);

    assert_eq!(server.control_rx.recv().await, Some(ControlSignal::Restart));
}

#[sqlx::test]
async fn non_admin_cannot_restart(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    let mut server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = login(&server, "alice", "alicepw").await;
    send_cmd(&mut ws, &ClientCommand::RestartServer).await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Failed);

    // No signal was emitted, and the session survives.
    assert!(matches!(
        server.control_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));
    close_socket(&mut ws).await;
}

#[sqlx::test]
async fn non_admin_cannot_shutdown(pool: PgPool) {
    seed_user(&pool, "alice", "alicepw").await;
    let mut server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = login(&server, "alice", "alicepw").await;
    send_cmd(&mut ws, &ClientCommand::ShutdownServer).await;
    assert_eq!(next_reply(&mut ws).await, ServerEvent::Failed);

    assert!(matches!(
        server.control_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));
    close_socket(&mut ws).await;
}
