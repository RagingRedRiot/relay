// Shared test support. Each integration test file pulls this in with
// `mod common;`. Not every test uses every helper, hence the allow.
#![allow(dead_code)]

use std::net::SocketAddr;

use sqlx::PgPool;
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub use futures_util::{SinkExt, StreamExt};
pub use tokio_tungstenite::tungstenite::Message;

pub use relay::model::{ClientCommand, Password, ServerEvent};

use relay::config::Config;
use relay::model::{NewCredential, NewUser};
use relay::user::{UserResponse, ensure_admin};

pub type Ws = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

// A running test instance of the app.
pub struct TestServer {
    pub addr: SocketAddr,
    pub pool: PgPool,
    pub shutdown: CancellationToken,
    // Receiving end of the lifecycle control channel. In production the supervisor
    // in `main` consumes this; here a test holds it so it can assert that a
    // RestartServer / ShutdownServer command actually signaled.
    pub control_rx: tokio::sync::mpsc::Receiver<relay::control::ControlSignal>,
}

/// Serve the app on an ephemeral port against the given (`#[sqlx::test]`) pool.
///
/// Config is loaded from the environment, then given generous rate limits so
/// unrelated tests never hit throttling, and `open_signups = false`. The
/// `configure` closure runs last, letting a test override any of that.
pub async fn spawn_app(pool: PgPool, configure: impl FnOnce(&mut Config)) -> TestServer {
    dotenvy::dotenv().ok();
    let mut config = Config::from_env().expect("invalid config");
    config.rate_limit_per_second = 1000;
    config.rate_limit_burst = 1000;
    config.open_signups = false;
    configure(&mut config);

    let shutdown = CancellationToken::new();
    let auth_handle = relay::auth::spawn(shutdown.clone(), pool.clone()).await;
    // The control receiver is handed to the TestServer rather than a supervisor, so
    // lifecycle-command tests can observe the emitted signal.
    let (control_tx, control_rx) = tokio::sync::mpsc::channel(8);
    let app = relay::app::app(
        shutdown.clone(),
        auth_handle,
        config,
        pool.clone(),
        relay::control::ServerControl::new(control_tx),
    )
    .await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await;
        }
    });

    TestServer {
        addr,
        pool,
        shutdown,
        control_rx,
    }
}

// ##### Seeding #####

// Seed a default admin (user + credentials + admins row) via ensure_admin.
pub async fn seed_admin(pool: &PgPool, username: &str, password: &str) {
    ensure_admin(
        pool.clone(),
        username,
        NewCredential {
            password: Password(password.to_owned()),
        },
    )
    .await
    .expect("failed to seed admin");
}

// Seed a non-default admin: create the user through the normal actor path,
// then INSERT directly into `admins` with is_default = false. There is no
// public promote-to-admin command yet, and the `admins_one_default` partial
// unique index only constrains rows where is_default is true, so multiple
// non-default admins are fine. Useful for exercising auth paths that depend
// on the default-admin distinction (e.g. delete_user's is_default guard).
pub async fn seed_extra_admin(pool: &PgPool, username: &str, password: &str) {
    seed_user(pool, username, password).await;
    sqlx::query(
        "INSERT INTO admins (user_id, is_default)
         SELECT user_id, false FROM users WHERE username = $1",
    )
    .bind(username)
    .execute(pool)
    .await
    .expect("failed to promote user to non-default admin");
}

// Seed a regular (non-admin) user through the real user actor, so seeding
// exercises the same code path as production rather than raw SQL.
pub async fn seed_user(pool: &PgPool, username: &str, password: &str) {
    let shutdown = CancellationToken::new();
    let user_handle = relay::user::spawn(shutdown.clone(), pool.clone()).await;
    let response = user_handle
        .new_user(
            NewUser {
                username: username.to_owned(),
                first_name: None,
                last_name: None,
                alias: None,
            },
            NewCredential {
                password: Password(password.to_owned()),
            },
        )
        .await;
    assert!(
        matches!(response, UserResponse::UserCreated { .. }),
        "failed to seed user {username}: {response:?}"
    );
    shutdown.cancel();
}

// ##### Socket helpers #####

pub async fn create_socket(addr: SocketAddr) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    ws
}

pub async fn send_cmd(ws: &mut Ws, cmd: &ClientCommand) {
    ws.send(Message::Text(serde_json::to_string(cmd).unwrap().into()))
        .await
        .unwrap();
}

// Like send_cmd, but returns false instead of panicking when the socket is
// gone — for "spam until the server cuts us off" style tests.
pub async fn try_send_cmd(ws: &mut Ws, cmd: &ClientCommand) -> bool {
    ws.send(Message::Text(serde_json::to_string(cmd).unwrap().into()))
        .await
        .is_ok()
}

// Send a raw text frame — for exercising malformed / non-protocol input.
pub async fn send_text(ws: &mut Ws, text: impl Into<String>) {
    let text: String = text.into();
    ws.send(Message::Text(text.into())).await.unwrap();
}

// Send one upload chunk as a binary frame, mirroring the wire layout the server
// parses in process_binary: [attachment_id 16B][seq u32 big-endian 4B][payload].
pub async fn send_chunk_frame(ws: &mut Ws, attachment_id: Uuid, seq: i32, payload: &[u8]) {
    let mut frame = Vec::with_capacity(20 + payload.len());
    frame.extend_from_slice(attachment_id.as_bytes());
    frame.extend_from_slice(&(seq as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    ws.send(Message::Binary(frame.into()))
        .await
        .expect("failed to send chunk frame");
}

// Read the next frame and parse it as a ServerEvent.
pub async fn next_event(ws: &mut Ws) -> ServerEvent {
    let msg = ws
        .next()
        .await
        .expect("connection closed unexpectedly")
        .expect("websocket error");
    let text = msg.to_text().expect("expected a text frame");
    serde_json::from_str::<ServerEvent>(text)
        .unwrap_or_else(|e| panic!("not a ServerEvent ({e}): {text:?}"))
}

// Read the next command *reply*, skipping live push events (NewMessage, Resync)
// that interleave on the shared socket once a session is subscribed to room
// fan-out. Use this anywhere a specific reply is expected; use next_event when the
// push events themselves are under test.
pub async fn next_reply(ws: &mut Ws) -> ServerEvent {
    loop {
        match next_event(ws).await {
            ServerEvent::NewMessage { .. } | ServerEvent::Resync { .. } => continue,
            other => return other,
        }
    }
}

// Assert the next frame is a Close frame.
pub async fn expect_close(ws: &mut Ws) {
    let msg = ws
        .next()
        .await
        .expect("connection closed unexpectedly")
        .expect("websocket error");
    assert!(msg.is_close(), "expected a Close frame, got {msg:?}");
}

// Authenticate and assert success.
pub async fn authenticate(ws: &mut Ws, username: &str, password: &str) {
    send_cmd(
        ws,
        &ClientCommand::Auth {
            username: username.to_owned(),
            password: Password(password.to_owned()),
        },
    )
    .await;
    assert!(
        matches!(next_event(ws).await, ServerEvent::AuthOk { .. }),
        "expected AuthOk",
    );
}

// Gracefully close the socket; reaching the Close frame proves the
// connection was still alive.
pub async fn close_socket(ws: &mut Ws) {
    send_cmd(ws, &ClientCommand::Close).await;
    expect_close(ws).await;
}

// Open a fresh socket, authenticate, and close. Asserts AuthOk.
// Useful for "after rotation, the new password works" assertions.
pub async fn assert_password_works(addr: SocketAddr, username: &str, password: &str) {
    let mut ws = create_socket(addr).await;
    authenticate(&mut ws, username, password).await;
    close_socket(&mut ws).await;
}

// Open a fresh socket, send Auth, and assert the server rejects it.
// The server's policy on auth failure is NoAuth followed by Close, so we
// assert both. Useful for "after rotation, the old password no longer works".
pub async fn assert_password_fails(addr: SocketAddr, username: &str, password: &str) {
    let mut ws = create_socket(addr).await;
    send_cmd(
        &mut ws,
        &ClientCommand::Auth {
            username: username.to_owned(),
            password: Password(password.to_owned()),
        },
    )
    .await;
    assert_eq!(next_event(&mut ws).await, ServerEvent::NoAuth);
    expect_close(&mut ws).await;
}

pub fn new_user_cmd(username: &str, password: &str) -> ClientCommand {
    ClientCommand::NewUser {
        username: username.to_owned(),
        password: Password(password.to_owned()),
        first_name: None,
        last_name: None,
        alias: None,
    }
}

pub fn get_user_by_username_cmd(username: &str) -> ClientCommand {
    ClientCommand::GetUserByUsername {
        username: username.to_string(),
    }
}

// Mirror of ServerEvent::UserInfo minus the dynamic `created_at`, so tests
// can compare profile state with a single assert_eq.
#[derive(Debug, PartialEq)]
pub struct UserInfoFields {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub alias: Option<String>,
    pub username: String,
}

// Send GetUserByUsername and parse the UserInfo response. Panics on any
// other event so the test fails at the call site instead of later.
pub async fn fetch_user_info(ws: &mut Ws, target_username: &str) -> UserInfoFields {
    send_cmd(ws, &get_user_by_username_cmd(target_username)).await;
    match next_event(ws).await {
        ServerEvent::UserInfo {
            first_name,
            last_name,
            alias,
            username,
            created_at: _,
        } => UserInfoFields {
            first_name,
            last_name,
            alias,
            username,
        },
        other => panic!("expected UserInfo for {target_username:?}, got {other:?}"),
    }
}

// ##### Room seeding #####

// Insert a room and seed `owner_username` as its first owner-member, mirroring
// what NewRoom does but directly, so tests can set up preconditions without
// going through the protocol.
pub async fn seed_room(
    pool: &PgPool,
    room_name: &str,
    owner_username: &str,
    is_public: bool,
    is_discoverable: bool,
) {
    sqlx::query("INSERT INTO rooms (room_name, is_public, is_discoverable) VALUES ($1, $2, $3)")
        .bind(room_name)
        .bind(is_public)
        .bind(is_discoverable)
        .execute(pool)
        .await
        .expect("failed to seed room");

    sqlx::query(
        "INSERT INTO memberships (room_id, user_id, is_owner)
         SELECT r.room_id, u.user_id, true
         FROM rooms r, users u
         WHERE r.room_name = $1 AND u.username = $2",
    )
    .bind(room_name)
    .bind(owner_username)
    .execute(pool)
    .await
    .expect("failed to seed room owner");
}

// Resolve a room's id by name.
pub async fn room_id(pool: &PgPool, room_name: &str) -> Uuid {
    sqlx::query_scalar("SELECT room_id FROM rooms WHERE room_name = $1")
        .bind(room_name)
        .fetch_one(pool)
        .await
        .expect("room lookup failed")
}

// Add `username` as a plain (non-owner) member of `room_name`.
pub async fn seed_membership(pool: &PgPool, room_name: &str, username: &str) {
    sqlx::query(
        "INSERT INTO memberships (room_id, user_id, is_owner)
         SELECT r.room_id, u.user_id, false
         FROM rooms r, users u
         WHERE r.room_name = $1 AND u.username = $2",
    )
    .bind(room_name)
    .bind(username)
    .execute(pool)
    .await
    .expect("failed to seed membership");
}

// True if `username` is a member of `room_name` (owner or not).
pub async fn is_member(pool: &PgPool, room_name: &str, username: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM memberships m
            JOIN rooms r ON r.room_id = m.room_id
            JOIN users u ON u.user_id = m.user_id
            WHERE r.room_name = $1 AND u.username = $2)",
    )
    .bind(room_name)
    .bind(username)
    .fetch_one(pool)
    .await
    .expect("membership query failed")
}

// True if `username` is an owner of `room_name`.
pub async fn is_owner(pool: &PgPool, room_name: &str, username: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM memberships m
            JOIN rooms r ON r.room_id = m.room_id
            JOIN users u ON u.user_id = m.user_id
            WHERE r.room_name = $1 AND u.username = $2 AND m.is_owner)",
    )
    .bind(room_name)
    .bind(username)
    .fetch_one(pool)
    .await
    .expect("ownership query failed")
}
