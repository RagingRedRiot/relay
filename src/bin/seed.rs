//! Development seed — creates test users, rooms, and messages so the full
//! UI can be explored without any manual setup.
//!
//! Usage:
//!   cargo run --bin seed
//!
//! Idempotent: users and rooms that already exist are skipped; messages are
//! only inserted when the messages table is empty, so a partial re-run won't
//! duplicate them.
//!
//! Credentials after seeding:
//!   admin  / admin123   (admin)
//!   alice  / password
//!   bob    / password
//!   carol  / password

use relay::config::Config;
use relay::model::{NewCredential, NewUser, Password};
use relay::user::{UserResponse, ensure_admin, spawn as spawn_users};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    dotenvy::dotenv().ok();

    // Use the same config the server uses so admin credentials stay in sync.
    // ensure_admin is called on every server start and overwrites the password,
    // so the seed must use the same values or the seeded password will be lost.
    let config = Config::from_env().map_err(|e| format!("config error: {e}"))?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await?;

    println!("running migrations...");
    sqlx::migrate!("./migrations").run(&pool).await?;

    println!("\n── users ─────────────────────────────────────────────");
    create_users(&pool, &config).await?;

    println!("\n── rooms ─────────────────────────────────────────────");
    create_rooms(&pool, &config.admin_username).await?;

    println!("\n── messages ──────────────────────────────────────────");
    create_messages(&pool, &config.admin_username).await?;

    println!("\ndone.");
    Ok(())
}

async fn create_users(pool: &PgPool, config: &Config) -> Result<(), BoxError> {
    // Admin — use the same username + credential the server reads from config so
    // they stay in sync (ensure_admin overwrites the password on every server start).
    ensure_admin(
        pool.clone(),
        &config.admin_username,
        NewCredential {
            password: Password(config.admin_credential.clone()),
        },
    )
    .await?;
    println!(
        "  {}  / {}  (admin)",
        config.admin_username, config.admin_credential
    );

    // Regular users — spawn one actor instance for all three.
    let shutdown = CancellationToken::new();
    let handle = spawn_users(shutdown.clone(), pool.clone()).await;

    for (username, password) in [
        ("alice", "password"),
        ("bob", "password"),
        ("carol", "password"),
    ] {
        let resp = handle
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

        match resp {
            UserResponse::UserCreated { .. } => println!("  {username}  / {password}"),
            _ => println!("  {username} already exists, skipping"),
        }
    }

    shutdown.cancel();
    Ok(())
}

async fn create_rooms(pool: &PgPool, admin: &str) -> Result<(), BoxError> {
    // (room_name, owner, is_public, is_discoverable, extra_members)
    let rooms: Vec<(&str, &str, bool, bool, &[&str])> = vec![
        // Fully public: anyone can join, appears in the discover list.
        ("general", admin, true, true, &["alice", "bob", "carol"]),
        // Public and discoverable: smaller group.
        ("random", "alice", true, true, &["bob"]),
        // Private but discoverable: visible in discover list, join requires request.
        ("staff", admin, false, true, &[]),
        // Private and non-discoverable: invitation-only, hidden from discover.
        ("secret", "alice", false, false, &["bob"]),
    ];

    for (room_name, owner, is_public, is_discoverable, extra) in rooms {
        let created: Option<bool> = sqlx::query_scalar(
            "INSERT INTO rooms (room_name, is_public, is_discoverable)
             VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING
             RETURNING true",
        )
        .bind(room_name)
        .bind(is_public)
        .bind(is_discoverable)
        .fetch_optional(pool)
        .await?;

        if created.is_none() {
            println!("  #{room_name} already exists, skipping");
            continue;
        }

        sqlx::query(
            "INSERT INTO memberships (room_id, user_id, is_owner)
             SELECT r.room_id, u.user_id, true
             FROM rooms r, users u
             WHERE r.room_name = $1 AND u.username = $2
             ON CONFLICT DO NOTHING",
        )
        .bind(room_name)
        .bind(owner)
        .execute(pool)
        .await?;

        for member in extra {
            sqlx::query(
                "INSERT INTO memberships (room_id, user_id, is_owner)
                 SELECT r.room_id, u.user_id, false
                 FROM rooms r, users u
                 WHERE r.room_name = $1 AND u.username = $2
                 ON CONFLICT DO NOTHING",
            )
            .bind(room_name)
            .bind(member)
            .execute(pool)
            .await?;
        }

        let vis = match (is_public, is_discoverable) {
            (true, _) => "public",
            (false, true) => "private, discoverable",
            (false, false) => "private",
        };
        println!("  #{room_name}  ({vis}, owner: {owner})");
    }

    Ok(())
}

async fn create_messages(pool: &PgPool, admin: &str) -> Result<(), BoxError> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(pool)
        .await?;

    if count > 0 {
        println!("  messages table not empty, skipping");
        return Ok(());
    }

    let admin_welcome = "welcome to relay. let me know if anything looks off".to_owned();
    let staff_msg = "staff-only channel — private but appears in the discover list".to_owned();

    // (room, sender, content)
    let messages: Vec<(&str, &str, &str)> = vec![
        ("general", "alice", "hey everyone, system seems to be up"),
        ("general", "bob", "nice, glad it's working"),
        ("general", "carol", "hello!"),
        (
            "general",
            "alice",
            "you can open more rooms from the directory — click the hamburger",
        ),
        ("general", admin, &admin_welcome),
        (
            "random",
            "bob",
            "anyone want to chat about nothing in particular?",
        ),
        ("random", "alice", "always"),
        (
            "random",
            "bob",
            "great, because i have a lot of nothing to say",
        ),
        ("staff", admin, &staff_msg),
        (
            "secret",
            "alice",
            "this room is private and non-discoverable, invite-only",
        ),
        ("secret", "bob", "sneaky"),
    ];

    let n = messages.len();
    for (room, sender, content) in messages {
        sqlx::query(
            "INSERT INTO messages (room_id, sender_id, sender_username_snapshot, content)
             SELECT r.room_id, u.user_id, u.username, $3
             FROM rooms r, users u
             WHERE r.room_name = $1 AND u.username = $2",
        )
        .bind(room)
        .bind(sender)
        .bind(content)
        .execute(pool)
        .await?;
    }

    println!("  {n} messages inserted");
    Ok(())
}
