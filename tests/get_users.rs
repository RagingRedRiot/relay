mod common;

use common::*;
use relay::model::UserDirectoryEntry;
use sqlx::PgPool;

fn get_users(starts_with: Option<&str>, after: Option<&str>, limit: Option<u32>) -> ClientCommand {
    ClientCommand::GetUsers {
        starts_with: starts_with.map(str::to_owned),
        after: after.map(str::to_owned),
        limit,
    }
}

// Read one directory page (skipping any interleaved live pushes).
async fn page(ws: &mut Ws) -> (Vec<UserDirectoryEntry>, bool) {
    match next_reply(ws).await {
        ServerEvent::Users { users, has_more } => (users, has_more),
        other => panic!("expected Users, got {other:?}"),
    }
}

fn usernames(users: &[UserDirectoryEntry]) -> Vec<String> {
    users.iter().map(|u| u.username.clone()).collect()
}

// The directory is ordered by username and pages via the `after` keyset cursor,
// reporting `has_more` until the last page.
#[sqlx::test]
async fn lists_users_ordered_and_paged(pool: PgPool) {
    for name in ["alice", "bob", "carol", "dave"] {
        seed_user(&pool, name, "pw").await;
    }
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "pw").await;

    // First page of two: ordered, more to come.
    send_cmd(&mut ws, &get_users(None, None, Some(2))).await;
    let (first, has_more) = page(&mut ws).await;
    assert_eq!(usernames(&first), vec!["alice", "bob"]);
    assert!(has_more);

    // Continue past the last username seen.
    send_cmd(&mut ws, &get_users(None, Some("bob"), Some(2))).await;
    let (second, has_more) = page(&mut ws).await;
    assert_eq!(usernames(&second), vec!["carol", "dave"]);
    assert!(!has_more, "the final page reports no more");

    close_socket(&mut ws).await;
}

// `starts_with` filters by a case-insensitive username prefix.
#[sqlx::test]
async fn starts_with_filters_by_prefix(pool: PgPool) {
    for name in ["alan", "alice", "bob"] {
        seed_user(&pool, name, "pw").await;
    }
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "bob", "pw").await;

    // Mixed-case prefix still matches.
    send_cmd(&mut ws, &get_users(Some("AL"), None, None)).await;
    let (users, has_more) = page(&mut ws).await;
    assert_eq!(usernames(&users), vec!["alan", "alice"]);
    assert!(!has_more);

    close_socket(&mut ws).await;
}

// A `_` or `%` in the prefix is matched literally, not as a LIKE wildcard.
#[sqlx::test]
async fn starts_with_escapes_like_metacharacters(pool: PgPool) {
    for name in ["a_b", "axb", "acb"] {
        seed_user(&pool, name, "pw").await;
    }
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "a_b", "pw").await;

    // "a_" must match only the literal "a_b", not "axb"/"acb".
    send_cmd(&mut ws, &get_users(Some("a_"), None, None)).await;
    let (users, _) = page(&mut ws).await;
    assert_eq!(usernames(&users), vec!["a_b"]);

    close_socket(&mut ws).await;
}

// An admin caller sees each entry's `is_admin` flag.
#[sqlx::test]
async fn admin_sees_admin_flag(pool: PgPool) {
    seed_admin(&pool, "boss", "pw").await;
    seed_user(&pool, "alice", "pw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "boss", "pw").await;

    send_cmd(&mut ws, &get_users(None, None, None)).await;
    let (users, _) = page(&mut ws).await;
    let alice = users.iter().find(|u| u.username == "alice").unwrap();
    let boss = users.iter().find(|u| u.username == "boss").unwrap();
    assert_eq!(alice.is_admin, Some(false));
    assert_eq!(
        boss.is_admin,
        Some(true),
        "admin caller can identify other admins"
    );

    close_socket(&mut ws).await;
}

// A regular caller never sees admin status: the flag is omitted (None) for every
// entry, including the admins themselves.
#[sqlx::test]
async fn regular_user_does_not_see_admin_flag(pool: PgPool) {
    seed_admin(&pool, "boss", "pw").await;
    seed_user(&pool, "alice", "pw").await;
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "alice", "pw").await;

    send_cmd(&mut ws, &get_users(None, None, None)).await;
    let (users, _) = page(&mut ws).await;
    assert!(
        users.iter().all(|u| u.is_admin.is_none()),
        "non-admin callers must not see is_admin on any entry"
    );

    close_socket(&mut ws).await;
}

// The page size is capped, and an unfiltered listing is available to any
// authenticated (non-admin) user.
#[sqlx::test]
async fn limit_is_clamped(pool: PgPool) {
    for i in 0..5 {
        seed_user(&pool, &format!("user{i}"), "pw").await;
    }
    let server = spawn_app(pool.clone(), |_| {}).await;

    let mut ws = create_socket(server.addr).await;
    authenticate(&mut ws, "user0", "pw").await;

    // Asking for more than the hard cap (100) still works; here we just confirm an
    // oversized request returns everything we have (5 < cap) and reports no more.
    send_cmd(&mut ws, &get_users(None, None, Some(10_000))).await;
    let (users, has_more) = page(&mut ws).await;
    assert_eq!(users.len(), 5);
    assert!(!has_more);

    close_socket(&mut ws).await;
}
