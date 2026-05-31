use std::time::Duration;

use sqlx::PgPool;
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

pub fn spawn(shutdown: CancellationToken, pool: PgPool, retention_days: i32, interval: Duration) {
    tokio::spawn(async move {
        let mut tick = time::interval(interval);
        loop {
            select! {
                _ = tick.tick() => {
                    if let Err(_e) = reap(&pool, retention_days).await {
                        // TODO - Logging
                    }
                }
                _ = shutdown.cancelled() => break,
            }
        }
    });
}

// Delete data that has aged past `retention_days`, in one transaction. Exposed
// (rather than only driven by the interval loop) so it can be invoked directly,
// e.g. from tests.
pub async fn reap(pool: &PgPool, retention_days: i32) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM messages WHERE timestamp < now() - make_interval(days => $1)")
        .bind(retention_days)
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        "DELETE FROM rooms r
        WHERE NOT EXISTS (SELECT 1 FROM messages m WHERE m.room_id = r.room_id)
            AND uuid_extract_timestamp(r.room_id) < now() - make_interval(days => $1)",
    )
    .bind(retention_days)
    .execute(&mut *tx)
    .await?;

    // Pending invites and join requests age out by their own creation time, so
    // ones that are never acted on (accepted/declined, approved/rejected) can't
    // accumulate unbounded. (Those for a reaped room are already gone via the
    // ON DELETE CASCADE above; this handles stale rows in rooms that survive.)
    sqlx::query("DELETE FROM room_invites WHERE created_at < now() - make_interval(days => $1)")
        .bind(retention_days)
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        "DELETE FROM room_join_requests WHERE created_at < now() - make_interval(days => $1)",
    )
    .bind(retention_days)
    .execute(&mut *tx)
    .await?;

    tx.commit().await
}
