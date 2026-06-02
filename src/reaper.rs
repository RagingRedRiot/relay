use std::time::Duration;

use sqlx::PgPool;
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

// Grace period for uploads abandoned mid-stream. Their parent message is younger
// than retention_days (else the message rule would take them first), so they need
// their own sweep. Generous enough that a stalled client can still resume by
// filling the missing seqs; short enough that orphaned partials don't sit for the
// full retention window. Matches the ~24h the schema's partial index is sized for.
const INCOMPLETE_UPLOAD_GRACE_HOURS: i32 = 24;

pub fn spawn(shutdown: CancellationToken, pool: PgPool, retention_days: i32, interval: Duration) {
    tokio::spawn(async move {
        let mut tick = time::interval(interval);
        loop {
            select! {
                _ = tick.tick() => {
                    if let Err(e) = reap(&pool, retention_days).await {
                        tracing::error!(error = %e, "reaper: run failed");
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
    let messages =
        sqlx::query("DELETE FROM messages WHERE timestamp < now() - make_interval(days => $1)")
            .bind(retention_days)
            .execute(&mut *tx)
            .await?
            .rows_affected();

    let rooms = sqlx::query(
        "DELETE FROM rooms r
        WHERE NOT EXISTS (SELECT 1 FROM messages m WHERE m.room_id = r.room_id)
            AND uuid_extract_timestamp(r.room_id) < now() - make_interval(days => $1)",
    )
    .bind(retention_days)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    // Pending invites and join requests age out by their own creation time, so
    // ones that are never acted on (accepted/declined, approved/rejected) can't
    // accumulate unbounded. (Those for a reaped room are already gone via the
    // ON DELETE CASCADE above; this handles stale rows in rooms that survive.)
    let invites = sqlx::query(
        "DELETE FROM room_invites WHERE created_at < now() - make_interval(days => $1)",
    )
    .bind(retention_days)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    let join_requests = sqlx::query(
        "DELETE FROM room_join_requests WHERE created_at < now() - make_interval(days => $1)",
    )
    .bind(retention_days)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    // Uploads abandoned mid-stream: the attachment row and any partial chunks were
    // committed alongside a still-young message, so neither the message rule above
    // nor the parent's ON DELETE CASCADE will reclaim them. Sweep incomplete rows
    // past the grace period; their chunks cascade. Completed attachments are left
    // to age out with their message. Uses the message_attachments_incomplete index.
    let incomplete_attachments = sqlx::query(
        "DELETE FROM message_attachments
            WHERE NOT is_complete
              AND created_at < now() - make_interval(hours => $1)",
    )
    .bind(INCOMPLETE_UPLOAD_GRACE_HOURS)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    tx.commit().await?;

    tracing::info!(
        messages,
        rooms,
        invites,
        join_requests,
        incomplete_attachments,
        "reaper run complete"
    );

    Ok(())
}
