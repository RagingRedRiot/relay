mod common;

use std::time::Duration;

use common::*;
use sqlx::PgPool;
use tokio::task::JoinSet;

// Cancelling the shutdown token should drop every connected client promptly.
#[sqlx::test]
async fn shutdown_disconnects_all_clients(pool: PgPool) {
    let server = spawn_app(pool, |_| {}).await;

    let mut clients = JoinSet::new();
    for _ in 1..100 {
        let addr = server.addr;
        clients.spawn(async move {
            let mut ws = create_socket(addr).await;
            while let Some(msg) = ws.next().await {
                if msg.is_err() {
                    break;
                }
            }
        });
    }

    server.shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(5), async {
        while clients.join_next().await.is_some() {}
    })
    .await
    .expect("clients did not shut down in time");
}
