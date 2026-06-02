use std::net::SocketAddr;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use relay::app::app;
use relay::auth;
use relay::config::Config;
use relay::control::{ControlSignal, ServerControl};
use relay::model::{NewCredential, Password};
use relay::user::ensure_admin;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    // Process-once setup. Tracing MUST be initialized exactly once -- it lives
    // across restarts, and a second init would panic.
    tracing_subscriber::fmt::init();

    // Control plane. Anything inside the app (via `ServerControl` in `AppState`) or
    // the OS-signal task below sends a `ControlSignal` here to drive the process
    // lifecycle. The supervisor loop is the sole consumer.
    let (control_tx, mut control_rx) = mpsc::channel::<ControlSignal>(8);

    // OS signals (SIGTERM / Ctrl-C) always mean "shut the whole process down".
    tokio::spawn({
        let control_tx = control_tx.clone();
        async move {
            shutdown_signal().await;
            let _ = control_tx.send(ControlSignal::Shutdown).await;
        }
    });

    // Bind the listen socket once for the whole process and keep it across restarts.
    // A restart never drops the port: in-flight connections queue in the backlog
    // instead of being refused, and there's no rebind race. (Changing the bind
    // address therefore requires a full process restart, not a hot restart.)
    let bind = Config::from_env().expect("invalid config").bind;
    let listener = std::net::TcpListener::bind(&bind).expect("failed to bind listener");
    listener
        .set_nonblocking(true)
        .expect("failed to set listener non-blocking");

    // Supervisor loop. Each pass stands the entire server up, runs it until a
    // control signal arrives, then tears it all down cleanly. A Restart loops back
    // into a fresh init; a Shutdown breaks out and the process exits.
    loop {
        match serve_once(control_tx.clone(), &mut control_rx, &listener).await {
            ControlSignal::Restart => {
                tracing::info!("restart requested — re-initializing");
                continue;
            }
            ControlSignal::Shutdown => {
                tracing::info!("shutdown requested — exiting");
                break;
            }
        }
    }
}

// One full server lifecycle: initialize every per-run resource, serve, and on the
// first control signal cancel everything and drain. Returns the signal that ended
// the pass.
//
// Every resource created here -- config, pool, the per-run cancellation token, the
// actors, the listener, the router -- is owned by this function and dropped when it
// returns. That is what makes a restart clean: the next pass re-reads config and
// re-creates the world from scratch, with nothing leaking across.
async fn serve_once(
    control_tx: mpsc::Sender<ControlSignal>,
    control_rx: &mut mpsc::Receiver<ControlSignal>,
    listener: &std::net::TcpListener,
) -> ControlSignal {
    let config = Config::from_env().expect("invalid config");

    let pool = PgPool::connect(&config.database_url).await.unwrap();
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("database migrations failed");

    ensure_admin(
        pool.clone(),
        &config.admin_username,
        NewCredential {
            password: Password(config.admin_credential.to_owned()),
        },
    )
    .await
    .expect("failed to ensure default admin");

    // Per-run cancellation. Cancelling this stops the server and every actor for
    // THIS pass only -- the supervisor loop above never sees it. Sessions take child
    // tokens of it, so a single cancel tears the whole tree down.
    let run_shutdown = CancellationToken::new();

    let auth_handle = auth::spawn(run_shutdown.clone(), pool.clone()).await;
    relay::reaper::spawn(
        run_shutdown.clone(),
        pool.clone(),
        config.retention_days,
        Duration::from_secs(config.reap_interval_secs),
    );

    // A per-pass tokio listener over a dup of the persistent socket. This dup's fd
    // is dropped when the pass ends (axum::serve consumes it); the original in
    // `main` keeps the port bound, so the socket survives the restart. config.bind
    // is intentionally not re-read here — the port is fixed for the process.
    let listener = listener
        .try_clone()
        .expect("failed to clone listener for this pass");
    listener
        .set_nonblocking(true)
        .expect("failed to set listener non-blocking");
    let listener =
        tokio::net::TcpListener::from_std(listener).expect("failed to adopt cloned listener");

    let app = app(
        run_shutdown.clone(),
        auth_handle,
        config,
        pool,
        ServerControl::new(control_tx),
    )
    .await;

    let serve_shutdown = run_shutdown.clone();
    let mut server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move { serve_shutdown.cancelled().await })
        .await
    });

    // Run until a control signal arrives, or until the server stops on its own.
    let signal;
    tokio::select! {
        sig = control_rx.recv() => {
            // The channel can't close while the OS-signal task holds a sender, but
            // default to Shutdown if it somehow does.
            signal = sig.unwrap_or(ControlSignal::Shutdown);
            run_shutdown.cancel();   // stop the server, actors, and sessions
            let _ = server.await;    // wait for axum's graceful drain to finish
        }
        res = &mut server => {
            // The server future ended itself (bind drop, fatal error). Treat it as a
            // shutdown rather than spinning the supervisor.
            if let Ok(Err(e)) = res {
                tracing::error!(error = %e, "server stopped unexpectedly");
            }
            run_shutdown.cancel();   // make sure the actors stop too
            signal = ControlSignal::Shutdown;
        }
    }

    // pool, handles, listener, and the router all drop as this returns -> clean slate
    // for the next pass.
    signal
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { println!("\nSHUTDOWN REQUESTED!") },
        _ = terminate => { println!("\nSHUTDOWN REQUESTED!") },
    }
}
