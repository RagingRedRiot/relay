use tokio::sync::mpsc;

// What the supervisor loop in `main` should do when the current server pass ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlSignal {
    // Tear the running server down and stand a fresh one back up: re-read config,
    // re-create the pool, re-spawn every actor. The process keeps running.
    Restart,
    // Tear the running server down and exit the process.
    Shutdown,
}

// Handle for asking the supervisor to restart or shut down from *inside* the running
// app -- e.g. a future admin command. Lives in `AppState`, so anything with access
// to it can drive the process lifecycle. Cloneable and cheap.
//
// The supervisor (`main`) holds the receiving end. Sends are best-effort: if the
// supervisor has already torn this pass down, the signal is simply dropped.
#[derive(Clone)]
pub struct ServerControl {
    tx: mpsc::Sender<ControlSignal>,
}

impl ServerControl {
    pub fn new(tx: mpsc::Sender<ControlSignal>) -> Self {
        Self { tx }
    }

    // Driven by the admin RestartServer / ShutdownServer commands; the consumer is
    // the supervisor loop in `main`.
    pub async fn restart(&self) {
        let _ = self.tx.send(ControlSignal::Restart).await;
    }

    pub async fn shutdown(&self) {
        let _ = self.tx.send(ControlSignal::Shutdown).await;
    }
}
