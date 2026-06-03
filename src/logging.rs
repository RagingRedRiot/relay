//! Logging policy and subscriber setup.
//!
//! This module is the single source of truth for *how* the server logs. The
//! conventions below are deliberately mechanical so that the ~150 call sites
//! across the actors stay consistent.
//!
//! ## Levels
//!
//! - `error!` — server-side faults an operator must fix: DB `begin`/`acquire`/
//!   `commit`/query failures, corrupt stored password hash, hash generation
//!   failure, actor-channel send failures, reaper failures.
//! - `warn!` — security-relevant denials and anomalies: authorization denials
//!   (JIT re-check fails), failed logins, signup rejected when `open_signups`
//!   is off.
//! - `info!` — lifecycle and operator-visible state: bind/start, restart/
//!   shutdown requested, connection authenticated, reaper run summaries, and
//!   successful admin actions (promote/demote/delete-user/reset-password).
//! - `debug!` — client-caused noise and fine lifecycle: malformed JSON, unknown
//!   commands, connection accept/close.
//! - `trace!` — reserved (e.g. per-chunk upload detail) if ever needed.
//!
//! The mapping is mechanical: today's `Err(_e) => { return Failed }` arms become
//! `Err(e) => { error!(error = %e, ...); return Failed }`. The error that was
//! being discarded *is* the log payload — the client still sees only a generic
//! `ServerEvent`, the detail goes to the server log.
//!
//! ## Targets
//!
//! Most events use the default crate target (operational logs). Security-
//! relevant events are emitted under the dedicated [`AUDIT`] target so operators
//! can route or retain them separately, e.g. `RUST_LOG=relay::audit=info`. Use
//! it for authentication outcomes, authorization denials, signup gating, admin
//! actions, and server control (restart/shutdown):
//!
//! ```ignore
//! tracing::warn!(target: logging::AUDIT, actor = %caller, target = %username, "promote denied");
//! ```
//!
//! ## Spans
//!
//! Each connection is wrapped in a per-connection span carrying `who` (peer
//! addr) and, once authenticated, `user_id`. Dispatch-side events inherit it.
//! The actors (`auth`/`user`/`room`/`message`) run as *separate tasks*, so their
//! events do not inherit the connection span — they must carry the relevant IDs
//! (`user_id`, `actor`, `target`, `room`) explicitly from the request.
//!
//! ## Field naming
//!
//! Keep keys consistent across call sites: `error = %e`, `who = %addr`,
//! `user_id`, `actor` (the caller), `target` (the object: username/room),
//! `action` (only when the message string isn't enough).
//!
//! ## Never log
//!
//! Passwords, password hashes, attachment bytes, and message body text never go
//! to the log. The [`crate::model::Password`] and [`crate::model::PasswordHash`]
//! newtypes already have redacting `Debug` impls, so they cannot leak through a
//! structured field — keep it that way.
//!
//! ## Retention
//!
//! Logs are written to stdout/stderr as a stream; they are **not** stored in the
//! database and are therefore **not** subject to the [`crate::reaper`], which
//! only ages out DB rows. Retention belongs to the platform layer (journald,
//! Docker/k8s logging drivers, or a log shipper). Audit logs typically warrant a
//! separate, longer-retained sink. If SQL-queryable audit history is ever
//! required, that would be a dedicated `audit_log` table with its own retention
//! policy — deliberately *not* something the message reaper deletes, since an
//! app that erases its own audit trail defeats the purpose of having one.

use serde::Deserialize;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Tracing target for security-relevant audit events. Route separately via
/// `RUST_LOG=relay::audit=info`.
pub const AUDIT: &str = "relay::audit";

/// Output format for the subscriber.
///
/// - `Pretty` — human-readable formatter to stdout. The default; best for local
///   dev and tests.
/// - `Json` — one JSON object per event to stdout, for log collectors (Loki,
///   CloudWatch, …) that parse fields back out.
/// - `Journald` — write natively to the systemd journal: tracing levels map to
///   journal `PRIORITY` (so `journalctl -p err` works) and each field becomes a
///   real journal field (so `journalctl RELAY_USER_ID=…` works). The intended
///   production format; set `LOG_FORMAT=journald` in the service unit. If the
///   journal socket is unavailable (e.g. running outside systemd), it falls back
///   to the `Pretty` stdout formatter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
    Journald,
}

/// Initialize the global tracing subscriber. Process-once: must be called
/// exactly once, before any logging, and never across a hot restart (a second
/// init would panic).
///
/// `RUST_LOG`, when set, takes precedence over `level`; otherwise `level` (from
/// config) is used as the filter directive.
pub fn init(level: &str, format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let registry = tracing_subscriber::registry().with(filter);
    match format {
        LogFormat::Pretty => registry.with(fmt::layer()).init(),
        LogFormat::Json => registry.with(fmt::layer().json()).init(),
        LogFormat::Journald => match tracing_journald::layer() {
            Ok(journald) => registry.with(journald).init(),
            // No journal socket (not under systemd, or it's unavailable). Fall
            // back to stdout so we still get logs, then report why once the
            // subscriber is live.
            Err(e) => {
                registry.with(fmt::layer()).init();
                tracing::warn!(error = %e, "journald unavailable; falling back to stdout");
            }
        },
    }
}
