# Relay

A self-hosted real-time chat platform written in async Rust.

> **Status:** Early development. POC stage — interfaces will change. Not production ready.

## Overview

Relay is a WebSocket chat server built on Axum, Tokio, and Postgres. Clients exchange a tagged JSON protocol over WebSocket; the server handles connection lifecycle, per-IP rate limiting, and graceful shutdown around it. User accounts, credentials, and admin authority are persisted in Postgres; password verification uses Argon2id.

The project is being built layer by layer — connection handling first, then the protocol shape, then real authentication and user management.

## Architecture

**Actor-per-domain.** The two stateful domains — authentication and user management — each run as their own task. Callers interact through a handle (mpsc request in, oneshot reply back). This keeps domain logic on a single task, removes the need for locks around shared state, and gives each domain a clear seam for tests.

**Library + binary split.** Production code lives in the `relay` library; the binary is a thin entry point that loads config and wires everything together. Integration tests build the real `app()` against a real Postgres database and exercise the WebSocket protocol end-to-end — no mocks of the database or auth path.

**Cancellation as a first-class concern.** Every long-lived task takes a `CancellationToken`. On `SIGTERM` or Ctrl-C the root token cancels and tasks shut down cooperatively. A dedicated shutdown test guards against regressions.

**Schema migrations via sqlx.** All schema is defined in versioned migration files. A bootstrap path ensures the default admin exists on startup from environment config, so a fresh deployment is never left without an entry point.

## Design choices

**Minimal client-event vocabulary.** The protocol intentionally collapses many backend outcomes into a small fixed set of `ServerEvent`s (`Success`, `Failed`, `NoChange`, `NoUserExists`, `NoAuth`, …). A legitimate client can't act on richer detail. Exposing distinct events for "wrong password" vs "no such user" vs "malformed JSON" mainly helps spoofed clients map backend state. Detail is intended to live in server-side logs (see Roadmap), not on the wire.

**Flat admin tier with a protected default admin.** Admins are peers — any admin can edit, delete, promote, or demote any user. The single exception is the default admin, who can't be edited, deleted, demoted, or password-updated through the app; their credentials are managed via environment config and applied on startup. The role is break-glass: if every other admin is compromised, the maintainer logs in with the bootstrap credentials and prunes. Requiring default-admin authority for routine admin-on-admin moves would push that account into daily use and erode its purpose.

**Just-in-time authorization.** Admin status is re-checked at the moment of each privileged action, not cached for the session. A demoted admin loses authority on their next command, on the same socket. The cost is one extra database read per privileged call; the gain is no class of stale-session privilege bugs.

**Idempotency via database constraints where they can express it.** Promote uses `INSERT ... ON CONFLICT DO NOTHING` and branches on `rows_affected()` — 1 means promoted, 0 means already an admin, both are successful outcomes. Demote can't express both "non-admin target" and "default-admin target" through `rows_affected()` alone, so it does a pre-flight read; the asymmetry is structural, not stylistic.

## Security recommendations

The default admin exists to make in-app compromise recoverable: if every other admin account is taken over, the maintainer logs in with the bootstrap credentials and uses normal in-app actions (demote, delete, password reset) to evict the attacker. This only works if the default admin is preserved for that role — used to bootstrap the first non-default admin at deployment, then left dormant until something has gone wrong.

The consequence is that the in-app surface is not where the most consequential attacks live. The default admin's credentials are loaded from environment config, and the authority of every other admin is recorded in the database. An attacker who reaches either of those substrates bypasses the app's authorization model entirely. For deployments, that means:

- Treat the environment config (`.env` or whatever feeds it) as the highest-value asset. Restrict who can read it, audit changes, and rotate the default-admin password on a deliberate schedule.
- Treat the Postgres instance as the second highest-value asset. Restrict network access, give the application a dedicated role without superuser privileges, and audit any direct-database write paths.
- Use the default admin to seed the first non-default admin at deployment and then stop. Day-to-day administration should happen through non-default admin accounts.

## Schema

The implemented domain lives in four tables:

- `users` — profile fields keyed by UUIDv7.
- `credentials` — password hash and last-set timestamp, 1:1 with `users`, `ON DELETE CASCADE`.
- `admins` — admin grants. Records who granted it, when, and whether the row is the default admin. A partial unique index allows at most one `is_default = true`.
- `last_active` — last-seen timestamp per user.

User deletion cascades to credentials and the admin grant.

`rooms`, `memberships`, and `messages` tables exist for the chat domain but the handlers are not yet implemented.

## What's implemented

- WebSocket lifecycle, tagged JSON protocol, graceful shutdown
- Per-IP rate limiting via `tower_governor` (configurable steady-state and burst)
- DB-backed user accounts with Argon2id password verification
- User CRUD: create (open-signup or admin-gated), look up, edit profile, delete, self password update, admin password reset
- Admin model: bootstrap default admin, promote, demote, default-admin protection
- Integration tests against real Postgres via `sqlx::test`

## Roadmap

- **TLS enforcement** — harden the release configuration so plain-text listeners can't be misconfigured.
- **Rooms and messaging** — implement room create/join/leave and persistent message delivery on the existing schema.
- **Structured logging and tracing** — replace `println!` and `// TODO - Logging` markers with structured events.

## Running locally

Requirements: Rust (edition 2024) and a running Postgres instance.

```bash
cp .env.example .env
# Edit .env so DATABASE_URL points at your local Postgres
cargo run
```

The server listens on the address from `BIND` (default `0.0.0.0:3000`).

Integration tests:

```bash
cargo test
```

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
