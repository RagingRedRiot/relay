# Relay

A self-hosted real-time chat platform written in async Rust.

> **Status:** Early development. Not production ready.

## Overview

Relay is a WebSocket chat server built on Axum, Tokio, and Postgres. Clients exchange a tagged JSON protocol over WebSocket; the server handles connection lifecycle, per-IP rate limiting, and graceful shutdown around it. Accounts, rooms, messages, attachments, reactions, and read state are persisted in Postgres; password verification uses Argon2id. Messages fan out to connected room members in real time. The binary also serves a bundled default web client (a Svelte + TypeScript frontend embedded at compile time), so a deployment is usable out of the box without a separate client.

> **Developer documentation** — for a full map of how the system is threaded together (actors, per-connection tasks, the fan-out hub, the request lifecycle) plus reference catalogs for the wire protocol, channels, and data model, see [`docs/`](docs/), starting with [`docs/architecture.md`](docs/architecture.md).

## Architecture

**Actor-per-domain.** Each stateful domain — authentication, users, rooms, messaging — runs as its own task. Callers interact through a handle (mpsc request in, oneshot reply back). This keeps domain logic on a single task, removes the need for locks around shared state, and gives each domain a clear seam for tests. Real-time delivery uses a separate in-memory fan-out hub; a periodic reaper ages out old data. See [`docs/architecture.md`](docs/architecture.md) for the full picture.

**Library + binary split.** Production code lives in the `relay` library; the binary is a thin entry point that loads config and wires everything together. Integration tests build the real `app()` against a real Postgres database and exercise the WebSocket protocol end-to-end — no mocks of the database or auth path.

**Cancellation as a first-class concern.** Each server pass runs under a `CancellationToken` that every actor holds and every connection child-clones, so one cancel tears the whole tree down cooperatively. A supervisor loop in `main` owns that token: SIGTERM/Ctrl-C or an admin restart/shutdown command cancels the current pass, and the supervisor then re-initializes or exits. A dedicated shutdown test guards against regressions.

**Schema migrations via sqlx.** All schema is defined in versioned migration files. A bootstrap path ensures the default admin exists on startup from environment config, so a fresh deployment is never left without an entry point.

## Design choices

**Minimal client-event vocabulary.** The protocol intentionally collapses many backend outcomes into a small fixed set of `ServerEvent`s (`Success`, `Failed`, `NoChange`, `NoAuth`, …). A legitimate client can't act on richer detail. Exposing distinct events for "wrong password" vs "malformed JSON" mainly helps map backend state. Detail is intended to live in server-side logs (see Roadmap), not on the wire.

**Flat admin tier with a protected default admin.** Admins are peers — any admin can edit, delete, promote, or demote any user. The single exception is the default admin, who can't be edited, deleted, demoted, or password-updated through the app; their credentials are managed via environment config and applied on startup. The role is break-glass: if every other admin is compromised, the maintainer logs in with the bootstrap credentials and prunes. Requiring default-admin authority for routine admin-on-admin moves would push that account into daily use and erode its purpose.

**Config-on-disk is authoritative for the bind address and default admin.** These are deliberately not changeable from inside the running app. The config file is loaded into the environment once at process start, and the listen socket is bound once and held across in-app restarts, so an admin `RestartServer` re-initializes live state without re-reading the config or moving the port. Changing the bind address or the default admin's username/password means editing the config on disk and performing a **true** process restart (a real stop/start, or `ShutdownServer` then relaunch) — see [`docs/architecture.md`](docs/architecture.md) §9.

**Just-in-time authorization.** Admin status is re-checked at the moment of each privileged action, not cached for the session. A demoted admin loses authority on their next command, on the same socket. The cost is one extra database read per privileged call; the gain is no class of stale-session privilege bugs.

**Idempotency via database constraints where they can express it.** Promote uses `INSERT ... ON CONFLICT DO NOTHING` and branches on `rows_affected()` — 1 means promoted, 0 means already an admin, both are successful outcomes. Demote can't express both "non-admin target" and "default-admin target" through `rows_affected()` alone, so it does a pre-flight read; the asymmetry is structural, not stylistic.

## Security recommendations

The default admin exists to make in-app compromise recoverable: if every other admin account is taken over, the maintainer logs in with the bootstrap credentials and uses normal in-app actions (demote, delete, password reset) to evict the attacker. This only works if the default admin is preserved for that role — used to bootstrap the first non-default admin at deployment, then left dormant until something has gone wrong.

The consequence is that the in-app surface is not where the most consequential attacks live. The default admin's credentials are loaded from environment config, and the authority of every other admin is recorded in the database. An attacker who reaches either of those substrates bypasses the app's authorization model entirely. For deployments, that means:

- Treat the environment config (`.env` or whatever feeds it) as the highest-value asset. Restrict who can read it, audit changes, and rotate the default-admin password on a deliberate schedule.
- Treat the Postgres instance as the second highest-value asset. Restrict network access, give the application a dedicated role without superuser privileges, and audit any direct-database write paths.
- Use the default admin to seed the first non-default admin at deployment and then stop. Day-to-day administration should happen through non-default admin accounts.

## Schema

Schema is defined in versioned `sqlx` migrations and spans the user, room, and messaging domains:

- **Users & auth** — `users` (UUIDv7-keyed profiles), `credentials` (Argon2id hash, 1:1, `ON DELETE CASCADE`), `admins` (grants, with a partial unique index allowing one default admin), `last_active`.
- **Rooms** — `rooms` (visibility flags), `memberships` (multi-owner, plus each member's read watermark), `room_invites`, `room_join_requests`.
- **Messages** — `messages` (sender preserved by id or username snapshot), `message_attachments` + `message_attachment_chunks` (chunked bytes, verified on completion), `message_reactions`.

Deleting a user cascades to credentials, admin grant, and memberships but preserves their messages via a username snapshot; deleting a room cascades to everything beneath it. For the full table catalog, keys, and invariants, see [`docs/reference/data-model.md`](docs/reference/data-model.md).

## What's implemented

- WebSocket lifecycle, tagged JSON protocol, graceful shutdown
- Per-IP rate limiting via `tower_governor` (configurable steady-state and burst), plus per-session message and chunk limits
- DB-backed user accounts with Argon2id password verification
- User CRUD: create (open-signup or admin-gated), look up, edit profile, delete, self password update, admin password reset
- Admin model: bootstrap default admin, promote, demote, default-admin protection
- Admin moderation: list every room (including private, non-discoverable), and read any room's message history regardless of membership
- Rooms: visibility (public/discoverable), multi-owner membership, rename, public join/leave, request-to-join with approve/reject (and requester self-cancel), invite with accept/decline, owner/admin removal of a member (kick)
- Messaging: send, paginated history (newest-first keyset), reactions
- Attachments: resumable chunked upload/download with size + SHA-256 verification, plus a server-side content-type policy (magic-byte sniff that corrects mislabeled types and rejects unsupported ones)
- Real-time fan-out: live message delivery to connected room members, with dynamic and cross-session subscription (and live unsubscribe when a member is kicked), and a lossy-with-resync delivery model
- Read state: per-room read watermark, mark-read, and unread counts
- Time-based reaper: ages out old messages, empty rooms, stale invites/requests, and abandoned uploads
- Admin server lifecycle: in-app restart and shutdown, via a supervisor loop that re-initializes a fresh server pass without dropping the listen port
- Structured logging and tracing across the server (`tracing`), with a dedicated audit target for security-relevant events
- Bundled default web client: a Svelte + TypeScript frontend embedded in the binary at compile time (`rust-embed`), overridable with `FRONTEND_DIR`
- Integration tests against real Postgres via `sqlx::test`

## Roadmap

- **TLS enforcement** — harden the release configuration so plain-text listeners can't be misconfigured.
- **Admin moderation depth** — extend admin read-through (it covers message history today) to attachment downloads, and consider live subscription so admins can watch a room in real time, not just on reload.
- **First-party client polish** — the bundled Svelte client now covers auth, rooms, messaging, attachments, reactions, admin tooling, and theming; remaining work is hardening it against the full [client contract](docs/client-contract.md) (notification, offline catch-up edge cases) and broadening test coverage.

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
