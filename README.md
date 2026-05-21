# Relay

A self-hosted real-time chat platform written in async Rust.

> **Status:** Early development. POC stage — interfaces will change. Not production ready.

## Overview

Relay is a WebSocket chat server built on Axum and Tokio. It handles connection lifecycle, authentication, per-IP rate limiting, and graceful shutdown over a tagged binary protocol, backed by Postgres for user state.

The project is being built deliberately, layer by layer — connection handling first, then authentication and protocol shape, then persistence, then transport hardening and credential storage. The git history reflects that progression.

## What's working today

- WebSocket upgrade and per-connection lifecycle handling
- Tagged protocol enums for client/server messages
- Auth handshake scaffolding in place — protocol and actor wired up; real credential verification is on the roadmap
- Per-IP rate limiting via `tower_governor` (configurable steady-state and burst)
- Graceful shutdown — coordinated cancellation across spawned tasks via `CancellationToken`
- Postgres user schema (sqlx, UUIDv7 identifiers)
- Library + binary split with dependency injection — integration tests exercise a real server against a swappable auth backend

## Architecture notes

**Auth as an actor.** Authentication runs as its own task. Callers interact through an `AuthHandle` (mpsc in, oneshot back), which decouples request handlers from the auth backend and keeps credential-handling logic on a single task. Tests swap in `auth::testing::spawn_test` with a fixed user list — nothing else has to change.

**Cancellation as a first-class concern.** Every long-lived task takes a `CancellationToken`. On `SIGTERM` or Ctrl-C the root token cancels and tasks shut down cooperatively. The test suite includes a dedicated shutdown test to catch regressions.

**Lib + bin split for testability.** Production code lives in the `relay` library; the binary is a thin entry point. Integration tests build a real `app()` against a real Postgres instance and exercise the WebSocket interface end-to-end. A `testing` cargo feature gates the test-only auth backend so it can't leak into release builds.

## Roadmap

Near-term focus areas:

- **TLS enforcement** — harden the release configuration so plain-text listeners can't be misconfigured.
- **Real credential verification** — replace the placeholder check with Argon2id verification, layered on top of a matured database flow.
- **Persistent message history** — durable chat semantics beyond the current scaffolding.

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
cargo test --features testing
```

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
