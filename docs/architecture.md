# Architecture — how Relay is threaded together

> **What this is.** A conceptual map of how the system works and what talks to what
> — read it to build a mental model. For exact catalogs (every task, every channel,
> every wire message, every table) see [`reference/`](reference/). For "how do I add
> X" recipes, see the how-to guides (to be written).

Relay is a single-process, async-Rust WebSocket chat server. One Tokio runtime
hosts everything: a **supervisor loop** that owns the process lifecycle, a small set
of long-lived **domain actors**, a pair of tasks per **connected client**, an
in-memory **fan-out hub**, and a periodic **reaper**. Postgres is the only external
dependency and the single source of truth.

This document maps those pieces and the channels between them. Read it top to
bottom once; after that the [reference catalogs](reference/) are where you look up
specifics.

---

## 1. The big picture

```
                                  ┌─────────────────────────────────────────┐
   WebSocket clients              │            relay process (1 Tokio rt)   │
        │                         │                                         │
        │   WS frames             │   ┌── domain actors (1 task each) ──┐   │
        ▼                         │   │  auth   user   room   message   │   │
  ┌───────────┐   per-conn tasks  │   └───┬──────┬──────┬───────┬───────┘   │
  │  axum /ws │──► receiver ──────────────┴──────┴──────┴───────┘           │
  │  upgrade  │       │           │       (mpsc request → oneshot reply)    │
  └───────────┘       ▼           │                  │        │             │
        ▲          sender ◄────────── user_tx        │        │ publish     │
        │             ▲           │                  ▼        ▼             │
        └─── WS frames┘           │   ┌── Hub (shared registry) ──┐         │
                  ▲   live events │   │  rooms: broadcast senders  │        │
                  └───────────────────│  sessions: presence map    │        │
                                  │   └────────────────────────────┘        │
                                  │                                         │
                                  │   reaper (interval task) ──► Postgres   │
                                  │   every actor / task ──────► Postgres   │
                                  └─────────────────────────────────────────┘
```

The diagram shows one server *pass*; the **supervisor loop** in `main` wraps the
whole box and can tear a pass down and stand a fresh one up (§3, §9). The kinds of
long-lived concurrency, plus disposable per-job tasks:

| Kind | Count | Owns | Talks to |
|---|---|---|---|
| **Supervisor loop** (`main`) | 1 (process) | the process lifecycle; the persistent listener + control channel | `ControlSignal`s from the OS-signal task and `ServerControl` |
| **Domain actor** (`auth`, `user`, `room`, `message`) | 1 each | an mpsc inbox; domain logic | callers via handle; Postgres; `message`/`room` also the Hub |
| **Session tasks** (`receiver` + `sender`) | 2 per connection | the socket halves; per-conn channels | actor handles; the Hub; Postgres |
| **Hub** | 1 | room broadcast senders + session presence | message/room actors (write); sender tasks (read) |
| **Reaper** | 1 | a ticking interval | Postgres |
| **Attachment up/download** | per job | one upload's chunk stream / one download | Postgres (via semaphores) |

The rest of this document explains each and the channels between them.

---

## 2. Why actors

Each *stateful domain* runs as **one task that owns its data and serves requests
serially over an mpsc channel**. Callers never touch the domain's state directly;
they hold a cloneable **handle** and send a request, getting the answer back on a
one-shot channel:

```
caller ──(Request { …args, tx: oneshot::Sender<Response> })──► actor inbox (mpsc)
caller ◄─────────────────── Response ──────────────────────── actor (oneshot tx)
```

Concretely, every actor module follows the same shape (see `src/message.rs` for the
cleanest example):

- a `Request` enum — one variant per operation, each carrying its args **and** a
  `oneshot::Sender<Response>`;
- a `Response` enum — the possible outcomes;
- a `Handle` struct wrapping `mpsc::Sender<Request>`, with one `async fn` per
  operation that sends a request and awaits the reply;
- a `spawn()` that starts the task and returns the `Handle`.

Why this shape:

- **No locks.** Domain state is reachable only from its one task, so there's no
  shared mutable state to guard. Serialization comes from the single consumer.
- **A clean seam.** Tests construct the real actor and drive it through its handle.
- **Backpressure for free.** The inbox is a bounded mpsc; a flood of requests
  blocks senders rather than growing unboundedly.

The actors are: **auth** (verify credentials → `user_id`), **user** (accounts,
credentials, admin authority), **room** (rooms, membership, invites, join
requests), **message** (send, history, reactions, read state — and it publishes
live events). The exact request/response vocabularies are in
[`reference/actors-and-channels.md`](reference/actors-and-channels.md).

Two things deliberately are **not** actors:

- The **Hub** (§5) is a shared registry of channels, not owned state to serialize.
  Publishing a fan-out event is a non-blocking, thread-safe send — putting it
  behind a serial task would only add latency. So it's an `Arc<Mutex<…>>` touched
  directly by whoever needs it.
- The **reaper** (§7) owns nothing but a timer; it just runs DELETEs on a tick.

---

## 3. Startup & the supervisor loop (`main.rs` → `app.rs`)

`main` does **process-once** setup, then runs a **supervisor loop** that can stand
the whole server up, tear it down, and stand a fresh one back up.

Process-once (outside the loop):

1. `dotenvy::dotenv()` — load `.env` into the environment. **This happens exactly
   once**; the loop never reloads it (see §9 for what that means for config edits).
2. Init `tracing` (also once — a second init would panic).
3. Create the **control channel** (`mpsc<ControlSignal>`): the seam by which the
   running app, or the OS-signal task, asks the supervisor to **Restart** or
   **Shutdown**. SIGTERM/Ctrl-C map to `Shutdown`.
4. **Bind the listen socket once** and keep it for the whole process, so a restart
   never drops the port — in-flight connections queue in the backlog rather than
   being refused.

Each pass of the loop is one `serve_once(...)`:

5. Re-read `Config` (`src/config.rs`, via `envy`); connect a fresh `PgPool`; run
   `sqlx` migrations; `ensure_admin(...)` (re-applies the default admin from config).
6. Create a **per-run `CancellationToken`** — distinct from the process. Cancelling
   it stops *this pass* (server, actors, sessions); the supervisor never sees it.
7. Spawn the **auth** and **reaper**, then `app(...)` spawns **user/room/message**
   actors, creates the **Hub** and chunk **semaphores**, and builds `AppState`.
8. `axum::serve(...)` over a `try_clone()` of the persistent socket, with graceful
   shutdown bound to the per-run token.
9. Wait (in a `select!`) for a `ControlSignal`. On one: cancel the per-run token,
   await axum's graceful drain, and **drop every per-run resource** as `serve_once`
   returns. **Restart** loops back to step 5 with a clean slate; **Shutdown** breaks
   the loop and the process exits.

`AppState` (in `src/app.rs`) is the dependency bundle handed to every request: the
actor handles, the `PgPool`, the `Hub`, the chunk semaphores, config, the per-run
cancellation token, and a `ServerControl` (the sending half of the control channel,
so an admin command can drive the lifecycle — §9). Axum clones it into each `/ws`
upgrade.

Routes: `/` and `/script.js` (a minimal browser test client), `/health`,
`/favicon.ico`, and `/ws` (the WebSocket upgrade). A per-IP rate-limit layer
(`tower_governor`) wraps the router (§9).

---

## 4. A connection — the two session tasks (`src/server.rs`)

When a client upgrades `/ws`, `handle_socket` splits the socket into a **sink** and
a **stream** and spawns **two tasks**:

```
                        ┌──────────────── receiver task ───────────────┐
   WS in ──► stream ───►│ prelude (auth / signup), then per-frame loop:│
                        │   text  → process_message → actor handles    │
                        │   binary→ process_binary → upload actor      │
                        │ registers session in Hub; owns sub_tx        │
                        └───────┬───────────────────────────┬──────────┘
                                │ user_tx (ServerEvent)     │ sub_tx (Subscription)
                                ▼                           ▼
                        ┌──────────────── sender task ─────────────────┐
   WS out ◄── sink ◄────│ select! over:                                │
                        │   user_rx     → write RPC replies/downloads  │
                        │   sub_rx      → add/remove room live streams │
                        │   StreamMap   → write live fan-out events    │
                        └──────────────────────────────────────────────┘
```

The split is deliberate: **receiver = inbound** (socket → actors), **sender =
outbound** (everything → socket). Keeping fan-out on the sender side means a slow
inbound command (a DB round-trip inside `process_message`) never stalls live
delivery, and vice versa.

Per-connection channels (the seams between these two tasks):

| Channel | Type | Direction | Carries |
|---|---|---|---|
| `user_tx` / `user_rx` | `mpsc<ServerEvent>` (cap 100) | receiver → sender | RPC replies, attachment-download frames, close |
| `sub_tx` / `sub_rx` | `mpsc<Subscription>` (cap 32) | receiver → sender | "subscribe/unsubscribe room X" control |
| child `CancellationToken` | — | either → both | tear this connection down |

The **prelude** runs before the command loop: the first frame must be `Auth`
(verified by the auth actor → `user_id`) or, when open signups are enabled, a
`NewUser`. Anything else closes the socket. After auth the receiver task
**registers the session in the Hub** (so it can be reached cross-session, §5),
subscribes to the rooms the user already belongs to, then loops.

`process_message` (text frames) is the command dispatcher: it parses a
`ClientCommand`, calls the relevant actor handle, and forwards the outcome to
`user_tx` as a `ServerEvent`. `process_binary` (binary frames) routes attachment
chunks (§6).

---

## 5. Real-time fan-out — the Hub (`src/hub.rs`)

A message posted by one user must reach every *other* connected member of the room.
That is the Hub's job. It is a shared, cloneable registry holding two maps:

```
Hub
├── rooms:    room_id  → broadcast::Sender<Arc<ServerEvent>>   (live event bus per room)
└── sessions: user_id  → [ { session_id, sub_tx } … ]          (presence: who's online)
```

**Publish path** (a new message reaches subscribers):

```
message actor: commit message
      │
      ├─ hub.publish(room_id, NewMessage{…})
      │       └─ broadcast::Sender.send(Arc<event>)  ── to every subscribed Receiver
      │
      ▼ (each subscribed session)
   sender task StreamMap yields the event ──► WS text frame
```

- `rooms` entries are **lazy** (created on first subscribe) and **self-pruning** (a
  publish whose `send` finds no receivers removes the entry). Keyed on the
  **immutable `room_id`**, never the renameable name.
- The payload is `Arc<ServerEvent>` so a busy room clones a refcount per subscriber,
  not the whole event.
- It is **best-effort / lossy by design.** If a session falls behind its room's
  ring buffer it gets `Lagged(n)`, which the sender task turns into a `Resync` hint
  — the client re-fetches that room from history. Durable correctness lives in the
  DB + the read watermark (§8), so dropping a live event is always recoverable.

**Subscription is dynamic.** A session's set of subscribed rooms changes as it
joins and leaves. The receiver task (which processes those commands) signals the
sender task over `sub_tx`:

- on connect → subscribe to all current rooms;
- on `JoinRoom` / `AcceptInvite` success → subscribe (self);
- on `LeaveRoom` success → unsubscribe.

**Cross-session subscription.** When an owner *approves* someone else's join
request, the new member isn't the caller — so the room actor reaches that user's
sessions through the `sessions` presence map and pushes a subscribe to each
(`hub.subscribe_user_to_room`). Sessions register on connect and deregister via a
`SessionGuard` (RAII) when the task ends. Offline users simply have no entry and
subscribe on their next connect.

> **Consequence for clients:** the socket is a *mixed stream*. Command replies and
> pushed events (`NewMessage`, `Resync`) interleave, and a sender even receives the
> echo of its own message. Clients must dedup by `message_id` and tolerate push
> events arriving at any time. (Tests model this with a `next_reply` helper that
> skips push events when awaiting a specific reply.)

---

## 6. Attachments — chunked binary (`src/attachment.rs`)

Attachments never travel as JSON. A message *declares* its attachments in
`SendMessage` (creating `message_attachments` rows in an incomplete state); the
bytes follow as **binary WebSocket frames**:

```
[ attachment_id : 16B ][ seq : u32 big-endian : 4B ][ payload … ]
```

**Upload.** `process_binary` routes each chunk frame to a **per-upload actor**
(spawned on the first chunk, disposable). The actor persists chunks idempotently
(`ON CONFLICT DO NOTHING` on `(attachment_id, seq)`), so a stalled upload *resumes*
by re-sending only the missing seqs. When every chunk is present it streams them in
order through a SHA-256 hasher, checks size + digest, then CAS-flips `is_complete`
and emits `AttachmentComplete`. A bounded **write semaphore** caps concurrent chunk
writes so an upload burst can't drain the pool. Idle uploads time out; the reaper
sweeps abandoned partials.

**Download.** `DownloadAttachment` spawns a disposable streaming task: a
membership check, then chunks streamed back as binary frames (same framing) in seq
order, ended by `AttachmentEnd`. A bounded **read semaphore** mirrors the write
side.

Full framing and event details: [`reference/wire-protocol.md`](reference/wire-protocol.md).

---

## 7. The reaper — time-based cleanup (`src/reaper.rs`)

A single task ticks on an interval (`reap_interval_secs`) and runs one transaction
of DELETEs, keeping storage bounded without manual intervention:

- messages older than `retention_days`;
- rooms that are empty *and* older than `retention_days`;
- stale `room_invites` / `room_join_requests`;
- **incomplete** attachments past a short grace (~24h) — abandoned uploads, whose
  parent message is still young so the message rule won't catch them.

Completed attachments and reactions need no rule of their own: they `CASCADE` off
`messages`. Users are never auto-aged.

---

## 8. Persistence model & invariants (`migrations/`, `src/model.rs`)

Postgres is the single source of truth; everything above is a cache or a delivery
optimization. Schema lives in versioned `sqlx` migrations. The table catalog and
relationships are in [`reference/data-model.md`](reference/data-model.md); the
load-bearing ideas:

- **`uuidv7` primary keys.** Time-ordered by byte value, so `message_id` doubles as
  a chronological cursor. This is what makes two features cheap:
  - **keyset pagination** of history (`WHERE (timestamp, message_id) < cursor
    ORDER BY … DESC`), and
  - the **read watermark** — `memberships.last_read_message_id`, a bare cursor (no
    FK, so reaping a message never resets it). "Unread" is just `message_id >
    last_read_message_id`.
- **Sender preservation.** A message keeps `sender_id` *or* a
  `sender_username_snapshot` (backfilled when a sender is deleted), so history
  survives account deletion. The raw `user_id` never goes over the wire; clients
  see display usernames.
- **Existence-hiding by construction.** Membership-gated reads/writes resolve the
  room and the caller's membership in one query; a non-member and a missing room
  both yield "no row" → the same generic failure. Nothing leaks whether a private
  room exists.

---

## 9. Cross-cutting concerns

**Lifecycle: cancellation, restart, shutdown.** Two levels of teardown:

- A **per-run `CancellationToken`** owns one `serve_once` pass. Every actor holds it;
  each session takes a *child* of it. Cancelling it tears down that whole tree — the
  basis of both restart and shutdown. `axum`'s graceful shutdown drains in-flight
  requests during the cancel.
- The **supervisor loop** in `main` owns the process. A `ControlSignal` — from the
  OS-signal task on SIGTERM/Ctrl-C, or from an admin `RestartServer` /
  `ShutdownServer` command via `ServerControl` in `AppState` — tells it to cancel the
  current pass and then re-initialize (**Restart**) or exit (**Shutdown**). The
  `shutdown` and `server_control` tests guard these paths.

**What a hot restart does and doesn't pick up.** A `RestartServer` re-reads
`Config::from_env()` and re-creates the pool and actors — but **`.env` is loaded into
the environment only once, at process start** (step 1 of §3), and the listen socket
is **bound once and held** across passes. So, *by design*:

- **The bind address** can't change on a hot restart — the port stays pinned to the
  held socket.
- **The default admin username and password** (re-applied each pass by
  `ensure_admin`) are read from config, which a hot restart sees *unchanged* because
  the file on disk isn't reloaded.

Changing either — or any config-file value — therefore requires editing the config
on disk and performing a **true process restart** (stop and start, or
`ShutdownServer` then relaunch), which is what re-runs `dotenv` and re-binds the
socket. A hot restart is for re-initializing live state (reconnecting the pool,
resetting actors), not for re-reading the config file.

**Rate limiting, in layers.** Per-IP HTTP limiting via `tower_governor` on the
router; inside an authed session, a per-message limiter (10/s, burst 20) and a
higher chunk limiter (200/s, burst 400) since chunks are high-volume.

**Backpressure.** Bounded mpsc everywhere (actor inboxes, `user_tx`, `sub_tx`); the
chunk semaphores bound concurrent attachment DB work; fan-out is lossy with
watermark-based recovery rather than unbounded buffering. No path grows without a
bound.

**Authorization philosophy.** *Just-in-time* — admin/membership rights are
re-checked at each privileged action, never cached on the session, so a revoked
right takes effect on the next command. Client-facing errors are intentionally
generic; detail belongs in server logs.

---

## 10. Where things live

| File | Role |
|---|---|
| `src/main.rs` | Process entry: process-once setup, the persistent listener, and the supervisor loop (`serve_once`: per-pass init, serve, restart/shutdown). |
| `src/app.rs` | `AppState` + `app()`: spawn user/room/message actors, Hub, semaphores, router. |
| `src/config.rs` | Env-driven `Config`. |
| `src/control.rs` | `ControlSignal` + `ServerControl`: the restart/shutdown seam between the app and the supervisor. |
| `src/handler.rs` | Axum handlers incl. the `/ws` upgrade (pins frame caps to max chunk size). |
| `src/server.rs` | Per-connection session: receiver/sender tasks, prelude, dispatch, subscription wiring. |
| `src/hub.rs` | Fan-out broadcast registry + session presence. |
| `src/auth.rs` | Auth actor (Argon2id credential verification). |
| `src/user.rs` | User actor: accounts, credentials, admin authority; `ensure_admin`. |
| `src/room.rs` | Room actor: rooms, membership, invites, join requests; cross-session subscribe. |
| `src/message.rs` | Message actor: send (+ publish), history, reactions, read state. |
| `src/attachment.rs` | Per-upload actor + download task; chunk framing constants. |
| `src/reaper.rs` | Interval cleanup task. |
| `src/model.rs` | Wire types (`ClientCommand`/`ServerEvent`) and DB row types. |
| `migrations/` | Versioned schema. |
| `tests/` | Integration tests: real `app()` against real Postgres, end-to-end over WS. |

---

## See also

- [`reference/actors-and-channels.md`](reference/actors-and-channels.md) — every
  task, the channel it owns, and its message vocabulary.
- [`reference/wire-protocol.md`](reference/wire-protocol.md) — framing and the full
  `ClientCommand` / `ServerEvent` catalog.
- [`reference/data-model.md`](reference/data-model.md) — tables, keys, and
  relationships.
