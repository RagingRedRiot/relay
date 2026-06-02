# Reference — actors, tasks, and channels

> A lookup catalog of every long-lived task, the channel it owns, and who talks to
> it. For the narrative of how these fit together, see
> [`../architecture.md`](../architecture.md).

## Long-lived tasks

A "pass" below means one iteration of the supervisor loop (one `serve_once`);
these tasks are dropped and re-spawned on a restart, not on every connection.

| Task | Spawned by | Lifetime | Inbox / owned channel | Talks to |
|---|---|---|---|---|
| **auth actor** | `auth::spawn` (serve_once) | pass | `mpsc<AuthRequest>` | callers (handle); Postgres |
| **user actor** | `user::spawn` (app) | pass | `mpsc<UserRequest>` | callers; Postgres |
| **room actor** | `room::spawn` (app) | pass | `mpsc<RoomRequest>` | callers; Postgres; **Hub** |
| **message actor** | `message::spawn` (app) | pass | `mpsc<MessageRequest>` | callers; Postgres; **Hub** |
| **reaper** | `reaper::spawn` (serve_once) | pass | a `tokio::time::interval` | Postgres |
| **OS-signal task** | `main` | process | — | the control channel (sends `Shutdown`) |
| **receiver task** | `handle_socket` | one connection | reads WS stream | actor handles; Hub; Postgres; `sub_tx`, `user_tx` |
| **sender task** | `handle_socket` | one connection | `mpsc<ServerEvent>` + `mpsc<Subscription>` + a `StreamMap` of broadcast receivers | writes WS sink |
| **upload actor** | `attachment::spawn` (per upload) | one upload | `mpsc<Chunk>` | Postgres (write semaphore) |
| **download task** | `attachment::download` (per request) | one download | — | Postgres (read semaphore); `user_tx` |

The **supervisor loop** itself runs in `main` (not a spawned task) and owns the
process: the persistent listen socket and the receiving end of the control channel.

The **Hub** (`src/hub.rs`) is **not** a task — it's a shared `Arc<Mutex<…>>`
registry (room broadcast senders + session presence) touched directly by the
message/room actors (write) and the sender tasks (read).

## Process lifecycle (control plane)

| Piece | Type | Who holds it | Purpose |
|---|---|---|---|
| control channel | `mpsc<ControlSignal>` (cap 8) | sender: `ServerControl` (in `AppState`) + OS-signal task; receiver: `main`'s supervisor loop | ask the supervisor to restart or shut down |
| `ServerControl` | handle over the sender | `AppState` → `Handles` → `process_message` | `restart()` / `shutdown()`, called by the admin `RestartServer` / `ShutdownServer` commands |
| per-run `CancellationToken` | — | `serve_once`; cloned into actors; child-cloned per session | cancel one pass (server + actors + sessions) |

A restart cancels the per-run token, drains, drops every per-pass resource, and
re-runs `serve_once`. It does **not** reload the config file or rebind the port —
see [`../architecture.md`](../architecture.md) §9.

## The actor handle pattern

Each domain actor exposes a cloneable `Handle` over a bounded `mpsc` (cap ~100).
Each operation is `async fn` on the handle that sends a `Request` carrying its args
plus a `oneshot::Sender<Response>`, then awaits the reply:

```
Handle::op(args)
  └─ mpsc.send(Request::Op { args, tx: oneshot })  ─►  actor loop (one consumer)
                                                          handle_request(...) → Response
  ◄─ oneshot.recv() ◄──────────────────────────────────  actor sends on tx
```

`spawn(shutdown, pool, …)` starts the loop (`select!` over the inbox and the
`CancellationToken`) and returns the `Handle`. The handles live in `AppState`.

| Actor | Handle | Representative operations |
|---|---|---|
| auth | `AuthHandle` | `authenticate(username, password) → user_id` |
| user | `UserHandle` | `new_user`, `edit_user`, `promote`, `demote`, `delete_user`, `update_password`, `reset_password`, `is_admin`, `get_user_by_username` |
| room | `RoomHandle` | `new_room`, `get_room`, `get_room_members`, `set_room_name`, `add_room_owner`, `join_room`, `leave_room`, invites (`invite`/`accept`/`decline`/`get_my_invites`), requests (`approve`/`reject`/`get_my`/`get_incoming`) |
| message | `MessageHandle` | `send_message`, `get_messages`, `mark_read`, `get_unread_summary`, `add_reaction`, `remove_reaction` |

> `auth` is spawned in `main` (before `app()`) so the WebSocket prelude can
> authenticate without the rest of `AppState`. `room` and `message` receive a `Hub`
> clone at spawn — `message` to **publish** `NewMessage`, `room` to **subscribe** a
> just-approved user's sessions cross-session.

## Per-connection channels (`handle_socket`)

| Channel | Type / cap | Direction | Purpose |
|---|---|---|---|
| `user_tx` → `user_rx` | `mpsc<ServerEvent>` / 100 | receiver → sender | RPC replies, download chunk frames, close |
| `sub_tx` → `sub_rx` | `mpsc<Subscription>` / 32 | receiver → sender | add/remove a room's live stream |
| child `CancellationToken` | — | shared | tear down this connection |

`Subscription` (`src/hub.rs`): `Add { room_id, room_name, rx }` |
`Remove { room_id }`. The sender task merges each `Add`'s broadcast `rx` into a
`StreamMap` keyed by `room_id`; dropping the task drops the receivers (RAII
teardown).

## The Hub registry (`src/hub.rs`)

| Map | Key → Value | Written by | Read by |
|---|---|---|---|
| `rooms` | `room_id → broadcast::Sender<Arc<ServerEvent>>` | `subscribe` (lazy create), `publish` (self-prune) | sender tasks (via `subscribe`) |
| `sessions` | `user_id → [ { session_id, sub_tx } ]` | `register_session` / `SessionGuard` drop | `subscribe_user_to_room` |

Methods: `subscribe(room_id) → Receiver`, `publish(room_id, event)`,
`register_session(user_id, sub_tx) → SessionGuard`,
`subscribe_user_to_room(user_id, room_id, room_name)`. Each is sync and
non-blocking; the lock is never held across `.await`.

## Concurrency bounds (where the limits are)

| Bound | Value | Where |
|---|---|---|
| actor inbox | ~100 | `*::spawn` |
| `user_tx` | 100 | `handle_socket` |
| `sub_tx` | 32 | `handle_socket` |
| control channel | 8 | `main` |
| room broadcast ring | 128 | `hub::ROOM_BROADCAST_CAPACITY` |
| concurrent chunk writes | 16 | `attachment::MAX_CONCURRENT_CHUNK_WRITES` |
| concurrent chunk reads | 16 | `attachment::MAX_CONCURRENT_CHUNK_READS` |
| per-session text rate | 10/s, burst 20 | `server.rs` |
| per-session chunk rate | 200/s, burst 400 | `server.rs` |
| per-IP HTTP rate | config (`rate_limit_*`) | `tower_governor` layer |
| history page | default 50, max 100 | `message::{DEFAULT,MAX}_HISTORY_LIMIT` |
