# Reference — data model

> A lookup catalog of the Postgres schema and its invariants. Source of truth:
> `migrations/` (versioned, run by `sqlx` on startup). Row types mirror these in
> `src/model.rs`. For *why*, see [`../architecture.md`](../architecture.md) §8.

Postgres is the single source of truth; the in-memory hub and read state are
caches/optimizations layered on top.

## Tables

| Table | Key | Purpose | Notable columns / behavior |
|---|---|---|---|
| `users` | `user_id` (uuidv7) | accounts | unique `LOWER(username)`; profile fields nullable, non-empty |
| `credentials` | `user_id` → users | Argon2id hashes | `ON DELETE CASCADE` |
| `admins` | `user_id` → users | admin authority | `is_default` flag; partial unique index `WHERE is_default` (one default admin) |
| `last_active` | `user_id` → users | presence timestamp | |
| `rooms` | `room_id` (uuidv7) | rooms | unique `LOWER(room_name)`; `is_public`, `is_discoverable` (both default false) |
| `memberships` | (`room_id`, `user_id`) | who's in a room | `is_owner`; `joined_at`; **`last_read_message_id`** (read watermark) |
| `room_invites` | (`room_id`, `user_id`) | pending invites | `invited_by` (audit, `SET NULL` on inviter delete) |
| `room_join_requests` | (`room_id`, `user_id`) | pending requests | |
| `messages` | `message_id` (uuidv7) | posts | `sender_id` (`SET NULL` on delete) **or** `sender_username_snapshot`; `content`; `timestamp` |
| `message_attachments` | `attachment_id` (uuidv7) | attachment metadata | `message_id` (CASCADE); `is_complete`; `content_sha256`; `chunk_count`, `size_bytes` |
| `message_attachment_chunks` | (`attachment_id`, `seq`) | attachment bytes | `data BYTEA`; CASCADE off attachment; idempotent upsert by PK |
| `message_reactions` | (`message_id`, `user_id`, `emoji`) | reactions | CASCADE off message |

## Relationships & cascade

```
users ──1:1── credentials
      ──1:1── admins
      ──1:N── memberships ──N:1── rooms
      ──1:N── room_invites / room_join_requests ──N:1── rooms
rooms ──1:N── messages ──1:N── message_attachments ──1:N── message_attachment_chunks
                       └─1:N── message_reactions
```

- Deleting a **room** cascades to its memberships, invites, requests, messages, and
  (transitively) attachments, chunks, reactions.
- Deleting a **user** cascades credentials/admin/memberships/reactions, but
  **preserves their messages**: `messages.sender_id` is set `NULL` and
  `sender_username_snapshot` carries the display name forward.

## Load-bearing invariants

**uuidv7 primary keys are chronological cursors.** uuidv7 sorts by byte value in
creation-time order, so `message_id` is a monotonic clock. This underpins:

- **History pagination** — keyset on `(timestamp, message_id)`, newest-first, using
  the `messages_room_timestamp` index. The `before` cursor is a `message_id`.
- **The read watermark** — `memberships.last_read_message_id` is a *bare* uuidv7
  value, **deliberately not a foreign key**. "Unread" is the comparison
  `message_id > last_read_message_id`, which stays correct even after the
  referenced message is reaped. `NULL` = nothing read.
  - Set caught-up on join (newest message at join time; via
    `ORDER BY message_id DESC LIMIT 1` — Postgres has no `max(uuid)` aggregate).
  - Advanced forward-only by `MarkRead`, and by `SendMessage` for the sender's own
    posts.

**Sender preservation.** The `CHECK (sender_id IS NOT NULL OR
sender_username_snapshot IS NOT NULL)` guarantees every message attributes to
*someone*; history shows `COALESCE(current username, snapshot)`. Raw `user_id`
never leaves the server.

**Name normalization.** Usernames and room names match case-insensitively and
whitespace-trimmed: lookups use `LOWER(trim_ws($1))` against the unique
`LOWER(...)` indexes (`trim_ws` is a custom immutable SQL function).

**Attachment completeness.** `message_attachments.is_complete` flips true only after
every chunk is present, the streamed SHA-256 + total size match the declaration, and
the bytes pass a **content-type policy** (a magic-byte sniff: the stored
`content_type` is set to the *detected* type for recognized formats, or the file is
rejected) — a monotonic `false → true` CAS that also writes the corrected
`content_type`. So `content_type` is **server-verified**, not merely client-declared.
Rejected and incomplete rows are swept by the reaper after a ~24h grace; the
partial-index `message_attachments_incomplete (created_at) WHERE NOT is_complete`
serves that sweep.

## Bootstrap & cleanup

- **Bootstrap:** `ensure_admin` (run at startup from config) guarantees a default
  admin row exists, so a fresh database always has an entry point.
- **Cleanup:** the reaper deletes aged messages, empty aged rooms, stale
  invites/requests, and abandoned incomplete uploads. See
  [`../architecture.md`](../architecture.md) §7.
