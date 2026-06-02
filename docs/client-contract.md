# Client contract — what a custom client must honor

> Conventions and obligations for anyone building a client against Relay. The
> message catalog in [`reference/wire-protocol.md`](reference/wire-protocol.md) tells
> you *what* the frames are; this document tells you *how a correct client must
> behave*. Where a rule has a "why", it links to
> [`architecture.md`](architecture.md).

These are requirements, not suggestions — the server is built around them, and a
client that ignores them will misbehave in ways the server won't report.

## At a glance — the checklist

- [ ] First frame is `Auth` (or `NewUser` when open signups); wait for `AuthOk`
      before anything else.
- [ ] Treat the socket as a **mixed stream**: demultiplex replies from pushed
      events — never assume "the next frame is my reply."
- [ ] **Dedup by `message_id`.** You will receive the echo of your own messages and
      overlap between history and live delivery.
- [ ] Query `GetMaxChunkSize` once after auth and never send a chunk larger than it.
- [ ] Honor `Resync` by re-fetching that room from history.
- [ ] Call `MarkRead` as the user reads — delivery is not read.
- [ ] On reconnect, re-fetch unread/history to catch up; live delivery is lossy.
- [ ] Don't branch on error *detail* — outcomes are intentionally generic.
- [ ] Pace yourself to the rate limits and handle `RateLimit` / `Close`.

## 1. Connecting and authenticating

The first frame on a fresh socket is the **prelude**, and only two commands are
valid there:

- `Auth { username, password }` → `AuthOk` (enter the session) or `NoAuth` followed
  by `Close` (rejected — the socket ends).
- `NewUser { … }`, **only if the server has open signups enabled**. This is a
  one-shot account-creation handshake: the server replies `UserCreated` then
  `Close`. To then *use* the account you must **open a new connection and `Auth`**.

Send nothing else until you've received `AuthOk`. Any other first frame closes the
socket.

## 2. The socket is a mixed stream

After `AuthOk` there is **no strict request/response framing**. Command replies and
unsolicited **pushed events** share the one socket and interleave in unspecified
order. Your read loop must dispatch by event type, not by position. In particular:

- `NewMessage`, `Resync` (live room events) can arrive at any moment — including
  *between* a command you sent and its reply, and *during* an attachment download's
  binary frames.
- A command's reply is identified by its **type/content**, not by being "next."

A client that does blocking "send command, read one frame as the reply" will
eventually read a push event where it expected a reply. See
[architecture.md §5](architecture.md#5-real-time-fan-out--the-hub) for why.

## 3. Dedup by `message_id`

You **will** see the same message more than once; this is by design, and dedup by
`message_id` is mandatory:

- When you post, you get a `MessageCreated` ack **and** a live `NewMessage` echo of
  your own message (the sender is subscribed to its own room). The ack's `message`
  field is the full canonical form — render from it, and treat the echo as a no-op
  for a `message_id` you already have.
- After reconnecting, a history fetch will overlap with messages you also receive
  live.

Use `message_id` as the identity key for optimistic-UI reconciliation and for
de-duplicating history-vs-live.

## 4. Attachments

Bytes never travel as JSON. The flow:

1. **Declare** files in `SendMessage.attachments[]` (`filename`, `content_type`,
   `size_bytes`, `chunk_count`, `content_sha256` — the SHA-256 of the *whole* file).
2. Read `MessageCreated.attachment_ids` — one id per declared file, in order.
3. **Upload** each file as binary frames, seq `0..chunk_count-1`:
   ```
   [ attachment_id : 16B ][ seq : u32 big-endian : 4B ][ payload … ]
   ```
4. On success you get `AttachmentComplete { attachment_id }`.

Obligations:

- **Respect the chunk size limit.** Query `GetMaxChunkSize` once after auth; keep
  every chunk payload at or below the reported value. An oversized frame is dropped
  by the transport — **the connection closes; you do not get an error event.**
- **Get the declaration right.** The server verifies the streamed bytes against the
  declared `size_bytes` and `content_sha256`. A mismatch leaves the attachment
  permanently incomplete and yields a generic failure — re-sending can't fix a bad
  declaration (chunks are keep-first). Recompute and re-declare on a new message.
- **Keep chunks flowing.** An upload that idles is abandoned server-side after a
  short timeout. Resumption is supported — re-sending a seq is idempotent, so a
  stalled upload continues by sending only the missing seqs — but you must drive it.
- **Downloads interleave.** `DownloadAttachment` streams binary chunk frames in seq
  order, ended by `AttachmentEnd`. Reassemble by seq, and tolerate text push events
  (`NewMessage`/`Resync`) arriving between chunk frames.

## 5. Read state is your job

The server does **not** mark messages read when it delivers them (delivery ≠ read).
It only advances a user's watermark for their *own* sends. So:

- Call `MarkRead { room_name, up_to_message_id }` as the user actually reads a room,
  passing the newest `message_id` they've seen. It's forward-only and idempotent —
  safe to call often, with stale ids, out of order.
- Use `GetUnreadSummary` for room-list unread badges.
- New members start *caught up* (they don't inherit the whole backlog as unread).

## 6. Resync and reconnection — live delivery is lossy

Live fan-out is best-effort. Two situations require you to fall back to history;
the durable data is always correct, so nothing is truly lost:

- **`Resync { room_name }`** means your session fell behind that room's live buffer
  and dropped events. Re-fetch the room with `GetMessages`. It is **not** an error.
- **After any disconnect**, you missed whatever was delivered while offline. On
  reconnect and re-`Auth`, the server re-subscribes you to your rooms automatically;
  you should pull `GetUnreadSummary` and `GetMessages` to catch up.

## 7. History pagination

`GetMessages { room_name, before?, limit? }` returns up to `limit` messages
**newest-first** (`limit` defaults to 50, capped at 100). To walk backwards, pass
the oldest `message_id` you've received as `before`. An empty page means you've
reached the start of available history (older messages may have been reaped).

## 8. Don't depend on error detail

Outcomes are intentionally coarse. A non-member and a non-existent private room both
return `Failed`; bad auth doesn't distinguish "wrong password" from "no such user".
This is deliberate (existence-hiding — [architecture.md §8–§9](architecture.md#8-persistence-model--invariants-migrations-srcmodelrs)).
Build your UX around the generic outcomes (`Success`, `Failed`, `NoChange`,
`NoUserExists`, `NoRoomExists`, `JoinRequested`, `RateLimit`, `Error`, `Close`);
don't try to infer hidden state from them.

## 9. Identity and naming conventions

- **Never send your own user id.** The server always acts as the authenticated
  session user; commands that operate "as you" (post, react, mark read) take no
  caller id. Messages identify senders by **display username** only.
- **Names are case-insensitive and whitespace-trimmed.** You may send a room or
  username in any casing/with surrounding whitespace; the server normalizes for
  matching and returns the canonical form. Match on the canonical value the server
  sends back.

## 10. Rate limits and shutdown

- **Text commands:** ~10/s (burst 20). Exceeding it returns `RateLimit`; repeated
  abuse (a few strikes) ends in `Close`.
- **Chunk frames:** higher (~200/s, burst 400), since uploads are high-volume.
- **Per-IP** limiting also applies at the HTTP/upgrade layer.
- A server `Close { reason }` means the connection is ending — stop sending. You may
  also send `Close` to end gracefully.

Pace your sends, back off on `RateLimit`, and reconnect (with fresh `Auth`) after a
`Close` if the user is still active.

**Admin lifecycle commands.** `RestartServer` and `ShutdownServer` (admin only)
return `Success` and then the socket closes as the server drains — treat that close
like any other. After a restart the server comes back up on the same port, so an
admin client can simply reconnect; after a shutdown it won't.
