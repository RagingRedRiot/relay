# Reference — WebSocket wire protocol

> A precise catalog of the framing and every message — for looking things up, not
> for learning the system. For *why* the protocol is shaped this way, see
> [`../architecture.md`](../architecture.md) §4–§6. For the **behavioral obligations
> a client must honor** (mixed stream, dedup, attachment flow, read state), see
> [`../client-contract.md`](../client-contract.md). Source of truth:
> `src/model.rs` (`ClientCommand`, `ServerEvent`), `src/server.rs` (dispatch),
> `src/attachment.rs` (chunk framing).

## Framing

The protocol runs over a single WebSocket at `/ws`. Two frame kinds:

- **Text frames** carry JSON — a `ClientCommand` inbound, a `ServerEvent` outbound.
- **Binary frames** carry attachment chunk payloads, both directions, with a fixed
  20-byte header:

  ```
  [ attachment_id : 16 bytes ][ seq : u32 big-endian : 4 bytes ][ payload … ]
  ```

JSON enums are **externally tagged** (serde default):

| Rust | JSON |
|---|---|
| unit variant `GetUnreadSummary` | `"GetUnreadSummary"` |
| struct variant `Echo { string }` | `{"Echo":{"string":"hi"}}` |

`Password` is a transparent newtype, serialized as a bare string.

The transport's max frame/message size is pinned to `max_chunk_bytes + 20` (see
`handler.rs`); an oversized chunk is dropped by the transport (connection closed),
not answered with an error. Query the limit with `GetMaxChunkSize`.

## Connection lifecycle

```
client ──► GetSignupStatus (optional, repeatable)     prelude — non-terminal
server ──► SignupStatus { open_signups }              ...prelude keeps waiting
client ──► Auth (or NewUser if open signups)          the terminal prelude frame
server ──► AuthOk            → enter command loop
       ──► NoAuth + Close    → rejected
       ──► UserCreated+Close → signup-only handshake done
... command loop: client sends ClientCommands, server sends replies + live pushes ...
either ──► Close             → teardown
```

In the prelude only `GetSignupStatus` is **non-terminal** (answered, then the server
keeps waiting); it's capped so an unauthenticated socket can't query indefinitely.
`Auth` / `NewUser` end the prelude, and any other frame closes the connection.

After `AuthOk` the socket is a **mixed stream**: command replies and **pushed
events** (`NewMessage`, `Resync`) interleave, and a sender receives the live echo of
its own message. Clients must **dedup by `message_id`** and tolerate pushes at any
time.

## `ClientCommand` (inbound)

### Session / messaging
| Command | Fields | Reply (success → …) |
|---|---|---|
| `Auth` | `username`, `password` | `AuthOk` / `NoAuth`+`Close` (prelude only) |
| `Echo` | `string` | `Echo` |
| `SendMessage` | `room_name`, `content`, `attachments[]` | `MessageCreated` |
| `GetMessages` | `room_name`, `before?`, `limit?` | `MessageHistory` (members **or admins**) |
| `MarkRead` | `room_name`, `up_to_message_id` | `Success` |
| `GetUnreadSummary` | — | `UnreadSummary` |
| `AddReaction` | `message_id`, `emoji` | `Success` |
| `RemoveReaction` | `message_id`, `emoji` | `Success` |
| `DeleteMessage` | `message_id` | `Success` (sender **or admin**); also fans out `MessageRemoved` |
| `DownloadAttachment` | `attachment_id` | binary chunk frames → `AttachmentEnd` |
| `GetMaxChunkSize` | — | `MaxChunkSize` |
| `GetSignupStatus` | — | `SignupStatus` (valid **in the prelude**, pre-auth, and after) |
| `Close` | — | `Close` |
| `Error` | `error` | (client→server error; ignored) |

`attachments[]` items are `NewMessageAttachment { filename, content_type,
size_bytes, chunk_count, content_sha256 }`. After `MessageCreated`, stream each
file's bytes as binary chunk frames keyed by the returned `attachment_ids` (in
declaration order). On completion the server **sniffs the uploaded bytes** and
enforces a content-type policy (see §"Attachment content-type policy" below): the
declared `content_type` is advisory, and a disallowed file is rejected with
`AttachmentRejected` instead of `AttachmentComplete`.

### Users / admin
| Command | Fields |
|---|---|
| `NewUser` | `username`, `password`, `first_name?`, `last_name?`, `alias?` |
| `GetUserByUsername` | `username` → `UserInfo` |
| `GetUsers` | `starts_with?`, `after?`, `limit?` → `Users` (any authenticated user) |
| `EditUser` | `target_username`, `username?`, `first_name?`, `last_name?`, `alias?` |
| `Promote` / `Demote` / `DeleteUser` | `target_username` |
| `UpdatePassword` | `current_password`, `new_password` |
| `ResetPassword` | `target_username`, `new_password` |

### Rooms
| Command | Fields |
|---|---|
| `NewRoom` | `room_name`, `is_public?`, `is_discoverable?` → `RoomCreated` |
| `GetRoom` | `room_name` → `RoomInfo` |
| `GetRoomMembership` | `room_name` → `RoomMembers` |
| `SetRoomName` | `current_name`, `new_name` |
| `AddRoomOwner` | `room_name`, `new_owner_username` |
| `JoinRoom` | `room_name` → `Success` (public) or `JoinRequested` (private+discoverable) |
| `LeaveRoom` | `room_name` |
| `RemoveRoomMember` | `room_name`, `member_username` — owner/admin removes another member (kick) |
| `InviteToRoom` | `room_name`, `invitee_username` |
| `GetMyInvites` | — → `MyInvites` |
| `AcceptInvite` / `DeclineInvite` | `room_name` |
| `GetMyJoinRequests` | — → `MyJoinRequests` |
| `CancelJoinRequest` | `room_name` — withdraw your own pending request |
| `GetIncomingJoinRequests` | — → `IncomingJoinRequests` |
| `ApproveJoinRequest` / `RejectJoinRequest` | `room_name`, `requester_username` |
| `ListDiscoverableRooms` | — → `DiscoverableRooms` (public or discoverable rooms) |
| `ListAllRooms` | — → `AllRooms` (**admin only**; every room, private included) |

Owner/admin authority on a room is re-checked just-in-time. `RemoveRoomMember`
also drops the kicked user's live subscription to that room (via the Hub), so they
stop receiving its messages immediately, not at next reconnect.

### Server lifecycle (admin only)
| Command | Fields | Effect |
|---|---|---|
| `RestartServer` | — | drain and re-initialize the process; `Success` ack, then the socket closes |
| `ShutdownServer` | — | shut the process down; `Success` ack, then the socket closes |

Both require admin (re-checked just-in-time); a non-admin gets the generic `Failed`.
A restart does **not** re-read the config file or move the bind port — see
[`../architecture.md`](../architecture.md) §9.

## `ServerEvent` (outbound)

### Replies & data
| Event | Fields |
|---|---|
| `AuthOk` | `is_admin` (whether the logged-in user is an admin) |
| `NoAuth` | — |
| `UserCreated` | — |
| `Echo` | `string` |
| `MessageCreated` | `message_id`, `attachment_ids[]`, `message` (`MessageHistoryItem`) |
| `MessageHistory` | `room_name`, `messages[]` (`MessageHistoryItem`, newest-first) |
| `UnreadSummary` | `rooms[]` (`RoomUnread { room_name, unread }`) |
| `MaxChunkSize` | `bytes` |
| `SignupStatus` | `open_signups` (whether unauthenticated account creation is enabled) |
| `UserInfo` | `first_name?`, `last_name?`, `alias?`, `username`, `created_at` |
| `RoomInfo` | `room_name`, `is_public`, `is_discoverable` |
| `RoomMembers` | `members[]` (`RoomMember` = `PublicUser` + `is_owner`, no `user_id`) |
| `MyInvites` / `MyJoinRequests` | `rooms[]` (names) |
| `IncomingJoinRequests` | `requests[]` (`JoinRequestInfo`) |
| `DiscoverableRooms` / `AllRooms` | `rooms[]` (`DiscoverableRoom { room_name, is_public, member_count }`) |
| `Users` | `users[]` (`UserDirectoryEntry`), `has_more` (another page follows) |

`UserDirectoryEntry` is `PublicUser` plus an optional `is_admin`, which is present
**only when the requester is an admin** (and omitted otherwise, so admin status
isn't exposed to non-admins). `GetUsers` pages the directory ordered by username:
pass the last entry's `username` as the next `after` cursor, and an optional
`starts_with` to filter by a case-insensitive username prefix. `limit` is clamped
server-side (default 50, max 100). Open to any authenticated user.

### Live push (after auth, unsolicited)
| Event | Fields | Meaning |
|---|---|---|
| `NewMessage` | `room_name`, `message` | a message was posted to a subscribed room |
| `MessageRemoved` | `room_name`, `message_id` | a message was deleted (unsend, admin removal, or a rejected file-only upload) — drop it from the view |
| `Resync` | `room_name` | fell behind the live buffer — re-fetch this room via `GetMessages` |
| `AttachmentComplete` | `attachment_id` | an upload finished, verified, and passed the content-type policy |
| `AttachmentRejected` | `attachment_id`, `reason` | an upload's bytes verified but failed the content-type policy; the file is not published |
| `AttachmentChunk` | binary frame | one chunk of an in-progress download |
| `AttachmentEnd` | `attachment_id` | download stream finished cleanly |

### Generic outcomes
`Success`, `Failed`, `NoChange`, `NoUserExists`, `NoRoomExists`, `RoomCreated`,
`JoinRequested`, `RateLimit { error }`, `Error { error }`, `Close { reason }`.

> The outcome vocabulary is intentionally coarse. A non-member and a missing room
> both yield `Failed`; detail lives in server logs, not on the wire
> (existence-hiding, see [`../architecture.md`](../architecture.md) §8–§9).

### `MessageHistoryItem` (shared by history, `MessageCreated`, `NewMessage`)
```
message_id, sender_username, content, timestamp,
attachments[]: { attachment_id, filename, content_type, size_bytes, is_complete },
reactions[]:   { emoji, count, reacted_by_me }
```

### Attachment content-type policy

Once an upload's bytes pass the size + SHA-256 check, the server sniffs the leading
bytes (chunk 0) and decides the **stored** `content_type` — the client's declared
value is only advisory:

- **Magic-detected types** (`image/png`, `image/jpeg`, `image/gif`, `image/webp`,
  `application/pdf`, `application/zip`): detection wins. The stored `content_type` is
  set to the detected type, **correcting** a mislabeled-but-supported file. A
  detected type outside this set is **rejected**.
- **Magicless text types** (`text/plain`, `text/csv`, `text/markdown`,
  `application/json`, `image/svg+xml`): no signature exists, so the declared type is
  honored — but only if it's in this allowlist and the bytes contain no NUL
  (otherwise it's a binary masquerading as text, and it's rejected).
- Anything else (e.g. `application/octet-stream`, unknown types) is **rejected**.

A rejection emits `AttachmentRejected { attachment_id, reason }` instead of
`AttachmentComplete`, and the upload is **cancelled**: the attachment row and its
chunks are deleted outright (not left for the reaper), and `AttachmentRejected` is
fanned out to the room so live clients remove that dead attachment. If that empties
the parent message of all attachments — the usual case, since a file post's caption
defaults to the filename — the message is deleted too and a `MessageRemoved` is
fanned out to the room, so it doesn't linger as a bare filename for other members.
Source: `src/attachment.rs` (`resolve_content_type`, `cancel_rejected_upload`).

> Legacy: `ServerEvent::Message { user_id, room_id, value }` still exists in
> `model.rs` but is never emitted — superseded by `NewMessage`. Slated for removal.
