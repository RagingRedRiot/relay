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
client ──► Auth (or NewUser if open signups)          first frame only (the "prelude")
server ──► AuthOk            → enter command loop
       ──► NoAuth + Close    → rejected
       ──► UserCreated+Close → signup-only handshake done
... command loop: client sends ClientCommands, server sends replies + live pushes ...
either ──► Close             → teardown
```

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
| `SendMessage` | `room_id`, `content`, `attachments[]` | `MessageCreated` |
| `GetMessages` | `room_name`, `before?`, `limit?` | `MessageHistory` |
| `MarkRead` | `room_name`, `up_to_message_id` | `Success` |
| `GetUnreadSummary` | — | `UnreadSummary` |
| `AddReaction` | `message_id`, `emoji` | `Success` |
| `RemoveReaction` | `message_id`, `emoji` | `Success` |
| `DownloadAttachment` | `attachment_id` | binary chunk frames → `AttachmentEnd` |
| `GetMaxChunkSize` | — | `MaxChunkSize` |
| `Close` | — | `Close` |
| `Error` | `error` | (client→server error; ignored) |

`attachments[]` items are `NewMessageAttachment { filename, content_type,
size_bytes, chunk_count, content_sha256 }`. After `MessageCreated`, stream each
file's bytes as binary chunk frames keyed by the returned `attachment_ids` (in
declaration order).

### Users / admin
| Command | Fields |
|---|---|
| `NewUser` | `username`, `password`, `first_name?`, `last_name?`, `alias?` |
| `GetUserByUsername` | `username` → `UserInfo` |
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
| `InviteToRoom` | `room_name`, `invitee_username` |
| `GetMyInvites` | — → `MyInvites` |
| `AcceptInvite` / `DeclineInvite` | `room_name` |
| `GetMyJoinRequests` | — → `MyJoinRequests` |
| `GetIncomingJoinRequests` | — → `IncomingJoinRequests` |
| `ApproveJoinRequest` / `RejectJoinRequest` | `room_name`, `requester_username` |

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
| `AuthOk` / `NoAuth` | — |
| `UserCreated` | — |
| `Echo` | `string` |
| `MessageCreated` | `message_id`, `attachment_ids[]`, `message` (`MessageHistoryItem`) |
| `MessageHistory` | `room_name`, `messages[]` (`MessageHistoryItem`, newest-first) |
| `UnreadSummary` | `rooms[]` (`RoomUnread { room_name, unread }`) |
| `MaxChunkSize` | `bytes` |
| `UserInfo` | `first_name?`, `last_name?`, `alias?`, `username`, `created_at` |
| `RoomInfo` | `room_name`, `is_public`, `is_discoverable` |
| `RoomMembers` | `members[]` (`PublicUser`, no `user_id`) |
| `MyInvites` / `MyJoinRequests` | `rooms[]` (names) |
| `IncomingJoinRequests` | `requests[]` (`JoinRequestInfo`) |

### Live push (after auth, unsolicited)
| Event | Fields | Meaning |
|---|---|---|
| `NewMessage` | `room_name`, `message` | a message was posted to a subscribed room |
| `Resync` | `room_name` | fell behind the live buffer — re-fetch this room via `GetMessages` |
| `AttachmentComplete` | `attachment_id` | an upload finished and verified |
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

> Legacy: `ServerEvent::Message { user_id, room_id, value }` still exists in
> `model.rs` but is never emitted — superseded by `NewMessage`. Slated for removal.
