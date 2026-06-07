use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// #### Application Datatypes ####
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ClientCommand {
    Auth {
        username: String,
        password: Password,
    },
    Echo {
        string: String,
    },
    // Post a message to a room. The sender is the authenticated user (taken
    // server-side), never supplied by the client. `attachments` declares zero or
    // more files whose bytes follow as binary chunk frames, each keyed by an
    // attachment_id the server hands back in MessageCreated.
    SendMessage {
        room_name: String,
        content: String,
        #[serde(default)]
        attachments: Vec<NewMessageAttachment>,
    },
    // Download one attachment's bytes. The server streams the chunks back as
    // binary frames (same framing as upload) in seq order, terminated by
    // AttachmentEnd. Read access requires membership of the attachment's room;
    // a forbidden, missing, or still-incomplete attachment all yield the same
    // generic error so nothing leaks.
    DownloadAttachment {
        attachment_id: Uuid,
    },
    // Ask the server the largest chunk payload it will accept per upload frame.
    // Answered with MaxChunkSize. A client should query this once after auth and
    // size its chunks at or below the reported value; a larger chunk is dropped by
    // the transport (the connection is closed), not rejected with an error.
    GetMaxChunkSize,
    // React to a message with an emoji. The reactor is the authenticated session
    // user, taken server-side -- only the target message and emoji come from the
    // client. Idempotent: re-adding an emoji the caller already reacted with is a
    // no-op success. Only members of the message's room may react; a forbidden or
    // missing message yields the same generic failure, so neither leaks.
    AddReaction {
        message_id: Uuid,
        emoji: String,
    },
    // Remove the caller's own reaction. Idempotent: removing one that isn't there
    // is a no-op success. Same membership gate and generic failure as AddReaction.
    RemoveReaction {
        message_id: Uuid,
        emoji: String,
    },
    // Permanently remove a message (an "unsend"). Allowed only for the message's
    // own sender or a server admin; the check is server-side per request. On
    // success the message -- with any attachments, their chunks, and reactions --
    // is deleted and a MessageRemoved is fanned out to the room so every client
    // drops it live. A forbidden or unknown message yields the same generic
    // failure, so neither the message's existence nor who may delete it leaks.
    DeleteMessage {
        message_id: Uuid,
    },
    // Page through a room's message history, newest first, each message carrying
    // its attachment metadata and reaction summary. Read access requires
    // membership of the room; a non-member and an unknown room yield the same
    // generic failure, so neither leaks. `before` is a keyset cursor: when set,
    // only messages older than that message_id are returned, so a client loads the
    // latest page first, then walks backwards by passing the oldest id it has seen.
    // `limit` is clamped server-side to a sane maximum.
    GetMessages {
        room_name: String,
        #[serde(default)]
        before: Option<Uuid>,
        #[serde(default)]
        limit: Option<u32>,
    },
    // Advance the caller's read position in a room to `up_to_message_id`. Forward
    // only: a cursor at or behind the current watermark is an idempotent no-op.
    // Membership-gated with the same generic failure as GetMessages.
    MarkRead {
        room_name: String,
        up_to_message_id: Uuid,
    },
    // Unread message counts for every room the caller belongs to, for room-list
    // badges. Answered with UnreadSummary.
    GetUnreadSummary,
    NewUser {
        username: String,
        password: Password,
        first_name: Option<String>,
        last_name: Option<String>,
        alias: Option<String>,
    },
    GetUserByUsername {
        username: String,
    },
    // Page through the user directory, ordered by username. Open to any
    // authenticated user (e.g. to find someone to invite without knowing their
    // exact handle). Answered with `Users`.
    //
    // - `starts_with`: optional case-insensitive username prefix filter. Empty or
    //   whitespace-only is treated as no filter.
    // - `after`: keyset cursor -- the `username` of the last entry from the
    //   previous page; the next page continues with usernames ordered after it.
    //   Omit for the first page.
    // - `limit`: page size, clamped server-side (defaults applied, hard cap).
    GetUsers {
        #[serde(default)]
        starts_with: Option<String>,
        #[serde(default)]
        after: Option<String>,
        #[serde(default)]
        limit: Option<u32>,
    },
    EditUser {
        target_username: String,
        username: Option<String>,
        first_name: Option<String>,
        last_name: Option<String>,
        alias: Option<String>,
    },
    Promote {
        target_username: String,
    },
    Demote {
        target_username: String,
    },
    DeleteUser {
        target_username: String,
    },
    UpdatePassword {
        current_password: Password,
        new_password: Password,
    },
    ResetPassword {
        target_username: String,
        new_password: Password,
    },
    NewRoom {
        room_name: String,
        is_public: Option<bool>,
        is_discoverable: Option<bool>,
    },
    AddRoomOwner {
        room_name: String,
        new_owner_username: String,
    },
    SetRoomName {
        current_name: String,
        new_name: String,
    },
    GetRoomMembership {
        room_name: String,
    },
    GetRoom {
        room_name: String,
    },
    JoinRoom {
        room_name: String,
    },
    LeaveRoom {
        room_name: String,
    },
    // Owner/admin removes another user's membership from a room.
    RemoveRoomMember {
        room_name: String,
        member_username: String,
    },
    // The caller's own outstanding join requests.
    GetMyJoinRequests,
    // The caller withdraws their own pending join request.
    CancelJoinRequest {
        room_name: String,
    },
    // Pending join requests for rooms the caller owns (or any room, if admin).
    GetIncomingJoinRequests,
    ApproveJoinRequest {
        room_name: String,
        requester_username: String,
    },
    RejectJoinRequest {
        room_name: String,
        requester_username: String,
    },
    // Owner/admin invites a user into a room.
    InviteToRoom {
        room_name: String,
        invitee_username: String,
    },
    // Invitations addressed to the caller.
    GetMyInvites,
    AcceptInvite {
        room_name: String,
    },
    DeclineInvite {
        room_name: String,
    },
    // Whether the server has open (unauthenticated) signups enabled. Valid in the
    // prelude (before auth) so a client can decide whether to offer registration,
    // and also after auth. Reply: SignupStatus.
    GetSignupStatus,
    // Returns all rooms that are publicly listed (is_discoverable = true or is_public = true),
    // so a client can show a room browser without knowing room names in advance.
    ListDiscoverableRooms,
    // Admin only: returns every room (including private, non-discoverable ones) so
    // an admin can browse and moderate any room. Non-admins are rejected.
    ListAllRooms,
    // Restart the entire server process: drain every connection and actor, then
    // re-initialize from a fresh config. Admin only. The issuing connection is torn
    // down with the rest, so the client should expect its socket to close shortly
    // after the Success ack.
    RestartServer,
    // Shut the entire server process down. Admin only. The socket closes as the
    // process exits.
    ShutdownServer,
    Close,
    Error {
        error: String,
    },
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ServerEvent {
    AuthOk {
        is_admin: bool,
    },
    NoAuth,
    Echo {
        string: String,
    },
    // A SendMessage was persisted. `message_id` identifies the new message and
    // `attachment_ids` are returned in declaration order so the client can key each
    // file's chunk stream. `message` is the full canonical form (timestamp, sender
    // display name, attachment summaries) -- the same shape that arrives live as
    // NewMessage, so the sender can render its own message identically and dedup
    // the live echo by message_id rather than synthesizing it from the ack.
    MessageCreated {
        message_id: Uuid,
        attachment_ids: Vec<Uuid>,
        message: MessageHistoryItem,
    },
    AttachmentComplete {
        attachment_id: Uuid,
    },
    // A fully-uploaded attachment failed the content-type policy (unsupported
    // format, or declared type doesn't match the actual bytes) and was not
    // published. Attachment-specific so the client can attribute it to the file.
    AttachmentRejected {
        attachment_id: Uuid,
        reason: String,
    },
    // Reply to GetMaxChunkSize: the largest chunk payload (file bytes, excluding
    // the frame header) the server will accept in one upload frame.
    MaxChunkSize {
        bytes: usize,
    },
    // Reply to GetSignupStatus: whether unauthenticated account creation is open.
    SignupStatus {
        open_signups: bool,
    },
    // One chunk of a download, streamed back in seq order. The sender task emits
    // this as a BINARY frame -- [attachment_id 16B][seq u32 BE 4B][payload] -- and
    // never JSON-serializes it, so the bytes don't pay base64/array inflation on
    // the hot path.
    AttachmentChunk {
        attachment_id: Uuid,
        seq: i32,
        data: Vec<u8>,
    },
    // Terminates a successful download: every chunk for attachment_id has been
    // sent. The client uses it to know the stream finished cleanly.
    AttachmentEnd {
        attachment_id: Uuid,
    },
    // Reply to GetMessages: one page of a room's history, newest first. `messages`
    // is empty when the room has none in range (or the cursor is past the start).
    // The sender is identified by display username only -- raw user_ids never go
    // over the wire -- and falls back to the snapshot taken when a since-deleted
    // sender's account was removed.
    MessageHistory {
        room_name: String,
        messages: Vec<MessageHistoryItem>,
    },
    // Reply to GetUnreadSummary: one entry per room the caller is in, including
    // rooms with zero unread, so a client can render the full room list.
    UnreadSummary {
        rooms: Vec<RoomUnread>,
    },
    // A message was posted to a room the caller is subscribed to, pushed live. Same
    // payload shape as a MessageHistory item so live and backlog render through one
    // path. The caller may receive this for their own message too (they dedup by
    // message_id against the MessageCreated ack).
    NewMessage {
        room_name: String,
        message: MessageHistoryItem,
    },
    // A message was removed server-side and should be dropped from the room view.
    // Emitted for unsend/admin deletion and when a rejected upload leaves a
    // message with no attachments, so it would otherwise linger as a bare filename
    // or caption for everyone in the room.
    MessageRemoved {
        room_name: String,
        message_id: Uuid,
    },
    // The session fell behind a room's live buffer and dropped events. Not an
    // error -- a hint to re-fetch that room from history (GetMessages); the read
    // watermark keeps the unread count correct meanwhile.
    Resync {
        room_name: String,
    },
    Close {
        reason: String,
    },
    UserCreated,
    Error {
        error: String,
    },
    RateLimit {
        error: String,
    },
    UserInfo {
        first_name: Option<String>,
        last_name: Option<String>,
        alias: Option<String>,
        username: String,
        created_at: DateTime<Utc>,
    },
    RoomMembers {
        members: Vec<RoomMember>,
    },
    RoomInfo {
        room_name: String,
        is_public: bool,
        is_discoverable: bool,
    },
    MyJoinRequests {
        rooms: Vec<String>,
    },
    IncomingJoinRequests {
        requests: Vec<JoinRequestInfo>,
    },
    MyInvites {
        rooms: Vec<String>,
    },
    DiscoverableRooms {
        rooms: Vec<DiscoverableRoom>,
    },
    // Reply to ListAllRooms (admin): every room with a live member count.
    AllRooms {
        rooms: Vec<DiscoverableRoom>,
    },
    // Reply to GetUsers: one page of the user directory, ordered by username.
    // `has_more` is true when another page exists after this one (continue by
    // passing the last entry's `username` as the next `after` cursor).
    Users {
        users: Vec<UserDirectoryEntry>,
        has_more: bool,
    },
    NoChange,
    NoUserExists,
    NoRoomExists,
    RoomCreated,
    JoinRequested,
    Success,
    Failed,
}

macro_rules! json_display {
    ($t:ty) => {
        impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match serde_json::to_string(self) {
                    Ok(s) => f.write_str(&s),
                    Err(_) => Err(std::fmt::Error),
                }
            }
        }
    };
}

json_display!(ServerEvent);
json_display!(ClientCommand);

// #### Database ####

pub struct NewUser {
    pub username: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub alias: Option<String>,
}

pub struct EditUser {
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub alias: Option<String>,
}

#[derive(sqlx::FromRow, Debug, Serialize, Deserialize, PartialEq)]
pub struct User {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub alias: Option<String>,
    pub username: String,
    pub user_id: Uuid,
    pub created_at: DateTime<Utc>,
}

// A user as shown to clients -- identical to User but without the internal
// user_id, which never goes over the wire.
#[derive(sqlx::FromRow, Debug, Serialize, Deserialize, PartialEq)]
pub struct PublicUser {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub alias: Option<String>,
    pub username: String,
    pub created_at: DateTime<Utc>,
}

// One entry in a GetUsers directory page: the public profile fields plus an
// optional admin flag. `is_admin` is populated only when the *requesting* user is
// an admin (the admin pane's "identify other admins"); for a regular caller it is
// None and omitted from the wire, so admin status isn't exposed to non-admins.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct UserDirectoryEntry {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub alias: Option<String>,
    pub username: String,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_admin: Option<bool>,
}

// A room member as shown to clients: the public profile fields plus whether the
// user is an owner of the room (memberships.is_owner), so the client can render
// ownership and gate owner-only actions.
#[derive(sqlx::FromRow, Debug, Serialize, Deserialize, PartialEq)]
pub struct RoomMember {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub alias: Option<String>,
    pub username: String,
    pub created_at: DateTime<Utc>,
    pub is_owner: bool,
}

#[derive(sqlx::FromRow)]
pub struct Admin {
    pub user_id: Uuid,
    pub granted_by: Option<Uuid>,
    pub granted_at: DateTime<Utc>,
    pub is_default: bool,
}

#[derive(sqlx::FromRow)]
pub struct LastActive {
    pub user_id: Uuid,
    pub last_active: DateTime<Utc>,
}

#[derive(sqlx::Type)]
#[sqlx(transparent)]
pub struct PasswordHash(String);

impl std::fmt::Debug for PasswordHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PasswordHash(***)")
    }
}

#[derive(sqlx::Type)]
#[sqlx(transparent)]
#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Password(pub String);

impl std::fmt::Debug for Password {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Password(***)")
    }
}

pub struct NewCredential {
    pub password: Password,
}

#[derive(sqlx::FromRow)]
pub struct Credential {
    pub user_id: Uuid,
    pub password_hash: PasswordHash,
    pub password_last_set: DateTime<Utc>,
}

pub struct NewRoom {
    pub room_name: String,
    pub owner_id: Uuid,
    pub is_public: bool,
    pub is_discoverable: bool,
}

#[derive(sqlx::FromRow, Debug, Serialize, Deserialize, PartialEq)]
pub struct DiscoverableRoom {
    pub room_name: String,
    pub is_public: bool,
    pub member_count: i64,
}

#[derive(sqlx::FromRow)]
pub struct Room {
    pub room_name: String,
    pub room_id: Uuid,
    pub is_public: bool,
    pub is_discoverable: bool,
}

// Joined View -- Constructed at the query level. Ownership is now multi-valued
// (memberships.is_owner), so a room carries a set of owners rather than one.
pub struct RoomWithOwners {
    pub room: Room,
    pub owners: Vec<User>,
}

pub struct NewMessage {
    pub room_id: Uuid,
    pub user_id: Uuid, // Foreign Key for Creation
    pub content: String,
}

#[derive(sqlx::FromRow)]
pub struct Message {
    pub message_id: Uuid,
    pub room_id: Uuid,
    pub sender_id: Option<Uuid>,
    pub sender_username_snapshot: Option<String>,
    pub content: String,
    pub timestamp: DateTime<Utc>,
}

// Joined View -- Constructed at the query level
pub struct MessageWithUser {
    pub message_id: Uuid,
    pub room_id: Room,
    pub sender: User,
    pub content: String,
    pub timestamp: DateTime<Utc>,
}

// One message as returned by GetMessages: the row plus its attachments and a
// per-emoji reaction summary. `sender_username` is the display name (current
// username, or the snapshot if the sender was deleted) -- the raw sender user_id
// is never serialized.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MessageHistoryItem {
    pub message_id: Uuid,
    pub sender_username: String,
    pub content: String,
    pub timestamp: DateTime<Utc>,
    pub attachments: Vec<AttachmentSummary>,
    pub reactions: Vec<ReactionSummary>,
}

// An attachment as shown in history: enough for a client to decide whether and how
// to download it. `is_complete` is false while an upload is still in flight; the
// bytes are fetched separately via DownloadAttachment using attachment_id.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct AttachmentSummary {
    pub attachment_id: Uuid,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub is_complete: bool,
}

// One room's unread tally for the caller: messages newer than their read
// watermark. Zero for a fully-read room (such rooms are still listed).
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RoomUnread {
    pub room_name: String,
    pub unread: i64,
}

// One emoji's reaction tally on a message: how many users reacted with it and
// whether the requesting caller is one of them (so a client can render its own
// reactions as toggled without a second round trip).
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ReactionSummary {
    pub emoji: String,
    pub count: i64,
    pub reacted_by_me: bool,
}

// Client-sourced metadata for one attachment, supplied in the same command as the
// message. Enough to create the attachment row up front (is_complete = false) and
// validate the chunk stream against it; the bytes follow as chunks keyed by the
// attachment_id the server returns. The parent message_id is assigned server-side
// when the message is inserted, so it isn't a field here. content_sha256 is the
// declared digest of the whole file, re-computed by streaming the chunks in seq
// order on completion.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct NewMessageAttachment {
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub chunk_count: i32,
    pub content_sha256: Vec<u8>,
}

// Persisted attachment. Bytes live in AttachmentChunk rows, not here. is_complete
// flips true once every seq is present and the streamed hash matches
// content_sha256. Auth derives from the parent message: only its sender may
// upload chunks; any room member may read them.
#[derive(sqlx::FromRow)]
pub struct MessageAttachment {
    pub attachment_id: Uuid,
    pub message_id: Uuid,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub chunk_count: i32,
    pub content_sha256: Vec<u8>,
    pub is_complete: bool,
    pub created_at: DateTime<Utc>,
}

// One chunk on its way in. seq is the client-supplied order index; (attachment_id,
// seq) is unique, so a re-sent chunk is an idempotent upsert and a stalled upload
// resumes by filling only the missing seqs.
pub struct NewAttachmentChunk {
    pub attachment_id: Uuid,
    pub seq: i32,
    pub data: Vec<u8>,
}

#[derive(sqlx::FromRow)]
pub struct AttachmentChunk {
    pub attachment_id: Uuid,
    pub seq: i32,
    pub data: Vec<u8>,
}

// Client-sourced: the authenticated caller reacts to a message with an emoji. The
// reactor is the session user, supplied server-side, so only the target message
// and emoji come from the client.
pub struct NewReaction {
    pub message_id: Uuid,
    pub emoji: String,
}

// Persisted reaction: a (message, user, emoji) triple. One of each emoji per user
// per message; adding an existing one is a no-op.
#[derive(sqlx::FromRow)]
pub struct Reaction {
    pub message_id: Uuid,
    pub user_id: Uuid,
    pub emoji: String,
    pub created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
pub struct Membership {
    pub room_id: Uuid,
    pub user_id: Uuid,
    pub is_owner: bool,
    pub joined_at: DateTime<Utc>,
}

// Client-sourced: an owner invites `invitee_username` into `room_name`. Clients
// don't know PKs, so the room actor resolves both names to IDs. The inviter is
// the authenticated caller, supplied server-side rather than carried here.
pub struct NewRoomInvite {
    pub room_name: String,
    pub invitee_username: String,
}

// Pending invitation: an owner invited this user into the room. Row exists only
// while the invite is outstanding. Persisted shape -- keyed by the IDs the actor
// resolved from NewRoomInvite.
#[derive(sqlx::FromRow)]
pub struct RoomInvite {
    pub room_id: Uuid,
    pub user_id: Uuid,
    pub invited_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

// Client-sourced: the authenticated caller requests to join `room_name`. The
// requesting user is the session user, so only the room name comes from the
// client; the actor resolves it to a room_id.
pub struct NewRoomJoinRequest {
    pub room_name: String,
}

// Pending join request: this user asked to join the room. Row exists only while
// the request is outstanding. Persisted shape -- keyed by resolved IDs.
#[derive(sqlx::FromRow)]
pub struct RoomJoinRequest {
    pub room_id: Uuid,
    pub user_id: Uuid,
    pub created_at: DateTime<Utc>,
}

// Client-facing view of a pending join request: the room and the user who asked.
// Names rather than IDs, so it can both come from a query and go out on the wire.
#[derive(sqlx::FromRow, Serialize, Deserialize, PartialEq, Debug)]
pub struct JoinRequestInfo {
    pub room_name: String,
    pub username: String,
}
