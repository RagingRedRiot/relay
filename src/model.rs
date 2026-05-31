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
    // TODO: redesign once rooms exist. Client should target a room
    // (room_id) and send content; the sender is the authenticated user,
    // not something the client supplies.
    Message {
        user_id: Uuid,
        room_id: Uuid,
        value: String,
    },
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
    // The caller's own outstanding join requests.
    GetMyJoinRequests,
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
    Close,
    Error {
        error: String,
    },
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ServerEvent {
    AuthOk,
    NoAuth,
    Echo {
        string: String,
    },
    // TODO: redesign once rooms and user details exist. Should carry
    // room context and sender details (display name, etc.) rather than
    // a raw internal user_id. Raw user_id stays server-side.
    Message {
        user_id: Uuid,
        room_id: Uuid,
        value: String,
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
        members: Vec<PublicUser>,
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
