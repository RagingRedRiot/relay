use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// #### Application Datatypes ####
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ClientCommand {
    Auth {
        username: String,
        password: String,
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
    Close,
    Error {
        error: String,
    },
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ServerEvent {
    AuthOk,
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
    Error {
        error: String,
    },
    RateLimit {
        error: String,
    },
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
    pub first_name: String,
    pub last_name: String,
    pub alias: String,
}

#[derive(sqlx::FromRow)]
pub struct User {
    pub first_name: String,
    pub last_name: String,
    pub alias: String,
    pub username: String,
    user_id: Uuid,
    created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
pub struct LastActive {
    user_id: Uuid,
    last_active: DateTime<Utc>,
}

#[derive(sqlx::Type)]
#[sqlx(transparent)]
pub struct PasswordHash(String);

impl std::fmt::Debug for PasswordHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PasswordHash(***)")
    }
}

pub struct NewCredential {
    username: String,
    password_hash: PasswordHash,
}

#[derive(sqlx::FromRow)]
pub struct Credential {
    username: String,
    password_hash: PasswordHash,
    password_last_set: DateTime<Utc>,
}

pub struct NewRoom {
    pub room_name: String,
    pub owner_id: Uuid,
}

#[derive(sqlx::FromRow)]
pub struct Room {
    pub room_name: String,
    pub room_id: Uuid,
    pub owner_id: Uuid,
}

// Joined View -- Constructed at the query level
pub struct RoomWithOwner {
    pub room_id: Uuid,
    pub owner: User,
}

pub struct NewMessage {
    room_id: Uuid,
    user_id: Uuid, // Foreign Key for Creation
    content: String,
}

#[derive(sqlx::FromRow)]
pub struct Message {
    message_id: Uuid,
    room_id: Uuid,
    sender_id: Uuid,
    content: String,
    timestamp: DateTime<Utc>,
}

// Joined View -- Constructed at the query level
pub struct MessageWithUser {
    message_id: Uuid,
    room_id: Room,
    sender: User,
    content: String,
    timestamp: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
pub struct Membership {
    room_id: Uuid,
    user_id: Uuid,
}
