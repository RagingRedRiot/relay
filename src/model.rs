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
        target: String,
        username: Option<String>,
        first_name: Option<String>,
        last_name: Option<String>,
        alias: Option<String>,
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
    NoUserExists,
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
    pub room_id: Uuid,
    pub user_id: Uuid, // Foreign Key for Creation
    pub content: String,
}

#[derive(sqlx::FromRow)]
pub struct Message {
    pub message_id: Uuid,
    pub room_id: Uuid,
    pub sender_id: Uuid,
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
}
