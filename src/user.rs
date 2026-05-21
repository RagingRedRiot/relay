use sqlx::{PgConnection, PgPool, Row};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use argon2::{Argon2, PasswordHasher, password_hash::{SaltString, rand_core::OsRng}};
use uuid::Uuid;

use crate::model::{self, Admin, NewCredential, NewUser, User};

pub enum UserRequest {
    NewUserRequest { 
        user: model::NewUser,
        credential: model::NewCredential,
        tx: oneshot::Sender<UserResponse>,
    },
    GetUserInfoRequest {
        user_id: Uuid,
        tx: oneshot::Sender<UserResponse>,
    },
    IsAdminRequest {
        user_id: Uuid,
        tx: oneshot::Sender<UserResponse>,
    }
}

#[derive(Debug)]
pub enum UserResponse {
    UserCreated { user_id: Uuid },
    UserInfo { user_info: User },
    IsAdmin { is_admin: bool },
    NoUserExists,
    Failed,
}

#[derive(Clone)]
pub struct UserHandle {
    sender: mpsc::Sender<UserRequest>,
}

impl UserHandle {
    pub async fn new_user(&self, new_user: model::NewUser, new_credential: model::NewCredential) -> UserResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            // Submit the new User Request to the Actor
            .send(UserRequest::NewUserRequest {
                user: new_user,
                credential: new_credential,
                tx,
            })
            .await
            .is_err() 
        {
            return UserResponse::Failed
        }

        rx.await.unwrap_or(UserResponse::Failed)
    }

    // Returns whether the given user is an admin. Fails closed: any channel
    // or actor error resolves to `false` rather than granting access.
    pub async fn is_admin(&self, user_id: Uuid) -> bool {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(UserRequest::IsAdminRequest { user_id, tx })
            .await
            .is_err()
        {
            return false
        }

        matches!(rx.await, Ok(UserResponse::IsAdmin { is_admin: true }))
    }
    // TODO
    // pub async fn get_user(&self, user_id: Uuid) -> UserResponse {

    // }
}

pub async fn spawn(shutdown: CancellationToken, pool: PgPool) -> UserHandle {
    // USER ACTOR COMMUNICATION CHANNELS
    // rx stays in the spawned actor
    // tx gets returned in UserHandle
    // Cloning user handle allows a new
    // communication channel to the actor
    let (tx, mut rx) = mpsc::channel::<UserRequest>(100);

    // USER ACTOR TASK
    tokio::spawn(async move {
        loop {
            select! {
                req = rx.recv() => {
                    // All handles dropped — no more requests will arrive.
                    let Some(req) = req else { break };
                    let (result, req_tx) = handle_request(req, pool.clone()).await;
                    let _ = req_tx.send(result);
                }
                _ = shutdown.cancelled() => {
                    break
                }
            }
        }
    });

    // USER ACTOR HANDLE
    UserHandle { sender: tx }
}

fn generate_hash(credential: NewCredential) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(credential.password.0.as_bytes(), &salt)?.to_string();
    Ok(hash)
}

async fn handle_request(req: UserRequest, pool: PgPool) -> (UserResponse, oneshot::Sender<UserResponse>) {
    match req {
        UserRequest::NewUserRequest { user, credential, tx } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => { db }
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx)
                }
            };

            let hash = match generate_hash(credential) {
                Ok(hash) => { hash }
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx)
                }
            };

            let user_id = match create_user(&mut db, user, &hash).await {
                Ok(user_id) => { user_id }
                Err(_e) => { 
                    // TODO - Logging
                    return (UserResponse::Failed, tx) }
            };

            match db.commit().await {
                Ok(_) => {
                    return (UserResponse::UserCreated { user_id: user_id }, tx)
                }
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx)
                }
            };
        }

        UserRequest::GetUserInfoRequest { user_id, tx } => {
            let user = match sqlx::query_as::<_, User>(
                "SELECT * FROM users WHERE user_id = $1"
            )
            .bind(user_id)
            .fetch_optional(&pool)
            .await {
                Ok(maybe_user) => { match maybe_user {
                    Some(user) => { user }
                    None => { return (UserResponse::NoUserExists, tx) }
                } 
            }
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx)
                }
            };
            return (UserResponse::UserInfo{ user_info: user }, tx)
        }

        UserRequest::IsAdminRequest { user_id, tx } => {
            match is_admin(&pool, user_id).await {
                Ok(value) => (UserResponse::IsAdmin { is_admin: value }, tx),
                Err(_e) => {
                    // TODO - Logging
                    (UserResponse::Failed, tx)
                }
            }
        }

    }
}

async fn create_user(db: &mut PgConnection, user: NewUser, password_hash: &str) -> Result<Uuid, sqlx::Error> {
    let user_id: Uuid = sqlx::query("INSERT INTO users (username, first_name, last_name, alias) VALUES ($1, $2, $3, $4) RETURNING user_id")
        .bind(user.username)
        .bind(user.first_name)
        .bind(user.last_name)
        .bind(user.alias)
        .fetch_one(&mut *db)
        .await?.try_get("user_id")?;

    sqlx::query("INSERT INTO credentials (user_id, password_hash) VALUES ($1, $2)")
        .bind(user_id)
        .bind(password_hash)
        .execute(&mut *db)
        .await?;

    Ok(user_id)
}

async fn create_admin(db: &mut PgConnection, username: &str, password_hash: &str) -> Result<Uuid, sqlx::Error> {

    let admin = NewUser { 
        username: username.to_owned(), 
        first_name: None, 
        last_name: None, 
        alias: None
    };

    let user_id = create_user(db, admin, password_hash).await?;
    sqlx::query("INSERT INTO admins (user_id, is_default) VALUES ($1, true)")
        .bind(user_id)
        .execute(&mut *db)
        .await?;
    Ok(user_id)
}

async fn update_admin(db: &mut PgConnection, user_id: Uuid, username: &str, password_hash: &str) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE users SET username = $1 WHERE user_id = $2")
    .bind(username)
    .bind(user_id)
    .execute(&mut *db)
    .await?;

    sqlx::query("UPDATE credentials SET password_hash = $1, password_last_set = now() WHERE user_id = $2")
    .bind(password_hash)
    .bind(user_id)
    .execute(&mut *db)
    .await?;

    Ok(())
}

async fn get_admin(db: &mut PgConnection) -> Result<Option<Admin>, sqlx::Error> {
    let row: Option<Admin> = sqlx::query_as("SELECT user_id, granted_by, granted_at, is_default FROM admins WHERE is_default = true")
    .fetch_optional(db)
    .await?;
    Ok(row)
}

async fn is_admin(pool: &PgPool, user_id: Uuid) -> Result<bool, sqlx::Error> {
    let is_admin: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM admins WHERE user_id = $1)"
    )
    .bind(user_id)
    .fetch_one(pool)
    .await?;
    Ok(is_admin)
}

pub async fn ensure_admin(pool: PgPool, username: &str, credential: NewCredential) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let mut db: sqlx::Transaction<'_, sqlx::Postgres> = pool.begin().await?;

    let maybe_admin = get_admin(&mut db).await?;

    let hash = generate_hash(credential)?;

    match maybe_admin {
        Some(admin) => {
            update_admin(&mut db, admin.user_id, username, &hash).await?
        }
        None => {
            create_admin(&mut db, username, &hash).await?;
        }
    }

    db.commit().await?;

    Ok(())

}