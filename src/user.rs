use argon2::{Argon2, PasswordHash, PasswordVerifier};
use sqlx::{PgConnection, PgPool, Row, postgres::PgRow};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    actor::generate_hash,
    model::{self, Admin, EditUser, NewCredential, NewUser, Password, User},
};

pub enum UserRequest {
    NewUserRequest {
        user: model::NewUser,
        credential: NewCredential,
        tx: oneshot::Sender<UserResponse>,
    },
    GetUserInfoRequest {
        user_id: Uuid,
        tx: oneshot::Sender<UserResponse>,
    },
    GetUserByUsername {
        username: String,
        tx: oneshot::Sender<UserResponse>,
    },
    IsAdminRequest {
        user_id: Uuid,
        tx: oneshot::Sender<UserResponse>,
    },
    Promote {
        source_user_id: Uuid,
        target_username: String,
        tx: oneshot::Sender<UserResponse>,
    },
    Demote {
        source_user_id: Uuid,
        target_username: String,
        tx: oneshot::Sender<UserResponse>,
    },
    EditUserRequest {
        source_user_id: Uuid,
        target_username: String,
        username: Option<String>,
        first_name: Option<String>,
        last_name: Option<String>,
        alias: Option<String>,
        tx: oneshot::Sender<UserResponse>,
    },
    DeleteUserRequest {
        source_user_id: Uuid,
        target_username: String,
        tx: oneshot::Sender<UserResponse>,
    },
    UpdatePassword {
        source_user_id: Uuid,
        current_password: Password,
        new_password: Password,
        tx: oneshot::Sender<UserResponse>,
    },
    ResetPassword {
        source_user_id: Uuid,
        target_username: String,
        new_password: Password,
        tx: oneshot::Sender<UserResponse>,
    },
}

#[derive(Debug)]
pub enum UserResponse {
    UserCreated { user_id: Uuid },
    UserInfo { user_info: User },
    IsAdmin { is_admin: bool },
    UserDeleted { is_self: bool },
    NoChange,
    NoUserExists,
    Success,
    Failed,
}

#[derive(Clone)]
pub struct UserHandle {
    sender: mpsc::Sender<UserRequest>,
}

impl UserHandle {
    pub async fn new_user(
        &self,
        new_user: model::NewUser,
        new_credential: model::NewCredential,
    ) -> UserResponse {
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
            return UserResponse::Failed;
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
            return false;
        }

        matches!(rx.await, Ok(UserResponse::IsAdmin { is_admin: true }))
    }

    pub async fn edit_user(
        &self,
        user_id: Uuid,
        target_username: &str,
        edit: EditUser,
    ) -> UserResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(UserRequest::EditUserRequest {
                source_user_id: user_id,
                target_username: target_username.to_owned(),
                username: edit.username,
                first_name: edit.first_name,
                last_name: edit.last_name,
                alias: edit.alias,
                tx,
            })
            .await
            .is_err()
        {
            return UserResponse::Failed;
        }

        rx.await.unwrap_or(UserResponse::Failed)
    }

    pub async fn delete_user(&self, target_username: &str, source_user_id: Uuid) -> UserResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(UserRequest::DeleteUserRequest {
                target_username: target_username.to_owned(),
                source_user_id,
                tx,
            })
            .await
            .is_err()
        {
            return UserResponse::Failed;
        }

        rx.await.unwrap_or(UserResponse::Failed)
    }

    pub async fn get_user_by_username(&self, username: &str) -> UserResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(UserRequest::GetUserByUsername {
                username: username.to_owned(),
                tx,
            })
            .await
            .is_err()
        {
            // TODO - Logging
            return UserResponse::Failed;
        }

        rx.await.unwrap_or(UserResponse::Failed)
    }

    pub async fn update_password(
        &self,
        source_user_id: Uuid,
        current_password: Password,
        new_password: Password,
    ) -> UserResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(UserRequest::UpdatePassword {
                source_user_id,
                current_password,
                new_password,
                tx,
            })
            .await
            .is_err()
        {
            return UserResponse::Failed;
        }

        rx.await.unwrap_or(UserResponse::Failed)
    }

    pub async fn reset_password(
        &self,
        source_user_id: Uuid,
        target_username: String,
        new_password: Password,
    ) -> UserResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(UserRequest::ResetPassword {
                source_user_id,
                target_username,
                new_password,
                tx,
            })
            .await
            .is_err()
        {
            return UserResponse::Failed;
        }

        rx.await.unwrap_or(UserResponse::Failed)
    }

    pub async fn promote(&self, source_user_id: Uuid, target_username: &str) -> UserResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(UserRequest::Promote {
                source_user_id,
                target_username: target_username.to_string(),
                tx,
            })
            .await
            .is_err()
        {
            return UserResponse::Failed;
        }

        rx.await.unwrap_or(UserResponse::Failed)
    }

    pub async fn demote(&self, source_user_id: Uuid, target_username: &str) -> UserResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(UserRequest::Demote {
                source_user_id,
                target_username: target_username.to_string(),
                tx,
            })
            .await
            .is_err()
        {
            return UserResponse::Failed;
        }

        rx.await.unwrap_or(UserResponse::Failed)
    }
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

async fn handle_request(
    req: UserRequest,
    pool: PgPool,
) -> (UserResponse, oneshot::Sender<UserResponse>) {
    match req {
        UserRequest::NewUserRequest {
            user,
            credential,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            let hash = match generate_hash(credential) {
                Ok(hash) => hash,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            let user_id = match create_user(&mut db, user, &hash).await {
                Ok(user_id) => user_id,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            match db.commit().await {
                Ok(_) => (UserResponse::UserCreated { user_id }, tx),
                Err(_e) => {
                    // TODO - Logging
                    (UserResponse::Failed, tx)
                }
            }
        }

        UserRequest::GetUserInfoRequest { user_id, tx } => {
            let user = match sqlx::query_as::<_, User>("SELECT * FROM users WHERE user_id = $1")
                .bind(user_id)
                .fetch_optional(&pool)
                .await
            {
                Ok(maybe_user) => match maybe_user {
                    Some(user) => user,
                    None => return (UserResponse::NoUserExists, tx),
                },
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };
            (UserResponse::UserInfo { user_info: user }, tx)
        }

        UserRequest::IsAdminRequest { user_id, tx } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            match is_admin(&mut db, user_id).await {
                Ok(value) => (UserResponse::IsAdmin { is_admin: value }, tx),
                Err(_e) => {
                    // TODO - Logging
                    (UserResponse::Failed, tx)
                }
            }
        }

        UserRequest::EditUserRequest {
            source_user_id,
            target_username,
            username,
            first_name,
            last_name,
            alias,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            match edit_user(
                &mut db,
                source_user_id,
                &target_username,
                EditUser {
                    username,
                    first_name,
                    last_name,
                    alias,
                },
            )
            .await
            {
                Ok(res) => (res, tx),
                Err(_e) => {
                    // TODO - Logging
                    (UserResponse::Failed, tx)
                }
            }
        }

        UserRequest::GetUserByUsername { username, tx } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            match get_user_by_username(&mut db, &username).await {
                Ok(res) => (res, tx),
                Err(_e) => {
                    // TODO - Logging
                    (UserResponse::Failed, tx)
                }
            }
        }

        UserRequest::DeleteUserRequest {
            target_username,
            source_user_id,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            // Translate Username into UserID
            let target_user_id: Uuid =
                match translate_username_to_id(&mut db, &target_username).await {
                    Ok(maybe_user_id) => match maybe_user_id {
                        Some(user_id) => user_id,
                        None => return (UserResponse::Failed, tx),
                    },
                    Err(_) => return (UserResponse::Failed, tx),
                };

            // Check if source user is admin
            let is_admin: bool = match is_admin(&mut db, source_user_id).await {
                Ok(maybe_admin) => maybe_admin,
                Err(_) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            // If the user source is not an admin or targeting itself, fail
            if !is_admin && target_user_id != source_user_id {
                // TODO - Logging
                return (UserResponse::Failed, tx);
            }

            // Check if the target user is Default Admin
            let maybe_admin: Option<Admin> = match get_admin(&mut db, target_user_id).await {
                Ok(maybe_admin) => maybe_admin,
                Err(_) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };
            let is_default = match maybe_admin {
                Some(admin) => admin.is_default, // Target user is an admin
                None => false,                   // Target user is not an admin
            };

            if is_default {
                return (UserResponse::Failed, tx); // Don't allow default admin to be deleted, even by admin or itself
            }

            let success = match sqlx::query("DELETE FROM users WHERE user_id = $1")
                .bind(target_user_id)
                .execute(&mut *db)
                .await
            {
                Ok(res) => res.rows_affected() == 1,
                Err(_e) => {
                    // TODO - Logging
                    false
                }
            };

            if source_user_id == target_user_id && success {
                (UserResponse::UserDeleted { is_self: true }, tx)
            } else if success {
                (UserResponse::UserDeleted { is_self: false }, tx)
            } else {
                (UserResponse::Failed, tx)
            }
        }

        UserRequest::UpdatePassword {
            source_user_id,
            current_password,
            new_password,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            // The default admin's credentials are config-managed (set at
            // boot via ensure_admin); they must not be mutable from the
            // app, even by the default admin itself.
            let source_is_default = match get_admin(&mut db, source_user_id).await {
                Ok(Some(admin)) => admin.is_default,
                Ok(None) => false,
                Err(_) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };
            if source_is_default {
                // TODO - Logging
                return (UserResponse::Failed, tx);
            }

            let verified = verify_password(&mut db, source_user_id, current_password).await;

            if verified {
                let hash = match generate_hash(NewCredential {
                    password: new_password,
                }) {
                    Ok(hash) => hash,
                    Err(_e) => {
                        // TODO - Logging
                        return (UserResponse::Failed, tx);
                    }
                };

                let row = match sqlx::query(
                    "UPDATE credentials
                    SET password_hash = $1,
                        password_last_set = NOW()
                    WHERE user_id = $2",
                )
                .bind(hash)
                .bind(source_user_id)
                .execute(&mut *db)
                .await
                {
                    Ok(res) => res,
                    Err(_e) => {
                        // TODO - Logging
                        return (UserResponse::Failed, tx);
                    }
                }
                .rows_affected();

                if row == 1 {
                    (UserResponse::Success, tx)
                } else {
                    // TODO - Logging
                    (UserResponse::Failed, tx)
                }
            } else {
                // TODO - Logging
                (UserResponse::Failed, tx)
            }
        }

        UserRequest::ResetPassword {
            source_user_id,
            target_username,
            new_password,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            let (source_is_admin, source_is_default) =
                match get_admin(&mut db, source_user_id).await {
                    Ok(is_admin) => match is_admin {
                        Some(admin) => (true, admin.is_default),
                        None => (false, false),
                    },
                    Err(_) => {
                        // TODO - Logging
                        return (UserResponse::Failed, tx);
                    }
                };

            // Non-admins cannot use ResetPassword
            if !source_is_admin {
                return (UserResponse::Failed, tx);
            }

            // Translate Username into UserID
            let target_user_id: Uuid =
                match translate_username_to_id(&mut db, &target_username).await {
                    Ok(maybe_user_id) => match maybe_user_id {
                        Some(user_id) => user_id,
                        None => return (UserResponse::Failed, tx),
                    },
                    Err(_) => return (UserResponse::Failed, tx),
                };

            // Admins can change other user passwords, but not their own
            if target_user_id == source_user_id {
                return (UserResponse::Failed, tx);
            }

            let (target_is_admin, target_is_default) =
                match get_admin(&mut db, target_user_id).await {
                    Ok(is_admin) => match is_admin {
                        Some(admin) => (true, admin.is_default),
                        None => (false, false),
                    },
                    Err(_) => {
                        // TODO - Logging
                        return (UserResponse::Failed, tx);
                    }
                };

            // Only the Default Admin can reset the password of admins
            if target_is_admin && !source_is_default || target_is_default {
                return (UserResponse::Failed, tx);
            }

            let hash = match generate_hash(NewCredential {
                password: new_password,
            }) {
                Ok(hash) => hash,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            let row = match sqlx::query(
                "UPDATE credentials
                SET password_hash = $1,
                    password_last_set = NOW()
                WHERE user_id = $2",
            )
            .bind(hash)
            .bind(target_user_id)
            .execute(&mut *db)
            .await
            {
                Ok(res) => res,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            }
            .rows_affected();

            if row == 1 {
                (UserResponse::Success, tx)
            } else {
                // TODO - Logging
                (UserResponse::Failed, tx)
            }
        }

        UserRequest::Promote {
            source_user_id,
            target_username,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            let source_is_admin = match is_admin(&mut db, source_user_id).await {
                Ok(is_admin) => is_admin,
                Err(_) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            if !source_is_admin {
                return (UserResponse::Failed, tx);
            }

            // Translate Username into UserID
            let target_user_id: Uuid =
                match translate_username_to_id(&mut db, &target_username).await {
                    Ok(maybe_user_id) => match maybe_user_id {
                        Some(user_id) => user_id,
                        None => return (UserResponse::Failed, tx),
                    },
                    Err(_) => return (UserResponse::Failed, tx),
                };

            let result = match sqlx::query(
                "INSERT INTO admins (user_id, granted_by) \
                VALUES ($1, $2) \
                ON CONFLICT DO NOTHING",
            )
            .bind(target_user_id)
            .bind(source_user_id)
            .execute(&mut *db)
            .await
            {
                Ok(res) => res,
                Err(_) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            match result.rows_affected() {
                1 => (UserResponse::Success, tx),
                0 => (UserResponse::NoChange, tx),
                _ => unreachable!(),
            }
        }

        UserRequest::Demote {
            source_user_id,
            target_username,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(_e) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            let source_is_admin = match is_admin(&mut db, source_user_id).await {
                Ok(is_admin) => is_admin,
                Err(_) => {
                    // TODO - Logging
                    return (UserResponse::Failed, tx);
                }
            };

            if !source_is_admin {
                return (UserResponse::Failed, tx);
            }

            // Translate Username into UserID
            let target_user_id: Uuid =
                match translate_username_to_id(&mut db, &target_username).await {
                    Ok(maybe_user_id) => match maybe_user_id {
                        Some(user_id) => user_id,
                        None => return (UserResponse::Failed, tx),
                    },
                    Err(_) => return (UserResponse::Failed, tx),
                };

            let (target_is_admin, target_is_default) =
                match get_admin(&mut db, target_user_id).await {
                    Ok(maybe_admin) => match maybe_admin {
                        Some(admin) => (true, admin.is_default),
                        None => (false, false),
                    },
                    Err(_) => {
                        // TODO - Logging
                        return (UserResponse::Failed, tx);
                    }
                };

            // If the target is not an admin, no need to perform additional db activity
            if !target_is_admin {
                return (UserResponse::NoChange, tx);
            }
            // Reject attempts to demote the default admin
            if target_is_default {
                return (UserResponse::Failed, tx);
            }

            let success = match sqlx::query("DELETE FROM admins WHERE user_id = $1")
                .bind(target_user_id)
                .execute(&mut *db)
                .await
            {
                Ok(res) => res.rows_affected() == 1,
                Err(_e) => {
                    // TODO - Logging
                    false
                }
            };

            if success {
                (UserResponse::Success, tx)
            } else {
                (UserResponse::Failed, tx)
            }
        }
    }
}

async fn create_user(
    db: &mut PgConnection,
    user: NewUser,
    password_hash: &str,
) -> Result<Uuid, sqlx::Error> {
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

async fn edit_user(
    db: &mut PgConnection,
    source_user_id: Uuid,
    target_username: &str,
    edit: EditUser,
) -> Result<UserResponse, sqlx::Error> {
    let row: PgRow = sqlx::query(
        "SELECT
      (SELECT user_id FROM users WHERE username = $1) AS target_id,
      EXISTS (SELECT 1 FROM admins WHERE user_id = $2) AS is_admin",
    )
    .bind(target_username)
    .bind(source_user_id)
    .fetch_one(&mut *db)
    .await?;

    let target_user_id: Option<Uuid> = row.try_get("target_id")?;
    let is_source_admin: bool = row.try_get("is_admin")?;

    // If the target user does not exist, fail
    let Some(target_user_id) = target_user_id else {
        // TODO - Logging
        return Ok(UserResponse::Failed);
    };

    // If the user is not modifying itself or the user is not an admin, fail
    if target_user_id != source_user_id && !is_source_admin {
        // TODO - Logging
        return Ok(UserResponse::Failed);
    }

    // Check if the target user is Default Admin
    let maybe_admin: Option<Admin> = match get_admin(db, target_user_id).await {
        Ok(maybe_admin) => maybe_admin,
        Err(_) => {
            // TODO - Logging
            return Ok(UserResponse::Failed);
        }
    };
    let is_default = match maybe_admin {
        Some(admin) => admin.is_default, // Target user is an admin
        None => false,                   // Target user is not an admin
    };

    if is_default {
        return Ok(UserResponse::Failed); // Don't allow edits to the default admin
    }

    let result = sqlx::query(
        "UPDATE users
        SET first_name = COALESCE($1, first_name),
            last_name  = COALESCE($2, last_name),
            alias      = COALESCE($3, alias),
            username   = COALESCE($4, username)
        WHERE user_id = $5",
    )
    .bind(edit.first_name)
    .bind(edit.last_name)
    .bind(edit.alias)
    .bind(edit.username)
    .bind(target_user_id)
    .execute(&mut *db)
    .await?
    .rows_affected();

    if result == 1 {
        Ok(UserResponse::Success)
    } else {
        // Zero rows were changed
        // TODO - Logging
        Ok(UserResponse::Failed)
    }
}

async fn get_user_by_username(
    db: &mut PgConnection,
    username: &str,
) -> Result<UserResponse, sqlx::Error> {
    let row: Option<User> = sqlx::query_as::<_, User> (
        "SELECT first_name, last_name, alias, username, user_id, created_at FROM users WHERE username = $1"
    ).bind(username)
    .fetch_optional(db)
    .await?;

    match row {
        Some(user) => Ok(UserResponse::UserInfo { user_info: user }),
        None => Ok(UserResponse::NoUserExists),
    }
}

async fn create_admin(
    db: &mut PgConnection,
    username: &str,
    password_hash: &str,
) -> Result<Uuid, sqlx::Error> {
    let admin = NewUser {
        username: username.to_owned(),
        first_name: None,
        last_name: None,
        alias: None,
    };

    let user_id = create_user(db, admin, password_hash).await?;
    sqlx::query("INSERT INTO admins (user_id, is_default) VALUES ($1, true)")
        .bind(user_id)
        .execute(&mut *db)
        .await?;
    Ok(user_id)
}

async fn update_admin(
    db: &mut PgConnection,
    user_id: Uuid,
    username: &str,
    password_hash: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE users SET username = $1 WHERE user_id = $2")
        .bind(username)
        .bind(user_id)
        .execute(&mut *db)
        .await?;

    sqlx::query(
        "UPDATE credentials SET password_hash = $1, password_last_set = now() WHERE user_id = $2",
    )
    .bind(password_hash)
    .bind(user_id)
    .execute(&mut *db)
    .await?;

    Ok(())
}

async fn translate_username_to_id(
    db: &mut PgConnection,
    username: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    let user_response = get_user_by_username(db, username).await?;
    match user_response {
        UserResponse::UserInfo { user_info } => Ok(Some(user_info.user_id)),
        _ => {
            // TODO - Logging
            Ok(None)
        }
    }
}

async fn get_default_admin(db: &mut PgConnection) -> Result<Option<Admin>, sqlx::Error> {
    let row: Option<Admin> = sqlx::query_as(
        "SELECT user_id, granted_by, granted_at, is_default FROM admins WHERE is_default = true",
    )
    .fetch_optional(db)
    .await?;
    Ok(row)
}

async fn is_admin(db: &mut PgConnection, user_id: Uuid) -> Result<bool, sqlx::Error> {
    let is_admin: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM admins WHERE user_id = $1)")
            .bind(user_id)
            .fetch_one(db)
            .await?;
    Ok(is_admin)
}

async fn get_admin(db: &mut PgConnection, user_id: Uuid) -> Result<Option<Admin>, sqlx::Error> {
    sqlx::query_as::<_, Admin>(
        "SELECT user_id, granted_by, granted_at, is_default FROM admins where user_id = $1",
    )
    .bind(user_id)
    .fetch_optional(db)
    .await
}

pub async fn ensure_admin(
    pool: PgPool,
    username: &str,
    credential: NewCredential,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut db: sqlx::Transaction<'_, sqlx::Postgres> = pool.begin().await?;

    let maybe_admin = get_default_admin(&mut db).await?;

    let hash = generate_hash(credential)?;

    match maybe_admin {
        Some(admin) => update_admin(&mut db, admin.user_id, username, &hash).await?,
        None => {
            create_admin(&mut db, username, &hash).await?;
        }
    }

    db.commit().await?;

    Ok(())
}

async fn verify_password(db: &mut PgConnection, user_id: Uuid, password: Password) -> bool {
    let maybe_cred: Option<String> =
        match sqlx::query_scalar("SELECT password_hash FROM credentials WHERE user_id = $1")
            .bind(user_id)
            .fetch_optional(db)
            .await
        {
            Ok(maybe_cred) => maybe_cred,
            Err(_) => return false,
        };

    match maybe_cred {
        Some(cred) => {
            let parsed = match PasswordHash::new(&cred) {
                Ok(parsed) => parsed,
                Err(_e) => return false,
            };

            Argon2::default()
                .verify_password(password.0.as_bytes(), &parsed)
                .is_ok()
        }
        None => false,
    }
}
