use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use sqlx::{PgConnection, PgPool, Row, postgres::PgRow};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::model::{self, Admin, EditUser, NewCredential, NewUser, Password, User};

// Directory page size when the client doesn't ask, and the hard cap when it does
// -- bounds one GetUsers response so a large user base can't be pulled at once.
const DEFAULT_USER_PAGE: i64 = 50;
const MAX_USER_PAGE: i64 = 100;

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
    GetUsers {
        source_user_id: Uuid,
        starts_with: Option<String>,
        after: Option<String>,
        limit: Option<u32>,
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
    UserCreated {
        user_id: Uuid,
    },
    UserInfo {
        user_info: User,
    },
    IsAdmin {
        is_admin: bool,
    },
    UserDeleted {
        is_self: bool,
    },
    // One page of the user directory, plus whether another page follows.
    Users {
        users: Vec<model::UserDirectoryEntry>,
        has_more: bool,
    },
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
            tracing::error!("user actor unavailable: get_user_by_username request dropped");
            return UserResponse::Failed;
        }

        rx.await.unwrap_or(UserResponse::Failed)
    }

    // Page the user directory on behalf of `source_user_id`. The caller's admin
    // status (which gates the per-entry `is_admin` flag) is resolved in the actor.
    pub async fn get_users(
        &self,
        source_user_id: Uuid,
        starts_with: Option<String>,
        after: Option<String>,
        limit: Option<u32>,
    ) -> UserResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(UserRequest::GetUsers {
                source_user_id,
                starts_with,
                after,
                limit,
                tx,
            })
            .await
            .is_err()
        {
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
                Err(e) => {
                    tracing::error!(error = %e, "new_user: begin transaction failed");
                    return (UserResponse::Failed, tx);
                }
            };

            let hash = match generate_hash(credential) {
                Ok(hash) => hash,
                Err(e) => {
                    tracing::error!(error = %e, "new_user: password hashing failed");
                    return (UserResponse::Failed, tx);
                }
            };

            // Captured for the audit log below; create_user consumes `user`.
            let username = user.username.clone();
            let user_id = match create_user(&mut db, user, &hash).await {
                Ok(user_id) => user_id,
                Err(e) => {
                    // Commonly a unique-violation (username already taken), which is
                    // a client error, not a server fault -- warn rather than error.
                    tracing::warn!(error = %e, "new_user: user insert failed (e.g. duplicate username)");
                    return (UserResponse::Failed, tx);
                }
            };

            match db.commit().await {
                Ok(_) => {
                    // Single audit point for account creation -- covers both the open
                    // signup and authenticated-creation paths that reach this actor.
                    tracing::info!(target: crate::logging::AUDIT, %user_id, username = %username, "account created");
                    (UserResponse::UserCreated { user_id }, tx)
                }
                Err(e) => {
                    tracing::error!(error = %e, %user_id, "new_user: commit failed");
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
                Err(e) => {
                    tracing::error!(error = %e, %user_id, "get_user_info: query failed");
                    return (UserResponse::Failed, tx);
                }
            };
            (UserResponse::UserInfo { user_info: user }, tx)
        }

        UserRequest::IsAdminRequest { user_id, tx } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "is_admin: acquire connection failed");
                    return (UserResponse::Failed, tx);
                }
            };

            match is_admin(&mut db, user_id).await {
                Ok(value) => (UserResponse::IsAdmin { is_admin: value }, tx),
                Err(e) => {
                    tracing::error!(error = %e, %user_id, "is_admin: query failed");
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
                Err(e) => {
                    tracing::error!(error = %e, "edit_user: acquire connection failed");
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
                Err(e) => {
                    tracing::error!(error = %e, actor = %source_user_id, target = %target_username, "edit_user: query failed");
                    (UserResponse::Failed, tx)
                }
            }
        }

        UserRequest::GetUserByUsername { username, tx } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "get_user_by_username: acquire connection failed");
                    return (UserResponse::Failed, tx);
                }
            };

            match get_user_by_username(&mut db, &username).await {
                Ok(res) => (res, tx),
                Err(e) => {
                    tracing::error!(error = %e, "get_user_by_username: query failed");
                    (UserResponse::Failed, tx)
                }
            }
        }

        UserRequest::GetUsers {
            source_user_id,
            starts_with,
            after,
            limit,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "get_users: acquire connection failed");
                    return (UserResponse::Failed, tx);
                }
            };

            match get_users(&mut db, source_user_id, starts_with, after, limit).await {
                Ok(res) => (res, tx),
                Err(e) => {
                    tracing::error!(error = %e, "get_users: query failed");
                    (UserResponse::Failed, tx)
                }
            }
        }

        UserRequest::DeleteUserRequest {
            target_username,
            source_user_id,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "delete_user: begin transaction failed");
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
                Err(e) => {
                    tracing::error!(error = %e, "delete_user: admin check query failed");
                    return (UserResponse::Failed, tx);
                }
            };

            // If the user source is not an admin or targeting itself, fail
            if !is_admin && target_user_id != source_user_id {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, "delete denied: caller is not an admin and not deleting self");
                return (UserResponse::Failed, tx);
            }

            // Check if the target user is Default Admin
            let maybe_admin: Option<Admin> = match get_admin(&mut db, target_user_id).await {
                Ok(maybe_admin) => maybe_admin,
                Err(e) => {
                    tracing::error!(error = %e, "delete_user: target admin lookup failed");
                    return (UserResponse::Failed, tx);
                }
            };
            let is_default = match maybe_admin {
                Some(admin) => admin.is_default, // Target user is an admin
                None => false,                   // Target user is not an admin
            };

            if is_default {
                // Don't allow the default admin to be deleted, even by an admin or itself.
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, "delete denied: target is the default admin");
                return (UserResponse::Failed, tx);
            }

            // Put a DB lock on the target user's ID
            if let Err(e) = sqlx::query("SELECT user_id FROM users WHERE user_id = $1 FOR UPDATE")
                .bind(target_user_id)
                .execute(&mut *db)
                .await
            {
                tracing::error!(error = %e, %target_user_id, "delete_user: row lock failed");
                return (UserResponse::Failed, tx);
            }

            if let Err(e) = sqlx::query(
                "UPDATE messages set sender_username_snapshot = u.username
                FROM users u
                WHERE messages.sender_id = $1 AND u.user_id = $1",
            )
            .bind(target_user_id)
            .execute(&mut *db)
            .await
            {
                tracing::error!(error = %e, %target_user_id, "delete_user: username snapshot update failed");
                return (UserResponse::Failed, tx);
            };

            // No room-ownership cleanup needed: ownership is a flag on memberships,
            // which cascade-delete with the user. A room may be left with no owners
            // -- that's allowed, not an error, because every owner-gated action also
            // permits admins, so an admin can re-grant ownership to a member. We
            // deliberately don't escalate to the default admin, so it never
            // accumulates ownership of orphaned rooms.

            let success = match sqlx::query("DELETE FROM users WHERE user_id = $1")
                .bind(target_user_id)
                .execute(&mut *db)
                .await
            {
                Ok(res) => res.rows_affected() == 1,
                Err(e) => {
                    tracing::error!(error = %e, %target_user_id, "delete_user: delete failed");
                    false
                }
            };

            if !success {
                return (UserResponse::Failed, tx);
            }
            if let Err(e) = db.commit().await {
                tracing::error!(error = %e, %target_user_id, "delete_user: commit failed");
                return (UserResponse::Failed, tx);
            }
            let is_self = source_user_id == target_user_id;

            tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, %target_user_id, is_self, "user deleted");
            (UserResponse::UserDeleted { is_self }, tx)
        }

        UserRequest::UpdatePassword {
            source_user_id,
            current_password,
            new_password,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "update_password: acquire connection failed");
                    return (UserResponse::Failed, tx);
                }
            };

            // The default admin's credentials are config-managed (set at
            // boot via ensure_admin); they must not be mutable from the
            // app, even by the default admin itself.
            let source_is_default = match get_admin(&mut db, source_user_id).await {
                Ok(Some(admin)) => admin.is_default,
                Ok(None) => false,
                Err(e) => {
                    tracing::error!(error = %e, "update_password: admin lookup failed");
                    return (UserResponse::Failed, tx);
                }
            };
            if source_is_default {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, "update_password denied: default admin credentials are config-managed");
                return (UserResponse::Failed, tx);
            }

            let verified = verify_password(&mut db, source_user_id, current_password).await;

            if verified {
                let hash = match generate_hash(NewCredential {
                    password: new_password,
                }) {
                    Ok(hash) => hash,
                    Err(e) => {
                        tracing::error!(error = %e, "update_password: password hashing failed");
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
                    Err(e) => {
                        tracing::error!(error = %e, %source_user_id, "update_password: credential update failed");
                        return (UserResponse::Failed, tx);
                    }
                }
                .rows_affected();

                if row == 1 {
                    tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, "password changed");
                    (UserResponse::Success, tx)
                } else {
                    // No credential row matched -- shouldn't happen for a verified user.
                    tracing::warn!(%source_user_id, "update_password: no credential row updated");
                    (UserResponse::Failed, tx)
                }
            } else {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, "update_password denied: current password incorrect");
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
                Err(e) => {
                    tracing::error!(error = %e, "reset_password: acquire connection failed");
                    return (UserResponse::Failed, tx);
                }
            };

            let (source_is_admin, source_is_default) =
                match get_admin(&mut db, source_user_id).await {
                    Ok(is_admin) => match is_admin {
                        Some(admin) => (true, admin.is_default),
                        None => (false, false),
                    },
                    Err(e) => {
                        tracing::error!(error = %e, "reset_password: caller admin lookup failed");
                        return (UserResponse::Failed, tx);
                    }
                };

            // Non-admins cannot use ResetPassword
            if !source_is_admin {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, "reset_password denied: caller is not an admin");
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
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, "reset_password denied: admin cannot reset own password");
                return (UserResponse::Failed, tx);
            }

            let (target_is_admin, target_is_default) =
                match get_admin(&mut db, target_user_id).await {
                    Ok(is_admin) => match is_admin {
                        Some(admin) => (true, admin.is_default),
                        None => (false, false),
                    },
                    Err(e) => {
                        tracing::error!(error = %e, "reset_password: target admin lookup failed");
                        return (UserResponse::Failed, tx);
                    }
                };

            // Only the Default Admin can reset the password of admins
            if target_is_admin && !source_is_default || target_is_default {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, "reset_password denied: only the default admin may reset an admin's password");
                return (UserResponse::Failed, tx);
            }

            let hash = match generate_hash(NewCredential {
                password: new_password,
            }) {
                Ok(hash) => hash,
                Err(e) => {
                    tracing::error!(error = %e, "reset_password: password hashing failed");
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
                Err(e) => {
                    tracing::error!(error = %e, %target_user_id, "reset_password: credential update failed");
                    return (UserResponse::Failed, tx);
                }
            }
            .rows_affected();

            if row == 1 {
                tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, %target_user_id, "password reset");
                (UserResponse::Success, tx)
            } else {
                tracing::warn!(%target_user_id, "reset_password: no credential row updated");
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
                Err(e) => {
                    tracing::error!(error = %e, "promote: acquire connection failed");
                    return (UserResponse::Failed, tx);
                }
            };

            let source_is_admin = match is_admin(&mut db, source_user_id).await {
                Ok(is_admin) => is_admin,
                Err(e) => {
                    tracing::error!(error = %e, "promote: caller admin check failed");
                    return (UserResponse::Failed, tx);
                }
            };

            if !source_is_admin {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, "promote denied: caller is not an admin");
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
                Err(e) => {
                    tracing::error!(error = %e, %target_user_id, "promote: admin insert failed");
                    return (UserResponse::Failed, tx);
                }
            };

            match result.rows_affected() {
                1 => {
                    tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, %target_user_id, "user promoted to admin");
                    (UserResponse::Success, tx)
                }
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
                Err(e) => {
                    tracing::error!(error = %e, "demote: acquire connection failed");
                    return (UserResponse::Failed, tx);
                }
            };

            let source_is_admin = match is_admin(&mut db, source_user_id).await {
                Ok(is_admin) => is_admin,
                Err(e) => {
                    tracing::error!(error = %e, "demote: caller admin check failed");
                    return (UserResponse::Failed, tx);
                }
            };

            if !source_is_admin {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, "demote denied: caller is not an admin");
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
                    Err(e) => {
                        tracing::error!(error = %e, "demote: target admin lookup failed");
                        return (UserResponse::Failed, tx);
                    }
                };

            // If the target is not an admin, no need to perform additional db activity
            if !target_is_admin {
                return (UserResponse::NoChange, tx);
            }
            // Reject attempts to demote the default admin
            if target_is_default {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, "demote denied: target is the default admin");
                return (UserResponse::Failed, tx);
            }

            let success = match sqlx::query("DELETE FROM admins WHERE user_id = $1")
                .bind(target_user_id)
                .execute(&mut *db)
                .await
            {
                Ok(res) => res.rows_affected() == 1,
                Err(e) => {
                    tracing::error!(error = %e, %target_user_id, "demote: admin delete failed");
                    false
                }
            };

            if success {
                tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, %target_user_id, "user demoted from admin");
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
    let user_id: Uuid = sqlx::query("INSERT INTO users (username, first_name, last_name, alias) VALUES (trim_ws($1), NULLIF(trim_ws($2), ''), NULLIF(trim_ws($3), ''), NULLIF(trim_ws($4), '')) RETURNING user_id")
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
      (SELECT user_id FROM users WHERE LOWER(username) = LOWER(trim_ws($1))) AS target_id,
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
        tracing::debug!(target = %target_username, "edit_user: target user not found");
        return Ok(UserResponse::Failed);
    };

    // If the user is not modifying itself or the user is not an admin, fail
    if target_user_id != source_user_id && !is_source_admin {
        tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, "edit denied: caller is not an admin and not editing self");
        return Ok(UserResponse::Failed);
    }

    // Check if the target user is Default Admin
    let maybe_admin: Option<Admin> = match get_admin(db, target_user_id).await {
        Ok(maybe_admin) => maybe_admin,
        Err(e) => {
            tracing::error!(error = %e, "edit_user: target admin lookup failed");
            return Ok(UserResponse::Failed);
        }
    };
    let is_default = match maybe_admin {
        Some(admin) => admin.is_default, // Target user is an admin
        None => false,                   // Target user is not an admin
    };

    if is_default {
        // Don't allow edits to the default admin.
        tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, "edit denied: target is the default admin");
        return Ok(UserResponse::Failed);
    }

    let result = sqlx::query(
        "UPDATE users
        SET first_name = CASE WHEN $1 IS NULL THEN first_name ELSE NULLIF(trim_ws($1), '') END,
            last_name  = CASE WHEN $2 IS NULL THEN last_name  ELSE NULLIF(trim_ws($2), '') END,
            alias      = CASE WHEN $3 IS NULL THEN alias       ELSE NULLIF(trim_ws($3), '') END,
            username   = COALESCE(NULLIF(trim_ws($4), ''), username)
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
        tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, target = %target_username, %target_user_id, "user profile edited");
        Ok(UserResponse::Success)
    } else {
        // Zero rows changed -- target vanished between the lookup and the update.
        tracing::debug!(%target_user_id, "edit_user: no rows updated");
        Ok(UserResponse::Failed)
    }
}

async fn get_user_by_username(
    db: &mut PgConnection,
    username: &str,
) -> Result<UserResponse, sqlx::Error> {
    let row: Option<User> = sqlx::query_as::<_, User> (
        "SELECT first_name, last_name, alias, username, user_id, created_at FROM users WHERE LOWER(username) = LOWER(trim_ws($1))"
    ).bind(username)
    .fetch_optional(db)
    .await?;

    match row {
        Some(user) => Ok(UserResponse::UserInfo { user_info: user }),
        None => Ok(UserResponse::NoUserExists),
    }
}

// One row of a directory page as it comes back from the DB. `is_admin` is always
// computed here; whether it's *revealed* to the caller is decided in `get_users`.
#[derive(sqlx::FromRow)]
struct DirectoryRow {
    first_name: Option<String>,
    last_name: Option<String>,
    alias: Option<String>,
    username: String,
    created_at: chrono::DateTime<chrono::Utc>,
    is_admin: bool,
}

// Escape the LIKE metacharacters (`\`, `%`, `_`) in a user-supplied prefix so they
// match literally rather than as wildcards. Backslash is escaped first so the
// escapes we add aren't themselves re-escaped. Pairs with `ESCAPE '\'` in the query.
fn escape_like(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

// Page the user directory, ordered by username (case-insensitive), with an
// optional prefix filter and a keyset cursor. Fetches one extra row to report
// `has_more` without a second count query. The per-entry `is_admin` flag is only
// populated when the caller is themselves an admin; for a regular caller it is
// None (and omitted on the wire), so admin status isn't leaked to non-admins.
async fn get_users(
    db: &mut PgConnection,
    source_user_id: Uuid,
    starts_with: Option<String>,
    after: Option<String>,
    limit: Option<u32>,
) -> Result<UserResponse, sqlx::Error> {
    // Clamp the page size: default when unset, hard cap, floor 1 so a request
    // always makes progress. Fetch one extra to detect a following page.
    let limit = limit
        .map(|l| (l as i64).clamp(1, MAX_USER_PAGE))
        .unwrap_or(DEFAULT_USER_PAGE);
    let fetch = limit + 1;

    // Normalize the optional prefix: trim, drop if empty, then escape LIKE
    // metacharacters so a literal `%`/`_` in the prefix isn't treated as a wildcard.
    let prefix = starts_with
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .map(|s| escape_like(&s));

    // Reveal admin status only to an admin caller (the admin-pane use case).
    let caller_is_admin = is_admin(&mut *db, source_user_id).await?;

    let rows = sqlx::query_as::<_, DirectoryRow>(
        "SELECT u.first_name, u.last_name, u.alias, u.username, u.created_at,
                EXISTS (SELECT 1 FROM admins a WHERE a.user_id = u.user_id) AS is_admin
            FROM users u
            WHERE ($1::text IS NULL OR LOWER(u.username) LIKE LOWER($1) || '%' ESCAPE '\\')
              AND ($2::text IS NULL OR LOWER(u.username) > LOWER($2))
            ORDER BY LOWER(u.username) ASC
            LIMIT $3",
    )
    .bind(prefix.as_deref())
    .bind(after.as_deref())
    .bind(fetch)
    .fetch_all(&mut *db)
    .await?;

    let has_more = rows.len() as i64 > limit;
    let users = rows
        .into_iter()
        .take(limit as usize)
        .map(|r| model::UserDirectoryEntry {
            first_name: r.first_name,
            last_name: r.last_name,
            alias: r.alias,
            username: r.username,
            created_at: r.created_at,
            is_admin: caller_is_admin.then_some(r.is_admin),
        })
        .collect();

    Ok(UserResponse::Users { users, has_more })
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
    sqlx::query("UPDATE users SET username = trim_ws($1) WHERE user_id = $2")
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
            tracing::debug!(username = %username, "translate_username_to_id: no user for username");
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

fn generate_hash(credential: NewCredential) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(credential.password.0.as_bytes(), &salt)?
        .to_string();
    Ok(hash)
}
