use std::sync::Arc;

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::{SaltString, rand_core::OsRng}};
use sqlx::PgPool;
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::model::Password;

struct AuthRequest {
    username: String,
    password: Password,
    tx: oneshot::Sender<AuthResult>,
}

#[derive(Debug)]
pub enum AuthResult {
    Ok { user_id: Uuid },
    Failed,
}

#[derive(Clone)]
pub struct AuthHandle {
    sender: mpsc::Sender<AuthRequest>,
}

impl AuthHandle {
    // Pass credentials to the actor
    // The actor performs the authentication
    pub async fn authenticate(&self, username: String, password: Password) -> AuthResult {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(AuthRequest {
                username,
                password,
                tx,
            })
            .await
            .is_err()
        {
            return AuthResult::Failed;
        }
        rx.await.unwrap_or(AuthResult::Failed)
    }
}

async fn authenticate(username: String, password: Password, dummy_hash: Arc<String>, pool: PgPool) -> AuthResult {
    let row: Option<(Uuid, String)> = match sqlx::query_as(
        "SELECT u.user_id, c.password_hash
        FROM users u
        JOIN credentials c USING (user_id)
        WHERE u.username = $1"
    )
    .bind(&username)
    .fetch_optional(&pool)
    .await {
        Ok(maybe_data) => { maybe_data }
        Err(_e) => { 
            // TODO - Logging
            return AuthResult::Failed
        }
    };

    let (user_id, verified) = match row {
        Some((user_id, hash)) => {
            let parsed = match PasswordHash::new(&hash) {
                Ok(parsed) => { parsed }
                Err(_e) => { 
                    // TODO - Logging
                    return AuthResult::Failed
                }
            };
            let verified = Argon2::default().verify_password(password.0.as_bytes(), &parsed).is_ok();
            (user_id, verified)
        }
        None => {
            // User Doesn't Exist
            // Perform a dummy check to take the same amount of time as if the user exists
            let parsed = match PasswordHash::new(&dummy_hash) {
                Ok(parsed) => { parsed }
                Err(_e) => { 
                    // TODO - Logging
                    return AuthResult::Failed
                }
            };
            let _ = Argon2::default().verify_password(password.0.as_bytes(), &parsed).is_ok();
            return AuthResult::Failed
        }
    };

    if verified {
        AuthResult::Ok { user_id }
    } else {
        AuthResult::Failed
    }
}

pub async fn spawn(shutdown: CancellationToken, pool: PgPool) -> AuthHandle {
    // AUTH ACTOR COMMUNICATION CHANNELS
    // rx stays in the spawned actor
    // tx gets returned in AuthHandle
    // Cloning auth handle allows a new
    // communication channel to the actor
    let (tx, mut rx) = mpsc::channel::<AuthRequest>(100);

    let dummy: String = {
      let salt = SaltString::generate(&mut OsRng);
      Argon2::default()
          .hash_password(b"dummy", &salt)
          .expect("hash dummy password")
          .to_string()
    };

    let dummy_hash = Arc::new(dummy);

    // AUTH ACTOR TASK
    tokio::spawn(async move {
        loop {
            select! {
                req = rx.recv() => {
                    // All handles dropped — no more requests will arrive.
                    let Some(req) = req else { break };
                    let result = authenticate(req.username, req.password, Arc::clone(&dummy_hash), pool.clone()).await;
                    let _ = req.tx.send(result);
                }
                _ = shutdown.cancelled() => {
                    break
                }
            }
        }
    });

    // AUTH ACTOR HANDLE
    AuthHandle { sender: tx }
}
