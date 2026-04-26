#[cfg(test)]
use std::collections::HashMap;

use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

struct AuthRequest {
    pub username: String,
    pub password: String,
    pub tx: oneshot::Sender<AuthResult>,
}

#[derive(Debug)]
pub enum AuthResult {
    Ok { user_id: u64 },
    Failed,
}

#[derive(Clone)]
pub struct AuthHandle {
    sender: mpsc::Sender<AuthRequest>,
}

impl AuthHandle {
    // Pass credentials to the actor
    // The actor performs the authentication
    pub async fn authenticate(&self, username: String, password: String) -> AuthResult {
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

#[cfg(test)]
pub struct TestUser {
    pub username: String,
    pub password: String,
    pub user_id: u64,
}

#[cfg(test)]
pub fn alice() -> TestUser {
    TestUser {
        username: "alice".to_owned(),
        password: "alicepass".to_owned(),
        user_id: 1,
    }
}

#[cfg(test)]
pub fn bob() -> TestUser {
    TestUser {
        username: "bob".to_owned(),
        password: "bobpass".to_owned(),
        user_id: 2,
    }
}

#[cfg(test)]
pub fn default_test_users() -> Vec<TestUser> {
    vec![alice(), bob()]
}

#[cfg(test)]
pub fn spawn_test(shutdown: CancellationToken, users: Vec<TestUser>) -> AuthHandle {
    // AUTH ACTOR COMMUNICATION CHANNELS
    // rx stays in the spawned actor
    // tx gets returned in AuthHandle
    // Cloning auth handle allows a new
    // communication channel to the actor
    let (tx, mut rx) = mpsc::channel::<AuthRequest>(100);

    let lookup: HashMap<String, (String, u64)> = users
        .into_iter()
        .map(|u| (u.username, (u.password, u.user_id)))
        .collect();

    // AUTH ACTOR
    tokio::spawn(async move {
        loop {
            select! {
                req = rx.recv() => {
                    let Some(req) = req else { continue };
                    let result = match lookup.get(&req.username) {
                        Some((pw, id)) if pw == &req.password => AuthResult::Ok {
                            user_id: *id },
                        _ => AuthResult::Failed,
                    };
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

pub fn spawn(shutdown: CancellationToken) -> AuthHandle {
    #[cfg(test)]
    {
        return spawn_test(shutdown, default_test_users());
    }

    #[cfg(not(test))]
    {
        // AUTH ACTOR COMMUNICATION CHANNELS
        // rx stays in the spawned actor
        // tx gets returned in AuthHandle
        // Cloning auth handle allows a new
        // communication channel to the actor

        let (tx, mut rx) = mpsc::channel::<AuthRequest>(100);

        // AUTH ACTOR
        tokio::spawn(async move {
            loop {
                select! {
                    req = rx.recv() => {
                        let Some(req) = req else { continue };
                        let result = authenticate(req.username, req.password);
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
}

#[cfg(not(test))]
fn authenticate(username: String, password: String) -> AuthResult {
    // TODO
    if username == "user" && password == "pass" {
        return AuthResult::Ok { user_id: 1 };
    } else {
        return AuthResult::Failed;
    }
}
