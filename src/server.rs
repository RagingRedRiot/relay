use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use futures_util::SinkExt;
use futures_util::{
    StreamExt,
    stream::{SplitSink, SplitStream},
};
use governor::{Quota, RateLimiter};
use std::{net::SocketAddr, num::NonZeroU32, ops::ControlFlow};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::model::{NewCredential, NewUser};
use crate::user::{UserHandle, UserResponse};
use crate::{
    app::AppState,
    auth::{AuthHandle, AuthResult},
    model::{ClientCommand, ServerEvent, Password},
};

struct Handles {
    user_handle: UserHandle,
}

async fn send_close(sender: &mut SplitSink<WebSocket, Message>, who: SocketAddr) {
    // TODO tracing
    println!("Sending close to {who}");
    if let Err(e) = sender
        .send(Message::Close(Some(CloseFrame {
            code: axum::extract::ws::close_code::NORMAL,
            reason: Utf8Bytes::from_static("Goodbye"),
        })))
        .await
    {
        println!("Could not send Close due to {e}, probably it is ok?");
    }
}

async fn spawn_sender_task(
    mut sender: SplitSink<WebSocket, Message>,
    shutdown: CancellationToken,
    mut user_rx: tokio::sync::mpsc::Receiver<ServerEvent>,
    who: SocketAddr,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                maybe_event = user_rx.recv() => {
                    let Some(event) = maybe_event else { break };
                    if matches!(event, ServerEvent::Close { .. }) {
                        send_close(&mut sender, who).await;
                        shutdown.cancel();
                        break
                    }
                    let text: Utf8Bytes = serde_json::to_string(&event).unwrap().into();
                    if sender.send(Message::Text(text)).await.is_err() {
                        shutdown.cancel();
                        break
                    }
                }
                _ = shutdown.cancelled() => {
                    // Best-effort drain
                    // while let Ok(event) = user_rx.try_recv() {
                        // TODO
                    // }
                    send_close(&mut sender, who).await;
                    break
                }
            }
        }
    })
}

async fn spawn_receiver_task(
    mut receiver: SplitStream<WebSocket>,
    shutdown: CancellationToken,
    mut user_tx: tokio::sync::mpsc::Sender<ServerEvent>,
    auth_handle: AuthHandle, // AuthHandle is purposely separate from handles
    handles: Handles,
    open_signups: bool,
    who: SocketAddr,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {

        let (user_id, server_event) = prelude(&mut receiver, &auth_handle, &handles.user_handle, open_signups).await;

        match server_event{
            ServerEvent::AuthOk => {
                let _ = user_tx.send(ServerEvent::AuthOk).await;
            }
            // Anything but AuthOk results in shutdown
            ServerEvent::NoAuth => {
                let _ = user_tx.send(ServerEvent::NoAuth).await;
                let _ = user_tx.send(
                    ServerEvent::Close { reason: "auth failed".to_owned() }
                ).await;
                return
            }
            ServerEvent::UserCreated => {
                let _ = user_tx.send(ServerEvent::UserCreated).await;
                let _ = user_tx.send(
                    ServerEvent::Close { reason: "user created".to_owned() }
                ).await;
                return
            }
            ServerEvent::NoUserCreated => {
                let _ = user_tx.send(ServerEvent::NoUserCreated).await;
                let _ = user_tx.send(
                    ServerEvent::Close { reason: "user creation failed".to_owned() }
                ).await;
                return
            }
            _ => {
                let _ = user_tx.send(
                    ServerEvent::Close { reason: "invalid command".to_owned() }
                ).await;
                return
            }
        };

        let limiter = RateLimiter::direct(
            Quota::per_second(NonZeroU32::new(10).unwrap())
                .allow_burst(NonZeroU32::new(20).unwrap()),
        );

        let mut limiter_count = 0;

        loop {
            tokio::select! {
                maybe_msg = receiver.next() => {
                    match maybe_msg {
                        Some(Ok(msg)) => {
                            match limiter.check() {
                                Ok(()) => {
                                    if process_message(msg, &mut user_tx, &handles, open_signups, user_id, who).await.is_break() {
                                        shutdown.cancel();
                                        break;
                                    }
                                }
                                Err(_) => {
                                    limiter_count = limiter_count + 1;

                                    if limiter_count > 3 {
                                        let _ = user_tx.send(
                                            ServerEvent::Close { reason: "rate limit exceeded three times".to_owned() }
                                        ).await;
                                    } else {
                                        let _ = user_tx.send(
                                            ServerEvent::RateLimit { error: "rate limit exceeded".to_owned() }
                                        ).await;
                                    }
                                }
                            }
                        }
                        Some(Err(_)) | None => {
                            shutdown.cancel();
                            break;
                        }
                    }
                }
                _ = shutdown.cancelled() => {
                    break
                }
            }
        }
    })
}

pub(crate) async fn handle_socket(socket: WebSocket, state: AppState, who: SocketAddr) {
    let (sender, receiver) = socket.split();

    let shutdown: CancellationToken = state.shutdown.child_token();
    let auth_handle: AuthHandle = state.auth_handle.clone();
    let open_signups: bool = state.config.open_signups;
    let handles = Handles {
        user_handle: state.user_handle.clone()
    };

    // TODO
    // Each user will be subscribed to various rooms
    // Here, we need to take the broadcast channels from each room to which the user belongs,
    // and "merge" each of the broadcast channels into the mpsc so the handler listens on a single channel.
    let (user_tx, user_rx) = tokio::sync::mpsc::channel::<ServerEvent>(100);

    let recv_task =
        spawn_receiver_task(receiver, shutdown.clone(), user_tx, auth_handle, handles, open_signups, who).await;

    let send_task = spawn_sender_task(sender, shutdown.clone(), user_rx, who).await;

    let _ = tokio::join!(send_task, recv_task);
}

async fn process_message(
    msg: Message,
    user_tx: &mut tokio::sync::mpsc::Sender<ServerEvent>,
    handles: &Handles,
    open_signups: bool,
    user_id: Uuid,
    who: SocketAddr,
) -> ControlFlow<(), ()> {
    match msg {
        Message::Text(t) => {
            match serde_json::from_str::<ClientCommand>(&t) {
                Ok(cmd) => match cmd {
                    ClientCommand::Close => {
                        user_tx
                            .send(ServerEvent::Close {
                                reason: "client closed".to_owned(),
                            })
                            .await
                            .unwrap();
                    }
                    // Authentication Request — already authed at this point, ignore
                    ClientCommand::Auth { .. } => {}
                    // Echo to Client
                    ClientCommand::Echo { string } => {
                        user_tx.send(ServerEvent::Echo { string }).await.unwrap();
                    }
                    ClientCommand::Message {
                        user_id,
                        room_id,
                        value,
                    } => {
                        println!(
                            "NEEDS IMPLEMENTED: user_id={} room_id={} value={}",
                            user_id, room_id, value
                        )
                    }
                    ClientCommand::Error { .. } => {
                        // ERROR from the CLIENT
                    }
                    ClientCommand::NewUser {
                        username,
                        password,
                        first_name,
                        last_name,
                        alias
                    } => {
                        // When signups are closed, only admins may create users.
                        // Validate admin rights just-in-time for this request
                        // rather than trusting a value cached at connect time.
                        let allowed = open_signups
                            || handles.user_handle.is_admin(user_id).await;
                        if !allowed {
                            // TODO - Logging
                            let _ = user_tx.send(ServerEvent::NoUserCreated).await;
                        } else {
                            match new_user(&handles.user_handle, username, password, first_name, last_name, alias).await {
                                UserResponse::UserCreated { .. } => {
                                    let _ = user_tx.send(ServerEvent::UserCreated).await;
                                }
                                UserResponse::Failed => {
                                    // TODO - Logging
                                    let _ = user_tx.send(ServerEvent::NoUserCreated).await;
                                }
                                _ => {
                                    // TODO - Logging
                                    let _ = user_tx.send(ServerEvent::NoUserCreated).await;
                                }
                            }
                        }
                    },
                },
                Err(e) => {
                    println!("ERR: {:?}", e);
                    if e.is_data() {
                        let _ = user_tx
                            .send(ServerEvent::Error {
                                error: "invalid command".to_owned(),
                            })
                            .await;
                    } else if e.is_syntax() {
                        let _ = user_tx
                            .send(ServerEvent::Error {
                                error: "malformed JSON".to_owned(),
                            })
                            .await;
                    } else if e.is_eof() {
                        let _ = user_tx
                            .send(ServerEvent::Error {
                                error: "incomplete message".to_owned(),
                            })
                            .await;
                    } else {
                        let _ = user_tx
                            .send(ServerEvent::Error {
                                error: "unknown error".to_owned(),
                            })
                            .await;
                        println!("unknown error: {}", e);
                        todo!()
                    };
                }
            }
            // TODO TRACING
            // println!(">>> {who} sent str: {t:?}");
        }
        Message::Binary(d) => {
            println!(">>> {who} sent {} bytes: {d:?}", d.len());
        }
        Message::Close(c) => {
            let reason = if let Some(cf) = c {
                format!("client requested shutdown : {} {}", cf.code, cf.reason)
            } else {
                "client requested shutdown without close frame.".to_owned()
            };
            user_tx.send(ServerEvent::Close { reason }).await.unwrap();
        }

        Message::Pong(v) => {
            println!(">>> {who} sent pong with {v:?}");
        }
        // You should never need to manually handle Message::Ping, as axum's websocket library
        // will do so for you automagically by replying with Pong and copying the v according to
        // spec. But if you need the contents of the pings you can see them here.
        Message::Ping(v) => {
            println!(">>> {who} sent ping with {v:?}");
        }
    }
    ControlFlow::Continue(())
}

async fn new_user(
    user_handle: &UserHandle,
    username: String,
    password: Password, 
    first_name: Option<String>, 
    last_name: Option<String>, 
    alias: Option<String>
) -> UserResponse {
    user_handle.new_user(
        NewUser {
            username: username.to_owned(),
            first_name,
            last_name,
            alias
        },
        NewCredential{
            password: password
        }
    ).await
}

async fn prelude(
    receiver: &mut SplitStream<WebSocket>,
    auth_handle: &AuthHandle,
    user_handle: &UserHandle,
    open_signups: bool,
) -> (Uuid, ServerEvent) {
    tokio::select! {
        maybe_auth = receiver.next() => {
            match maybe_auth {
                Some(Ok(Message::Text(t))) => {
                    match serde_json::from_str::<ClientCommand>(&t) {
                        Ok(ClientCommand::Auth{username, password}) => {
                            match auth_handle.authenticate(username, password).await {
                                AuthResult::Ok { user_id } => (user_id, ServerEvent::AuthOk),
                                AuthResult::Failed => (Uuid::nil(), ServerEvent::NoAuth),
                            }
                        },
                        // Unauthenticated user creation is only allowed when open
                        // signups are enabled in the config; otherwise reject it.
                        Ok(ClientCommand::NewUser { .. }) if !open_signups => {
                            // TODO - Logging
                            (Uuid::nil(), ServerEvent::NoAuth)
                        }
                        Ok(ClientCommand::NewUser {
                            username,
                            password,
                            first_name,
                            last_name,
                            alias
                        }) => {
                            match new_user(&user_handle, username, password, first_name, last_name, alias).await {
                                UserResponse::UserCreated { user_id } => { (user_id, ServerEvent::UserCreated ) }
                                UserResponse::Failed => {
                                    // TODO - Logging
                                    (Uuid::nil(), ServerEvent::NoUserCreated)
                                }
                                _ => { 
                                    // TODO - Logging
                                    (Uuid::nil(), ServerEvent::NoUserCreated)
                                }
                            }
                        },
                        Err(_e) => {
                            // TODO - Logging
                            (Uuid::nil(), ServerEvent::NoAuth)
                        }
                        _ => (Uuid::nil(), ServerEvent::NoAuth)
                    }
                }
                Some(Err(_e)) => {
                    // TODO - Logging
                    (Uuid::nil(), ServerEvent::NoAuth)
                }
                _ => (Uuid::nil(), ServerEvent::NoAuth)
            }
        }
    }
}