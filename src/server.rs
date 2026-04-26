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

use crate::{
    app::AppState,
    auth::{AuthHandle, AuthResult},
    model::{ClientCommand, ServerEvent},
};

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
    auth_handle: AuthHandle,
    who: SocketAddr,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // TODO - Add a timer so a non-authenticated socket cannot remain open indefinitely

        let mut auth_result: Option<AuthResult> = None;

        let limiter = RateLimiter::direct(
            Quota::per_second(NonZeroU32::new(10).unwrap())
                .allow_burst(NonZeroU32::new(20).unwrap()),
        );

        let mut limiter_trigger = 0;

        loop {
            tokio::select! {
                maybe_msg = receiver.next() => {
                    if auth_result.is_none() {
                        match maybe_msg {
                            Some(Ok(Message::Text(t))) => {
                                match serde_json::from_str::<ClientCommand>(&t) {
                                    Ok(ClientCommand::Auth { username, password }) => {
                                        match auth_handle.authenticate(username, password).await {
                                            AuthResult::Ok { user_id } => {
                                                auth_result = Some(AuthResult::Ok { user_id });
                                                let _ = user_tx.send(ServerEvent::AuthOk).await;
                                            }
                                            AuthResult::Failed => {
                                                let _ = user_tx.send(
                                                    ServerEvent::Close { reason: "auth failed".to_owned() }
                                                ).await;
                                            }
                                        }
                                    }
                                    Ok(_) => {
                                        let _ = user_tx.send(
                                            ServerEvent::Close { reason: "auth expected".to_owned() }
                                        ).await;
                                    }
                                    Err(e) => {
                                        println!("ERR {}", e);
                                        let _ = user_tx.send(
                                            ServerEvent::Close { reason: "error occurred".to_owned() }
                                        ).await;
                                    }
                                }
                            }
                            Some(Ok(_)) => {
                                let _ = user_tx.send(
                                    ServerEvent::Close { reason: "auth expected".to_owned() }
                                ).await;
                            }
                            Some(Err(_)) | None => {
                                // TODO
                            }
                        }
                    } else {
                        match maybe_msg {
                            Some(Ok(msg)) => {
                                match limiter.check() {
                                    Ok(()) => {
                                        if process_message(msg, &mut user_tx, who).await.is_break() {
                                            shutdown.cancel();
                                            break;
                                        }
                                    }
                                    Err(_) => {
                                        limiter_trigger = limiter_trigger + 1;

                                        if limiter_trigger > 3 {
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

    // TODO
    // Each user will be subscribed to various rooms
    // Here, we need to take the broadcast channels from each room to which the user belongs,
    // and "merge" each of the broadcast channels into the mpsc so the handler listens on a single channel.
    let (user_tx, user_rx) = tokio::sync::mpsc::channel::<ServerEvent>(100);
    let test_tx = user_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let _ = test_tx
            .send(ServerEvent::Message {
                user_id: Uuid::now_v7(),
                room_id: Uuid::now_v7(),
                value: "TESTING".to_owned(),
            })
            .await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let _ = test_tx
            .send(ServerEvent::Message {
                user_id: Uuid::now_v7(),
                room_id: Uuid::now_v7(),
                value: "TESTING2".to_owned(),
            })
            .await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let _ = test_tx
            .send(ServerEvent::Close {
                reason: "".to_owned(),
            })
            .await;
    });

    let recv_task =
        spawn_receiver_task(receiver, shutdown.clone(), user_tx, auth_handle, who).await;

    let send_task = spawn_sender_task(sender, shutdown.clone(), user_rx, who).await;

    let _ = tokio::join!(send_task, recv_task);
}

async fn process_message(
    msg: Message,
    user_tx: &mut tokio::sync::mpsc::Sender<ServerEvent>,
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
