use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use futures_util::{StreamExt, stream::{SplitSink, SplitStream}};
use futures_util::SinkExt;
use governor::{Quota, RateLimiter};
use tokio_util::sync::CancellationToken;
use std::{net::SocketAddr, num::NonZeroU32, ops::ControlFlow};

use core::fmt;
use serde::{Deserialize, Serialize};

use crate::handler::AppState;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ServerEvent {
    pub action: ACTION,
    pub content: String
}

impl fmt::Display for ServerEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ServerEvent ({}, {})", self.action, self.content)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ClientCommand {
    pub(crate) action: ACTION,
    pub(crate) content: String
}

impl fmt::Display for ClientCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientCommand ({}, {})", self.action, self.content)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ACTION {
    CLOSE,
    MESSAGE,
    ECHO,
    ERROR,
    RATELIMIT
}

impl fmt::Display for ACTION {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {                                                                                                
        match self {
            ACTION::CLOSE => write!(f, "CLOSE"),
            ACTION::MESSAGE => write!(f, "MESSAGE"),
            ACTION::ECHO => write!(f, "ECHO"),
            ACTION::ERROR => write!(f, "ERROR"),
            ACTION::RATELIMIT => write!(f, "RATELIMIT")
        }
    }
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

async fn spawn_sender_task(mut sender: SplitSink<WebSocket, Message>, shutdown: CancellationToken, mut user_rx: tokio::sync::mpsc::Receiver<ServerEvent>, who: SocketAddr) -> tokio::task::JoinHandle<()>{
    tokio::spawn(async move {
        loop{
            tokio::select!{
                maybe_event = user_rx.recv() => {
                    let Some(event) = maybe_event else { break };
                    if matches!(event.action, ACTION::CLOSE) {
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

async fn spawn_receiver_task(mut receiver: SplitStream<WebSocket>, shutdown: CancellationToken, mut user_tx: tokio::sync::mpsc::Sender<ServerEvent>, who: SocketAddr) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        
        // TODO

        let limiter = RateLimiter::direct(
            Quota::per_second(NonZeroU32::new(10).unwrap())
                .allow_burst(NonZeroU32::new(20).unwrap())
        );

        let mut limiter_trigger = 0;

        loop{
            tokio::select! {
                maybe_msg = receiver.next() => {
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
                                        match user_tx.send(
                                            ServerEvent { action: ACTION::CLOSE, content: "rate limit exceeded three times".to_owned() }
                                        ).await {
                                            Ok(()) => (),
                                            Err(_e) => {
                                                // TODO
                                            }
                                        };
                                    } else {
                                        match user_tx.send(
                                            ServerEvent { action: ACTION::RATELIMIT, content: "rate limit exceeded".to_owned() }
                                        ).await {
                                            Ok(()) => (),
                                            Err(_e) => {
                                                // TODO
                                            }
                                        };
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

    let shutdown = state.shutdown.child_token();
    
    // TODO
    // Each user will be subscribed to various rooms
    // Here, we need to take the broadcast channels from each room to which the user belongs,
    // and "merge" each of the broadcast channels into the mpsc so the handler listens on a single channel.
    let (user_tx, user_rx) = tokio::sync::mpsc::channel::<ServerEvent>(100);
    let test_tx = user_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let _ = test_tx.send(ServerEvent { action: ACTION::MESSAGE, content: "TESTING".to_owned() }).await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let _ = test_tx.send(ServerEvent { action: ACTION::MESSAGE, content: "TESTING2".to_owned() }).await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let _ = test_tx.send(ServerEvent { action: ACTION::CLOSE, content: "".to_owned() }).await;
    });

    let recv_task = spawn_receiver_task(receiver, shutdown.clone(), user_tx, who).await;

    let send_task = spawn_sender_task(sender, shutdown.clone(), user_rx, who).await;

    let _ = tokio::join!(send_task, recv_task);
}

async fn process_message(msg: Message, user_tx: &mut tokio::sync::mpsc::Sender<ServerEvent>, who: SocketAddr) -> ControlFlow<(), ()> {
    match msg {
        Message::Text(t) => {
            match serde_json::from_str::<ClientCommand>(&t) {
                Ok(cmd) => {
                    match cmd.action {
                        ACTION::CLOSE => {
                            user_tx.send(
                                ServerEvent {
                                    action: cmd.action,
                                    content: cmd.content
                                }
                            ).await.unwrap();
                        }
                        ACTION::ECHO => {
                            user_tx.send(
                                ServerEvent {
                                    action: cmd.action,
                                    content: cmd.content
                                }
                            ).await.unwrap();
                        }
                        ACTION::MESSAGE => {
                            println!("NEEDS IMPLEMENTED: {}", cmd)
                        }
                        ACTION::ERROR => {
                            // ERROR from the CLIENT
                        }
                        ACTION::RATELIMIT => {
                            user_tx.send(
                                ServerEvent {
                                    action: cmd.action,
                                    content: cmd.content
                                }
                            ).await.unwrap();
                        }
                    }
                }
                Err(e) => {
                    println!("ERR: {:?}", e);
                    if e.is_data() {
                        let _ = user_tx.send(ServerEvent {
                            action: ACTION::ERROR,
                            content: "invalid command".to_owned()
                        }).await;
                    } else if e.is_syntax() {
                        let _ = user_tx.send(ServerEvent {
                            action: ACTION::ERROR,
                            content: "malformed JSON".to_owned()
                        }).await;
                    } else if e.is_eof() {
                        let _ = user_tx.send(ServerEvent {
                            action: ACTION::ERROR,
                            content: "incomplete message".to_owned()
                        }).await;
                    } else {
                        let _ = user_tx.send(ServerEvent {
                            action: ACTION::ERROR,
                            content: "unknown error".to_owned()
                        }).await;
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
            if let Some(cf) = c {
                user_tx.send(
                    ServerEvent {
                        action: ACTION::CLOSE,
                        content: format!("client requested shutdown : {} {}", cf.code, cf.reason)
                    }
                ).await.unwrap();
            } else {
                user_tx.send(
                    ServerEvent {
                        action: ACTION::CLOSE,
                        content: "client requested shutdown without close frame.".to_owned()
                    }
                ).await.unwrap();
            }
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