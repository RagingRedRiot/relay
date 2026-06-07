use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use futures_util::SinkExt;
use futures_util::{
    StreamExt,
    stream::{SplitSink, SplitStream},
};
use governor::{Quota, RateLimiter};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::{net::SocketAddr, num::NonZeroU32, ops::ControlFlow};
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::StreamMap;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use uuid::Uuid;

use crate::attachment::{self, AttachmentHandle, Chunk};
use crate::control::ServerControl;
use crate::hub::{Hub, Subscription};
use crate::message::{MessageHandle, MessageResponse};
use crate::model::{EditUser, NewCredential, NewUser};
use crate::room::RoomHandle;
use crate::user::{UserHandle, UserResponse};
use crate::{
    app::AppState,
    auth::{AuthHandle, AuthResult},
    model::{ClientCommand, Password, ServerEvent},
};

// Binary chunk uploads get a higher rate limit than text commands (10/s): chunks
// are high-volume, and throughput is otherwise bounded by the global write
// semaphore and the per-upload actor's bounded channel.
const CHUNK_RATE_PER_SEC: u32 = 200;
const CHUNK_BURST: u32 = 400;

struct Handles {
    user_handle: UserHandle,
    room_handle: RoomHandle,
    message_handle: MessageHandle,
    control: ServerControl,
}

async fn send_close(sender: &mut SplitSink<WebSocket, Message>, who: SocketAddr) {
    tracing::debug!(who = %who, "sending close frame");
    if let Err(e) = sender
        .send(Message::Close(Some(CloseFrame {
            code: axum::extract::ws::close_code::NORMAL,
            reason: Utf8Bytes::from_static("Goodbye"),
        })))
        .await
    {
        // The peer is often already gone by the time we try to close; not an error.
        tracing::debug!(who = %who, error = %e, "could not send close frame (peer likely gone)");
    }
}

async fn spawn_sender_task(
    mut sender: SplitSink<WebSocket, Message>,
    shutdown: CancellationToken,
    mut user_rx: mpsc::Receiver<ServerEvent>,
    mut sub_rx: mpsc::Receiver<Subscription>,
    who: SocketAddr,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // The live room streams this session is subscribed to, merged into one
        // poll. Keyed by room_id so a join is an insert and a leave a remove.
        // Dropping this map on teardown drops every Receiver, so the hub's senders
        // self-prune -- no "remove me from every room" bookkeeping. `room_names`
        // mirrors the keys so a Lagged event can name the room in its Resync hint.
        let mut rooms: StreamMap<Uuid, BroadcastStream<Arc<ServerEvent>>> = StreamMap::new();
        let mut room_names: HashMap<Uuid, String> = HashMap::new();

        loop {
            tokio::select! {
                // 1. Outbound application events: RPC replies, downloads, Close.
                maybe_event = user_rx.recv() => {
                    let Some(event) = maybe_event else { break };
                    if write_event(&mut sender, event, who, &shutdown).await.is_break() {
                        break;
                    }
                }

                // 2. Subscription control from the receiver task: add/remove a
                //    room's live stream as the session joins/leaves.
                Some(sub) = sub_rx.recv() => {
                    match sub {
                        Subscription::Add { room_id, room_name, rx } => {
                            room_names.insert(room_id, room_name);
                            rooms.insert(room_id, BroadcastStream::new(rx));
                        }
                        Subscription::Remove { room_id } => {
                            rooms.remove(&room_id);
                            room_names.remove(&room_id);
                        }
                    }
                }

                // 3. Live fan-out from any subscribed room. An empty map just never
                //    fires this branch; select! waits on the others (no busy-loop).
                Some((room_id, item)) = rooms.next() => {
                    match item {
                        // Broadcast payloads are always plain JSON events (NewMessage,
                        // ...), never Close/AttachmentChunk, so serialize straight out.
                        Ok(event) => {
                            let text: Utf8Bytes = serde_json::to_string(&*event).unwrap().into();
                            if sender.send(Message::Text(text)).await.is_err() {
                                shutdown.cancel();
                                break;
                            }
                        }
                        // Fell behind the room's ring and dropped events: don't silently
                        // lose them -- tell the client to re-sync this room from history.
                        Err(BroadcastStreamRecvError::Lagged(_)) => {
                            if let Some(room_name) = room_names.get(&room_id) {
                                let hint = ServerEvent::Resync { room_name: room_name.clone() };
                                let text: Utf8Bytes = serde_json::to_string(&hint).unwrap().into();
                                if sender.send(Message::Text(text)).await.is_err() {
                                    shutdown.cancel();
                                    break;
                                }
                            }
                        }
                    }
                }

                _ = shutdown.cancelled() => {
                    send_close(&mut sender, who).await;
                    break
                }
            }
        }
    })
}

// Write one outbound application event to the socket, returning Break when the
// session should tear down (a Close request, or the socket dropped). Shared by the
// user_rx branch; the live-fan-out branch never carries Close/AttachmentChunk.
async fn write_event(
    sender: &mut SplitSink<WebSocket, Message>,
    event: ServerEvent,
    who: SocketAddr,
    shutdown: &CancellationToken,
) -> ControlFlow<()> {
    match event {
        ServerEvent::Close { .. } => {
            send_close(sender, who).await;
            shutdown.cancel();
            ControlFlow::Break(())
        }
        // Binary egress: a download chunk goes out as a framed binary frame --
        // [attachment_id 16B][seq u32 BE 4B][payload] -- not JSON, so the bytes skip
        // base64/array inflation. Same framing the upload path parses.
        ServerEvent::AttachmentChunk {
            attachment_id,
            seq,
            data,
        } => {
            let mut frame = Vec::with_capacity(20 + data.len());
            frame.extend_from_slice(attachment_id.as_bytes());
            frame.extend_from_slice(&(seq as u32).to_be_bytes());
            frame.extend_from_slice(&data);
            if sender.send(Message::Binary(frame.into())).await.is_err() {
                shutdown.cancel();
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
        other => {
            let text: Utf8Bytes = serde_json::to_string(&other).unwrap().into();
            if sender.send(Message::Text(text)).await.is_err() {
                shutdown.cancel();
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn spawn_receiver_task(
    mut receiver: SplitStream<WebSocket>,
    shutdown: CancellationToken,
    mut user_tx: tokio::sync::mpsc::Sender<ServerEvent>,
    auth_handle: AuthHandle, // AuthHandle is purposely separate from handles
    handles: Handles,
    open_signups: bool,
    max_chunk_bytes: usize,
    pool: PgPool,
    write_semaphore: Arc<Semaphore>,
    download_semaphore: Arc<Semaphore>,
    hub: Hub,
    sub_tx: mpsc::Sender<Subscription>,
    who: SocketAddr,
) -> tokio::task::JoinHandle<()> {
    // One span per connection. `user_id` starts empty and is recorded once the
    // prelude authenticates, so every event this task emits (here and down through
    // process_message) is attributable to the connection and, post-auth, the user.
    let span = tracing::info_span!("conn", %who, user_id = tracing::field::Empty);
    tokio::spawn(
        async move {
        // PRELUDE - PREAUTH'd
        let (user_id, server_event) = prelude(
            &mut receiver,
            &auth_handle,
            &handles.user_handle,
            &user_tx,
            open_signups,
        )
        .await;

        match server_event {
            ServerEvent::AuthOk { is_admin } => {
                tracing::Span::current().record("user_id", tracing::field::display(user_id));
                tracing::debug!("session authenticated");
                let _ = user_tx.send(ServerEvent::AuthOk { is_admin }).await;
            }
            // Anything but AuthOk results in shutdown
            ServerEvent::NoAuth => {
                let _ = user_tx.send(ServerEvent::NoAuth).await;
                let _ = user_tx
                    .send(ServerEvent::Close {
                        reason: "auth failed".to_owned(),
                    })
                    .await;
                return;
            }
            ServerEvent::UserCreated => {
                let _ = user_tx.send(ServerEvent::UserCreated).await;
                let _ = user_tx
                    .send(ServerEvent::Close {
                        reason: "user created".to_owned(),
                    })
                    .await;
                return;
            }
            ServerEvent::Failed => {
                let _ = user_tx.send(ServerEvent::Failed).await;
                let _ = user_tx
                    .send(ServerEvent::Close {
                        reason: "user creation failed".to_owned(),
                    })
                    .await;
                return;
            }
            _ => {
                let _ = user_tx
                    .send(ServerEvent::Close {
                        reason: "invalid command".to_owned(),
                    })
                    .await;
                return;
            }
        };

        let limiter = RateLimiter::direct(
            Quota::per_second(NonZeroU32::new(10).unwrap())
                .allow_burst(NonZeroU32::new(20).unwrap()),
        );

        let chunk_limiter = RateLimiter::direct(
            Quota::per_second(NonZeroU32::new(CHUNK_RATE_PER_SEC).unwrap())
                .allow_burst(NonZeroU32::new(CHUNK_BURST).unwrap()),
        );

        let mut attachments: HashMap<Uuid, AttachmentHandle> = HashMap::new();

        let mut limiter_count = 0;

        // Register this session under its user so it can be reached cross-session
        // (e.g. subscribed to a room it's just been approved into). The guard
        // deregisters on drop -- it lives until this task ends.
        let _session = hub.register_session(user_id, sub_tx.clone());

        // Subscribe to live events for every room the user already belongs to, so a
        // message posted while they're connected reaches this session without
        // polling. Joins/leaves during the session adjust this set incrementally.
        subscribe_existing_rooms(&pool, &hub, &sub_tx, user_id).await;

        // AUTH'd
        loop {
            tokio::select! {
                maybe_msg = receiver.next() => {
                    match maybe_msg {
                        Some(Ok(msg)) => match msg {
                            Message::Text(t) => match limiter.check() {
                                Ok(()) => {
                                    if process_message(
                                        t,
                                        &mut user_tx,
                                        &handles,
                                        open_signups,
                                        max_chunk_bytes,
                                        user_id,
                                        &pool,
                                        &download_semaphore,
                                        &hub,
                                        &sub_tx,
                                    )
                                        .await
                                        .is_break()
                                    {
                                        shutdown.cancel();
                                        break;
                                    }
                                }
                                Err(_) => {
                                    limiter_count += 1;

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

                            Message::Binary(d) => match chunk_limiter.check() {
                                Ok(()) => {
                                    process_binary(
                                        d.to_vec(),
                                        &mut attachments,
                                        &pool,
                                        &write_semaphore,
                                        &user_tx,
                                        user_id,
                                        &hub,
                                        &shutdown,
                                    )
                                    .await;
                                }
                                Err(_) => {
                                    let _ = user_tx.send(
                                        ServerEvent::RateLimit { error: "rate limit exceeded".to_owned() }
                                    ).await;
                                }
                            }

                            Message::Close(c) => {
                                let reason = if let Some(cf) = c {
                                    format!("client requested shutdown : {} {}", cf.code, cf.reason)
                                } else {
                                    "client requested shutdown without close frame.".to_owned()
                                };
                                let _ = user_tx.send(ServerEvent::Close { reason }).await;
                                break;
                            }

                            // axum auto-replies to Ping; nothing to do for Ping/Pong.
                            Message::Ping(_) | Message::Pong(_) => {}
                        },
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

        tracing::debug!("session closed");
        }
        .instrument(span),
    )
}

pub(crate) async fn handle_socket(socket: WebSocket, state: AppState, who: SocketAddr) {
    let (sender, receiver) = socket.split();

    let shutdown: CancellationToken = state.shutdown.child_token();
    let auth_handle: AuthHandle = state.auth_handle.clone();
    let open_signups: bool = state.config.open_signups;
    let max_chunk_bytes: usize = state.config.max_chunk_bytes;
    let handles = Handles {
        user_handle: state.user_handle.clone(),
        room_handle: state.room_handle.clone(),
        message_handle: state.message_handle.clone(),
        control: state.control.clone(),
    };
    let pool = state.pool.clone();
    let write_semaphore = state.write_semaphore.clone();
    let download_semaphore = state.download_semaphore.clone();
    let hub = state.hub.clone();

    // The session's single outbound channel: RPC replies and downloads flow from the
    // receiver task into here, drained by the sender task to the socket.
    let (user_tx, user_rx) = tokio::sync::mpsc::channel::<ServerEvent>(100);

    // Subscription control: the receiver task (which sees join/leave) tells the
    // sender task (which owns the merged live-event StreamMap) what to subscribe to.
    let (sub_tx, sub_rx) = mpsc::channel::<Subscription>(32);

    let recv_task = spawn_receiver_task(
        receiver,
        shutdown.clone(),
        user_tx,
        auth_handle,
        handles,
        open_signups,
        max_chunk_bytes,
        pool,
        write_semaphore,
        download_semaphore,
        hub,
        sub_tx,
        who,
    )
    .await;

    let send_task = spawn_sender_task(sender, shutdown.clone(), user_rx, sub_rx, who).await;

    let _ = tokio::join!(send_task, recv_task);
}

// Subscribe the session to live events for every room the user is already in.
// Best-effort: a query error just means no live delivery until the next
// join/reconnect; the durable read paths are unaffected.
async fn subscribe_existing_rooms(
    pool: &PgPool,
    hub: &Hub,
    sub_tx: &mpsc::Sender<Subscription>,
    user_id: Uuid,
) {
    let rooms = sqlx::query_as::<_, (Uuid, String)>(
        "SELECT r.room_id, r.room_name FROM memberships mb
            JOIN rooms r ON r.room_id = mb.room_id
            WHERE mb.user_id = $1",
    )
    .bind(user_id)
    .fetch_all(pool)
    .await;

    if let Ok(rooms) = rooms {
        for (room_id, room_name) in rooms {
            let rx = hub.subscribe(room_id);
            let _ = sub_tx
                .send(Subscription::Add {
                    room_id,
                    room_name,
                    rx,
                })
                .await;
        }
    }
}

// Subscribe the session to one room the caller just joined, resolving its id and
// canonical name by (normalized) name.
async fn subscribe_room(
    pool: &PgPool,
    hub: &Hub,
    sub_tx: &mpsc::Sender<Subscription>,
    room_name: &str,
) {
    if let Ok(Some((room_id, canonical))) = sqlx::query_as::<_, (Uuid, String)>(
        "SELECT room_id, room_name FROM rooms WHERE LOWER(room_name) = LOWER(trim_ws($1))",
    )
    .bind(room_name)
    .fetch_optional(pool)
    .await
    {
        let rx = hub.subscribe(room_id);
        let _ = sub_tx
            .send(Subscription::Add {
                room_id,
                room_name: canonical,
                rx,
            })
            .await;
    }
}

// Drop the session's subscription to one room the caller just left.
async fn unsubscribe_room(pool: &PgPool, sub_tx: &mpsc::Sender<Subscription>, room_name: &str) {
    if let Ok(Some(room_id)) = sqlx::query_scalar::<_, Uuid>(
        "SELECT room_id FROM rooms WHERE LOWER(room_name) = LOWER(trim_ws($1))",
    )
    .bind(room_name)
    .fetch_optional(pool)
    .await
    {
        let _ = sub_tx.send(Subscription::Remove { room_id }).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_message(
    t: Utf8Bytes,
    user_tx: &mut tokio::sync::mpsc::Sender<ServerEvent>,
    handles: &Handles,
    open_signups: bool,
    max_chunk_bytes: usize,
    user_id: Uuid,
    pool: &PgPool,
    download_semaphore: &Arc<Semaphore>,
    hub: &Hub,
    sub_tx: &mpsc::Sender<Subscription>,
) -> ControlFlow<(), ()> {
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
            // Server lifecycle — admin only. Admin authority is re-checked here,
            // just-in-time, never trusted from connect time. A non-admin gets the
            // same generic Failed as any other denied action. On success we send a
            // best-effort Success ack, then signal the supervisor in `main`; the
            // run-shutdown it triggers tears this very session down with the rest, so
            // the socket closes shortly after.
            ClientCommand::RestartServer => {
                if handles.user_handle.is_admin(user_id).await {
                    tracing::info!(target: crate::logging::AUDIT, actor = %user_id, "server restart requested");
                    let _ = user_tx.send(ServerEvent::Success).await;
                    handles.control.restart().await;
                } else {
                    tracing::warn!(target: crate::logging::AUDIT, actor = %user_id, "server restart denied: caller is not an admin");
                    let _ = user_tx.send(ServerEvent::Failed).await;
                }
            }

            ClientCommand::ShutdownServer => {
                if handles.user_handle.is_admin(user_id).await {
                    tracing::info!(target: crate::logging::AUDIT, actor = %user_id, "server shutdown requested");
                    let _ = user_tx.send(ServerEvent::Success).await;
                    handles.control.shutdown().await;
                } else {
                    tracing::warn!(target: crate::logging::AUDIT, actor = %user_id, "server shutdown denied: caller is not an admin");
                    let _ = user_tx.send(ServerEvent::Failed).await;
                }
            }

            // Authentication Request — already authed at this point, ignore
            ClientCommand::Auth { .. } => {}
            // Echo to Client
            ClientCommand::Echo { string } => {
                user_tx.send(ServerEvent::Echo { string }).await.unwrap();
            }

            ClientCommand::SendMessage {
                room_name,
                content,
                attachments,
            } => {
                // Resolve the caller-supplied room name to a UUID for the
                // internal message actor (which keys everything by room_id).
                let room_id = match sqlx::query_scalar::<_, Uuid>(
                    "SELECT room_id FROM rooms WHERE LOWER(room_name) = LOWER(trim_ws($1))",
                )
                .bind(&room_name)
                .fetch_optional(pool)
                .await
                {
                    Ok(Some(id)) => id,
                    Ok(None) => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                        return ControlFlow::Continue(());
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "send_message: room name lookup failed");
                        let _ = user_tx.send(ServerEvent::Failed).await;
                        return ControlFlow::Continue(());
                    }
                };
                // The sender is the authenticated session user, never taken from
                // the client. Membership auth happens inside the actor's
                // transaction. On success the client gets the new message_id plus
                // an attachment_id per declared file to stream chunks against.
                match handles
                    .message_handle
                    .send_message(user_id, room_id, content, attachments)
                    .await
                {
                    MessageResponse::MessageCreated {
                        message_id,
                        attachment_ids,
                        message,
                    } => {
                        let _ = user_tx
                            .send(ServerEvent::MessageCreated {
                                message_id,
                                attachment_ids,
                                message,
                            })
                            .await;
                    }
                    // Failed (or the reaction-only Success, which SendMessage never
                    // returns) collapses to a generic failure for the client. The
                    // detailed cause is already logged in the message actor.
                    _ => {
                        tracing::debug!("send_message: request failed");
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::DownloadAttachment { attachment_id } => {
                // Spawn a disposable streaming task; it runs its own room-membership
                // auth and streams the chunks back as binary frames. We don't await
                // it, so the receive loop stays responsive to other commands while a
                // download is in flight.
                attachment::download(
                    attachment_id,
                    user_id,
                    pool.clone(),
                    download_semaphore.clone(),
                    user_tx.clone(),
                );
            }

            ClientCommand::GetMaxChunkSize => {
                // Static, per-connection answer: the configured payload cap the
                // transport was pinned to at upgrade. No auth or DB needed.
                let _ = user_tx
                    .send(ServerEvent::MaxChunkSize {
                        bytes: max_chunk_bytes,
                    })
                    .await;
            }

            ClientCommand::GetSignupStatus => {
                // Public server setting; also answerable in the prelude (pre-auth).
                let _ = user_tx
                    .send(ServerEvent::SignupStatus { open_signups })
                    .await;
            }

            ClientCommand::AddReaction { message_id, emoji } => {
                // The reactor is the authenticated session user, never client-
                // supplied. Membership auth happens inside the actor; an
                // idempotent re-add still reports Success.
                let response = handles
                    .message_handle
                    .add_reaction(user_id, message_id, emoji)
                    .await;
                match response {
                    MessageResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    _ => {
                        tracing::debug!("add_reaction: request failed");
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::RemoveReaction { message_id, emoji } => {
                let response = handles
                    .message_handle
                    .remove_reaction(user_id, message_id, emoji)
                    .await;
                match response {
                    MessageResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    _ => {
                        tracing::debug!("remove_reaction: request failed");
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::DeleteMessage { message_id } => {
                // The deleter is the authenticated session user; sender-or-admin auth
                // happens inside the actor, which also fans out the MessageRemoved on
                // success. A forbidden or unknown message collapses to a generic Failed.
                let response = handles
                    .message_handle
                    .delete_message(user_id, message_id)
                    .await;
                match response {
                    MessageResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    _ => {
                        tracing::debug!("delete_message: request failed");
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::GetMessages {
                room_name,
                before,
                limit,
            } => {
                // Membership auth and name resolution happen inside the actor; a
                // non-member or unknown room collapses to a generic failure.
                let response = handles
                    .message_handle
                    .get_messages(user_id, room_name, before, limit)
                    .await;
                match response {
                    MessageResponse::History {
                        room_name,
                        messages,
                    } => {
                        let _ = user_tx
                            .send(ServerEvent::MessageHistory {
                                room_name,
                                messages,
                            })
                            .await;
                    }
                    _ => {
                        tracing::debug!("get_messages: request failed");
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::MarkRead {
                room_name,
                up_to_message_id,
            } => {
                let response = handles
                    .message_handle
                    .mark_read(user_id, room_name, up_to_message_id)
                    .await;
                match response {
                    MessageResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    _ => {
                        tracing::debug!("mark_read: request failed");
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::GetUnreadSummary => {
                match handles.message_handle.get_unread_summary(user_id).await {
                    MessageResponse::UnreadSummary { rooms } => {
                        let _ = user_tx.send(ServerEvent::UnreadSummary { rooms }).await;
                    }
                    _ => {
                        tracing::debug!("get_unread_summary: request failed");
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::Error { .. } => {
                // ERROR from the CLIENT
            }

            ClientCommand::NewUser {
                username,
                password,
                first_name,
                last_name,
                alias,
            } => {
                // When signups are closed, only admins may create users.
                // Validate admin rights just-in-time for this request
                // rather than trusting a value cached at connect time.
                let allowed = open_signups || handles.user_handle.is_admin(user_id).await;
                if !allowed {
                    tracing::warn!(target: crate::logging::AUDIT, actor = %user_id, "user creation denied: signups closed and caller is not an admin");
                    let _ = user_tx.send(ServerEvent::Failed).await;
                } else {
                    match new_user(
                        &handles.user_handle,
                        username,
                        password,
                        first_name,
                        last_name,
                        alias,
                    )
                    .await
                    {
                        UserResponse::UserCreated { .. } => {
                            let _ = user_tx.send(ServerEvent::UserCreated).await;
                        }
                        _ => {
                            tracing::debug!("new_user: request failed");
                            let _ = user_tx.send(ServerEvent::Failed).await;
                        }
                    }
                }
            }

            ClientCommand::EditUser {
                target_username,
                username,
                first_name,
                last_name,
                alias,
            } => {
                match handles
                    .user_handle
                    .edit_user(
                        user_id,
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
                    UserResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::GetUserByUsername { username } => {
                match handles.user_handle.get_user_by_username(&username).await {
                    UserResponse::UserInfo { user_info } => {
                        let _ = user_tx
                            .send(ServerEvent::UserInfo {
                                first_name: user_info.first_name,
                                last_name: user_info.last_name,
                                alias: user_info.alias,
                                username: user_info.username,
                                created_at: user_info.created_at,
                            })
                            .await;
                    }
                    UserResponse::NoUserExists => {
                        let _ = user_tx.send(ServerEvent::NoUserExists).await;
                    }
                    _ => {
                        tracing::debug!("get_user_by_username: request failed");
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::DeleteUser { target_username } => {
                match handles
                    .user_handle
                    .delete_user(&target_username, user_id)
                    .await
                {
                    UserResponse::UserDeleted { is_self } => {
                        if is_self {
                            let _ = user_tx.send(ServerEvent::Success).await;
                            let _ = user_tx
                                .send(ServerEvent::Close {
                                    reason: "client closed".to_owned(),
                                })
                                .await;
                        } else {
                            let _ = user_tx.send(ServerEvent::Success).await;
                        }
                    }
                    _ => {
                        tracing::debug!("delete_user: request failed");
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::UpdatePassword {
                current_password,
                new_password,
            } => {
                match handles
                    .user_handle
                    .update_password(user_id, current_password, new_password)
                    .await
                {
                    UserResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::ResetPassword {
                target_username,
                new_password,
            } => {
                match handles
                    .user_handle
                    .reset_password(user_id, target_username, new_password)
                    .await
                {
                    UserResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::Promote { target_username } => {
                match handles.user_handle.promote(user_id, &target_username).await {
                    UserResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    UserResponse::NoChange => {
                        let _ = user_tx.send(ServerEvent::NoChange).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::Demote { target_username } => {
                match handles.user_handle.demote(user_id, &target_username).await {
                    UserResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    UserResponse::NoChange => {
                        let _ = user_tx.send(ServerEvent::NoChange).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::NewRoom {
                room_name,
                is_public,
                is_discoverable,
            } => {
                match handles
                    .room_handle
                    .new_room(crate::model::NewRoom {
                        room_name,
                        owner_id: user_id,
                        is_public: is_public.unwrap_or(false),
                        is_discoverable: is_discoverable.unwrap_or(false),
                    })
                    .await
                {
                    crate::room::RoomResponse::RoomCreated { .. } => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::AddRoomOwner {
                room_name,
                new_owner_username,
            } => {
                match handles
                    .room_handle
                    .add_room_owner(user_id, room_name, new_owner_username)
                    .await
                {
                    crate::room::RoomResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    crate::room::RoomResponse::NoChange => {
                        let _ = user_tx.send(ServerEvent::NoChange).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::SetRoomName {
                current_name,
                new_name,
            } => {
                match handles
                    .room_handle
                    .set_room_name(user_id, current_name, new_name)
                    .await
                {
                    crate::room::RoomResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::GetRoomMembership { room_name } => {
                match handles
                    .room_handle
                    .get_room_members(user_id, room_name)
                    .await
                {
                    crate::room::RoomResponse::RoomMembership { members } => {
                        let _ = user_tx.send(ServerEvent::RoomMembers { members }).await;
                    }
                    crate::room::RoomResponse::NoRoomExists => {
                        let _ = user_tx.send(ServerEvent::NoRoomExists).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::GetRoom { room_name } => {
                match handles.room_handle.get_room(user_id, room_name).await {
                    crate::room::RoomResponse::RoomInfo {
                        room_name,
                        is_public,
                        is_discoverable,
                    } => {
                        let _ = user_tx
                            .send(ServerEvent::RoomInfo {
                                room_name,
                                is_public,
                                is_discoverable,
                            })
                            .await;
                    }
                    crate::room::RoomResponse::NoRoomExists => {
                        let _ = user_tx.send(ServerEvent::NoRoomExists).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::JoinRoom { room_name } => {
                match handles
                    .room_handle
                    .join_room(user_id, room_name.clone())
                    .await
                {
                    crate::room::RoomResponse::Success => {
                        // Now a member: start receiving the room's live events.
                        subscribe_room(pool, hub, sub_tx, &room_name).await;
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    crate::room::RoomResponse::JoinRequested => {
                        let _ = user_tx.send(ServerEvent::JoinRequested).await;
                    }
                    crate::room::RoomResponse::NoChange => {
                        let _ = user_tx.send(ServerEvent::NoChange).await;
                    }
                    crate::room::RoomResponse::NoRoomExists => {
                        let _ = user_tx.send(ServerEvent::NoRoomExists).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::LeaveRoom { room_name } => {
                match handles
                    .room_handle
                    .leave_room(user_id, room_name.clone())
                    .await
                {
                    crate::room::RoomResponse::Success => {
                        // No longer a member: stop receiving the room's live events.
                        unsubscribe_room(pool, sub_tx, &room_name).await;
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    crate::room::RoomResponse::NoChange => {
                        let _ = user_tx.send(ServerEvent::NoChange).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::GetMyJoinRequests => {
                match handles.room_handle.get_my_join_requests(user_id).await {
                    crate::room::RoomResponse::MyJoinRequests { rooms } => {
                        let _ = user_tx.send(ServerEvent::MyJoinRequests { rooms }).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::GetIncomingJoinRequests => {
                match handles
                    .room_handle
                    .get_incoming_join_requests(user_id)
                    .await
                {
                    crate::room::RoomResponse::IncomingJoinRequests { requests } => {
                        let _ = user_tx
                            .send(ServerEvent::IncomingJoinRequests { requests })
                            .await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::ApproveJoinRequest {
                room_name,
                requester_username,
            } => {
                match handles
                    .room_handle
                    .approve_join_request(user_id, room_name, requester_username)
                    .await
                {
                    crate::room::RoomResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    crate::room::RoomResponse::NoChange => {
                        let _ = user_tx.send(ServerEvent::NoChange).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::RejectJoinRequest {
                room_name,
                requester_username,
            } => {
                match handles
                    .room_handle
                    .reject_join_request(user_id, room_name, requester_username)
                    .await
                {
                    crate::room::RoomResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    crate::room::RoomResponse::NoChange => {
                        let _ = user_tx.send(ServerEvent::NoChange).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::InviteToRoom {
                room_name,
                invitee_username,
            } => {
                match handles
                    .room_handle
                    .invite_to_room(user_id, room_name, invitee_username)
                    .await
                {
                    crate::room::RoomResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    crate::room::RoomResponse::NoChange => {
                        let _ = user_tx.send(ServerEvent::NoChange).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::GetMyInvites => {
                match handles.room_handle.get_my_invites(user_id).await {
                    crate::room::RoomResponse::MyInvites { rooms } => {
                        let _ = user_tx.send(ServerEvent::MyInvites { rooms }).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::AcceptInvite { room_name } => {
                match handles
                    .room_handle
                    .accept_invite(user_id, room_name.clone())
                    .await
                {
                    crate::room::RoomResponse::Success => {
                        // Joined via the invite: subscribe to the room's live events.
                        subscribe_room(pool, hub, sub_tx, &room_name).await;
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    crate::room::RoomResponse::NoChange => {
                        let _ = user_tx.send(ServerEvent::NoChange).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }

            ClientCommand::DeclineInvite { room_name } => {
                match handles.room_handle.decline_invite(user_id, room_name).await {
                    crate::room::RoomResponse::Success => {
                        let _ = user_tx.send(ServerEvent::Success).await;
                    }
                    crate::room::RoomResponse::NoChange => {
                        let _ = user_tx.send(ServerEvent::NoChange).await;
                    }
                    _ => {
                        let _ = user_tx.send(ServerEvent::Failed).await;
                    }
                }
            }
        },
        Err(e) => {
            // Client-caused: malformed/oversized/incomplete input. Not a server
            // fault, so debug, not warn. The client still gets a generic reason.
            tracing::debug!(error = %e, "rejected undecodable client command");
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
            };
        }
    }
    ControlFlow::Continue(())
}

// Route an incoming binary chunk frame to its upload actor.
// Spawns an actor on the first chunk for an attachment or on resumed upload.
//
// Frame layout:
// [attachment_id: 16B][seq: u32 big-endian: 4B][payload...]
#[allow(clippy::too_many_arguments)]
async fn process_binary(
    data: Vec<u8>,
    attachments: &mut HashMap<Uuid, AttachmentHandle>,
    pool: &PgPool,
    write_semaphore: &Arc<Semaphore>,
    user_tx: &tokio::sync::mpsc::Sender<ServerEvent>,
    user_id: Uuid,
    hub: &Hub,
    shutdown: &CancellationToken,
) {
    const HEADER_LEN: usize = attachment::CHUNK_HEADER_LEN;

    // Need a full header (20-Bytes) plus at least one payload byte.
    // Generic error; specifics stay server-side.
    if data.len() <= HEADER_LEN {
        // Client-caused: a frame too short to hold the header plus a payload byte.
        tracing::debug!(len = data.len(), "process_binary: undersized chunk frame");
        let _ = user_tx
            .send(ServerEvent::Error {
                error: "invalid chunk".to_owned(),
            })
            .await;
        return;
    }

    // Extract the attachment_id from header
    let attachment_id = match Uuid::from_slice(&data[0..16]) {
        Ok(id) => id,
        Err(_) => {
            let _ = user_tx
                .send(ServerEvent::Error {
                    error: "invalid chunk".to_owned(),
                })
                .await;
            return;
        }
    };

    // Extract the sequence identifier from the heahder
    let seq = u32::from_be_bytes([data[16], data[17], data[18], data[19]]) as i32;

    // Check if a live actor is running in this session for the attachment.
    // Uses the hashmap attachments to identify already spawned actors.
    // The payload is sliced fresh from the owned `data` each time so the borrow
    // checker doesn't have to reason about a moved-then-recovered value.
    let delivered = match attachments.get(&attachment_id) {
        Some(handle) => handle
            .route(Chunk {
                seq,
                data: data[HEADER_LEN..].to_vec(),
            })
            .await
            .is_ok(),
        None => false,
    };

    // Confirmed the actor already is live and received the chunk
    if delivered {
        return;
    }

    // No live actor. Drop any dead entry and fall through
    // to verify ownership and (re)spawn -- the resume
    // path. remove() on an absent key is a harmless no-op.
    attachments.remove(&attachment_id);

    // Spawn-on-first-chunk / resume: confirm the caller is the sender of the
    // attachment's message and fetch the metadata needed to detect completion. One
    // ownership check per attachment, not per chunk.
    let meta = sqlx::query_as::<_, (i32, i64, Vec<u8>, String, bool)>(
        "SELECT a.chunk_count, a.size_bytes, a.content_sha256, a.content_type, a.is_complete
            FROM message_attachments a
            JOIN messages m ON m.message_id = a.message_id
            WHERE a.attachment_id = $1 AND m.sender_id = $2",
    )
    .bind(attachment_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await;

    let (chunk_count, size_bytes, content_sha256, content_type, is_complete) = match meta {
        Ok(Some(row)) => row,
        Ok(None) => {
            let _ = user_tx
                .send(ServerEvent::Error {
                    error: "invalid chunk".to_owned(),
                })
                .await;
            return;
        }
        Err(e) => {
            tracing::error!(error = %e, %attachment_id, "process_binary: attachment metadata lookup failed");
            let _ = user_tx.send(ServerEvent::Failed).await;
            return;
        }
    };

    // Already finished: nothing to upload, just re-affirm completion.
    if is_complete {
        let _ = user_tx
            .send(ServerEvent::AttachmentComplete { attachment_id })
            .await;
        return;
    }

    let handle = attachment::spawn(
        attachment_id,
        chunk_count,
        size_bytes,
        content_sha256,
        content_type,
        pool.clone(),
        write_semaphore.clone(),
        user_tx.clone(),
        hub.clone(),
        shutdown.clone(),
    );

    // Hand off the triggering chunk

    let _ = handle
        .route(Chunk {
            seq,
            data: data[HEADER_LEN..].to_vec(),
        })
        .await;

    // Keep the handle for subsequent chunks
    attachments.insert(attachment_id, handle);
}

async fn new_user(
    user_handle: &UserHandle,
    username: String,
    password: Password,
    first_name: Option<String>,
    last_name: Option<String>,
    alias: Option<String>,
) -> UserResponse {
    user_handle
        .new_user(
            NewUser {
                username: username.to_owned(),
                first_name,
                last_name,
                alias,
            },
            NewCredential { password },
        )
        .await
}

// Largest number of pre-auth frames a client may send before it must Auth/NewUser.
// Only GetSignupStatus is non-terminal, so this caps a flood of signup-status
// queries on an unauthenticated socket, where the per-session limiter isn't active.
const MAX_PRELUDE_FRAMES: u32 = 16;

async fn prelude(
    receiver: &mut SplitStream<WebSocket>,
    auth_handle: &AuthHandle,
    user_handle: &UserHandle,
    user_tx: &mpsc::Sender<ServerEvent>,
    open_signups: bool,
) -> (Uuid, ServerEvent) {
    // The prelude loops only for a GetSignupStatus query (answered, then it keeps
    // waiting). Auth / NewUser / any other frame returns a terminal outcome and ends
    // it, exactly as before — so a first Auth/NewUser frame behaves unchanged.
    let mut frames: u32 = 0;
    loop {
        frames += 1;
        if frames > MAX_PRELUDE_FRAMES {
            tracing::debug!("prelude frame cap exceeded without auth");
            return (Uuid::nil(), ServerEvent::NoAuth);
        }

        let maybe_auth = receiver.next().await;
        return match maybe_auth {
            Some(Ok(Message::Text(t))) => {
                match serde_json::from_str::<ClientCommand>(&t) {
                    // Pre-auth query: answer and keep waiting for Auth/NewUser.
                    Ok(ClientCommand::GetSignupStatus) => {
                        let _ = user_tx
                            .send(ServerEvent::SignupStatus { open_signups })
                            .await;
                        continue;
                    }
                    Ok(ClientCommand::Auth { username, password }) => {
                        // Clone the username for audit logging; authenticate consumes it.
                        match auth_handle.authenticate(username.clone(), password).await {
                            AuthResult::Ok { user_id } => {
                                tracing::info!(target: crate::logging::AUDIT, %user_id, username = %username, "login succeeded");
                                let is_admin = user_handle.is_admin(user_id).await;
                                (user_id, ServerEvent::AuthOk { is_admin })
                            }
                            AuthResult::Failed => {
                                tracing::warn!(target: crate::logging::AUDIT, username = %username, "login failed");
                                (Uuid::nil(), ServerEvent::NoAuth)
                            }
                            // Server-side fault, not a credential rejection: already
                            // logged at error! in the auth actor, so don't audit it
                            // as a failed login.
                            AuthResult::Error => (Uuid::nil(), ServerEvent::NoAuth),
                        }
                    }
                    // Unauthenticated user creation is only allowed when open
                    // signups are enabled in the config; otherwise reject it.
                    Ok(ClientCommand::NewUser { username, .. }) if !open_signups => {
                        tracing::warn!(target: crate::logging::AUDIT, username = %username, "signup rejected: open signups disabled");
                        (Uuid::nil(), ServerEvent::NoAuth)
                    }
                    Ok(ClientCommand::NewUser {
                        username,
                        password,
                        first_name,
                        last_name,
                        alias,
                    }) => {
                        // Clone the username for audit logging; new_user consumes it.
                        match new_user(
                            user_handle,
                            username.clone(),
                            password,
                            first_name,
                            last_name,
                            alias,
                        )
                        .await
                        {
                            // Account-creation audit happens in the user actor (the
                            // single point both signup and authenticated paths hit).
                            UserResponse::UserCreated { user_id } => {
                                (user_id, ServerEvent::UserCreated)
                            }
                            UserResponse::Failed => {
                                // Detailed cause is logged in the user actor; this is just the prelude outcome.
                                tracing::debug!(username = %username, "open-signup user creation failed");
                                (Uuid::nil(), ServerEvent::Failed)
                            }
                            _ => {
                                tracing::debug!(username = %username, "open-signup user creation returned unexpected response");
                                (Uuid::nil(), ServerEvent::Failed)
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "undecodable command during prelude");
                        (Uuid::nil(), ServerEvent::NoAuth)
                    }
                    _ => (Uuid::nil(), ServerEvent::NoAuth),
                }
            }
            Some(Err(e)) => {
                tracing::debug!(error = %e, "websocket error during prelude");
                (Uuid::nil(), ServerEvent::NoAuth)
            }
            _ => (Uuid::nil(), ServerEvent::NoAuth),
        };
    }
}
