use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::hub::Hub;
use crate::model::{
    AttachmentSummary, MessageHistoryItem, NewMessageAttachment, ReactionSummary, RoomUnread,
    ServerEvent,
};

// History page size when the client doesn't ask, and the hard cap when it asks for
// more -- bounds the response so one request can't pull an unbounded backlog.
const DEFAULT_HISTORY_LIMIT: i64 = 50;
const MAX_HISTORY_LIMIT: i64 = 100;

pub enum MessageRequest {
    SendMessage {
        sender_id: Uuid,
        room_id: Uuid,
        content: String,
        attachments: Vec<NewMessageAttachment>,
        tx: oneshot::Sender<MessageResponse>,
    },
    AddReaction {
        user_id: Uuid,
        message_id: Uuid,
        emoji: String,
        tx: oneshot::Sender<MessageResponse>,
    },
    RemoveReaction {
        user_id: Uuid,
        message_id: Uuid,
        emoji: String,
        tx: oneshot::Sender<MessageResponse>,
    },
    GetMessages {
        user_id: Uuid,
        room_name: String,
        before: Option<Uuid>,
        limit: Option<u32>,
        tx: oneshot::Sender<MessageResponse>,
    },
    MarkRead {
        user_id: Uuid,
        room_name: String,
        up_to_message_id: Uuid,
        tx: oneshot::Sender<MessageResponse>,
    },
    GetUnreadSummary {
        user_id: Uuid,
        tx: oneshot::Sender<MessageResponse>,
    },
}

pub enum MessageResponse {
    MessageCreated {
        message_id: Uuid,
        // One id per declared attachment, in declaration order, so the client can
        // key each file's chunk stream. Empty when the message had no attachments.
        attachment_ids: Vec<Uuid>,
        // The canonical form of the new message, mirrored to the client so its ack
        // matches the live NewMessage broadcast.
        message: MessageHistoryItem,
    },
    // A reaction add/remove that succeeded (including the idempotent no-op cases:
    // re-adding an existing reaction or removing one that isn't there).
    Success,
    // One page of room history, newest first.
    History {
        room_name: String,
        messages: Vec<MessageHistoryItem>,
    },
    // Per-room unread counts for the caller.
    UnreadSummary {
        rooms: Vec<RoomUnread>,
    },
    Failed,
}

#[derive(Clone)]
pub struct MessageHandle {
    sender: mpsc::Sender<MessageRequest>,
}

impl MessageHandle {
    // Persist a message (and any declared attachments) on behalf of `sender_id`,
    // the authenticated caller -- never a client-supplied id.
    pub async fn send_message(
        &self,
        sender_id: Uuid,
        room_id: Uuid,
        content: String,
        attachments: Vec<NewMessageAttachment>,
    ) -> MessageResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(MessageRequest::SendMessage {
                sender_id,
                room_id,
                content,
                attachments,
                tx,
            })
            .await
            .is_err()
        {
            return MessageResponse::Failed;
        }

        rx.await.unwrap_or(MessageResponse::Failed)
    }

    // Add `emoji` as a reaction from `user_id` to `message_id`. Membership is
    // checked in the actor; `user_id` is the authenticated caller, never client-
    // supplied. Re-adding the same emoji is an idempotent success.
    pub async fn add_reaction(
        &self,
        user_id: Uuid,
        message_id: Uuid,
        emoji: String,
    ) -> MessageResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(MessageRequest::AddReaction {
                user_id,
                message_id,
                emoji,
                tx,
            })
            .await
            .is_err()
        {
            return MessageResponse::Failed;
        }

        rx.await.unwrap_or(MessageResponse::Failed)
    }

    // Remove `user_id`'s `emoji` reaction from `message_id`. Removing a reaction
    // that isn't there is an idempotent success.
    pub async fn remove_reaction(
        &self,
        user_id: Uuid,
        message_id: Uuid,
        emoji: String,
    ) -> MessageResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(MessageRequest::RemoveReaction {
                user_id,
                message_id,
                emoji,
                tx,
            })
            .await
            .is_err()
        {
            return MessageResponse::Failed;
        }

        rx.await.unwrap_or(MessageResponse::Failed)
    }

    // Fetch one page of `room_name`'s history on behalf of `user_id`, the
    // authenticated caller. Membership is checked in the actor. `before` is a
    // keyset cursor (a message_id); `limit` is clamped server-side.
    pub async fn get_messages(
        &self,
        user_id: Uuid,
        room_name: String,
        before: Option<Uuid>,
        limit: Option<u32>,
    ) -> MessageResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(MessageRequest::GetMessages {
                user_id,
                room_name,
                before,
                limit,
                tx,
            })
            .await
            .is_err()
        {
            return MessageResponse::Failed;
        }

        rx.await.unwrap_or(MessageResponse::Failed)
    }

    // Advance `user_id`'s read watermark in `room_name` to `up_to_message_id`.
    // Membership is checked in the actor; the advance is forward-only and
    // idempotent.
    pub async fn mark_read(
        &self,
        user_id: Uuid,
        room_name: String,
        up_to_message_id: Uuid,
    ) -> MessageResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(MessageRequest::MarkRead {
                user_id,
                room_name,
                up_to_message_id,
                tx,
            })
            .await
            .is_err()
        {
            return MessageResponse::Failed;
        }

        rx.await.unwrap_or(MessageResponse::Failed)
    }

    // Unread counts across every room `user_id` belongs to.
    pub async fn get_unread_summary(&self, user_id: Uuid) -> MessageResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(MessageRequest::GetUnreadSummary { user_id, tx })
            .await
            .is_err()
        {
            return MessageResponse::Failed;
        }

        rx.await.unwrap_or(MessageResponse::Failed)
    }
}

pub async fn spawn(shutdown: CancellationToken, pool: PgPool, hub: Hub) -> MessageHandle {
    // MESSAGE ACTOR COMMUNICATION CHANNELS
    let (tx, mut rx) = mpsc::channel::<MessageRequest>(100);

    tokio::spawn(async move {
        loop {
            select! {
                req = rx.recv() => {
                    let Some(req) = req else { break };
                    let (result, req_tx) = handle_request(req, pool.clone(), &hub).await;
                    let _ = req_tx.send(result);
                }
                _ = shutdown.cancelled() => {
                    break
                }
            }
        }
    });

    MessageHandle { sender: tx }
}

async fn handle_request(
    req: MessageRequest,
    pool: PgPool,
    hub: &Hub,
) -> (MessageResponse, oneshot::Sender<MessageResponse>) {
    match req {
        MessageRequest::SendMessage {
            sender_id,
            room_id,
            content,
            attachments,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(_e) => {
                    // TODO - Logging
                    return (MessageResponse::Failed, tx);
                }
            };

            // JIT auth + room name in one shot: only a member of the room may post,
            // and we need the room's (canonical) name for the live NewMessage event.
            // A non-member and a non-existent room both yield no row -> Failed, so a
            // non-member can't probe a room's existence. Re-checked per send rather
            // than trusting anything cached at connect time.
            let room_name: String = match sqlx::query_scalar(
                "SELECT r.room_name FROM rooms r
                    JOIN memberships mb ON mb.room_id = r.room_id
                    WHERE r.room_id = $1 AND mb.user_id = $2",
            )
            .bind(room_id)
            .bind(sender_id)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(Some(name)) => name,
                Ok(None) => return (MessageResponse::Failed, tx),
                Err(_e) => {
                    // TODO - Logging
                    return (MessageResponse::Failed, tx);
                }
            };

            // Insert the message. sender_id is set; sender_username_snapshot stays
            // NULL and is only backfilled if the sender is later deleted. Empty
            // content is rejected by the table's CHECK -> Failed. We read back the
            // server-assigned timestamp, the (round-tripped) content, and the
            // sender's current display name so the same canonical item can go in the
            // ack and the live broadcast.
            let (message_id, timestamp, content, sender_username) =
                match sqlx::query_as::<_, (Uuid, chrono::DateTime<chrono::Utc>, String, String)>(
                    "INSERT INTO messages (room_id, sender_id, content)
                        VALUES ($1, $2, $3)
                        RETURNING message_id, timestamp, content,
                                  (SELECT username FROM users WHERE user_id = $2)",
                )
                .bind(room_id)
                .bind(sender_id)
                .bind(content)
                .fetch_one(&mut *db)
                .await
                {
                    Ok(row) => row,
                    Err(_e) => {
                        // TODO - Logging
                        return (MessageResponse::Failed, tx);
                    }
                };

            // Create one incomplete attachment row per declaration, in order, so
            // the returned ids line up with the client's `attachments`. The bytes
            // are NOT here -- they arrive later as chunks keyed by these ids, and
            // is_complete flips once every chunk lands and the hash matches. The
            // table's CHECKs (non-empty filename/content_type, positive size and
            // chunk_count, 32-byte digest) reject malformed declarations -> Failed,
            // rolling back the whole message.
            let mut attachment_ids = Vec::with_capacity(attachments.len());
            let mut attachment_summaries = Vec::with_capacity(attachments.len());
            for attachment in attachments {
                let NewMessageAttachment {
                    filename,
                    content_type,
                    size_bytes,
                    chunk_count,
                    content_sha256,
                } = attachment;
                let attachment_id = match sqlx::query_scalar::<_, Uuid>(
                    "INSERT INTO message_attachments
                        (message_id, filename, content_type, size_bytes, chunk_count, content_sha256)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        RETURNING attachment_id",
                )
                .bind(message_id)
                .bind(filename.as_str())
                .bind(content_type.as_str())
                .bind(size_bytes)
                .bind(chunk_count)
                .bind(content_sha256.as_slice())
                .fetch_one(&mut *db)
                .await
                {
                    Ok(id) => id,
                    Err(_e) => {
                        // TODO - Logging
                        return (MessageResponse::Failed, tx);
                    }
                };
                attachment_ids.push(attachment_id);
                // Freshly declared, so always incomplete here; is_complete flips once
                // its chunks land. The bytes are fetched later via DownloadAttachment.
                attachment_summaries.push(AttachmentSummary {
                    attachment_id,
                    filename,
                    content_type,
                    size_bytes,
                    is_complete: false,
                });
            }

            // Sending to a room counts as reading it: advance the sender's own
            // watermark to this message so their own posts never show as unread to
            // them. Forward-only guard, though the just-inserted id is always the
            // newest. Part of the same transaction as the insert.
            if let Err(_e) = sqlx::query(
                "UPDATE memberships
                    SET last_read_message_id = $3
                    WHERE room_id = $1 AND user_id = $2
                      AND (last_read_message_id IS NULL OR $3 > last_read_message_id)",
            )
            .bind(room_id)
            .bind(sender_id)
            .bind(message_id)
            .execute(&mut *db)
            .await
            {
                // TODO - Logging
                return (MessageResponse::Failed, tx);
            }

            match db.commit().await {
                Ok(_) => {
                    // The canonical form of the new message: a fresh post has no
                    // reactions yet. Used both for the ack and the live broadcast.
                    let message = MessageHistoryItem {
                        message_id,
                        sender_username,
                        content,
                        timestamp,
                        attachments: attachment_summaries,
                        reactions: Vec::new(),
                    };

                    // Fan out to the room's live subscribers (including the sender's
                    // own other sessions; they dedup by message_id). Best-effort: no
                    // channel means nobody's connected, nothing to do. Only after the
                    // commit, so we never announce a message that rolled back.
                    hub.publish(
                        room_id,
                        ServerEvent::NewMessage {
                            room_name,
                            message: message.clone(),
                        },
                    );

                    (
                        MessageResponse::MessageCreated {
                            message_id,
                            attachment_ids,
                            message,
                        },
                        tx,
                    )
                }
                Err(_e) => {
                    // TODO - Logging
                    (MessageResponse::Failed, tx)
                }
            }
        }

        MessageRequest::AddReaction {
            user_id,
            message_id,
            emoji,
            tx,
        } => {
            // JIT auth: only a member of the message's room may react. A non-member
            // and an unknown message are indistinguishable (both fail the EXISTS),
            // so neither the message's existence nor the room's membership leaks.
            // Re-checked per action rather than trusting anything cached at connect.
            if !is_room_member_for_message(&pool, message_id, user_id).await {
                return (MessageResponse::Failed, tx);
            }

            // Idempotent add: the (message_id, user_id, emoji) PK turns a repeat of
            // the same emoji into a no-op. An empty emoji trips the table CHECK ->
            // Failed. Either way the caller learns nothing about other users' rows.
            let result = sqlx::query(
                "INSERT INTO message_reactions (message_id, user_id, emoji)
                    VALUES ($1, $2, $3)
                    ON CONFLICT DO NOTHING",
            )
            .bind(message_id)
            .bind(user_id)
            .bind(emoji)
            .execute(&pool)
            .await;

            match result {
                Ok(_) => (MessageResponse::Success, tx),
                Err(_e) => {
                    // TODO - Logging
                    (MessageResponse::Failed, tx)
                }
            }
        }

        MessageRequest::RemoveReaction {
            user_id,
            message_id,
            emoji,
            tx,
        } => {
            // Same membership gate as AddReaction. The DELETE is already scoped to
            // the caller's own row, but gating on membership keeps the existence-
            // hiding symmetric: a non-member can't tell a real message from a fake.
            if !is_room_member_for_message(&pool, message_id, user_id).await {
                return (MessageResponse::Failed, tx);
            }

            // Idempotent remove: deleting a reaction that isn't there affects 0 rows
            // and is still a success.
            let result = sqlx::query(
                "DELETE FROM message_reactions
                    WHERE message_id = $1 AND user_id = $2 AND emoji = $3",
            )
            .bind(message_id)
            .bind(user_id)
            .bind(emoji)
            .execute(&pool)
            .await;

            match result {
                Ok(_) => (MessageResponse::Success, tx),
                Err(_e) => {
                    // TODO - Logging
                    (MessageResponse::Failed, tx)
                }
            }
        }

        MessageRequest::GetMessages {
            user_id,
            room_name,
            before,
            limit,
            tx,
        } => {
            // Clamp the page size: an unset limit gets the default, an oversized one
            // is capped, and 0 floors to 1 so a request always makes progress.
            let limit = limit
                .map(|l| (l as i64).clamp(1, MAX_HISTORY_LIMIT))
                .unwrap_or(DEFAULT_HISTORY_LIMIT);

            // JIT auth + name resolution: members only, existence hidden.
            let room_id = match resolve_member_room(&pool, &room_name, user_id).await {
                Ok(Some(id)) => id,
                Ok(None) => return (MessageResponse::Failed, tx),
                Err(_e) => {
                    // TODO - Logging
                    return (MessageResponse::Failed, tx);
                }
            };

            match load_history(&pool, room_id, user_id, before, limit).await {
                Ok(messages) => (
                    MessageResponse::History {
                        room_name,
                        messages,
                    },
                    tx,
                ),
                Err(_e) => {
                    // TODO - Logging
                    (MessageResponse::Failed, tx)
                }
            }
        }

        MessageRequest::MarkRead {
            user_id,
            room_name,
            up_to_message_id,
            tx,
        } => {
            // Same membership gate as GetMessages: a non-member or unknown room is a
            // generic failure, so neither leaks.
            let room_id = match resolve_member_room(&pool, &room_name, user_id).await {
                Ok(Some(id)) => id,
                Ok(None) => return (MessageResponse::Failed, tx),
                Err(_e) => {
                    // TODO - Logging
                    return (MessageResponse::Failed, tx);
                }
            };

            // Advance the watermark forward-only, and only to a message that's
            // actually in this room. Zero rows -- already at/past it, or a bogus id
            // -- is a harmless idempotent no-op (it's the caller's own state), so we
            // don't distinguish it from a real advance.
            let result = sqlx::query(
                "UPDATE memberships
                    SET last_read_message_id = $3
                    WHERE room_id = $1 AND user_id = $2
                      AND EXISTS (SELECT 1 FROM messages m
                                  WHERE m.message_id = $3 AND m.room_id = $1)
                      AND (last_read_message_id IS NULL OR $3 > last_read_message_id)",
            )
            .bind(room_id)
            .bind(user_id)
            .bind(up_to_message_id)
            .execute(&pool)
            .await;

            match result {
                Ok(_) => (MessageResponse::Success, tx),
                Err(_e) => {
                    // TODO - Logging
                    (MessageResponse::Failed, tx)
                }
            }
        }

        MessageRequest::GetUnreadSummary { user_id, tx } => {
            // One row per room the caller is in (including fully-read rooms, via the
            // LEFT JOIN), counting messages newer than their per-room watermark.
            let rows = sqlx::query_as::<_, (String, i64)>(
                "SELECT r.room_name, COUNT(m.message_id) AS unread
                    FROM memberships mb
                    JOIN rooms r ON r.room_id = mb.room_id
                    LEFT JOIN messages m
                      ON m.room_id = mb.room_id
                      AND (mb.last_read_message_id IS NULL
                           OR m.message_id > mb.last_read_message_id)
                    WHERE mb.user_id = $1
                    GROUP BY r.room_name
                    ORDER BY r.room_name",
            )
            .bind(user_id)
            .fetch_all(&pool)
            .await;

            match rows {
                Ok(rows) => {
                    let rooms = rows
                        .into_iter()
                        .map(|(room_name, unread)| RoomUnread { room_name, unread })
                        .collect();
                    (MessageResponse::UnreadSummary { rooms }, tx)
                }
                Err(_e) => {
                    // TODO - Logging
                    (MessageResponse::Failed, tx)
                }
            }
        }
    }
}

// Resolve a (normalized) room name to its id, but only for a caller who is a
// member -- a non-member and an unknown room both yield None, so neither the
// room's existence nor its membership leaks. Shared by the member-gated read paths
// (history, mark-read).
async fn resolve_member_room(
    pool: &PgPool,
    room_name: &str,
    user_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT r.room_id
            FROM rooms r
            JOIN memberships mb ON mb.room_id = r.room_id
            WHERE LOWER(r.room_name) = LOWER(trim_ws($1)) AND mb.user_id = $2",
    )
    .bind(room_name)
    .bind(user_id)
    .fetch_optional(pool)
    .await
}

// Load one page of a room's messages (newest first) with their attachments and
// reaction summary, given that the caller's membership was already verified.
//
// Three queries rather than one nested aggregation: the page, then its attachments
// and reaction tallies fetched by the page's message_ids and stitched together in
// memory. Keeps each statement simple and lets the page use the
// (room_id, timestamp DESC) index directly.
async fn load_history(
    pool: &PgPool,
    room_id: Uuid,
    user_id: Uuid,
    before: Option<Uuid>,
    limit: i64,
) -> Result<Vec<MessageHistoryItem>, sqlx::Error> {
    // Keyset page: when `before` is set, take only messages ordering before that
    // (timestamp, message_id) pair, so paging backwards never repeats or skips a
    // row even when timestamps collide. A cursor that doesn't resolve yields an
    // empty page rather than an error.
    let rows = sqlx::query_as::<_, (Uuid, String, String, DateTime<Utc>)>(
        "SELECT m.message_id,
                COALESCE(u.username, m.sender_username_snapshot) AS sender_username,
                m.content,
                m.timestamp
            FROM messages m
            LEFT JOIN users u ON u.user_id = m.sender_id
            WHERE m.room_id = $1
              AND ($2::uuid IS NULL
                   OR (m.timestamp, m.message_id) <
                      (SELECT c.timestamp, c.message_id FROM messages c WHERE c.message_id = $2))
            ORDER BY m.timestamp DESC, m.message_id DESC
            LIMIT $3",
    )
    .bind(room_id)
    .bind(before)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut items: Vec<MessageHistoryItem> = Vec::with_capacity(rows.len());
    let mut index: HashMap<Uuid, usize> = HashMap::with_capacity(rows.len());
    for (message_id, sender_username, content, timestamp) in rows {
        index.insert(message_id, items.len());
        items.push(MessageHistoryItem {
            message_id,
            sender_username,
            content,
            timestamp,
            attachments: Vec::new(),
            reactions: Vec::new(),
        });
    }

    // No messages -> no children to fetch.
    if items.is_empty() {
        return Ok(items);
    }

    let ids: Vec<Uuid> = items.iter().map(|m| m.message_id).collect();

    // Attachments for the page, grouped onto their message. Ordered so a message's
    // files come back in a stable (declaration) order.
    let attachments = sqlx::query_as::<_, (Uuid, Uuid, String, String, i64, bool)>(
        "SELECT attachment_id, message_id, filename, content_type, size_bytes, is_complete
            FROM message_attachments
            WHERE message_id = ANY($1)
            ORDER BY message_id, created_at, attachment_id",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await?;

    for (attachment_id, message_id, filename, content_type, size_bytes, is_complete) in attachments
    {
        if let Some(&i) = index.get(&message_id) {
            items[i].attachments.push(AttachmentSummary {
                attachment_id,
                filename,
                content_type,
                size_bytes,
                is_complete,
            });
        }
    }

    // Per-emoji reaction tallies for the page, plus whether the caller reacted.
    let reactions = sqlx::query_as::<_, (Uuid, String, i64, bool)>(
        "SELECT message_id, emoji, COUNT(*) AS count, BOOL_OR(user_id = $2) AS reacted_by_me
            FROM message_reactions
            WHERE message_id = ANY($1)
            GROUP BY message_id, emoji
            ORDER BY message_id, emoji",
    )
    .bind(&ids)
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    for (message_id, emoji, count, reacted_by_me) in reactions {
        if let Some(&i) = index.get(&message_id) {
            items[i].reactions.push(ReactionSummary {
                emoji,
                count,
                reacted_by_me,
            });
        }
    }

    Ok(items)
}

// True if `user_id` is a member of the room that owns `message_id`. A missing
// message yields false, so callers can't distinguish "not a member" from "no such
// message" -- both deny the action without leaking which.
async fn is_room_member_for_message(pool: &PgPool, message_id: Uuid, user_id: Uuid) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1 FROM messages m
            JOIN memberships mb ON mb.room_id = m.room_id
            WHERE m.message_id = $1 AND mb.user_id = $2)",
    )
    .bind(message_id)
    .bind(user_id)
    .fetch_one(pool)
    .await
    .unwrap_or(false)
}
