use sqlx::{PgPool, Row};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::hub::Hub;
use crate::model::{self, DiscoverableRoom, JoinRequestInfo, Room, RoomMember};

pub enum RoomRequest {
    NewRoomRequest {
        room_name: String,
        source_user_id: Uuid,
        is_public: bool,
        is_discoverable: bool,
        tx: oneshot::Sender<RoomResponse>,
    },
    AddRoomOwner {
        source_user_id: Uuid,
        room_name: String,
        new_owner_username: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    SetRoomName {
        source_user_id: Uuid,
        current_name: String,
        new_name: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    GetRoomMembershipByName {
        source_user_id: Uuid,
        room_name: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    GetRoomByName {
        source_user_id: Uuid,
        room_name: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    NewRoomMembershipRequest {
        user_id: Uuid,
        room_name: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    RemoveRoomMembershipRequest {
        user_id: Uuid,
        room_name: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    // Owner/admin removes another user's membership.
    RemoveRoomMember {
        source_user_id: Uuid,
        room_name: String,
        member_username: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    GetMyJoinRequests {
        source_user_id: Uuid,
        tx: oneshot::Sender<RoomResponse>,
    },
    // The caller withdraws their own pending join request.
    CancelJoinRequest {
        user_id: Uuid,
        room_name: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    GetIncomingJoinRequests {
        source_user_id: Uuid,
        tx: oneshot::Sender<RoomResponse>,
    },
    ApproveJoinRequest {
        source_user_id: Uuid,
        room_name: String,
        requester_username: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    RejectJoinRequest {
        source_user_id: Uuid,
        room_name: String,
        requester_username: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    InviteToRoom {
        source_user_id: Uuid,
        room_name: String,
        invitee_username: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    GetMyInvites {
        source_user_id: Uuid,
        tx: oneshot::Sender<RoomResponse>,
    },
    AcceptInvite {
        user_id: Uuid,
        room_name: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    DeclineInvite {
        user_id: Uuid,
        room_name: String,
        tx: oneshot::Sender<RoomResponse>,
    },
    ListDiscoverableRooms {
        tx: oneshot::Sender<RoomResponse>,
    },
    // Admin only: every room, including private non-discoverable ones.
    ListAllRooms {
        source_user_id: Uuid,
        tx: oneshot::Sender<RoomResponse>,
    },
}

pub enum RoomResponse {
    RoomCreated {
        room_id: Uuid,
    },
    RoomInfo {
        room_name: String,
        is_public: bool,
        is_discoverable: bool,
    },
    RoomMembership {
        members: Vec<RoomMember>,
    },
    MyJoinRequests {
        rooms: Vec<String>,
    },
    IncomingJoinRequests {
        requests: Vec<JoinRequestInfo>,
    },
    MyInvites {
        rooms: Vec<String>,
    },
    DiscoverableRooms {
        rooms: Vec<DiscoverableRoom>,
    },
    AllRooms {
        rooms: Vec<DiscoverableRoom>,
    },
    NoRoomExists,
    JoinRequested,
    Success,
    NoChange,
    Failed,
}

#[derive(Clone)]
pub struct RoomHandle {
    sender: mpsc::Sender<RoomRequest>,
}

impl RoomHandle {
    pub async fn new_room(&self, new_room: model::NewRoom) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::NewRoomRequest {
                room_name: new_room.room_name,
                source_user_id: new_room.owner_id,
                is_public: new_room.is_public,
                is_discoverable: new_room.is_discoverable,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn add_room_owner(
        &self,
        source_user_id: Uuid,
        room_name: String,
        new_owner_username: String,
    ) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::AddRoomOwner {
                source_user_id,
                room_name,
                new_owner_username,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn set_room_name(
        &self,
        source_user_id: Uuid,
        current_name: String,
        new_name: String,
    ) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::SetRoomName {
                source_user_id,
                current_name,
                new_name,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn get_room(&self, source_user_id: Uuid, room_name: String) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::GetRoomByName {
                source_user_id,
                room_name,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn get_room_members(&self, source_user_id: Uuid, room_name: String) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::GetRoomMembershipByName {
                source_user_id,
                room_name,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn join_room(&self, user_id: Uuid, room_name: String) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::NewRoomMembershipRequest {
                user_id,
                room_name,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn leave_room(&self, user_id: Uuid, room_name: String) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::RemoveRoomMembershipRequest {
                user_id,
                room_name,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn remove_room_member(
        &self,
        source_user_id: Uuid,
        room_name: String,
        member_username: String,
    ) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::RemoveRoomMember {
                source_user_id,
                room_name,
                member_username,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn get_my_join_requests(&self, source_user_id: Uuid) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::GetMyJoinRequests { source_user_id, tx })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn cancel_join_request(&self, user_id: Uuid, room_name: String) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::CancelJoinRequest {
                user_id,
                room_name,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn get_incoming_join_requests(&self, source_user_id: Uuid) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::GetIncomingJoinRequests { source_user_id, tx })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn approve_join_request(
        &self,
        source_user_id: Uuid,
        room_name: String,
        requester_username: String,
    ) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::ApproveJoinRequest {
                source_user_id,
                room_name,
                requester_username,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn reject_join_request(
        &self,
        source_user_id: Uuid,
        room_name: String,
        requester_username: String,
    ) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::RejectJoinRequest {
                source_user_id,
                room_name,
                requester_username,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn invite_to_room(
        &self,
        source_user_id: Uuid,
        room_name: String,
        invitee_username: String,
    ) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::InviteToRoom {
                source_user_id,
                room_name,
                invitee_username,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn get_my_invites(&self, source_user_id: Uuid) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::GetMyInvites { source_user_id, tx })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn accept_invite(&self, user_id: Uuid, room_name: String) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::AcceptInvite {
                user_id,
                room_name,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn decline_invite(&self, user_id: Uuid, room_name: String) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::DeclineInvite {
                user_id,
                room_name,
                tx,
            })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn list_discoverable_rooms(&self) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::ListDiscoverableRooms { tx })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }

    pub async fn list_all_rooms(&self, source_user_id: Uuid) -> RoomResponse {
        let (tx, rx) = oneshot::channel();

        if self
            .sender
            .send(RoomRequest::ListAllRooms { source_user_id, tx })
            .await
            .is_err()
        {
            return RoomResponse::Failed;
        }

        rx.await.unwrap_or(RoomResponse::Failed)
    }
}

pub async fn spawn(shutdown: CancellationToken, pool: PgPool, hub: Hub) -> RoomHandle {
    // ROOM ACTOR COMMUNICATION CHANNELS
    // rx stays in the spawned actor
    // tx gets returned in RoomHandle
    // Cloning user handle allows a new
    // communication channel to the actor
    let (tx, mut rx) = mpsc::channel::<RoomRequest>(100);

    // ROOM ACTOR TASK
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

    // ROOM ACTOR HANDLE
    RoomHandle { sender: tx }
}

async fn handle_request(
    req: RoomRequest,
    pool: PgPool,
    hub: &Hub,
) -> (RoomResponse, oneshot::Sender<RoomResponse>) {
    match req {
        RoomRequest::NewRoomRequest {
            room_name,
            source_user_id,
            is_public,
            is_discoverable,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "new_room: begin transaction failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Captured for the audit log below; room_name is consumed by the insert.
            let room_name_for_log = room_name.clone();

            // Create the room with the requested visibility
            let room_id: Uuid = match sqlx::query(
                "INSERT INTO rooms (room_name, is_public, is_discoverable)
                    VALUES (trim_ws($1), $2, $3)
                    RETURNING room_id",
            )
            .bind(room_name)
            .bind(is_public)
            .bind(is_discoverable)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(Some(row)) => match row.try_get("room_id") {
                    Ok(room_id) => room_id,
                    Err(e) => {
                        tracing::error!(error = %e, "new_room: room_id column read failed");
                        return (RoomResponse::Failed, tx);
                    }
                },
                Ok(None) => {
                    tracing::error!("new_room: insert returned no row");
                    return (RoomResponse::Failed, tx);
                }
                Err(e) => {
                    // Commonly a unique-violation (room name already taken), a client
                    // error rather than a server fault -- warn rather than error.
                    tracing::warn!(error = %e, "new_room: room insert failed (e.g. duplicate name)");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Seed the creator as the room's first member and owner
            if let Err(e) = sqlx::query(
                "INSERT INTO memberships (room_id, user_id, is_owner) VALUES ($1, $2, true)",
            )
            .bind(room_id)
            .bind(source_user_id)
            .execute(&mut *db)
            .await
            {
                tracing::error!(error = %e, %room_id, "new_room: owner membership insert failed");
                return (RoomResponse::Failed, tx);
            }

            match db.commit().await {
                Ok(_) => {
                    tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_name_for_log, %room_id, "room created");
                    (RoomResponse::RoomCreated { room_id }, tx)
                }
                Err(e) => {
                    tracing::error!(error = %e, %room_id, "new_room: commit failed");
                    (RoomResponse::Failed, tx)
                }
            }
        }

        RoomRequest::AddRoomOwner {
            source_user_id,
            room_name,
            new_owner_username,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "add_room_owner: begin transaction failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Captured for audit/denial logs; new_owner_username is consumed below.
            let new_owner_for_log = new_owner_username.clone();

            // Authorize: caller must own the room (or be an admin). "No such
            // room" and "not authorized" both return Failed so existence isn't
            // leaked.
            let (room_id, authorized) = match gate_room(&mut db, source_user_id, &room_name).await {
                Ok(Some(gate)) => gate,
                // No such room: existence-hidden, not a fault.
                Ok(None) => {
                    tracing::debug!(room = %room_name, "add_room_owner: no such room");
                    return (RoomResponse::Failed, tx);
                }
                Err(e) => {
                    tracing::error!(error = %e, "add_room_owner: room authorization lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            if !authorized {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_name, "add_room_owner denied: caller is not an owner or admin");
                return (RoomResponse::Failed, tx);
            }

            // Translate the targeted user into a User ID
            let row = match sqlx::query(
                "SELECT user_id FROM users WHERE LOWER(username) = LOWER(trim_ws($1))",
            )
            .bind(new_owner_username)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(maybe_user_id) => match maybe_user_id {
                    Some(row) => row,
                    None => {
                        tracing::debug!(target = %new_owner_for_log, "add_room_owner: target user not found");
                        return (RoomResponse::Failed, tx);
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, "add_room_owner: target user lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            let maybe_user_id: Result<Uuid, sqlx::Error> = row.try_get("user_id");

            let user_id = match maybe_user_id {
                Ok(user_id) => user_id,
                Err(e) => {
                    tracing::error!(error = %e, "add_room_owner: user_id column read failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Read the targeted user's current ownership, locking the membership
            // row so the grant below is atomic with this check.
            //   None        -> not a member of the room  (Failed)
            //   Some(true)  -> already an owner          (NoChange)
            //   Some(false) -> member, not yet an owner  (grant -> Success)
            let already_owner: Option<bool> = match sqlx::query_scalar(
                "SELECT is_owner FROM memberships
                    WHERE room_id = $1 AND user_id = $2
                    FOR UPDATE",
            )
            .bind(room_id)
            .bind(user_id)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(maybe) => maybe,
                Err(e) => {
                    tracing::error!(error = %e, %room_id, "add_room_owner: ownership lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            match already_owner {
                None => {
                    // Targeted user is not a member of the room
                    tracing::debug!(%room_id, target = %new_owner_for_log, "add_room_owner: target is not a member");
                    (RoomResponse::Failed, tx)
                }
                Some(true) => {
                    // Already an owner
                    (RoomResponse::NoChange, tx)
                }
                Some(false) => {
                    // Member but not yet an owner
                    if let Err(e) = sqlx::query(
                        "UPDATE memberships SET is_owner = true
                            WHERE room_id = $1 AND user_id = $2",
                    )
                    .bind(room_id)
                    .bind(user_id)
                    .execute(&mut *db)
                    .await
                    {
                        tracing::error!(error = %e, %room_id, "add_room_owner: ownership grant failed");
                        return (RoomResponse::Failed, tx);
                    }

                    match db.commit().await {
                        Ok(_) => {
                            tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_name, target = %new_owner_for_log, %room_id, "room owner added");
                            (RoomResponse::Success, tx)
                        }
                        Err(e) => {
                            tracing::error!(error = %e, %room_id, "add_room_owner: commit failed");
                            (RoomResponse::Failed, tx)
                        }
                    }
                }
            }
        }

        RoomRequest::SetRoomName {
            source_user_id,
            current_name,
            new_name,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "set_room_name: begin transaction failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Captured for the audit log; new_name is consumed by the update.
            let new_name_for_log = new_name.clone();

            // Authorize: caller must own the room (or be an admin). "No such
            // room" and "not authorized" both return Failed so existence isn't
            // leaked.
            let (room_id, authorized) = match gate_room(&mut db, source_user_id, &current_name)
                .await
            {
                Ok(Some(gate)) => gate,
                // No such room: existence-hidden, not a fault.
                Ok(None) => {
                    tracing::debug!(room = %current_name, "set_room_name: no such room");
                    return (RoomResponse::Failed, tx);
                }
                Err(e) => {
                    tracing::error!(error = %e, "set_room_name: room authorization lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            if !authorized {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, room = %current_name, "set_room_name denied: caller is not an owner or admin");
                return (RoomResponse::Failed, tx);
            }

            // Update the room's name to the new name
            let result = match sqlx::query(
                "UPDATE rooms SET room_name = trim_ws($1) WHERE room_id = $2",
            )
            .bind(new_name)
            .bind(room_id)
            .execute(&mut *db)
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    // Commonly a unique-violation (new name already taken).
                    tracing::warn!(error = %e, %room_id, "set_room_name: rename failed (e.g. duplicate name)");
                    return (RoomResponse::Failed, tx);
                }
            };

            match result.rows_affected() {
                1 => match db.commit().await {
                    Ok(_) => {
                        tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, room = %current_name, new_name = %new_name_for_log, %room_id, "room renamed");
                        (RoomResponse::Success, tx)
                    }
                    Err(e) => {
                        tracing::error!(error = %e, %room_id, "set_room_name: commit failed");
                        (RoomResponse::Failed, tx)
                    }
                },
                0 => (RoomResponse::Failed, tx),
                _ => unreachable!(),
            }
        }

        RoomRequest::GetRoomByName {
            source_user_id,
            room_name,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "get_room: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            let maybe_room = match sqlx::query_as::<_, Room>(
                "SELECT room_name, room_id, is_public, is_discoverable
                    FROM rooms WHERE LOWER(room_name) = LOWER(trim_ws($1))",
            )
            .bind(room_name)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(maybe_room) => maybe_room,
                Err(e) => {
                    tracing::error!(error = %e, "get_room: room lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            let Some(room) = maybe_room else {
                return (RoomResponse::NoRoomExists, tx);
            };

            // Public rooms are visible to anyone. A private room is visible only
            // to its members (owners included) and to admins; to everyone else it
            // is reported as non-existent so its existence isn't leaked.
            if !room.is_public {
                let visible: bool = match sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM memberships m
                                    WHERE m.room_id = $1 AND m.user_id = $2)
                         OR EXISTS (SELECT 1 FROM admins a WHERE a.user_id = $2)",
                )
                .bind(room.room_id)
                .bind(source_user_id)
                .fetch_one(&mut *db)
                .await
                {
                    Ok(visible) => visible,
                    Err(e) => {
                        tracing::error!(error = %e, room_id = %room.room_id, "get_room: visibility check failed");
                        return (RoomResponse::Failed, tx);
                    }
                };

                if !visible {
                    return (RoomResponse::NoRoomExists, tx);
                }
            }

            (
                RoomResponse::RoomInfo {
                    room_name: room.room_name,
                    is_public: room.is_public,
                    is_discoverable: room.is_discoverable,
                },
                tx,
            )
        }

        RoomRequest::GetRoomMembershipByName {
            source_user_id,
            room_name,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "get_room_membership: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Resolve the room and its visibility first, so a private room is
            // hidden from non-members rather than leaking its member list.
            let maybe_room = match sqlx::query_as::<_, Room>(
                "SELECT room_name, room_id, is_public, is_discoverable
                    FROM rooms WHERE LOWER(room_name) = LOWER(trim_ws($1))",
            )
            .bind(room_name)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(maybe_room) => maybe_room,
                Err(e) => {
                    tracing::error!(error = %e, "get_room_membership: room lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            let Some(room) = maybe_room else {
                return (RoomResponse::NoRoomExists, tx);
            };

            // Public rooms' membership is listable by anyone. A private room's
            // membership is visible only to its members (owners included) and to
            // admins; to everyone else the room is reported as non-existent.
            if !room.is_public {
                let visible: bool = match sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM memberships m
                                    WHERE m.room_id = $1 AND m.user_id = $2)
                         OR EXISTS (SELECT 1 FROM admins a WHERE a.user_id = $2)",
                )
                .bind(room.room_id)
                .bind(source_user_id)
                .fetch_one(&mut *db)
                .await
                {
                    Ok(visible) => visible,
                    Err(e) => {
                        tracing::error!(error = %e, room_id = %room.room_id, "get_room_membership: visibility check failed");
                        return (RoomResponse::Failed, tx);
                    }
                };

                if !visible {
                    return (RoomResponse::NoRoomExists, tx);
                }
            }

            let users = match sqlx::query_as::<_, RoomMember>(
                "SELECT u.first_name, u.last_name, u.alias, u.username, u.created_at, m.is_owner
                FROM memberships m
                JOIN users u ON u.user_id = m.user_id
                WHERE m.room_id = $1
                ORDER BY m.is_owner DESC, m.joined_at",
            )
            .bind(room.room_id)
            .fetch_all(&mut *db)
            .await
            {
                Ok(users) => users,
                Err(e) => {
                    tracing::error!(error = %e, room_id = %room.room_id, "get_room_membership: member list query failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            (RoomResponse::RoomMembership { members: users }, tx)
        }

        RoomRequest::NewRoomMembershipRequest {
            user_id,
            room_name,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "join_room: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Resolve the room and its visibility to pick the join path.
            let maybe_room = match sqlx::query_as::<_, Room>(
                "SELECT room_name, room_id, is_public, is_discoverable
                    FROM rooms WHERE LOWER(room_name) = LOWER(trim_ws($1))",
            )
            .bind(room_name)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(maybe_room) => maybe_room,
                Err(e) => {
                    tracing::error!(error = %e, "join_room: room lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            let Some(room) = maybe_room else {
                return (RoomResponse::NoRoomExists, tx);
            };

            if room.is_public {
                // Public: join immediately. ON CONFLICT DO NOTHING makes a
                // repeat join a no-op (0 rows) rather than an error.
                // Start caught up: the watermark is the room's newest message at
                // join time, so a new member's history isn't a wall of "unread".
                let result = match sqlx::query(
                    "INSERT INTO memberships (room_id, user_id, last_read_message_id)
                        VALUES ($1, $2,
                                (SELECT message_id FROM messages
                             WHERE room_id = $1 ORDER BY message_id DESC LIMIT 1))
                        ON CONFLICT DO NOTHING",
                )
                .bind(room.room_id)
                .bind(user_id)
                .execute(&mut *db)
                .await
                {
                    Ok(res) => res,
                    Err(e) => {
                        tracing::error!(error = %e, room_id = %room.room_id, "join_room: membership insert failed");
                        return (RoomResponse::Failed, tx);
                    }
                };

                match result.rows_affected() {
                    1 => (RoomResponse::Success, tx),
                    0 => (RoomResponse::NoChange, tx),
                    _ => unreachable!(),
                }
            } else if room.is_discoverable {
                // Private but discoverable: queue a join request. Insert only if
                // not already a member; ON CONFLICT DO NOTHING drops a duplicate
                // request. So 0 rows == already a member or already requested,
                // both of which are NoChange.
                let result = match sqlx::query(
                    "INSERT INTO room_join_requests (room_id, user_id)
                        SELECT $1, $2
                        WHERE NOT EXISTS (
                            SELECT 1 FROM memberships m
                            WHERE m.room_id = $1 AND m.user_id = $2)
                        ON CONFLICT DO NOTHING",
                )
                .bind(room.room_id)
                .bind(user_id)
                .execute(&mut *db)
                .await
                {
                    Ok(res) => res,
                    Err(e) => {
                        tracing::error!(error = %e, room_id = %room.room_id, "join_room: join request insert failed");
                        return (RoomResponse::Failed, tx);
                    }
                };

                match result.rows_affected() {
                    1 => (RoomResponse::JoinRequested, tx),
                    0 => (RoomResponse::NoChange, tx),
                    _ => unreachable!(),
                }
            } else {
                // Private and non-discoverable: invite-only. Report as
                // non-existent so the room's existence stays hidden.
                (RoomResponse::NoRoomExists, tx)
            }
        }

        RoomRequest::RemoveRoomMembershipRequest {
            user_id,
            room_name,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "leave_room: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Remove the caller's membership, resolving the room by name inline.
            // A non-existent room or a non-member both delete zero rows -> NoChange.
            // Leaving as the last owner is allowed; ownerless rooms are supported.
            let result = match sqlx::query(
                "DELETE FROM memberships
                    WHERE user_id = $1
                    AND room_id = (SELECT room_id FROM rooms
                                   WHERE LOWER(room_name) = LOWER(trim_ws($2)))",
            )
            .bind(user_id)
            .bind(room_name)
            .execute(&mut *db)
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!(error = %e, "leave_room: membership delete failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            match result.rows_affected() {
                1 => (RoomResponse::Success, tx),
                0 => (RoomResponse::NoChange, tx),
                _ => unreachable!(),
            }
        }

        RoomRequest::RemoveRoomMember {
            source_user_id,
            room_name,
            member_username,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "remove_room_member: begin transaction failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            let room_for_log = room_name.clone();
            let member_for_log = member_username.clone();

            // Authorize: caller must own the room (or be an admin). Lock the room
            // row so the membership delete sees a stable authorization decision.
            let gate = match sqlx::query_as::<_, (Uuid, bool)>(
                "SELECT
                    r.room_id,
                    (EXISTS (SELECT 1 FROM memberships m
                             WHERE m.room_id = r.room_id AND m.user_id = $1 AND m.is_owner)
                     OR EXISTS (SELECT 1 FROM admins a WHERE a.user_id = $1)) AS authorized
                FROM rooms r
                WHERE LOWER(r.room_name) = LOWER(trim_ws($2))
                FOR UPDATE OF r",
            )
            .bind(source_user_id)
            .bind(room_name)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(gate) => gate,
                Err(e) => {
                    tracing::error!(error = %e, "remove_room_member: room authorization lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // "No such room" and "not authorized" are reported identically so a
            // non-owner can't probe a private room's existence.
            let Some((room_id, authorized)) = gate else {
                tracing::debug!(room = %room_for_log, "remove_room_member: no such room");
                return (RoomResponse::Failed, tx);
            };
            if !authorized {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_for_log, "remove_room_member denied: caller is not an owner or admin");
                return (RoomResponse::Failed, tx);
            }

            // Remove the target's membership, capturing their id so we can drop the
            // room's live stream from their sessions. Zero rows -- not a member --
            // NoChange.
            let removed_user_id = match sqlx::query_scalar::<_, Uuid>(
                "DELETE FROM memberships
                    WHERE room_id = $1
                    AND user_id = (SELECT user_id FROM users
                                   WHERE LOWER(username) = LOWER(trim_ws($2)))
                    RETURNING user_id",
            )
            .bind(room_id)
            .bind(member_username)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(id) => id,
                Err(e) => {
                    tracing::error!(error = %e, %room_id, "remove_room_member: membership delete failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            let Some(removed_user_id) = removed_user_id else {
                return (RoomResponse::NoChange, tx);
            };

            match db.commit().await {
                Ok(_) => {
                    tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_for_log, target = %member_for_log, %room_id, "room member removed");
                    // Cross-session: the removed user isn't the caller. Drop the
                    // room's live stream from any of their open sessions now, so
                    // they stop receiving its messages immediately instead of at
                    // their next reconnect. Their JIT membership checks already block
                    // posting/reading/downloading.
                    hub.unsubscribe_user_from_room(removed_user_id, room_id);
                    (RoomResponse::Success, tx)
                }
                Err(e) => {
                    tracing::error!(error = %e, %room_id, "remove_room_member: commit failed");
                    (RoomResponse::Failed, tx)
                }
            }
        }

        RoomRequest::CancelJoinRequest {
            user_id,
            room_name,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "cancel_join_request: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Withdraw the caller's own pending request, resolving the room by name
            // inline. A non-existent room or no pending request both delete zero
            // rows -> NoChange. No authorization needed: a user owns their request.
            let result = match sqlx::query(
                "DELETE FROM room_join_requests
                    WHERE user_id = $1
                    AND room_id = (SELECT room_id FROM rooms
                                   WHERE LOWER(room_name) = LOWER(trim_ws($2)))",
            )
            .bind(user_id)
            .bind(room_name)
            .execute(&mut *db)
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!(error = %e, "cancel_join_request: request delete failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            match result.rows_affected() {
                0 => (RoomResponse::NoChange, tx),
                _ => (RoomResponse::Success, tx),
            }
        }

        RoomRequest::GetMyJoinRequests { source_user_id, tx } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "get_my_join_requests: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // The caller's own pending requests -- just the room names.
            let rooms = match sqlx::query_scalar::<_, String>(
                "SELECT r.room_name
                    FROM room_join_requests jr
                    JOIN rooms r ON r.room_id = jr.room_id
                    WHERE jr.user_id = $1
                    ORDER BY jr.created_at",
            )
            .bind(source_user_id)
            .fetch_all(&mut *db)
            .await
            {
                Ok(rooms) => rooms,
                Err(e) => {
                    tracing::error!(error = %e, "get_my_join_requests: query failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            (RoomResponse::MyJoinRequests { rooms }, tx)
        }

        RoomRequest::GetIncomingJoinRequests { source_user_id, tx } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "get_incoming_join_requests: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Requests the caller may act on: those for rooms they own, plus --
            // if the caller is an admin -- every pending request.
            let requests = match sqlx::query_as::<_, JoinRequestInfo>(
                "SELECT r.room_name, u.username
                    FROM room_join_requests jr
                    JOIN rooms r ON r.room_id = jr.room_id
                    JOIN users u ON u.user_id = jr.user_id
                    WHERE EXISTS (SELECT 1 FROM memberships m
                                  WHERE m.room_id = jr.room_id AND m.user_id = $1 AND m.is_owner)
                       OR EXISTS (SELECT 1 FROM admins a WHERE a.user_id = $1)
                    ORDER BY r.room_name, jr.created_at",
            )
            .bind(source_user_id)
            .fetch_all(&mut *db)
            .await
            {
                Ok(requests) => requests,
                Err(e) => {
                    tracing::error!(error = %e, "get_incoming_join_requests: query failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            (RoomResponse::IncomingJoinRequests { requests }, tx)
        }

        RoomRequest::ApproveJoinRequest {
            source_user_id,
            room_name,
            requester_username,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "approve_join_request: begin transaction failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Captured for audit/denial logs; requester_username is consumed below.
            let requester_for_log = requester_username.clone();

            // Authorize: caller must own the room (or be an admin). "No such
            // room" and "not authorized" both return Failed so existence isn't
            // leaked.
            let (room_id, authorized) = match gate_room(&mut db, source_user_id, &room_name).await {
                Ok(Some(gate)) => gate,
                // No such room: existence-hidden, not a fault.
                Ok(None) => {
                    tracing::debug!(room = %room_name, "approve_join_request: no such room");
                    return (RoomResponse::Failed, tx);
                }
                Err(e) => {
                    tracing::error!(error = %e, "approve_join_request: room authorization lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            if !authorized {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_name, "approve_join_request denied: caller is not an owner or admin");
                return (RoomResponse::Failed, tx);
            }

            // Delete the pending request, resolving the requester by name. None
            // means there was no such pending request -- nothing to approve.
            let approved_user_id = match sqlx::query_scalar::<_, Uuid>(
                "DELETE FROM room_join_requests
                    WHERE room_id = $1
                    AND user_id = (SELECT user_id FROM users
                                   WHERE LOWER(username) = LOWER(trim_ws($2)))
                    RETURNING user_id",
            )
            .bind(room_id)
            .bind(requester_username)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    tracing::error!(error = %e, %room_id, "approve_join_request: request delete failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            let Some(approved_user_id) = approved_user_id else {
                return (RoomResponse::NoChange, tx);
            };

            // Admit the requester. ON CONFLICT DO NOTHING in case they became a
            // member by some other path in the meantime.
            // Start caught up: watermark = newest message at admission time.
            if let Err(e) = sqlx::query(
                "INSERT INTO memberships (room_id, user_id, last_read_message_id)
                    VALUES ($1, $2,
                            (SELECT message_id FROM messages
                             WHERE room_id = $1 ORDER BY message_id DESC LIMIT 1))
                    ON CONFLICT DO NOTHING",
            )
            .bind(room_id)
            .bind(approved_user_id)
            .execute(&mut *db)
            .await
            {
                tracing::error!(error = %e, %room_id, %approved_user_id, "approve_join_request: membership insert failed");
                return (RoomResponse::Failed, tx);
            }

            // The canonical room name, for the live subscription's Resync hint.
            let room_name = match sqlx::query_scalar::<_, String>(
                "SELECT room_name FROM rooms WHERE room_id = $1",
            )
            .bind(room_id)
            .fetch_one(&mut *db)
            .await
            {
                Ok(name) => name,
                Err(e) => {
                    tracing::error!(error = %e, %room_id, "approve_join_request: canonical name lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            match db.commit().await {
                Ok(_) => {
                    tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_name, target = %requester_for_log, %approved_user_id, %room_id, "join request approved");
                    // Cross-session: the admitted user isn't the caller. If any of
                    // their sessions are online, subscribe those sessions to the
                    // room's live stream now, so they get messages without
                    // reconnecting. Offline users subscribe on their next connect.
                    hub.subscribe_user_to_room(approved_user_id, room_id, room_name);
                    (RoomResponse::Success, tx)
                }
                Err(e) => {
                    tracing::error!(error = %e, %room_id, "approve_join_request: commit failed");
                    (RoomResponse::Failed, tx)
                }
            }
        }

        RoomRequest::RejectJoinRequest {
            source_user_id,
            room_name,
            requester_username,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "reject_join_request: begin transaction failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Captured for audit/denial logs; both are consumed by the queries below.
            let room_for_log = room_name.clone();
            let requester_for_log = requester_username.clone();

            // Authorize: caller must own the room (or be an admin).
            let gate = match sqlx::query_as::<_, (Uuid, bool)>(
                "SELECT
                    r.room_id,
                    (EXISTS (SELECT 1 FROM memberships m
                             WHERE m.room_id = r.room_id AND m.user_id = $1 AND m.is_owner)
                     OR EXISTS (SELECT 1 FROM admins a WHERE a.user_id = $1)) AS authorized
                FROM rooms r
                WHERE LOWER(r.room_name) = LOWER(trim_ws($2))
                FOR UPDATE OF r",
            )
            .bind(source_user_id)
            .bind(room_name)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(gate) => gate,
                Err(e) => {
                    tracing::error!(error = %e, "reject_join_request: room authorization lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Treat "no such room" and "not authorized" identically, so a
            // non-owner can't probe a private room's existence here.
            let Some((room_id, authorized)) = gate else {
                tracing::debug!(room = %room_for_log, "reject_join_request: no such room");
                return (RoomResponse::Failed, tx);
            };
            if !authorized {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_for_log, "reject_join_request denied: caller is not an owner or admin");
                return (RoomResponse::Failed, tx);
            }

            // Drop the pending request. Zero rows -- no such request -- NoChange.
            let result = match sqlx::query(
                "DELETE FROM room_join_requests
                    WHERE room_id = $1
                    AND user_id = (SELECT user_id FROM users
                                   WHERE LOWER(username) = LOWER(trim_ws($2)))",
            )
            .bind(room_id)
            .bind(requester_username)
            .execute(&mut *db)
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!(error = %e, %room_id, "reject_join_request: request delete failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            match result.rows_affected() {
                0 => (RoomResponse::NoChange, tx),
                _ => match db.commit().await {
                    Ok(_) => {
                        tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_for_log, target = %requester_for_log, %room_id, "join request rejected");
                        (RoomResponse::Success, tx)
                    }
                    Err(e) => {
                        tracing::error!(error = %e, %room_id, "reject_join_request: commit failed");
                        (RoomResponse::Failed, tx)
                    }
                },
            }
        }

        RoomRequest::InviteToRoom {
            source_user_id,
            room_name,
            invitee_username,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "invite_to_room: begin transaction failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Captured for audit/denial logs; invitee_username is consumed below.
            let invitee_for_log = invitee_username.clone();

            // Authorize: only an owner (or admin) may invite. Missing room and
            // not-authorized both return Failed so existence isn't leaked.
            let (room_id, authorized) = match gate_room(&mut db, source_user_id, &room_name).await {
                Ok(Some(gate)) => gate,
                // No such room: existence-hidden, not a fault.
                Ok(None) => {
                    tracing::debug!(room = %room_name, "invite_to_room: no such room");
                    return (RoomResponse::Failed, tx);
                }
                Err(e) => {
                    tracing::error!(error = %e, "invite_to_room: room authorization lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            if !authorized {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_name, "invite_to_room denied: caller is not an owner or admin");
                return (RoomResponse::Failed, tx);
            }

            // Resolve the invitee. No such user -> Failed.
            let invitee_id = match sqlx::query_scalar::<_, Uuid>(
                "SELECT user_id FROM users WHERE LOWER(username) = LOWER(trim_ws($1))",
            )
            .bind(invitee_username)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    tracing::error!(error = %e, "invite_to_room: invitee lookup failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            let Some(invitee_id) = invitee_id else {
                tracing::debug!(target = %invitee_for_log, "invite_to_room: invitee not found");
                return (RoomResponse::Failed, tx);
            };

            // Record the invite, but not for someone already a member. ON CONFLICT
            // DO NOTHING drops a duplicate invite. 0 rows == already a member or
            // already invited -> NoChange.
            let result = match sqlx::query(
                "INSERT INTO room_invites (room_id, user_id, invited_by)
                    SELECT $1, $2, $3
                    WHERE NOT EXISTS (
                        SELECT 1 FROM memberships m
                        WHERE m.room_id = $1 AND m.user_id = $2)
                    ON CONFLICT DO NOTHING",
            )
            .bind(room_id)
            .bind(invitee_id)
            .bind(source_user_id)
            .execute(&mut *db)
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!(error = %e, %room_id, %invitee_id, "invite_to_room: invite insert failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            match result.rows_affected() {
                1 => match db.commit().await {
                    Ok(_) => {
                        tracing::info!(target: crate::logging::AUDIT, actor = %source_user_id, room = %room_name, target = %invitee_for_log, %invitee_id, %room_id, "user invited to room");
                        (RoomResponse::Success, tx)
                    }
                    Err(e) => {
                        tracing::error!(error = %e, %room_id, "invite_to_room: commit failed");
                        (RoomResponse::Failed, tx)
                    }
                },
                0 => (RoomResponse::NoChange, tx),
                _ => unreachable!(),
            }
        }

        RoomRequest::GetMyInvites { source_user_id, tx } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "get_my_invites: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Rooms the caller has been invited to -- just the names.
            let rooms = match sqlx::query_scalar::<_, String>(
                "SELECT r.room_name
                    FROM room_invites ri
                    JOIN rooms r ON r.room_id = ri.room_id
                    WHERE ri.user_id = $1
                    ORDER BY ri.created_at",
            )
            .bind(source_user_id)
            .fetch_all(&mut *db)
            .await
            {
                Ok(rooms) => rooms,
                Err(e) => {
                    tracing::error!(error = %e, "get_my_invites: query failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            (RoomResponse::MyInvites { rooms }, tx)
        }

        RoomRequest::AcceptInvite {
            user_id,
            room_name,
            tx,
        } => {
            let mut db: sqlx::Transaction<'_, sqlx::Postgres> = match pool.begin().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "accept_invite: begin transaction failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Consume the caller's invite, resolving the room by name. None means
            // there was no invite for this caller -> nothing to accept.
            let accepted_room_id = match sqlx::query_scalar::<_, Uuid>(
                "DELETE FROM room_invites
                    WHERE user_id = $1
                    AND room_id = (SELECT room_id FROM rooms
                                   WHERE LOWER(room_name) = LOWER(trim_ws($2)))
                    RETURNING room_id",
            )
            .bind(user_id)
            .bind(room_name)
            .fetch_optional(&mut *db)
            .await
            {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    tracing::error!(error = %e, "accept_invite: invite consume failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            let Some(room_id) = accepted_room_id else {
                return (RoomResponse::NoChange, tx);
            };

            // Join the room. ON CONFLICT DO NOTHING in case membership already
            // exists by some other path.
            // Start caught up: watermark = newest message at accept time.
            if let Err(e) = sqlx::query(
                "INSERT INTO memberships (room_id, user_id, last_read_message_id)
                    VALUES ($1, $2,
                            (SELECT message_id FROM messages
                             WHERE room_id = $1 ORDER BY message_id DESC LIMIT 1))
                    ON CONFLICT DO NOTHING",
            )
            .bind(room_id)
            .bind(user_id)
            .execute(&mut *db)
            .await
            {
                tracing::error!(error = %e, %room_id, "accept_invite: membership insert failed");
                return (RoomResponse::Failed, tx);
            }

            match db.commit().await {
                Ok(_) => (RoomResponse::Success, tx),
                Err(e) => {
                    tracing::error!(error = %e, %room_id, "accept_invite: commit failed");
                    (RoomResponse::Failed, tx)
                }
            }
        }

        RoomRequest::DeclineInvite {
            user_id,
            room_name,
            tx,
        } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "decline_invite: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Drop the caller's invite. Zero rows -> no such invite -> NoChange.
            let result = match sqlx::query(
                "DELETE FROM room_invites
                    WHERE user_id = $1
                    AND room_id = (SELECT room_id FROM rooms
                                   WHERE LOWER(room_name) = LOWER(trim_ws($2)))",
            )
            .bind(user_id)
            .bind(room_name)
            .execute(&mut *db)
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!(error = %e, "decline_invite: invite delete failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            match result.rows_affected() {
                1 => (RoomResponse::Success, tx),
                0 => (RoomResponse::NoChange, tx),
                _ => unreachable!(),
            }
        }

        RoomRequest::ListDiscoverableRooms { tx } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "list_discoverable_rooms: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // All rooms that are public or discoverable, with a live member count.
            // Private non-discoverable rooms are excluded -- their existence must
            // not be leaked to non-members.
            let rooms = match sqlx::query_as::<_, DiscoverableRoom>(
                "SELECT r.room_name, r.is_public,
                        COUNT(m.user_id) AS member_count
                    FROM rooms r
                    LEFT JOIN memberships m ON m.room_id = r.room_id
                    WHERE r.is_public OR r.is_discoverable
                    GROUP BY r.room_id
                    ORDER BY member_count DESC, r.room_name",
            )
            .fetch_all(&mut *db)
            .await
            {
                Ok(rooms) => rooms,
                Err(e) => {
                    tracing::error!(error = %e, "list_discoverable_rooms: query failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            (RoomResponse::DiscoverableRooms { rooms }, tx)
        }

        RoomRequest::ListAllRooms { source_user_id, tx } => {
            let mut db = match pool.acquire().await {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(error = %e, "list_all_rooms: acquire connection failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            // Admin only: listing every room (private ones included) is a
            // moderation capability. A non-admin caller is rejected outright.
            let is_admin: bool = match sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM admins a WHERE a.user_id = $1)",
            )
            .bind(source_user_id)
            .fetch_one(&mut *db)
            .await
            {
                Ok(is_admin) => is_admin,
                Err(e) => {
                    tracing::error!(error = %e, "list_all_rooms: admin check failed");
                    return (RoomResponse::Failed, tx);
                }
            };
            if !is_admin {
                tracing::warn!(target: crate::logging::AUDIT, actor = %source_user_id, "list_all_rooms denied: caller is not an admin");
                return (RoomResponse::Failed, tx);
            }

            // Every room, with a live member count -- no visibility filter.
            let rooms = match sqlx::query_as::<_, DiscoverableRoom>(
                "SELECT r.room_name, r.is_public,
                        COUNT(m.user_id) AS member_count
                    FROM rooms r
                    LEFT JOIN memberships m ON m.room_id = r.room_id
                    GROUP BY r.room_id
                    ORDER BY member_count DESC, r.room_name",
            )
            .fetch_all(&mut *db)
            .await
            {
                Ok(rooms) => rooms,
                Err(e) => {
                    tracing::error!(error = %e, "list_all_rooms: query failed");
                    return (RoomResponse::Failed, tx);
                }
            };

            (RoomResponse::AllRooms { rooms }, tx)
        }
    }
}

// Resolve a room by name and decide whether `source_user_id` may act on it as an
// owner or admin, locking the room row (FOR UPDATE) for the rest of the caller's
// transaction. Returns None when no such room exists; the boolean is the
// owner-or-admin authorization. Callers must run inside a transaction for the
// lock to hold.
async fn gate_room(
    db: &mut sqlx::PgConnection,
    source_user_id: Uuid,
    room_name: &str,
) -> Result<Option<(Uuid, bool)>, sqlx::Error> {
    sqlx::query_as::<_, (Uuid, bool)>(
        "SELECT
            r.room_id,
            (EXISTS (SELECT 1 FROM memberships m
                     WHERE m.room_id = r.room_id AND m.user_id = $1 AND m.is_owner)
             OR EXISTS (SELECT 1 FROM admins a WHERE a.user_id = $1)) AS authorized
        FROM rooms r
        WHERE LOWER(r.room_name) = LOWER(trim_ws($2))
        FOR UPDATE OF r",
    )
    .bind(source_user_id)
    .bind(room_name)
    .fetch_optional(db)
    .await
}
