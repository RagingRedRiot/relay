use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

use crate::model::ServerEvent;

// Per-room broadcast ring size. A subscriber that falls this far behind gets
// Lagged(n) instead of events; its session then re-syncs that room from history
// (GetMessages) and its read watermark keeps the unread count exact. Sized for a
// burst, not for durability -- the DB is the source of truth, this is best-effort
// live delivery.
const ROOM_BROADCAST_CAPACITY: usize = 128;

// One live session's subscription-control channel, tagged with a unique id so the
// right entry can be removed when that session ends.
struct SessionEntry {
    id: Uuid,
    sub_tx: mpsc::Sender<Subscription>,
}

// Shared, cloneable handle to the live fan-out registry. Lives in AppState; the
// message actor holds a clone to publish, the room actor to reach a just-admitted
// user's sessions, and every session holds one to subscribe.
#[derive(Clone, Default)]
pub struct Hub {
    // room_id -> the room's live sender. Keyed on the IMMUTABLE id, never the
    // (renameable, normalized) name. Created lazily on first subscribe; reclaimed
    // when the last receiver goes away. A std Mutex is fine: every op below is sync
    // and non-blocking, so the lock is never held across an .await.
    rooms: Arc<Mutex<HashMap<Uuid, broadcast::Sender<Arc<ServerEvent>>>>>,
    // user_id -> that user's live sessions' control channels. Lets the server push
    // a subscription to a user's open sessions when they're admitted to a room by
    // someone else (a request approval). Entries are removed when a session ends
    // (via SessionGuard).
    sessions: Arc<Mutex<HashMap<Uuid, Vec<SessionEntry>>>>,
}

impl Hub {
    pub fn new() -> Self {
        Self::default()
    }

    // Subscribe to a room's live stream, creating the channel on first use. The
    // returned Receiver buffers from this instant, so an event published between
    // here and the sender task inserting it into its StreamMap is not lost.
    pub fn subscribe(&self, room_id: Uuid) -> broadcast::Receiver<Arc<ServerEvent>> {
        let mut rooms = self.rooms.lock().unwrap();
        rooms
            .entry(room_id)
            .or_insert_with(|| broadcast::channel(ROOM_BROADCAST_CAPACITY).0)
            .subscribe()
    }

    // Publish to a room's current subscribers. No channel == nobody connected ==
    // nothing to do (we don't even allocate). Self-pruning: send() errs only when
    // every receiver has dropped, which is our cue to reclaim the slot. Lossy by
    // design -- a full ring lags slow receivers, it never blocks the publisher.
    pub fn publish(&self, room_id: Uuid, event: ServerEvent) {
        let mut rooms = self.rooms.lock().unwrap();
        if let Some(tx) = rooms.get(&room_id)
            && tx.send(Arc::new(event)).is_err()
        {
            rooms.remove(&room_id);
        }
    }

    // Register a session's subscription-control channel under its user. The
    // returned guard deregisters it on drop -- hold it for the session's lifetime.
    pub fn register_session(
        &self,
        user_id: Uuid,
        sub_tx: mpsc::Sender<Subscription>,
    ) -> SessionGuard {
        let id = Uuid::now_v7();
        self.sessions
            .lock()
            .unwrap()
            .entry(user_id)
            .or_default()
            .push(SessionEntry { id, sub_tx });
        SessionGuard {
            hub: self.clone(),
            user_id,
            session_id: id,
        }
    }

    fn deregister_session(&self, user_id: Uuid, session_id: Uuid) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(entries) = sessions.get_mut(&user_id) {
            entries.retain(|e| e.id != session_id);
            if entries.is_empty() {
                sessions.remove(&user_id);
            }
        }
    }

    // Subscribe every currently-connected session of `user_id` to `room_id`'s live
    // stream. Used when a user is admitted to a room by someone else (a request
    // approval), so their open sessions get live events without reconnecting. A
    // closed session's send simply fails; it's cleaned up when that session's guard
    // deregisters. Offline users have no sessions and subscribe on next connect.
    pub fn subscribe_user_to_room(&self, user_id: Uuid, room_id: Uuid, room_name: String) {
        // Snapshot the senders, then release the lock before subscribing/sending so
        // we never hold the sessions lock across the rooms lock or a channel send.
        let senders: Vec<mpsc::Sender<Subscription>> = {
            let sessions = self.sessions.lock().unwrap();
            match sessions.get(&user_id) {
                Some(entries) => entries.iter().map(|e| e.sub_tx.clone()).collect(),
                None => return,
            }
        };
        for sub_tx in senders {
            let rx = self.subscribe(room_id);
            let _ = sub_tx.try_send(Subscription::Add {
                room_id,
                room_name: room_name.clone(),
                rx,
            });
        }
    }

    // Drop `room_id`'s live stream from every currently-connected session of
    // `user_id`. The mirror of subscribe_user_to_room: used when a user is removed
    // from a room by someone else (a kick), so their open sessions stop receiving
    // the room's messages immediately rather than at their next reconnect. A closed
    // session's send simply fails; it's cleaned up when that session's guard
    // deregisters. Offline users have no sessions and re-subscribe (as a member)
    // only if re-admitted.
    pub fn unsubscribe_user_from_room(&self, user_id: Uuid, room_id: Uuid) {
        let senders: Vec<mpsc::Sender<Subscription>> = {
            let sessions = self.sessions.lock().unwrap();
            match sessions.get(&user_id) {
                Some(entries) => entries.iter().map(|e| e.sub_tx.clone()).collect(),
                None => return,
            }
        };
        for sub_tx in senders {
            let _ = sub_tx.try_send(Subscription::Remove { room_id });
        }
    }
}

// Deregisters a session from the Hub's presence registry when dropped, so a session
// ending (any path) cleans up without explicit bookkeeping.
pub struct SessionGuard {
    hub: Hub,
    user_id: Uuid,
    session_id: Uuid,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.hub.deregister_session(self.user_id, self.session_id);
    }
}

// Control signal from a session's receiver task to its sender task: which rooms'
// live streams to merge in or drop. Carried on a dedicated channel so the existing
// user_tx (mpsc<ServerEvent>, held in many places) stays untouched.
pub enum Subscription {
    Add {
        room_id: Uuid,
        room_name: String, // stashed for the Lagged -> Resync hint
        rx: broadcast::Receiver<Arc<ServerEvent>>,
    },
    Remove {
        room_id: Uuid,
    },
}
