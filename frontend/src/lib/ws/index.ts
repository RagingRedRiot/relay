import { writable, derived, type Readable } from 'svelte/store';
import { RelayConnection } from './connection';
import type { ClientCommand, ConnectionState, ServerEventMap, RoomUnread } from './types';

export type { ClientCommand, ServerEventMap, ConnectionState };
export type {
  MessageHistoryItem,
  AttachmentSummary,
  ReactionSummary,
  RoomUnread,
  PublicUser,
  RoomMember,
  UserDirectoryEntry,
  JoinRequestInfo,
  NewMessageAttachment,
  BinaryChunk,
  DiscoverableRoom,
} from './types';

// ── Reactive state ────────────────────────────────────────────────────────────

const _state = writable<ConnectionState>('disconnected');
const _username = writable<string | null>(null);
const _isAdmin = writable<boolean>(false);
const _unreadRooms = writable<RoomUnread[]>([]);
const _signupsOpen = writable<boolean>(false);

export const connectionState: Readable<ConnectionState> = _state;
export const currentUsername: Readable<string | null> = _username;
export const authed: Readable<boolean> = derived(_username, ($u) => $u !== null);
// Whether the logged-in user is an admin. Set from the AuthOk payload at login,
// cleared on any session termination.
export const isAdmin: Readable<boolean> = _isAdmin;
export const unreadRooms: Readable<RoomUnread[]> = _unreadRooms;
// Whether the server has open signups. Queried automatically once the socket
// connects (pre-auth), so the login screen knows whether to offer registration.
export const signupsOpen: Readable<boolean> = _signupsOpen;

// ── Connection singleton ──────────────────────────────────────────────────────

let conn: RelayConnection | null = null;
let unreadRefreshTimer: ReturnType<typeof setTimeout> | null = null;

function requestUnreadSummarySoon() {
  if (unreadRefreshTimer !== null) clearTimeout(unreadRefreshTimer);
  unreadRefreshTimer = setTimeout(() => {
    unreadRefreshTimer = null;
    conn?.send('GetUnreadSummary');
  }, 100);
}

export function connect(url = `ws://${location.host}/ws`) {
  conn?.disconnect();
  if (unreadRefreshTimer !== null) {
    clearTimeout(unreadRefreshTimer);
    unreadRefreshTimer = null;
  }
  conn = new RelayConnection(url);
  conn.onStateChange((s) => {
    _state.set(s);
    // Ask whether signups are open as soon as the socket is up (works pre-auth).
    if (s === 'connected') conn?.send('GetSignupStatus');
  });
  conn.on('SignupStatus', ({ open_signups }) => _signupsOpen.set(open_signups));
  // Capture admin status from the login ack.
  conn.on('AuthOk', ({ is_admin }) => {
    _isAdmin.set(is_admin);
    requestUnreadSummarySoon();
  });
  conn.on('UnreadSummary', ({ rooms }) => _unreadRooms.set(rooms));
  conn.on('NewMessage', requestUnreadSummarySoon);
  conn.on('MessageCreated', requestUnreadSummarySoon);
  // Clear auth on any server-initiated session termination.
  conn.on('NoAuth', () => { _username.set(null); _isAdmin.set(false); _unreadRooms.set([]); });
  conn.on('Close', () => { _username.set(null); _isAdmin.set(false); _unreadRooms.set([]); });
}

export function disconnect() {
  if (unreadRefreshTimer !== null) {
    clearTimeout(unreadRefreshTimer);
    unreadRefreshTimer = null;
  }
  conn?.disconnect();
  conn = null;
  _username.set(null);
  _isAdmin.set(false);
  _unreadRooms.set([]);
  _state.set('disconnected');
}

// ── Auth helpers ──────────────────────────────────────────────────────────────

// Call immediately after sending Auth so the store reflects the logged-in user
// once AuthOk arrives. The caller owns deciding when auth is confirmed.
export function setCurrentUser(username: string | null) {
  _username.set(username);
}

// ── Messaging ─────────────────────────────────────────────────────────────────

export function send(cmd: ClientCommand) {
  conn?.send(cmd);
}

export function sendChunk(attachmentId: string, seq: number, data: Uint8Array) {
  conn?.sendChunk(attachmentId, seq, data);
}

// ── Event subscriptions ───────────────────────────────────────────────────────

// Returns an unsubscribe function. Safe to call before connect(); returns a no-op.
export function on<K extends keyof ServerEventMap>(
  event: K,
  handler: (payload: ServerEventMap[K]) => void,
): () => void {
  return conn?.on(event, handler) ?? (() => {});
}
