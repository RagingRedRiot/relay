// Types mirror the Rust model exactly so JSON round-trips without transforms.

export interface MessageHistoryItem {
  message_id: string;
  sender_username: string;
  content: string;
  timestamp: string;
  attachments: AttachmentSummary[];
  reactions: ReactionSummary[];
}

export interface AttachmentSummary {
  attachment_id: string;
  filename: string;
  content_type: string;
  size_bytes: number;
  is_complete: boolean;
}

export interface ReactionSummary {
  emoji: string;
  count: number;
  reacted_by_me: boolean;
}

export interface RoomUnread {
  room_name: string;
  unread: number;
}

export interface PublicUser {
  first_name: string | null;
  last_name: string | null;
  alias: string | null;
  username: string;
  created_at: string;
}

// One entry in a GetUsers directory page. `is_admin` is present only when the
// requester is an admin (the admin pane); for a regular caller it's omitted, so
// treat `undefined` as "unknown / not exposed", distinct from `false`.
export interface UserDirectoryEntry {
  first_name: string | null;
  last_name: string | null;
  alias: string | null;
  username: string;
  created_at: string;
  is_admin?: boolean;
}

// A room member: public profile fields plus whether they own the room.
export interface RoomMember {
  first_name: string | null;
  last_name: string | null;
  alias: string | null;
  username: string;
  created_at: string;
  is_owner: boolean;
}

export interface JoinRequestInfo {
  room_name: string;
  username: string;
}

export interface DiscoverableRoom {
  room_name: string;
  is_public: boolean;
  member_count: number;
}

export interface NewMessageAttachment {
  filename: string;
  content_type: string;
  size_bytes: number;
  chunk_count: number;
  content_sha256: number[];
}

// Parsed from a binary WebSocket frame: [uuid 16B][seq u32 BE 4B][payload].
export interface BinaryChunk {
  attachment_id: string;
  seq: number;
  data: Uint8Array;
}

// Matches Rust serde enum serialization:
// - unit variants → plain string  e.g. "GetMaxChunkSize"
// - struct variants → {"VariantName": {fields}}
export type ClientCommand =
  | 'GetMaxChunkSize'
  | 'GetUnreadSummary'
  | 'GetMyJoinRequests'
  | 'GetIncomingJoinRequests'
  | 'GetMyInvites'
  | 'RestartServer'
  | 'ShutdownServer'
  | 'Close'
  | { Auth: { username: string; password: string } }
  | { Echo: { string: string } }
  | { SendMessage: { room_name: string; content: string; attachments?: NewMessageAttachment[] } }
  | { DownloadAttachment: { attachment_id: string } }
  | { AddReaction: { message_id: string; emoji: string } }
  | { RemoveReaction: { message_id: string; emoji: string } }
  | { DeleteMessage: { message_id: string } }
  | { GetMessages: { room_name: string; before?: string; limit?: number } }
  | { MarkRead: { room_name: string; up_to_message_id: string } }
  | { NewUser: { username: string; password: string; first_name?: string; last_name?: string; alias?: string } }
  | { GetUserByUsername: { username: string } }
  | { GetUsers: { starts_with?: string; after?: string; limit?: number } }
  | { EditUser: { target_username: string; username?: string; first_name?: string; last_name?: string; alias?: string } }
  | { Promote: { target_username: string } }
  | { Demote: { target_username: string } }
  | { DeleteUser: { target_username: string } }
  | { UpdatePassword: { current_password: string; new_password: string } }
  | { ResetPassword: { target_username: string; new_password: string } }
  | { NewRoom: { room_name: string; is_public?: boolean; is_discoverable?: boolean } }
  | { AddRoomOwner: { room_name: string; new_owner_username: string } }
  | { SetRoomName: { current_name: string; new_name: string } }
  | { GetRoomMembership: { room_name: string } }
  | { GetRoom: { room_name: string } }
  | { JoinRoom: { room_name: string } }
  | { LeaveRoom: { room_name: string } }
  | { RemoveRoomMember: { room_name: string; member_username: string } }
  | { CancelJoinRequest: { room_name: string } }
  | { ApproveJoinRequest: { room_name: string; requester_username: string } }
  | { RejectJoinRequest: { room_name: string; requester_username: string } }
  | { InviteToRoom: { room_name: string; invitee_username: string } }
  | { AcceptInvite: { room_name: string } }
  | { DeclineInvite: { room_name: string } }
  | 'ListDiscoverableRooms'
  | 'ListAllRooms'
  | 'GetSignupStatus'
  | { Error: { error: string } };

// Maps every ServerEvent variant name to its payload type.
// void = unit variant (no payload). Used to type the event emitter.
export interface ServerEventMap {
  AuthOk: { is_admin: boolean };
  NoAuth: void;
  UserCreated: void;
  NoChange: void;
  NoUserExists: void;
  NoRoomExists: void;
  RoomCreated: void;
  JoinRequested: void;
  Success: void;
  Failed: void;
  Echo: { string: string };
  MessageCreated: { message_id: string; attachment_ids: string[]; message: MessageHistoryItem };
  AttachmentComplete: { attachment_id: string };
  AttachmentRejected: { attachment_id: string; reason: string };
  MaxChunkSize: { bytes: number };
  SignupStatus: { open_signups: boolean };
  AttachmentEnd: { attachment_id: string };
  MessageHistory: { room_name: string; messages: MessageHistoryItem[] };
  UnreadSummary: { rooms: RoomUnread[] };
  NewMessage: { room_name: string; message: MessageHistoryItem };
  MessageRemoved: { room_name: string; message_id: string };
  Resync: { room_name: string };
  Close: { reason: string };
  Error: { error: string };
  RateLimit: { error: string };
  UserInfo: { first_name: string | null; last_name: string | null; alias: string | null; username: string; created_at: string };
  RoomMembers: { members: RoomMember[] };
  RoomInfo: { room_name: string; is_public: boolean; is_discoverable: boolean };
  MyJoinRequests: { rooms: string[] };
  IncomingJoinRequests: { requests: JoinRequestInfo[] };
  MyInvites: { rooms: string[] };
  DiscoverableRooms: { rooms: DiscoverableRoom[] };
  AllRooms: { rooms: DiscoverableRoom[] };
  Users: { users: UserDirectoryEntry[]; has_more: boolean };
  // Synthetic: emitted for incoming binary frames rather than JSON events.
  _BinaryChunk: BinaryChunk;
}

export type ConnectionState = 'connecting' | 'connected' | 'disconnected';
