-- Strip leading/trailing whitespace of every kind (space, tab, newline, CR,
-- form feed, vertical tab) -- unlike SQL TRIM, which only removes spaces.
-- Used to normalize user input on write and lookup.
CREATE FUNCTION trim_ws(text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$ SELECT regexp_replace($1, '^\s+|\s+$', '', 'g') $$;

CREATE TABLE users (
    user_id          UUID         PRIMARY KEY DEFAULT uuidv7(),
    username         TEXT         NOT NULL CHECK (username <> ''),
    first_name       TEXT         CHECK (first_name <> ''),
    last_name        TEXT         CHECK (last_name <> ''),
    alias            TEXT         CHECK (alias <> ''),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX users_username_lower ON users (LOWER(username));

CREATE TABLE credentials (
    user_id            UUID         PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    password_hash      TEXT         NOT NULL,
    password_last_set  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE admins (
    user_id     UUID         PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    granted_by  UUID         REFERENCES users(user_id) ON DELETE SET NULL,
    granted_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    is_default  BOOLEAN      NOT NULL DEFAULT false
);

CREATE UNIQUE INDEX admins_one_default ON admins (is_default) WHERE is_default;

CREATE TABLE last_active (
    user_id      UUID         PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    last_active  TIMESTAMPTZ  NOT NULL
);

CREATE TABLE rooms (
    room_id          UUID         PRIMARY KEY DEFAULT uuidv7(),
    room_name        TEXT         NOT NULL CHECK (room_name <> ''),
    -- is_public:       anyone may join directly (no invite/request needed).
    --                  Private rooms admit members only via invite or approved request.
    -- is_discoverable: the room appears in listings/search. Independent of is_public,
    --                  so a room can be public-but-unlisted or private-but-visible.
    -- Defaults are "closed": callers opt in to openness explicitly.
    is_public        BOOLEAN      NOT NULL DEFAULT false,
    is_discoverable  BOOLEAN      NOT NULL DEFAULT false
);

CREATE UNIQUE INDEX rooms_room_name_lower ON rooms (LOWER(room_name));

-- Ownership lives here, not on rooms: a room may have multiple owners, and an
-- owner is always also a member. A room may also have NO owners (e.g. after its
-- sole owner is deleted) -- that is allowed, not an error. Every owner-gated
-- action also permits admins, so an ownerless room stays manageable and an admin
-- can re-grant ownership to a member.
CREATE TABLE memberships (
    room_id    UUID         NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    user_id    UUID         NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    is_owner   BOOLEAN      NOT NULL DEFAULT false,
    joined_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- Read high-water mark: the newest message_id this member has read in the room.
    -- A bare uuidv7 cursor, deliberately NOT a foreign key -- "unread" is the value
    -- comparison message_id > last_read_message_id, and uuidv7's byte order is
    -- chronological, so the cursor stays meaningful even after the message it once
    -- pointed at is reaped. NULL = nothing read yet (everything counts as unread).
    last_read_message_id UUID,
    PRIMARY KEY (room_id, user_id)
);

-- Fast lookup of a room's owners (e.g. to list them or check is_owner).
CREATE INDEX memberships_room_owners ON memberships (room_id) WHERE is_owner;

-- Pending invitations: an owner invites a user into a room. Row present = invite
-- pending; accepting inserts a membership and deletes the row, declining just
-- deletes it. invited_by is kept for audit and survives the inviter's deletion.
CREATE TABLE room_invites (
    room_id     UUID         NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    user_id     UUID         NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    invited_by  UUID         REFERENCES users(user_id) ON DELETE SET NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (room_id, user_id)
);

-- Pending join requests: a user asks to join a room. Row present = request
-- pending; an owner approving inserts a membership and deletes the row, denying
-- just deletes it.
CREATE TABLE room_join_requests (
    room_id     UUID         NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    user_id     UUID         NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (room_id, user_id)
);

CREATE TABLE messages (
    message_id               UUID         PRIMARY KEY DEFAULT uuidv7(),
    room_id                  UUID         NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    sender_id                UUID         REFERENCES users(user_id) ON DELETE SET NULL,
    sender_username_snapshot TEXT         CHECK (sender_username_snapshot <> ''),
    content                  TEXT         NOT NULL CHECK (content <> ''),
    timestamp                TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CHECK (sender_id IS NOT NULL OR sender_username_snapshot IS NOT NULL)
);

CREATE INDEX messages_room_timestamp ON messages (room_id, timestamp DESC);

-- Attachments and reactions: child rows of a message, lazy-loaded by the client.
-- Both cascade off messages, so the time-based reaper that ages out messages
-- cleans up *completed* attachments and reactions for free -- no new reaper rule
-- for those. Incomplete uploads are the exception (see is_complete below).

-- One attachment on a message. The bytes are NOT stored here -- they arrive as
-- chunks (message_attachment_chunks) after the message is committed, streamed by
-- the sender against the returned attachment_id. The row is created up front in
-- an incomplete state alongside the message; is_complete flips true once every
-- chunk is present and the streamed SHA-256 matches content_sha256.
--
-- Authorization derives from the parent message, not a column here: only
-- messages.sender_id may upload chunks (write), and any member of the message's
-- room may download them (read).
CREATE TABLE message_attachments (
    attachment_id   UUID         PRIMARY KEY DEFAULT uuidv7(),
    message_id      UUID         NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    filename        TEXT         NOT NULL CHECK (filename <> ''),
    content_type    TEXT         NOT NULL CHECK (content_type <> ''),   -- client-declared MIME
    size_bytes      BIGINT       NOT NULL CHECK (size_bytes > 0),       -- client-declared total
    chunk_count     INTEGER      NOT NULL CHECK (chunk_count > 0),      -- client-declared # of chunks
    content_sha256  BYTEA        NOT NULL CHECK (octet_length(content_sha256) = 32),  -- verified on completion
    is_complete     BOOLEAN      NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX message_attachments_message ON message_attachments (message_id);

-- Reaper target: uploads abandoned mid-stream. Their parent message is young, so
-- they won't age out via the message rule -- the reaper sweeps incomplete rows
-- older than its grace period (~24h), and their chunks cascade.
CREATE INDEX message_attachments_incomplete ON message_attachments (created_at) WHERE NOT is_complete;

-- One chunk of an attachment. seq is the client-supplied order index; the PK
-- makes re-sends idempotent (upsert at a seq) so a stalled upload resumes by
-- filling only the missing seqs rather than restarting. Bytes stay chunked end to
-- end -- never reassembled into one blob -- so both upload and download stream
-- without holding the whole file in memory; even completion-time hashing reads
-- the chunks in seq order through the hasher.
CREATE TABLE message_attachment_chunks (
    attachment_id  UUID     NOT NULL REFERENCES message_attachments(attachment_id) ON DELETE CASCADE,
    seq            INTEGER  NOT NULL CHECK (seq >= 0),
    data           BYTEA    NOT NULL CHECK (octet_length(data) > 0),
    PRIMARY KEY (attachment_id, seq)
);

-- One reaction: a (message, user, emoji) triple. The composite PK enforces one
-- of each emoji per user per message and makes add/toggle idempotent. A reaction
-- carries no value once its author is gone, so user_id CASCADEs -- unlike a
-- message's sender, which is preserved via sender_username_snapshot.
CREATE TABLE message_reactions (
    message_id  UUID         NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    user_id     UUID         NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    emoji       TEXT         NOT NULL CHECK (emoji <> ''),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, user_id, emoji)
);

CREATE INDEX message_reactions_message ON message_reactions (message_id);
