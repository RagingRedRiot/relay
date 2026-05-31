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
