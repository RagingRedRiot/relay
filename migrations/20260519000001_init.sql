CREATE TABLE users (
    user_id          UUID         PRIMARY KEY DEFAULT uuidv7(),
    username         TEXT         NOT NULL UNIQUE,
    first_name       TEXT,
    last_name        TEXT,
    alias            TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

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
    room_id    UUID  PRIMARY KEY DEFAULT uuidv7(),
    room_name  TEXT  NOT NULL,
    owner_id   UUID  NOT NULL REFERENCES users(user_id)
);

CREATE TABLE memberships (
    room_id  UUID  NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    user_id  UUID  NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    PRIMARY KEY (room_id, user_id)
);

CREATE TABLE messages (
    message_id               UUID         PRIMARY KEY DEFAULT uuidv7(),
    room_id                  UUID         NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    sender_id                UUID         REFERENCES users(user_id) ON DELETE SET NULL,
    sender_username_snapshot TEXT,
    content                  TEXT         NOT NULL,
    timestamp                TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CHECK (sender_id IS NOT NULL OR sender_username_snapshot IS NOT NULL)
);

CREATE INDEX messages_room_timestamp ON messages (room_id, timestamp DESC);
