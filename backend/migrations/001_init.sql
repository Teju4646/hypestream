-- HypeStream PostgreSQL schema.
-- Phase 1 runs in memory with the same rules; Phase 2 moves storage here.

CREATE TABLE users (
    login       TEXT PRIMARY KEY CHECK (login ~ '^[a-z0-9_]{3,25}$'),
    email       TEXT UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per user. The CHECK makes overspending impossible even if two
-- requests race: the second UPDATE fails instead of going negative.
CREATE TABLE wallets (
    user_login  TEXT PRIMARY KEY REFERENCES users(login),
    bits        INTEGER NOT NULL DEFAULT 0 CHECK (bits >= 0),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only money history. Rows are never updated or deleted, so the
-- sum of bits_delta for a user always equals their wallet balance.
CREATE TABLE ledger_entries (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    entry_type  TEXT NOT NULL CHECK (entry_type IN ('bits_purchase', 'cheer', 'subscription', 'gift_sub')),
    user_login  TEXT NOT NULL REFERENCES users(login),
    channel     TEXT REFERENCES users(login),
    recipient   TEXT REFERENCES users(login),
    bits_delta  INTEGER NOT NULL DEFAULT 0,
    tier        SMALLINT CHECK (tier BETWEEN 1 AND 3),
    points      INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX ledger_user_idx    ON ledger_entries (user_login, created_at DESC);
CREATE INDEX ledger_channel_idx ON ledger_entries (channel, created_at DESC);

-- A retry with the same key finds this row and returns the saved receipt.
-- The primary key makes two parallel retries impossible to both succeed.
CREATE TABLE idempotency_keys (
    user_login   TEXT NOT NULL REFERENCES users(login),
    key          TEXT NOT NULL CHECK (length(key) BETWEEN 8 AND 100),
    fingerprint  TEXT NOT NULL,
    receipt      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_login, key)
);

CREATE TABLE subscriptions (
    user_login  TEXT NOT NULL REFERENCES users(login),
    channel     TEXT NOT NULL REFERENCES users(login),
    tier        SMALLINT NOT NULL CHECK (tier BETWEEN 1 AND 3),
    gifted_by   TEXT REFERENCES users(login),
    months      INTEGER NOT NULL DEFAULT 1,
    started_at  TIMESTAMPTZ NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (channel, user_login),
    CHECK (user_login <> channel)
);
CREATE INDEX subscriptions_expiry_idx ON subscriptions (expires_at);

-- History of finished trains for the creator dashboard.
CREATE TABLE hype_trains (
    id           BIGSERIAL PRIMARY KEY,
    channel      TEXT NOT NULL REFERENCES users(login),
    started_at   TIMESTAMPTZ NOT NULL,
    ended_at     TIMESTAMPTZ,
    final_level  INTEGER NOT NULL DEFAULT 1,
    total_points INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX hype_trains_channel_idx ON hype_trains (channel, started_at DESC);
