-- Email accounts with magic-link login. A device is "claimed" once
-- account_id is set; claimed devices match against the account's watch
-- list, unclaimed devices keep their per-device list in `watches`.
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL
);

ALTER TABLE devices ADD COLUMN account_id INTEGER REFERENCES accounts(id);

CREATE TABLE account_watches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    callsign TEXT NOT NULL,
    dmr_id INTEGER NOT NULL DEFAULT 0,
    label TEXT NOT NULL DEFAULT '',
    -- JSON array of talkgroup numbers; empty = any
    tgs TEXT NOT NULL DEFAULT '[]'
);

-- Magic-link tokens: raw token only ever lives in the email; we store
-- its SHA-256. device_id set = an iOS claim link, NULL = web login.
CREATE TABLE login_tokens (
    token_hash TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    device_id TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    used_at INTEGER
);

CREATE TABLE sessions (
    token_hash TEXT PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

-- Every APNs send attempt, for the web history page. account_id is
-- denormalized at send time; pre-claim rows are picked up via device_id.
CREATE TABLE notification_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    account_id INTEGER,
    callsign TEXT NOT NULL,
    dmr_id INTEGER NOT NULL DEFAULT 0,
    talkgroup INTEGER NOT NULL DEFAULT 0,
    talkgroup_name TEXT NOT NULL DEFAULT '',
    event_time INTEGER NOT NULL,
    sent_at INTEGER NOT NULL,
    outcome TEXT NOT NULL
);
CREATE INDEX idx_notification_log_account ON notification_log(account_id, sent_at DESC);
CREATE INDEX idx_notification_log_device ON notification_log(device_id, sent_at DESC);
