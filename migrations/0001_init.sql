CREATE TABLE devices (
    id TEXT PRIMARY KEY,
    apns_token TEXT NOT NULL,
    platform TEXT NOT NULL DEFAULT 'ios',
    app_version TEXT NOT NULL DEFAULT '',
    -- Quiet hours: local hour ints in tz; NULL = disabled
    quiet_start INTEGER,
    quiet_end INTEGER,
    tz TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE watches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    callsign TEXT NOT NULL,
    dmr_id INTEGER NOT NULL DEFAULT 0,
    label TEXT NOT NULL DEFAULT '',
    -- JSON array of talkgroup numbers; empty = any
    tgs TEXT NOT NULL DEFAULT '[]'
);

-- Cooldown state survives restarts so a redeploy doesn't re-notify.
-- Keyed by a stable watch key (callsign or dmr id), not the watch row id,
-- because PUT replaces the whole watch list and re-mints row ids.
CREATE TABLE notify_state (
    device_id TEXT NOT NULL,
    watch_key TEXT NOT NULL,
    last_push_at INTEGER NOT NULL,
    PRIMARY KEY (device_id, watch_key)
);
