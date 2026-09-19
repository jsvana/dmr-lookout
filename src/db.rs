//! SQLite persistence. The dataset is tiny (one device row, a handful of
//! watches); the hot path reads the in-memory WatchIndex, never this.

use crate::watch::{Device, Watch};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use std::str::FromStr;

pub async fn open(path: &str) -> anyhow::Result<SqlitePool> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{path}"))?
        .create_if_missing(true)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    Ok(pool)
}

pub async fn load_devices(pool: &SqlitePool) -> anyhow::Result<Vec<Device>> {
    let rows = sqlx::query(
        "SELECT id, apns_token, apns_env, account_id, quiet_start, quiet_end, tz FROM devices",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Device {
            id: row.get("id"),
            apns_token: row.get("apns_token"),
            apns_env: row.get("apns_env"),
            account_id: row.get("account_id"),
            quiet_start: row.get("quiet_start"),
            quiet_end: row.get("quiet_end"),
            tz: row.get("tz"),
        })
        .collect())
}

fn row_to_watch(row: sqlx::sqlite::SqliteRow) -> Watch {
    Watch {
        device_id: row.get("device_id"),
        callsign: row.get("callsign"),
        dmr_id: row.get::<i64, _>("dmr_id") as u32,
        label: row.get("label"),
        talkgroups: serde_json::from_str(row.get::<String, _>("tgs").as_str()).unwrap_or_default(),
    }
}

/// The watches the matcher indexes: per-device lists for unclaimed
/// devices, plus the account list fanned out to every claimed device.
pub async fn load_effective_watches(pool: &SqlitePool) -> anyhow::Result<Vec<Watch>> {
    let rows = sqlx::query(
        "SELECT w.device_id, w.callsign, w.dmr_id, w.label, w.tgs
         FROM watches w JOIN devices d ON d.id = w.device_id
         WHERE d.account_id IS NULL
         UNION ALL
         SELECT d.id AS device_id, aw.callsign, aw.dmr_id, aw.label, aw.tgs
         FROM account_watches aw JOIN devices d ON d.account_id = aw.account_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(row_to_watch).collect())
}

pub async fn load_device_watches(pool: &SqlitePool, device_id: &str) -> anyhow::Result<Vec<Watch>> {
    let rows = sqlx::query(
        "SELECT device_id, callsign, dmr_id, label, tgs FROM watches WHERE device_id = ?1",
    )
    .bind(device_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(row_to_watch).collect())
}

pub async fn upsert_device(
    pool: &SqlitePool,
    id: &str,
    apns_token: &str,
    apns_env: &str,
    platform: &str,
    app_version: &str,
    now: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO devices (id, apns_token, apns_env, platform, app_version, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
         ON CONFLICT(id) DO UPDATE SET
           apns_token = ?2, apns_env = ?3, platform = ?4, app_version = ?5, updated_at = ?6",
    )
    .bind(id)
    .bind(apns_token)
    .bind(apns_env)
    .bind(platform)
    .bind(app_version)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn replace_watches(
    pool: &SqlitePool,
    device_id: &str,
    watches: &[Watch],
) -> anyhow::Result<()> {
    let mut txn = pool.begin().await?;
    sqlx::query("DELETE FROM watches WHERE device_id = ?1")
        .bind(device_id)
        .execute(&mut *txn)
        .await?;
    for watch in watches {
        sqlx::query(
            "INSERT INTO watches (device_id, callsign, dmr_id, label, tgs)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(device_id)
        .bind(&watch.callsign)
        .bind(watch.dmr_id as i64)
        .bind(&watch.label)
        .bind(serde_json::to_string(&watch.talkgroups)?)
        .execute(&mut *txn)
        .await?;
    }
    txn.commit().await?;
    Ok(())
}

pub async fn delete_device(pool: &SqlitePool, id: &str) -> anyhow::Result<bool> {
    let result = sqlx::query("DELETE FROM devices WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM notify_state WHERE device_id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Prune a device whose APNs token Apple reported dead.
pub async fn delete_device_by_token(pool: &SqlitePool, token: &str) -> anyhow::Result<bool> {
    let row = sqlx::query("SELECT id FROM devices WHERE apns_token = ?1")
        .bind(token)
        .fetch_optional(pool)
        .await?;
    match row {
        Some(row) => delete_device(pool, row.get::<String, _>("id").as_str()).await,
        None => Ok(false),
    }
}

pub async fn load_cooldowns(
    pool: &SqlitePool,
) -> anyhow::Result<std::collections::HashMap<(String, String), i64>> {
    let rows = sqlx::query("SELECT device_id, watch_key, last_push_at FROM notify_state")
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                (row.get("device_id"), row.get("watch_key")),
                row.get("last_push_at"),
            )
        })
        .collect())
}

pub async fn record_push(
    pool: &SqlitePool,
    device_id: &str,
    watch_key: &str,
    now: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO notify_state (device_id, watch_key, last_push_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(device_id, watch_key) DO UPDATE SET last_push_at = ?3",
    )
    .bind(device_id)
    .bind(watch_key)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

// ---- Accounts & magic-link auth ----

pub async fn find_or_create_account(
    pool: &SqlitePool,
    email: &str,
    now: i64,
) -> anyhow::Result<i64> {
    sqlx::query(
        "INSERT INTO accounts (email, created_at) VALUES (?1, ?2) ON CONFLICT(email) DO NOTHING",
    )
    .bind(email)
    .bind(now)
    .execute(pool)
    .await?;
    let row = sqlx::query("SELECT id FROM accounts WHERE email = ?1")
        .bind(email)
        .fetch_one(pool)
        .await?;
    Ok(row.get("id"))
}

pub async fn account_email(pool: &SqlitePool, account_id: i64) -> anyhow::Result<Option<String>> {
    let row = sqlx::query("SELECT email FROM accounts WHERE id = ?1")
        .bind(account_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.get("email")))
}

pub async fn device_account(pool: &SqlitePool, device_id: &str) -> anyhow::Result<Option<i64>> {
    let row = sqlx::query("SELECT account_id FROM devices WHERE id = ?1")
        .bind(device_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.and_then(|r| r.get("account_id")))
}

pub async fn insert_login_token(
    pool: &SqlitePool,
    token_hash: &str,
    email: &str,
    device_id: Option<&str>,
    now: i64,
    ttl_secs: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO login_tokens (token_hash, email, device_id, created_at, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind(token_hash)
    .bind(email)
    .bind(device_id)
    .bind(now)
    .bind(now + ttl_secs)
    .execute(pool)
    .await?;
    Ok(())
}

/// Redeem a magic-link token: single use, must be unexpired.
/// Returns (email, device_id) on success.
pub async fn consume_login_token(
    pool: &SqlitePool,
    token_hash: &str,
    now: i64,
) -> anyhow::Result<Option<(String, Option<String>)>> {
    let result = sqlx::query(
        "UPDATE login_tokens SET used_at = ?2
         WHERE token_hash = ?1 AND used_at IS NULL AND expires_at > ?2
         RETURNING email, device_id",
    )
    .bind(token_hash)
    .bind(now)
    .fetch_optional(pool)
    .await?;
    Ok(result.map(|row| (row.get("email"), row.get("device_id"))))
}

/// Attach a device to an account, merging the device's watches into the
/// account list (deduped by watch key). Returns false if the device is gone.
pub async fn claim_device(
    pool: &SqlitePool,
    device_id: &str,
    account_id: i64,
) -> anyhow::Result<bool> {
    let mut txn = pool.begin().await?;
    let updated = sqlx::query("UPDATE devices SET account_id = ?2 WHERE id = ?1")
        .bind(device_id)
        .bind(account_id)
        .execute(&mut *txn)
        .await?;
    if updated.rows_affected() == 0 {
        return Ok(false);
    }
    let existing =
        sqlx::query("SELECT callsign, dmr_id FROM account_watches WHERE account_id = ?1")
            .bind(account_id)
            .fetch_all(&mut *txn)
            .await?;
    let existing_keys: std::collections::HashSet<String> = existing
        .into_iter()
        .map(|row| {
            watch_key(
                row.get::<String, _>("callsign").as_str(),
                row.get::<i64, _>("dmr_id"),
            )
        })
        .collect();
    let device_watches =
        sqlx::query("SELECT callsign, dmr_id, label, tgs FROM watches WHERE device_id = ?1")
            .bind(device_id)
            .fetch_all(&mut *txn)
            .await?;
    for row in device_watches {
        let callsign: String = row.get("callsign");
        let dmr_id: i64 = row.get("dmr_id");
        if existing_keys.contains(&watch_key(&callsign, dmr_id)) {
            continue;
        }
        sqlx::query(
            "INSERT INTO account_watches (account_id, callsign, dmr_id, label, tgs)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(account_id)
        .bind(&callsign)
        .bind(dmr_id)
        .bind(row.get::<String, _>("label"))
        .bind(row.get::<String, _>("tgs"))
        .execute(&mut *txn)
        .await?;
    }
    sqlx::query("DELETE FROM watches WHERE device_id = ?1")
        .bind(device_id)
        .execute(&mut *txn)
        .await?;
    txn.commit().await?;
    Ok(true)
}

/// Same stable key as Watch::key.
fn watch_key(callsign: &str, dmr_id: i64) -> String {
    if callsign.is_empty() {
        format!("id:{dmr_id}")
    } else {
        callsign.to_string()
    }
}

// ---- Account watches (web + mapped device API) ----

#[derive(Debug, Clone)]
pub struct AccountWatch {
    pub id: i64,
    pub callsign: String,
    pub dmr_id: u32,
    pub label: String,
    pub talkgroups: Vec<u32>,
}

pub async fn load_account_watches(
    pool: &SqlitePool,
    account_id: i64,
) -> anyhow::Result<Vec<AccountWatch>> {
    let rows = sqlx::query(
        "SELECT id, callsign, dmr_id, label, tgs FROM account_watches
         WHERE account_id = ?1 ORDER BY callsign, dmr_id",
    )
    .bind(account_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| AccountWatch {
            id: row.get("id"),
            callsign: row.get("callsign"),
            dmr_id: row.get::<i64, _>("dmr_id") as u32,
            label: row.get("label"),
            talkgroups: serde_json::from_str(row.get::<String, _>("tgs").as_str())
                .unwrap_or_default(),
        })
        .collect())
}

pub async fn insert_account_watch(
    pool: &SqlitePool,
    account_id: i64,
    callsign: &str,
    dmr_id: u32,
    label: &str,
    talkgroups: &[u32],
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO account_watches (account_id, callsign, dmr_id, label, tgs)
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind(account_id)
    .bind(callsign)
    .bind(dmr_id as i64)
    .bind(label)
    .bind(serde_json::to_string(talkgroups)?)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_account_watch(
    pool: &SqlitePool,
    account_id: i64,
    watch_id: i64,
    callsign: &str,
    dmr_id: u32,
    label: &str,
    talkgroups: &[u32],
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE account_watches SET callsign = ?3, dmr_id = ?4, label = ?5, tgs = ?6
         WHERE id = ?2 AND account_id = ?1",
    )
    .bind(account_id)
    .bind(watch_id)
    .bind(callsign)
    .bind(dmr_id as i64)
    .bind(label)
    .bind(serde_json::to_string(talkgroups)?)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn delete_account_watch(
    pool: &SqlitePool,
    account_id: i64,
    watch_id: i64,
) -> anyhow::Result<bool> {
    let result = sqlx::query("DELETE FROM account_watches WHERE id = ?2 AND account_id = ?1")
        .bind(account_id)
        .bind(watch_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn replace_account_watches(
    pool: &SqlitePool,
    account_id: i64,
    watches: &[Watch],
) -> anyhow::Result<()> {
    let mut txn = pool.begin().await?;
    sqlx::query("DELETE FROM account_watches WHERE account_id = ?1")
        .bind(account_id)
        .execute(&mut *txn)
        .await?;
    for watch in watches {
        sqlx::query(
            "INSERT INTO account_watches (account_id, callsign, dmr_id, label, tgs)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(account_id)
        .bind(&watch.callsign)
        .bind(watch.dmr_id as i64)
        .bind(&watch.label)
        .bind(serde_json::to_string(&watch.talkgroups)?)
        .execute(&mut *txn)
        .await?;
    }
    txn.commit().await?;
    Ok(())
}

// ---- Sessions ----

pub async fn insert_session(
    pool: &SqlitePool,
    token_hash: &str,
    account_id: i64,
    now: i64,
    ttl_secs: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO sessions (token_hash, account_id, created_at, expires_at)
         VALUES (?1, ?2, ?3, ?4)",
    )
    .bind(token_hash)
    .bind(account_id)
    .bind(now)
    .bind(now + ttl_secs)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn session_account(
    pool: &SqlitePool,
    token_hash: &str,
    now: i64,
) -> anyhow::Result<Option<i64>> {
    let row =
        sqlx::query("SELECT account_id FROM sessions WHERE token_hash = ?1 AND expires_at > ?2")
            .bind(token_hash)
            .bind(now)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.get("account_id")))
}

pub async fn delete_session(pool: &SqlitePool, token_hash: &str) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM sessions WHERE token_hash = ?1")
        .bind(token_hash)
        .execute(pool)
        .await?;
    Ok(())
}

// ---- Notification history ----

pub struct HistoryRow {
    pub callsign: String,
    pub dmr_id: u32,
    pub talkgroup: u32,
    pub talkgroup_name: String,
    pub device_id: String,
    pub sent_at: i64,
    pub outcome: String,
}

#[allow(clippy::too_many_arguments)]
pub async fn log_notification(
    pool: &SqlitePool,
    device_id: &str,
    account_id: Option<i64>,
    callsign: &str,
    dmr_id: u32,
    talkgroup: u32,
    talkgroup_name: &str,
    event_time: i64,
    sent_at: i64,
    outcome: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO notification_log
           (device_id, account_id, callsign, dmr_id, talkgroup, talkgroup_name,
            event_time, sent_at, outcome)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
    .bind(device_id)
    .bind(account_id)
    .bind(callsign)
    .bind(dmr_id as i64)
    .bind(talkgroup as i64)
    .bind(talkgroup_name)
    .bind(event_time)
    .bind(sent_at)
    .bind(outcome)
    .execute(pool)
    .await?;
    Ok(())
}

/// Account history, newest first. Also picks up rows logged before the
/// device was claimed (account_id NULL) via the device join.
pub async fn load_history(
    pool: &SqlitePool,
    account_id: i64,
    limit: i64,
    offset: i64,
) -> anyhow::Result<Vec<HistoryRow>> {
    let rows = sqlx::query(
        "SELECT callsign, dmr_id, talkgroup, talkgroup_name, device_id, sent_at, outcome
         FROM notification_log
         WHERE account_id = ?1
            OR device_id IN (SELECT id FROM devices WHERE account_id = ?1)
         ORDER BY sent_at DESC, id DESC
         LIMIT ?2 OFFSET ?3",
    )
    .bind(account_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| HistoryRow {
            callsign: row.get("callsign"),
            dmr_id: row.get::<i64, _>("dmr_id") as u32,
            talkgroup: row.get::<i64, _>("talkgroup") as u32,
            talkgroup_name: row.get("talkgroup_name"),
            device_id: row.get("device_id"),
            sent_at: row.get("sent_at"),
            outcome: row.get("outcome"),
        })
        .collect())
}

/// Periodic cleanup: old history, expired sessions and login tokens.
pub async fn prune(pool: &SqlitePool, now: i64, history_retention_secs: i64) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM notification_log WHERE sent_at < ?1")
        .bind(now - history_retention_secs)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM sessions WHERE expires_at <= ?1")
        .bind(now)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM login_tokens WHERE expires_at <= ?1")
        .bind(now)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
pub async fn open_memory() -> SqlitePool {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn add_device(pool: &SqlitePool, id: &str) {
        upsert_device(pool, id, "tok", "sandbox", "ios", "1.0", 100)
            .await
            .unwrap();
    }

    fn device_watch(device_id: &str, callsign: &str, dmr_id: u32) -> Watch {
        Watch {
            device_id: device_id.into(),
            callsign: callsign.into(),
            dmr_id,
            label: String::new(),
            talkgroups: vec![],
        }
    }

    #[tokio::test]
    async fn claim_merges_and_dedupes_watches() {
        let pool = open_memory().await;
        add_device(&pool, "dev1").await;
        replace_watches(
            &pool,
            "dev1",
            &[
                device_watch("dev1", "W6JY", 0),
                device_watch("dev1", "", 3121234),
            ],
        )
        .await
        .unwrap();
        let account_id = find_or_create_account(&pool, "a@b.co", 100).await.unwrap();
        // Account already watches W6JY: the merge must not duplicate it
        insert_account_watch(&pool, account_id, "W6JY", 0, "existing", &[])
            .await
            .unwrap();

        assert!(claim_device(&pool, "dev1", account_id).await.unwrap());

        let watches = load_account_watches(&pool, account_id).await.unwrap();
        assert_eq!(watches.len(), 2);
        assert_eq!(watches.iter().filter(|w| w.callsign == "W6JY").count(), 1);
        // W6JY kept the account's copy, not the device's
        assert_eq!(
            watches.iter().find(|w| w.callsign == "W6JY").unwrap().label,
            "existing"
        );
        // Device rows are gone
        assert!(load_device_watches(&pool, "dev1").await.unwrap().is_empty());
        assert_eq!(
            device_account(&pool, "dev1").await.unwrap(),
            Some(account_id)
        );
        // Claiming a missing device reports false
        assert!(!claim_device(&pool, "ghost", account_id).await.unwrap());
    }

    #[tokio::test]
    async fn effective_watches_fan_out_to_account_devices() {
        let pool = open_memory().await;
        add_device(&pool, "claimed1").await;
        add_device(&pool, "claimed2").await;
        add_device(&pool, "loner").await;
        replace_watches(&pool, "loner", &[device_watch("loner", "K6AAA", 0)])
            .await
            .unwrap();
        let account_id = find_or_create_account(&pool, "a@b.co", 100).await.unwrap();
        claim_device(&pool, "claimed1", account_id).await.unwrap();
        claim_device(&pool, "claimed2", account_id).await.unwrap();
        insert_account_watch(&pool, account_id, "W6JY", 0, "", &[])
            .await
            .unwrap();

        let effective = load_effective_watches(&pool).await.unwrap();
        // Account watch appears once per claimed device, loner keeps its own
        assert_eq!(effective.len(), 3);
        let devices_watching_w6jy: Vec<_> = effective
            .iter()
            .filter(|w| w.callsign == "W6JY")
            .map(|w| w.device_id.as_str())
            .collect();
        assert_eq!(devices_watching_w6jy.len(), 2);
        assert!(devices_watching_w6jy.contains(&"claimed1"));
        assert!(devices_watching_w6jy.contains(&"claimed2"));
        assert!(effective
            .iter()
            .any(|w| w.device_id == "loner" && w.callsign == "K6AAA"));
    }

    #[tokio::test]
    async fn login_tokens_are_single_use_and_expire() {
        let pool = open_memory().await;
        insert_login_token(&pool, "hash1", "a@b.co", Some("dev1"), 100, 900)
            .await
            .unwrap();
        // Expired token never redeems
        insert_login_token(&pool, "hash2", "a@b.co", None, 100, 900)
            .await
            .unwrap();
        assert!(consume_login_token(&pool, "hash2", 2000)
            .await
            .unwrap()
            .is_none());
        // Valid token redeems exactly once
        let redeemed = consume_login_token(&pool, "hash1", 500).await.unwrap();
        assert_eq!(redeemed, Some(("a@b.co".into(), Some("dev1".into()))));
        assert!(consume_login_token(&pool, "hash1", 501)
            .await
            .unwrap()
            .is_none());
        // Unknown token
        assert!(consume_login_token(&pool, "nope", 500)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn sessions_expire_and_delete() {
        let pool = open_memory().await;
        let account_id = find_or_create_account(&pool, "a@b.co", 100).await.unwrap();
        insert_session(&pool, "s1", account_id, 100, 1000)
            .await
            .unwrap();
        assert_eq!(
            session_account(&pool, "s1", 500).await.unwrap(),
            Some(account_id)
        );
        assert_eq!(session_account(&pool, "s1", 2000).await.unwrap(), None);
        delete_session(&pool, "s1").await.unwrap();
        assert_eq!(session_account(&pool, "s1", 500).await.unwrap(), None);
    }

    #[tokio::test]
    async fn history_includes_preclaim_device_rows_and_prunes() {
        let pool = open_memory().await;
        add_device(&pool, "dev1").await;
        // Logged before the device was claimed: account_id NULL
        log_notification(
            &pool,
            "dev1",
            None,
            "W6JY",
            1,
            91,
            "Local",
            100,
            100,
            "delivered",
        )
        .await
        .unwrap();
        let account_id = find_or_create_account(&pool, "a@b.co", 200).await.unwrap();
        claim_device(&pool, "dev1", account_id).await.unwrap();
        log_notification(
            &pool,
            "dev1",
            Some(account_id),
            "K6AAA",
            2,
            91,
            "",
            300,
            300,
            "failed",
        )
        .await
        .unwrap();

        let rows = load_history(&pool, account_id, 10, 0).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].callsign, "K6AAA"); // newest first
        assert_eq!(rows[1].callsign, "W6JY");

        // Retention prune drops the old row
        prune(&pool, 1000, 800).await.unwrap();
        let rows = load_history(&pool, account_id, 10, 0).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].callsign, "K6AAA");
    }

    #[tokio::test]
    async fn find_or_create_account_is_idempotent() {
        let pool = open_memory().await;
        let first = find_or_create_account(&pool, "a@b.co", 100).await.unwrap();
        let second = find_or_create_account(&pool, "a@b.co", 200).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(
            account_email(&pool, first).await.unwrap(),
            Some("a@b.co".into())
        );
    }
}
