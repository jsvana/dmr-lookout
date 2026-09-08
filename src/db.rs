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
        "SELECT id, apns_token, quiet_start, quiet_end, tz FROM devices",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Device {
            id: row.get("id"),
            apns_token: row.get("apns_token"),
            quiet_start: row.get("quiet_start"),
            quiet_end: row.get("quiet_end"),
            tz: row.get("tz"),
        })
        .collect())
}

pub async fn load_watches(pool: &SqlitePool) -> anyhow::Result<Vec<Watch>> {
    let rows = sqlx::query("SELECT device_id, callsign, dmr_id, label, tgs FROM watches")
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| Watch {
            device_id: row.get("device_id"),
            callsign: row.get("callsign"),
            dmr_id: row.get::<i64, _>("dmr_id") as u32,
            label: row.get("label"),
            talkgroups: serde_json::from_str(row.get::<String, _>("tgs").as_str())
                .unwrap_or_default(),
        })
        .collect())
}

pub async fn upsert_device(
    pool: &SqlitePool,
    id: &str,
    apns_token: &str,
    platform: &str,
    app_version: &str,
    now: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO devices (id, apns_token, platform, app_version, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5)
         ON CONFLICT(id) DO UPDATE SET
           apns_token = ?2, platform = ?3, app_version = ?4, updated_at = ?5",
    )
    .bind(id)
    .bind(apns_token)
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

pub async fn device_token(pool: &SqlitePool, id: &str) -> anyhow::Result<Option<String>> {
    let row = sqlx::query("SELECT apns_token FROM devices WHERE id = ?1")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.get("apns_token")))
}
