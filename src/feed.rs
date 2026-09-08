//! BrandMeister last-heard follower: Engine.IO v4 over websocket, joined to
//! the `everything` room. Ported from DMRMonitor's BrandmeisterLH.swift.

use crate::event::{parse_frame, LhEvent};
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// Shared, lock-free view of feed health for /v1/health and the matcher.
pub struct FeedStatus {
    pub connected: AtomicBool,
    /// Unix seconds of the last received message.
    pub last_activity: AtomicI64,
    /// Unix seconds of the last successful (re)connect; drives the grace gate.
    pub connected_at: AtomicI64,
    pub events_seen: AtomicU64,
}

impl FeedStatus {
    pub fn new() -> Arc<FeedStatus> {
        Arc::new(FeedStatus {
            connected: AtomicBool::new(false),
            last_activity: AtomicI64::new(0),
            connected_at: AtomicI64::new(0),
            events_seen: AtomicU64::new(0),
        })
    }
}

/// Run forever: connect, stream events into `events`, reconnect with
/// exponential backoff + jitter on any failure.
pub async fn run(url: String, status: Arc<FeedStatus>, events: mpsc::Sender<LhEvent>) {
    let mut attempts: u32 = 0;
    loop {
        match connect_once(&url, &status, &events).await {
            Ok(()) => attempts = 0,
            Err(error) => {
                status.connected.store(false, Ordering::Relaxed);
                attempts += 1;
                let base = 30.0_f64.min(2.0_f64.powi(attempts.saturating_sub(1) as i32));
                let jitter = 0.8 + rand::random::<f64>() * 0.4;
                let delay = Duration::from_secs_f64(base * jitter);
                tracing::warn!(%error, ?delay, "feed disconnected, reconnecting");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn connect_once(
    url: &str,
    status: &FeedStatus,
    events: &mpsc::Sender<LhEvent>,
) -> anyhow::Result<()> {
    let (stream, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut sink, mut source) = stream.split();
    let mut ping_interval: u64 = 25;
    let mut ping_timeout: u64 = 20;

    loop {
        // Stall watchdog: the server pings every ping_interval; a silent
        // socket past interval+timeout is dead even if TCP disagrees.
        let deadline = Duration::from_secs(ping_interval + ping_timeout + 5);
        let message = tokio::time::timeout(deadline, source.next())
            .await
            .map_err(|_| anyhow::anyhow!("feed stalled (no traffic in {deadline:?})"))?
            .ok_or_else(|| anyhow::anyhow!("feed closed"))??;

        let now = chrono::Utc::now().timestamp();
        status.last_activity.store(now, Ordering::Relaxed);
        let Message::Text(text) = message else {
            continue;
        };

        if let Some(rest) = text.strip_prefix('0') {
            if !text.starts_with("0{") && text != "0" {
                // "0" only opens the engine.io session when followed by JSON
            }
            if let Ok(open) = serde_json::from_str::<serde_json::Value>(rest) {
                if let Some(millis) = open.get("pingInterval").and_then(|v| v.as_u64()) {
                    ping_interval = millis / 1000;
                }
                if let Some(millis) = open.get("pingTimeout").and_then(|v| v.as_u64()) {
                    ping_timeout = millis / 1000;
                }
            }
            sink.send(Message::Text("40".into())).await?;
        } else if text.starts_with("40") {
            sink.send(Message::Text("42[\"join\",\"everything\"]".into()))
                .await?;
            status.connected.store(true, Ordering::Relaxed);
            status.connected_at.store(now, Ordering::Relaxed);
            tracing::info!("feed connected, joined everything");
        } else if text == "2" {
            sink.send(Message::Text("3".into())).await?;
        } else if text.starts_with("42") {
            if let Some(event) = parse_frame(&text) {
                status.events_seen.fetch_add(1, Ordering::Relaxed);
                // A full channel means the matcher wedged; dropping events
                // is preferable to unbounded memory
                let _ = events.try_send(event);
            }
        } else if text == "1" || text.starts_with("41") {
            anyhow::bail!("server closed the namespace");
        }
    }
}
