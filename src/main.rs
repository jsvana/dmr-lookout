mod api;
mod config;
mod db;
mod event;
mod feed;
mod push;
mod watch;

use api::AppState;
use event::LhEvent;
use push::{build_payload, PushClient, SendOutcome};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;
use watch::{RuleParams, Suppression};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config_path =
        std::env::var("LOOKOUT_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
    let config = config::Config::load(&config_path)?;
    let api_token = config::api_token()?;

    let push_client = match &config.push {
        Some(push_config) => {
            let client = Arc::new(PushClient::from_config(push_config)?);
            tracing::info!(host = client.host, "APNs configured");
            Some(client)
        }
        None => {
            tracing::warn!("no [push] config; notifications disabled");
            None
        }
    };

    let pool = db::open(&config.db.path).await?;
    let feed_status = feed::FeedStatus::new();
    let state = Arc::new(AppState {
        pool: pool.clone(),
        index: tokio::sync::RwLock::new(watch::WatchIndex::default()),
        feed: feed_status.clone(),
        push: push_client.clone(),
        api_token,
        started_at: chrono::Utc::now().timestamp(),
    });
    state.rebuild_index().await?;

    let (events_tx, events_rx) = mpsc::channel::<LhEvent>(1024);
    tokio::spawn(feed::run(
        config.feed.url.clone(),
        feed_status.clone(),
        events_tx,
    ));
    tokio::spawn(matcher(state.clone(), config.rules.clone().into(), events_rx));

    let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
    tracing::info!(bind = %config.server.bind, "listening");
    axum::serve(listener, api::router(state)).await?;
    Ok(())
}

impl From<config::RulesConfig> for RuleParams {
    fn from(rules: config::RulesConfig) -> RuleParams {
        RuleParams {
            cooldown_secs: rules.cooldown_secs,
            max_pushes_per_hour: rules.max_pushes_per_hour,
            freshness_secs: rules.freshness_secs,
            connect_grace_secs: rules.connect_grace_secs,
        }
    }
}

/// Consume feed events, match against the index, apply the notify rules,
/// send pushes, and persist cooldowns.
async fn matcher(state: Arc<AppState>, params: RuleParams, mut events: mpsc::Receiver<LhEvent>) {
    let mut cooldowns = db::load_cooldowns(&state.pool).await.unwrap_or_default();
    let mut hourly: HashMap<String, VecDeque<i64>> = HashMap::new();

    while let Some(event) = events.recv().await {
        let now = chrono::Utc::now().timestamp();
        let connected_at = state.feed.connected_at.load(Ordering::Relaxed);

        // Collect decisions under the read lock, send after releasing it
        let mut jobs = Vec::new();
        {
            let index = state.index.read().await;
            for matched in index.matches(&event) {
                let Some(device) = index.devices.get(&matched.device_id) else {
                    continue;
                };
                let key = matched.key();
                let window = hourly.entry(device.id.clone()).or_default();
                while window.front().is_some_and(|t| now - t > 3600) {
                    window.pop_front();
                }
                let decision = watch::decide(
                    &event,
                    device,
                    &params,
                    now,
                    connected_at,
                    cooldowns.get(&(device.id.clone(), key.clone())).copied(),
                    window.len(),
                );
                match decision {
                    Ok(()) => jobs.push((device.id.clone(), device.apns_token.clone(), key)),
                    Err(Suppression::Cooldown) | Err(Suppression::Inactive) => {}
                    Err(reason) => {
                        tracing::debug!(call = %event.source_call, ?reason, "suppressed");
                    }
                }
            }
        }

        for (device_id, token, key) in jobs {
            let Some(push) = state.push.as_ref() else {
                tracing::info!(call = %event.source_call, "match (push disabled)");
                continue;
            };
            let payload = build_payload(
                &event.source_call,
                event.source_name.as_deref(),
                event.source_id,
                event.destination_id,
                event.destination_name.as_deref(),
            );
            let collapse = format!("buddy-{}", event.source_call);
            match push.send(&token, &payload, &collapse).await {
                SendOutcome::Delivered => {
                    tracing::info!(call = %event.source_call, tg = event.destination_id,
                        device = %device_id, "pushed");
                    cooldowns.insert((device_id.clone(), key.clone()), now);
                    hourly.entry(device_id.clone()).or_default().push_back(now);
                    let _ = db::record_push(&state.pool, &device_id, &key, now).await;
                }
                SendOutcome::DeadToken => {
                    tracing::info!(device = %device_id, "dead token, pruning device");
                    let _ = db::delete_device_by_token(&state.pool, &token).await;
                    let _ = state.rebuild_index().await;
                }
                SendOutcome::Failed(reason) => {
                    tracing::warn!(%reason, "push failed");
                }
            }
        }
    }
}
