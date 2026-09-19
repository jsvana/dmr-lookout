mod api;
mod auth;
mod config;
mod db;
mod email;
mod event;
mod feed;
mod push;
mod watch;
mod web;

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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config_path = std::env::var("LOOKOUT_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
    let config = config::Config::load(&config_path)?;
    let api_token = config::api_token()?;

    let push_client = match &config.push {
        Some(push_config) => {
            let client = Arc::new(PushClient::from_config(push_config)?);
            tracing::info!("APNs configured (per-device sandbox/production routing)");
            Some(client)
        }
        None => {
            tracing::warn!("no [push] config; notifications disabled");
            None
        }
    };

    let resend_key = config::resend_key();
    if resend_key.is_none() {
        tracing::warn!("no LOOKOUT_RESEND_KEY; magic links will be logged, not emailed");
    }
    let mailer = email::Mailer::new(resend_key, &config.web.email_from);

    let pool = db::open(&config.db.path).await?;
    let feed_status = feed::FeedStatus::new();
    let state = Arc::new(AppState {
        pool: pool.clone(),
        index: tokio::sync::RwLock::new(watch::WatchIndex::default()),
        feed: feed_status.clone(),
        push: push_client.clone(),
        api_token,
        started_at: chrono::Utc::now().timestamp(),
        mailer,
        base_url: config.web.base_url.clone(),
        login_rate: tokio::sync::Mutex::new(auth::RateLimiter::default()),
        channels: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        http: reqwest::Client::new(),
    });
    state.rebuild_index().await?;

    // History retention + expired session/token cleanup
    tokio::spawn(maintenance(pool.clone()));

    let (events_tx, events_rx) = mpsc::channel::<LhEvent>(1024);
    tokio::spawn(feed::run(
        config.feed.url.clone(),
        feed_status.clone(),
        events_tx,
    ));
    tokio::spawn(matcher(
        state.clone(),
        config.rules.clone().into(),
        events_rx,
    ));

    let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
    tracing::info!(bind = %config.server.bind, "listening");
    axum::serve(listener, api::router(state)).await?;
    Ok(())
}

/// Every 6 hours: drop notification history past 90 days plus expired
/// sessions and magic-link tokens.
async fn maintenance(pool: sqlx::SqlitePool) {
    const HISTORY_RETENTION_SECS: i64 = 90 * 24 * 3600;
    loop {
        let now = chrono::Utc::now().timestamp();
        if let Err(error) = db::prune(&pool, now, HISTORY_RETENTION_SECS).await {
            tracing::warn!(%error, "prune failed");
        }
        tokio::time::sleep(std::time::Duration::from_secs(6 * 3600)).await;
    }
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

        // Collect decisions under the read locks, send after releasing them
        let mut jobs = Vec::new();
        let mut channel_jobs = Vec::new();
        {
            let index = state.index.read().await;
            let channels = state.channels.read().await;
            for matched in index.matches(&event) {
                let key = matched.key();
                // "acct:<id>" rows are email/webhook deliveries, one per
                // account per event, sharing the device rules (no quiet
                // hours — accounts have none)
                let (decider, is_channel) = match index.devices.get(&matched.device_id) {
                    Some(device) => (device.clone(), false),
                    None => {
                        let Some(config) = channels.get(&matched.device_id) else {
                            continue;
                        };
                        (
                            watch::Device {
                                id: matched.device_id.clone(),
                                apns_token: String::new(),
                                apns_env: String::new(),
                                account_id: Some(config.account_id),
                                quiet_start: None,
                                quiet_end: None,
                                tz: String::new(),
                            },
                            true,
                        )
                    }
                };
                let window = hourly.entry(decider.id.clone()).or_default();
                while window.front().is_some_and(|t| now - t > 3600) {
                    window.pop_front();
                }
                let decision = watch::decide(
                    &event,
                    &decider,
                    &params,
                    now,
                    connected_at,
                    cooldowns.get(&(decider.id.clone(), key.clone())).copied(),
                    window.len(),
                );
                match decision {
                    Ok(()) if is_channel => {
                        let config = channels[&matched.device_id].clone();
                        channel_jobs.push(ChannelJob {
                            acct_key: matched.device_id.clone(),
                            config,
                            key,
                            watch_call: matched.callsign.clone(),
                            watch_label: matched.label.clone(),
                        });
                    }
                    Ok(()) => jobs.push(PushJob {
                        device_id: decider.id.clone(),
                        token: decider.apns_token.clone(),
                        apns_env: decider.apns_env.clone(),
                        account_id: decider.account_id,
                        key,
                        watch_call: matched.callsign.clone(),
                        watch_label: matched.label.clone(),
                    }),
                    Err(Suppression::Cooldown) | Err(Suppression::Inactive) => {}
                    Err(reason) => {
                        tracing::debug!(call = %event.source_call, ?reason, "suppressed");
                    }
                }
            }
        }

        for job in jobs {
            let Some(push) = state.push.as_ref() else {
                tracing::info!(call = %event.source_call, "match (push disabled)");
                continue;
            };
            // Open Terminal (app-only) transmissions ship a blank SourceCall;
            // the watch knows who it matched, so fall back to its own data
            let call = if event.source_call.is_empty() {
                if job.watch_call.is_empty() {
                    format!("DMR {}", event.source_id)
                } else {
                    job.watch_call.clone()
                }
            } else {
                event.source_call.clone()
            };
            let name = event
                .source_name
                .clone()
                .or_else(|| (!job.watch_label.is_empty()).then(|| job.watch_label.clone()));
            let payload = build_payload(
                &call,
                name.as_deref(),
                event.source_id,
                event.destination_id,
                event.destination_name.as_deref(),
            );
            let collapse = format!("buddy-{call}");
            let outcome = push
                .send(&job.token, &payload, &collapse, &job.apns_env)
                .await;
            let outcome_label = match &outcome {
                SendOutcome::Delivered => "delivered",
                SendOutcome::DeadToken => "dead_token",
                SendOutcome::Failed(_) => "failed",
            };
            let _ = db::log_notification(
                &state.pool,
                &job.device_id,
                job.account_id,
                "apns",
                &call,
                event.source_id,
                event.destination_id,
                event.destination_name.as_deref().unwrap_or(""),
                if event.start > 0 { event.start } else { now },
                now,
                outcome_label,
            )
            .await;
            match outcome {
                SendOutcome::Delivered => {
                    tracing::info!(call = %event.source_call, tg = event.destination_id,
                        device = %job.device_id, "pushed");
                    cooldowns.insert((job.device_id.clone(), job.key.clone()), now);
                    hourly
                        .entry(job.device_id.clone())
                        .or_default()
                        .push_back(now);
                    let _ = db::record_push(&state.pool, &job.device_id, &job.key, now).await;
                }
                SendOutcome::DeadToken => {
                    tracing::info!(device = %job.device_id, "dead token, pruning device");
                    let _ = db::delete_device_by_token(&state.pool, &job.token).await;
                    let _ = state.rebuild_index().await;
                }
                SendOutcome::Failed(reason) => {
                    tracing::warn!(%reason, "push failed");
                }
            }
        }

        // Email/webhook deliveries: one per account per event
        for job in channel_jobs {
            let call = display_call(&event, &job.watch_call);
            let buddy = if job.watch_label.is_empty() {
                call.clone()
            } else {
                format!("{call} ({})", job.watch_label)
            };
            let talkgroup = match &event.destination_name {
                Some(name) if !name.is_empty() => {
                    format!("{name} (TG {})", event.destination_id)
                }
                _ => format!("TG {}", event.destination_id),
            };
            let mut any_delivered = false;

            if job.config.notify_email {
                let history_url = format!("{}/history", state.base_url.trim_end_matches('/'));
                let outcome = match state
                    .mailer
                    .send_notification(&job.config.email, &buddy, &talkgroup, &history_url)
                    .await
                {
                    Ok(()) => {
                        any_delivered = true;
                        "delivered"
                    }
                    Err(error) => {
                        tracing::warn!(%error, "notification email failed");
                        "failed"
                    }
                };
                let _ = db::log_notification(
                    &state.pool,
                    &job.acct_key,
                    Some(job.config.account_id),
                    "email",
                    &call,
                    event.source_id,
                    event.destination_id,
                    event.destination_name.as_deref().unwrap_or(""),
                    if event.start > 0 { event.start } else { now },
                    now,
                    outcome,
                )
                .await;
            }

            if !job.config.webhook_url.is_empty() {
                let payload = serde_json::json!({
                    "callsign": call,
                    "label": job.watch_label,
                    "dmr_id": event.source_id,
                    "talkgroup": event.destination_id,
                    "talkgroup_name": event.destination_name.as_deref().unwrap_or(""),
                    "event_time": if event.start > 0 { event.start } else { now },
                });
                let sent = state
                    .http
                    .post(&job.config.webhook_url)
                    .json(&payload)
                    .timeout(std::time::Duration::from_secs(10))
                    .send()
                    .await;
                let outcome = match sent {
                    Ok(response) if response.status().is_success() => {
                        any_delivered = true;
                        "delivered"
                    }
                    Ok(response) => {
                        tracing::warn!(status = %response.status(), "webhook rejected");
                        "failed"
                    }
                    Err(error) => {
                        tracing::warn!(%error, "webhook failed");
                        "failed"
                    }
                };
                let _ = db::log_notification(
                    &state.pool,
                    &job.acct_key,
                    Some(job.config.account_id),
                    "webhook",
                    &call,
                    event.source_id,
                    event.destination_id,
                    event.destination_name.as_deref().unwrap_or(""),
                    if event.start > 0 { event.start } else { now },
                    now,
                    outcome,
                )
                .await;
            }

            if any_delivered {
                tracing::info!(call = %call, account = job.config.account_id, "channel notified");
                cooldowns.insert((job.acct_key.clone(), job.key.clone()), now);
                hourly
                    .entry(job.acct_key.clone())
                    .or_default()
                    .push_back(now);
                let _ = db::record_push(&state.pool, &job.acct_key, &job.key, now).await;
            }
        }
    }
}

/// Open Terminal (app-only) transmissions ship a blank SourceCall; fall
/// back to the watch's own callsign, then the raw DMR ID.
fn display_call(event: &LhEvent, watch_call: &str) -> String {
    if !event.source_call.is_empty() {
        event.source_call.clone()
    } else if !watch_call.is_empty() {
        watch_call.to_string()
    } else {
        format!("DMR {}", event.source_id)
    }
}

struct ChannelJob {
    acct_key: String,
    config: db::AccountChannels,
    key: String,
    watch_call: String,
    watch_label: String,
}

struct PushJob {
    device_id: String,
    token: String,
    apns_env: String,
    account_id: Option<i64>,
    key: String,
    watch_call: String,
    watch_label: String,
}
