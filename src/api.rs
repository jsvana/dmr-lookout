//! HTTP API: health (open) plus bearer-gated device/watch management.

use crate::event::normalize_call;
use crate::push::{build_test_payload, PushClient, SendOutcome};
use crate::watch::{Watch, WatchIndex};
use crate::{db, feed::FeedStatus};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct AppState {
    pub pool: SqlitePool,
    pub index: RwLock<WatchIndex>,
    pub feed: Arc<FeedStatus>,
    pub push: Option<Arc<PushClient>>,
    pub api_token: String,
    pub started_at: i64,
}

impl AppState {
    pub async fn rebuild_index(&self) -> anyhow::Result<()> {
        let devices = db::load_devices(&self.pool).await?;
        let watches = db::load_watches(&self.pool).await?;
        *self.index.write().await = WatchIndex::build(devices, watches);
        Ok(())
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    // Auth runs as middleware so a bad request without a token is 401,
    // never a body-shape error
    let protected = Router::new()
        .route("/v1/devices", post(register_device))
        .route(
            "/v1/devices/:id/watches",
            get(list_watches).put(put_watches),
        )
        .route("/v1/devices/:id", axum::routing::delete(remove_device))
        .route("/v1/devices/:id/test", post(test_push))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_bearer,
        ));
    Router::new()
        .route("/v1/health", get(health))
        .merge(protected)
        .with_state(state)
}

async fn require_bearer(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let ok = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token == state.api_token);
    if ok {
        next.run(request).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let now = chrono::Utc::now().timestamp();
    let last_activity = state.feed.last_activity.load(Ordering::Relaxed);
    let index = state.index.read().await;
    Json(serde_json::json!({
        "ok": true,
        "feed_connected": state.feed.connected.load(Ordering::Relaxed),
        "last_event_ago_secs": if last_activity > 0 { now - last_activity } else { -1 },
        "events_seen": state.feed.events_seen.load(Ordering::Relaxed),
        "devices": index.devices.len(),
        "watches": index.watch_count(),
        "apns_host": state.push.as_ref().map(|p| p.host),
        "uptime_secs": now - state.started_at,
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

#[derive(Deserialize)]
struct RegisterBody {
    device_id: String,
    apns_token: String,
    #[serde(default)]
    platform: String,
    #[serde(default)]
    app_version: String,
}

async fn register_device(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RegisterBody>,
) -> Response {
    if body.device_id.is_empty() || body.apns_token.is_empty() {
        return (StatusCode::BAD_REQUEST, "device_id and apns_token required").into_response();
    }
    let now = chrono::Utc::now().timestamp();
    let result = db::upsert_device(
        &state.pool,
        &body.device_id,
        &body.apns_token,
        if body.platform.is_empty() { "ios" } else { &body.platform },
        &body.app_version,
        now,
    )
    .await;
    match result {
        Ok(()) => {
            let _ = state.rebuild_index().await;
            tracing::info!(device = %body.device_id, "device registered");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => {
            tracing::warn!(%error, "device upsert failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Serialize, Deserialize)]
struct WatchBody {
    callsign: String,
    #[serde(default)]
    dmr_id: u32,
    #[serde(default)]
    label: String,
    #[serde(default)]
    talkgroups: Vec<u32>,
}

async fn list_watches(
    State(state): State<Arc<AppState>>,
    Path(device_id): Path<String>,
) -> Response {
    match db::load_watches(&state.pool).await {
        Ok(watches) => {
            let list: Vec<WatchBody> = watches
                .into_iter()
                .filter(|watch| watch.device_id == device_id)
                .map(|watch| WatchBody {
                    callsign: watch.callsign,
                    dmr_id: watch.dmr_id,
                    label: watch.label,
                    talkgroups: watch.talkgroups,
                })
                .collect();
            Json(list).into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn put_watches(
    State(state): State<Arc<AppState>>,
    Path(device_id): Path<String>,
    Json(body): Json<Vec<WatchBody>>,
) -> Response {
    let watches: Vec<Watch> = body
        .into_iter()
        .filter(|entry| !entry.callsign.trim().is_empty() || entry.dmr_id > 0)
        .map(|entry| Watch {
            device_id: device_id.clone(),
            callsign: normalize_call(&entry.callsign),
            dmr_id: entry.dmr_id,
            label: entry.label,
            talkgroups: entry.talkgroups,
        })
        .collect();
    match db::replace_watches(&state.pool, &device_id, &watches).await {
        Ok(()) => {
            let _ = state.rebuild_index().await;
            tracing::info!(device = %device_id, count = watches.len(), "watch list replaced");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => {
            tracing::warn!(%error, "watch replace failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn remove_device(
    State(state): State<Arc<AppState>>,
    Path(device_id): Path<String>,
) -> Response {
    match db::delete_device(&state.pool, &device_id).await {
        Ok(_) => {
            let _ = state.rebuild_index().await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn test_push(
    State(state): State<Arc<AppState>>,
    Path(device_id): Path<String>,
) -> Response {
    let Some(push) = state.push.as_ref() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "push not configured").into_response();
    };
    let token = match db::device_token(&state.pool, &device_id).await {
        Ok(Some(token)) => token,
        Ok(None) => return (StatusCode::NOT_FOUND, "unknown device").into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    match push.send(&token, &build_test_payload(), "buddy-test").await {
        SendOutcome::Delivered => StatusCode::NO_CONTENT.into_response(),
        SendOutcome::DeadToken => {
            let _ = db::delete_device_by_token(&state.pool, &token).await;
            let _ = state.rebuild_index().await;
            (StatusCode::GONE, "token dead, device pruned").into_response()
        }
        SendOutcome::Failed(reason) => (StatusCode::BAD_GATEWAY, reason).into_response(),
    }
}
