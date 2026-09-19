//! Server-rendered web UI: magic-link login, account watch editing, and
//! notification history. Plain form POSTs — works without JavaScript.

use crate::api::AppState;
use crate::auth;
use crate::db;
use crate::event::normalize_call;
use askama::Template;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Router};
use serde::Deserialize;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(watches_page))
        .route("/login", get(login_page).post(login_submit))
        .route("/logout", post(logout))
        .route("/auth/verify", get(verify))
        .route("/watches/add", post(watch_add))
        .route("/watches/:id/update", post(watch_update))
        .route("/watches/:id/delete", post(watch_delete))
        .route("/settings/notifications", post(notification_settings))
        .route("/history", get(history_page))
        .with_state(state)
}

// ---- Templates ----

#[derive(Template)]
#[template(path = "login.html")]
struct LoginTemplate {
    sent: bool,
    error: String,
}

struct WatchRow {
    id: i64,
    callsign: String,
    dmr_id: String,
    label: String,
    talkgroups: String,
}

#[derive(Template)]
#[template(path = "watches.html")]
struct WatchesTemplate {
    email: String,
    flash: String,
    watches: Vec<WatchRow>,
    notify_email: bool,
    webhook_url: String,
}

struct HistoryView {
    when: String,
    buddy: String,
    talkgroup: String,
    via: String,
    outcome: String,
}

#[derive(Template)]
#[template(path = "history.html")]
struct HistoryTemplate {
    email: String,
    rows: Vec<HistoryView>,
    page: i64,
    has_more: bool,
}

#[derive(Template)]
#[template(path = "message.html")]
struct MessageTemplate {
    title: String,
    body: String,
    link_href: String,
    link_label: String,
}

fn render<T: Template>(template: T) -> Response {
    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(error) => {
            tracing::error!(%error, "template render failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---- Session helper ----

struct Session {
    account_id: i64,
    email: String,
}

async fn current_session(state: &AppState, headers: &HeaderMap) -> Option<Session> {
    let token = auth::session_token_from_headers(headers)?;
    let now = chrono::Utc::now().timestamp();
    let account_id = db::session_account(&state.pool, &auth::hash_token(&token), now)
        .await
        .ok()??;
    let email = db::account_email(&state.pool, account_id).await.ok()??;
    Some(Session { account_id, email })
}

// ---- Login flow ----

async fn login_page() -> Response {
    render(LoginTemplate {
        sent: false,
        error: String::new(),
    })
}

#[derive(Deserialize)]
struct LoginForm {
    email: String,
}

async fn login_submit(State(state): State<Arc<AppState>>, Form(form): Form<LoginForm>) -> Response {
    let Some(email) = auth::normalize_email(&form.email) else {
        return render(LoginTemplate {
            sent: false,
            error: "That doesn't look like an email address.".into(),
        });
    };
    let now = chrono::Utc::now().timestamp();
    // Rate-limited requests still render the "sent" page: no oracle for
    // which addresses exist or are being hammered.
    if state.login_rate.lock().await.allow(&email, now) {
        if let Err(error) = send_magic_link(&state, &email, None, now).await {
            tracing::warn!(%error, "magic link send failed");
            return render(LoginTemplate {
                sent: false,
                error: "Couldn't send the email — try again in a minute.".into(),
            });
        }
    } else {
        tracing::info!(%email, "login request rate limited");
    }
    render(LoginTemplate {
        sent: true,
        error: String::new(),
    })
}

pub async fn send_magic_link(
    state: &AppState,
    email: &str,
    device_id: Option<&str>,
    now: i64,
) -> anyhow::Result<()> {
    let token = auth::new_token();
    db::insert_login_token(
        &state.pool,
        &auth::hash_token(&token),
        email,
        device_id,
        now,
        auth::LOGIN_TOKEN_TTL_SECS,
    )
    .await?;
    let link = format!(
        "{}/auth/verify?token={token}",
        state.base_url.trim_end_matches('/')
    );
    state
        .mailer
        .send_magic_link(email, &link, device_id.is_some())
        .await
}

#[derive(Deserialize)]
struct VerifyQuery {
    #[serde(default)]
    token: String,
}

async fn verify(State(state): State<Arc<AppState>>, Query(query): Query<VerifyQuery>) -> Response {
    let now = chrono::Utc::now().timestamp();
    let consumed = db::consume_login_token(&state.pool, &auth::hash_token(&query.token), now)
        .await
        .unwrap_or(None);
    let Some((email, device_id)) = consumed else {
        return render(MessageTemplate {
            title: "Link expired".into(),
            body: "That sign-in link is invalid, already used, or older than \
                   15 minutes. Request a fresh one."
                .into(),
            link_href: "/login".into(),
            link_label: "Back to sign in".into(),
        });
    };
    let account_id = match db::find_or_create_account(&state.pool, &email, now).await {
        Ok(id) => id,
        Err(error) => {
            tracing::error!(%error, "account create failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let mut linked = false;
    if let Some(device_id) = &device_id {
        match db::claim_device(&state.pool, device_id, account_id).await {
            Ok(claimed) => {
                linked = claimed;
                let _ = state.rebuild_index().await;
                tracing::info!(device = %device_id, %email, claimed, "device claim");
            }
            Err(error) => tracing::error!(%error, "device claim failed"),
        }
    }
    let session_token = auth::new_token();
    if let Err(error) = db::insert_session(
        &state.pool,
        &auth::hash_token(&session_token),
        account_id,
        now,
        auth::SESSION_TTL_SECS,
    )
    .await
    {
        tracing::error!(%error, "session create failed");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let cookie = auth::session_cookie(&session_token, &state.base_url, auth::SESSION_TTL_SECS);
    let destination = if linked { "/?flash=linked" } else { "/" };
    ([(header::SET_COOKIE, cookie)], Redirect::to(destination)).into_response()
}

async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Some(token) = auth::session_token_from_headers(&headers) {
        let _ = db::delete_session(&state.pool, &auth::hash_token(&token)).await;
    }
    let cookie = auth::clear_session_cookie(&state.base_url);
    ([(header::SET_COOKIE, cookie)], Redirect::to("/login")).into_response()
}

// ---- Watches ----

#[derive(Deserialize)]
struct WatchesQuery {
    #[serde(default)]
    flash: String,
}

async fn watches_page(
    State(state): State<Arc<AppState>>,
    Query(query): Query<WatchesQuery>,
    headers: HeaderMap,
) -> Response {
    let Some(session) = current_session(&state, &headers).await else {
        return Redirect::to("/login").into_response();
    };
    let watches = match db::load_account_watches(&state.pool, session.account_id).await {
        Ok(list) => list,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let flash = match query.flash.as_str() {
        "linked" => "Device linked — its watches were merged into this account.".to_string(),
        "channels" => "Notification channels saved.".to_string(),
        "badhook" => "Webhook URL must be a public http(s) address.".to_string(),
        _ => String::new(),
    };
    let (notify_email, webhook_url) = db::account_channel_settings(&state.pool, session.account_id)
        .await
        .unwrap_or((false, String::new()));
    render(WatchesTemplate {
        email: session.email,
        flash,
        notify_email,
        webhook_url,
        watches: watches
            .into_iter()
            .map(|watch| WatchRow {
                id: watch.id,
                callsign: watch.callsign,
                dmr_id: if watch.dmr_id > 0 {
                    watch.dmr_id.to_string()
                } else {
                    String::new()
                },
                label: watch.label,
                talkgroups: watch
                    .talkgroups
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            })
            .collect(),
    })
}

#[derive(Deserialize)]
struct WatchForm {
    #[serde(default)]
    callsign: String,
    #[serde(default)]
    dmr_id: String,
    #[serde(default)]
    label: String,
    #[serde(default)]
    talkgroups: String,
}

struct ParsedWatch {
    callsign: String,
    dmr_id: u32,
    label: String,
    talkgroups: Vec<u32>,
}

fn parse_watch_form(form: &WatchForm) -> Option<ParsedWatch> {
    let callsign = normalize_call(form.callsign.trim());
    let dmr_id: u32 = form.dmr_id.trim().parse().unwrap_or(0);
    if callsign.is_empty() && dmr_id == 0 {
        return None;
    }
    let talkgroups = form
        .talkgroups
        .split([',', ' '])
        .filter_map(|part| part.trim().parse::<u32>().ok())
        .collect();
    Some(ParsedWatch {
        callsign,
        dmr_id,
        label: form.label.trim().to_string(),
        talkgroups,
    })
}

async fn watch_add(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Form(form): Form<WatchForm>,
) -> Response {
    let Some(session) = current_session(&state, &headers).await else {
        return Redirect::to("/login").into_response();
    };
    if let Some(parsed) = parse_watch_form(&form) {
        let result = db::insert_account_watch(
            &state.pool,
            session.account_id,
            &parsed.callsign,
            parsed.dmr_id,
            &parsed.label,
            &parsed.talkgroups,
        )
        .await;
        if result.is_ok() {
            let _ = state.rebuild_index().await;
        }
    }
    Redirect::to("/").into_response()
}

async fn watch_update(
    State(state): State<Arc<AppState>>,
    Path(watch_id): Path<i64>,
    headers: HeaderMap,
    Form(form): Form<WatchForm>,
) -> Response {
    let Some(session) = current_session(&state, &headers).await else {
        return Redirect::to("/login").into_response();
    };
    match parse_watch_form(&form) {
        Some(parsed) => {
            let result = db::update_account_watch(
                &state.pool,
                session.account_id,
                watch_id,
                &parsed.callsign,
                parsed.dmr_id,
                &parsed.label,
                &parsed.talkgroups,
            )
            .await;
            if matches!(result, Ok(true)) {
                let _ = state.rebuild_index().await;
            }
        }
        // Cleared both identifiers = delete
        None => {
            if matches!(
                db::delete_account_watch(&state.pool, session.account_id, watch_id).await,
                Ok(true)
            ) {
                let _ = state.rebuild_index().await;
            }
        }
    }
    Redirect::to("/").into_response()
}

async fn watch_delete(
    State(state): State<Arc<AppState>>,
    Path(watch_id): Path<i64>,
    headers: HeaderMap,
) -> Response {
    let Some(session) = current_session(&state, &headers).await else {
        return Redirect::to("/login").into_response();
    };
    if matches!(
        db::delete_account_watch(&state.pool, session.account_id, watch_id).await,
        Ok(true)
    ) {
        let _ = state.rebuild_index().await;
    }
    Redirect::to("/").into_response()
}

// ---- Notification channels ----

#[derive(Deserialize)]
struct ChannelsForm {
    #[serde(default)]
    notify_email: String,
    #[serde(default)]
    webhook_url: String,
}

/// Reject non-http(s) and obviously private/loopback hosts — this URL is
/// fetched by the server, so don't let it aim at internal services.
fn valid_webhook(url: &str) -> bool {
    let rest = match url.split_once("://") {
        Some(("http" | "https", rest)) => rest,
        _ => return false,
    };
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .rsplit('@')
        .next()
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if host.is_empty() {
        return false;
    }
    let private_prefixes = ["127.", "10.", "192.168.", "169.254.", "0."];
    if host == "localhost"
        || host == "::1"
        || host.starts_with('[')
        || private_prefixes.iter().any(|p| host.starts_with(p))
    {
        return false;
    }
    // 172.16.0.0/12
    if let Some(second) = host.strip_prefix("172.").and_then(|r| r.split('.').next()) {
        if second.parse::<u8>().is_ok_and(|n| (16..=31).contains(&n)) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::valid_webhook;

    #[test]
    fn webhook_validation() {
        assert!(valid_webhook("https://example.com/hook"));
        assert!(valid_webhook("http://ntfy.sh/mytopic"));
        assert!(valid_webhook("https://user:pass@example.com:8443/x?y=1"));
        assert!(!valid_webhook("ftp://example.com"));
        assert!(!valid_webhook("example.com/hook"));
        assert!(!valid_webhook("https://localhost/x"));
        assert!(!valid_webhook("http://127.0.0.1:8084/x"));
        assert!(!valid_webhook("http://10.1.2.3/x"));
        assert!(!valid_webhook("http://192.168.1.5/x"));
        assert!(!valid_webhook("http://172.20.0.1/x"));
        assert!(valid_webhook("http://172.15.0.1/x"));
        assert!(valid_webhook("http://172.32.0.1/x"));
        assert!(!valid_webhook("http://[::1]/x"));
        assert!(!valid_webhook("https://"));
    }
}

async fn notification_settings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Form(form): Form<ChannelsForm>,
) -> Response {
    let Some(session) = current_session(&state, &headers).await else {
        return Redirect::to("/login").into_response();
    };
    let webhook_url = form.webhook_url.trim().to_string();
    if !webhook_url.is_empty() && !valid_webhook(&webhook_url) {
        return Redirect::to("/?flash=badhook").into_response();
    }
    let notify_email = form.notify_email == "on";
    match db::set_account_channels(&state.pool, session.account_id, notify_email, &webhook_url)
        .await
    {
        Ok(()) => {
            let _ = state.rebuild_index().await;
            Redirect::to("/?flash=channels").into_response()
        }
        Err(error) => {
            tracing::error!(%error, "channel settings save failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---- History ----

const HISTORY_PAGE_SIZE: i64 = 50;

#[derive(Deserialize)]
struct HistoryQuery {
    #[serde(default)]
    page: i64,
}

async fn history_page(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HistoryQuery>,
    headers: HeaderMap,
) -> Response {
    let Some(session) = current_session(&state, &headers).await else {
        return Redirect::to("/login").into_response();
    };
    let page = query.page.max(1);
    // Fetch one extra row to know whether an older page exists
    let mut rows = match db::load_history(
        &state.pool,
        session.account_id,
        HISTORY_PAGE_SIZE + 1,
        (page - 1) * HISTORY_PAGE_SIZE,
    )
    .await
    {
        Ok(rows) => rows,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let has_more = rows.len() as i64 > HISTORY_PAGE_SIZE;
    rows.truncate(HISTORY_PAGE_SIZE as usize);
    render(HistoryTemplate {
        email: session.email,
        page,
        has_more,
        rows: rows
            .into_iter()
            .map(|row| HistoryView {
                when: chrono::DateTime::from_timestamp(row.sent_at, 0)
                    .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
                    .unwrap_or_default(),
                buddy: if row.callsign.is_empty() {
                    format!("DMR {}", row.dmr_id)
                } else {
                    row.callsign
                },
                talkgroup: if row.talkgroup_name.is_empty() {
                    format!("TG {}", row.talkgroup)
                } else {
                    format!("{} (TG {})", row.talkgroup_name, row.talkgroup)
                },
                via: if row.channel == "apns" {
                    format!(
                        "push · {}",
                        row.device_id.chars().take(8).collect::<String>()
                    )
                } else {
                    row.channel
                },
                outcome: row.outcome,
            })
            .collect(),
    })
}
