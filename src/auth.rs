//! Magic-link auth primitives: opaque tokens (only the SHA-256 lands in
//! SQLite), session cookies, and a per-email rate limit on link requests.

use axum::http::HeaderMap;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub const LOGIN_TOKEN_TTL_SECS: i64 = 15 * 60;
pub const SESSION_TTL_SECS: i64 = 30 * 24 * 3600;
pub const SESSION_COOKIE: &str = "lookout_session";

/// Requests allowed per email inside the window.
const RATE_LIMIT_MAX: usize = 3;
const RATE_LIMIT_WINDOW_SECS: i64 = 15 * 60;

pub fn new_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex(&bytes)
}

pub fn hash_token(token: &str) -> String {
    hex(&Sha256::digest(token.as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Lowercased, trimmed; minimal shape check — the real validation is
/// whether the inbox receives the link.
pub fn normalize_email(input: &str) -> Option<String> {
    let email = input.trim().to_ascii_lowercase();
    let (local, domain) = email.split_once('@')?;
    if local.is_empty()
        || !domain.contains('.')
        || email.len() > 254
        || email.contains(char::is_whitespace)
    {
        return None;
    }
    Some(email)
}

pub fn session_token_from_headers(headers: &HeaderMap) -> Option<String> {
    let cookies = headers.get("cookie")?.to_str().ok()?;
    cookies.split(';').find_map(|pair| {
        let (name, value) = pair.trim().split_once('=')?;
        (name == SESSION_COOKIE && !value.is_empty()).then(|| value.to_string())
    })
}

pub fn session_cookie(token: &str, base_url: &str, max_age_secs: i64) -> String {
    let secure = if base_url.starts_with("https://") {
        "; Secure"
    } else {
        ""
    };
    format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age_secs}{secure}"
    )
}

pub fn clear_session_cookie(base_url: &str) -> String {
    session_cookie("", base_url, 0)
}

/// Sliding-window per-email limiter for magic-link requests.
#[derive(Default)]
pub struct RateLimiter {
    requests: HashMap<String, Vec<i64>>,
}

impl RateLimiter {
    pub fn allow(&mut self, email: &str, now: i64) -> bool {
        let stamps = self.requests.entry(email.to_string()).or_default();
        stamps.retain(|t| now - *t < RATE_LIMIT_WINDOW_SECS);
        if stamps.len() >= RATE_LIMIT_MAX {
            return false;
        }
        stamps.push(now);
        // Bound the map so it can't grow forever on junk emails
        if self.requests.len() > 10_000 {
            self.requests
                .retain(|_, v| v.iter().any(|t| now - *t < RATE_LIMIT_WINDOW_SECS));
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_hash_is_stable_and_distinct() {
        let token = new_token();
        assert_eq!(token.len(), 64);
        assert_eq!(hash_token(&token), hash_token(&token));
        assert_ne!(hash_token(&token), token);
        assert_ne!(new_token(), token);
    }

    #[test]
    fn email_normalization() {
        assert_eq!(
            normalize_email("  Foo@Example.COM "),
            Some("foo@example.com".into())
        );
        assert_eq!(normalize_email("nope"), None);
        assert_eq!(normalize_email("@example.com"), None);
        assert_eq!(normalize_email("a@nodot"), None);
    }

    #[test]
    fn cookie_roundtrip() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            "other=1; lookout_session=abc123; x=y".parse().unwrap(),
        );
        assert_eq!(session_token_from_headers(&headers), Some("abc123".into()));
        assert!(session_cookie("t", "https://x.example", 60).contains("Secure"));
        assert!(!session_cookie("t", "http://127.0.0.1:8084", 60).contains("Secure"));
    }

    #[test]
    fn rate_limit_caps_then_slides() {
        let mut limiter = RateLimiter::default();
        assert!(limiter.allow("a@b.co", 0));
        assert!(limiter.allow("a@b.co", 1));
        assert!(limiter.allow("a@b.co", 2));
        assert!(!limiter.allow("a@b.co", 3));
        assert!(limiter.allow("other@b.co", 3));
        assert!(limiter.allow("a@b.co", 1000));
    }
}
