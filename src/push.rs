//! APNs client, adapted from hearth's push.rs: `.p8` token auth signing a
//! short-lived ES256 JWT, HTTP/2 POST via reqwest+rustls, dead-token
//! classification so the device table self-heals.

use crate::config::PushConfig;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;
use std::sync::Mutex;

/// Refresh the signing JWT at 50 minutes; Apple rejects tokens older than 60.
const JWT_TTL_SECS: i64 = 50 * 60;

pub struct PushClient {
    key: EncodingKey,
    key_id: String,
    team_id: String,
    bundle_id: String,

    http: reqwest::Client,
    jwt_cache: Mutex<Option<(String, i64)>>,
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    iat: i64,
}

/// What happened to one send, so the caller can prune dead tokens.
#[derive(Debug, PartialEq)]
pub enum SendOutcome {
    Delivered,
    DeadToken,
    Failed(String),
}

impl PushClient {
    pub fn from_config(cfg: &PushConfig) -> anyhow::Result<Self> {
        let pem = std::fs::read(&cfg.key_path)
            .map_err(|e| anyhow::anyhow!("reading APNs key {}: {e}", cfg.key_path))?;
        let key = EncodingKey::from_ec_pem(&pem)
            .map_err(|e| anyhow::anyhow!("parsing APNs .p8 key: {e}"))?;

        Ok(PushClient {
            key,
            key_id: cfg.key_id.clone(),
            team_id: cfg.team_id.clone(),
            bundle_id: cfg.bundle_id.clone(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            jwt_cache: Mutex::new(None),
        })
    }

    fn jwt(&self, now: i64) -> anyhow::Result<String> {
        let mut cache = self.jwt_cache.lock().unwrap();
        if let Some((token, minted)) = cache.as_ref() {
            if now - minted < JWT_TTL_SECS {
                return Ok(token.clone());
            }
        }
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.key_id.clone());
        let claims = Claims {
            iss: &self.team_id,
            iat: now,
        };
        let token = jsonwebtoken::encode(&header, &claims, &self.key)?;
        *cache = Some((token.clone(), now));
        Ok(token)
    }

    /// Send one payload to one device token. `collapse_id` replaces earlier
    /// notifications for the same buddy on the lock screen.
    pub async fn send(
        &self,
        token: &str,
        payload: &serde_json::Value,
        collapse_id: &str,
        env: &str,
    ) -> SendOutcome {
        let host = if env == "production" {
            "api.push.apple.com"
        } else {
            "api.sandbox.push.apple.com"
        };
        let now = chrono::Utc::now().timestamp();
        let jwt = match self.jwt(now) {
            Ok(jwt) => jwt,
            Err(error) => return SendOutcome::Failed(format!("JWT sign failed: {error}")),
        };
        let url = format!("https://{host}/3/device/{token}");
        let response = self
            .http
            .post(&url)
            .header("authorization", format!("bearer {jwt}"))
            .header("apns-topic", self.bundle_id.as_str())
            .header("apns-push-type", "alert")
            .header("apns-priority", "10")
            .header("apns-collapse-id", collapse_id)
            .json(payload)
            .send()
            .await;
        match response {
            Ok(reply) => {
                let status = reply.status();
                if status.is_success() {
                    return SendOutcome::Delivered;
                }
                let body = reply.text().await.unwrap_or_default();
                if is_dead_token(status.as_u16(), &body) {
                    SendOutcome::DeadToken
                } else {
                    SendOutcome::Failed(format!("APNs {status}: {body}"))
                }
            }
            Err(error) => SendOutcome::Failed(error.to_string()),
        }
    }
}

/// The buddy-heard notification payload.
pub fn build_payload(
    callsign: &str,
    name: Option<&str>,
    dmr_id: u32,
    talkgroup: u32,
    tg_name: Option<&str>,
) -> serde_json::Value {
    let title = format!("{callsign} on TG {talkgroup}");
    let body = match (name, tg_name) {
        (Some(who), Some(where_)) => format!("{who} · {where_}"),
        (Some(who), None) => who.to_string(),
        (None, Some(where_)) => where_.to_string(),
        (None, None) => "keyed up".to_string(),
    };
    serde_json::json!({
        "aps": {
            "alert": { "title": title, "body": body },
            "sound": "default",
            "thread-id": "buddy",
            "interruption-level": "time-sensitive",
        },
        "call": callsign,
        "dmr_id": dmr_id,
        "tg": talkgroup,
    })
}

/// The test-push payload for POST /v1/devices/:id/test.
pub fn build_test_payload() -> serde_json::Value {
    serde_json::json!({
        "aps": {
            "alert": {
                "title": "Buddy watch is working",
                "body": "Test notification from dmr-lookout",
            },
            "sound": "default",
            "thread-id": "buddy",
        },
    })
}

/// 410 always; 400 only for the token-specific reasons (a 400 for a
/// malformed payload must NOT prune a real device).
fn is_dead_token(status: u16, body: &str) -> bool {
    status == 410
        || (status == 400 && (body.contains("BadDeviceToken") || body.contains("Unregistered")))
}

#[cfg(test)]
mod tests {
    use super::*;

    // A throwaway P-256 key generated for tests only — NOT an Apple key.
    const TEST_P8: &[u8] = b"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgqBMZVe9+O4rb0iWq
W+AwiMMJ/ltOpC94rlK2EN6uI4yhRANCAAQUV9sjXiNRM3nB2a3J7WItoZbJ6KvL
veKTfRGwsrK+uQR/lpLpNn6SZzuYnI9aXf8XgJa4WZrVLQ8A2KIKZKhW
-----END PRIVATE KEY-----";

    fn test_client() -> PushClient {
        PushClient {
            key: EncodingKey::from_ec_pem(TEST_P8).unwrap(),
            key_id: "KEY1234567".into(),
            team_id: "TEAM123456".into(),
            bundle_id: "com.carrierwave.DMRMonitor".into(),
            http: reqwest::Client::new(),
            jwt_cache: Mutex::new(None),
        }
    }

    #[test]
    fn payload_shape() {
        let payload = build_payload("W1ABC", Some("Bob"), 3121234, 3100, Some("US Nationwide"));
        assert_eq!(payload["aps"]["alert"]["title"], "W1ABC on TG 3100");
        assert_eq!(payload["aps"]["alert"]["body"], "Bob · US Nationwide");
        assert_eq!(payload["aps"]["thread-id"], "buddy");
        assert_eq!(payload["call"], "W1ABC");
        assert_eq!(payload["tg"], 3100);
    }

    #[test]
    fn payload_degrades_without_name() {
        let payload = build_payload("W1ABC", None, 1, 91, None);
        assert_eq!(payload["aps"]["alert"]["body"], "keyed up");
    }

    #[test]
    fn dead_token_classification() {
        assert!(is_dead_token(410, ""));
        assert!(is_dead_token(400, r#"{"reason":"BadDeviceToken"}"#));
        assert!(is_dead_token(400, r#"{"reason":"Unregistered"}"#));
        assert!(!is_dead_token(400, r#"{"reason":"PayloadTooLarge"}"#));
        assert!(!is_dead_token(429, ""));
    }

    #[test]
    fn jwt_is_es256_and_cached() {
        let client = test_client();
        let first = client.jwt(1_000_000).unwrap();
        assert_eq!(first.split('.').count(), 3);
        let cached = client.jwt(1_000_000 + JWT_TTL_SECS - 1).unwrap();
        assert_eq!(first, cached);
        let reminted = client.jwt(1_000_000 + JWT_TTL_SECS + 1).unwrap();
        assert_ne!(first, reminted);
    }
}
