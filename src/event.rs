//! Pure parsing of BrandMeister last-heard socket.io frames into events.
//! Ported from DMRMonitor's BrandmeisterLH.swift; unit-tested against
//! captured fixtures.

use serde_json::Value;

/// One call event off the feed.
#[derive(Debug, Clone, PartialEq)]
pub struct LhEvent {
    pub source_id: u32,
    pub source_call: String,
    pub source_name: Option<String>,
    pub destination_id: u32,
    pub destination_name: Option<String>,
    /// Unix seconds from the payload; 0 when absent.
    pub start: i64,
    pub stop: i64,
    /// Session-Start (or absent-with-stop-0) — the only kind that notifies.
    pub active: bool,
}

/// Parse a raw `42[...]` socket.io text frame. Returns None for anything
/// that isn't a well-formed mqtt call event.
pub fn parse_frame(text: &str) -> Option<LhEvent> {
    let bracket = text.find('[')?;
    let array: Value = serde_json::from_str(&text[bracket..]).ok()?;
    let array = array.as_array()?;
    if array.first()?.as_str()? != "mqtt" {
        return None;
    }
    // The wrapper may be a dict or a JSON string, and its payload likewise
    let wrapper = unwrap(array.get(1)?);
    let payload = match wrapper.get("payload") {
        Some(inner) => unwrap(inner),
        None => wrapper,
    };
    parse_call(&payload)
}

fn parse_call(call: &Value) -> Option<LhEvent> {
    let source_id = as_u32(call.get("SourceID"))?;
    let destination_id = as_u32(call.get("DestinationID"))?;
    let source_call = call
        .get("SourceCall")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_uppercase();
    let event = call.get("Event").and_then(Value::as_str).unwrap_or("");
    let start = as_u32(call.get("Start")).unwrap_or(0) as i64;
    let stop = as_u32(call.get("Stop")).unwrap_or(0) as i64;
    Some(LhEvent {
        source_id,
        source_call,
        source_name: nonempty(call.get("SourceName")),
        destination_id,
        destination_name: nonempty(call.get("DestinationName")),
        start,
        stop,
        active: event != "Session-Stop" && stop == 0,
    })
}

/// BM has shipped both dicts and JSON-encoded strings for the same fields.
fn unwrap(value: &Value) -> Value {
    if let Some(text) = value.as_str() {
        if let Ok(parsed) = serde_json::from_str::<Value>(text) {
            return parsed;
        }
    }
    value.clone()
}

/// BM ships numerics as int, float, or string depending on the day.
fn as_u32(value: Option<&Value>) -> Option<u32> {
    let value = value?;
    if let Some(num) = value.as_u64() {
        return u32::try_from(num).ok();
    }
    if let Some(num) = value.as_f64() {
        if num >= 0.0 && num <= u32::MAX as f64 {
            return Some(num as u32);
        }
    }
    value.as_str()?.parse().ok()
}

fn nonempty(value: Option<&Value>) -> Option<String> {
    let text = value?.as_str()?.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

/// Normalize a callsign to its base: uppercase, truncated at the first
/// `/` or `-` (W6JY/P, W6JY-7 -> W6JY).
pub fn normalize_call(call: &str) -> String {
    let upper = call.trim().to_uppercase();
    upper
        .split(['/', '-'])
        .next()
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const WRAPPED: &str = r#"42["mqtt",{"topic":"LH","payload":"{\"SourceID\":3121234,\"SourceCall\":\"W1ABC\",\"SourceName\":\"Bob\",\"DestinationID\":91,\"DestinationName\":\"Worldwide\",\"Start\":1788886237,\"Stop\":0,\"Event\":\"Session-Start\"}"}]"#;

    #[test]
    fn parses_wrapped_payload() {
        let event = parse_frame(WRAPPED).unwrap();
        assert_eq!(event.source_id, 3_121_234);
        assert_eq!(event.source_call, "W1ABC");
        assert_eq!(event.source_name.as_deref(), Some("Bob"));
        assert_eq!(event.destination_id, 91);
        assert!(event.active);
        assert_eq!(event.start, 1_788_886_237);
    }

    #[test]
    fn session_stop_is_inactive() {
        let frame = r#"42["mqtt",{"topic":"LH","payload":"{\"SourceID\":1,\"SourceCall\":\"X\",\"DestinationID\":91,\"Start\":10,\"Stop\":20,\"Event\":\"Session-Stop\"}"}]"#;
        assert!(!parse_frame(frame).unwrap().active);
    }

    #[test]
    fn nonzero_stop_without_event_is_inactive() {
        let frame = r#"42["mqtt",{"topic":"LH","payload":"{\"SourceID\":1,\"SourceCall\":\"X\",\"DestinationID\":91,\"Start\":10,\"Stop\":20}"}]"#;
        assert!(!parse_frame(frame).unwrap().active);
    }

    #[test]
    fn dict_payload_and_string_numbers() {
        let frame = r#"42["mqtt",{"topic":"LH","payload":{"SourceID":"3121234","SourceCall":"w6jy/p","DestinationID":3100.0,"Start":1,"Stop":0}}]"#;
        let event = parse_frame(frame).unwrap();
        assert_eq!(event.source_id, 3_121_234);
        assert_eq!(event.source_call, "W6JY/P");
        assert_eq!(event.destination_id, 3100);
    }

    #[test]
    fn blank_call_still_parses() {
        let frame = r#"42["mqtt",{"topic":"LH","payload":"{\"SourceID\":2150290,\"SourceCall\":\"\",\"DestinationID\":91,\"Start\":1,\"Stop\":0}"}]"#;
        let event = parse_frame(frame).unwrap();
        assert_eq!(event.source_call, "");
        assert!(event.active);
    }

    #[test]
    fn junk_frames_are_none() {
        assert!(parse_frame("2").is_none());
        assert!(parse_frame(r#"42["searchHouseComplete"]"#).is_none());
        assert!(parse_frame(r#"42["mqtt","not json"]"#).is_none());
    }

    #[test]
    fn callsign_normalization() {
        assert_eq!(normalize_call("W6JY/P"), "W6JY");
        assert_eq!(normalize_call("w6jy-7"), "W6JY");
        assert_eq!(normalize_call(" ea1abc "), "EA1ABC");
        assert_eq!(normalize_call(""), "");
    }

    #[test]
    fn fixtures_parse() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/lh_frames.jsonl"
        );
        let Ok(data) = std::fs::read_to_string(path) else {
            // Fixture capture is optional in CI
            return;
        };
        let mut parsed = 0;
        let mut active = 0;
        for line in data.lines().filter(|l| !l.is_empty()) {
            if let Some(event) = parse_frame(line) {
                parsed += 1;
                if event.active {
                    active += 1;
                }
            }
        }
        assert!(parsed > 10, "expected >10 parsed events, got {parsed}");
        assert!(active > 0, "expected some Session-Start events");
    }
}
