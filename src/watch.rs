//! Pure matching + notification decision logic.

use crate::event::{normalize_call, LhEvent};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Watch {
    pub device_id: String,
    pub callsign: String,
    pub dmr_id: u32,
    pub label: String,
    pub talkgroups: Vec<u32>,
}

impl Watch {
    /// Stable cooldown key: survives whole-list PUT replaces.
    pub fn key(&self) -> String {
        if self.callsign.is_empty() {
            format!("id:{}", self.dmr_id)
        } else {
            self.callsign.clone()
        }
    }
}

#[derive(Debug, Clone)]
pub struct Device {
    pub id: String,
    pub apns_token: String,
    pub apns_env: String,
    pub quiet_start: Option<i64>,
    pub quiet_end: Option<i64>,
    pub tz: String,
}

/// In-memory index rebuilt from the DB on boot and after every write;
/// the hot path never queries SQLite.
#[derive(Default)]
pub struct WatchIndex {
    pub devices: HashMap<String, Device>,
    by_call: HashMap<String, Vec<Watch>>,
    by_id: HashMap<u32, Vec<Watch>>,
}

impl WatchIndex {
    pub fn build(devices: Vec<Device>, watches: Vec<Watch>) -> WatchIndex {
        let mut index = WatchIndex {
            devices: devices.into_iter().map(|d| (d.id.clone(), d)).collect(),
            ..WatchIndex::default()
        };
        for watch in watches {
            if !watch.callsign.is_empty() {
                index
                    .by_call
                    .entry(watch.callsign.clone())
                    .or_default()
                    .push(watch.clone());
            }
            if watch.dmr_id > 0 {
                index.by_id.entry(watch.dmr_id).or_default().push(watch);
            }
        }
        index
    }

    pub fn watch_count(&self) -> usize {
        let calls: usize = self.by_call.values().map(Vec::len).sum();
        let ids: usize = self
            .by_id
            .values()
            .flatten()
            .filter(|w| w.callsign.is_empty())
            .count();
        calls + ids
    }

    /// Watches matching an event by normalized callsign OR source DMR ID,
    /// deduped per (device, watch key), with the TG filter applied.
    pub fn matches(&self, event: &LhEvent) -> Vec<&Watch> {
        let call = normalize_call(&event.source_call);
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        let candidates = self
            .by_call
            .get(&call)
            .into_iter()
            .flatten()
            .chain(self.by_id.get(&event.source_id).into_iter().flatten());
        for watch in candidates {
            if !watch.talkgroups.is_empty() && !watch.talkgroups.contains(&event.destination_id)
            {
                continue;
            }
            if seen.insert((watch.device_id.clone(), watch.key())) {
                result.push(watch);
            }
        }
        result
    }
}

/// Why a matched watch did not notify; surfaced in logs.
#[derive(Debug, PartialEq)]
pub enum Suppression {
    Inactive,
    Stale,
    ConnectGrace,
    Cooldown,
    HourlyCap,
    QuietHours,
}

pub struct RuleParams {
    pub cooldown_secs: i64,
    pub max_pushes_per_hour: usize,
    pub freshness_secs: i64,
    pub connect_grace_secs: i64,
}

/// The pure notify decision for one (event, watch, device).
#[allow(clippy::too_many_arguments)]
pub fn decide(
    event: &LhEvent,
    device: &Device,
    params: &RuleParams,
    now: i64,
    connected_at: i64,
    last_push_at: Option<i64>,
    pushes_last_hour: usize,
) -> Result<(), Suppression> {
    if !event.active {
        return Err(Suppression::Inactive);
    }
    if event.start > 0 && now - event.start > params.freshness_secs {
        return Err(Suppression::Stale);
    }
    if now - connected_at < params.connect_grace_secs {
        return Err(Suppression::ConnectGrace);
    }
    if let Some(last) = last_push_at {
        if now - last < params.cooldown_secs {
            return Err(Suppression::Cooldown);
        }
    }
    if pushes_last_hour >= params.max_pushes_per_hour {
        return Err(Suppression::HourlyCap);
    }
    if in_quiet_hours(device, now) {
        return Err(Suppression::QuietHours);
    }
    Ok(())
}

/// Quiet hours in the device's own timezone; suppressed entirely, not queued.
fn in_quiet_hours(device: &Device, now: i64) -> bool {
    let (Some(start), Some(end)) = (device.quiet_start, device.quiet_end) else {
        return false;
    };
    let Ok(zone) = device.tz.parse::<chrono_tz::Tz>() else {
        return false;
    };
    let Some(stamp) = chrono::DateTime::from_timestamp(now, 0) else {
        return false;
    };
    let hour = i64::from(chrono::Timelike::hour(&stamp.with_timezone(&zone)));
    if start <= end {
        hour >= start && hour < end
    } else {
        // Wrapping window, e.g. 22..7
        hour >= start || hour < end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(call: &str, src: u32, dst: u32, active: bool, start: i64) -> LhEvent {
        LhEvent {
            source_id: src,
            source_call: call.into(),
            source_name: None,
            destination_id: dst,
            destination_name: None,
            start,
            stop: if active { 0 } else { start + 5 },
            active,
        }
    }

    fn watch(call: &str, dmr_id: u32, tgs: Vec<u32>) -> Watch {
        Watch {
            device_id: "dev1".into(),
            callsign: call.into(),
            dmr_id,
            label: String::new(),
            talkgroups: tgs,
        }
    }

    fn device() -> Device {
        Device {
            id: "dev1".into(),
            apns_token: "tok".into(),
            apns_env: "sandbox".into(),
            quiet_start: None,
            quiet_end: None,
            tz: String::new(),
        }
    }

    fn params() -> RuleParams {
        RuleParams {
            cooldown_secs: 1800,
            max_pushes_per_hour: 12,
            freshness_secs: 120,
            connect_grace_secs: 10,
        }
    }

    #[test]
    fn matches_by_normalized_call() {
        let index = WatchIndex::build(vec![device()], vec![watch("W6JY", 0, vec![])]);
        assert_eq!(index.matches(&event("W6JY/P", 123, 91, true, 0)).len(), 1);
        assert_eq!(index.matches(&event("K6XYZ", 123, 91, true, 0)).len(), 0);
    }

    #[test]
    fn blank_source_call_matches_by_id() {
        let index = WatchIndex::build(vec![device()], vec![watch("W6JY", 3121234, vec![])]);
        assert_eq!(index.matches(&event("", 3121234, 91, true, 0)).len(), 1);
    }

    #[test]
    fn call_and_id_match_dedupes() {
        let index = WatchIndex::build(vec![device()], vec![watch("W6JY", 3121234, vec![])]);
        assert_eq!(index.matches(&event("W6JY", 3121234, 91, true, 0)).len(), 1);
    }

    #[test]
    fn tg_filter_applies() {
        let index = WatchIndex::build(vec![device()], vec![watch("W6JY", 0, vec![3100])]);
        assert_eq!(index.matches(&event("W6JY", 1, 91, true, 0)).len(), 0);
        assert_eq!(index.matches(&event("W6JY", 1, 3100, true, 0)).len(), 1);
    }

    #[test]
    fn session_stop_never_notifies() {
        let now = 1000;
        let result = decide(
            &event("W6JY", 1, 91, false, now),
            &device(),
            &params(),
            now,
            0,
            None,
            0,
        );
        assert_eq!(result, Err(Suppression::Inactive));
    }

    #[test]
    fn stale_events_are_dropped() {
        let now = 10_000;
        let result = decide(
            &event("W6JY", 1, 91, true, now - 500),
            &device(),
            &params(),
            now,
            0,
            None,
            0,
        );
        assert_eq!(result, Err(Suppression::Stale));
    }

    #[test]
    fn connect_grace_suppresses() {
        let now = 10_000;
        let result = decide(
            &event("W6JY", 1, 91, true, now),
            &device(),
            &params(),
            now,
            now - 5,
            None,
            0,
        );
        assert_eq!(result, Err(Suppression::ConnectGrace));
    }

    #[test]
    fn cooldown_suppresses_then_expires() {
        let now = 100_000;
        let base = (
            event("W6JY", 1, 91, true, now),
            device(),
            params(),
        );
        let inside = decide(&base.0, &base.1, &base.2, now, 0, Some(now - 60), 0);
        assert_eq!(inside, Err(Suppression::Cooldown));
        let outside = decide(&base.0, &base.1, &base.2, now, 0, Some(now - 2000), 0);
        assert_eq!(outside, Ok(()));
    }

    #[test]
    fn hourly_cap_suppresses() {
        let now = 100_000;
        let result = decide(
            &event("W6JY", 1, 91, true, now),
            &device(),
            &params(),
            now,
            0,
            None,
            12,
        );
        assert_eq!(result, Err(Suppression::HourlyCap));
    }

    #[test]
    fn quiet_hours_wrap_midnight() {
        let mut dev = device();
        dev.quiet_start = Some(22);
        dev.quiet_end = Some(7);
        dev.tz = "UTC".into();
        // 23:00 UTC on 2026-09-08
        let late = 1_788_908_400;
        let result = decide(
            &event("W6JY", 1, 91, true, late),
            &dev,
            &params(),
            late,
            0,
            None,
            0,
        );
        assert_eq!(result, Err(Suppression::QuietHours));
        // 12:00 UTC is outside the window
        let noon = 1_788_868_800;
        let result = decide(
            &event("W6JY", 1, 91, true, noon),
            &dev,
            &params(),
            noon,
            0,
            None,
            0,
        );
        assert_eq!(result, Ok(()));
    }
}
