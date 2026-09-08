# dmr-lookout

BrandMeister buddy watch. Follows the BM last-heard Socket.IO feed
(`everything` room, ~11 events/sec), matches watched callsigns/DMR IDs
against per-device watch lists, and sends APNs push notifications to the
DMRMonitor iOS app when a buddy keys up.

## How it works

- `feed.rs` — Engine.IO v4 websocket client (reconnect w/ backoff, stall
  watchdog). Never calls `searchHouse`: history must not push.
- `event.rs` — pure frame parsing, tested against captured fixtures.
- `watch.rs` — pure matching (normalized callsign OR DMR ID, optional TG
  filter) + notify rules: Session-Start only, 120s freshness gate, 10s
  connect grace, 30-min per-buddy cooldown, 12/hour per-device cap,
  optional quiet hours.
- `push.rs` — APNs token auth (ES256 JWT off a `.p8`), HTTP/2 via
  reqwest+rustls, dead-token pruning. `apns-collapse-id` per buddy so
  repeat hits replace instead of stacking.
- `api.rs` — axum: open `/v1/health`; bearer-gated device registration,
  whole-list watch replace, test push.

## Run

```
cp config.example.toml config.toml   # fill in [push]
LOOKOUT_API_TOKEN=... cargo run --release
```

Sandbox vs production APNs matters: a dev-signed app vends sandbox
tokens; sending one to the production host gets it pruned as dead.
`/v1/health` reports which host is configured.

## API

| Route | Auth | |
|---|---|---|
| `GET /v1/health` | none | feed + push status |
| `POST /v1/devices` | bearer | `{device_id, apns_token, platform, app_version}` |
| `GET/PUT /v1/devices/:id/watches` | bearer | PUT replaces the whole list |
| `DELETE /v1/devices/:id` | bearer | deregister |
| `POST /v1/devices/:id/test` | bearer | send a test push |

Watch entry: `{callsign, dmr_id, label, talkgroups: [..]}` (empty
talkgroups = match anywhere).

## Deploy

GitHub Actions builds a musl x86_64 tarball on tag push. Deployed to the
Hetzner VPS as `dmr.carrierwave.app` via `carrier_wave/infra/ansible`
(`--tags dmr-lookout`): `/opt/dmr-lookout`, systemd, nginx + certbot.
