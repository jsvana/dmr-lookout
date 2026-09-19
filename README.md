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
  whole-list watch replace, test push, claim-link requests.
- `auth.rs` / `email.rs` — magic-link auth (SHA-256'd single-use tokens,
  15-min TTL, per-email rate limit) delivered via the Resend API; without
  `LOOKOUT_RESEND_KEY` links are logged instead (dev mode).
- `web.rs` + `templates/` — server-rendered, mobile-friendly UI: email
  sign-in, account watch editing, notification channels, notification
  history (90-day retention, pruned by a 6-hourly maintenance task).
  Plain form POSTs, no JS.

Beyond APNs, an account can turn on email delivery (to the account
address) and/or a webhook: the server POSTs
`{callsign, label, dmr_id, talkgroup, talkgroup_name, event_time}` as
JSON. Channel notifications fire once per account per event — devices
optional — under the same cooldown/hourly-cap rules, keyed `acct:<id>`
in `notify_state`. Webhook URLs must be public http(s).

## Accounts

Devices start anonymous, exactly as before. The iOS app can request a
magic link (`POST /v1/auth/request`); clicking it claims the device into
an email account and merges the device's watches into the account list
(deduped by watch key). From then on the account owns the list: the web
UI and every claimed device edit the same watches, and
`GET/PUT /v1/devices/:id/watches` transparently maps to the account list
so old app builds keep working. Unclaimed devices keep per-device lists
indefinitely.

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
| `GET/PUT /v1/devices/:id/watches` | bearer | PUT replaces the whole list (account's if claimed) |
| `GET /v1/devices/:id` | bearer | claim status: `{claimed, account_email}` |
| `DELETE /v1/devices/:id` | bearer | deregister |
| `POST /v1/devices/:id/test` | bearer | send a test push |
| `POST /v1/auth/request` | bearer | `{email, device_id}` → emails a claim link |
| `/login`, `/auth/verify`, `/`, `/history` | session cookie | web UI (magic-link sign-in) |

Watch entry: `{callsign, dmr_id, label, talkgroups: [..]}` (empty
talkgroups = match anywhere).

## Deploy

GitHub Actions builds a musl x86_64 tarball on tag push. Deployed to the
Hetzner VPS as `dmr.carrierwave.app` via `carrier_wave/infra/ansible`
(`--tags dmr-lookout`): `/opt/dmr-lookout`, systemd, nginx + certbot.
