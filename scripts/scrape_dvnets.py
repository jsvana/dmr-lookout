#!/usr/bin/env python3
"""Scrape dvnets.com into DMRMonitor's dmr-nets.json import format.

Polite: ~2 req/sec, identifiable UA, local page cache. Net ids are
uuid5(slug), so re-running and re-importing updates nets in place.
Only DMR nets with a parseable talkgroup are emitted; everything ships
enabled=false so importing 200 nets doesn't blow the reminder budget.
"""
import json
import re
import sys
import time
import uuid
import urllib.request
from pathlib import Path

BASE = "https://dvnets.com"
UA = "dmr-lookout-nets/0.1 (ham radio app; contact: jay@jsvana.net)"
CACHE = Path("/tmp/dvnets-cache")
DELAY_SECS = 0.5
WEEKDAYS = {
    "sunday": 1, "monday": 2, "tuesday": 3, "wednesday": 4,
    "thursday": 5, "friday": 6, "saturday": 7,
}
NETWORKS = ["BrandMeister", "TGIF", "TIGF", "DMR+", "FreeDMR", "DMR-MARC",
            "QRM Network", "QRM"]


def fetch(url: str) -> str:
    CACHE.mkdir(exist_ok=True)
    key = CACHE / re.sub(r"[^a-z0-9-]", "_", url.lower())
    if key.exists():
        return key.read_text()
    request = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(request, timeout=30) as reply:
        text = reply.read().decode("utf-8", errors="replace")
    key.write_text(text)
    time.sleep(DELAY_SECS)
    return text


def net_urls() -> list[str]:
    sitemap = fetch(f"{BASE}/sitemap-0.xml")
    urls = re.findall(r"<loc>([^<]+/nets/[^<]+)</loc>", sitemap)
    return [u.rstrip("/") for u in urls if not u.rstrip("/").endswith("/nets")]


def strip_tags(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


def parse_schedules(page: str) -> list[dict]:
    """Rows like 'Saturday 8:00 PM America/New_York · 60 minutes'."""
    schedules = []
    for row_html in re.findall(
        r'schedule-row[^>]*>(.*?)</div>\s*</div>', page, re.S
    ):
        text = strip_tags(row_html)
        match = re.search(
            r"(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)s?\s+"
            r"(\d{1,2}):(\d{2})\s*(AM|PM)?\s+([A-Za-z_]+/[A-Za-z_+-]+|UTC)",
            text,
        )
        if not match:
            continue
        weekday = WEEKDAYS[match.group(1).lower()]
        hour = int(match.group(2)) % 12
        if match.group(4) == "PM":
            hour += 12
        elif match.group(4) is None and int(match.group(2)) > 12:
            hour = int(match.group(2))
        duration = 60
        dur_match = re.search(r"(\d+)\s*minutes", text)
        if dur_match:
            duration = int(dur_match.group(1))
        schedules.append({
            "weekday": weekday,
            "hour": hour,
            "minute": int(match.group(3)),
            "zone": match.group(5),
            "duration": duration,
        })
    return schedules


def parse_dmr_access(page: str) -> tuple[str, int, str] | None:
    """The main 'How to join' DMR access box -> (network, talkgroup, text).

    Markup: <div class="access-box"><span>DMR</span><strong>TGIF TG 45768
    or 50853</strong>…  Multiple TGs listed -> first one wins, the rest
    stay in the notes.
    """
    for match in re.finditer(
        r'access-box[^>]*>\s*<span[^>]*>\s*DMR\s*</span>\s*'
        r"<strong[^>]*>(.*?)</strong>",
        page,
        re.S,
    ):
        detail = strip_tags(match.group(1))
        network = "DMR"
        for name in NETWORKS:
            if re.search(re.escape(name), detail, re.I):
                network = {"TIGF": "TGIF", "QRM": "QRM Network"}.get(name, name)
                break
        tg_match = re.search(r"\b(\d{2,8})\b", detail)
        if not tg_match:
            continue
        return network, int(tg_match.group(1)), detail
    return None


def scrape_net(url: str) -> list[dict]:
    slug = url.rsplit("/", 1)[-1]
    try:
        page = fetch(url)
    except Exception as error:
        print(f"  ! {slug}: {error}", file=sys.stderr)
        return []
    title = re.search(r"<h1[^>]*>(.*?)</h1>", page, re.S)
    name = strip_tags(title.group(1)) if title else slug
    # The page embeds OTHER nets' cards under "Related nets" — without
    # this cut, every net inherits the first related card's talkgroup
    related = re.search(r"Related nets|net-card card compact", page)
    if related:
        page = page[:related.start()]
    access = parse_dmr_access(page)
    if access is None:
        return []
    network, talkgroup, detail = access
    schedules = parse_schedules(page)
    if not schedules:
        return []

    # One Net per distinct (time, zone); same-time weekdays merge
    grouped: dict[tuple, dict] = {}
    for sched in schedules:
        key = (sched["hour"], sched["minute"], sched["zone"])
        entry = grouped.setdefault(key, {**sched, "weekdays": []})
        if sched["weekday"] not in entry["weekdays"]:
            entry["weekdays"].append(sched["weekday"])

    nets = []
    for index, entry in enumerate(grouped.values()):
        suffix = f"#{index}" if index else ""
        nets.append({
            "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{BASE}/nets/{slug}{suffix}")),
            "name": name,
            "network": network,
            "talkgroup": talkgroup,
            "weekdays": sorted(entry["weekdays"]),
            "hour": entry["hour"],
            "minute": entry["minute"],
            "timeZoneID": entry["zone"],
            "durationMin": entry["duration"],
            "leads": [10],
            "enabled": False,
            "notes": f"{detail} — via dvnets.com/nets/{slug}",
        })
    return nets


def main() -> None:
    urls = net_urls()
    print(f"{len(urls)} net pages", file=sys.stderr)
    nets = []
    for count, url in enumerate(urls, 1):
        nets.extend(scrape_net(url))
        if count % 50 == 0:
            print(f"  {count}/{len(urls)} pages, {len(nets)} DMR nets",
                  file=sys.stderr)
    nets.sort(key=lambda item: item["name"].lower())
    print(json.dumps({"version": 1, "nets": nets}, indent=1))
    print(f"done: {len(nets)} DMR nets", file=sys.stderr)


if __name__ == "__main__":
    main()
