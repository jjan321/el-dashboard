"""
fetch_data.py  –  Körs av GitHub Actions varje timme.
Hämtar spotpriser + meta och sparar till data/prices.json

Datakällor (server-side = noll CORS-problem):
  • SE1-SE4 : www.elprisenligenu.se
  • NO1-NO5  : www.hvakosterstrommen.no
  • FI       : www.hvakosterstrommen.no
"""
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta, datetime, timezone

import requests

ZONES = {
    "SE1": "SE", "SE2": "SE", "SE3": "SE", "SE4": "SE",
    "NO1": "NO", "NO2": "NO", "NO3": "NO", "NO4": "NO", "NO5": "NO",
    "FI":  "NO",
}
SE_BASE = "https://www.elprisenligenu.se/api/v1/prices"
NO_BASE = "https://www.hvakosterstrommen.no/api/v1/prices"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "nordic-el-dashboard/1.0 (github.com)"})


def fetch_day(zone: str, dt: date) -> list[dict]:
    base = SE_BASE if ZONES[zone] == "SE" else NO_BASE
    url  = f"{base}/{dt.year}/{dt.month:02d}-{dt.day:02d}_{zone}.json"
    try:
        r = SESSION.get(url, timeout=10)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return [
            {"time": rec["time_start"], "price": rec["EUR_per_kWh"] * 1000}
            for rec in r.json()
            if rec.get("EUR_per_kWh") is not None
        ]
    except Exception as e:
        print(f"  WARN {zone} {dt}: {e}")
        return []


def fetch_zone(zone: str, days_back: int = 32) -> list[dict]:
    today = date.today()
    days  = [today - timedelta(days=i) for i in range(days_back, -2, -1)]
    rows  = []
    # Fetch all days in parallel
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch_day, zone, d): d for d in days}
        for fut in as_completed(futures):
            rows.extend(fut.result())
    # Deduplicate and sort
    seen, deduped = set(), []
    for r in sorted(rows, key=lambda x: x["time"]):
        if r["time"] not in seen:
            seen.add(r["time"])
            deduped.append(r)
    return deduped


def main():
    print(f"Starting fetch at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    all_data = {}

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(fetch_zone, z): z for z in ZONES}
        for fut in as_completed(futures):
            zone = futures[fut]
            data = fut.result()
            all_data[zone] = data
            print(f"  {zone}: {len(data)} timmar")

    # Write to data/prices.json
    os.makedirs("data", exist_ok=True)
    output = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "zones":   all_data,
    }
    with open("data/prices.json", "w") as f:
        json.dump(output, f, separators=(",", ":"))

    # Verify
    total = sum(len(v) for v in all_data.values())
    print(f"Done. {total} totala datapunkter sparade till data/prices.json")


if __name__ == "__main__":
    main()
