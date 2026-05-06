"""
fetch_data.py  –  Körs av GitHub Actions varje timme.
Hämtar och beräknar:
  1. Spotpriser (SE/NO/FI) – 32 dagars historik + day-ahead
  2. Capture rates – vindvägd genomsnittspris vs spot-snitt per månad och zon
  3. Prisprogtnossignal 14 dagar – trafikljus baserat på vind, temperatur, nederbörd
"""
import json, os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta, datetime, timezone
from collections import defaultdict

import requests

# ── KONFIGURATION ──────────────────────────────────────────────────────────────
ZONES = {
    "SE1": "SE", "SE2": "SE", "SE3": "SE", "SE4": "SE",
    "NO1": "NO", "NO2": "NO", "NO3": "NO", "NO4": "NO", "NO5": "NO",
    "FI":  "NO",
}
ZONE_COORDS = {
    "SE1": (65.58, 22.15), "SE2": (62.39, 17.31),
    "SE3": (58.50, 15.50), "SE4": (56.20, 13.80),
    "NO1": (59.91, 10.75), "NO2": (58.16,  7.99),
    "NO3": (63.43, 10.39), "NO4": (67.28, 14.38),
    "NO5": (60.39,  5.32), "FI":  (62.50, 25.50),
}
# Representativa punkter för prognosdata (täcker hela Norden)
FORECAST_POINTS = {
    "Norrland":    (65.0, 19.0),
    "Mellansv.":   (59.5, 15.5),
    "Sydsv.":      (56.0, 13.5),
    "Norska fjord": (60.5,  6.0),
    "N. Norge":    (67.5, 14.5),
}

SE_BASE = "https://www.elprisenligenu.se/api/v1/prices"
NO_BASE = "https://www.hvakosterstrommen.no/api/v1/prices"
OM_FORECAST = "https://api.open-meteo.com/v1/forecast"
OM_ARCHIVE  = "https://archive-api.open-meteo.com/v1/archive"

S = requests.Session()
S.headers.update({"User-Agent": "locus-energy-dashboard/1.0"})

def wcap(ws):
    """Vindkraft effektkurva: inkörning 3 m/s, märkeffekt 12 m/s, stopp 25 m/s."""
    if ws < 3:   return 0.0
    if ws < 12:  return ((ws - 3) / 9) ** 3
    if ws <= 25: return 1.0
    return 0.0

# ── 1. SPOTPRISER ──────────────────────────────────────────────────────────────
def fetch_day(zone: str, dt: date) -> list:
    base = SE_BASE if ZONES[zone] == "SE" else NO_BASE
    url  = f"{base}/{dt.year}/{dt.month:02d}-{dt.day:02d}_{zone}.json"
    try:
        r = S.get(url, timeout=10)
        if r.status_code == 404: return []
        r.raise_for_status()
        return [{"time": x["time_start"], "price": x["EUR_per_kWh"] * 1000}
                for x in r.json() if x.get("EUR_per_kWh") is not None]
    except Exception as e:
        print(f"  WARN {zone} {dt}: {e}")
        return []

def fetch_zone_prices(zone: str, days_back=32) -> list:
    today = date.today()
    days  = [today - timedelta(days=i) for i in range(days_back, -2, -1)]
    rows  = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for fut in as_completed({ex.submit(fetch_day, zone, d): d for d in days}):
            rows.extend(fut.result())
    seen, out = set(), []
    for r in sorted(rows, key=lambda x: x["time"]):
        if r["time"] not in seen:
            seen.add(r["time"]); out.append(r)
    return out

# ── 2. HISTORISK VIND (ERA5) ───────────────────────────────────────────────────
def fetch_archive_wind(lat: float, lon: float, start: date, end: date) -> dict:
    """Hämtar ERA5-reanalysdata timme för timme (100 m navhöjd)."""
    try:
        r = S.get(OM_ARCHIVE, params={
            "latitude": lat, "longitude": lon,
            "start_date": start.isoformat(), "end_date": end.isoformat(),
            "hourly": "wind_speed_100m",
            "wind_speed_unit": "ms", "timezone": "Europe/Stockholm",
        }, timeout=20)
        r.raise_for_status()
        d = r.json()["hourly"]
        return {t: wcap(ws) for t, ws in zip(d["time"], d["wind_speed_100m"]) if ws is not None}
    except Exception as e:
        print(f"  WARN ERA5 ({lat},{lon}): {e}")
        return {}

# ── 3. CAPTURE RATES ──────────────────────────────────────────────────────────
def calculate_capture_rates(price_data: dict) -> dict:
    """
    Capture rate = vindvägd genomsnittspris / enkelt månadssnitt
    Visar hur mycket under spot-genomsnittet vindkraft faktiskt säljer.
    """
    today = date.today()
    start = today - timedelta(days=90)   # 3 månader bakåt
    capture = {}

    print("  Hämtar ERA5-vinddata för capture rate-beräkning…")
    for zone, (lat, lon) in ZONE_COORDS.items():
        if zone not in price_data or not price_data[zone]:
            continue

        wind_cf = fetch_archive_wind(lat, lon, start, today)
        if not wind_cf:
            continue

        # Bygg upp prisordboken: ISO-timestamp → EUR/MWh
        price_by_hour = {}
        for row in price_data[zone]:
            # Trunkera till närmaste timme i lokaltid-liknande ISO-format
            t = row["time"][:16]  # "2026-04-01T14:00"
            price_by_hour[t] = row["price"]

        # Aggregera per månad
        monthly = defaultdict(lambda: {"wind_sum": 0.0, "wind_weighted_price": 0.0,
                                       "simple_sum": 0.0, "n": 0})
        for ts_full, cf in wind_cf.items():
            ts = ts_full[:16]
            if ts not in price_by_hour:
                continue
            price = price_by_hour[ts]
            month = ts[:7]  # "2026-04"
            monthly[month]["wind_sum"]            += cf
            monthly[month]["wind_weighted_price"] += cf * price
            monthly[month]["simple_sum"]          += price
            monthly[month]["n"]                   += 1

        zone_rates = []
        for month in sorted(monthly):
            m = monthly[month]
            if m["wind_sum"] < 1 or m["n"] < 24:
                continue
            capture_price = m["wind_weighted_price"] / m["wind_sum"]
            spot_avg      = m["simple_sum"] / m["n"]
            zone_rates.append({
                "month":         month,
                "capture_price": round(capture_price, 2),
                "spot_avg":      round(spot_avg, 2),
                "capture_rate":  round(capture_price / spot_avg, 4) if spot_avg else None,
            })
        capture[zone] = zone_rates
        if zone_rates:
            last = zone_rates[-1]
            print(f"    {zone}: capture rate {last['month']} = {last['capture_rate']:.1%}")

    return capture

# ── 4. PRISPROGNOSSIGNAL (TRAFIKLJUS) ─────────────────────────────────────────
def fetch_forecast_drivers() -> dict:
    """
    Hämtar 14-dagars prognos för vind, temperatur och nederbörd.
    Jämför mot 30-dagars historisk baseline för att beräkna avvikelse.
    """
    results = {}
    today = date.today()

    for name, (lat, lon) in FORECAST_POINTS.items():
        try:
            # 14-dagars prognos
            rf = S.get(OM_FORECAST, params={
                "latitude": lat, "longitude": lon,
                "hourly": "wind_speed_100m,temperature_2m",
                "daily":  "precipitation_sum",
                "wind_speed_unit": "ms",
                "forecast_days": 14,
                "timezone": "auto",
            }, timeout=15)
            rf.raise_for_status()
            fcast = rf.json()

            # 30-dagars historisk baseline
            hist_start = (today - timedelta(days=60)).isoformat()
            hist_end   = (today - timedelta(days=1)).isoformat()
            rh = S.get(OM_ARCHIVE, params={
                "latitude": lat, "longitude": lon,
                "start_date": hist_start, "end_date": hist_end,
                "hourly": "wind_speed_100m,temperature_2m",
                "daily":  "precipitation_sum",
                "wind_speed_unit": "ms",
                "timezone": "auto",
            }, timeout=15)
            rh.raise_for_status()
            hist = rh.json()

            # Beräkna nyckeltal
            def safe_avg(lst): return sum(x for x in lst if x is not None) / max(1, sum(1 for x in lst if x is not None))

            fws   = [wcap(w) for w in fcast["hourly"]["wind_speed_100m"] if w is not None]
            hws   = [wcap(w) for w in hist["hourly"]["wind_speed_100m"]  if w is not None]
            ftemp = [t for t in fcast["hourly"]["temperature_2m"] if t is not None]
            htemp = [t for t in hist["hourly"]["temperature_2m"]  if t is not None]
            fprec = [p for p in fcast["daily"]["precipitation_sum"] if p is not None]
            hprec = [p for p in hist["daily"]["precipitation_sum"]  if p is not None]

            avg_fcf   = safe_avg(fws);   avg_hcf   = safe_avg(hws)
            avg_ftemp = safe_avg(ftemp); avg_htemp = safe_avg(htemp)
            avg_fprec = safe_avg(fprec); avg_hprec = safe_avg(hprec)

            results[name] = {
                "wind_cf_forecast":  round(avg_fcf,   3),
                "wind_cf_baseline":  round(avg_hcf,   3),
                "wind_cf_delta":     round(avg_fcf - avg_hcf, 3),
                "temp_forecast":     round(avg_ftemp, 1),
                "temp_baseline":     round(avg_htemp, 1),
                "temp_delta":        round(avg_ftemp - avg_htemp, 1),
                "precip_forecast":   round(avg_fprec, 2),
                "precip_baseline":   round(avg_hprec, 2),
                "precip_delta":      round(avg_fprec - avg_hprec, 2),
            }
        except Exception as e:
            print(f"  WARN forecast drivers {name}: {e}")

    return results

def compute_price_signals(forecast_drivers: dict, price_data: dict) -> dict:
    """
    Trafikljus per priszone baserat på:
      - Vindproduktion (hög CF → mer utbud → lägre priser)
      - Temperatur (kall → mer efterfrågan → högre priser)
      - Nederbörd (hög → mer vatten → lägre priser i NO/SE)

    Signal ur PRODUCENTPERSPEKTIV (Locus Energy):
      🟢 Grön  = priser förväntas ÖVER historisk median → bra intäkter
      🟡 Gul   = priser nära median
      🔴 Röd   = priser förväntas UNDER historisk median → pressat kassaflöde
    """
    if not forecast_drivers:
        return {}

    # Aggregera Nordic-snitt av drivare
    vals = list(forecast_drivers.values())
    avg_wind_delta  = sum(v["wind_cf_delta"]  for v in vals) / len(vals)
    avg_temp_delta  = sum(v["temp_delta"]     for v in vals) / len(vals)
    avg_prec_delta  = sum(v["precip_delta"]   for v in vals) / len(vals)

    # Prisimplikation per drivare (ur producentperspektiv, positivt = högre priser)
    # Vind: hög CF → lägre priser → negativt för producenter
    wind_score = -avg_wind_delta / 0.15   # normalisera, ±0.15 = ±1 std
    # Temp: kallare → högre priser → positivt
    temp_score =  avg_temp_delta / 3.0    # ±3°C = ±1 std
    # Nederbörd: mer → lägre priser → negativt
    prec_score = -avg_prec_delta / 2.0    # ±2 mm/dag = ±1 std

    # Klamra scores till [-1, 1]
    clamp = lambda x: max(-1.0, min(1.0, x))
    ws = clamp(wind_score)
    ts = clamp(temp_score)
    ps = clamp(prec_score)

    # Viktat snitt: vind väger tyngst i Norden
    combined = ws * 0.50 + ts * 0.30 + ps * 0.20

    if combined >  0.15: signal = "green"
    elif combined < -0.15: signal = "red"
    else:                  signal = "yellow"

    # Prisnivå vs historisk median (baserat på faktisk prisdata)
    historical_prices = []
    for zone_rows in price_data.values():
        historical_prices.extend(r["price"] for r in zone_rows)
    median_price = sorted(historical_prices)[len(historical_prices) // 2] if historical_prices else None

    # Zonsignaler: NO är mer hydropåverkat, SE mer vindpåverkat
    zone_signals = {}
    for zone in ZONE_COORDS:
        if zone not in price_data: continue
        # Lägg till zonsspecifik justering
        zone_prec_w = 0.35 if zone.startswith("NO") else 0.15
        zone_wind_w = 0.35 if zone.startswith("SE") else 0.25
        zone_temp_w = 1.0 - zone_prec_w - zone_wind_w
        z_combined = ws * zone_wind_w + ts * zone_temp_w + ps * zone_prec_w
        z_clamped  = clamp(z_combined)

        if z_clamped >  0.15: z_sig = "green"
        elif z_clamped < -0.15: z_sig = "red"
        else:                   z_sig = "yellow"

        zone_signals[zone] = {
            "signal":        z_sig,
            "score":         round(z_clamped, 3),
            "wind_score":    round(ws, 3),
            "temp_score":    round(ts, 3),
            "precip_score":  round(ps, 3),
        }

    return {
        "signal":          signal,
        "combined_score":  round(combined, 3),
        "wind_score":      round(ws, 3),
        "temp_score":      round(ts, 3),
        "precip_score":    round(ps, 3),
        "wind_cf_delta":   round(avg_wind_delta, 3),
        "temp_delta":      round(avg_temp_delta, 1),
        "precip_delta":    round(avg_prec_delta, 2),
        "median_price_eur_mwh": round(median_price, 1) if median_price else None,
        "zone_signals":    zone_signals,
        "drivers":         forecast_drivers,
    }

# ── MAIN ────────────────────────────────────────────────────────────────────────
def main():
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"=== Fetch started {now_str} ===")

    # 1. Spotpriser
    print("\n[1/3] Hämtar spotpriser…")
    price_data = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(fetch_zone_prices, z): z for z in ZONES}
        for fut in as_completed(futures):
            z = futures[fut]; d = fut.result()
            price_data[z] = d
            print(f"  {z}: {len(d)} timmar")

    # 2. Capture rates
    print("\n[2/3] Beräknar capture rates…")
    capture_rates = calculate_capture_rates(price_data)

    # 3. Prisprognosdrivare + signal
    print("\n[3/3] Beräknar prisprognossignal…")
    forecast_drivers = fetch_forecast_drivers()
    price_signal     = compute_price_signals(forecast_drivers, price_data)
    print(f"  Nordisk signal: {price_signal.get('signal','n/a')} "
          f"(score={price_signal.get('combined_score','?')})")

    # Spara allt
    os.makedirs("data", exist_ok=True)
    output = {
        "updated":       now_str,
        "zones":         price_data,
        "capture_rates": capture_rates,
        "price_signal":  price_signal,
    }
    with open("data/prices.json", "w") as f:
        json.dump(output, f, separators=(",", ":"))

    total = sum(len(v) for v in price_data.values())
    print(f"\n=== Done. {total} prisdatapunkter, {len(capture_rates)} capture rate-zoner ===")

if __name__ == "__main__":
    main()
