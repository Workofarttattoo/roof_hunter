"""
noaa_forecast.py
----------------
Wraps paulokuong/noaa-sdk to pull live NOAA weather.gov grid data
for any storm site in the ECH0-ROOF platform.

Provides:
  - get_site_forecast(lat, lon)      → 7-day hourly + thunder/precip probs
  - get_active_alerts(lat, lon)      → NOAA active weather alerts (tornado, SVR)
  - enrich_storm_sites(sites)        → loop over all pinned major sites
  - score_storm_potential(grid_data) → roof-damage risk score 0-100

Usage:
  python3 src/noaa_forecast.py

Integrates with live_hail_ingest.MANUAL_SITES automatically.
"""

import os
import sys
import json
import requests
from datetime import datetime, timezone

try:
    from noaa_sdk import NOAA
except ImportError:
    print("[ERROR] noaa-sdk not installed. Run: pip install noaa-sdk")
    sys.exit(1)

# ── Pinned major storm sites (mirrored from live_hail_ingest.py) ─────────────
STORM_CORRIDOR = [
    {"label": "6 SSW Marion, KS",    "lat": 38.2800, "lon": -97.0800, "magnitude": 4.00, "state": "KS"},
    {"label": "Peabody, KS",          "lat": 38.1700, "lon": -97.1100, "magnitude": 2.25, "state": "KS"},
    {"label": "Elbing, KS",           "lat": 38.0500, "lon": -97.1300, "magnitude": 2.00, "state": "KS"},
    {"label": "7 SSW Mayfield, KS",  "lat": 37.1700, "lon": -97.5900, "magnitude": 3.50, "state": "KS"},
    {"label": "4 W Braman, OK",      "lat": 36.9300, "lon": -97.4100, "magnitude": 3.75, "state": "OK"},
    {"label": "6 W Braman, OK",      "lat": 36.9200, "lon": -97.4400, "magnitude": 3.00, "state": "OK"},
    {"label": "5 NW Deer Creek, OK", "lat": 36.8600, "lon": -97.5800, "magnitude": 2.75, "state": "OK"},
    {"label": "3 S Renfrow, OK",     "lat": 36.8818, "lon": -97.6561, "magnitude": 2.50, "state": "OK"},
    {"label": "1 E Douglas, OK",     "lat": 36.2600, "lon": -97.6500, "magnitude": 2.75, "state": "OK"},
]

NWS_ALERTS_URL = "https://api.weather.gov/alerts/active?point={lat},{lon}"

n = NOAA()

def get_grid_data(lat, lon):
    """Pull full gridpoint forecast data for a lat/lon."""
    try:
        data = n.points_forecast(lat, lon, type='forecastGridData')
        return data.get('properties', {})
    except Exception as e:
        print(f"    [grid err] ({lat},{lon}): {e}")
        return {}

def get_hourly_forecast(lat, lon):
    """Pull next 7-day hourly forecast periods."""
    try:
        data = n.points_forecast(lat, lon, type='forecastHourly')
        if isinstance(data, dict):
            return data.get('properties', {}).get('periods', [])
        return []
    except Exception as e:
        print(f"    [hourly err] ({lat},{lon}): {e}")
        return []

def get_active_alerts(lat, lon):
    """Pull active NWS alerts (tornado warnings, SVR thunderstorm, etc.)"""
    try:
        url = NWS_ALERTS_URL.format(lat=lat, lon=lon)
        r = requests.get(url, timeout=10, headers={"User-Agent": "RoofHunter/2.0"})
        if r.status_code == 200:
            features = r.json().get('features', [])
            alerts = []
            for f in features:
                p = f.get('properties', {})
                alerts.append({
                    "event":    p.get('event', ''),
                    "severity": p.get('severity', ''),
                    "headline": p.get('headline', ''),
                    "expires":  p.get('expires', ''),
                    "area":     p.get('areaDesc', ''),
                })
            return alerts
        return []
    except Exception as e:
        print(f"    [alerts err]: {e}")
        return []

def score_storm_potential(props):
    """
    Score roof-damage risk 0-100 from NOAA grid properties.
    Weights: thunder prob (30) + wind gust (30) + precip prob (20) + hazards (20)
    """
    score = 0

    # Thunder probability (%)
    thunder = props.get('probabilityOfThunder', {})
    t_vals  = thunder.get('values', []) if isinstance(thunder, dict) else []
    max_thunder = max((v.get('value', 0) or 0 for v in t_vals), default=0)
    score += min(30, (max_thunder / 100) * 30)

    # Wind gust (km/h → score)
    gust = props.get('windGust', {})
    g_vals = gust.get('values', []) if isinstance(gust, dict) else []
    max_gust = max((v.get('value', 0) or 0 for v in g_vals), default=0)
    # 100 km/h gust = full 30 pts
    score += min(30, (max_gust / 100) * 30)

    # Precip probability (%)
    precip = props.get('probabilityOfPrecipitation', {})
    p_vals = precip.get('values', []) if isinstance(precip, dict) else []
    max_precip = max((v.get('value', 0) or 0 for v in p_vals), default=0)
    score += min(20, (max_precip / 100) * 20)

    # Active hazards bonus
    hazards = props.get('hazards', {})
    h_vals  = hazards.get('values', []) if isinstance(hazards, dict) else []
    active_hazards = [v for v in h_vals if v.get('value')]
    score += min(20, len(active_hazards) * 10)

    return round(score, 1)

def get_weather_label(props):
    """Return a plain-English weather summary from grid data."""
    weather = props.get('weather', {})
    w_vals  = weather.get('values', []) if isinstance(weather, dict) else []
    labels  = []
    for wv in w_vals[:3]:
        val = wv.get('value', [])
        if isinstance(val, list):
            for item in val:
                wx = item.get('weather', '')
                cov = item.get('coverage', '')
                if wx and wx != 'None':
                    labels.append(f"{cov} {wx}".strip())
    return ', '.join(set(labels)) if labels else 'Clear'

def enrich_storm_sites(sites=None, verbose=True):
    """
    Loop over all pinned storm sites, pull NOAA grid data,
    score each, and return enriched list sorted by risk score.
    """
    if sites is None:
        sites = STORM_CORRIDOR

    enriched = []
    total = len(sites)

    for i, site in enumerate(sites):
        lat = site['lat']
        lon = site['lon']
        label = site['label']

        if verbose:
            print(f"\n[{i+1}/{total}] {label} | Hail: {site['magnitude']}\" | ({lat},{lon})")

        props   = get_grid_data(lat, lon)
        alerts  = get_active_alerts(lat, lon)
        score   = score_storm_potential(props)
        wx_label = get_weather_label(props)

        # Extract key metrics
        thunder = props.get('probabilityOfThunder', {})
        t_vals  = thunder.get('values', []) if isinstance(thunder, dict) else []
        max_thunder = max((v.get('value', 0) or 0 for v in t_vals), default=0)

        gust = props.get('windGust', {})
        g_vals = gust.get('values', []) if isinstance(gust, dict) else []
        max_gust_kmh = max((v.get('value', 0) or 0 for v in g_vals), default=0)
        max_gust_mph = round(max_gust_kmh * 0.621371, 1)

        precip = props.get('probabilityOfPrecipitation', {})
        p_vals = precip.get('values', []) if isinstance(precip, dict) else []
        max_precip = max((v.get('value', 0) or 0 for v in p_vals), default=0)

        alert_names = [a['event'] for a in alerts] if alerts else []

        result = {
            **site,
            "risk_score":      score,
            "max_thunder_pct": max_thunder,
            "max_gust_mph":    max_gust_mph,
            "max_precip_pct":  max_precip,
            "wx_summary":      wx_label,
            "active_alerts":   alert_names,
            "grid_office":     props.get('forecastOffice', '').split('/')[-1] if props else '',
            "updated":         props.get('updateTime', ''),
        }
        enriched.append(result)

        if verbose:
            alert_str = f" ⚠ {', '.join(alert_names)}" if alert_names else ""
            print(f"  Risk Score: {score}/100 | Thunder: {max_thunder}% | Gust: {max_gust_mph}mph | "
                  f"Precip: {max_precip}%{alert_str}")
            print(f"  Wx: {wx_label}")

    enriched.sort(key=lambda x: x['risk_score'], reverse=True)
    return enriched

def print_report(enriched):
    """Print ranked corridor risk report."""
    print(f"\n{'='*65}")
    print(f"  KS/OK STORM CORRIDOR — NOAA RISK RANKING")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*65}")
    print(f"  {'Rank':<4} {'Site':<25} {'Hail':>5} {'Risk':>5} {'Gust':>8} {'Thunder':>8} {'Alerts'}")
    print(f"  {'-'*60}")

    for i, site in enumerate(enriched, 1):
        alerts = '⚠ ' + ', '.join(site['active_alerts']) if site['active_alerts'] else '—'
        print(f"  #{i:<3} {site['label']:<25} {site['magnitude']}\"  "
              f"{site['risk_score']:>5}/100  {site['max_gust_mph']:>5}mph  "
              f"{site['max_thunder_pct']:>6}%  {alerts}")

    print(f"{'='*65}")

if __name__ == "__main__":
    print("=" * 65)
    print("  ECH0-ROOF NOAA FORECAST INTELLIGENCE ENGINE")
    print("  Powered by: paulokuong/noaa-sdk + api.weather.gov")
    print("=" * 65)
    print(f"\nAnalyzing {len(STORM_CORRIDOR)} pinned storm sites along KS/OK corridor...\n")

    enriched = enrich_storm_sites()
    print_report(enriched)

    # Save JSON for dashboard API consumption
    out_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'data_cache', 'corridor_forecast.json'
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump({
            "generated": datetime.now(timezone.utc).isoformat(),
            "sites": enriched
        }, f, indent=2)
    print(f"\n[✓] Forecast cache written to: {out_path}")
    print(f"[✓] Expose via API: GET /api/corridor-forecast")
