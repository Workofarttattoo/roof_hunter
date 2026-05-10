"""
storm_tracker.py
----------------
Real-time and historical storm tracking for the Roof Hunter pipeline.

Data sources (all free, no API key required):
  1. NOAA NCEI Storm Events Database (historical) — CSV archives
  2. NOAA SPC Local Storm Reports (live) — today/yesterday hail reports
  3. NOAA MRMS MESH (live radar) — via mrms_mesh_ingest module
  4. NWS Active Alerts (live) — via nws_hail_alerts module
  5. NOAA SWDI Nexrad hail (live) — direct API

Provides:
  - check_for_hail()        → search historical NCEI data
  - check_live_storms()     → poll SPC + NWS for active hail events
  - get_recent_hail_events()→ combined historical + live for a region
  - monitor_loop()          → continuous monitoring daemon
"""

import logging
import time
import re
import os
import io
import csv
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, 'leads_manifests', 'authoritative_storms.db')

# SPC (Storm Prediction Center) Live Reports
SPC_TODAY_HAIL = "https://www.spc.noaa.gov/climo/reports/today_filtered_hail.csv"
SPC_YESTERDAY_HAIL = "https://www.spc.noaa.gov/climo/reports/yesterday_filtered_hail.csv"

# NWS Active Alerts
NWS_ALERTS_URL = "https://api.weather.gov/alerts/active?event=Severe%20Thunderstorm%20Warning"


class StormTracker:
    """
    Multi-source storm tracker that combines historical archives with live feeds.
    """

    def __init__(self, target_states=None):
        self.cache_dir = os.path.join(BASE, 'data_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        # Default target states for hail alley
        self.target_states = target_states or [
            "OKLAHOMA", "TEXAS", "KANSAS", "NEBRASKA", "COLORADO",
            "ARKANSAS", "MISSOURI", "IOWA", "ALABAMA", "MISSISSIPPI",
            "GEORGIA", "TENNESSEE", "SOUTH CAROLINA", "NORTH CAROLINA"
        ]

    # ─── HISTORICAL: NOAA NCEI Storm Events ─────────────────────────────

    def download_ncei_data(self, year=None):
        """Download NCEI Storm Events CSV for a given year."""
        if year is None:
            year = datetime.now().year

        cached_file = os.path.join(self.cache_dir, f"storm_events_{year}.csv.gz")
        
        # Check for existing cached files (might have different timestamps)
        for f in os.listdir(self.cache_dir):
            if f.startswith(f"StormEvents_details-ftp_v1.0_d{year}") and f.endswith(".csv.gz"):
                logger.info(f"Using pre-cached NCEI file: {f}")
                return os.path.join(self.cache_dir, f)

        index_url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
        try:
            res = requests.get(index_url, timeout=30)
            match = re.search(rf'href="(StormEvents_details-ftp_v1.0_d{year}_c\d+\.csv\.gz)"', res.text)
            if not match:
                logger.warning(f"No NCEI file found for year {year}")
                return None
            filename = match.group(1)
            download_url = index_url + filename

            logger.info(f"Downloading NCEI dataset: {filename}...")
            file_res = requests.get(download_url, timeout=120)
            save_path = os.path.join(self.cache_dir, filename)
            with open(save_path, 'wb') as f:
                f.write(file_res.content)
            logger.info(f"Downloaded to {save_path}")
            return save_path
        except Exception as e:
            logger.error(f"NCEI download failed: {e}")
            return None

    def check_for_hail(self, qualifier_inches=1.5, location="Oklahoma County, OK",
                       years=None, max_events=50):
        """
        Search NCEI historical data for qualifying hail events.
        Returns the most severe event matching criteria, or None.
        
        Now searches current year + previous year by default.
        """
        if years is None:
            current_year = datetime.now().year
            years = [current_year, current_year - 1]

        # Extract state from location string
        state_filter = None
        if location:
            parts = location.upper().replace(",", "").split()
            state_map = {
                "OK": "OKLAHOMA", "TX": "TEXAS", "KS": "KANSAS",
                "NE": "NEBRASKA", "CO": "COLORADO", "AR": "ARKANSAS",
                "MO": "MISSOURI", "IA": "IOWA", "AL": "ALABAMA",
                "MS": "MISSISSIPPI", "GA": "GEORGIA", "TN": "TENNESSEE"
            }
            for part in parts:
                if part in state_map:
                    state_filter = state_map[part]
                    break
                if part in state_map.values():
                    state_filter = part
                    break

        all_events = []

        for year in years:
            # Try to find cached file
            csv_path = None
            for f in os.listdir(self.cache_dir):
                if f"d{year}" in f and f.endswith(".csv.gz"):
                    csv_path = os.path.join(self.cache_dir, f)
                    break

            if not csv_path:
                csv_path = self.download_ncei_data(year)

            if not csv_path:
                continue

            try:
                df = pd.read_csv(csv_path, compression='gzip', low_memory=False)

                hail_df = df[
                    (df['EVENT_TYPE'] == 'Hail') &
                    (df['MAGNITUDE'] >= qualifier_inches)
                ]

                if state_filter:
                    hail_df = hail_df[hail_df['STATE'] == state_filter]

                if hail_df.empty:
                    continue

                # Convert to event dicts
                for _, row in hail_df.nlargest(max_events, 'MAGNITUDE').iterrows():
                    lat = row.get('BEGIN_LAT')
                    lon = row.get('BEGIN_LON')
                    if pd.isna(lat) or pd.isna(lon):
                        continue

                    date_str = str(row.get('BEGIN_YEARMONTH', ''))
                    day_str = str(row.get('BEGIN_DAY', '01')).zfill(2)
                    if date_str and date_str != 'nan' and len(date_str) >= 6:
                        event_date = f"{date_str[:4]}-{date_str[4:6]}-{day_str}"
                    else:
                        event_date = f"{year}-01-01"

                    all_events.append({
                        "date": event_date,
                        "hail_size_inches": float(row['MAGNITUDE']),
                        "latitude": float(lat),
                        "longitude": float(lon),
                        "state": row.get('STATE', ''),
                        "city": row.get('BEGIN_LOCATION', ''),
                        "county": row.get('CZ_NAME', ''),
                        "source": "ncei",
                        "impact_polygon": [
                            float(lon) - 0.02, float(lat) - 0.02,
                            float(lon) + 0.02, float(lat) + 0.02
                        ]
                    })
            except Exception as e:
                logger.error(f"NCEI parse error for year {year}: {e}")

        if not all_events:
            logger.info(f"No qualifying hail events found (>= {qualifier_inches}\" in {location})")
            return None

        # Return the most severe event
        all_events.sort(key=lambda e: e['hail_size_inches'], reverse=True)
        best = all_events[0]
        logger.info(f"Top event: {best['date']} - {best['hail_size_inches']}\" hail at "
                     f"{best.get('city', '?')}, {best.get('state', '?')}")
        return best

    # ─── LIVE: SPC Local Storm Reports ──────────────────────────────────

    def check_live_spc(self, min_hail_inches=1.5):
        """
        Pull today's and yesterday's SPC hail reports.
        Returns list of qualifying events.
        """
        events = []
        
        for url, label in [(SPC_TODAY_HAIL, "today"), (SPC_YESTERDAY_HAIL, "yesterday")]:
            try:
                res = requests.get(url, timeout=15)
                if res.status_code != 200:
                    logger.warning(f"SPC {label} returned {res.status_code}")
                    continue

                reader = csv.DictReader(io.StringIO(res.text))
                for row in reader:
                    try:
                        size = float(row.get('Size', '0') or '0') / 100.0  # SPC reports in hundredths of inch
                        if size < min_hail_inches:
                            continue

                        lat = float(row.get('Lat', '0') or '0')
                        lon = float(row.get('Lon', '0') or '0')
                        if lat == 0 or lon == 0:
                            continue
                        # SPC longitude is positive but should be negative for western hemisphere
                        if lon > 0:
                            lon = -lon

                        events.append({
                            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                            "hail_size_inches": size,
                            "latitude": lat,
                            "longitude": lon,
                            "state": row.get('St', ''),
                            "city": row.get('Location', ''),
                            "county": row.get('County', ''),
                            "source": f"spc_{label}",
                            "impact_polygon": [
                                lon - 0.02, lat - 0.02,
                                lon + 0.02, lat + 0.02
                            ]
                        })
                    except (ValueError, TypeError):
                        continue

                logger.info(f"SPC {label}: Found {len(events)} qualifying hail reports (>= {min_hail_inches}\")")
            except Exception as e:
                logger.error(f"SPC fetch error ({label}): {e}")

        return events

    # ─── LIVE: NWS Active Alerts ────────────────────────────────────────

    def check_nws_alerts(self):
        """
        Pull active NWS severe thunderstorm warnings that mention hail.
        Returns list of alert summaries.
        """
        try:
            headers = {"User-Agent": "RoofHunter/2.0 (storm-intel)"}
            res = requests.get(
                "https://api.weather.gov/alerts/active",
                headers=headers,
                timeout=15
            )
            if res.status_code != 200:
                return []

            features = res.json().get("features", [])
            hail_alerts = []

            for f in features:
                props = f.get("properties", {})
                text = " ".join([
                    str(props.get("event", "")),
                    str(props.get("headline", "")),
                    str(props.get("description", ""))
                ]).lower()

                if "hail" not in text:
                    continue

                # Extract hail size from description if mentioned
                import re as _re
                size_match = _re.search(r'(\d+\.?\d*)\s*inch\s*hail', text)
                hail_size = float(size_match.group(1)) if size_match else 1.0

                hail_alerts.append({
                    "event": props.get("event", ""),
                    "headline": props.get("headline", ""),
                    "area": props.get("areaDesc", ""),
                    "severity": props.get("severity", ""),
                    "hail_size_estimate": hail_size,
                    "sent": props.get("sent", ""),
                    "expires": props.get("expires", ""),
                    "source": "nws_alert"
                })

            logger.info(f"NWS: {len(hail_alerts)} active hail-related alerts")
            return hail_alerts
        except Exception as e:
            logger.error(f"NWS alerts error: {e}")
            return []

    # ─── COMBINED: Get all recent events ────────────────────────────────

    def get_recent_hail_events(self, min_hail_inches=1.5, location=None):
        """
        Combined search across all sources: live SPC + NWS alerts + NCEI history.
        Returns events sorted by severity (largest hail first).
        """
        all_events = []

        # 1. Live SPC reports
        spc_events = self.check_live_spc(min_hail_inches=min_hail_inches)
        all_events.extend(spc_events)

        # 2. NWS active alerts (informational)
        alerts = self.check_nws_alerts()
        logger.info(f"Active NWS hail alerts: {len(alerts)}")

        # 3. Historical NCEI
        historical = self.check_for_hail(
            qualifier_inches=min_hail_inches,
            location=location
        )
        if historical:
            all_events.append(historical)

        # Sort by hail size descending
        all_events.sort(key=lambda e: e.get('hail_size_inches', 0), reverse=True)
        return all_events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    tracker = StormTracker()

    print("\n=== LIVE SPC REPORTS ===")
    spc = tracker.check_live_spc(min_hail_inches=1.5)
    for e in spc[:5]:
        print(f"  🌨️ {e['hail_size_inches']}\" at {e['city']}, {e['state']} ({e['source']})")

    print("\n=== NWS ACTIVE ALERTS ===")
    alerts = tracker.check_nws_alerts()
    for a in alerts[:5]:
        print(f"  ⚠️ {a['headline']}")

    print("\n=== HISTORICAL (OK) ===")
    event = tracker.check_for_hail(qualifier_inches=2.0, location="Oklahoma")
    if event:
        print(f"  📊 Top: {event['hail_size_inches']}\" on {event['date']} at {event.get('city', '?')}")
