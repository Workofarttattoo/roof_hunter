"""
geocode_manifest.py
-------------------
Resolves blank/partial addresses in national_lead_manifest_2026.csv using
reverse-geocoding (Nominatim via geopy, no API key required).

Strategy:
  1. For rows where the Address field has blank street components (e.g. "365 , , Oklahoma 74084")
     → pull lat/lon from authoritative_storms.db by matching City+State+Date
     → reverse-geocode to get real street address
  2. Deduplicate rows with identical address after resolution
  3. Write national_lead_manifest_2026.csv back in-place with a backup
"""

import csv
import sqlite3
import os
import re
import time
import shutil
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MANIFEST = os.path.join(BASE, "leads_manifests", "national_lead_manifest_2026.csv")
DB_PATH  = os.path.join(BASE, "leads_manifests", "authoritative_storms.db")
BACKUP   = MANIFEST.replace(".csv", "_backup.csv")

geolocator = Nominatim(user_agent="roof_hunter_geocoder_v1", timeout=10)

def is_blank_address(addr: str) -> bool:
    """Returns True if the address has a missing street segment (e.g. '365 , , Oklahoma 74084')."""
    if not addr:
        return True
    # Pattern: number followed immediately by ' , ,'  — street name is empty
    if re.search(r'\d+\s*,\s*,', addr):
        return True
    # Or the whole address is just city/state/zip with no street
    parts = [p.strip() for p in addr.split(',')]
    if len(parts) >= 2 and (not parts[0] or parts[0].isdigit()):
        return True
    return False

def reverse_geocode(lat: float, lon: float) -> str | None:
    """Reverse geocode lat/lon → human-readable street address."""
    try:
        location = geolocator.reverse((lat, lon), language='en', exactly_one=True)
        if location and location.address:
            return location.address
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"  [WARN] Geocoder error for ({lat}, {lon}): {e}")
    return None

def get_coords_for_row(conn, city: str, state: str, date: str):
    """Look up lat/lon in the DB by matching city + state (best available match)."""
    c = conn.cursor()
    # Try exact city + state + date match first
    c.execute("""
        SELECT latitude, longitude FROM storms
        WHERE UPPER(city) = UPPER(?) AND UPPER(state) = UPPER(?) AND event_date = ?
        LIMIT 1
    """, (city, state, date))
    row = c.fetchone()
    if row:
        return row[0], row[1]

    # Fallback: city + state only
    c.execute("""
        SELECT latitude, longitude FROM storms
        WHERE UPPER(city) = UPPER(?) AND UPPER(state) = UPPER(?)
        LIMIT 1
    """, (city, state))
    row = c.fetchone()
    if row:
        return row[0], row[1]

    # Fallback: state only
    c.execute("""
        SELECT latitude, longitude FROM storms
        WHERE UPPER(state) = UPPER(?) AND event_date = ?
        LIMIT 1
    """, (state, date))
    row = c.fetchone()
    if row:
        return row[0], row[1]

    return None, None

def run():
    print(f"[INFO] Reading manifest: {MANIFEST}")
    shutil.copy(MANIFEST, BACKUP)
    print(f"[INFO] Backup saved to: {BACKUP}")

    conn = sqlite3.connect(DB_PATH)

    with open(MANIFEST, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"[INFO] Total rows: {len(rows)}")

    resolved = 0
    skipped  = 0
    failed   = 0

    for i, row in enumerate(rows):
        addr = row.get("Address", "").strip()
        city  = row.get("City", "").strip()
        state = row.get("State", "").strip()
        date  = row.get("Date", "").strip()

        if not is_blank_address(addr):
            skipped += 1
            continue  # address looks fine

        print(f"  [{i+1}/{len(rows)}] Resolving: {city}, {state} | Current: '{addr[:60]}'")

        lat, lon = get_coords_for_row(conn, city, state, date)
        if lat is None:
            print(f"    [SKIP] No coords in DB for {city}, {state}")
            failed += 1
            continue

        real_addr = reverse_geocode(lat, lon)
        time.sleep(1.1)  # Nominatim rate limit: 1 req/sec

        if real_addr:
            row["Address"] = real_addr
            resolved += 1
            print(f"    [✓] → {real_addr}")
        else:
            failed += 1
            print(f"    [✗] Geocode failed for ({lat:.4f}, {lon:.4f})")

    conn.close()

    # Remove exact duplicate rows (same Address + State + Date)
    seen = set()
    deduped = []
    for row in rows:
        key = (row.get("State"), row.get("Date"), row.get("City"), row.get("Address"))
        if key not in seen:
            seen.add(key)
            deduped.append(row)

    removed_dupes = len(rows) - len(deduped)

    # Write back
    with open(MANIFEST, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduped)

    print(f"\n[DONE] Resolved: {resolved} | Skipped (OK): {skipped} | Failed: {failed} | Dupes removed: {removed_dupes}")
    print(f"[DONE] Final row count: {len(deduped)} (was {len(rows)})")
    print(f"[DONE] Written to: {MANIFEST}")

if __name__ == "__main__":
    run()
