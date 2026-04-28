import time
import logging
import os
import sqlite3
import subprocess
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, 'leads_manifests', 'authoritative_storms.db')

def storm_chaser_loop():
    logger.info("🌪️ STORM CHASER DAEMON ACTIVATED")
    logger.info("Monitoring for Hail >= 1.75\" and Tornado activity...")
    
    while True:
        try:
            # 1. Trigger Live Ingest (SPC/NOAA)
            logger.info("📡 Fetching latest SPC reports...")
            # We run the existing ingest script with a 1.75" threshold
            subprocess.run(["venv/bin/python", "src/live_hail_ingest.py", "--days", "1", "--min", "1.75"], cwd=BASE)
            
            # 2. Identify Epicenters (Zip codes with latest activity)
            identify_epicenters()
            
            # 3. Geocode and Populate Lead List
            populate_forensic_leads()
            
            logger.info("💤 Sleeping for 15 minutes until next sweep...")
            time.sleep(900)
            
        except Exception as e:
            logger.error(f"Storm Chaser Error: {e}")
            time.sleep(60)

def identify_epicenters():
    """Find zip codes with high density of recent (>1.75\") hits."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Simple logic: Count hits in the last 24h grouped by city/state/zip
    query = """
    SELECT city, state, zipcode, COUNT(*) as hits
    FROM storms
    WHERE magnitude >= 1.75
    AND ingest_time >= datetime('now', '-24 hours')
    GROUP BY city, state, zipcode
    ORDER BY hits DESC
    LIMIT 10
    """
    c.execute(query)
    epicenters = c.fetchall()
    
    for city, state, zip_c, hits in epicenters:
        logger.info(f"📍 EPICENTER DETECTED: {city}, {state} ({zip_c}) - {hits} massive hits")
    
    conn.close()

def populate_forensic_leads():
    """Convert storm coordinates into forensic lead addresses."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get storms from the last 2 hours that haven't been geocoded into contacts
    # Optimized: We target the highest magnitude storms first
    query = """
    SELECT id, latitude, longitude, city, state, magnitude, event_date
    FROM storms
    WHERE magnitude >= 1.75
    AND ingest_time >= datetime('now', '-2 hours')
    AND id NOT IN (SELECT event_id FROM contacts)
    ORDER BY magnitude DESC
    """
    c.execute(query)
    new_storms = c.fetchall()
    
    if not new_storms:
        return

    logger.info(f"🏗️ Generating forensic addresses for {len(new_storms)} new storm hits...")
    
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="roof_hunter_chaser_v1")
    
    for sid, lat, lon, city, state, mag, date in new_storms:
        try:
            # We perform a small radial search or just a direct reverse geocode for the 'Ground Zero' address
            location = geolocator.reverse((lat, lon), timeout=10)
            if location and location.raw.get('address'):
                addr = location.address
                zip_c = location.raw['address'].get('postcode', '')
                
                # Ingest into contacts as 'Pending' qualification
                c.execute("INSERT INTO contacts (event_id, homeowner_name, street_address, zip_code, qualification_status, status) VALUES (?, ?, ?, ?, 'Pending', 'Pending')",
                         (sid, "Homeowner", addr, zip_c))
                logger.info(f"💎 New Forensic Lead: {addr} (Storm ID: {sid})")
        except:
            pass
        time.sleep(1) # Rate limit for Nominatim
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    storm_chaser_loop()
