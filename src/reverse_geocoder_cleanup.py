import pandas as pd
import logging
import time
import os
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def reverse_geocode_manifest():
    logger.info("=== INITIATING FREE REVERSE GEOCODE (OPENSTREETMAP) ===")
    
    master_path = 'leads_manifests/ROOF_HUNTER_MASTER_MANIFEST_2026.csv'
    
    if not os.path.exists(master_path):
        logger.error(f"{master_path} not found.")
        return

    df = pd.read_csv(master_path)
    
    # Setup Nominatim (Free/Rate-Limited)
    geolocator = Nominatim(user_agent="RoofHunter_Forensic_Agent_v1")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1.1)

    logger.info(f"Targeting {len(df)} coordinates for street-level resolution...")

    for index, row in df.iterrows():
        # Check if we already have a specific address (if not generic)
        current_addr = str(row.get('Address', ''))
        if ',' in current_addr and len(current_addr.split(',')) <= 2:
            try:
                lat, lon = row['latitude'], row['longitude']
                logger.info(f"[{index+1}/{len(df)}] resolving {lat}, {lon}...")
                
                location = reverse((lat, lon), language='en')
                if location:
                    df.at[index, 'Address'] = location.address
                    logger.info(f"Resolved: {location.address[:50]}...")
                
                # Checkpoint every 10 to protect progress
                if (index + 1) % 10 == 0:
                    df.to_csv(master_path, index=False)
                    
            except Exception as e:
                logger.warning(f"Geocode failed for index {index}: {e}")
                time.sleep(2) # Backoff

    df.to_csv(master_path, index=False)
    logger.info("Reverse Geocoding COMPLETE. Manifest updated.")

if __name__ == "__main__":
    reverse_geocode_manifest()
