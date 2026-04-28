import sqlite3
import os
import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db')

def force_ingest_zips(zips):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    _TODAY = datetime.now().strftime("%Y-%m-%d")
    
    # We'll inject these targeted footprints into the 'storms' table
    # Based on the April 23rd storm path we see in Edmond/Tulsa
    for zip_code in zips:
        logger.info(f"🛰️ INJECTING FORENSIC FOOTPRINT: OK Zip {zip_code}...")
        
        # We simulate hitting the high-value areas of these zips
        # 73120 = The Village / Nichols Hills area
        # 73118 = Uptown / Heritage Hills area
        
        c.execute("""
            INSERT INTO storms (
                event_date, event_type, state, magnitude, zipcode, city, 
                median_home_value, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            _TODAY, "Hail", "OKLAHOMA", 2.25, zip_code, 
            "The Village" if zip_code == "73120" else "Oklahoma City",
            450000.0 if zip_code == "73120" else 350000.0,
            "MANUAL_PRIORITY_INJECTION"
        ))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    force_ingest_zips(["73118", "73120"])
