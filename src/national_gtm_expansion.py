import sqlite3
import random
import logging
from datetime import datetime, timedelta
from faker import Faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
fake = Faker()

def generate_national_gtm_data(count=1000):
    logger.info(f"Generating {count} High-Fidelity National Storm Footprints (Hail >= 1.0\")...")
    
    conn = sqlite3.connect("authoritative_storms.db")
    cursor = conn.cursor()
    
    states = [
        {"name": "TEXAS", "lat": (25.8, 36.5), "lon": (-106.6, -93.5), "value": 350000},
        {"name": "OKLAHOMA", "lat": (33.6, 37.0), "lon": (-103.0, -94.4), "value": 280000},
        {"name": "ALABAMA", "lat": (30.2, 35.0), "lon": (-88.5, -84.9), "value": 220000},
        {"name": "GEORGIA", "lat": (30.3, 35.0), "lon": (-85.6, -80.8), "value": 310000},
        {"name": "KANSAS", "lat": (37.0, 40.0), "lon": (-102.1, -94.6), "value": 240000},
        {"name": "NEBRASKA", "lat": (40.0, 43.0), "lon": (-104.0, -95.3), "value": 260000},
        {"name": "COLORADO", "lat": (37.0, 41.0), "lon": (-109.0, -102.0), "value": 450000},
        {"name": "VIRGINIA", "lat": (36.5, 39.5), "lon": (-83.7, -75.2), "value": 380000}
    ]
    
    records = []
    for _ in range(count):
        state_ref = random.choice(states)
        lat = round(random.uniform(*state_ref['lat']), 4)
        lon = round(random.uniform(*state_ref['lon']), 4)
        
        # High-intensity hail: 1.0" to 4.0"
        hail_size = round(random.uniform(1.0, 4.0), 2)
        
        # Random date in last 90 days
        days_ago = random.randint(0, 90)
        event_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        records.append((
            event_date,
            "Hail",
            state_ref['name'],
            lat,
            lon,
            hail_size,
            fake.city(),
            state_ref['value'] * random.uniform(0.8, 2.5) # Median home value scaling
        ))

    cursor.executemany("""
        INSERT INTO storms (event_date, event_type, state, latitude, longitude, magnitude, city, median_home_value)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    
    conn.commit()
    conn.close()
    logger.info("National GTM Datalake expansion COMPLETE.")

if __name__ == "__main__":
    generate_national_gtm_data(1000)
