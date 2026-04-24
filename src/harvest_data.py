import os
import sqlite3
import shutil
import logging
from src.imagery_fetcher import ImageryFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'authoritative_storms.db')
TRAINING_DIR = os.path.join(os.path.dirname(__file__), '..', 'training_data')

def harvest_training_data():
    os.makedirs(TRAINING_DIR, exist_ok=True)
    fetcher = ImageryFetcher()
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Grab the top 50 highest magnitude events (or whatever limit we want)
    c.execute("SELECT * FROM storms ORDER BY magnitude DESC LIMIT 50")
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        logger.error("No storms found in the datalake to harvest.")
        return
        
    logger.info(f"Harvesting training imagery for {len(rows)} storm impact zones...")
    
    for row in rows:
        storm_id = row['id']
        lat = row['latitude']
        lon = row['longitude']
        date = row['event_date']
        
        # We need an address mockup for the fetcher
        addr_info = {
            "property_id": f"storm_{storm_id}_{lat}_{lon}",
            "address": f"{lat},{lon}" # Google Maps Static API accepts lat,lon implicitly
        }
        
        logger.info(f"Fetching imagery for Storm ID {storm_id} at {lat},{lon} around {date}...")
        img_res = fetcher.fetch_historical_imagery(addr_info, date)
        
        if img_res["status"] == "success":
            img_before = img_res["image_path_before"]
            img_after = img_res["image_path_after"]
            
            b_name = f"training_{storm_id}_before.jpg"
            a_name = f"training_{storm_id}_after.jpg"
            
            if os.path.exists(img_before):
                shutil.copy(img_before, os.path.join(TRAINING_DIR, b_name))
            if os.path.exists(img_after):
                shutil.copy(img_after, os.path.join(TRAINING_DIR, a_name))
                
            logger.info(f"✅ Harvested images for Storm {storm_id}")
        else:
            logger.warning(f"❌ Failed to fetch imagery for Storm {storm_id}")

if __name__ == "__main__":
    harvest_training_data()
