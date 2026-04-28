"""
aws_discovery_worker.py
-----------------------
AWS-Optimized Lead Discovery Worker.
Functions:
  1. Identifies targets from Authoritative DB (Hail >= 1.5\", last 30 days).
  2. Harvests sub-meter resolution satellite imagery via Google Maps Static API.
  3. Runs YOLOv8 Deep-Inspection for spectral damage markers.
  4. Updates Lead Manifest with 'Forensic Damage Score'.

Deployment: Designed for EC2 us-east-2.
"""

import os
import sqlite3
import requests
import logging
from dotenv import load_dotenv, dotenv_values
from cloud_notifier import send_lead_dispatch
from insurance_enricher import enrich_leads_with_insurance

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Dynamic Path Resolution
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DOTENV_PATH = os.path.join(BASE_DIR, '.env')
config = dotenv_values(DOTENV_PATH)
MAPS_KEY = config.get("GOOGLE_MAPS_API_KEY")
DB_PATH = os.path.join(BASE_DIR, 'leads_manifests', 'authoritative_storms.db')
IMAGE_DIR = os.path.join(BASE_DIR, 'training_data')

# Ensure image directory exists
os.makedirs(IMAGE_DIR, exist_ok=True)

class AWSDiscoveryWorker:
    def __init__(self):
        self.api_key = MAPS_KEY
        if not self.api_key:
            logger.error("GOOGLE_MAPS_API_KEY missing. Imagery harvest disabled.")
        
    def get_discovery_targets(self, min_hail=1.5, days=30):
        """Query DB for leads hit by significant hail recently."""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # PRIORITY: Oklahoma catastrophic wind swath (Vance AFB area)
        # Sort by Magnitude DESC and prioritize Oklahoma states
        query = """
        SELECT DISTINCT c.street_address, s.id, s.latitude, s.longitude, s.city, s.state, s.magnitude
        FROM storms s
        JOIN contacts c ON s.id = c.event_id
        WHERE s.magnitude >= ? 
        AND s.event_date >= date('now', ?)
        ORDER BY (CASE WHEN s.state = 'OKLAHOMA' THEN 0 ELSE 1 END), s.magnitude DESC
        """
        params = (min_hail, f'-{days} days')
        c.execute(query, params)
        targets = c.fetchall()
        conn.close()
        return targets

    def harvest_imagery(self, lat, lon, target_id):
        """Fetch 640x640 Satellite view at Max Zoom (z20)."""
        if not self.api_key: return None
        
        filename = f"hail_target_{target_id}.png"
        save_path = os.path.join(IMAGE_DIR, filename)
        
        # Google Static Maps API
        url = "https://maps.googleapis.com/maps/api/staticmap"
        params = {
            "center": f"{lat},{lon}",
            "zoom": 20,
            "size": "640x640",
            "maptype": "satellite",
            "key": self.api_key
        }
        
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                with open(save_path, 'wb') as f:
                    f.write(r.content)
                return save_path
            else:
                logger.warning(f"Map API Error {r.status_code} for target {target_id}")
        except Exception as e:
            logger.error(f"Imagery harvest failed for {target_id}: {e}")
        return None

    def process_pipeline(self):
        """Main loop: Discovery -> Harvest -> Assessment."""
        targets = self.get_discovery_targets()
        logger.info(f"Targeting {len(targets)} leads for forensic discovery.")
        
        try:
            from yolo_detector import RoofDeepLens
            brain = RoofDeepLens()
        except ImportError:
            logger.error("RoofDeepLens (yolo_detector.py) not found in src/")
            return

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        results_found = 0
        for t in targets:
            logger.info(f"Scanning target #{t['id']} | {t['city']}, {t['state']} ({t['magnitude']}\")")
            
            # 1. Harvest Imagery
            image_path = self.harvest_imagery(t['latitude'], t['longitude'], t['id'])
            
            if image_path:
                # 2. Run AI Inference
                analysis = brain.detect_and_quantify(image_path)
                damage_score = analysis.get('damage_percent', 0)
                
                # 3. Update DB with Damage Metrics
                c.execute('''
                    UPDATE contacts 
                    SET status = ?, 
                        proof_msg = ?,
                        damage_score = ?
                    WHERE event_id = ?
                ''', (
                    f"AI_SCANNED_{damage_score}%", 
                    f"Damage: {damage_score}% | Hail: {t['magnitude']}\"",
                    damage_score,
                    t['id']
                ))
                results_found += 1
                logger.info(f"  --> Result: {damage_score}% Damage Detected. DB Updated.")

        conn.commit()
        conn.close()
        logger.info(f"AWS Discovery Pipeline Complete. Processed {results_found} properties.")
        
        # Trigger Insurance Enrichment
        logger.info("Enriching leads with Insurance Carrier data...")
        enrich_leads_with_insurance()

        # Trigger Auto-Dispatch to Inventor
        logger.info("Triggering Lead Dispatch to inventor@aios.is...")
        send_lead_dispatch()

if __name__ == "__main__":
    worker = AWSDiscoveryWorker()
    worker.process_pipeline()
