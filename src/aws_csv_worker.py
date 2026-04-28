import os
import csv
import sqlite3
import requests
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv, dotenv_values

# Ensure correct path resolution
sys.path.append(os.path.dirname(__file__))

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DOTENV_PATH = os.path.join(BASE_DIR, '.env')
config = dotenv_values(DOTENV_PATH)
MAPS_KEY = config.get("GOOGLE_MAPS_API_KEY")
IMAGE_DIR = os.path.join(BASE_DIR, 'training_data')
DB_PATH = os.path.join(BASE_DIR, 'leads_manifests', 'authoritative_storms.db')
CSV_PATH = os.path.join(BASE_DIR, 'leads_manifests', 'exhaustive_hail_leads.csv')

class CSVDiscoveryWorker:
    def __init__(self, max_workers=20):
        self.api_key = MAPS_KEY
        self.max_workers = max_workers
        if not self.api_key:
            logger.error("GOOGLE_MAPS_API_KEY missing. Imagery harvest disabled.")
            
    def process_pipeline(self):
        if not os.path.exists(CSV_PATH):
            logger.error(f"No CSV found at {CSV_PATH}")
            return
            
        with open(CSV_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        logger.info(f"Loaded {len(rows)} exhaustive leads. Starting High-Speed Parallel Harvest...")
        
        try:
            from yolo_detector import RoofDeepLens
            brain = RoofDeepLens()
            logger.info("YOLOv8 Brain successfully mounted.")
        except ImportError as e:
            logger.error(f"RoofDeepLens import error: {e}")
            return

        # Pre-filter existing leads to avoid redundant work
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute("SELECT street_address, zip_code FROM contacts")
        existing = set(f"{row[0]}_{row[1]}" for row in c.fetchall())
        conn.close()

        def process_single_lead(i, r):
            if f"{r['Address']}_{r['Zip']}" in existing:
                return "SKIPPED"
                
            address_str = r['Full_Address']
            magnitude = float(r.get('Hail_Magnitude', 2.0))
            event_date = r.get('Event_Date', '2026-04-25')
            zip_code = r['Zip']
            
            # 1. Geocode & Resolve Canonical Address
            lat, lon, resolved_addr = None, None, r['Address']
            try:
                g_res = requests.get("https://maps.googleapis.com/maps/api/geocode/json", 
                                     params={"address": address_str, "key": self.api_key}, timeout=10).json()
                if g_res['status'] == 'OK':
                    loc = g_res['results'][0]['geometry']['location']
                    lat, lon = loc['lat'], loc['lng']
                    # AUTO-TURN LAT/LONG INTO ADDRESS INFO
                    resolved_addr = g_res['results'][0].get('formatted_address', r['Address']).split(',')[0]
            except: pass

            # 2. Harvest Image
            zoom_level = 21 if r['State'] == 'NE' else 20
            center = f"{lat},{lon}" if lat and lon else address_str
            image_filename = f"hail_{zip_code}_{i}.png"
            image_path = os.path.join(IMAGE_DIR, image_filename)
            
            try:
                params = {"center": center, "zoom": zoom_level, "size": "640x640", "maptype": "satellite", "key": self.api_key}
                res = requests.get("https://maps.googleapis.com/maps/api/staticmap", params=params, timeout=10)
                if res.status_code == 200:
                    with open(image_path, 'wb') as img_f:
                        img_f.write(res.content)
                else:
                    return "MAPS_API_ERROR"
            except Exception as e:
                return f"HARVEST_ERROR: {e}"

            # 3. AI Scan
            analysis = brain.detect_and_quantify(image_path)
            damage_score = analysis.get('damage_percent', 0)
            qual_status = "QUALIFIED" if damage_score >= 5.0 else "PENDING"

            # 4. Save to DB (Single Threaded Entry to prevent lock during parallel scan)
            return {
                "address": resolved_addr,
                "zip": r['Zip'],
                "city": r['City'],
                "state": r['State'],
                "magnitude": magnitude,
                "date": event_date,
                "score": damage_score,
                "qual": qual_status
            }

        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_row = {executor.submit(process_single_lead, i, row): i for i, row in enumerate(rows)}
            
            for future in as_completed(future_to_row):
                res = future.result()
                if isinstance(res, dict):
                    results.append(res)
                
                if len(results) >= 50:
                    self._batch_commit(results)
                    results = []

        if results:
            self._batch_commit(results)
            
        logger.info(f"Parallel Harvest Cycle Complete.")

    def _batch_commit(self, data_list):
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        for d in data_list:
            # Get Storm Event
            c.execute("SELECT id FROM storms WHERE city=? AND state=? AND event_date=?", (d['city'], d['state'], d['date']))
            row = c.fetchone()
            if not row:
                c.execute("INSERT INTO storms (event_date, event_type, state, city, magnitude, source) VALUES (?,?,?,?,?,?)",
                          (d['date'], 'Hail', d['state'], d['city'], d['magnitude'], 'PARALLEL_HARVEST'))
                event_id = c.lastrowid
            else:
                event_id = row[0]

            c.execute('''
                INSERT INTO contacts 
                (event_id, street_address, homeowner_name, status, damage_score, proof_msg, qualification_status, zip_code)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (event_id, d['address'], "Homeowner", f"AI_DETECTOR_{d['score']}%", d['score'], f"Damage: {d['score']}%", d['qual'], d['zip']))
        conn.commit()
        conn.close()

if __name__ == '__main__':
    # Hyper-Drive Mode: 50 concurrent workers for prioritized baseline harvest
    worker = CSVDiscoveryWorker(max_workers=50)
    worker.process_pipeline()
    
    # Cascade to high-speed enrichment
    try:
        from skip_trace_qualified import skip_trace_qualified_leads
        skip_trace_qualified_leads()
    except Exception as e:
        logger.error(f"Enrichment cascade failed: {e}")
