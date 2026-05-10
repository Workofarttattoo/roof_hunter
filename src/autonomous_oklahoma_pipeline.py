import os
import sqlite3
import time
import logging
from dotenv import load_dotenv
from apollo_oklahoma_enricher import enrich_oklahoma_leads
from bland_batch_sender import send_batch
from src.weather_twin.roof_hunter_digital_twin import RoofHunterWeatherTwin
from src.weather_twin.models import ForecastState
from src.weather_twin.elevenlabs_dispatch import ElevenLabsDispatch

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'leads_manifests', 'authoritative_storms.db')

def get_dispatch_ready_leads():
    """Fetches leads that are APOLLO_ENRICHED and ready for Bland AI."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    query = """
    SELECT c.id, c.homeowner_name, c.phone_number, c.street_address, s.city, s.state, 
           s.event_date as hail_date, s.magnitude as hail_size, c.damage_score as damage_probability,
           c.structures_hit, c.image_findings
    FROM contacts c
    JOIN storms s ON c.event_id = s.id
    WHERE c.status = 'APOLLO_ENRICHED'
    AND c.phone_number IS NOT NULL
    AND c.phone_number != ''
    LIMIT 100
    """
    c.execute(query)
    rows = c.fetchall()
    conn.close()
    return rows

def mark_leads_dispatched(lead_ids):
    """Updates status to DISPATCHED to prevent double-calling."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executemany("UPDATE contacts SET status = 'DISPATCHED' WHERE id = ?", [(lid,) for lid in lead_ids])
    conn.commit()
    conn.close()

def run_pipeline_cycle():
    logger.info("=== STARTING AUTONOMOUS PIPELINE CYCLE ===")
    
    # 0. Run Atmospheric Refinement (The 4-Hour Beat)
    logger.info("Step 0: Running Atmospheric Twin to identify upcoming locks...")
    # In a real cycle, we would pull the latest HRRR/Radar data here
    # For now, we ensure the ElevenLabs Dispatcher is ready
    dispatcher = ElevenLabsDispatch()
    
    # 1. Fetch leads that are ALREADY call-ready (Have phone + High Damage)
    # This ensures we don't wait for Apollo if we already have the data
    logger.info("Step 1: Fetching existing call-ready Oklahoma leads...")
    ready_leads = get_dispatch_ready_leads()
    
    # Also fetch leads that have phone numbers but aren't marked as 'APOLLO_ENRICHED'
    # (e.g. from the original CSV ingestion)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT c.id, c.homeowner_name, c.phone_number, c.street_address, s.city, s.state, 
               s.event_date as hail_date, s.magnitude as hail_size, c.damage_score as damage_probability,
               c.structures_hit, c.image_findings
        FROM contacts c
        JOIN storms s ON c.event_id = s.id
        WHERE c.phone_number IS NOT NULL 
        AND c.phone_number != ''
        AND c.damage_score >= 40.0
        AND c.status NOT IN ('DISPATCHED', 'APOLLO_ENRICHED')
        AND s.state IN ('OK', 'OKLAHOMA')
        LIMIT 20
    """)
    extra_leads = c.fetchall()
    conn.close()
    
    all_ready = list(ready_leads) + list(extra_leads)
    
    if all_ready:
        logger.info(f"Step 2: Dispatching {len(all_ready)} leads to Bland AI...")
        dispatch_batch = []
        lead_ids = []
        for r in all_ready:
            full_name = r['homeowner_name'] or "Homeowner"
            parts = full_name.split(' ', 1)
            
            lead_dict = {
                "first_name": parts[0],
                "last_name": parts[1] if len(parts) > 1 else "",
                "phone_number": r['phone_number'],
                "property_address": r['street_address'],
                "city": r['city'],
                "state": r['state'],
                "hail_date": r['hail_date'],
                "hail_size": r['hail_size'],
                "damage_probability": r['damage_probability'],
                "structures_hit": r['structures_hit'] or "Main roof and structures",
                "image_findings": r['image_findings'] or f"Detected {r['damage_probability']}% damage probability.",
                "lead_priority": "PRIORITY_1_EMERGENCY" if float(r['damage_probability'] or 0) >= 40 else "PRIORITY_2_HIGH"
            }
            dispatch_batch.append(lead_dict)
            lead_ids.append(r['id'])

        result = send_batch(dispatch_batch)
        if result and result.get('status') != 'error':
            logger.info(f"✅ SUCCESS: {len(lead_ids)} leads dispatched.")
            mark_leads_dispatched(lead_ids)
            
            # If any lead has extremely high atmospheric probability, trigger ElevenLabs Voice Briefing
            high_conf_leads = [l for l in dispatch_batch if l['damage_probability'] >= 70]
            if high_conf_leads:
                dispatcher.trigger_hail_alert({
                    "county": high_conf_leads[0]['city'], # Fallback to city if county missing
                    "hail_probability": high_conf_leads[0]['damage_probability'] / 100.0,
                    "lead_time_min": 240,
                    "target_count": len(high_conf_leads)
                })
        else:
            logger.error(f"❌ FAILED: Bland AI dispatch failed.")
    else:
        logger.info("Step 2: No immediate call-ready leads found. Attempting enrichment...")
        # Only try enrichment if we have nothing else to do
        try:
            enrich_oklahoma_leads()
        except Exception as e:
            logger.error(f"Enrichment error: {e}")

if __name__ == "__main__":
    while True:
        try:
            run_pipeline_cycle()
            logger.info("Cycle complete. Sleeping for 5 minutes...")
            time.sleep(300) 
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            time.sleep(60)
