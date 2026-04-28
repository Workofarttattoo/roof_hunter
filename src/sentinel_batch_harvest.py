import sqlite3
import logging
import os
import ee
from src.spectral_forensic_deep_analysis import perform_deep_spectral_analysis

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db')

def batch_sentinel_harvest():
    logger.info("=== STARTING NATIONAL SENTINEL-2 FORENSIC HARVEST ===")
    
    try:
        ee.Initialize(project='roof-hunter-494309')
    except Exception as e:
        logger.error(f"GEE Initialization Failed: {e}")
        # Note: GEE might be already initialized by the analyzer, which is fine
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Pull leads that are Pending qualification and have storm metadata
    # We join with 'storms' to get the event_date, lat, lon
    query = """
    SELECT contacts.id, storms.latitude, storms.longitude, storms.event_date, storms.city, storms.state
    FROM contacts
    JOIN storms ON contacts.event_id = storms.id
    WHERE (contacts.qualification_status = 'Pending' OR contacts.qualification_status IS NULL)
    AND storms.latitude IS NOT NULL
    AND storms.longitude IS NOT NULL
    """
    c.execute(query)
    leads = c.fetchall()
    
    if not leads:
        logger.info("No leads in the spectral queue.")
        conn.close()
        return

    logger.info(f"Targeting {len(leads)} leads for multispectral validation...")

    for lid, lat, lon, event_date, city, state in leads:
        logger.info(f"📡 ANALYZING LEAD #{lid}: {city}, {state} ({lat}, {lon})")
        
        try:
            result = perform_deep_spectral_analysis(lat, lon, event_date, city)
            
            if result and result['severity'] != "INSIGNIFICANT":
                logger.info(f"✅ QUALIFIED: Lead #{lid} shows {result['severity']} (Score: {result['score']:.4f})")
                new_status = 'QUALIFIED'
            else:
                logger.info(f"❌ DISQUALIFIED: Lead #{lid} shows insufficient spectral signal.")
                new_status = 'DISQUALIFIED_SPECTRAL'
            
            # Update DB instantly
            with sqlite3.connect(DB_PATH, timeout=30) as update_conn:
                uc = update_conn.cursor()
                uc.execute("UPDATE contacts SET qualification_status = ?, status = 'SPECTRAL_VERIFIED' WHERE id = ?", (new_status, lid))
        
        except Exception as e:
            logger.error(f"Spectral analysis failed for lead #{lid}: {e}")

    conn.close()
    logger.info("=== SENTINEL HARVEST COMPLETE ===")

if __name__ == "__main__":
    batch_sentinel_harvest()
