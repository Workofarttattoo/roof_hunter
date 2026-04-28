import sqlite3
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db')

def trigger_edmond_dispatch():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Update Edmond harvested leads to QUALIFIED to trigger dispatch
    c.execute("""
        UPDATE contacts 
        SET qualification_status = 'QUALIFIED' 
        WHERE status = 'NEW_EDMOND_HARVEST'
    """)
    rows_updated = c.rowcount
    conn.commit()
    conn.close()
    
    logger.info(f"🚀 TRIGGERED: {rows_updated} Edmond leads moved to Global Dispatch Queue.")

if __name__ == "__main__":
    trigger_edmond_dispatch()
