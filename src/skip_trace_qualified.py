import sqlite3
import os
import sys
import logging

# Add src to path for imports
sys.path.append(os.path.dirname(__file__))

from contact_finder import SkipTracer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'leads_manifests', 'authoritative_storms.db')

def skip_trace_qualified_leads():
    logger.info("=== AUTO-TRACING QUALIFIED ROOF DAMAGE LEADS ===")
    
    tracer = SkipTracer(mock_mode=False) # Use false to hit RentCast if API key exists
    
    conn = sqlite3.connect(DB_PATH, timeout=30)
    c = conn.cursor()
    
    # Priority: Qualified (Damage > 5%) and from Oklahoma, then others.
    # Exclude those already traced.
    query = """
    SELECT contacts.id, contacts.street_address, storms.city, storms.state, contacts.zip_code
    FROM contacts
    JOIN storms ON contacts.event_id = storms.id
    WHERE contacts.qualification_status = 'QUALIFIED'
    AND (contacts.homeowner_name = 'Homeowner' OR contacts.homeowner_name IS NULL OR contacts.homeowner_name = 'DEEP SEARCH REQ')
    ORDER BY (storms.state = 'OK') DESC, contacts.damage_score DESC
    LIMIT 100
    """
    
    c.execute(query)
    rows = c.fetchall()
    
    if not rows:
        logger.info("No untraced qualified leads found.")
        conn.close()
        return

    logger.info(f"Found {len(rows)} qualified leads targeting for skip-trace enrichment.")

    traced_count = 0
    for lead_id, address, city, state, zip_code in rows:
        full_address = f"{address}, {city}, {state} {zip_code}"
        logger.info(f"Skip-Tracing Lead #{lead_id}: {full_address}")
        
        contact_info = tracer.skip_trace_homeowner(full_address)
        
        if contact_info and contact_info.get('name'):
            owner_name = contact_info['name']
            logger.info(f"✅ FOUND OWNER: {owner_name}")
            
            c.execute("""
                UPDATE contacts 
                SET homeowner_name = ?, 
                    status = 'TRACED'
                WHERE id = ?
            """, (owner_name, lead_id))
            traced_count += 1
        else:
            logger.warning(f"❌ Skip-trace failed for {full_address}")
            c.execute("UPDATE contacts SET homeowner_name = 'DEEP SEARCH REQ' WHERE id = ?", (lead_id,))
            
        if (traced_count + 1) % 10 == 0:
            conn.commit()
            
    conn.commit()
    conn.close()
    logger.info(f"Finished. Successfully traced {traced_count} new premium leads.")

if __name__ == "__main__":
    skip_trace_qualified_leads()
