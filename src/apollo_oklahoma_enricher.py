import os
import sqlite3
import time
import requests
import json
import logging
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db')
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")

def enrich_oklahoma_leads():
    """
    Enriches Oklahoma leads using Apollo.io restful API.
    Specifically targets leads that are QUALIFIED but need phone/email.
    """
    if not APOLLO_API_KEY:
        logger.error("🛑 APOLLO_API_KEY not found in .env. Please add it to start enrichment.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 1. Fetch Oklahoma leads needing enrichment
    query = """
    SELECT c.id, c.homeowner_name, c.street_address, s.city, s.state, c.zip_code
    FROM contacts c
    JOIN storms s ON c.event_id = s.id
    WHERE (s.state = 'OK' OR s.state = 'OKLAHOMA')
    AND c.qualification_status = 'QUALIFIED'
    AND (c.phone_number IS NULL OR c.phone_number LIKE '%555%' OR c.phone_number = 'UNVERIFIED')
    """
    c.execute(query)
    leads = c.fetchall()
    
    if not leads:
        logger.info("✅ No Oklahoma leads require enrichment at this time.")
        conn.close()
        return

    logger.info(f"🚀 Found {len(leads)} Oklahoma leads for Apollo enrichment.")

    for lid, name, addr, city, state, zip_c in leads:
        name_parts = name.split(' ') if name else ["Homeowner"]
        first = name_parts[0]
        last = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
        
        payload = {
            "first_name": first,
            "last_name": last,
            "street_address": addr,
            "city": city,
            "state": state,
            "zip_code": zip_c
        }

        logger.info(f"📡 Matching Lead #{lid}: {name} at {addr}...")
        
        try:
            # Switch to 'match' for Free Tier compatibility
            response = requests.post(
                "https://api.apollo.io/v1/people/match",
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "no-cache",
                    "X-Api-Key": APOLLO_API_KEY
                },
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                person = response.json().get('person', {})
                phone = person.get('sanitized_phone') or person.get('phone_numbers', [{}])[0].get('sanitized_number') if person.get('phone_numbers') else None
                email = person.get('email')
                
                if phone or email:
                    c.execute("""
                        UPDATE contacts 
                        SET phone_number = ?, email = ?, status = 'APOLLO_ENRICHED' 
                        WHERE id = ?
                    """, (phone, email, lid))
                    logger.info(f"   ✅ SUCCESS: Match found for #{lid}")
                else:
                    logger.warning(f"   ⚠️ No contact found for #{lid}")
                    
            elif response.status_code == 429:
                logger.warning("🕒 Apollo Rate Limit hit. Pausing for 5 seconds...")
                time.sleep(5)
            else:
                logger.error(f"❌ Apollo API Error ({response.status_code}): {response.text}")
                # Log error and continue to next lead
                continue

        except Exception as e:
            logger.error(f"💥 Critical Failure for lead #{lid}: {e}")
            continue

        # Commit every 10 leads to ensure progress is saved
        if lid % 10 == 0:
            conn.commit()
            time.sleep(0.5) # Graceful delay for free tier

    conn.commit()
    conn.close()
    logger.info("🏁 Oklahoma Enrichment Cycle Complete.")

if __name__ == "__main__":
    enrich_oklahoma_leads()
