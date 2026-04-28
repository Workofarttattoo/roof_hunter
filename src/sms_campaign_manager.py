import sqlite3
import os
import json
import requests
import logging
import time
from dotenv import load_dotenv, dotenv_values

# Load environment
dotenv_path = '/Users/noone/.gemini/antigravity/scratch/roof_hunter/.env'
load_dotenv(dotenv_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = '/Users/noone/.gemini/antigravity/scratch/roof_hunter/leads_manifests/authoritative_storms.db'

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def send_sms_via_telnyx(to_phone, message):
    """
    Dispatches a forensics-themed SMS via the Telnyx Messaging API.
    """
    env = dotenv_values(dotenv_path)
    api_key = env.get("TELNYX_API_KEY")
    from_phone = env.get("TELNYX_PHONE_NUMBER")
    
    url = "https://api.telnyx.com/v2/messages"
    payload = {
        "from": from_phone,
        "to": to_phone,
        "text": message
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        r = requests.post(url, json=payload, headers=headers)
        if r.status_code == 201 or r.status_code == 200:
            return True, r.json()
        else:
            return False, r.text
    except Exception as e:
        return False, str(e)

def run_sms_night_campaign():
    """
    Targets TRACED or OSINT_VERIFIED leads in TX, OK, NE.
    Sends a soft 'Forensic Verification' SMS to prepare for tomorrow's visits.
    """
    logger.info("📱 STARTING NATIONAL SMS FORENSIC ATTACK (TX/OK/NE)...")
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # Select leads that have phone numbers but haven't been contacted or were pending
    query = """
    SELECT c.id, c.homeowner_name, c.phone_number, c.street_address, s.city, s.state
    FROM contacts c
    JOIN storms s ON c.event_id = s.id
    WHERE c.phone_number IS NOT NULL
    AND c.status IN ('TRACED', 'OSINT_VERIFIED', 'Pending')
    AND s.state IN ('TX', 'OK', 'NE', 'KS', 'TEXAS', 'OKLAHOMA', 'NEBRASKA', 'KANSAS')
    AND c.qualification_status = 'QUALIFIED'
    """
    c.execute(query)
    leads = c.fetchall()
    
    if not leads:
        logger.info("No fresh leads available for SMS outreach.")
        conn.close()
        return

    success_count = 0
    for lead in leads:
        l_id = lead['id']
        l_name = lead['homeowner_name'].split(' ')[0] # First name for rapport
        l_phone = lead['phone_number']
        l_addr = lead['street_address']
        l_city = lead['city']
        
        # High-Authority Forensic SMS Script
        message = (f"Hi {l_name}, this is NOT a sales call. We identified significant structural damage at {l_addr} "
                   f"during the recent storm. We are reaching out as part of a County initiative to connect "
                   f"homeowners with licensed, pre-screened contractors for immediate help. "
                   f"Reply YES if you need repairs and are ready for a 100% FREE estimate today.")
        
        logger.info(f"📲 Sending to {l_name} ({l_phone})...")
        success, response = send_sms_via_telnyx(l_phone, message)
        
        if success:
            success_count += 1
            # Update status to SMS_SENT
            c.execute("UPDATE contacts SET status = 'SMS_SENT' WHERE id = ?", (l_id,))
        else:
            logger.error(f"❌ Failed for {l_phone}: {response}")
        
        # Rate limit safety
        time.sleep(1)
        
    conn.commit()
    conn.close()
    logger.info(f"✅ CAMPAIGN COMPLETE: {success_count} forensics texts dispatched.")

if __name__ == "__main__":
    run_sms_night_campaign()
