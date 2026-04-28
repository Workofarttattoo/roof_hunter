import os
import sqlite3
import logging
import time
import asyncio
import requests
from telnyx import Telnyx
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db'))
logger.info(f"📊 DATABASE AT: {DB_PATH}")

class TelnyxVoiceAgent:
    """
    High-Speed Telnyx Voice Agent (V4 SDK).
    Replaces Twilio to bypass trial restrictions.
    Integrated with ElevenLabs Rachel Voice.
    """
    def __init__(self):
        self.api_key = os.getenv('TELNYX_API_KEY')
        self.client = Telnyx(api_key=self.api_key)
        self.from_phone = os.getenv('TELNYX_PHONE_NUMBER')
        self.connection_id = os.getenv('TELNYX_CONNECTION_ID')
        
    def make_verification_call(self, lead_id, name, phone, address, state):
        # E.164 Sanitization
        phone = ''.join(filter(str.isdigit, phone))
        if not phone.startswith('1') and len(phone) == 10:
            phone = '+1' + phone
        elif not phone.startswith('+'):
            phone = '+' + phone

        # 🎯 STATE-SPECIFIC GEOGRAPHIC SIGNALING
        # Use authorized numbers from your Telnyx profile
        if state in ['OK', 'OKLAHOMA']:
            from_phone = "+14052991068" # Verified Oklahoma Uplink
        else:
            from_phone = "+19458370141" # Verified Texas Forensic Primary

        logger.info(f"☎️ DISPATCHING FORENSIC CALL: {name} ({state}) | To: {phone} | Via: {from_phone}")
        
        url = "https://api.telnyx.com/v2/calls"
        payload = {
            "to": phone, 
            "from": from_phone,
            "connection_id": self.connection_id,
            "timeout_secs": 30
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code in [200, 201]:
                logger.info(f"✅ TELNYX DISPATCH SUCCESS -> {phone} (Internal ID: {response.json().get('data', {}).get('call_control_id')})")
                self._update_status(lead_id, "CALL_INITIATED_TELNYX")
                return True
            else:
                resp_json = response.json()
                err_detail = resp_json.get('errors', [{}])[0].get('detail', 'Unknown error')
                logger.error(f"❌ Telnyx API Error ({response.status_code}): {err_detail}")
                self._update_status(lead_id, f"TELNYX_ERR_{response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Telnyx Transport Error for {phone}: {e}")
            return False

    def _update_status(self, lead_id, status):
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute("UPDATE contacts SET status = ? WHERE id = ?", (status, lead_id))
        conn.commit()
        conn.close()

def run_telnyx_dispatch():
    agent = TelnyxVoiceAgent()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # FORENSIC STATE-SPLIT DISPATCH (TEXAS + OKLAHOMA 1:1)
    query = """
    WITH RankedLeads AS (
        SELECT 
            c.id, c.homeowner_name, c.phone_number, c.street_address, s.state,
            ROW_NUMBER() OVER (PARTITION BY s.state ORDER BY s.magnitude DESC) as state_rank
        FROM contacts c
        JOIN storms s ON c.event_id = s.id
        WHERE c.qualification_status = 'QUALIFIED'
        AND c.phone_number IS NOT NULL
        AND (c.status = 'Pending' OR c.status = 'TRACED' OR c.status = 'OSINT_VERIFIED')
        AND (s.state IN ('TX', 'TEXAS', 'OK', 'OKLAHOMA'))
        AND s.magnitude >= 2.0
    )
    SELECT id, homeowner_name, phone_number, street_address, state
    FROM RankedLeads
    ORDER BY state_rank, state
    LIMIT 2000
    """
    c.execute(query)
    leads = c.fetchall()
    conn.close()

    if not leads:
        logger.info("No Qualified TX/OK leads ready for forensic dispatch.")
        return

    logger.info(f"🎙️ STARTING MULTI-STATE FORENSIC DISPATCH FOR {len(leads)} LEADS...")
    with ThreadPoolExecutor(max_workers=15) as executor:
        for lid, name, phone, addr, state in leads:
            executor.submit(agent.make_verification_call, lid, name, phone, addr, state)
            time.sleep(1.2) # Optimized precision velocity

if __name__ == "__main__":
    run_telnyx_dispatch()
