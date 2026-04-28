import os
import sqlite3
import logging
import requests
from twilio.rest import Client
from dotenv import load_dotenv
from elevenlabs.client import ElevenLabs

# Load credentials
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db')

class TwilioVoiceAgent:
    def __init__(self):
        self.account_sid = os.getenv('TWILIO_ACCOUNT_SID')
        self.auth_token = os.getenv('TWILIO_AUTH_TOKEN')
        self.from_phone = os.getenv("TWILIO_PHONE_NUMBER")
        self.el_api_key = os.getenv('ELEVENLABS_API_KEY')
        
        if not self.account_sid or not self.auth_token:
            logger.error("CRITICAL: Twilio credentials missing. VOICE DISPATCH DISABLED.")
            self.client = None
        else:
            self.client = Client(self.account_sid, self.auth_token)
            logger.info("Twilio Client initialized for REAL AGENT ACTIVITY.")

        if self.el_api_key:
            self.el_client = ElevenLabs(api_key=self.el_api_key)
            self._verify_elevenlabs()
        else:
            self.el_client = None
            logger.warning("ElevenLabs API Key missing.")

    def _verify_elevenlabs(self):
        """Checks if the ElevenLabs connector is fully operational."""
        try:
            # Simple ping to get available voices
            voices = self.el_client.voices.get_all()
            logger.info("✅ ElevenLabs Connector: ONLINE. Ready for high-fidelity dispatch.")
        except Exception as e:
            logger.error(f"❌ ElevenLabs Connector Error: {e}")
            self.el_client = None

    def make_verification_call(self, lead_id, name, phone, address):
        """
        Triggers an AI voice call to verify interest for an immediate site visit.
        """
        logger.info(f"Initiating Voice Verification for: {name} at {address}")
        
        if not phone or phone == "UNVERIFIED":
            logger.warning(f"Skipping Lead #{lead_id}: No valid phone number.")
            return False

        # TwiML for the AI conversation using ElevenLabs High-Fidelity Voice
        # We use the ElevenLabs Connector syntax: voice="elevenlabs:VOICE_ID"
        voice_id = "21m00Tcm4TlvDq8ikWAM" # Rachel
        twiml = f"""
        <Response>
            <Say voice="elevenlabs:{voice_id}">
                Hi {name}, this is the Roof Hunter forensic team. 
                Our satellite analysis just flagged your property at {address} for significant hail damage from the recent storm. 
                We have a licensed contractor in your neighborhood today. 
                Would you like a free, 10-minute visual inspection this afternoon? 
                Please say yes or press 1 to confirm an ASAP visit.
            </Say>
            <Gather input="speech dtmf" timeout="5" numDigits="1" action="/verification-response">
                <Say voice="elevenlabs:{voice_id}">We're standing by to help verify your damage. Say yes or press 1.</Say>
            </Gather>
            <Say voice="elevenlabs:{voice_id}">We didn't catch that. We'll follow up via text. Goodbye.</Say>
        </Response>
        """

        if not self.client:
            logger.error("Twilio client not initialized. Skipping real-world call.")
            return False

        try:
            # Use ElevenLabs High-Fidelity Voice via Twilio Connector
            logger.info(f"🎤 ENGAGING ELEVENLABS NEURAL ENGINE (Voice: {voice_id})")
            call = self.client.calls.create(
                twiml=twiml,
                to=phone,
                from_=self.from_phone
            )
            logger.info(f"☎️ CALL INITIATED: SID {call.sid}")
            self._update_call_status(lead_id, "CALL_INITIATED")
            return True
        except Exception as e:
            logger.error(f"Twilio API Error: {e}")
            return False

    def _update_call_status(self, lead_id, status):
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute("""
            UPDATE contacts 
            SET status = ?, 
                call_log = COALESCE(call_log, '') || ? || '\n'
            WHERE id = ?
        """, (status, f"Voice Agent: {status}", lead_id))
        conn.commit()
        conn.close()

from concurrent.futures import ThreadPoolExecutor

def run_voice_dispatch_queue():
    agent = TwilioVoiceAgent()
    
    conn = sqlite3.connect(DB_PATH, timeout=30)
    c = conn.cursor()
    
    # Target leads that are QUALIFIED and skip-traced (have a name) but not yet called
    # Targeted Nebraska/Omaha Priority Sweep
    query = """
    SELECT contacts.id, contacts.homeowner_name, contacts.phone_number, contacts.street_address, storms.city, storms.state
    FROM contacts
    JOIN storms ON contacts.event_id = storms.id
    WHERE contacts.qualification_status = 'QUALIFIED'
    AND contacts.homeowner_name IS NOT NULL
    AND contacts.phone_number IS NOT NULL
    AND (contacts.status = 'Pending' OR contacts.status = 'TRACED' OR contacts.status = 'OSINT_VERIFIED')
    ORDER BY 
        (storms.state = 'NE' AND storms.magnitude >= 3.5) DESC,
        (storms.city = 'Sherman' AND storms.magnitude >= 3.75) DESC,
        storms.magnitude DESC
    LIMIT 200
    """
    
    c.execute(query)
    queue = c.fetchall()
    conn.close()
    
    if not queue:
        logger.info("No qualified leads ready for voice dispatch.")
        return

    logger.info(f"🔥 SCALING CONCURRENCY: Dispatching {len(queue)} concurrent ElevenLabs calls...")

    def dispatch_call(lead):
        lid, name, phone, addr, city, state = lead
        agent.make_verification_call(lid, name, phone, f"{addr}, {city}")

    # Fire off calls in parallel (10 at a time to stay safe within Twilio/ElevenLabs trunking limits)
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(dispatch_call, queue)

if __name__ == "__main__":
    run_voice_dispatch_queue()
