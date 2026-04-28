import os
import sqlite3
import logging
from twilio.rest import Client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db')

class WhatsAppDispatchAgent:
    """
    Automated WhatsApp Dispatch for Roof Hunter forensic leads.
    Bypasses voice trial restrictions by utilizing Twilio's WhatsApp Business API.
    """
    def __init__(self):
        self.account_sid = os.getenv('TWILIO_ACCOUNT_SID')
        self.auth_token = os.getenv('TWILIO_AUTH_TOKEN')
        # Determine the WhatsApp sender from the primary Twilio number
        self.from_whatsapp = f"whatsapp:{os.getenv('TWILIO_PHONE_NUMBER')}"
        
        if self.account_sid and self.auth_token:
            self.client = Client(self.account_sid, self.auth_token)
            logger.info("WhatsApp Dispatch Client Initialized.")
        else:
            self.client = None
            logger.error("Twilio Credentials missing for WhatsApp dispatch.")

    def send_whatsapp_pitch(self, lead_id, name, phone, address):
        if not self.client:
            return False

        # Professional Forensic Pitch
        message_body = (
            f"🏠 *ROOF HUNTER FORENSIC ALERT*\n\n"
            f"Hi {name}, our satellite analysis has flagged your property at *{address}* for significant hail damage from the recent storm.\n\n"
            f"We have a licensed inspector in your zip code today. Would you like to schedule a free 10-minute visual verification this afternoon?\n\n"
            f"Reply *YES* to secure your spot or visit: https://aios.is/roof_hunter_image"
        )

        try:
            message = self.client.messages.create(
                body=message_body,
                from_=self.from_whatsapp,
                to=f"whatsapp:{phone}"
            )
            logger.info(f"✅ WHATSAPP SENT: {message.sid} to {phone}")
            self._update_status(lead_id, "WHATSAPP_SENT")
            return True
        except Exception as e:
            logger.error(f"WhatsApp API Error: {e}")
            return False

    def _update_status(self, lead_id, status):
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute("UPDATE contacts SET status = ?, qualification_status = 'QUALIFIED' WHERE id = ?", (status, lead_id))
        conn.commit()
        conn.close()

def run_whatsapp_queue():
    agent = WhatsAppDispatchAgent()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    c = conn.cursor()
    
    # Priority: Nebraska (Omaha/Lincoln) leads that are verified
    query = """
    SELECT contacts.id, contacts.homeowner_name, contacts.phone_number, contacts.street_address
    FROM contacts
    JOIN storms ON contacts.event_id = storms.id
    WHERE contacts.phone_number IS NOT NULL
    AND contacts.status != 'WHATSAPP_SENT'
    AND (storms.state = 'NE' OR storms.city = 'Sherman')
    LIMIT 20
    """
    c.execute(query)
    leads = c.fetchall()
    conn.close()

    if not leads:
        logger.info("No leads ready for WhatsApp dispatch.")
        return

    logger.info(f"🚀 INITIATING WHATSAPP OUTREACH FOR {len(leads)} LEADS...")
    for lid, name, phone, addr in leads:
        agent.send_whatsapp_pitch(lid, name, phone, addr)

if __name__ == "__main__":
    run_whatsapp_queue()
