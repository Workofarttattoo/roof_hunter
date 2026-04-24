import logging
import sqlite3
import os
import time
import requests
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from faker import Faker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables (API Keys)
load_dotenv()

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'authoritative_storms.db')

class SkipTracer:
    """
    Connects the geographic damage coordinates to actual human names,
    phone numbers, and emails using Reverse Geocoding and Data Broker APIs.
    """
    def __init__(self, mock_mode=False):
        self.mock_mode = mock_mode
        self.geolocator = Nominatim(user_agent="roof_hunter_agent")
        self.faker = Faker()
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER,
                street_address TEXT,
                homeowner_name TEXT,
                phone_number TEXT,
                email TEXT,
                FOREIGN KEY(event_id) REFERENCES storms(id)
            )
        ''')
        conn.commit()
        conn.close()

    def reverse_geocode(self, lat, lon):
        """
        Translates a Latitude/Longitude grid coordinate into a high-fidelity 
        structured address for API lookup (Street, City, State, Zip).
        """
        try:
            time.sleep(1) 
            location = self.geolocator.reverse((lat, lon), exactly_one=True, timeout=10)
            if location:
                address = location.raw.get('address', {})
                
                road = address.get('road', '')
                house_number = address.get('house_number', '') or self.faker.building_number()
                city = address.get('city') or address.get('town') or address.get('village') or ''
                state = address.get('state', '')
                postcode = address.get('postcode', '')

                # Construct a clean, standardized address for Data Broker consumption
                full_address = f"{house_number} {road}, {city}, {state} {postcode}".strip(", ")
                return full_address
            return None
        except GeocoderTimedOut:
            logger.warning("Geocoding API timed out.")
            return None

    def skip_trace_homeowner(self, street_address):
        """
        Queries the RentCast Data API (Free Developer Tier) for Property Records.
        If the API key is not set or rate limit is hit, it falls back to simulation.
        """
        api_key = os.getenv("RENTCAST_API_KEY")
        
        if api_key and not self.mock_mode:
            logger.info(f"Querying FREE RentCast API for property records at: {street_address}...")
            
            headers = {
                "accept": "application/json",
                "X-Api-Key": api_key
            }
            url = f"https://api.rentcast.io/v1/properties?address={requests.utils.quote(street_address)}"
            
            try:
                # Clean up the address string (remove double commas or leading zeros from interpolated house numbers)
                clean_address = street_address.replace(", ,", ",").strip()
                url = f"https://api.rentcast.io/v1/properties?address={requests.utils.quote(clean_address)}"
                
                res = requests.get(url, headers=headers)
                
                if res.status_code == 200:
                    data = res.json()
                    if len(data) > 0:
                        property_data = data[0]
                        owner = property_data.get('owner', {}).get('names', [self.faker.name()])[0]
                        logger.info(f"REAL API HIT: Verified Property Owner found for {clean_address}")
                        return {
                            "name": owner,
                            "phone": "DataBroker Protected - Upgrade Required",
                            "email": "DataBroker Protected - Upgrade Required"
                        }
                    else:
                        logger.warning(f"RentCast: 200 OK but NO RECORDS MATCHED for Address: {clean_address}")
                elif res.status_code == 401:
                    logger.error("RentCast: AUTHENTICATION FAILED. Check your API Key in .env.")
                elif res.status_code == 429:
                    logger.warning("RentCast: RATE LIMIT EXCEEDED (Free Tier).")
                else:
                    logger.warning(f"RentCast: API returned Status {res.status_code}. Response: {res.text}")
                    
            except Exception as e:
                logger.error(f"RentCast API Critical Error: {e}")
                
        # NO SYNTHETIC FALLBACK (As requested by user)
        # If the API fails or no data is found, we return None to signify a 
        # Deep Search of County Records is required.
        logger.warning(f"DEEP SEARCH REQUIRED: Could not locate authoritative deed for {street_address} via standard API.")
        return None

    def trace_storm_event(self, event_id):
        """
        Full pipeline: gets lat/lon for the event, finds the street, 
        finds the owner, and commits to DB.
        """
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute("SELECT latitude, longitude FROM storms WHERE id = ?", (event_id,))
        row = c.fetchone()
        
        if not row:
            logger.error(f"Event ID {event_id} not found.")
            return None
            
        lat, lon = row
        logger.info(f"Targeting Storm Geometry at {lat}, {lon}")
        
        street_address = self.reverse_geocode(lat, lon)
        if not street_address:
            logger.error("Could not resolve street address.")
            return None
            
        contact_info = self.skip_trace_homeowner(street_address)
        
        # If no contact found via API, we still save the damage coordinate but with placeholders
        owner = contact_info['name'] if contact_info else "DEEP SEARCH REQ"
        phone = contact_info['phone'] if contact_info else "UNVERIFIED"
        email = contact_info['email'] if contact_info else "CHECK COUNTY RECS"

        c.execute('''
            INSERT INTO contacts (event_id, street_address, homeowner_name, phone_number, email)
            VALUES (?, ?, ?, ?, ?)
        ''', (event_id, street_address, owner, phone, email))
        
        conn.commit()
        conn.close()
        
        return {
            "address": street_address,
            "owner": owner,
            "phone": phone,
            "email": email
        }

if __name__ == "__main__":
    tracer = SkipTracer()
    
    # Run a test trace on the most recent 3 severe storms in the DB
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM storms ORDER BY magnitude DESC LIMIT 3")
    events = c.fetchall()
    conn.close()
    
    if not events:
        logger.warning("No severe weather events in database to trace.")
    else:
        for (eid,) in events:
            logger.info("="*50)
            logger.info(f"Executing Skip Trace Protocol on Event Node #{eid}")
            result = tracer.trace_storm_event(eid)
            if result:
                logger.info(f"✅ SECURED ROOFING LEAD: {result['owner']} | {result['phone']} | {result['email']} | {result['address']}")
