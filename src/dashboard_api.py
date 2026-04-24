import sqlite3
import os
import logging
from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv, dotenv_values

# Force absolute path resolution for reliable credential loading
dotenv_path = '/Users/noone/.gemini/antigravity/scratch/roof_hunter/.env'
load_dotenv(dotenv_path, override=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="ECH0-ROOF Intelligence API v2")

def get_api_keys():
    """Helper to read credentials directly from disk (bypasses stale os.environ)"""
    config = dotenv_values(dotenv_path)
    # Debug: log keys found (not values)
    logger.info(f"Keys found in .env: {list(config.keys())}")
    return {
        "RENTCAST": config.get("RENTCAST_API_KEY"),
        "GOOGLE_MAPS": config.get("GOOGLE_MAPS_API_KEY"),
        "TWILIO_SID": config.get("TWILIO_ACCOUNT_SID"),
        "TWILIO_AUTH": config.get("TWILIO_AUTH_TOKEN"),
        "TWILIO_PHONE": config.get("TWILIO_PHONE_NUMBER")
    }


# End of credentials


class SMSRequest(BaseModel):
    to_phone: str
    message: str

class LeadUpdate(BaseModel):
    lead_id: int
    status: str = None
    insurance: str = None

# Mount training_data for image serving
TRAINING_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'training_data')
os.makedirs(TRAINING_DATA_DIR, exist_ok=True)
app.mount("/images", StaticFiles(directory=TRAINING_DATA_DIR), name="images")

# CORS — allow Vite frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db')

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_tier(magnitude, home_value, income):
    """Calculate lead tier based on storm intensity + property value."""
    magnitude = magnitude or 0
    home_value = home_value or 0
    income = income or 0
    if magnitude >= 3.0 and home_value > 400000:
        return "PLATINUM"
    elif magnitude >= 2.0 or home_value > 200000 or income > 80000:
        return "GOLD"
    else:
        return "SILVER"

@app.get("/api/leads")
def get_leads(
    state: str = None,
    min_income: float = None,
    min_hail: float = None,
    limit: int = 50
):
    """
    Returns high-confidence homeowner leads from the authoritative storm database.
    Joins storms + contacts and applies tier scoring.
    """
    conn = get_db_connection()
    c = conn.cursor()

    query = """
    SELECT 
        s.id,
        s.event_date,
        s.event_type,
        s.magnitude,
        s.city,
        s.state,
        s.zipcode,
        s.latitude,
        s.longitude,
        s.median_home_value,
        s.median_household_income,
        c.homeowner_name,
        c.phone_number,
        c.email,
        c.street_address,
        c.insurance_company,
        c.status
    FROM storms s
    JOIN contacts c ON s.id = c.event_id
    WHERE 1=1
    """
    params = []

    if state:
        query += " AND UPPER(s.state) = ?"
        params.append(state.upper())

    if min_income is not None:
        query += " AND s.median_household_income >= ?"
        params.append(min_income)

    if min_hail is not None:
        query += " AND s.magnitude >= ?"
        params.append(min_hail)

    query += " ORDER BY s.magnitude DESC, s.median_home_value DESC LIMIT ?"
    params.append(limit)

    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    results = []
    for row in rows:
        d = dict(row)
        mag = d.get('magnitude') or 0
        val = d.get('median_home_value') or 0
        inc = d.get('median_household_income') or 0

        d['tier'] = get_tier(mag, val, inc)

        # Build proof message
        val_str = f"${val:,.0f}" if val else "N/A"
        inc_str = f"${inc:,.0f}/yr" if inc else "N/A"
        d['proof_msg'] = f"Magnitude: {mag}\" | Home Value: {val_str} | Income: {inc_str}"

        # Image existence check (training_data)
        img_filename = f"hail_target_{d['id']}.png"
        street_filename = f"street_hail_target_{d['id']}.jpg"
        img_path = os.path.join(TRAINING_DATA_DIR, img_filename)
        street_path = os.path.join(TRAINING_DATA_DIR, street_filename)
        d['image_url'] = f"http://127.0.0.1:8000/images/{img_filename}" if os.path.exists(img_path) else None
        d['street_view_url'] = f"http://127.0.0.1:8000/images/{street_filename}" if os.path.exists(street_path) else None

        # Deep scan result placeholder
        d['deep_scan_result'] = None

        results.append(d)

    return {"status": "success", "count": len(results), "data": results}

@app.get("/api/storms")
def get_storms(
    state: str = None,
    min_income: float = None,
    min_hail: float = None,
    limit: int = 50
):
    """Raw storm events (no contact requirement)."""
    conn = get_db_connection()
    c = conn.cursor()

    query = "SELECT * FROM storms WHERE 1=1"
    params = []

    if state:
        query += " AND UPPER(state) = ?"
        params.append(state.upper())

    if min_income is not None:
        query += " AND median_household_income >= ?"
        params.append(min_income)

    if min_hail is not None:
        query += " AND event_type = 'Hail' AND magnitude >= ?"
        params.append(min_hail)

    query += " ORDER BY magnitude DESC, median_household_income DESC LIMIT ?"
    params.append(limit)

    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    return {"status": "success", "count": len(rows), "data": [dict(r) for r in rows]}

@app.get("/api/stats")
def get_stats():
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("SELECT COUNT(*) as total_events FROM storms")
    total_events = c.fetchone()["total_events"]

    c.execute("SELECT COUNT(DISTINCT state) as states_affected FROM storms")
    states_affected = c.fetchone()["states_affected"]

    c.execute("SELECT COUNT(*) as total_contacts FROM contacts")
    total_contacts = c.fetchone()["total_contacts"]

    c.execute("SELECT COUNT(*) as total_leads FROM storms s JOIN contacts c ON s.id = c.event_id")
    total_leads = c.fetchone()["total_leads"]

    c.execute("SELECT MAX(event_date) as latest_event FROM storms")
    latest_event = c.fetchone()["latest_event"]

    conn.close()

    return {
        "status": "success",
        "total_events_tracked": total_events,
        "states_affected": states_affected,
        "total_contacts": total_contacts,
        "total_leads": total_leads,
        "latest_event": latest_event
    }

@app.post("/api/leads/update")
def update_lead(update: LeadUpdate):
    conn = get_db_connection()
    c = conn.cursor()

    if update.status:
        c.execute("UPDATE contacts SET status = ? WHERE event_id = ?", (update.status, update.lead_id))
    if update.insurance:
        c.execute("UPDATE contacts SET insurance_company = ? WHERE event_id = ?", (update.insurance, update.lead_id))

    conn.commit()
    conn.close()
    return {"status": "success", "message": "Lead updated."}

@app.post("/api/send-sms")
def send_sms(req: SMSRequest):
    keys = get_api_keys()
    if not all([keys["TWILIO_SID"], keys["TWILIO_AUTH"], keys["TWILIO_PHONE"]]):
        return {"status": "error", "message": "Twilio credentials not configured in .env"}

    try:
        from twilio.rest import Client
        client = Client(keys["TWILIO_SID"], keys["TWILIO_AUTH"])
        message = client.messages.create(
            body=req.message,
            from_=keys["TWILIO_PHONE"],
            to=req.to_phone
        )
        return {"status": "success", "sid": message.sid}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/leads/deep-scan")
def deep_scan_lead(req: dict = Body(...)):
    """
    Performs a Deep Learning (YOLO/Segmentation) pass on a specific lead.
    Returns insurance-grade quantifiable metrics.
    """
    lead_id = req.get('lead_id')
    img_filename = f"hail_target_{lead_id}.png"
    img_path = os.path.join(TRAINING_DATA_DIR, img_filename)

    if not os.path.exists(img_path):
        return {
            "status": "no_imagery",
            "message": "High-res imagery not yet harvested for this lead.",
            "lead_id": lead_id
        }

    try:
        from src.yolo_detector import RoofDeepLens
        deep_lens = RoofDeepLens()
        analysis = deep_lens.detect_and_quantify(img_path)
        return {
            "status": "success",
            "lead_id": lead_id,
            "damage_assessment": f"{analysis['damage_percent']}% Area Compromised",
            "metrics": analysis
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/corridor-forecast")
def get_corridor_forecast():
    """Returns the pre-computed storm corridor risk analysis."""
    path = os.path.join(os.path.dirname(__file__), '..', 'data_cache', 'corridor_forecast.json')
    if os.path.exists(path):
        import json
        with open(path, 'r') as f:
            return json.load(f)
    return {"status": "error", "message": "Corridor forecast not generated yet."}

@app.get("/api/property-data")
def get_property_data(address: str = Query(...)):
    """Fetches high-fidelity property specs and valuation from RentCast."""
    keys = get_api_keys()
    if not keys["RENTCAST"]:
        return {"status": "error", "message": "RentCast API key not configured."}
    
    import requests
    url = "https://api.rentcast.io/v1/properties/value"
    headers = {"X-Api-Key": keys["RENTCAST"]}
    params = {"address": address}
    
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code == 200:
            return {"status": "success", "data": r.json()}
        return {"status": "error", "message": f"RentCast error: {r.status_code}", "detail": r.text}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
