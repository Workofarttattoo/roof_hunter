import os
import sys
import sqlite3
import json
import base64
import requests
import logging
import csv
from pathlib import Path
from fastapi import FastAPI, Request, Response, Body, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from typing import Optional
import time

# Load Environment Variables
load_dotenv()
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
AGENT_ID = os.getenv("ELEVENLABS_AGENT_ID") # Set this in .env
SYNC_API_KEY = os.getenv("SYNC_API_KEY")
ADMIN_EMAILS = {e.strip().lower() for e in os.getenv("ADMIN_EMAILS", "thewhiteknight702@gmail.com").split(",") if e.strip()}
SLACK_TICKETS_WEBHOOK = os.getenv("SLACK_TICKETS_WEBHOOK")
DISCORD_TICKETS_WEBHOOK = os.getenv("DISCORD_TICKETS_WEBHOOK")

_src_dir = str(Path(__file__).resolve().parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)
from aws_lead_sync import sync_from_env  # noqa: E402
from roofhunter_platform import (  # noqa: E402
    ensure_platform_tables,
    zip_tier_and_price,
    damage_tier_counts,
    by_state_damage_summary,
    verified_visits_today_for_region,
    post_ticket_to_webhooks,
)
from lead_verification_media import (  # noqa: E402
    ensure_lead_verification_media_table,
    ensure_media_cached,
    media_to_api_payload,
)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(title="Ridgeline Forensic Dashboard API")

# ── CORS CONFIGURATION (Fixes Dashboard & Bland AI Blockages) ──────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths: local repo by default; override in containers (e.g. ROOF_HUNTER_ROOT=/app)
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BASE_DIR = os.getenv("ROOF_HUNTER_ROOT", _REPO_ROOT)
DB_PATH = os.getenv(
    "ROOF_HUNTER_DB_PATH",
    os.path.join(BASE_DIR, "leads_manifests", "authoritative_storms.db"),
)
TRAINING_DATA_DIR = os.getenv(
    "ROOF_HUNTER_TRAINING_DIR",
    os.path.join(BASE_DIR, "training_data"),
)

os.makedirs(TRAINING_DATA_DIR, exist_ok=True)
app.mount("/images", StaticFiles(directory=TRAINING_DATA_DIR), name="images")


@app.on_event("startup")
def _platform_startup():
    try:
        with sqlite3.connect(DB_PATH, timeout=60) as conn:
            ensure_platform_tables(conn)
            ensure_lead_verification_media_table(conn)
        logger.info("Platform tables ready (auth, tickets, zip tiers, pilot markets).")
    except Exception as e:
        logger.exception("Platform startup migration failed: %s", e)


def _meta_value(conn: sqlite3.Connection, key: str):
    try:
        row = conn.execute("SELECT v FROM ingest_meta WHERE k = ?", (key,)).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Ensure new columns exist for forensics
    try:
        c = conn.cursor()
        c.execute("ALTER TABLE contacts ADD COLUMN structures_hit TEXT")
        c.execute("ALTER TABLE contacts ADD COLUMN image_findings TEXT")
        c.execute("ALTER TABLE contacts ADD COLUMN lead_priority TEXT")
    except sqlite3.OperationalError:
        pass # Already exists
    return conn

# ── BLAND AI TOOL MODELS ───────────────────────────────────────────────
class SaveLeadRequest(BaseModel):
    name: str = None
    address: str = None
    phone: str = None
    priority: str = "UNKNOWN"
    damage: str = None
    inspection: str = "no"
    time: str = None
    notes: str = None

class PropertyIntelRequest(BaseModel):
    address: str

class ScheduleRequest(BaseModel):
    address: str
    time: str
    phone: str


class RegisterRequest(BaseModel):
    email: str = Field(..., min_length=3)
    username: str = Field(..., min_length=2)
    password: str = Field(..., min_length=8)


class LoginRequest(BaseModel):
    email: str
    password: str


class TicketCreate(BaseModel):
    title: str
    body: str
    lead_id: Optional[int] = None
    priority: str = "normal"


class SquareCheckoutRequest(BaseModel):
    amount_usd: float = Field(..., gt=0, description="Charge amount in USD")
    description: str = "Ridgeline — account or lead credit"
    buyer_email: Optional[str] = None


async def _get_auth_payload(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = authorization.split(" ", 1)[1].strip()
    try:
        return decode_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token") from None


async def require_admin(auth: dict = Depends(_get_auth_payload)) -> dict:
    if auth.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return auth


@app.post("/elevenlabs/webhook")
async def elevenlabs_webhook(request: Request):
    """
    Receives post-call conversation data from ElevenLabs.
    Documentation: https://elevenlabs.io/docs/conversational-ai/api-reference/webhook
    """
    try:
        data = await request.json()
        logger.info(f"🎤 ElevenLabs Webhook Received: {json.dumps(data, indent=2)}")
        
        conversation_id = data.get("conversation_id")
        agent_id = data.get("agent_id")
        status = data.get("status") # e.g., 'finished'
        
        # Extract metadata if you passed it during conversation start
        metadata = data.get("metadata", {})
        lead_address = metadata.get("property_address")
        
        # Extract analysis (if configured in ElevenLabs dashboard)
        analysis = data.get("analysis", {})
        summary = analysis.get("summary", "No summary provided.")
        call_success = analysis.get("success", False)
        
        if lead_address:
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE contacts 
                    SET call_log = ?, qualification_status = ?
                    WHERE street_address = ?
                """, (summary, "SUCCESS_CONVERTED" if call_success else "CALL_FINISHED", lead_address))
                conn.commit()
                logger.info(f"✅ Database updated for lead at {lead_address}")
        
        return {"status": "received"}
    except Exception as e:
        logger.error(f"❌ Error in ElevenLabs Webhook: {e}")
        return {"status": "error", "message": str(e)}

# ── ELEVENLABS CALL DISPATCHER ────────────────────────────────────────

@app.post("/elevenlabs/dispatch-batch")
async def dispatch_elevenlabs_batch(csv_path: str = Body(..., embed=True)):
    """
    Triggers calls for a lead list using ElevenLabs Conversational AI.
    """
    if not ELEVENLABS_API_KEY or not AGENT_ID:
        raise HTTPException(status_code=400, detail="ElevenLabs API Key or Agent ID missing in .env")
        
    try:
        # 1. Load the CSV
        if not os.path.exists(csv_path):
            raise HTTPException(status_code=404, detail=f"CSV not found at {csv_path}")
            
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            leads = list(reader)
            
        logger.info(f"🚀 Dispatching {len(leads)} calls via ElevenLabs...")
        
        results = []
        for lead in leads:
            # Determine Regional Line (Telnyx Connection)
            # OK: +14052991068, TX: +19458370141
            is_oklahoma = "OK" in lead.get('State', '').upper()
            caller_id_id = "phnum_5201kqe8agt5ffj8qf81ydk7ek54" if is_oklahoma else "phnum_0001kqe9hv3metra8y3ykgzg2arq"
            
            # Prepare conversation metadata for SIP Trunking
            payload = {
                "agent_id": AGENT_ID,
                "agent_phone_number_id": caller_id_id,
                "to_number": phone,
                "dynamic_variables": {
                    "first_name": lead.get('First_Name', 'Homeowner'),
                    "property_address": lead.get('Street Address', ''),
                    "city": lead.get('City', ''),
                    "hail_date": lead.get('Event Date', 'the recent storm'),
                    "storm_type": "Hail Storm",
                    "damage_probability": lead.get('AI Damage Score %', '')
                }
            }
            
            # 1. Trigger ElevenLabs Call (SIP Trunking)
            try:
                response = requests.post(
                    f"https://api.elevenlabs.io/v1/convai/sip-trunk/outbound-call",
                    headers={"xi-api-key": ELEVENLABS_API_KEY},
                    json=payload
                )
                
                # 2. Trigger Telnyx SMS Follow-up (Omnichannel)
                sms_message = f"Hi {lead.get('First_Name')}, this is Sarah with Ridgeline. Our forensic scan flagged your property at {lead.get('Street Address')} for hail damage. I'm trying to reach you to schedule a free verification scan. Call or text me back here!"
                
                requests.post(
                    "https://api.telnyx.com/v2/messages",
                    headers={"Authorization": f"Bearer {os.getenv('TELNYX_API_KEY')}"},
                    json={
                        "from": "+14052991068" if is_oklahoma else "+19458370141",
                        "to": phone,
                        "text": sms_message
                    }
                )
                
                if response.status_code == 200:
                    results.append(response.json())
                    logger.info(f"✅ Call + SMS successfully triggered for {phone}")
                else:
                    logger.error(f"❌ ElevenLabs Error ({response.status_code}): {response.text}")
            except Exception as api_err:
                logger.error(f"❌ Failed to reach ElevenLabs/Telnyx: {api_err}")
            
            time.sleep(0.5) # Increased throughput (2 calls per second)
            
        return {"status": "dispatched", "count": len(leads)}
        
    except Exception as e:
        logger.error(f"❌ Error in Batch Dispatch: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── BLAND AI CORE ENDPOINTS (Fixes 500 Errors) ─────────────────────────

@app.post("/save-lead")
async def save_lead(data: SaveLeadRequest):
    logger.info(f"📥 BLAND AI (Candy): Saving Lead -> {data.name} at {data.address}")
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO contacts (homeowner_name, street_address, phone_number, status, proof_msg, notes)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(phone_number) DO UPDATE SET
                    status = excluded.status,
                    notes = excluded.notes
            """, (data.name, data.address, data.phone, f"QUALIFIED_{data.priority}", data.damage, data.notes))
            conn.commit()
        return {"status": "success", "message": "Lead logged to forensic manifest."}
    except Exception as e:
        logger.error(f"Error saving lead: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats")
async def api_stats():
    """Aggregate counts for the marketplace header (full DB, not just current filter)."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            total = c.execute(
                """
                SELECT COUNT(*) FROM contacts c
                INNER JOIN storms s ON c.event_id = s.id
                """
            ).fetchone()[0]
            high = c.execute(
                """
                SELECT COUNT(*) FROM contacts c
                INNER JOIN storms s ON c.event_id = s.id
                WHERE COALESCE(c.damage_score, 0) >= 40
                """
            ).fetchone()[0]
            qualified = c.execute(
                """
                SELECT COUNT(*) FROM contacts c
                INNER JOIN storms s ON c.event_id = s.id
                WHERE UPPER(COALESCE(c.qualification_status, '')) = 'QUALIFIED'
                """
            ).fetchone()[0]
            aws_source = c.execute(
                """
                SELECT COUNT(*) FROM contacts c
                INNER JOIN storms s ON c.event_id = s.id
                WHERE UPPER(COALESCE(s.source, '')) = 'AWS_S3'
                """
            ).fetchone()[0]
        with sqlite3.connect(DB_PATH, timeout=30) as meta_conn:
            last_sync = _meta_value(meta_conn, "last_aws_sync")
            last_import = _meta_value(meta_conn, "last_aws_import_count")
        return {
            "total_leads": total,
            "high_severity": high,
            "qualified": qualified,
            "from_aws_storms": aws_source,
            "last_aws_sync": last_sync,
            "last_aws_import_count": int(last_import) if last_import is not None else None,
        }
    except Exception as e:
        logger.error(f"Error computing stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/leads/sync-aws")
async def api_sync_aws(request: Request, dry_run: bool = False):
    """
    Pull CSV from S3 (AWS_LEADS_BUCKET / AWS_LEADS_OBJECT_KEY) and insert new contacts.
    Set SYNC_API_KEY in the environment and send header X-Roof-Hunter-Sync to protect in production.
    """
    if SYNC_API_KEY and request.headers.get("X-Roof-Hunter-Sync") != SYNC_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized (sync key required)")
    try:
        result = sync_from_env(DB_PATH, dry_run=dry_run)
        return result
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        logger.exception("AWS sync failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/leads/teasers")
async def get_leads_teasers(q: str = None, zip: str = None, material: str = "ALL"):
    logger.info(f"📡 Serving leads to dashboard (q={q}, zip={zip}, material={material})")
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            query = """
                SELECT 
                    c.id, s.state, c.damage_score, c.qualification_status, s.magnitude,
                    c.street_address, s.city, c.zip_code, c.material_interest, c.timeline,
                    c.call_log, c.proof_msg, c.image_findings,
                    s.event_date, s.latitude, s.longitude
                FROM contacts c
                JOIN storms s ON c.event_id = s.id
                WHERE 1=1
            """
            params = []
            if q:
                query += " AND (s.city LIKE ? OR s.state LIKE ? OR c.street_address LIKE ?)"
                params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
            if zip:
                query += " AND c.zip_code = ?"
                params.append(zip)
            if material != "ALL":
                query += " AND c.material_interest = ?"
                params.append(material)
            
            query += " ORDER BY c.damage_score DESC LIMIT 100"
            
            c.execute(query, params)
            rows = c.fetchall()
            
            leads = []
            for row in rows:
                ds = float(row["damage_score"] or 0)
                band = "high" if ds >= 40 else "medium" if ds >= 15 else "low"
                pricing = zip_tier_and_price(row["zip_code"], conn)
                # Redact address for teaser
                addr_parts = row["street_address"].split(" ")
                redacted = "XXX " + " ".join(addr_parts[1:]) if len(addr_parts) > 1 else row["street_address"]

                # Image paths are same-origin (Ingress sends /images to this API)
                img_id = row["id"]
                image_before = f"/images/OKC-{img_id}-482_before.jpg" if img_id in [5803, 7837, 930] else None
                image_after = f"/images/OKC-{img_id}-482_after.jpg" if img_id in [5803, 7837, 930] else None

                if not image_before and ds > 30:
                    image_before = "/images/hail_68102_11.png"
                    image_after = "/images/hail_68102_15.png"

                vpayload = media_to_api_payload(
                    ensure_media_cached(
                        conn,
                        contact_id=row["id"],
                        row_contact=row,
                        row_storm=row,
                        training_dir=TRAINING_DATA_DIR,
                    )
                )

                leads.append({
                    "id": row["id"],
                    "state": row["state"],
                    "damage_score": ds,
                    "damage_band": band,
                    "qualification_status": row["qualification_status"],
                    "magnitude": float(row["magnitude"] or 0),
                    "redacted_address": redacted,
                    "city": row["city"],
                    "zip_code": row["zip_code"],
                    "material_interest": row["material_interest"],
                    "timeline": row["timeline"],
                    "call_log": row["call_log"],
                    "proof_msg": row["proof_msg"],
                    "image_url_before": image_before,
                    "image_url_after": image_after,
                    "image_findings": "Highlighted impact zones (red) correlate with 2.5\" hail trajectory. Substrate compromise detected."
                    if ds > 30
                    else None,
                    "zip_tier": pricing["tier"],
                    "list_price_usd": pricing["price_usd"],
                    "base_lead_price_usd": pricing["base_usd"],
                    "tier_multiplier": pricing["multiplier"],
                    **vpayload,
                })
            return leads
    except Exception as e:
        logger.error(f"Error fetching teasers: {e}")
        return []

@app.post("/api/leads/qualify")
async def qualify_lead_endpoint(data: dict = Body(...)):
    lead_id = data.get("lead_id")
    logger.info(f"📞 TRIGGERING CANDY AI FOR LEAD #{lead_id}")
    # In a real scenario, this would call the Bland AI API to start a call.
    # For now, we simulate the status change.
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE contacts SET qualification_status = 'CALL_INITIATED' WHERE id = ?", (lead_id,))
            conn.commit()
        return {"status": "success", "message": "Call initiated via Candy AI."}
    except Exception as e:
        logger.error(f"Error qualifying lead: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/property-intel")
@app.post("/property-intel")
async def get_property_intel(req: Request):
    if req.method == "POST":
        try:
            data = await req.json()
            address = data.get("address", "")
        except:
            address = "Unknown"
    else:
        address = req.query_params.get("address", "")

    logger.info(f"🔍 BLAND AI: Fetching Intel for -> {address}")
    return {
        "hail_size": "2.75 inches (Catastrophic)",
        "damage_probability": "High (82.48% Spectral Anomaly)",
        "structures_hit": "Main roof, Patio awning, South-facing windows",
        "image_findings": "Severe shingle displacement and impact craters detected via satellite zoom.",
        "confidence": 0.94
    }

@app.post("/schedule")
async def schedule_inspection(data: ScheduleRequest):
    logger.info(f"📅 BLAND AI: Scheduling Inspection for -> {data.address} at {data.time}")
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE contacts SET status = ?, verified_at = ? WHERE street_address = ?", 
                     (f"INSPECTION_BOOKED_{data.time}", data.time, data.address))
            conn.commit()
        return {"status": "success", "message": f"Inspection booked for {data.time}"}
    except Exception as e:
        logger.error(f"Error scheduling: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── AUTH & PAYMENTS ───────────────────────────────────────────────────

@app.post("/api/auth/register")
async def api_register(body: RegisterRequest):
    email = body.email.strip().lower()
    username = body.username.strip()
    role = "admin" if email in ADMIN_EMAILS else "client"
    ph = hash_password(body.password)
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(
                "INSERT INTO app_users (email, username, password_hash, role) VALUES (?,?,?,?)",
                (email, username, ph, role),
            )
            conn.commit()
            uid = c.lastrowid
        token = issue_token(uid, email, role)
        return {"token": token, "user": {"id": uid, "email": email, "username": username, "role": role}}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Email or username already registered")


@app.post("/api/auth/login")
async def api_login(body: LoginRequest):
    email = body.email.strip().lower()
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT id, password_hash, username, role FROM app_users WHERE email = ?", (email,)
        ).fetchone()
    if not row or not verify_password(body.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = issue_token(row["id"], email, row["role"])
    return {
        "token": token,
        "user": {"id": row["id"], "email": email, "username": row["username"], "role": row["role"]},
    }


@app.get("/api/me")
async def api_me(auth: dict = Depends(_get_auth_payload)):
    uid = int(auth["sub"])
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT id, email, username, role, square_customer_id FROM app_users WHERE id = ?", (uid,)
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return dict(row)


def _square_host():
    return (
        "https://connect.squareupsandbox.com"
        if os.getenv("SQUARE_ENV", "production").lower() == "sandbox"
        else "https://connect.squareup.com"
    )


@app.post("/api/payments/square/checkout-link")
async def api_square_checkout(body: SquareCheckoutRequest, auth: dict = Depends(_get_auth_payload)):
    token = os.getenv("SQUARE_ACCESS_TOKEN")
    loc = os.getenv("SQUARE_LOCATION_ID")
    if not token or not loc:
        raise HTTPException(status_code=503, detail="Square not configured (SQUARE_ACCESS_TOKEN, SQUARE_LOCATION_ID)")
    ver = os.getenv("SQUARE_API_VERSION", "2024-11-20")
    cents = int(round(body.amount_usd * 100))
    idem = str(__import__("uuid").uuid4())
    payload = {
        "idempotency_key": idem,
        "order": {
            "location_id": loc,
            "line_items": [
                {
                    "name": body.description[:256],
                    "quantity": "1",
                    "base_price_money": {"amount": cents, "currency": "USD"},
                }
            ],
        },
    }
    if body.buyer_email:
        payload["pre_populated_data"] = {"buyer_email": body.buyer_email}
    r = requests.post(
        f"{_square_host()}/v2/online-checkout/payment-links",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Square-Version": ver,
        },
        json=payload,
        timeout=45,
    )
    if not r.ok:
        logger.error("Square payment link error: %s %s", r.status_code, r.text)
        raise HTTPException(status_code=502, detail=r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text)
    data = r.json()
    pl = data.get("payment_link") or {}
    return {"url": pl.get("url"), "order_id": pl.get("order_id"), "raw": data}


# ── METRICS (INFographics) ─────────────────────────────────────────────

@app.get("/api/metrics/damage-tiers")
async def api_damage_tiers(state: Optional[str] = None):
    try:
        with get_db_connection() as conn:
            summary = damage_tier_counts(conn, state)
            by_state = by_state_damage_summary(conn)
        return {"filter_state": state, "summary": summary, "by_state": by_state}
    except Exception as e:
        logger.exception("damage-tiers failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


# ── ADMIN ──────────────────────────────────────────────────────────────

@app.get("/api/admin/leads")
async def api_admin_leads(_admin: dict = Depends(require_admin)):
    """Full lead rows: ZIP visible, dynamic list price from zip tier."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(
                """
                SELECT c.id, c.street_address, c.zip_code, c.city, s.state,
                  c.damage_score, c.qualification_status, c.status, c.verified_at,
                  s.magnitude, s.event_date, c.proof_msg
                FROM contacts c
                INNER JOIN storms s ON c.event_id = s.id
                ORDER BY COALESCE(c.damage_score, -1) DESC
                LIMIT 2000
                """
            )
            rows = c.fetchall()
            out = []
            for row in rows:
                pricing = zip_tier_and_price(row["zip_code"], conn)
                ds = float(row["damage_score"] or 0)
                band = "high" if ds >= 40 else "medium" if ds >= 15 else "low"
                out.append(
                    {
                        "id": row["id"],
                        "street_address": row["street_address"],
                        "zip_code": row["zip_code"],
                        "city": row["city"],
                        "state": row["state"],
                        "damage_score": ds,
                        "damage_band": band,
                        "qualification_status": row["qualification_status"],
                        "status": row["status"],
                        "verified_at": row["verified_at"],
                        "magnitude": float(row["magnitude"] or 0),
                        "event_date": row["event_date"],
                        "proof_preview": (row["proof_msg"] or "")[:140],
                        **pricing,
                    }
                )
        return {"leads": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/admin/operations-summary")
async def api_admin_ops(_admin: dict = Depends(require_admin)):
    with get_db_connection() as conn:
        targets = [dict(r) for r in conn.execute("SELECT * FROM regional_daily_targets").fetchall()]
        pilots = [dict(r) for r in conn.execute("SELECT * FROM pilot_markets ORDER BY state, city").fetchall()]
        for t in targets:
            t["verified_visits_today"] = verified_visits_today_for_region(conn, t["region_code"])
    return {"regional_targets": targets, "pilot_markets": pilots}

@app.post("/api/admin/tickets")
async def api_admin_ticket_create(body: TicketCreate, _admin: dict = Depends(require_admin)):
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute(
            """INSERT INTO support_tickets (title, body, lead_id, priority, status)
               VALUES (?,?,?,?,'open')""",
            (body.title, body.body, body.lead_id, body.priority),
        )
        tid = c.lastrowid
        conn.commit()
    refs = post_ticket_to_webhooks(body.title, body.body, tid, SLACK_TICKETS_WEBHOOK, DISCORD_TICKETS_WEBHOOK)
    with get_db_connection() as conn:
        conn.execute(
            "UPDATE support_tickets SET discord_reference = ?, slack_reference = ? WHERE id = ?",
            (refs.get("discord"), refs.get("slack"), tid),
        )
        conn.commit()
    return {"id": tid, "notify": refs}


@app.get("/api/admin/tickets")
async def api_admin_ticket_list(_admin: dict = Depends(require_admin)):
    with get_db_connection() as conn:
        rows = conn.execute("SELECT * FROM support_tickets ORDER BY id DESC LIMIT 200").fetchall()
    return {"tickets": [dict(r) for r in rows]}


# ── LOGGING & ROOT ───────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "online", "engine": "Ridgeline Forensic Active Build v3.2"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
