"""
cloud_notifier.py
-----------------
Automated Lead Dispatcher for Roof Hunter AWS Engine.
Generates CSV manifestations of AI-verified leads and emails to inventor@aios.is.
"""

import os
import sqlite3
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from dotenv import dotenv_values

# Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'leads_manifests', 'authoritative_storms.db')
DOTENV_PATH = os.path.join(BASE_DIR, '.env')
TARGET_EMAIL = "inventor@aios.is"

def export_new_leads_to_csv():
    """Extracts AI-Verified leads (Damage >= 5%) to a CSV file."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # We select c.rowid to track which ones we've notified
    query = """
    SELECT c.rowid, c.street_address, s.city, s.state, s.magnitude, c.damage_score, c.insurance_company, s.event_date
    FROM contacts c
    JOIN storms s ON c.event_id = s.id
    WHERE c.damage_score >= 5.0
    AND c.notified = 0
    ORDER BY c.damage_score DESC
    """
    
    c.execute(query)
    leads = c.fetchall()
    
    # USER RULE: Don't email new leads until at least 25 new ones are found
    if len(leads) < 25:
        print(f"[BATCHING] Currently {len(leads)}/25 high-damage leads found. Waiting for more discoveries...")
        conn.close()
        return None
        
    # Mark these as notified so they don't get sent twice
    row_ids = [r[0] for r in leads]
    c.executemany("UPDATE contacts SET notified = 1 WHERE rowid = ?", [(rid,) for rid in row_ids])
    conn.commit()
    conn.close()
        
    filename = f"forensic_leads_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    filepath = os.path.join(BASE_DIR, 'leads_manifests', filename)
    
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Street Address', 'City', 'State', 'Storm Magnitude', 'AI Damage Score %', 'Likely Insurance', 'Event Date'])
        # Strip the rowid from the exported CSV
        writer.writerows([r[1:] for r in leads])
        
    return filepath

def send_lead_dispatch():
    """Batches leads and sends to the inventor. Includes Emergency Individual Alerts for 40%+."""
    config = dotenv_values(DOTENV_PATH)
    smtp_server = config.get("SMTP_SERVER")
    smtp_port = config.get("SMTP_PORT", 587)
    smtp_user = config.get("SMTP_USER")
    smtp_pass = config.get("SMTP_PASS")
    
    # ── 1. EMERGENCY ALERT CHECK (>= 40% Damage) ──────────────────
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT c.rowid, c.street_address, s.city, s.state, c.damage_score FROM contacts c JOIN storms s ON c.event_id=s.id WHERE c.damage_score >= 40.0 AND c.notified = 0")
    emergency_targets = c.fetchall()
    
    if emergency_targets and all([smtp_server, smtp_user, smtp_pass]):
        for rid, addr, city, state, score in emergency_targets:
            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = TARGET_EMAIL
            msg['Subject'] = f"🚨 EMERGENCY: TOTAL LOSS DETECTED ({score}%)"
            body = f"Inventor,\n\nURGENT: The AI has verified a Category 5 Total Loss property.\n\nAddress: {addr}, {city}, {state}\nDamage: {score}%\n\nEngagement recommended immediately."
            msg.attach(MIMEText(body, 'plain'))
            
            try:
                server = smtplib.SMTP(smtp_server, int(smtp_port))
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
                server.quit()
                c.execute("UPDATE contacts SET notified = 1 WHERE rowid = ?", (rid,))
                conn.commit()
                print(f"[EMERGENCY] Instant Alert sent for Total Loss: {addr}")
            except Exception as e:
                print(f"[ERROR] Emergency Alert Failed: {e}")

    # ── 2. STANDARD BATCH CHECK (25 Lead Rule) ────────────────────
    csv_path = export_new_leads_to_csv()
    
    if not csv_path:
        # Either zero leads or under the 25-lead threshold
        return

    if not all([smtp_server, smtp_user, smtp_pass]):
        print(f"[NOTIFIER] CSV Created at {csv_path} - Configure SMTP in .env to enable email dispatch.")
        return

    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = TARGET_EMAIL
    msg['Subject'] = f"🚀 ROOF HUNTER: {datetime.now().strftime('%Y-%m-%d')} Lead Dispatch (25+ BATCH)"

    body = f"Inventor,\n\nAttached is a high-density forensic lead manifest from the AWS Discovery Node.\nWe have accumulated {csv_path.count('_')} leads for this dispatch.\n\nAll properties have been AI-verified with >= 5% damage markers.\n\nHappy Hunting."
    msg.attach(MIMEText(body, 'plain'))

    with open(csv_path, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(csv_path)}")
        msg.attach(part)

    try:
        server = smtplib.SMTP(smtp_server, int(smtp_port))
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
        server.quit()
        print(f"[SUCCESS] High-Volume Dispatch sent to {TARGET_EMAIL}")
    except Exception as e:
        print(f"[ERROR] Email Dispatch Failed: {e}")

if __name__ == "__main__":
    send_lead_dispatch()
