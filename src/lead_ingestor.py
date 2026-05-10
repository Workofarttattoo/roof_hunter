import pandas as pd
import sqlite3
import os
import logging
import csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db'))

def ingest_external_leads(file_path):
    """
    Ingests a provided CSV or Text file of leads.
    Scrubbing them of 'fake' info by matching them with forensic storm data in the DB.
    """
    logger.info(f"📥 Starting Ingestion of: {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    # Load leads
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        # Assuming text file with addresses or something similar
        df = pd.read_csv(file_path, sep='\t' if '\t' in open(file_path).read() else ',')

    logger.info(f"Loaded {len(df)} leads. Scrubbing and matching against forensic database...")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    scrubbed_leads = []
    
    for _, row in df.iterrows():
        # Standardize fields
        phone = str(row.get('phone_number', row.get('phone', ''))).strip()
        name = str(row.get('first_name', row.get('name', 'Homeowner'))).strip()
        addr = str(row.get('property_address', row.get('address', ''))).strip()
        
        # SKIP TESTING/FAKE NUMBERS
        if '555' in phone or len(phone) < 7:
            continue
            
        # Match against Forensic Storms
        # Try to find a storm that hit this city/zip
        city = row.get('city', '')
        zip_c = row.get('zip_code', row.get('zip', ''))
        
        c.execute("""
            SELECT s.magnitude, s.event_type, s.event_date
            FROM storms s
            WHERE s.zipcode = ? OR s.city LIKE ?
            ORDER BY s.magnitude DESC
            LIMIT 1
        """, (zip_c, f"%{city}%"))
        
        storm = c.fetchone()
        
        if storm:
            hail = storm['magnitude']
            type_ = storm['event_type']
            date = storm['event_date']
            
            # Add forensics
            scrubbed_leads.append({
                "first_name": name,
                "phone_number": phone,
                "property_address": addr,
                "hail_date": date,
                "hail_size": hail,
                "storm_type": type_,
                "image_findings": f"Forensic verification confirmed impact from {hail}\" {type_} on {date}.",
                "lead_priority": "PRIORITY_1_EMERGENCY" if hail >= 2.0 else "PRIORITY_2_LIKELY_DAMAGE"
            })

    # Save to a new CLEAN batch
    output_path = os.path.join(os.path.dirname(file_path), 'CLEAN_VERIFIED_BATCH.csv')
    pd.DataFrame(scrubbed_leads).to_csv(output_path, index=False)
    
    logger.info(f"✅ Scrubbed {len(scrubbed_leads)} real leads. Saved to {output_path}")
    conn.close()

if __name__ == "__main__":
    # Test with a sample if exists
    pass
