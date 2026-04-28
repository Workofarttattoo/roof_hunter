# apollo_enrichment_manifest.py
import sqlite3
import pandas as pd
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'apollo_export_ready_leads.csv')

def generate_apollo_export():
    """
    Generates a CSV formatted for Apollo.io 'Bulk Search' or 'Enrichment' workflows.
    Includes Company Name (if available), Name, and Full Address.
    """
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT homeowner_name as First_Last, street_address as Address, city, state, zip_code as Zip
    FROM contacts
    JOIN storms ON contacts.event_id = storms.id
    WHERE contacts.qualification_status = 'QUALIFIED'
    AND contacts.phone_number IS NULL;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df.to_csv(OUTPUT_PATH, index=False)
        print(f"✅ APOLLO ENRICHMENT MANIFEST CREATED: {OUTPUT_PATH}")
        print(f"Ready to upload {len(df)} leads to Apollo.io enrichment service.")
    else:
        print("No qualified leads require enrichment today.")

if __name__ == "__main__":
    generate_apollo_export()
