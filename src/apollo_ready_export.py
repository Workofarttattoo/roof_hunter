import sqlite3
import csv
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'leads_manifests', 'authoritative_storms.db')
OUTPUT_FILE = os.path.join(BASE_DIR, 'leads_manifests', 'apollo_import_ready_leads.csv')

def generate_apollo_export():
    print("=== GENERATING APOLLO.IO ENRICHMENT MANIFEST ===")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Query for leads that have owner names but need phone/email
    query = """
    SELECT 
        homeowner_name as "Full Name",
        street_address as "Street Address",
        storms.city as "City",
        storms.state as "State",
        contacts.zip_code as "Zip Code",
        storms.magnitude as "Damage Magnitude",
        insurance_company as "Insurance Carrier",
        'Homeowner' as "Job Title"
    FROM contacts
    JOIN storms ON contacts.event_id = storms.id
    WHERE homeowner_name NOT IN ('Homeowner', 'DEEP SEARCH REQ', 'STORM IMPACT DETECTED')
    AND homeowner_name IS NOT NULL
    """
    
    c.execute(query)
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    
    if not rows:
        print("No enriched leads found for export.")
        return

    # Apollo recommends splitting Full Name into First and Last if possible
    for row in rows:
        name_parts = row["Full Name"].split(" ")
        row["First Name"] = name_parts[0]
        row["Last Name"] = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
        row["Company"] = "Residential Property"
    
    fieldnames = ["First Name", "Last Name", "Street Address", "City", "State", "Zip Code", "Company", "Job Title", "Damage Magnitude", "Insurance Carrier"]
    
    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
        
    print(f"✅ SUCCESS: {len(rows)} leads formatted for Apollo.io enrichment.")
    print(f"File location: {OUTPUT_FILE}")
    print("\nNext Step: Upload this CSV to Apollo.io -> Search -> Import for phone/email discovery.")

if __name__ == "__main__":
    generate_apollo_export()
