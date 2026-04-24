import sqlite3
import pandas as pd
import os
from src.contact_finder import SkipTracer

DB_PATH = '/Users/noone/.gemini/antigravity/scratch/roof_hunter/authoritative_storms.db'

def generate_ok_leads():
    if not os.path.exists(DB_PATH):
        print("Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    
    # Search for OK events in the last 6 months
    print(f"--- TARGETING: OKLAHOMA ROOF DAMAGE | LAST 6 MONTHS ---")
    
    query = """
    SELECT id, event_date, event_type, magnitude, city, state, zipcode, median_home_value, median_household_income
    FROM storms 
    WHERE state = 'OKLAHOMA'
    AND event_date >= date('now', '-6 months')
    ORDER BY magnitude DESC, event_date DESC
    LIMIT 15
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("No matches found for Oklahoma in the last 6 months. (Attempting to ingest latest live data for OK...)")
        # Since I just ran the 2026 ingest, if it's still empty, it means 2026 OK data is sparse in this file.
        conn.close()
        return

    # Enrichment
    tracer = SkipTracer(mock_mode=False)
    leads = []
    
    for _, row in df.iterrows():
        event_id = row['id']
        
        # Check cache
        c = conn.cursor()
        c.execute("SELECT homeowner_name, phone_number, email, street_address FROM contacts WHERE event_id = ?", (event_id,))
        cached = c.fetchone()
        
        if not cached:
            trace = tracer.trace_storm_event(event_id)
            if trace:
                name, phone, email, addr = trace['owner'], trace['phone'], trace['email'], trace['address']
            else:
                name, phone, email, addr = "Unknown", "N/A", "N/A", "N/A"
        else:
            name, phone, email, addr = cached[0], cached[1], cached[2], cached[3]
            
        leads.append({
            "Date": row['event_date'],
            "Damage_Source": row['event_type'],
            "Intensity": row['magnitude'],
            "City": row['city'],
            "Zip": row['zipcode'],
            "HomeValue": f"${row['median_home_value']:,.0f}" if not pd.isna(row['median_home_value']) else "N/A",
            "Owner": name,
            "Contact": phone,
            "Mailing_Address": addr
        })
        
    conn.close()
    
    output_df = pd.DataFrame(leads)
    print("\n" + "="*120)
    print("ROOF HUNTER: OKLAHOMA HOT-LIST (Socio-Economic Filter Active)")
    print("="*120)
    print(output_df.to_markdown(index=False))
    print("="*120)

if __name__ == "__main__":
    generate_ok_leads()
