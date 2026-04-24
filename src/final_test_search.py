import sqlite3
import pandas as pd
import os
from datetime import datetime, timedelta
from src.contact_finder import SkipTracer

DB_PATH = '/Users/noone/.gemini/antigravity/scratch/roof_hunter/authoritative_storms.db'

def run_comprehensive_search():
    if not os.path.exists(DB_PATH):
        print("Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    
    # 1. Search for roof damage in the last 3 months
    # We include Hail and Thunderstorm Wind (high magnitude) as our damage vectors.
    print(f"--- SEARCHING DATALAKE: ROOF DAMAGE (HAIL/SEVERE WIND) | LAST 3 MONTHS ---")
    
    query = """
    SELECT id, event_date, event_type, magnitude, city, state, zipcode, median_home_value, median_household_income
    FROM storms 
    WHERE (event_type = 'Hail' OR event_type = 'Thunderstorm Wind' OR event_type = 'Tornado')
    AND event_date >= date('now', '-3 months')
    ORDER BY magnitude DESC
    LIMIT 10
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("No matches found in the last 3 months. (Note: Data might be pending ingestion).")
        # Fallback to general latest for the test
        print("Falling back to All-Time Latest matches for demonstration...")
        query = """
        SELECT id, event_date, event_type, magnitude, city, state, zipcode, median_home_value, median_household_income
        FROM storms 
        ORDER BY event_date DESC, magnitude DESC
        LIMIT 5
        """
        df = pd.read_sql_query(query, conn)

    if df.empty:
        print("No records found in database.")
        conn.close()
        return

    # 2. Enrich with CONTACTS (Real-time Skip Tracing)
    tracer = SkipTracer(mock_mode=False) # Respecting the RentCast API Key
    
    enriched_results = []
    
    print("\nTracing homeowner identities for high-priority targets...")
    for _, row in df.iterrows():
        event_id = row['id']
        
        # Check if already traced
        c = conn.cursor()
        c.execute("SELECT homeowner_name, phone_number, street_address FROM contacts WHERE event_id = ?", (event_id,))
        contact = c.fetchone()
        
        if not contact:
            # Trigger real-time skip trace
            trace_res = tracer.trace_storm_event(event_id)
            if trace_res:
                owner = trace_res['owner']
                phone = trace_res['phone']
                addr = trace_res['address']
            else:
                owner, phone, addr = "Unknown", "N/A", "N/A"
        else:
            owner, phone, addr = contact[0], contact[1], contact[2]
            
        res = {
            "Date": row['event_date'],
            "Type": row['event_type'],
            "Mag": row['magnitude'],
            "City": row['city'] or "Unknown",
            "Zip": row['zipcode'] or "Unknown",
            "HomeValue": f"${row['median_home_value']:,.0f}" if not pd.isna(row['median_home_value']) else "N/A",
            "Owner": owner,
            "Phone": phone,
            "Address": addr
        }
        enriched_results.append(res)
        
    conn.close()
    
    # 3. Output Final Lead Sheet
    final_df = pd.DataFrame(enriched_results)
    
    print("\n" + "="*100)
    print("ROOF HUNTER INTELLIGENCE: HIGH-VALUE LEADS GENERATED")
    print("="*100)
    print(final_df.to_markdown(index=False))
    print("="*100)

if __name__ == "__main__":
    run_comprehensive_search()
