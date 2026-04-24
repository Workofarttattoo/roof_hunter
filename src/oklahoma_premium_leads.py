import sqlite3
import pandas as pd
import os
from src.contact_finder import SkipTracer

DB_PATH = '/Users/noone/.gemini/antigravity/scratch/roof_hunter/authoritative_storms.db'

def get_premium_ok_leads():
    if not os.path.exists(DB_PATH):
        print("Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    
    # Target: OKLAHOMA, Home Value > $100,000, Hail/Wind/Tornado
    print(f"--- QUERYING: OKLAHOMA PREMIUM LEADS ($100,000+ HOME VALUE) ---")
    
    query = """
    SELECT id, event_date, event_type, magnitude, city, zipcode, median_home_value, median_household_income
    FROM storms 
    WHERE state = 'OKLAHOMA'
    AND median_home_value >= 100000
    ORDER BY event_date DESC, magnitude DESC
    LIMIT 10
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No matches found for homes over $100k in Oklahoma.")
        return

    # Skip Tracing the top 5
    tracer = SkipTracer(mock_mode=False)
    results = []
    
    print("\nTracing homeowner identities for top 5 premium OK targets...")
    for _, row in df.head(5).iterrows():
        event_id = row['id']
        trace = tracer.trace_storm_event(event_id)
        
        if trace:
            results.append({
                "Date": row['event_date'],
                "Source": row['event_type'],
                "Intensity": row['magnitude'],
                "City": row['city'],
                "Home_Value": f"${row['median_home_value']:,.0f}",
                "Income": f"${row['median_household_income']:,.0f}",
                "Owner": trace['owner'],
                "Contact": trace['phone'],
                "Address": trace['address']
            })

    output_df = pd.DataFrame(results)
    
    print("\n" + "="*120)
    print("ROOF HUNTER PREMIUM OUTPUT: OKLAHOMA HIGH-VALUE INSURANCE TARGETS")
    print("="*120)
    print(output_df.to_markdown(index=False))
    print("="*120)

if __name__ == "__main__":
    get_premium_ok_leads()
