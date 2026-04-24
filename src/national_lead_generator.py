import os
import sqlite3
import pandas as pd
import logging
from src.contact_finder import SkipTracer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_multi_state_manifest():
    logger.info("=== GENERATING 2026 NATIONAL ROOFING LEAD MANIFEST ===")
    
    conn = sqlite3.connect('authoritative_storms.db')
    
    # Target top 5 most hit states: TX, AL, IL, KS, OK
    target_states = ('TEXAS', 'ALABAMA', 'ILLINOIS', 'KANSAS', 'OKLAHOMA')
    
    # Select the top 10 most severe events per state to get a broad multi-state list
    query = """
    SELECT id, state, city, event_date, magnitude, median_home_value 
    FROM storms 
    WHERE state IN (?, ?, ?, ?, ?)
    AND event_date >= '2026-01-01'
    ORDER BY magnitude DESC
    LIMIT 50
    """
    
    df = pd.read_sql_query(query, conn, params=target_states)
    conn.close()
    
    if df.empty:
        logger.warning("No recent massive storms found in target states.")
        return

    tracer = SkipTracer(mock_mode=False)
    manifest = []

    logger.info(f"Initiating Bulk Skip-Trace on {len(df)} National Footprints...")

    for _, row in df.iterrows():
        # Trace the lead
        contact = tracer.trace_storm_event(row['id'])
        
        if contact:
            manifest.append({
                "State": row['state'],
                "Date": row['event_date'],
                "City": row['city'],
                "Intensity": f"{row['magnitude']} mph",
                "Home Value": f"${row['median_home_value']:,.0f}",
                "Client Name": contact['owner'],
                "Contact Info": f"{contact['phone']} | {contact['email']}",
                "Address": contact['address']
            })

    if manifest:
        final_df = pd.DataFrame(manifest)
        
        # Save to Artifact/CSV for user consumption
        output_file = "national_lead_manifest_2026.csv"
        final_df.to_csv(output_file, index=False)
        
        print("\n" + "="*140)
        print("THE NATIONAL MANIFEST: TOP 50 HIGH-VALUE 2026 LEADS")
        print("="*140)
        print(final_df[['State', 'Date', 'City', 'Intensity', 'Client Name', 'Address']].to_markdown(index=False))
        print("="*140)
        logger.info(f"Full manifest exported to {output_file}")
    else:
        logger.warning("No contacts could be recovered for the selected footprints.")

if __name__ == "__main__":
    generate_multi_state_manifest()
