import sqlite3
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def inject_gps_data():
    logger.info("=== INJECTING GPS COORDINATES INTO MASTER MANIFEST ===")
    
    # Corrected path due to reorganization
    master_path = 'leads_manifests/ROOF_HUNTER_MASTER_MANIFEST_2026.csv'
    db_path = 'leads_manifests/authoritative_storms.db'
    
    if not os.path.exists(master_path):
        logger.error(f"{master_path} not found.")
        return

    df = pd.read_csv(master_path)
    conn = sqlite3.connect(db_path)
    
    enriched_results = []
    
    for _, row in df.iterrows():
        # Query for precise Lat/Lon
        query = "SELECT latitude, longitude FROM storms WHERE city=? AND state=? AND event_date=? LIMIT 1"
        res = conn.execute(query, (row['City'], row['State'], row['Date'])).fetchone()
        
        if res:
            row['latitude'] = res[0]
            row['longitude'] = res[1]
        
        # If Address is missing, we ensure we have the target city/state as a fallback
        if pd.isna(row.get('Address')):
            row['Address'] = f"{row['City']}, {row['State']}"
            
        enriched_results.append(row)
        
    conn.close()
    
    final_df = pd.DataFrame(enriched_results)
    
    # Final column organization for GTM readability
    cols = ['Date', 'City', 'State', 'latitude', 'longitude', 'Intensity', 'Valuation', 'Owner', 'Phone', 'Address']
    existing_cols = [c for c in cols if c in final_df.columns]
    final_df = final_df[existing_cols]
    
    final_df.to_csv(master_path, index=False)
    logger.info(f"Injecton COMPLETE. {len(final_df)} leads updated with GPS data.")

if __name__ == "__main__":
    inject_gps_data()
