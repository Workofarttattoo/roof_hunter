import os
import sqlite3
import pandas as pd
import logging
from src.imagery_fetcher import GoogleStaticImagery
from src.street_view_scraper import StreetViewScraper
from src.contact_finder import SkipTracer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def harvest_national_visuals():
    logger.info("=== STARTING NATIONAL VISUAL HARVEST (360-DEGREE PROOF) ===")
    
    conn = sqlite3.connect('authoritative_storms.db')
    
    # Target top most hit states
    target_states = ('TEXAS', 'ALABAMA', 'ILLINOIS', 'KANSAS', 'OKLAHOMA')
    
    # Select the top 10 most severe 2026 events 
    query = """
    SELECT id, state, city, latitude, longitude, event_date, magnitude 
    FROM storms 
    WHERE state IN (?, ?, ?, ?, ?)
    AND event_date >= '2026-01-01'
    ORDER BY magnitude DESC
    LIMIT 25
    """
    
    df = pd.read_sql_query(query, conn, params=target_states)
    conn.close()
    
    if df.empty:
        logger.warning("No recent massive storms found.")
        return

    fetcher = GoogleStaticImagery()
    street_view = StreetViewScraper()
    tracer = SkipTracer(mock_mode=False)
    
    results = []

    logger.info(f"Harvesting Dual-Perspective proof for {len(df)} National Assets...")

    for _, row in df.iterrows():
        logger.info(f"Proofing: {row['city']}, {row['state']} ({row['magnitude']} mph)...")
        
        # A. Fetch Satellite
        sat_path = fetcher.fetch_roof_image(row['latitude'], row['longitude'], f"hail_target_{row['id']}")
        
        # B. Fetch Street View
        street_path = street_view.fetch_street_view(row['latitude'], row['longitude'], f"hail_target_{row['id']}")
        
        # C. Ensure Lead exists in Contacts
        tracer.trace_storm_event(row['id'])
        
        results.append({
            "State": row['state'],
            "City": row['city'],
            "Date": row['event_date'],
            "Satellite": "✅ SECURED" if sat_path else "❌ FAILED",
            "Street View": "✅ SECURED" if street_path else "❌ FAILED"
        })

    if results:
        final_df = pd.DataFrame(results)
        print("\n" + "="*80)
        print("VISUAL EVIDENCE MANIFEST: READY FOR DASHBOARD")
        print("="*80)
        print(final_df.to_markdown(index=False))
        print("="*80)
    else:
        logger.warning("No visual proof could be secured.")

if __name__ == "__main__":
    harvest_national_visuals()
