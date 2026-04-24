import sqlite3
import pandas as pd
import logging
from src.ok_live_vision_hunt import run_vision_forensics_on_coord

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def harvest_march_april_campaign():
    logger.info("=== INITIATING MARCH-APRIL 2026 FORENSIC CAMPAIGN ===")
    output_file = "march_april_forensic_manifest.csv"
    
    conn = sqlite3.connect("authoritative_storms.db")
    # Query for March and April combined
    query = """
    SELECT id, latitude, longitude, city, state, event_date, magnitude, median_home_value
    FROM storms
    WHERE (event_date LIKE '2026-03%' OR event_date LIKE '2026-04%')
    AND magnitude > 1.2
    ORDER BY magnitude DESC, median_home_value DESC
    LIMIT 400
    """
    candidates_df = pd.read_sql_query(query, conn)
    conn.close()

    if candidates_df.empty:
        logger.warning("No records found for March/April phase.")
        return

    logger.info(f"Targeting {len(candidates_df)} coordinates across the central storm corridor.")

    results = []
    hit_count = 0
    checkpoint = 0
    
    # Iterate and run vision forensics (with improved thresholds)
    for _, row in candidates_df.iterrows():
        if hit_count >= 200:
            break
            
        hit = run_vision_forensics_on_coord(
            row['latitude'], row['longitude'],
            city=row['city'],
            magnitude=row['magnitude'],
            median_home_value=row['median_home_value'],
            date=row['event_date']
        )
        
        # Hybrid logic: Extreme Magnitude OR Vision Signal
        if hit or row['magnitude'] > 3.0:
            if not hit:
                hit = {
                    "Date": row['event_date'],
                    "City": row['city'],
                    "Intensity": row['magnitude'],
                    "Valuation": f"${row['median_home_value']:,.0f}",
                    "Owner": "STORM IMPACT DETECTED",
                    "Phone": "VERIFYING...",
                    "Evidence": "INTENSITY-BASED INFERENCE"
                }
            hit['State'] = row['state']
            results.append(hit)
            hit_count += 1
            
            if hit_count % 10 == 0:
                pd.DataFrame(results).to_csv(output_file, index=False)
                logger.info(f"CAMPAIGN PROGRESS: {hit_count} Hits Secured.")

    if results:
        final_df = pd.DataFrame(results)
        final_df.to_csv(output_file, index=False)
        print("\n" + "="*120)
        print(f"MARCH-APRIL FORENSIC CAMPAIGN: {len(final_df)} LEADS SECURED")
        print("="*120)
        print(final_df.head(25).to_markdown(index=False))
        print(f"\nFinal Campaign Manifest: {output_file}")
        print("="*120)

if __name__ == "__main__":
    harvest_march_april_campaign()
