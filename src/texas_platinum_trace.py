import sqlite3
import pandas as pd
import logging
from src.contact_finder import SkipTracer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def trace_texas_targets():
    logger.info("=== INITIATING TEXAS HIGH-INTENSITY SKIP TRACE ===")
    
    conn = sqlite3.connect("authoritative_storms.db")
    query = """
    SELECT id, city, state, event_date, magnitude, median_home_value
    FROM storms
    WHERE state = 'TEXAS' 
    AND event_date LIKE '2026-04%'
    AND magnitude > 3.0
    ORDER BY magnitude DESC
    LIMIT 50
    """
    targets_df = pd.read_sql_query(query, conn)
    conn.close()

    if targets_df.empty:
        logger.warning("No qualifying Texas targets found.")
        return

    tracer = SkipTracer(mock_mode=False)
    results = []

    for _, row in targets_df.iterrows():
        logger.info(f"Tracing Owner for {row['city']}, TX ({row['magnitude']}\" storm)...")
        try:
            contact = tracer.trace_storm_event(row['id'])
            
            if contact:
                results.append({
                    "Date": row['event_date'],
                    "City": row['city'],
                    "Intensity": row['magnitude'],
                    "Valuation": f"${row['median_home_value']:,.0f}",
                    "Owner": contact.get('owner', 'STORM IMPACT DETECTED'),
                    "Phone": contact.get('phone', 'VERIFYING...'),
                    "Address": contact.get('address', f"{row['city']}, TX"),
                    "Status": "OUTREACH READY"
                })
            else:
                # Fallback for failed traces
                results.append({
                    "Date": row['event_date'],
                    "City": row['city'],
                    "Intensity": row['magnitude'],
                    "Valuation": f"${row['median_home_value']:,.0f}",
                    "Owner": "STORM IMPACT DETECTED",
                    "Phone": "VERIFYING...",
                    "Address": f"{row['city']}, TX",
                    "Status": "OWNER UNRESOLVED"
                })
        except Exception as e:
            logger.error(f"Skip Trace Failed for {row['city']}: {e}")

    if results:
        final_df = pd.DataFrame(results)
        output_file = "texas_platinum_contacts.csv"
        final_df.to_csv(output_file, index=False)
        print("\n" + "#"*100)
        print("TEXAS PLATINUM CONTACT MANIFEST: GENERATED")
        print("#"*100)
        print(final_df.to_markdown(index=False))
        print(f"\nFinal export saved to: {output_file}")

if __name__ == "__main__":
    trace_texas_targets()
