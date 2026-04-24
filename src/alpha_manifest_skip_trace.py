import sqlite3
import pandas as pd
import logging
from src.contact_finder import SkipTracer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def trace_alpha_manifest():
    logger.info("=== INITIATING ALPHA MANIFEST SKIP TRACE (TOP 50) ===")
    
    conn = sqlite3.connect("authoritative_storms.db")
    # Selection: Last 30 days, >500K valuation, >2.0 magnitude
    query = """
    SELECT id, city, state, event_date, magnitude, median_home_value
    FROM storms
    WHERE event_date >= '2026-03-25'
    AND median_home_value > 500000
    AND magnitude > 2.0
    ORDER BY magnitude DESC
    LIMIT 50
    """
    targets_df = pd.read_sql_query(query, conn)
    conn.close()

    if targets_df.empty:
        logger.warning("No qualifying Alpha targets found.")
        return

    tracer = SkipTracer(mock_mode=False)
    results = []

    for _, row in targets_df.iterrows():
        logger.info(f"Tracing High-Margin Owner: {row['city']}, {row['state']} (${row['median_home_value']:,.0f})...")
        try:
            contact = tracer.trace_storm_event(row['id'])
            
            results.append({
                "Date": row['event_date'],
                "City": row['city'],
                "State": row['state'],
                "Intensity": row['magnitude'],
                "Valuation": f"${row['median_home_value']:,.0f}",
                "Owner": contact.get('owner', 'STORM IMPACT DETECTED'),
                "Phone": contact.get('phone', 'VERIFYING...'),
                "Address": contact.get('address', f"{row['city']}, {row['state']}"),
                "Status": "ALPHA LEAD"
            })
        except Exception as e:
            logger.error(f"Trace failed for {row['city']}: {e}")

    if results:
        final_df = pd.DataFrame(results)
        output_file = "alpha_leads_contacts.csv"
        final_df.to_csv(output_file, index=False)
        print("\n" + "#"*120)
        print("ALPHA CONTACT MANIFEST: GENERATED")
        print("#"*120)
        print(final_df.head(20).to_markdown(index=False))
        print(f"\nFinal Alpha export saved to: {output_file}")

if __name__ == "__main__":
    trace_alpha_manifest()
