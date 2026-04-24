import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def enrich_manifest():
    logger.info("Enriching April Manifest with precise GPS coordinates...")
    
    try:
        df = pd.read_csv('april_100_platinum_leads.csv')
    except:
        logger.error("Could not find april_100_platinum_leads.csv")
        return

    conn = sqlite3.connect("authoritative_storms.db")
    
    enriched_data = []
    for _, row in df.iterrows():
        # Quick lookup by city/state/date
        query = "SELECT latitude, longitude FROM storms WHERE city=? AND state=? AND event_date=? LIMIT 1"
        res = conn.execute(query, (row['City'], row['State'], row['Date'])).fetchone()
        
        if res:
            row['latitude'] = res[0]
            row['longitude'] = res[1]
        enriched_data.append(row)
        
    conn.close()
    
    enriched_df = pd.DataFrame(enriched_data)
    enriched_df.to_csv('april_100_platinum_leads_enriched.csv', index=False)
    logger.info("Enrichment COMPLETE. Enriched file saved.")

if __name__ == "__main__":
    enrich_manifest()
