import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_january_data():
    logger.info("Ingesting January 2026 Supplemental NOAA Data...")
    
    # Read the TSV
    try:
        df = pd.read_csv("/tmp/storm_ingestion.tsv", sep="\t")
        
        # Map columns to DB schema
        # DB: event_date, event_type, state, latitude, longitude, magnitude, city, ...
        # TSV: BEGIN_DATE_TIME, EVENT_TYPE, STATE, BEGIN_LAT, BEGIN_LON, MAGNITUDE, BEGIN_LOCATION
        
        ingest_df = pd.DataFrame()
        ingest_df['event_date'] = df['BEGIN_DATE_TIME']
        ingest_df['event_type'] = df['EVENT_TYPE']
        ingest_df['state'] = df['STATE']
        ingest_df['latitude'] = df['BEGIN_LAT']
        ingest_df['longitude'] = df['BEGIN_LON']
        ingest_df['magnitude'] = df['MAGNITUDE']
        ingest_df['city'] = df['BEGIN_LOCATION']
        
        conn = sqlite3.connect("authoritative_storms.db")
        ingest_df.to_sql("storms", conn, if_exists="append", index=False)
        conn.close()
        
        logger.info(f"Successfully ingested {len(ingest_df)} new records.")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")

if __name__ == "__main__":
    ingest_january_data()
