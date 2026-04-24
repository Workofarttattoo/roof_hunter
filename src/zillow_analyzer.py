import pandas as pd
import numpy as np
import os
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ZillowIntelligence:
    """
    Ingests Zillow Home Value Index (ZHVI) datasets to identify
    neighborhoods with high equity growth and financial liquiditiy.
    """
    def __init__(self, db_path='authoritative_storms.db'):
        self.db_path = db_path

    def ingest_zhvi(self, csv_path):
        """
        Parses Zillow ZHVI (Smoothed, Seasonally Adjusted) CSV.
        Expected format: ZipCode as RegionName, followed by YYYY-MM-DD columns.
        """
        if not os.path.exists(csv_path):
            logger.error(f"Zillow source file not found: {csv_path}")
            return False

        logger.info(f"Ingesting Zillow ZHVI data from {csv_path}...")
        df = pd.read_csv(csv_path)
        
        # Identify date columns (usually the last ~240 columns)
        date_cols = [col for col in df.columns if '-' in col]
        if not date_cols:
            logger.error("No date columns found in Zillow CSV. Check format.")
            return False

        # Calculate 3-month and 12-month growth rates
        latest_month = date_cols[-1]
        three_months_ago = date_cols[-4] if len(date_cols) >= 4 else date_cols[0]
        
        df['current_val'] = df[latest_month]
        df['prev_val'] = df[three_months_ago]
        df['growth_3m'] = ((df['current_val'] / df['prev_val']) - 1) * 100
        
        # We assume RegionName is the ZIP Code for 'Zip' geography files
        # Or RegionName is the City for 'City' geography files
        # If StateName is present, we filter for meaningful data
        processed_data = df[['RegionName', 'StateName', 'current_val', 'growth_3m']].copy()
        processed_data.rename(columns={'RegionName': 'zipcode', 'StateName': 'state', 'current_val': 'zhvi'}, inplace=True)
        
        self._update_datalake(processed_data)
        return True

    def _update_datalake(self, df):
        """
        Enriches the storm datalake with Zillow growth metrics.
        """
        conn = sqlite3.connect(self.db_path)
        
        # Create a zillow_metrics table for high-speed joining
        df.to_sql('zillow_metrics', conn, if_exists='replace', index=False)
        
        # Update existing storms with the latest ZHVI if zip matches
        query = """
        UPDATE storms
        SET median_home_value = (
            SELECT zhvi FROM zillow_metrics 
            WHERE zillow_metrics.zipcode = storms.zipcode 
            LIMIT 1
        )
        WHERE EXISTS (
            SELECT 1 FROM zillow_metrics WHERE zillow_metrics.zipcode = storms.zipcode
        )
        """
        conn.execute(query)
        conn.commit()
        conn.close()
        logger.info("Datalake successfully enriched with Zillow Value Indices.")

    def get_equity_surge_leads(self, min_growth=2.0):
        """
        Identifies storm-hit properties in neighborhoods where 
        values increased by at least X% in the last 3 months.
        """
        conn = sqlite3.connect(self.db_path)
        query = f"""
        SELECT s.*, z.growth_3m
        FROM storms s
        JOIN zillow_metrics z ON s.zipcode = z.zipcode
        WHERE z.growth_3m >= {min_growth}
        ORDER BY z.growth_3m DESC, s.magnitude DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

if __name__ == "__main__":
    # Example usage:
    # intel = ZillowIntelligence()
    # intel.ingest_zhvi('Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv')
    pass
