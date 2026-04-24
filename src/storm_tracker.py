import logging
import time
import requests
import re
import os
import pandas as pd

logger = logging.getLogger(__name__)

class StormTracker:
    def __init__(self):
        self.year = 2024 # We pull recent historical for Gather/Training Mode
        self.cache_dir = os.path.join(os.path.dirname(__file__), '..', 'data_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cached_file = os.path.join(self.cache_dir, f"storm_events_{self.year}.csv.gz")
        
    def download_ncei_data(self):
        index_url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
        try:
            res = requests.get(index_url)
            # Find the most recent details file for the specific year
            match = re.search(rf'href="(StormEvents_details-ftp_v1.0_d{self.year}_c\d+\.csv\.gz)"', res.text)
            if not match:
                logger.error("Could not find NCEI URL for year " + str(self.year))
                return None
            filename = match.group(1)
            download_url = index_url + filename
            
            logger.info(f"Downloading NCEI dataset: {filename} (This may take a minute...)")
            file_res = requests.get(download_url)
            with open(self.cached_file, 'wb') as f:
                f.write(file_res.content)
            logger.info("Download complete.")
            return self.cached_file
        except Exception as e:
            logger.error(f"Failed to fetch NCEI data: {e}")
            return None

    def check_for_hail(self, qualifier_inches=1.5, location="Oklahoma County, OK"):
        """
        Parses the NOAA NCEI dataset for historical hail swaths hitting our metrics.
        """
        logger.info(f"Querying NOAA NCEI Historical Database for hail >= {qualifier_inches} inches near {location}")
        
        if not os.path.exists(self.cached_file):
            if not self.download_ncei_data():
                return None
        else:
            logger.info("Using cached NCEI CSV data.")
            
        try:
            # Load the gzipped CSV into pandas
            df = pd.read_csv(self.cached_file, compression='gzip', low_memory=False)
            
            # Filter for severe hail in Oklahoma
            hail_df = df[(df['EVENT_TYPE'] == 'Hail') & 
                         (df['MAGNITUDE'] >= qualifier_inches) &
                         (df['STATE'] == 'OKLAHOMA')]
                         
            if hail_df.empty:
                return None
                
            # Grab the largest incident on record for this chunk
            top_event = hail_df.loc[hail_df['MAGNITUDE'].idxmax()]
            
            lat = top_event.get('BEGIN_LAT')
            lon = top_event.get('BEGIN_LON')
            if pd.isna(lat) or pd.isna(lon):
                 lat, lon = 35.4676, -97.5164 # Fallback
            
            date_str = str(top_event.get('BEGIN_YEARMONTH'))
            day_str = str(top_event.get('BEGIN_DAY')).zfill(2)
            event_date = f"{date_str[:4]}-{date_str[4:]}-{day_str}" if date_str != 'nan' else "2024-05-19"
            
            return {
                "date": event_date,
                "hail_size_inches": top_event['MAGNITUDE'],
                "impact_polygon": [lon, lat, lon+0.1, lat+0.1]
            }
            
        except Exception as e:
            logger.error(f"Data parse error: {e}")
            return None
