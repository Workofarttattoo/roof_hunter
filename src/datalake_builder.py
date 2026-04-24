import os
import sqlite3
import pandas as pd
import requests
import re
import logging
from bs4 import BeautifulSoup
from uszipcode import SearchEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatalakeBuilder:
    def __init__(self, year=None):
        # Default to 2026 or latest live year
        self.year = year if year else 2026
        self.db_path = os.path.join(os.path.dirname(__file__), '..', 'authoritative_storms.db')
        self.cache_dir = os.path.join(os.path.dirname(__file__), '..', 'data_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        self.search = SearchEngine() # Load uszipcode engine
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS storms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_date TEXT,
                event_type TEXT,
                state TEXT,
                latitude REAL,
                longitude REAL,
                magnitude REAL,
                zipcode TEXT,
                city TEXT,
                county TEXT,
                population INTEGER,
                population_density REAL,
                median_home_value REAL,
                median_household_income REAL
            )
        ''')
        conn.commit()
        conn.close()

    def download_ncei_data(self):
        index_url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
        try:
            res = requests.get(index_url)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Find all links to "details" files
            links = []
            for a in soup.find_all('a', href=True):
                if 'StormEvents_details-ftp_v1.0_d' in a['href']:
                    links.append(a['href'])
                    
            if not links:
                logger.error("Could not scrape any CSV files from NCEI.")
                return None
                
            # If user wants a specific year, filter for it, otherwise grab the absolute latest year (2026/2025)
            if self.year:
                year_links = [l for l in links if f"_d{self.year}_" in l]
                if not year_links:
                    logger.warning(f"Could not find live data for {self.year}. Defaulting to absolute newest...")
                    target_file = sorted(links)[-1]
                else:
                    target_file = sorted(year_links)[-1]
            else:
                target_file = sorted(links)[-1]

            download_url = index_url + target_file
            cached_csv = os.path.join(self.cache_dir, target_file)
            
            if os.path.exists(cached_csv):
                 logger.info(f"Using already downloaded live dataset: {target_file}")
                 return cached_csv
                 
            logger.info(f"Downloading live NCEI datalake: {target_file}")
            file_res = requests.get(download_url)
            with open(cached_csv, 'wb') as f:
                f.write(file_res.content)
            logger.info("Download complete.")
            return cached_csv
        except Exception as e:
            logger.error(f"Failed to fetch NCEI data via BeautifulSoup: {e}")
            return None

    def build_lake(self):
        logger.info("Starting Authoritative Datalake Build Process...")
        csv_path = self.download_ncei_data()
        if not csv_path:
            logger.error("No CSV source to parse.")
            return
            
        # 1. Load CSV
        logger.info("Parsing NOAA bulk data...")
        df = pd.read_csv(csv_path, compression='gzip', low_memory=False)
        
        # 2. Filter for significant roof-damaging events
        target_events = ['Hail', 'Thunderstorm Wind', 'Tornado']
        filtered = df[(df['EVENT_TYPE'].isin(target_events)) & (df['BEGIN_LAT'].notna()) & (df['BEGIN_LON'].notna())]
        
        logger.info(f"Filtered to {len(filtered)} significant roof-damaging acts of god.")
        
        # Limit to the highest magnitudes for prototype speed. 
        # We increase this to 1000 for a more "populated" experience as requested.
        filtered = filtered.sort_values(by="MAGNITUDE", ascending=False).head(1000)

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # 3. Cross-reference geographically and socioeconomically
        count = 0
        for _, row in filtered.iterrows():
            lat = row['BEGIN_LAT']
            lon = row['BEGIN_LON']
            mag = row['MAGNITUDE']
            event_type = row['EVENT_TYPE']
            state = row['STATE']
            
            date_str = str(row['BEGIN_YEARMONTH'])
            day_str = str(row['BEGIN_DAY']).zfill(2)
            event_date = f"{date_str[:4]}-{date_str[4:]}-{day_str}" if date_str != 'nan' else f"{self.year}-01-01"

            # Reverse Geocode
            zip_info = self.search.by_coordinates(lat, lon, radius=5, returns=1)
            
            z = None
            if zip_info and len(zip_info) > 0:
                z = zip_info[0]
                
            c.execute('''
                INSERT INTO storms 
                (event_date, event_type, state, latitude, longitude, magnitude, zipcode, city, county, population, population_density, median_home_value, median_household_income)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event_date,
                event_type,
                state,
                lat,
                lon,
                mag,
                z.zipcode if z else None,
                z.major_city if z else None,
                z.county if z else None,
                z.population if z else None,
                z.population_density if z else None,
                z.median_home_value if z else None,
                z.median_household_income if z else None
            ))
            count += 1
            if count % 50 == 0:
                logger.info(f"Geocoded and inserted {count} authoritative records...")
                
        conn.commit()
        conn.close()
        logger.info(f"Successfully minted {count} events into Authoritative Data Lake (authoritative_storms.db)")

if __name__ == "__main__":
    builder = DatalakeBuilder(year=2026) # Pulls modern timelines!
    builder.build_lake()
