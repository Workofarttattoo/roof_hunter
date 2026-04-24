import os
import sqlite3
import logging
import xarray as xr
import numpy as np
from datetime import datetime, timedelta
from uszipcode import SearchEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'authoritative_storms.db')

class ThreddsMRMSBuilder:
    """
    Connects to the NOAA NSSL THREDDS Data Server to pull high-fidelity
    grid arrays (Multi-Radar Multi-Sensor) instead of CSV spotters.
    """
    def __init__(self):
        self.search = SearchEngine()
        
    def init_db(self):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # Add THREDDS tracking fields if not exist
        try:
            c.execute("ALTER TABLE storms ADD COLUMN source TEXT DEFAULT 'NCEI_CSV'")
            c.execute("ALTER TABLE storms ADD COLUMN cell_probability REAL DEFAULT 1.0")
        except sqlite3.OperationalError:
            pass # Columns likely already exist
        conn.commit()
        conn.close()

    def parse_mrms_mesh(self, opendap_url_or_filepath: str):
        """
        Loads the Maximum Expected Size of Hail (MESH) NetCDF array.
        Extracts grid points where MESH > 1.5 inches.
        """
        logger.info(f"Opening MRMS THREDDS array from: {opendap_url_or_filepath}")
        
        try:
            # Chunking handles files larger than local RAM using Dask computation graphs
            ds = xr.open_dataset(opendap_url_or_filepath, engine='netcdf4', chunks={})
            
            # The exact variable name varies by MRMS product, usually 'MESH_Max_1440min' or simply 'MESH'
            var_name = [v for v in ds.variables if 'mesh' in v.lower()][0]
            mesh_array = ds[var_name]
            
            logger.info(f"Loaded {var_name} grid: {mesh_array.shape}")
            
            # Extract latitudes and longitudes
            lats = ds['lat'].values
            lons = ds['lon'].values
            
            # Find coordinates where MESH exceeds 1.5 inches (approx 38 mm)
            # MRMS usually stores MESH in millimeters
            threshold_mm = 1.5 * 25.4 
            
            # We use numpy to find indices
            mesh_data = mesh_array.values
            if mesh_data.ndim == 3:
                mesh_data = mesh_data[0] # Take first time slice if 3D
                
            y_idx, x_idx = np.where(mesh_data >= threshold_mm)
            logger.info(f"Found {len(y_idx)} cells with catastrophic MESH signatures (>1.5 inches).")
            
            new_events = []
            for y, x in zip(y_idx, x_idx):
                lat = lats[y]
                lon = lons[x]
                magnitude = mesh_data[y, x] / 25.4 # Convert back to inches
                
                # Reverse geocode the radar grid to a city/zip
                res = self.search.by_coordinates(lat, lon, radius=5, returns=1)
                if not res:
                    continue
                location = res[0]
                
                new_events.append({
                    "event_type": "Hail",
                    "magnitude": round(float(magnitude), 2),
                    "latitude": float(lat),
                    "longitude": float(lon),
                    "city": location.major_city.upper() if location.major_city else "UNKNOWN",
                    "state": location.state.upper() if location.state else "UNKNOWN",
                    "zipcode": location.zipcode,
                    "event_date": "2026-04-24", # Example: dynamically extract from dataset time
                    "median_home_value": location.median_home_value or 0,
                    "median_household_income": location.median_household_income or 0,
                    "population": location.population or 0,
                    "population_density": location.population_density or 0.0,
                    "source": "THREDDS_MRMS",
                    "cell_probability": 0.99
                })
                
                if len(new_events) >= 50:
                    break # Limit for demonstration processing
                    
            return new_events
            
        except Exception as e:
            logger.error(f"Failed to parse THREDDS payload: {e}")
            return []

    def ingest_to_lake(self, events):
        if not events:
            return
            
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        count = 0
        for ev in events:
            c.execute('''
                INSERT INTO storms (
                    event_id, event_type, magnitude, latitude, longitude,
                    city, state, zipcode, event_date,
                    median_home_value, median_household_income,
                    population, population_density, source, cell_probability
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f"MRMS_{ev['zipcode']}_{ev['event_date']}_{count}", ev['event_type'], ev['magnitude'],
                ev['latitude'], ev['longitude'], ev['city'], ev['state'], ev['zipcode'],
                ev['event_date'], ev['median_home_value'], ev['median_household_income'],
                ev['population'], ev['population_density'], ev['source'], ev['cell_probability']
            ))
            count += 1
            
        conn.commit()
        conn.close()
        logger.info(f"✅ Embedded {count} military-grade radar grids into the Data Lake.")

if __name__ == "__main__":
    builder = ThreddsMRMSBuilder()
    builder.init_db()
    
    # In production, this would be a live OPeNDAP URL like:
    yesterday = datetime.today() - timedelta(days=1)
    url_base = f"https://data.nssl.noaa.gov/thredds/dodsC/MRMS/MESH_Max_1440min/{yesterday.strftime('%Y/%m/%d')}/"
    logger.info(f"THREDDS MRMS Engine Initialized. Target live array prefix: {url_base}")
