import requests
import json
import os
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class USGSHarvester:
    """
    Interfaces with the USGS EarthExplorer Machine-to-Machine (M2M) API
    to download high-resolution satellite imagery (Sentinel-2 / Landsat).
    """
    API_URL = "https://m2m.cr.usgs.gov/api/v1/"

    def __init__(self, username=None, password=None):
        self.username = username or os.getenv("USGS_USERNAME")
        self.password = password or os.getenv("USGS_PASSWORD")
        self.api_key = None

    def login(self):
        """
        Authenticates with USGS and retrieves a temporary API Key.
        """
        if not self.username or not self.password:
            logger.error("USGS Credentials missing. Set USGS_USERNAME and USGS_PASSWORD.")
            return False

        payload = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            response = requests.post(f"{self.API_URL}login", json=payload)
            response_data = response.json()
            
            if response_data.get('errorCode'):
                logger.error(f"USGS Login Failed: {response_data.get('errorMessage')}")
                return False
            
            self.api_key = response_data.get('data')
            logger.info("Successfully authenticated with USGS EarthExplorer.")
            return True
        except Exception as e:
            logger.error(f"USGS API Error: {e}")
            return False

    def search_scenes(self, lat, lon, dataset="sentinel_2a", start_date="2024-01-01", end_date="2026-12-31"):
        """
        Searches for satellite scenes covering a specific coordinate.
        """
        if not self.api_key:
            if not self.login(): return []

        # Define a small bounding box around the coordinate
        buffer = 0.01
        spatial_filter = {
            "filterType": "mbr",
            "lowerLeft": {"latitude": lat - buffer, "longitude": lon - buffer},
            "upperRight": {"latitude": lat + buffer, "longitude": lon + buffer}
        }
        
        temporal_filter = {
            "start": start_date,
            "end": end_date
        }

        payload = {
            "datasetName": dataset,
            "spatialFilter": spatial_filter,
            "temporalFilter": temporal_filter
        }

        headers = {"X-Auth-Token": self.api_key}
        
        try:
            response = requests.post(f"{self.API_URL}scene-search", json=payload, headers=headers)
            results = response.json().get('data', {}).get('results', [])
            logger.info(f"Found {len(results)} scenes for coordinate ({lat}, {lon})")
            return results
        except Exception as e:
            logger.error(f"Scene Search Error: {e}")
            return []

    def logout(self):
        if self.api_key:
            requests.post(f"{self.API_URL}logout", headers={"X-Auth-Token": self.api_key})
            self.api_key = None

if __name__ == "__main__":
    # To use this, the user must set credentials in .env
    # from dotenv import load_dotenv
    # load_dotenv()
    # harvest = USGSHarvester()
    # harvest.login()
    pass
