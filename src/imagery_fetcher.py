import logging
import os
import requests
from src.oam_harvester import OAMHarvester

logger = logging.getLogger(__name__)

class GoogleStaticImagery:
    """
    Fetches high-resolution satellite imagery from the Google Maps Static API
    for fine-grained roofing damage classification.
    """
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("GOOGLE_MAPS_API_KEY")
        self.oam = OAMHarvester()
        self.save_dir = os.path.join(os.path.dirname(__file__), '..', 'training_data')
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

    def fetch_roof_image(self, lat, lon, filename):
        """
        Fetches imagery from Google Static Maps (Primary) or OpenAerialMap (Fallback).
        """
        output_path = os.path.join(self.save_dir, f"{filename}.png")
        
        if not self.api_key:
            logger.info(f"Google Key missing. Attempting OpenAerialMap deep-search for ({lat}, {lon})...")
            oam_scene = self.oam.get_best_image(lat, lon)
            if oam_scene and oam_scene.get('url'):
                try:
                    res = requests.get(oam_scene['url'], timeout=10)
                    if res.status_code == 200:
                        with open(output_path, 'wb') as f:
                            f.write(res.content)
                        logger.info(f"OAM HIT: Successfully harvested OpenAerialMap tile: {output_path}")
                        return output_path
                except:
                    pass
            
            logger.warning("No real imagery found. Generating mock noise for signal testing.")
            return self._generate_mock_image(output_path)

        url = "https://maps.googleapis.com/maps/api/staticmap"
        params = {
            "center": f"{lat},{lon}",
            "zoom": 20,
            "size": "640x640",
            "maptype": "satellite",
            "key": self.api_key
        }

        try:
            res = requests.get(url, params=params)
            if res.status_code == 200:
                with open(output_path, 'wb') as f:
                    f.write(res.content)
                logger.info(f"Successfully harvested satellite image: {output_path}")
                return output_path
            else:
                logger.error(f"Google Maps API Error: {res.status_code}")
                return None
        except Exception as e:
            logger.error(f"Image fetch failed: {e}")
            return None

    def _generate_mock_image(self, path):
        """Creates a dummy file to simulate imagery acquisition."""
        with open(path, 'wb') as f:
            f.write(os.urandom(1024))
        return path
