import requests
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OAMHarvester:
    """
    Search and retrieve high-resolution drone and satellite imagery 
    from the OpenAerialMap (OAM) Open Imagery Network.
    """
    API_URL = "https://api.openaerialmap.org/meta"

    def search_imagery(self, lat, lon, buffer=0.01):
        """
        Search for images covering a coordinate.
        OAM uses BBOX: xmin, ymin, xmax, ymax
        """
        xmin = lon - buffer
        ymin = lat - buffer
        xmax = lon + buffer
        ymax = lat + buffer
        
        url = f"{self.API_URL}?bbox={xmin},{ymin},{xmax},{ymax}"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                logger.info(f"OAM: Found {len(results)} high-res scenes for coordinate ({lat}, {lon})")
                return results
            else:
                logger.error(f"OAM API Error: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"OAM Request Failed: {e}")
            return []

    def get_best_image(self, lat, lon):
        """
        Returns the download URL for the highest resolution (smallest mean_gsd)
        and most recent image for a location.
        """
        scenes = self.search_imagery(lat, lon)
        if not scenes:
            return None
        
        # Sort by resolution (mean_gsd) then by date
        scenes.sort(key=lambda x: (x.get('gsd', 999), x.get('acquisition_end', '')), reverse=True)
        
        best = scenes[0]
        return {
            "uuid": best.get('uuid'),
            "url": best.get('uuid'), # OAM often uses uuid as the base for tile access
            "gsd": best.get('gsd'),
            "date": best.get('acquisition_end'),
            "provider": best.get('provider')
        }

if __name__ == "__main__":
    # Test for Oklahoma coordinates
    # harvester = OAMHarvester()
    # print(harvester.get_best_image(35.3917, -97.9229))
    pass
