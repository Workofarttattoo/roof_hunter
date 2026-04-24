import os
import requests
import logging
import cv2
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StreetViewScraper:
    """
    Harvests ground-level "Street View" imagery for roofing leads.
    Used for verifying siding/gutter damage and gable-end roof integrity.
    """
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("GOOGLE_MAPS_API_KEY")
        self.save_dir = os.path.join(os.path.dirname(__file__), '..', 'training_data')
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)
        self.quality_threshold = 100.0 # Standard Laplacian Variance threshold for blur

    def analyze_image_quality(self, image_path):
        """
        Implements Google Street View Insights Module #9: Image Quality Analysis.
        Ensures imagery is suitable for computer vision tasks by checking blur.
        """
        img = cv2.imread(image_path)
        if img is None: return False
        
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        fm = cv2.Laplacian(gray, cv2.CV_64F).var()
        
        if fm < self.quality_threshold:
            logger.warning(f"Street View Insights: Image at {image_path} rejected due to BLUR (Score: {fm:.2f})")
            return False
            
        logger.info(f"Street View Insights: Image verified for Forensic Analysis (Quality: {fm:.2f})")
        return True

    def fetch_street_view(self, lat, lon, filename):
        """
        Fetches a 640x480 ground-level shot for a given coordinate.
        """
        output_path = os.path.join(self.save_dir, f"street_{filename}.jpg")
        
        if not self.api_key:
            logger.warning("GOOGLE_MAPS_API_KEY missing - generating mock street view.")
            return self._generate_mock_view(output_path)

        url = "https://maps.googleapis.com/maps/api/streetview"
        params = {
            "size": "640x480",
            "location": f"{lat},{lon}",
            "key": self.api_key,
            "fov": 90, # Field of view
            "pitch": 10 # Slight upward angle to see the roof/gutters
        }

        try:
            res = requests.get(url, params=params)
            if res.status_code == 200:
                # Check for "no imagery found" transparent pixel (Google quirk)
                if len(res.content) < 5000:
                    logger.warning(f"Street View: No imagery available for ({lat}, {lon})")
                    return None
                    
                with open(output_path, 'wb') as f:
                    f.write(res.content)
                
                # Run Insights Quality Check
                if not self.analyze_image_quality(output_path):
                    # Flag as low quality rather than deleting, for manual review
                    os.rename(output_path, output_path.replace(".jpg", "_LOW_RES.jpg"))
                    return None
                    
                logger.info(f"Successfully harvested STREET VIEW image: {output_path}")
                return output_path
            else:
                logger.error(f"Street View API Error: {res.status_code}")
                return None
        except Exception as e:
            logger.error(f"Street View fetch failed: {e}")
            return None

    def _generate_mock_view(self, path):
        # Create a placeholder indicating street view intent
        with open(path, 'wb') as f:
            f.write(os.urandom(512))
        return path

if __name__ == "__main__":
    # Test
    # sv = StreetViewScraper()
    # sv.fetch_street_view(36.1540, -95.9928, "tulsa_lead")
    pass
