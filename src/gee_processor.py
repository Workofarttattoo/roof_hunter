import os
import logging
import ee
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GEESentinelDiscovery:
    """
    Implements high-fidelity Sentinel-2 Spectral Discovery.
    Uses official EE Cloud Masking and Multispectral Normalization.
    """
    def __init__(self, key_path=None):
        self.key_path = key_path or os.getenv('GEE_SERVICE_ACCOUNT_KEY')
        self.project_id = os.getenv('GEE_PROJECT_ID', 'roof-hunter-494309')
        self._authenticate()

    def _authenticate(self):
        try:
            if self.key_path and os.path.exists(self.key_path):
                logger.info(f"GEE: Initializing with Service Account Key for project {self.project_id}")
                credentials = service_account.Credentials.from_service_account_file(
                    self.key_path,
                    scopes=['https://www.googleapis.com/auth/earthengine', 'https://www.googleapis.com/auth/cloud-platform']
                )
                ee.Initialize(credentials, project=self.project_id)
                logger.info("GEE: Initialization Successful.")
            else:
                logger.warning("GEE: No Service Account Key found.")
        except Exception as e:
            logger.error(f"GEE Initialization Failed: {e}")

    def mask_s2_clouds(self, image):
        """
        Masks clouds in a Sentinel-2 image using the QA60 band.
        (From Earth Engine Official Samples)
        """
        qa = image.select('QA60')
        cloud_bit_mask = 1 << 10
        cirrus_bit_mask = 1 << 11
        mask = (
            qa.bitwiseAnd(cloud_bit_mask)
            .eq(0)
            .And(qa.bitwiseAnd(cirrus_bit_mask).eq(0))
        )
        return image.updateMask(mask).divide(10000)

    def find_damaged_cells(self, lat, lon, radius_deg=0.01):
        """
        Uses Sentinel-2 Multispectral data to identify the most recent 
        cloud-free scenes over a coordinate.
        """
        try:
            point = ee.Geometry.Point([lon, lat])
            
            # Query the Harmonized collection for Surface Reflectance
            dataset = (
                ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                .filterBounds(point)
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
                .map(self.mask_s2_clouds)
                .sort('system:time_start', False)
            )
            
            first_image = dataset.first()
            img_info = first_image.getInfo()
            
            if not img_info:
                logger.warning(f"GEE: No clear Sentinel-2 scenes found for ({lat}, {lon})")
                return []
                
            asset_id = img_info['id']
            logger.info(f"GEE DISCOVERY: Secured Forensic Scene: {asset_id}")
            return [asset_id]
            
        except Exception as e:
            logger.error(f"GEE Search Error: {e}")
            return []

if __name__ == "__main__":
    discovery = GEESentinelDiscovery()
    # Test on Tulsa coordinate
    hits = discovery.find_damaged_cells(36.154, -95.9928)
    print(f"Verified Scenes: {hits}")
