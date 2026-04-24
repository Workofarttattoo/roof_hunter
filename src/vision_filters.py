import os
os.environ["OPENCV_IO_MAX_IMAGE_PIXELS"] = pow(2,40).__str__()
import cv2
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RoofVisionFilters:
    def __init__(self):
        logger.info("Forensic Vision Filters (Memory-Safe) initialized.")

    def extract_geobia_features(self, image_path):
        """
        Geographic Object-Based Image Analysis (GEOBIA)
        Extracts structural features with strict memory safety for large aerial scenes.
        """
        try:
            # 0. Pre-read Size Check (Stability Optimization)
            file_size_mb = os.path.getsize(image_path) / (1024 * 1024)
            if file_size_mb > 100:
                logger.warning(f"Image too large for current RAM constraints ({file_size_mb:.1f} MB). Skipping for stability.")
                return None

            img = cv2.imread(image_path)
            if img is None:
                return None
            
            # 1. Check for ultra-high-res and downsample (RAM Safety)
            h, w = img.shape[:2]
            if h > 3000 or w > 3000:
                logger.info(f"High-res scene detected ({w}x{h}). Scaling to 3000px limit...")
                scale = 3000 / max(h, w)
                img = cv2.resize(img, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)
            
            # 2. Edge Detection (Shingle scouring)
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            edges = cv2.Canny(gray, 100, 200)
            edge_density = np.sum(edges) / (img.shape[0] * img.shape[1])
            
            # 3. Color Anomaly (Rust/Missing granules)
            red_channel = img[:,:,2]
            red_variance = np.var(red_channel)
            
            # 4. Geometric Shadow Analysis
            shadow_mask = gray < 50
            shadow_area = np.sum(shadow_mask) / (img.shape[0] * img.shape[1])
            
            return {
                "edge_density": float(edge_density),
                "shingle_pixel_variance": float(red_variance),
                "shadow_profile": float(shadow_area)
            }
        except Exception as e:
            logger.error(f"GEOBIA Extraction Error on {image_path}: {e}")
            return None
