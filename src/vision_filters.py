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

    @staticmethod
    def calculate_exg_mask(image_path):
        """
        Calculate Excess Green Index (ExG) mask for vegetation detection.
        ExG = 2*G - R - B (highlights green vegetation vs structures)
        Useful for detecting canopy loss after storms.
        """
        try:
            img = cv2.imread(image_path)
            if img is None:
                return None
            
            b, g, r = img[:,:,0].astype(float), img[:,:,1].astype(float), img[:,:,2].astype(float)
            total = r + g + b + 1e-6  # avoid division by zero
            
            # Normalized ExG
            exg = 2 * (g / total) - (r / total) - (b / total)
            
            # Threshold to binary mask (vegetation vs non-vegetation)
            mask = (exg > 0.1).astype(np.uint8) * 255
            
            veg_ratio = np.sum(mask > 0) / (mask.shape[0] * mask.shape[1])
            return {"mask": mask, "vegetation_ratio": float(veg_ratio)}
        except Exception as e:
            logger.debug(f"ExG mask failed: {e}")
            return None

    @staticmethod
    def detect_structural_change(before_path, after_path):
        """
        Compare before/after images for structural geometry changes.
        Returns the percentage of changed area (0-100).
        
        Uses edge subtraction: if the Canny edge map changes significantly,
        roof geometry has been altered (collapsed, blown off, etc.)
        """
        try:
            before = cv2.imread(before_path)
            after = cv2.imread(after_path)
            
            if before is None or after is None:
                return 0.0
            
            # Resize to same dimensions
            h = min(before.shape[0], after.shape[0], 1000)
            w = min(before.shape[1], after.shape[1], 1000)
            before = cv2.resize(before, (w, h))
            after = cv2.resize(after, (w, h))
            
            # Convert to grayscale
            gray_before = cv2.cvtColor(before, cv2.COLOR_BGR2GRAY)
            gray_after = cv2.cvtColor(after, cv2.COLOR_BGR2GRAY)
            
            # Canny edge detection
            edges_before = cv2.Canny(gray_before, 50, 150)
            edges_after = cv2.Canny(gray_after, 50, 150)
            
            # Calculate difference
            diff = cv2.absdiff(edges_before, edges_after)
            changed_pixels = np.sum(diff > 0)
            total_pixels = diff.shape[0] * diff.shape[1]
            
            change_percent = (changed_pixels / total_pixels) * 100
            return float(change_percent)
        except Exception as e:
            logger.debug(f"Structural change detection failed: {e}")
            return 0.0
