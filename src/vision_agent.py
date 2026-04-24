import logging
import random
import os
from src.vision_filters import RoofVisionFilters

logger = logging.getLogger(__name__)

class VisionAgent:
    def __init__(self, use_mock=True):
        self.use_mock = use_mock
        self.api_key = os.getenv("GEMINI_API_KEY")
        
        # If we weren't mocking, setup the google-genai client here
        if not self.use_mock and self.api_key:
            try:
                from google import genai
                self.client = genai.Client(api_key=self.api_key)
            except ImportError:
                logger.error("google-genai package not found. Using mock mode.")
                self.use_mock = True
    
    def inspect_roof_historical(self, before_img_path, after_img_path):
        """
        Takes a pre-storm and post-storm image and prompts the vision model to do a comparative analysis.
        """
        logger.info(f"AI Vision Agent performing comparative analysis on {before_img_path} vs {after_img_path}...")
        
        prompt = '''
        You are an expert insurance adjuster. 
        I am giving you TWO high-resolution overhead aerial images of the same roof. Image A is BEFORE the storm. Image B is AFTER the storm.
        
        Examine the roof and perform a comparative analysis to prove the Date of Loss. 
        Crucially, you must accurately categorize the DIFFERENT anomalies present. Look specifically for:
        - Hail damage (impact craters, dark spots, granular loss)
        - Wind damage (lifted, creased, or missing shingles)
        - Mechanical damage (foot traffic, tool marks)
        - Blistering or Fading (heat/UV damage, aging)
        - Manufacturer defects or Animal damage
        
        Compare Image A and Image B. Distinguish between pre-existing anomalies (in Image A) and new anomalies (only in Image B).
        
        Return a JSON response with:
        {
           "PreStormCondition": "Brief description of the roof before the storm.",
           "PostStormCondition": "Brief description of the new damage.",
           "DetectedAnomalies": [ {"Type": "Hail", "Confidence": "High", "IsNew": true}, {"Type": "Blistering", "Confidence": "Low", "IsNew": false} ],
           "DamageLevel": "None" | "Minor" | "Severe",
           "EstimatedReplacementEligibility": true | false
        }
        '''
        
        # Execute the new deterministic pre-filters extracted from the scientific paper
        logger.info("Executing ExG Vegetation filters and Canny Edge pre-processing...")
        
        # 1. Vegetation difference (Canopy torn off by wind)
        # In a real pipeline, we'd calculate ExG ratio of white pixels
        mask_before = RoofVisionFilters.calculate_exg_mask(before_img_path)
        mask_after = RoofVisionFilters.calculate_exg_mask(after_img_path)
        
        # 2. Structural Subtraction (Roof footprint collapsed or altered > 16m2)
        structural_loss_percent = RoofVisionFilters.detect_structural_change(before_img_path, after_img_path)
        logger.info(f"Deterministic logic calculated a geometric structural loss of {structural_loss_percent:.2f}%")
        
        if self.use_mock or not self.client:
            # Overwrite mock logic with real physics if catastrophic loss occurs
            if structural_loss_percent > 15.0:
                logger.warning("CATASTROPHIC LOSS DETECTED BY EDGE PHYSICS. BYPASSING AI ESTIMATE.")
                chosen_level = "Severe"
                post_desc = f"PHYSICS OVERRIDE: {structural_loss_percent:.2f}% of the physical geometry vanished post-storm. Total Collapse likely."
                eligible = True
                anomalies = [{"Type": "Total Structural Collapse", "Confidence": "100%", "IsNew": True}]
            else:
                # Mock reasoning simulation
                damage_levels = ["None", "Minor", "Severe"]
                chosen_level = random.choice(damage_levels)
                
                pre_desc = "Roof appears slightly aged but intact. No missing shingles."
                post_desc = "No changes detected." if chosen_level == "None" else "Fresh bruising and 3 new missing shingles not present in Before image."
                eligible = True if chosen_level == "Severe" else False
                
                anomalies = []
                if chosen_level == "Severe":
                    anomalies.append({"Type": "Hail", "Confidence": "High", "IsNew": True})
                    anomalies.append({"Type": "Wind", "Confidence": "Medium", "IsNew": True})
                elif chosen_level == "Minor":
                    anomalies.append({"Type": "Blistering", "Confidence": "High", "IsNew": False})
                    anomalies.append({"Type": "Mechanical", "Confidence": "Low", "IsNew": True})
            
            return {
                "PreStormCondition": "Calculated via physics engine." if structural_loss_percent > 15.0 else pre_desc,
                "PostStormCondition": post_desc,
                "DetectedAnomalies": anomalies,
                "DamageLevel": chosen_level,
                "EstimatedReplacementEligibility": eligible,
                "StructuralLossPercentage": structural_loss_percent,
                "DamageDescription": post_desc # Legacy key compatibility for other components
            }
        else:
            # Actual Integration pseudo-code
            pass
            # try:
            #     with open(before_img_path, "rb") as f1, open(after_img_path, "rb") as f2:
            #          before_data = f1.read()
            #          after_data = f2.read()
            #     # Note: genai client usually takes loaded image objects or paths directly
            #     response = self.client.models.generate_content(
            #         model='gemini-2.5-pro',
            #         contents=[before_data, after_data, prompt]
            #     )
            #     return parse_json(response.text)
            # except Exception as e:
            #     logger.error(f"Vision API failed: {e}")
            #     return {}
