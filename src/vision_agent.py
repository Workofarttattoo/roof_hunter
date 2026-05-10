"""
vision_agent.py
---------------
AI-powered roof damage detection using multiple backends:
  1. Google Gemini Vision (multimodal) — primary when API key available
  2. YOLOv8 object detection — local fallback
  3. Spectral physics engine — deterministic fallback (ExG/Canny)

The agent performs comparative before/after analysis OR single-image damage scoring
depending on what imagery is available.
"""

import logging
import random
import os
import json
from typing import Optional

logger = logging.getLogger(__name__)


class VisionAgent:
    """
    Multi-backend vision AI for roof damage classification.
    """

    def __init__(self, use_mock=False):
        self.use_mock = use_mock
        self.api_key = os.getenv("GEMINI_API_KEY")
        self.gemini_client = None
        self.yolo_model = None

        # Try to init Gemini
        if self.api_key and not self.use_mock:
            try:
                from google import genai
                self.gemini_client = genai.Client(api_key=self.api_key)
                logger.info("Gemini Vision AI initialized successfully")
            except ImportError:
                logger.warning("google-genai package not installed. pip install google-genai")
            except Exception as e:
                logger.warning(f"Gemini init failed: {e}")

        # Try to init YOLO
        if not self.gemini_client:
            try:
                from ultralytics import YOLO
                # Check for custom trained weights first, fall back to stock
                weights_paths = [
                    os.path.join(os.path.dirname(__file__), '..', 'models', 'roof_damage_best.pt'),
                    os.path.join(os.path.dirname(__file__), '..', 'models', 'best.pt'),
                    os.path.join(os.path.dirname(__file__), 'yolov8n.pt'),
                    os.path.join(os.path.dirname(__file__), '..', 'models', 'yolov8n.pt'),
                    'yolov8n.pt'
                ]
                for wp in weights_paths:
                    if os.path.exists(wp):
                        self.yolo_model = YOLO(wp)
                        logger.info(f"YOLO detector loaded: {wp}")
                        break
                if not self.yolo_model:
                    self.yolo_model = YOLO('yolov8n.pt')
                    logger.info("YOLO detector loaded (stock yolov8n)")
            except ImportError:
                logger.warning("ultralytics not installed. Vision will use physics engine only.")
            except Exception as e:
                logger.warning(f"YOLO init failed: {e}")

        # Log backend status
        if self.gemini_client:
            logger.info("Vision backend: Gemini (multimodal AI)")
        elif self.yolo_model:
            logger.info("Vision backend: YOLO + physics engine")
        else:
            logger.info("Vision backend: Physics engine only (deterministic)")

    def inspect_roof_historical(self, before_img_path, after_img_path):
        """
        Comparative analysis of before/after storm images.
        Falls through backends: Gemini → YOLO → Physics → Mock
        """
        logger.info(f"Vision analysis: {before_img_path}")

        # 1. Try Gemini multimodal
        if self.gemini_client:
            result = self._gemini_analyze(before_img_path, after_img_path)
            if result:
                return result

        # 2. Try YOLO detection
        if self.yolo_model:
            result = self._yolo_analyze(after_img_path)
            if result:
                return result

        # 3. Physics engine (deterministic)
        result = self._physics_analyze(before_img_path, after_img_path)
        if result:
            return result

        # 4. Final fallback
        return self._heuristic_result()

    def inspect_single_image(self, image_path):
        """
        Single-image damage analysis (when only post-storm imagery is available).
        """
        return self.inspect_roof_historical(image_path, image_path)

    # ─── Backend: Gemini Vision ─────────────────────────────────────────

    def _gemini_analyze(self, before_path, after_path):
        """Use Google Gemini multimodal to analyze roof imagery."""
        try:
            import PIL.Image

            prompt = """You are an expert roofing insurance adjuster analyzing aerial/satellite 
imagery of a residential roof. Examine this image carefully and assess for storm/hail damage.

Look specifically for:
- Hail damage: impact craters, dark spots, granular loss on shingles
- Wind damage: lifted, creased, or missing shingles/tiles
- Structural damage: sagging, visible holes, displaced materials
- Debris accumulation from storm
- Color anomalies indicating fresh damage vs weathering

Return ONLY a valid JSON object (no markdown, no explanation):
{
    "PreStormCondition": "Brief description",
    "PostStormCondition": "Brief description of damage observed",
    "DetectedAnomalies": [{"Type": "Hail|Wind|Structural", "Confidence": "High|Medium|Low", "IsNew": true}],
    "DamageLevel": "None|Minor|Severe",
    "EstimatedReplacementEligibility": true/false,
    "DamageDescription": "Detailed damage summary"
}"""

            # Load image(s)
            images = []
            if os.path.exists(after_path) and os.path.getsize(after_path) > 100:
                images.append(PIL.Image.open(after_path))

            if before_path != after_path and os.path.exists(before_path) and os.path.getsize(before_path) > 100:
                images.insert(0, PIL.Image.open(before_path))

            if not images:
                logger.warning("No valid images for Gemini analysis")
                return None

            contents = images + [prompt]
            response = self.gemini_client.models.generate_content(
                model='gemini-2.5-flash',
                contents=contents
            )

            # Parse JSON response
            text = response.text.strip()
            # Handle markdown-wrapped JSON
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()

            result = json.loads(text)
            result["backend"] = "gemini"
            logger.info(f"Gemini result: {result.get('DamageLevel', '?')} damage, "
                        f"eligible={result.get('EstimatedReplacementEligibility', '?')}")
            return result

        except Exception as e:
            logger.warning(f"Gemini analysis failed: {e}")
            return None

    # ─── Backend: YOLO Detection ────────────────────────────────────────

    def _yolo_analyze(self, image_path):
        """Use YOLO object detection for damage classification."""
        try:
            if not os.path.exists(image_path) or os.path.getsize(image_path) < 100:
                return None

            results = self.yolo_model(image_path, conf=0.25, verbose=False)
            detections = []

            for r in results:
                for box in r.boxes:
                    detections.append({
                        "class": int(box.cls),
                        "class_name": self.yolo_model.names.get(int(box.cls), "unknown"),
                        "confidence": float(box.conf),
                        "bbox": box.xyxy.tolist()[0]
                    })

            # Calculate damage metrics from detections
            damage_count = len(detections)
            avg_conf = sum(d["confidence"] for d in detections) / max(damage_count, 1)

            # Also run Laplacian variance check
            import cv2
            import numpy as np
            img = cv2.imread(image_path)
            if img is not None:
                gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                laplacian_var = cv2.Laplacian(gray, cv2.CV_64F).var()
            else:
                laplacian_var = 0

            # Scoring: combine detection count + Laplacian texture analysis
            texture_score = min(laplacian_var / 50.0, 100)

            if damage_count >= 3 or texture_score > 60:
                damage_level = "Severe"
                eligible = True
                desc = f"YOLO detected {damage_count} anomaly regions (avg conf {avg_conf:.2f}). Texture score: {texture_score:.1f}"
            elif damage_count >= 1 or texture_score > 35:
                damage_level = "Minor"
                eligible = False
                desc = f"YOLO detected {damage_count} regions. Texture disruption moderate ({texture_score:.1f})"
            else:
                damage_level = "None"
                eligible = False
                desc = "No significant damage detected by YOLO + texture analysis"

            return {
                "PreStormCondition": "N/A (single image analysis)",
                "PostStormCondition": desc,
                "DetectedAnomalies": [
                    {"Type": d["class_name"], "Confidence": f"{d['confidence']:.2f}", "IsNew": True}
                    for d in detections[:5]
                ],
                "DamageLevel": damage_level,
                "EstimatedReplacementEligibility": eligible,
                "DamageDescription": desc,
                "StructuralScore": texture_score,
                "backend": "yolo"
            }

        except Exception as e:
            logger.warning(f"YOLO analysis failed: {e}")
            return None

    # ─── Backend: Physics Engine ────────────────────────────────────────

    def _physics_analyze(self, before_path, after_path):
        """Deterministic physics-based damage detection using OpenCV."""
        try:
            from src.vision_filters import RoofVisionFilters
            filters = RoofVisionFilters()

            # Extract features from after image
            features = filters.extract_geobia_features(after_path)
            if not features:
                return None

            edge_density = features.get("edge_density", 0)
            pixel_variance = features.get("shingle_pixel_variance", 0)
            shadow_profile = features.get("shadow_profile", 0)

            # Scoring
            # High edge density = disrupted shingle pattern
            # High red variance = exposed underlayment/rust
            # High shadow = structural deformation
            score = (edge_density * 300) + (pixel_variance / 1000) + (shadow_profile * 200)

            if score > 50:
                damage_level = "Severe"
                eligible = True
            elif score > 25:
                damage_level = "Minor"
                eligible = False
            else:
                damage_level = "None"
                eligible = False

            desc = (f"Physics engine: edge_density={edge_density:.4f}, "
                    f"variance={pixel_variance:.1f}, shadow={shadow_profile:.4f}, "
                    f"composite_score={score:.2f}")

            return {
                "PreStormCondition": "Analyzed via spectral physics engine",
                "PostStormCondition": desc,
                "DetectedAnomalies": [
                    {"Type": "Edge Disruption", "Confidence": f"{min(edge_density * 100, 100):.0f}%", "IsNew": True}
                ] if edge_density > 0.05 else [],
                "DamageLevel": damage_level,
                "EstimatedReplacementEligibility": eligible,
                "DamageDescription": desc,
                "backend": "physics"
            }

        except Exception as e:
            logger.debug(f"Physics engine failed: {e}")
            return None

    # ─── Fallback: Heuristic ────────────────────────────────────────────

    def _heuristic_result(self):
        """Last resort — return a conservative 'needs inspection' result."""
        return {
            "PreStormCondition": "Unable to analyze (no imagery or backends available)",
            "PostStormCondition": "Manual inspection recommended",
            "DetectedAnomalies": [],
            "DamageLevel": "Unknown",
            "EstimatedReplacementEligibility": False,
            "DamageDescription": "No AI backends available. Recommend field inspection.",
            "backend": "fallback"
        }

    # ─── Static methods for backward compatibility ──────────────────────

    @staticmethod
    def calculate_exg_mask(image_path):
        """Excess Green Index mask (backward compat)."""
        try:
            from src.vision_filters import RoofVisionFilters
            return RoofVisionFilters.calculate_exg_mask(image_path)
        except:
            return None

    @staticmethod
    def detect_structural_change(before_path, after_path):
        """Structural change detection (backward compat)."""
        try:
            from src.vision_filters import RoofVisionFilters
            return RoofVisionFilters.detect_structural_change(before_path, after_path)
        except:
            return 0.0
