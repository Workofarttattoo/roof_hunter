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
    
    Backend priority:
      1. CLIP damage head (clip_damage_head.pt) — custom-trained binary classifier
      2. Google Gemini Vision (multimodal) — when API key available
      3. YOLOv8 object detection — local fallback
      4. Spectral physics engine — deterministic fallback (ExG/Canny)
    """

    def __init__(self, use_mock=False):
        self.use_mock = use_mock
        self.api_key = os.getenv("GEMINI_API_KEY")
        self.gemini_client = None
        self.yolo_model = None
        self.clip_model = None
        self.clip_head = None

        models_dir = os.path.join(os.path.dirname(__file__), '..', 'models')

        # ── Try to init CLIP damage head (custom-trained) ──
        clip_head_path = os.path.join(models_dir, 'clip_damage_head.pt')
        if os.path.exists(clip_head_path) and not self.use_mock:
            try:
                import torch
                self.clip_head = torch.load(clip_head_path, map_location='cpu', weights_only=False)
                # Try loading CLIP for feature extraction
                try:
                    import clip as clip_module
                    self.clip_model, self.clip_preprocess = clip_module.load("ViT-B/32", device="cpu")
                    logger.info(f"CLIP damage head loaded: {clip_head_path}")
                except ImportError:
                    # Try open_clip as fallback
                    try:
                        import open_clip
                        self.clip_model, _, self.clip_preprocess = open_clip.create_model_and_transforms(
                            'ViT-B-32', pretrained='openai'
                        )
                        logger.info(f"CLIP damage head loaded (via open_clip): {clip_head_path}")
                    except ImportError:
                        logger.warning("Neither 'clip' nor 'open_clip' installed. "
                                       "pip install openai-clip OR pip install open-clip-torch")
                        self.clip_head = None
            except Exception as e:
                logger.warning(f"CLIP damage head init failed: {e}")
                self.clip_head = None

        # ── Try to init Gemini ──
        if self.api_key and not self.use_mock:
            try:
                from google import genai
                self.gemini_client = genai.Client(api_key=self.api_key)
                logger.info("Gemini Vision AI initialized successfully")
            except ImportError:
                logger.warning("google-genai package not installed. pip install google-genai")
            except Exception as e:
                logger.warning(f"Gemini init failed: {e}")

        # ── Try to init YOLO ──
        if not self.gemini_client and not self.clip_head:
            try:
                from ultralytics import YOLO
                weights_paths = [
                    os.path.join(models_dir, 'roof_damage_best.pt'),
                    os.path.join(models_dir, 'best.pt'),
                    os.path.join(os.path.dirname(__file__), 'yolov8n.pt'),
                    os.path.join(models_dir, 'yolov8n.pt'),
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
        backends = []
        if self.clip_head and self.clip_model:
            backends.append("CLIP damage head (custom-trained)")
        if self.gemini_client:
            backends.append("Gemini (multimodal AI)")
        if self.yolo_model:
            backends.append("YOLO")
        backends.append("Physics engine")
        logger.info(f"Vision backends available: {' → '.join(backends)}")

    def inspect_roof_historical(self, before_img_path, after_img_path):
        """
        Comparative analysis of before/after storm images.
        Falls through backends: Gemini → YOLO → Physics → Mock
        """
        logger.info(f"Vision analysis: {before_img_path}")

        # 1. Try CLIP damage head (custom-trained classifier)
        if self.clip_head and self.clip_model:
            result = self._clip_analyze(after_img_path)
            if result:
                return result

        # 2. Try Gemini multimodal
        if self.gemini_client:
            result = self._gemini_analyze(before_img_path, after_img_path)
            if result:
                return result

        # 3. Try YOLO detection
        if self.yolo_model:
            result = self._yolo_analyze(after_img_path)
            if result:
                return result

        # 4. Physics engine (deterministic)
        result = self._physics_analyze(before_img_path, after_img_path)
        if result:
            return result

        # 5. Final fallback
        return self._heuristic_result()

    def inspect_single_image(self, image_path):
        """
        Single-image damage analysis (when only post-storm imagery is available).
        """
        return self.inspect_roof_historical(image_path, image_path)

    # ─── Backend: CLIP Damage Head (Custom-Trained) ───────────────────────

    def _clip_analyze(self, image_path):
        """Use custom-trained CLIP damage classification head."""
        try:
            import torch
            import PIL.Image

            if not os.path.exists(image_path) or os.path.getsize(image_path) < 100:
                return None

            # Load and preprocess image
            image = PIL.Image.open(image_path).convert("RGB")
            image_tensor = self.clip_preprocess(image).unsqueeze(0)

            # Extract CLIP features
            with torch.no_grad():
                image_features = self.clip_model.encode_image(image_tensor)
                image_features = image_features / image_features.norm(dim=-1, keepdim=True)

                # Run through custom damage classification head
                head = self.clip_head
                if isinstance(head, dict):
                    # State dict — reconstruct linear head
                    if 'weight' in head:
                        linear = torch.nn.Linear(head['weight'].shape[1], head['weight'].shape[0])
                        linear.load_state_dict(head)
                        logits = linear(image_features.float())
                    elif 'model_state_dict' in head:
                        # Common training checkpoint format
                        state = head['model_state_dict']
                        w = state.get('weight', state.get('fc.weight', state.get('classifier.weight')))
                        b = state.get('bias', state.get('fc.bias', state.get('classifier.bias')))
                        if w is not None:
                            linear = torch.nn.Linear(w.shape[1], w.shape[0])
                            linear.weight = torch.nn.Parameter(w)
                            if b is not None:
                                linear.bias = torch.nn.Parameter(b)
                            logits = linear(image_features.float())
                        else:
                            logger.warning(f"CLIP head state_dict keys not recognized: {list(state.keys())[:5]}")
                            return None
                    else:
                        # Try to find any weight tensor
                        for k, v in head.items():
                            if isinstance(v, torch.Tensor) and v.dim() == 2:
                                logits = image_features.float() @ v.T
                                break
                        else:
                            logger.warning(f"CLIP head dict keys not recognized: {list(head.keys())[:5]}")
                            return None
                elif isinstance(head, torch.nn.Module):
                    logits = head(image_features.float())
                else:
                    logger.warning(f"CLIP head type not supported: {type(head)}")
                    return None

                # Convert logits to probabilities
                probs = torch.softmax(logits, dim=-1)
                damage_prob = probs[0, -1].item()  # Assume last class = damage
                
                # If only 2 classes, index 1 = damage
                if probs.shape[-1] == 2:
                    damage_prob = probs[0, 1].item()

            # Classify based on damage probability
            if damage_prob >= 0.70:
                damage_level = "Severe"
                eligible = True
            elif damage_prob >= 0.45:
                damage_level = "Minor"
                eligible = False
            else:
                damage_level = "None"
                eligible = False

            desc = (f"CLIP damage classifier: {damage_prob:.1%} damage probability. "
                    f"{'Roof replacement recommended.' if eligible else 'Below damage threshold.'}")

            return {
                "PreStormCondition": "N/A (CLIP single-image classifier)",
                "PostStormCondition": desc,
                "DetectedAnomalies": [
                    {"Type": "Hail/Storm Damage", "Confidence": f"{damage_prob:.1%}", "IsNew": True}
                ] if damage_prob >= 0.45 else [],
                "DamageLevel": damage_level,
                "EstimatedReplacementEligibility": eligible,
                "DamageDescription": desc,
                "damage_probability": damage_prob,
                "backend": "clip_damage_head"
            }

        except Exception as e:
            logger.warning(f"CLIP damage analysis failed: {e}")
            return None

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
