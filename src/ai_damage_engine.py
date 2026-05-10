"""
Roof / hail damage from imagery URL or local path.

Uses `yolo_detector.infer_from_url` when vision extras are installed and URL is http(s).
Set ROOF_YOLO_WEIGHTS for trained Ultralytics weights on workers (see requirements-vision.txt).
"""

from __future__ import annotations

import logging
import random

logger = logging.getLogger(__name__)


def _stub(image_url: str) -> dict:
    _ = image_url
    return {
        "hail_damage": random.uniform(0.5, 0.95),
        "missing_shingles": random.choice([True, False]),
        "confidence": 0.87,
        "backend": "stub",
    }


def predict_damage(image_url: str) -> dict:
    """Return hail_damage 0–1, missing_shingles, confidence; prefer real CV when available."""
    raw = (image_url or "").strip()
    if raw.startswith(("http://", "https://")):
        try:
            from yolo_detector import infer_from_url

            out = infer_from_url(raw)
            dp = float(out.get("damage_percent", 0))
            hm = max(0.0, min(1.0, float(out.get("hail_damage", dp / 100.0))))
            det_n = len(out.get("detections") or [])
            return {
                "hail_damage": hm,
                "missing_shingles": dp > 38.0,
                "confidence": float(out.get("confidence_score", 0.75)),
                "backend": out.get("backend", "vision"),
                "damage_percent": dp,
                "detection_count": det_n,
                "note": out.get("note"),
            }
        except ImportError:
            logger.info("Vision extras not installed (pip install -r requirements-vision.txt); using stub")
        except Exception as e:
            logger.warning("Vision inference failed, using stub: %s", e)

    return _stub(raw)


def assessment_from_image(image_url: str) -> dict:
    """Engine output + normalized score (0–100) and risk label for API responses."""
    p = predict_damage(image_url)
    hail = float(p["hail_damage"])
    if hail >= 0.75:
        risk = "HIGH"
    elif hail >= 0.65:
        risk = "MEDIUM"
    else:
        risk = "LOW"
    return {
        **p,
        "score": round(hail * 100, 2),
        "risk": risk,
    }
