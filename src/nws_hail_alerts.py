"""
Active alerts from api.weather.gov (NWS) — filter GeoJSON features that mention hail.

NWS requires a descriptive User-Agent. Set NWS_USER_AGENT in the environment or rely on default.
"""

from __future__ import annotations

import os
from typing import Any

import requests

NWS_ALERTS_ACTIVE = "https://api.weather.gov/alerts/active"

DEFAULT_USER_AGENT = (
    "RidgelineRoofHunter/1.0 (forensic storm intel; "
    "https://github.com/roof-hunter/roof_hunter)"
)


def _user_agent() -> str:
    return (os.getenv("NWS_USER_AGENT") or DEFAULT_USER_AGENT).strip() or DEFAULT_USER_AGENT


def _feature_mentions_hail(props: dict[str, Any]) -> bool:
    blob = " ".join(
        str(props.get(k) or "")
        for k in ("event", "headline", "description", "instruction")
    ).lower()
    return "hail" in blob


def get_hail_events(timeout: float = 25.0) -> list[dict[str, Any]]:
    """
    Return active alert *features* (GeoJSON objects) whose text mentions hail.

    Same idea as filtering data['features'] for 'hail', but scoped to official fields
    and with required NWS headers.
    """
    headers = {
        "User-Agent": _user_agent(),
        "Accept": "application/geo+json",
    }
    r = requests.get(NWS_ALERTS_ACTIVE, headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    features = data.get("features") or []
    return [f for f in features if _feature_mentions_hail((f or {}).get("properties") or {})]


def summarize_hail_alerts(features: list[dict[str, Any]], limit: int = 40) -> list[dict[str, Any]]:
    """Small JSON-safe list for API responses."""
    out: list[dict[str, Any]] = []
    for f in features[:limit]:
        p = (f or {}).get("properties") or {}
        out.append(
            {
                "id": p.get("id"),
                "event": p.get("event"),
                "areaDesc": (p.get("areaDesc") or "")[:240],
                "headline": (p.get("headline") or "")[:160],
                "sent": p.get("sent"),
                "ends": p.get("ends"),
            }
        )
    return out
