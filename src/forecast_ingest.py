"""Forecast ingestion for pre-impact hail and severe storm risk.

This module pulls free government data and stores it in the authoritative storm
DB so the platform can start scoring leads before damage is confirmed.

Sources:
- NWS active alerts API
- SPC convective outlook text pages
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "..", "leads_manifests", "authoritative_storms.db")
NWS_ALERTS_URL = "https://api.weather.gov/alerts/active"
SPC_OUTLOOK_TEMPLATE = "https://www.spc.noaa.gov/products/outlook/day{day}otlk.html"
HEADERS = {
    "User-Agent": "RoofHunterForecast/1.0 (https://github.com/roof-hunter/roof_hunter)",
    "Accept": "application/geo+json, application/json, text/html",
}

NWS_EVENTS = [
    "Severe Thunderstorm Watch",
    "Tornado Watch",
    "Severe Thunderstorm Warning",
    "Tornado Warning",
    "Special Weather Statement",
]

EVENT_BASE_SCORE = {
    "Tornado Warning": 0.95,
    "Severe Thunderstorm Warning": 0.85,
    "Tornado Watch": 0.80,
    "Severe Thunderstorm Watch": 0.70,
    "Special Weather Statement": 0.35,
}

OUTLOOK_SCORE = {
    "HIGH": 0.95,
    "ENH": 0.80,
    "SLGT": 0.60,
    "MRGL": 0.40,
    "TSTM": 0.25,
}


def _utcnow_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_forecast_schema(conn: sqlite3.Connection) -> None:
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            event_id TEXT,
            forecast_day INTEGER,
            event_type TEXT,
            area_desc TEXT,
            headline TEXT,
            risk_level TEXT,
            risk_score REAL,
            severity TEXT,
            status TEXT,
            begins TEXT,
            ends TEXT,
            geometry TEXT,
            raw_text TEXT,
            raw_json TEXT,
            created_at TEXT
        )
        """
    )
    c.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_forecast_events_unique ON forecast_events(source, event_id, forecast_day)"
    )
    conn.commit()


def run_forecast_ingest(db_path: str | None = None) -> dict[str, Any]:
    db_path = db_path or DB_PATH
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        _ensure_forecast_schema(conn)
        alerts = ingest_nws_alerts(conn)
        outlooks = ingest_spc_outlook(conn, days=(1, 2, 3))
        return {"alerts": alerts, "outlooks": outlooks}
    finally:
        conn.close()


def fetch_nws_alerts() -> list[dict[str, Any]]:
    params = [("status", "actual")] + [("event", event) for event in NWS_EVENTS]
    try:
        response = requests.get(NWS_ALERTS_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        payload = response.json()
        return payload.get("features", [])
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch NWS alerts: {exc}") from exc


def _extract_spc_outlook_text(html: str) -> str:
    match = re.search(r"<pre[^>]*>(.*?)</pre>", html, re.S | re.I)
    if match:
        return re.sub(r"<[^>]+>", "", match.group(1)).strip()
    clean = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.S | re.I)
    clean = re.sub(r"<style[^>]*>.*?</style>", "", clean, flags=re.S | re.I)
    clean = re.sub(r"<[^>]+>", "", clean)
    return clean.strip()


def fetch_spc_outlook(day: int = 1) -> dict[str, Any]:
    url = SPC_OUTLOOK_TEMPLATE.format(day=day)
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        raw = response.text
        text = _extract_spc_outlook_text(raw)
        risk_level = "TSTM"
        upper = text.upper()
        for token in ("HIGH", "ENH", "SLGT", "MRGL", "TSTM"):
            if token in upper:
                risk_level = token
                break
        return {
            "event_id": f"SPC_DAY{day}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
            "source": "SPC_OUTLOOK",
            "forecast_day": day,
            "event_type": "Convective Outlook",
            "area_desc": f"SPC Day {day} Convective Outlook",
            "headline": f"SPC Day {day} Outlook ({risk_level})",
            "risk_level": risk_level,
            "risk_score": float(OUTLOOK_SCORE.get(risk_level, 0.25)),
            "severity": "Forecast",
            "status": "forecast",
            "begins": datetime.now(timezone.utc).isoformat(),
            "ends": (datetime.now(timezone.utc) + timedelta(days=day)).isoformat(),
            "geometry": None,
            "raw_text": text[:8000],
            "raw_json": None,
        }
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch SPC Day {day} outlook: {exc}") from exc


def _score_from_alert(properties: dict[str, Any]) -> float:
    event = properties.get("event", "")
    base = float(EVENT_BASE_SCORE.get(event, 0.25))
    severity = (properties.get("severity") or "").upper()
    if severity == "SEVERE":
        base = min(0.99, base + 0.05)
    if severity == "EXTREME":
        base = min(0.99, base + 0.10)
    return base


def _serialize_geometry(geometry: Any) -> str | None:
    if not geometry:
        return None
    try:
        return json.dumps(geometry)
    except (TypeError, ValueError):
        return None


def ingest_nws_alerts(conn: sqlite3.Connection) -> int:
    features = fetch_nws_alerts()
    inserted = 0
    c = conn.cursor()
    for feature in features:
        props = feature.get("properties", {})
        event_id = props.get("id") or props.get("@id") or props.get("eventUri") or None
        alert = {
            "source": "NWS_ALERT",
            "event_id": event_id,
            "forecast_day": 0,
            "event_type": props.get("event"),
            "area_desc": props.get("areaDesc"),
            "headline": props.get("headline"),
            "risk_level": props.get("event"),
            "risk_score": _score_from_alert(props),
            "severity": props.get("severity"),
            "status": props.get("status"),
            "begins": props.get("onset"),
            "ends": props.get("ends"),
            "geometry": _serialize_geometry(feature.get("geometry")),
            "raw_text": props.get("description", "")[:8000],
            "raw_json": json.dumps(feature)[:20000],
        }
        try:
            c.execute(
                "INSERT OR IGNORE INTO forecast_events (source, event_id, forecast_day, event_type, area_desc, headline, risk_level, risk_score, severity, status, begins, ends, geometry, raw_text, raw_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    alert["source"],
                    alert["event_id"],
                    alert["forecast_day"],
                    alert["event_type"],
                    alert["area_desc"],
                    alert["headline"],
                    alert["risk_level"],
                    alert["risk_score"],
                    alert["severity"],
                    alert["status"],
                    alert["begins"],
                    alert["ends"],
                    alert["geometry"],
                    alert["raw_text"],
                    alert["raw_json"],
                    _utcnow_text(),
                ),
            )
            inserted += 1
        except sqlite3.DatabaseError:
            continue
    conn.commit()
    return inserted


def ingest_spc_outlook(conn: sqlite3.Connection, days: tuple[int, ...] = (1, 2, 3)) -> int:
    inserted = 0
    c = conn.cursor()
    for day in days:
        outlook = fetch_spc_outlook(day)
        try:
            c.execute(
                "INSERT OR IGNORE INTO forecast_events (source, event_id, forecast_day, event_type, area_desc, headline, risk_level, risk_score, severity, status, begins, ends, geometry, raw_text, raw_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    outlook["source"],
                    outlook["event_id"],
                    outlook["forecast_day"],
                    outlook["event_type"],
                    outlook["area_desc"],
                    outlook["headline"],
                    outlook["risk_level"],
                    outlook["risk_score"],
                    outlook["severity"],
                    outlook["status"],
                    outlook["begins"],
                    outlook["ends"],
                    outlook["geometry"],
                    outlook["raw_text"],
                    json.dumps(outlook, default=str)[:20000],
                    _utcnow_text(),
                ),
            )
            inserted += 1
        except sqlite3.DatabaseError:
            continue
    conn.commit()
    return inserted


def _point_in_polygon(lat: float, lon: float, polygon: list[list[list[float]]]) -> bool:
    inside = False
    for ring in polygon:
        j = len(ring) - 1
        for i in range(len(ring)):
            xi, yi = ring[i][1], ring[i][0]
            xj, yj = ring[j][1], ring[j][0]
            intersect = ((yi > lat) != (yj > lat)) and (lon < (xj - xi) * (lat - yi) / (yj - yi + 1e-12) + xi)
            if intersect:
                inside = not inside
            j = i
    return inside


def _geometry_contains_point(lat: float, lon: float, geometry: Any) -> bool:
    if not geometry:
        return False
    gtype = geometry.get("type")
    coords = geometry.get("coordinates")
    if not coords:
        return False
    if gtype == "Polygon":
        return _point_in_polygon(lat, lon, coords)
    if gtype == "MultiPolygon":
        return any(_point_in_polygon(lat, lon, polygon) for polygon in coords)
    return False


def forecast_risk_for_point(lat: float, lon: float) -> float:
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        c = conn.cursor()
        c.execute(
            "SELECT event_type, risk_score, geometry, status FROM forecast_events WHERE source = 'NWS_ALERT' ORDER BY created_at DESC"
        )
        alerts = c.fetchall()
        best = 0.0
        for row in alerts:
            geometry = None
            if row["geometry"]:
                try:
                    geometry = json.loads(row["geometry"])
                except json.JSONDecodeError:
                    geometry = None
            if geometry and _geometry_contains_point(lat, lon, geometry):
                best = max(best, float(row["risk_score"] or 0.0))
        c.execute(
            "SELECT risk_score FROM forecast_events WHERE source = 'SPC_OUTLOOK' ORDER BY forecast_day ASC LIMIT 3"
        )
        outlook_scores = [float(r["risk_score" ] or 0.0) for r in c.fetchall()]
        if outlook_scores:
            best = max(best, max(outlook_scores) * 0.85)
        return round(best, 3)
    finally:
        conn.close()


def latest_forecast_summary() -> dict[str, Any]:
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        c = conn.cursor()
        c.execute(
            "SELECT source, event_type, area_desc, headline, risk_score, begins, ends, status FROM forecast_events ORDER BY created_at DESC LIMIT 10"
        )
        rows = [dict(row) for row in c.fetchall()]
        return {"latest": rows}
    finally:
        conn.close()


if __name__ == "__main__":
    summary = run_forecast_ingest()
    print(f"Inserted forecasts: alerts={summary['alerts']} outlooks={summary['outlooks']}")
    print(json.dumps(latest_forecast_summary(), indent=2))
