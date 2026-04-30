"""
Export call-ready leads: last N days, damage_score >= threshold, hail >= min inches,
and corroboration = at least TWO distinct storm observations (SQLite rows) for the same
day/state and either the same 0.1deg cell OR the same normalized city.

This approximates "multiple station / multiple report" agreement without ingesting full
NOAA LSR feeds into a separate table. Tune with --strict-spatial-only to drop city-only paths.

Hail Alley is tagged for downstream prioritization; export includes all qualifying US
states (anywhere with 2"+ corroborated storms), not only Alley.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

_src = Path(__file__).resolve().parent
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from lead_verification_media import build_media_items_for_lead, media_to_api_payload  # noqa: E402

# Extended corridor (Great Plains / High Plains / adjacent): sales + meteorology blend
HAIL_ALLEY_STATES = frozenset(
    {
        "TX",
        "OK",
        "KS",
        "NE",
        "SD",
        "ND",
        "CO",
        "WY",
        "MT",
        "NM",
        "MN",
        "IA",
        "MO",
        "AR",
        "LA",
    }
)

STATE_ALIASES = {
    "TEXAS": "TX",
    "OKLAHOMA": "OK",
    "KANSAS": "KS",
    "NEBRASKA": "NE",
    "SOUTH DAKOTA": "SD",
    "NORTH DAKOTA": "ND",
    "COLORADO": "CO",
    "WYOMING": "WY",
    "MONTANA": "MT",
    "NEW MEXICO": "NM",
    "MINNESOTA": "MN",
    "IOWA": "IA",
    "MISSOURI": "MO",
    "ARKANSAS": "AR",
    "LOUISIANA": "LA",
    "ALABAMA": "AL",
    "ILLINOIS": "IL",
    "WISCONSIN": "WI",
    "MISSISSIPPI": "MS",
    "TENNESSEE": "TN",
    "GEORGIA": "GA",
    "FLORIDA": "FL",
    "OHIO": "OH",
    "INDIANA": "IN",
    "MICHIGAN": "MI",
    "KENTUCKY": "KY",
    "SOUTH CAROLINA": "SC",
    "NORTH CAROLINA": "NC",
    "VIRGINIA": "VA",
    "WEST VIRGINIA": "WV",
    "PENNSYLVANIA": "PA",
    "NEW YORK": "NY",
    "MARYLAND": "MD",
    "DELAWARE": "DE",
    "NEW JERSEY": "NJ",
    "CONNECTICUT": "CT",
    "RHODE ISLAND": "RI",
    "MASSACHUSETTS": "MA",
    "VERMONT": "VT",
    "NEW HAMPSHIRE": "NH",
    "MAINE": "ME",
    "ARIZONA": "AZ",
    "UTAH": "UT",
    "IDAHO": "ID",
    "NEVADA": "NV",
    "CALIFORNIA": "CA",
    "WASHINGTON": "WA",
    "OREGON": "OR",
    "ALASKA": "AK",
    "HAWAII": "HI",
}


def normalize_state(st: str | None) -> str:
    if not st:
        return ""
    u = re.sub(r"\s+", " ", str(st).strip().upper())
    if len(u) == 2 and u.isalpha():
        return u
    return STATE_ALIASES.get(u, u[:2] if len(u) == 2 else u)


SCHEMA = [
    "phone_number",
    "first_name",
    "last_name",
    "property_address",
    "city",
    "state",
    "zip_code",
    "hail_date",
    "hail_size_in",
    "storm_type",
    "damage_probability",
    "structures_hit",
    "image_findings",
    "verification_images_json",
    "verification_text_json",
    "call_pitch_why",
    "call_script_reasons_json",
    "lead_priority",
    "in_hail_alley",
    "corroboration",
    "observation_count",
]


def clean_phone(phone: str | None) -> str:
    if not phone:
        return ""
    digits = "".join(filter(str.isdigit, str(phone)))
    if "555" in digits:
        return ""
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return f"+1{digits}" if len(digits) > 7 else ""


def build_query(strict_spatial_only: bool) -> str:
    city_exists = "" if strict_spatial_only else """
  OR EXISTS (
    SELECT 1 FROM city_corroboration cc
    WHERE cc.state = s.state AND date(cc.event_date) = date(s.event_date)
      AND s.city IS NOT NULL AND trim(s.city) != ''
      AND cc.cty = upper(trim(s.city))
  )"""

    return f"""
WITH recent AS (
  SELECT * FROM storms
  WHERE COALESCE(magnitude, 0) >= ?
  AND date(event_date) >= date('now', '-' || ? || ' days')
),
spatial_corroboration AS (
  SELECT state, event_date,
         round(latitude, 1) AS la,
         round(longitude, 1) AS lo,
         COUNT(DISTINCT id) AS n_obs
  FROM recent
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL
  GROUP BY state, event_date, la, lo
  HAVING COUNT(DISTINCT id) >= 2
),
city_corroboration AS (
  SELECT state, event_date,
         upper(trim(city)) AS cty,
         COUNT(DISTINCT id) AS n_obs
  FROM recent
  WHERE city IS NOT NULL AND trim(city) != ''
  GROUP BY state, event_date, cty
  HAVING COUNT(DISTINCT id) >= 2
)
SELECT
  c.id AS contact_id,
  c.homeowner_name,
  c.phone_number,
  c.street_address,
  s.city,
  s.state,
  c.zip_code,
  s.event_date,
  s.magnitude,
  c.damage_score,
  c.proof_msg,
  c.structures_hit,
  c.image_findings,
  s.latitude,
  s.longitude,
  (SELECT MAX(sc.n_obs) FROM spatial_corroboration sc
     WHERE sc.state = s.state AND date(sc.event_date) = date(s.event_date)
       AND s.latitude IS NOT NULL AND s.longitude IS NOT NULL
       AND sc.la = round(s.latitude, 1) AND sc.lo = round(s.longitude, 1)
  ) AS spatial_n,
  (SELECT MAX(cc.n_obs) FROM city_corroboration cc
     WHERE cc.state = s.state AND date(cc.event_date) = date(s.event_date)
       AND s.city IS NOT NULL AND trim(s.city) != ''
       AND cc.cty = upper(trim(s.city))
  ) AS city_n
FROM contacts c
JOIN recent s ON c.event_id = s.id
WHERE COALESCE(c.damage_score, 0) >= ?
AND (
  EXISTS (
    SELECT 1 FROM spatial_corroboration sc
    WHERE sc.state = s.state AND date(sc.event_date) = date(s.event_date)
      AND s.latitude IS NOT NULL AND s.longitude IS NOT NULL
      AND sc.la = round(s.latitude, 1) AND sc.lo = round(s.longitude, 1)
  ){city_exists}
)
ORDER BY c.damage_score DESC, s.magnitude DESC
"""


def export_batch(
    db_path: str,
    output_path: str,
    *,
    days: int = 365,
    min_damage: float = 40.0,
    min_hail: float = 2.0,
    phones_only: bool = False,
    strict_spatial_only: bool = False,
    training_dir: str | None = None,
) -> dict[str, Any]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    q = build_query(strict_spatial_only)
    params = (min_hail, days, min_damage)

    cur = conn.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    td = training_dir or os.getenv("ROOF_HUNTER_TRAINING_DIR", os.path.join(base, "training_data"))

    out: list[dict[str, Any]] = []
    for r in rows:
        phone = clean_phone(r["phone_number"])
        if phones_only and not phone:
            continue

        full_name = r["homeowner_name"] or "Local Resident"
        parts = full_name.split(" ", 1)
        first = parts[0]
        last = parts[1] if len(parts) > 1 else ""

        st = normalize_state(r["state"])
        hail = float(r["magnitude"] or 0)
        damage = float(r["damage_score"] or 0)

        spatial_n = r["spatial_n"] or 0
        city_n = r["city_n"] or 0
        if spatial_n and city_n:
            corroboration = "spatial+city"
            obs = max(spatial_n, city_n)
        elif spatial_n:
            corroboration = "spatial_grid"
            obs = spatial_n
        else:
            corroboration = "city_reports"
            obs = city_n

        structs = r["structures_hit"]
        if not structs:
            structs = "Primary roof system and ancillary metals (gutters, soft metals)"

        findings = r["image_findings"] or r["proof_msg"]
        if not findings or len(str(findings)) < 10:
            findings = (
                f"Multi-report hail corridor: {hail}\" max reports; forensic damage index {damage}%. "
                f"Corroboration: {corroboration} ({obs} observations)."
            )

        in_alley = "yes" if st in HAIL_ALLEY_STATES else "no"

        cid = int(r["contact_id"])
        lat = r["latitude"]
        lon = r["longitude"]
        vpay = media_to_api_payload(
            build_media_items_for_lead(
                contact_id=cid,
                damage_score=damage,
                magnitude=hail,
                event_date=r["event_date"],
                state=st,
                city=r["city"],
                zip_code=r["zip_code"],
                latitude=float(lat) if lat is not None else None,
                longitude=float(lon) if lon is not None else None,
                proof_msg=r["proof_msg"],
                image_findings=r["image_findings"],
                training_dir=td,
            )
        )

        out.append(
            {
                "phone_number": phone,
                "first_name": first,
                "last_name": last,
                "property_address": r["street_address"],
                "city": r["city"],
                "state": st,
                "zip_code": r["zip_code"],
                "hail_date": r["event_date"],
                "hail_size_in": hail,
                "storm_type": "Hail",
                "damage_probability": damage,
                "structures_hit": structs,
                "image_findings": findings,
                "verification_images_json": json.dumps(vpay.get("verification_images") or [], ensure_ascii=False),
                "verification_text_json": json.dumps(vpay.get("verification_text") or [], ensure_ascii=False),
                "call_pitch_why": vpay.get("call_pitch_why") or "",
                "call_script_reasons_json": json.dumps(vpay.get("call_script_reasons") or [], ensure_ascii=False),
                "lead_priority": "PRIORITY_1_EMERGENCY",
                "in_hail_alley": in_alley,
                "corroboration": corroboration,
                "observation_count": obs,
            }
        )

    parent = os.path.dirname(os.path.abspath(output_path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SCHEMA, extrasaction="ignore")
        w.writeheader()
        w.writerows(out)

    return {
        "written": len(out),
        "output": output_path,
        "days": days,
        "min_damage": min_damage,
        "min_hail_in": min_hail,
        "strict_spatial_only": strict_spatial_only,
        "phones_only": phones_only,
    }


def main() -> None:
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    default_db = os.path.join(base, "leads_manifests", "authoritative_storms.db")
    default_out = os.path.join(
        base,
        "leads_manifests",
        "VERIFIED_HAIL_MULTISTATION_1YR_40PCT_2IN.csv",
    )

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=default_db, help="Path to authoritative_storms.db")
    ap.add_argument("-o", "--output", default=default_out, help="Output CSV path")
    ap.add_argument("--days", type=int, default=365, help="Lookback window (default 365)")
    ap.add_argument("--min-damage", type=float, default=40.0, help="Minimum damage_score (default 40)")
    ap.add_argument("--min-hail", type=float, default=2.0, help='Minimum reported hail inches (default 2.0")')
    ap.add_argument(
        "--phones-only",
        action="store_true",
        help="Restrict to rows with a clean 10-digit US phone (skip-trace / dial campaigns)",
    )
    ap.add_argument(
        "--strict-spatial-only",
        action="store_true",
        help="Require 0.1deg multi-report match only (exclude city-name corroboration)",
    )
    ap.add_argument(
        "--training-dir",
        default=None,
        help="Directory with verification imagery (default: ROOF_HUNTER_TRAINING_DIR or repo/training_data)",
    )
    args = ap.parse_args()

    info = export_batch(
        args.db,
        args.output,
        days=args.days,
        min_damage=args.min_damage,
        min_hail=args.min_hail,
        phones_only=args.phones_only,
        strict_spatial_only=args.strict_spatial_only,
        training_dir=args.training_dir,
    )
    print(
        f"Wrote {info['written']} rows → {info['output']} "
        f"(days={info['days']}, damage>={info['min_damage']}, hail>={info['min_hail_in']}\" in, "
        f"strict_spatial={info['strict_spatial_only']})"
    )


if __name__ == "__main__":
    main()
