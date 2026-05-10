"""
csv_phone_enricher.py
---------------------
Fill phone_number (and optional email) on lead CSVs using:

  1) RentCast property lookup (optional) → owner name from street address
  2) Apollo people/match → phone / email

Expects canonical columns like FORT_WORTH_TARGET_BATCH (property_address, city, state, zip_code, ...).

Usage:
  python src/csv_phone_enricher.py \\
    --input leads_manifests/FORT_WORTH_TARGET_BATCH_20260429_2124.csv

Env:
  APOLLO_API_KEY   — required for phone enrichment
  RENTCAST_API_KEY — optional; improves match rate when leads use placeholder \"Homeowner\"
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import time
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
APOLLO_URL = "https://api.apollo.io/v1/people/match"
RENTCAST_URL = "https://api.rentcast.io/v1/properties"


def _norm(s: str | None) -> str:
    return (s or "").strip()


def _looks_placeholder_name(fn: str, ln: str) -> bool:
    fn_l = fn.lower()
    if not fn and not ln:
        return True
    if fn_l in ("homeowner", "owner", "resident", "unknown"):
        return True
    return False


def rentcast_owner_name(street_line: str, api_key: str) -> tuple[str | None, str | None]:
    """
    Return (first_name, last_name) from RentCast owner record, or (None, None).
    """
    clean = street_line.replace(", ,", ",").strip()
    if not clean:
        return None, None
    try:
        r = requests.get(
            RENTCAST_URL,
            params={"address": clean},
            headers={"accept": "application/json", "X-Api-Key": api_key},
            timeout=30,
        )
    except requests.RequestException as e:
        logger.warning("RentCast request failed: %s", e)
        return None, None

    if r.status_code != 200:
        logger.warning("RentCast %s for %s", r.status_code, clean[:80])
        return None, None

    data = r.json()
    if not isinstance(data, list) or not data:
        return None, None
    owner_block = data[0].get("owner") or {}
    names = owner_block.get("names") or []
    if not names:
        return None, None
    full = _norm(str(names[0]))
    if not full:
        return None, None
    parts = full.split(None, 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def apollo_match(
    api_key: str,
    first: str,
    last: str,
    street: str,
    city: str,
    state: str,
    zip_code: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "first_name": first or None,
        "last_name": last or None,
        "street_address": street or None,
        "city": city or None,
        "state": state or None,
        "zip_code": zip_code or None,
    }
    payload = {k: v for k, v in payload.items() if v}

    r = requests.post(
        APOLLO_URL,
        headers={
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "X-Api-Key": api_key,
        },
        json=payload,
        timeout=30,
    )
    if r.status_code == 429:
        return {"_error": "rate_limit", "status": 429}
    if r.status_code != 200:
        return {"_error": "http", "status": r.status_code, "body": r.text[:500]}
    person = r.json().get("person") or {}
    phones = person.get("phone_numbers") or []
    phone = person.get("sanitized_phone")
    if not phone and phones:
        phone = phones[0].get("sanitized_number")
    return {
        "phone": phone,
        "email": person.get("email"),
        "apollo_id": person.get("id"),
    }


def enrich_csv(
    input_path: str,
    output_path: str | None,
    *,
    limit: int | None = None,
    sleep_s: float = 0.6,
    skip_rentcast: bool = False,
) -> str:
    apollo_key = os.getenv("APOLLO_API_KEY")
    if not apollo_key:
        raise SystemExit(
            "APOLLO_API_KEY is not set. Add it to .env to enrich phones via Apollo."
        )

    rentcast_key = os.getenv("RENTCAST_API_KEY")
    if not skip_rentcast and not rentcast_key:
        logger.info("RENTCAST_API_KEY not set — using CSV names only (often weak for 'Homeowner' rows).")

    out_path = output_path or re.sub(r"\.csv$", "_phone_enriched.csv", input_path, flags=re.I)
    if out_path == input_path:
        out_path = input_path.replace(".csv", "_phone_enriched.csv")

    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        extras = ["email", "enrichment_source"]
        for e in extras:
            if e not in fieldnames:
                fieldnames.append(e)

        rows_out: list[dict[str, Any]] = []
        enrich_attempts = 0

        def _defaults(r: dict[str, Any]) -> None:
            r.setdefault("email", "")
            r.setdefault("enrichment_source", "")

        for row in reader:
            row = {k: (v if v is not None else "") for k, v in row.items()}
            _defaults(row)

            phone_existing = _norm(row.get("phone_number"))
            if phone_existing:
                rows_out.append(row)
                continue

            street = _norm(row.get("property_address"))
            city = _norm(row.get("city"))
            state = _norm(row.get("state"))
            zip_code = _norm(row.get("zip_code"))
            fn = _norm(row.get("first_name"))
            ln = _norm(row.get("last_name"))

            if not street:
                row["enrichment_source"] = "skipped_no_address"
                rows_out.append(row)
                continue

            if limit is not None and enrich_attempts >= limit:
                row["enrichment_source"] = "not_attempted_limit"
                rows_out.append(row)
                continue

            src_bits: list[str] = []

            if rentcast_key and not skip_rentcast and _looks_placeholder_name(fn, ln):
                line = ", ".join(p for p in [street, f"{city}, {state} {zip_code}".strip()] if p)
                rf, rl = rentcast_owner_name(line, rentcast_key)
                if rf:
                    fn, ln = rf, rl or ""
                    row["first_name"], row["last_name"] = fn, ln
                    src_bits.append("rentcast_name")
                    time.sleep(sleep_s)

            if _looks_placeholder_name(fn, ln):
                fn, ln = "", ""

            logger.info("Apollo match: %s %s @ %s, %s", fn, ln, street, city)
            res = apollo_match(apollo_key, fn, ln, street, city, state, zip_code)
            if res.get("_error") == "rate_limit":
                logger.warning("Apollo rate limited — sleeping 5s")
                time.sleep(5)
                res = apollo_match(apollo_key, fn, ln, street, city, state, zip_code)

            if res.get("_error"):
                logger.error("Apollo error: %s", res)
                row["enrichment_source"] = f"apollo_error_{res.get('status')}"
            else:
                if res.get("phone"):
                    row["phone_number"] = res["phone"]
                    src_bits.append("apollo")
                if res.get("email"):
                    row["email"] = res["email"]
                row["enrichment_source"] = "+".join(src_bits) if src_bits else "apollo_no_phone"

            enrich_attempts += 1
            rows_out.append(row)
            time.sleep(sleep_s)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows_out:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    logger.info("Wrote %s rows → %s", len(rows_out), out_path)
    return out_path


def main() -> None:
    p = argparse.ArgumentParser(description="Enrich lead CSV with phones (RentCast + Apollo).")
    p.add_argument("--input", required=True, help="Input CSV path")
    p.add_argument("--output", default=None, help="Output CSV (default: *_phone_enriched.csv)")
    p.add_argument("--limit", type=int, default=None, help="Max rows to attempt (excluding already-has-phone)")
    p.add_argument("--sleep", type=float, default=0.6, help="Delay between API calls (seconds)")
    p.add_argument("--skip-rentcast", action="store_true", help="Do not call RentCast for owner names")
    args = p.parse_args()

    path = args.input
    if not os.path.isabs(path):
        path = os.path.join(BASE_DIR, path)
    enrich_csv(
        path,
        args.output,
        limit=args.limit,
        sleep_s=args.sleep,
        skip_rentcast=args.skip_rentcast,
    )


if __name__ == "__main__":
    main()
