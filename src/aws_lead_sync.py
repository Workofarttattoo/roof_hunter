"""
Pull new lead rows from AWS S3 (CSV) and upsert into authoritative_storms.db.
Configure via environment (typical for EKS / ECS / local .env):

  AWS_REGION=us-east-1
  AWS_LEADS_BUCKET=my-bucket
  AWS_LEADS_OBJECT_KEY=roof-hunter/inbound/leads.csv   # optional, default below
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY           # or instance role / IRSA

Optional: SYNC_API_KEY — if set, HTTP sync endpoint must send header X-Roof-Hunter-Sync.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lead_csv_schema import coerce_lead_row

logger = logging.getLogger(__name__)

DEFAULT_OBJECT_KEY = "roof-hunter/leads/latest.csv"

US_STATE_ABBR = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}


def normalize_state(state: str) -> str:
    s = (state or "").strip()
    if not s:
        return ""
    if len(s) == 2:
        return s.upper()
    return US_STATE_ABBR.get(s.lower(), s[:2].upper())


def _norm_header(h: str) -> str:
    return re.sub(r"\s+", "_", (h or "").strip().lower())


def _row_dict(reader_row: dict[str, Any]) -> dict[str, str]:
    return {_norm_header(k): (str(v).strip() if v is not None else "") for k, v in reader_row.items()}


def _pick(row: dict[str, str], *keys: str, default: str = "") -> str:
    for key in keys:
        nk = _norm_header(key)
        v = row.get(nk)
        if v:
            return v
    return default


def _pick_float(row: dict[str, str], *keys: str, default: float = 0.0) -> float:
    raw = _pick(row, *keys, default="")
    if not raw:
        return default
    try:
        return float(re.sub(r"[^\d.\-]", "", raw))
    except ValueError:
        return default


def _ensure_storm_schema(c: sqlite3.Cursor) -> None:
    for col, decl in [
        ("source", "TEXT"),
        ("latitude", "REAL"),
        ("longitude", "REAL"),
    ]:
        try:
            c.execute(f"ALTER TABLE storms ADD COLUMN {col} {decl}")
        except sqlite3.OperationalError:
            pass


def _ensure_contact_columns(c: sqlite3.Cursor) -> None:
    for col, decl in [
        ("damage_score", "REAL DEFAULT 0"),
        ("qualification_status", "TEXT DEFAULT 'PENDING'"),
        ("zip_code", "TEXT"),
        ("status", "TEXT"),
        ("proof_msg", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE contacts ADD COLUMN {col} {decl}")
        except sqlite3.OperationalError:
            pass


def _ensure_meta_table(c: sqlite3.Cursor) -> None:
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_meta (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
        """
    )


def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO ingest_meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        (key, value),
    )


def _get_s3_body(bucket: str, key: str) -> bytes:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    client = boto3.client("s3", region_name=region)
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()
    except (ClientError, BotoCoreError) as e:
        logger.exception("S3 get_object failed: %s/%s", bucket, key)
        raise RuntimeError(f"S3 download failed: {e}") from e


def sync_from_s3(
    db_path: str,
    bucket: str,
    object_key: str,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    body = _get_s3_body(bucket, object_key)
    text = body.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSV has no header row")

    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        c = conn.cursor()
        _ensure_storm_schema(c)
        _ensure_contact_columns(c)
        _ensure_meta_table(c)

        imported = 0
        skipped = 0
        errors: list[str] = []

        for i, raw in enumerate(reader):
            try:
                canon = coerce_lead_row(dict(raw))
                row = _row_dict(raw)

                street = (
                    (canon.get("property_address") or "").strip()
                    or _pick(row, "address", "street_address", "street", "property_address")
                )
                zip_c = (
                    (canon.get("zip_code") or "").strip()
                    or _pick(row, "zip", "zip_code", "zipcode")
                )
                city = (canon.get("city") or "").strip() or _pick(row, "city")
                state_raw = (canon.get("state") or "").strip() or _pick(row, "state")
                state = normalize_state(state_raw)

                event_date = (
                    (canon.get("hail_date") or "").strip()
                    or _pick(
                        row,
                        "event_date",
                        "hail_date",
                        "date",
                        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    )
                )
                hail_cell = str(canon.get("hail_size") or "").strip()
                try:
                    magnitude = float(re.sub(r"[^\d.\-]", "", hail_cell)) if hail_cell else 0.0
                except ValueError:
                    magnitude = 0.0
                if magnitude <= 0:
                    magnitude = _pick_float(row, "hail_magnitude", "magnitude", "hail", "hail_size", default=1.75)

                dmg = canon.get("damage_probability")
                if dmg is not None and str(dmg).strip():
                    try:
                        damage_score = float(dmg)
                    except (TypeError, ValueError):
                        damage_score = _pick_float(
                            row, "damage_score", "ai_damage_score_%", "ai damage score %", default=min(100.0, magnitude * 12.0)
                        )
                else:
                    damage_score = _pick_float(
                        row, "damage_score", "ai_damage_score_%", "ai damage score %", default=min(100.0, magnitude * 12.0)
                    )

                if not street or not zip_c or not state:
                    skipped += 1
                    continue

                lat = _pick_float(row, "latitude", "lat", default=0.0) or None
                lon = _pick_float(row, "longitude", "lon", "lng", default=0.0) or None
                if lat == 0.0:
                    lat = None
                if lon == 0.0:
                    lon = None

                fn = (canon.get("first_name") or "").strip()
                ln = (canon.get("last_name") or "").strip()
                homeowner = f"{fn} {ln}".strip() or _pick(row, "homeowner_name", "name", default="Homeowner")
                phone = (canon.get("phone_number") or "").strip() or _pick(row, "phone", "phone_number", default="")

                qual = _pick(row, "qualification_status", default="PENDING") or "PENDING"
                proof = (
                    (canon.get("image_findings") or "").strip()
                    or _pick(
                        row,
                        "proof_msg",
                        "image_findings",
                        default=f"Imported from S3 ({bucket}). Hail {magnitude}\" on {event_date}.",
                    )
                )

                c.execute(
                    "SELECT id FROM contacts WHERE street_address = ? AND COALESCE(zip_code, '') = ?",
                    (street, zip_c),
                )
                if c.fetchone():
                    skipped += 1
                    continue

                c.execute(
                    """
                    SELECT id FROM storms
                    WHERE city = ? AND state = ? AND event_date = ?
                    AND ABS(COALESCE(magnitude, 0) - ?) < 0.001
                    LIMIT 1
                    """,
                    (city, state, event_date, magnitude),
                )
                storm_row = c.fetchone()
                if storm_row:
                    event_id = storm_row["id"]
                else:
                    c.execute(
                        """
                        INSERT INTO storms
                        (event_date, event_type, state, city, magnitude, zipcode, latitude, longitude, source)
                        VALUES (?, 'Hail', ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (event_date, state, city, magnitude, zip_c, lat, lon, "AWS_S3"),
                    )
                    event_id = c.lastrowid

                if not dry_run:
                    c.execute(
                        """
                        INSERT INTO contacts
                        (event_id, street_address, homeowner_name, phone_number, status,
                         damage_score, proof_msg, qualification_status, zip_code)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            event_id,
                            street,
                            homeowner,
                            phone or None,
                            f"AWS_IMPORT_{damage_score:.0f}%",
                            damage_score,
                            proof,
                            qual,
                            zip_c,
                        ),
                    )
                imported += 1
            except Exception as e:
                errors.append(f"row {i + 2}: {e}")
                logger.exception("AWS lead row failed")

        if not dry_run:
            now = datetime.now(timezone.utc).isoformat()
            _set_meta(conn, "last_aws_sync", now)
            _set_meta(conn, "last_aws_bucket", bucket)
            _set_meta(conn, "last_aws_key", object_key)
            _set_meta(conn, "last_aws_import_count", str(imported))
            conn.commit()
        else:
            conn.rollback()

        return {
            "ok": True,
            "dry_run": dry_run,
            "imported": imported,
            "skipped": skipped,
            "errors": errors[:25],
            "bucket": bucket,
            "key": object_key,
        }
    finally:
        conn.close()


def sync_from_env(db_path: str | None = None, *, dry_run: bool = False) -> dict[str, Any]:
    path = (db_path or os.getenv("ROOF_HUNTER_DB_PATH") or "").strip()
    if not path:
        path = str(Path(__file__).resolve().parent.parent / "leads_manifests" / "authoritative_storms.db")
    bucket = os.getenv("AWS_LEADS_BUCKET", "").strip()
    key = os.getenv("AWS_LEADS_OBJECT_KEY", DEFAULT_OBJECT_KEY).strip()
    if not bucket:
        raise RuntimeError("AWS_LEADS_BUCKET is not set")
    return sync_from_s3(path, bucket, key, dry_run=dry_run)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import argparse

    parser = argparse.ArgumentParser(description="Sync leads from S3 into authoritative_storms.db")
    parser.add_argument("--dry-run", action="store_true", help="Parse CSV but do not write DB")
    args = parser.parse_args()
    out = sync_from_env(dry_run=args.dry_run)
    print(out)
