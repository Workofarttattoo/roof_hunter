#!/usr/bin/env python3
"""Backfill lead_verification_media for all contacts (batch). Safe to re-run."""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "src") not in sys.path:
    sys.path.insert(0, str(_REPO / "src"))

from lead_verification_media import (  # noqa: E402
    build_media_items_for_lead,
    ensure_lead_verification_media_table,
    replace_media_for_contact,
)

DEFAULT_DB = _REPO / "leads_manifests" / "authoritative_storms.db"
TRAINING = _REPO / "training_data"


def main() -> None:
    db_path = os.getenv("ROOF_HUNTER_DB_PATH", str(DEFAULT_DB))
    training_dir = os.getenv("ROOF_HUNTER_TRAINING_DIR", str(TRAINING))
    conn = sqlite3.connect(db_path, timeout=120)
    conn.row_factory = sqlite3.Row
    ensure_lead_verification_media_table(conn)
    cur = conn.execute(
        """
        SELECT c.id AS cid, c.damage_score, c.proof_msg, c.image_findings, c.zip_code,
               s.magnitude, s.event_date, s.state, s.city, s.latitude, s.longitude
        FROM contacts c
        JOIN storms s ON c.event_id = s.id
        """
    )
    n = 0
    while True:
        chunk = cur.fetchmany(2000)
        if not chunk:
            break
        for row in chunk:
            cid = row["cid"]
            built = build_media_items_for_lead(
                contact_id=cid,
                damage_score=float(row["damage_score"] or 0),
                magnitude=float(row["magnitude"] or 0),
                event_date=row["event_date"],
                state=row["state"],
                city=row["city"],
                zip_code=row["zip_code"],
                latitude=float(row["latitude"]) if row["latitude"] is not None else None,
                longitude=float(row["longitude"]) if row["longitude"] is not None else None,
                proof_msg=row["proof_msg"],
                image_findings=row["image_findings"],
                training_dir=training_dir,
            )
            replace_media_for_contact(conn, cid, built)
            n += 1
            if n % 5000 == 0:
                print(f"Processed {n} contacts...", flush=True)
    conn.close()
    print(f"Done. Updated verification media for {n} contacts.")


if __name__ == "__main__":
    main()
