"""
Lead ↔ verification imagery: each asset used for a contact is stored with a plain-English
rationale and dialer hook. Populated by populate_lead_verification_media.py or lazily on read.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any


def ensure_lead_verification_media_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lead_verification_media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER NOT NULL,
            image_path TEXT NOT NULL,
            role TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0,
            rationale TEXT NOT NULL,
            call_hook TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(contact_id, image_path)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lead_verification_media_contact "
        "ON lead_verification_media(contact_id)"
    )
    conn.commit()


def _training_file_ok(training_dir: str, rel_name: str) -> bool:
    p = os.path.join(training_dir, rel_name)
    try:
        return os.path.isfile(p) and os.path.getsize(p) > 0
    except OSError:
        return False


def build_media_items_for_lead(
    *,
    contact_id: int,
    damage_score: float,
    magnitude: float,
    event_date: str | None,
    state: str | None,
    city: str | None,
    zip_code: str | None,
    latitude: float | None,
    longitude: float | None,
    proof_msg: str | None,
    image_findings: str | None,
    training_dir: str,
) -> list[dict[str, Any]]:
    """Return rows ready for INSERT (no DB)."""
    items: list[dict[str, Any]] = []
    ds = float(damage_score or 0)
    mag = float(magnitude or 0)
    loc = ", ".join(x for x in [city, state] if x)
    storm_bit = f'{mag}" hail on {event_date or "recorded event"}'
    base_why = (
        f"Storm-linked lead: {storm_bit} in {loc or state or 'target market'}. "
        f"Damage model index {ds:.0f}% flags this property for scripted outreach."
    )

    order = 0

    # Dedicated OKC forensic pair (legacy high-value IDs)
    if contact_id in (5803, 7837, 930):
        bfn = f"OKC-{contact_id}-482_before.jpg"
        afn = f"OKC-{contact_id}-482_after.jpg"
        if _training_file_ok(training_dir, bfn) and _training_file_ok(training_dir, afn):
            items.append(
                {
                    "image_path": f"/images/{bfn}",
                    "role": "pre_storm",
                    "sort_order": order,
                    "rationale": "Paired pre/post capture tied to this lead id for consistent QA on calls.",
                    "call_hook": f"Use before/after stills for lead #{contact_id}: establishes visual proof before quoting.",
                }
            )
            order += 1
            items.append(
                {
                    "image_path": f"/images/{afn}",
                    "role": "post_storm",
                    "sort_order": order,
                    "rationale": "Post-event frame shows impact alignment with reported hail severity.",
                    "call_hook": "Reference the flagged post-storm tile when explaining why the address was prioritized.",
                }
            )
            order += 1

    # Forensic geotile if present on disk
    if latitude is not None and longitude is not None:
        fn = f"forensic_{latitude:.4f}_{longitude:.4f}.png"
        if _training_file_ok(training_dir, fn):
            items.append(
                {
                    "image_path": f"/images/{fn}",
                    "role": "forensic_tile",
                    "sort_order": order,
                    "rationale": (
                        f"Raster tile keyed to storm coordinates ({latitude:.4f},{longitude:.4f}) "
                        "for footprint verification against the event."
                    ),
                    "call_hook": "Mention the hail footprint tile matching the NOAA-reported cell for this property.",
                }
            )
            order += 1

    # Comparative hail gallery (when damage band warrants imagery for calls)
    if ds > 30 and _training_file_ok(training_dir, "hail_68102_11.png") and _training_file_ok(
        training_dir, "hail_68102_15.png"
    ):
        if not any("hail_68102_11" in x["image_path"] for x in items):
            items.append(
                {
                    "image_path": "/images/hail_68102_11.png",
                    "role": "damage_reference_a",
                    "sort_order": order,
                    "rationale": (
                        "Reference comparative hail-impact signature used when property-specific "
                        f"imagery is incomplete; supports narrative for {ds:.0f}% damage band."
                    ),
                    "call_hook": (
                        "Script: tie homeowner pain to high-index damage and typical granule/soft-metal loss patterns."
                    ),
                }
            )
            order += 1
        if not any("hail_68102_15" in x["image_path"] for x in items):
            items.append(
                {
                    "image_path": "/images/hail_68102_15.png",
                    "role": "damage_reference_b",
                    "sort_order": order,
                    "rationale": "Paired reference tile for call consistency across high-severity hail cohort.",
                    "call_hook": "Second still backs up inspection ask without overstating property-specific proof.",
                }
            )
            order += 1

    # Text-derived proof from DB
    if proof_msg and len(proof_msg.strip()) > 20:
        items.append(
            {
                "image_path": "__text_proof__",
                "role": "proof_message",
                "sort_order": order,
                "rationale": proof_msg.strip()[:800],
                "call_hook": f"Lead proof_msg excerpt for dialer context: {proof_msg.strip()[:200]}",
            }
        )
        order += 1
    elif image_findings and len(image_findings.strip()) > 10:
        items.append(
            {
                "image_path": "__text_findings__",
                "role": "image_findings",
                "sort_order": order,
                "rationale": image_findings.strip()[:800],
                "call_hook": f"Forensic notes: {image_findings.strip()[:200]}",
            }
        )
        order += 1

    if not items:
        items.append(
            {
                "image_path": "__metadata_only__",
                "role": "pending_imagery",
                "sort_order": 0,
                "rationale": base_why,
                "call_hook": (
                    f"Outbound: {storm_bit}; damage index {ds:.0f}%; queue property imagery harvest if required."
                ),
            }
        )

    return items


def replace_media_for_contact(conn: sqlite3.Connection, contact_id: int, rows: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM lead_verification_media WHERE contact_id = ?", (contact_id,))
    for r in rows:
        conn.execute(
            """
            INSERT INTO lead_verification_media
            (contact_id, image_path, role, sort_order, rationale, call_hook)
            VALUES (?,?,?,?,?,?)
            """,
            (
                contact_id,
                r["image_path"],
                r.get("role"),
                int(r.get("sort_order", 0)),
                r["rationale"],
                r.get("call_hook") or "",
            ),
        )
    conn.commit()


def fetch_media_rows(conn: sqlite3.Connection, contact_id: int) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT image_path, role, sort_order, rationale, call_hook
        FROM lead_verification_media
        WHERE contact_id = ?
        ORDER BY sort_order ASC, id ASC
        """,
        (contact_id,),
    )
    return [dict(zip(["image_path", "role", "sort_order", "rationale", "call_hook"], row)) for row in cur.fetchall()]


def ensure_media_cached(
    conn: sqlite3.Connection,
    *,
    contact_id: int,
    row_contact: sqlite3.Row,
    row_storm: sqlite3.Row,
    training_dir: str,
) -> list[dict[str, Any]]:
    existing = fetch_media_rows(conn, contact_id)
    if existing:
        return existing
    lat = row_storm["latitude"] if "latitude" in row_storm.keys() else None
    lon = row_storm["longitude"] if "longitude" in row_storm.keys() else None
    built = build_media_items_for_lead(
        contact_id=contact_id,
        damage_score=float(row_contact["damage_score"] or 0),
        magnitude=float(row_storm["magnitude"] or 0),
        event_date=row_storm["event_date"],
        state=row_storm["state"],
        city=row_storm["city"],
        zip_code=row_contact["zip_code"],
        latitude=float(lat) if lat is not None else None,
        longitude=float(lon) if lon is not None else None,
        proof_msg=row_contact["proof_msg"] if "proof_msg" in row_contact.keys() else None,
        image_findings=row_contact["image_findings"] if "image_findings" in row_contact.keys() else None,
        training_dir=training_dir,
    )
    replace_media_for_contact(conn, contact_id, built)
    return fetch_media_rows(conn, contact_id)


def media_to_api_payload(media_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Shape for JSON API: separate visual assets from text-only markers."""
    visuals = []
    text_evidence = []
    hooks: list[str] = []
    for m in media_rows:
        if m.get("call_hook"):
            hooks.append(m["call_hook"])
        if m["image_path"].startswith("__"):
            text_evidence.append(
                {
                    "type": m["role"],
                    "rationale": m["rationale"],
                    "call_hook": m.get("call_hook"),
                }
            )
        else:
            visuals.append(
                {
                    "url": m["image_path"],
                    "role": m.get("role"),
                    "why": m["rationale"],
                    "call_hook": m.get("call_hook"),
                }
            )
    return {
        "verification_images": visuals,
        "verification_text": text_evidence,
        "call_script_reasons": hooks,
        "call_pitch_why": " ".join(hooks[:3]) if hooks else None,
    }
