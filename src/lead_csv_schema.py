"""
Canonical lead CSV columns for dialer/upload integrations (e.g. dynamic variables).

Upload validators often require exact header names: hail_size (not hail_size_in), etc.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any, Iterable, TextIO

# Full row shape written by reformatters and most manifests (dialing + uploads).
CANONICAL_LEAD_CSV_FIELDS: list[str] = [
    "phone_number",
    "first_name",
    "last_name",
    "property_address",
    "city",
    "state",
    "zip_code",
    "hail_date",
    "hail_size",
    "storm_type",
    "damage_probability",
    "structures_hit",
    "image_findings",
    "lead_priority",
]

# Subset some platforms validate explicitly.
UPLOAD_DYNAMIC_VARIABLES: frozenset[str] = frozenset(
    {
        "lead_priority",
        "hail_date",
        "first_name",
        "property_address",
        "city",
        "state",
        "hail_size",
        "storm_type",
        "damage_probability",
        "structures_hit",
        "image_findings",
    }
)


def _norm_key(s: str) -> str:
    return re.sub(r"\s+", "_", str(s).strip().lower())


# Source headers consumed by coerce_lead_row — omit from "extra" passthrough columns.
_CONSUMED_INPUT_HEADERS_NORM: frozenset[str] = frozenset(
    {
        _norm_key(x)
        for x in (
            "hail_size_in",
            "magnitude",
            "hailmagnitude",
            "damage_magnitude",
            "damage_score",
            "intensity",
            "proof_msg",
            "street_address",
            "address",
            "verified_address",
            "original_address",
            "phone",
            "event_date",
            "date",
            "customer_name",
            "homeowner_name",
            "full_name",
            "owner",
            "zip",
            "zipcode",
            "firstname",
            "lastname",
            "findings",
        )
    }
)


def is_preserved_extra_column(header: str) -> bool:
    if not header:
        return False
    hn = _norm_key(header)
    if hn in {_norm_key(c) for c in CANONICAL_LEAD_CSV_FIELDS}:
        return False
    if hn in _CONSUMED_INPUT_HEADERS_NORM:
        return False
    return True


def _first_nonempty(inv: dict[str, Any], *candidates: str) -> Any:
    for c in candidates:
        v = inv.get(_norm_key(c))
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return ""


def coerce_lead_row(row: dict[str, Any]) -> dict[str, Any]:
    """
    Build a row with exactly CANONICAL_LEAD_CSV_FIELDS keys.
    Maps synonyms (hail_size_in, magnitude, damage_score, etc.) into canonical names.
    """
    inv: dict[str, Any] = {}
    for k, v in row.items():
        if k is None:
            continue
        inv[_norm_key(k)] = v

    storm_type = _first_nonempty(inv, "storm_type") or "Hail"
    hail_raw = _first_nonempty(inv, "hail_size", "hail_size_in", "magnitude", "hailmagnitude")

    dmg_raw = _first_nonempty(inv, "damage_probability", "damage_score", "intensity")
    if dmg_raw != "" and dmg_raw is not None:
        try:
            dmg_clean = re.sub(r"[^\d.]", "", str(dmg_raw))
            damage_probability = float(dmg_clean) if dmg_clean else 0.0
        except ValueError:
            damage_probability = 0.0
    else:
        damage_probability = 0.0

    try:
        hail_float = float(re.sub(r'[^\d.]', "", str(hail_raw))) if hail_raw != "" else 0.0
    except ValueError:
        hail_float = 0.0

    proof = _first_nonempty(inv, "image_findings", "proof_msg", "findings")
    structures = _first_nonempty(inv, "structures_hit")
    if not structures:
        structures = (
            "Primary roof system and ancillary structures (gutters, soft metals)"
            if hail_float >= 2.0
            else "Main roof surface"
        )

    if not proof:
        proof = (
            f"Forensic index {damage_probability}% aligned with reported {storm_type.lower()} "
            f"event; property flagged for verification outreach."
        )

    priority = _first_nonempty(inv, "lead_priority")
    if not priority:
        if damage_probability > 80 or hail_float >= 2.5:
            priority = "PRIORITY_1_EMERGENCY"
        elif hail_float >= 1.5 or damage_probability > 30:
            priority = "PRIORITY_2_LIKELY_DAMAGE"
        else:
            priority = "PRIORITY_3_LOW_INTENT"

    fn = _first_nonempty(inv, "first_name", "firstname")
    if not fn:
        cust = _first_nonempty(inv, "customer_name", "homeowner_name", "full_name", "owner")
        if cust:
            parts = str(cust).strip().split(None, 1)
            fn = parts[0]
            extra_last = parts[1] if len(parts) > 1 else ""

    ln = _first_nonempty(inv, "last_name", "lastname")
    if not ln:
        cust = _first_nonempty(inv, "customer_name", "homeowner_name", "full_name", "owner")
        if cust:
            parts = str(cust).strip().split(None, 1)
            ln = parts[1] if len(parts) > 1 else ""

    out: dict[str, Any] = {
        "phone_number": str(_first_nonempty(inv, "phone_number", "phone") or "").strip(),
        "first_name": str(fn or "").strip(),
        "last_name": str(ln or "").strip(),
        "property_address": str(
            _first_nonempty(inv, "property_address", "street_address", "address", "verified_address")
            or ""
        ).strip(),
        "city": str(_first_nonempty(inv, "city") or "").strip(),
        "state": str(_first_nonempty(inv, "state") or "").strip(),
        "zip_code": str(_first_nonempty(inv, "zip_code", "zip", "zipcode") or "").strip(),
        "hail_date": str(_first_nonempty(inv, "hail_date", "event_date", "date", "event date") or "").strip(),
        "hail_size": str(hail_raw).strip() if hail_raw != "" else (str(hail_float) if hail_float else ""),
        "storm_type": str(storm_type).strip(),
        "damage_probability": damage_probability,
        "structures_hit": str(structures).strip(),
        "image_findings": str(proof).strip(),
        "lead_priority": str(priority).strip(),
    }
    return out


def row_with_extras(base: dict[str, Any], extras: dict[str, Any]) -> dict[str, Any]:
    """Merge canonical lead fields with extra export columns (verification JSON, etc.)."""
    merged = {**base, **extras}
    # Canonical keys from base take precedence
    for k in CANONICAL_LEAD_CSV_FIELDS:
        merged[k] = base.get(k, merged.get(k, ""))
    return merged


def validate_upload_headers(fieldnames: list[str]) -> list[str]:
    """Return missing required dynamic-variable column names."""
    have = {_norm_key(h) for h in fieldnames if h}
    missing: list[str] = []
    for req in sorted(UPLOAD_DYNAMIC_VARIABLES):
        if _norm_key(req) not in have:
            missing.append(req)
    return missing


def _stringify_cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        if v == int(v):
            return str(int(v))
        return str(v)
    return str(v).strip()


def row_to_canonical_ordered(row: dict[str, Any]) -> dict[str, str]:
    """Run synonym coercion, then emit one string per canonical column in fixed order."""
    base = coerce_lead_row(row)
    return {k: _stringify_cell(base.get(k, "")) for k in CANONICAL_LEAD_CSV_FIELDS}


def normalize_file_to_canonical_rows(path: str | Path) -> list[dict[str, str]]:
    """Read arbitrary header CSV; return canonical rows (stable columns, string cells)."""
    path = Path(path)
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return [
            row_to_canonical_ordered(dict(r))
            for r in reader
            if any((v or "").strip() for v in r.values())
        ]


def write_canonical_leads_csv(
    dest: str | Path | TextIO,
    rows: Iterable[dict[str, Any]],
    *,
    extras_headers: list[str] | None = None,
) -> int:
    """
    Write CSV: header = CANONICAL_LEAD_CSV_FIELDS + optional extras.
    Each row is coerced so category names match across all inbound pipelines (e.g. S3 → DB).
    """
    extras = list(extras_headers or [])
    for ex in extras:
        if ex in CANONICAL_LEAD_CSV_FIELDS:
            raise ValueError(f"extras header duplicates canonical field: {ex}")

    fieldnames = list(CANONICAL_LEAD_CSV_FIELDS) + extras
    count = 0
    close_after = False
    if isinstance(dest, (str, Path)):
        f = open(dest, "w", newline="", encoding="utf-8")
        close_after = True
    else:
        f = dest
    try:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for raw in rows:
            ordered = row_to_canonical_ordered(raw)
            extras_out = {h: _stringify_cell((raw or {}).get(h)) for h in extras}
            w.writerow({**ordered, **extras_out})
            count += 1
    finally:
        if close_after:
            f.close()
    return count
