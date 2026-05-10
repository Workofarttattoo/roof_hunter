"""
Normalize lead CSV phone_number to strict US E.164 (+1 + 10 digits).

- Valid NANP only (NPA/exchange first digit 2–9).
- Strips formatting; rejects wrong lengths and non-digit junk (e.g. Excel "....0").

Invalid or empty phones:
  --on-invalid drop      Remove the row (default).
  --on-invalid synthetic Fill with a deterministic, NANP-shaped number that avoids
                         obvious patterns (555, toll-free NPAs). Adds column
                         phone_is_synthetic=yes for those rows — still not safe to dial
                         blindly; use for schema/testing only.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import shutil
from typing import Any

# US NANP: NPA NXX-XXXX (10 digits), no 0/1 in first position of NPA or exchange (NXX).
_US_NANP = re.compile(r"^([2-9]\d{2})([2-9]\d{2})(\d{4})$")

# NPAs to skip for synthetic placeholders (toll-free, obvious blocks).
_BLOCKED_NPA = frozenset(
    {
        "800",
        "822",
        "833",
        "844",
        "855",
        "866",
        "877",
        "880",
        "888",
        "887",
        "899",
        "900",
        "555",
    }
)

def _digits(s: str) -> str:
    t = str(s or "").strip()
    if re.fullmatch(r"\d+\.0+", t):
        t = t.split(".")[0]
    return re.sub(r"\D", "", t)


def to_us_e164(raw: str) -> str | None:
    """Return +1XXXXXXXXXX or None if not a valid US NANP mobile/landline pattern."""
    d = _digits(raw)
    if not d:
        return None

    if len(d) == 10:
        nat = d
    elif len(d) == 11 and d.startswith("1"):
        nat = d[1:]
    else:
        return None

    if len(nat) != 10 or not _US_NANP.match(nat):
        return None
    return "+1" + nat


def _synthetic_e164(seed: str) -> str:
    """Deterministic NANP-shaped +1 number; avoids common fake/toll-free NPAs."""
    h = hashlib.sha256(seed.encode("utf-8")).digest()
    for i in range(min(200, len(h) - 3)):
        x = int.from_bytes(h[i : i + 4], "big") % (10**10)
        n = f"{x:010d}"
        if not _US_NANP.match(n):
            continue
        npa = n[:3]
        if npa in _BLOCKED_NPA or npa[1:] == "11":
            continue
        if n[3:6] == "555" or n[-7:] == "1111111":
            continue
        if n in ("1234567890", "9876543210"):
            continue
        return "+1" + n
    # Last resort: still valid pattern, unusual block
    return "+12738594022"


def clean_csv(
    path: str,
    *,
    backup: bool = True,
    phone_column: str = "phone_number",
    on_invalid: str = "drop",
) -> tuple[int, int, int, int]:
    """
    Overwrite CSV in place (after optional backup).
    Returns (kept, dropped, reformatted_to_e164, synthetic_filled).
    """
    if on_invalid not in ("drop", "synthetic"):
        raise ValueError("on_invalid must be 'drop' or 'synthetic'")

    path = os.path.abspath(path)
    if backup:
        shutil.copy2(path, path + ".bak")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        if phone_column not in fieldnames:
            raise SystemExit(f"Column {phone_column!r} not in CSV headers: {fieldnames}")
        rows_in = list(reader)

    kept: list[dict[str, Any]] = []
    dropped = 0
    reformatted = 0
    synthetic_filled = 0

    for idx, row in enumerate(rows_in):
        raw = row.get(phone_column, "") or ""
        e164 = to_us_e164(raw)

        if e164:
            if (raw or "").strip() != e164:
                reformatted += 1
            row[phone_column] = e164
            kept.append(row)
            continue

        if on_invalid == "drop":
            dropped += 1
            continue

        seed = "|".join(
            str(row.get(k, "") or "")
            for k in ("property_address", "city", "state", "zip_code", "last_name", "first_name")
        ) + f"|row{idx}"
        row[phone_column] = _synthetic_e164(seed)
        row["phone_is_synthetic"] = "yes"
        synthetic_filled += 1
        kept.append(row)

    if on_invalid == "synthetic" and synthetic_filled > 0:
        if "phone_is_synthetic" not in fieldnames:
            fieldnames.append("phone_is_synthetic")
        for row in kept:
            row.setdefault("phone_is_synthetic", "no")

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(kept)

    return len(kept), dropped, reformatted, synthetic_filled


def main() -> None:
    p = argparse.ArgumentParser(description="Normalize US phones to strict +E.164.")
    p.add_argument("csv_path", help="Path to CSV (updated in place)")
    p.add_argument("--no-backup", action="store_true")
    p.add_argument(
        "--on-invalid",
        choices=("drop", "synthetic"),
        default="drop",
        help="drop row or fill with tagged synthetic NANP-shaped number (default: drop)",
    )
    p.add_argument("--phone-column", default="phone_number")
    args = p.parse_args()
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    path = args.csv_path
    if not os.path.isabs(path):
        path = os.path.join(base, path)
    kept, dropped, reformatted, synth = clean_csv(
        path,
        backup=not args.no_backup,
        phone_column=args.phone_column,
        on_invalid=args.on_invalid,
    )
    msg = f"Kept {kept} | dropped {dropped} | reformatted {reformatted} | synthetic_fill {synth} → {path}"
    print(msg)
    if args.on_invalid == "synthetic" and synth:
        print("Note: rows with phone_is_synthetic=yes are placeholders — verify before dialing.")


if __name__ == "__main__":
    main()
