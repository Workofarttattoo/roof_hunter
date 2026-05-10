"""
mrms_mesh_ingest.py
-------------------
Ingest **Maximum Estimated Size of Hail (MESH)** from NOAA/NCEP **operational MRMS**
GRIB2 grids into `leads_manifests/authoritative_storms.db`, using the same `storms`
schema as `live_hail_ingest.py`.

Data source (public, no key):
  https://mrms.ncep.noaa.gov/data/2D/MESH_Max_1440min/MRMS_MESH_Max_1440min.latest.grib2.gz

The 1440-minute product is the **24-hour running maximum** MESH swath (CONUS).

Run:
  pip install -r requirements-radar.txt
  python3 src/mrms_mesh_ingest.py
  python3 src/mrms_mesh_ingest.py --min-inches 1.5 --max-events 200 --geocode

Env:
  MRMS_MESH_URL          — full URL to a .grib2 or .grib2.gz file (overrides product/latest)
  MRMS_MESH_PRODUCT_DIR — under https://mrms.ncep.noaa.gov/data/2D/ (default MESH_Max_1440min)
  MRMS_MESH_FILE        — filename (default MRMS_MESH_Max_1440min.latest.grib2.gz)
"""

from __future__ import annotations

import argparse
import gzip
import logging
import math
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from typing import Any

import numpy as np
import requests

logger = logging.getLogger(__name__)

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(BASE, "leads_manifests", "authoritative_storms.db")
MRMS_HOST = "https://mrms.ncep.noaa.gov/data/2D"

DEFAULT_PRODUCT_DIR = os.environ.get("MRMS_MESH_PRODUCT_DIR", "MESH_Max_1440min")
DEFAULT_FILENAME = os.environ.get(
    "MRMS_MESH_FILE",
    "MRMS_MESH_Max_1440min.latest.grib2.gz",
)


def _mesh_url() -> str:
    if os.environ.get("MRMS_MESH_URL"):
        return os.environ["MRMS_MESH_URL"].strip()
    return f"{MRMS_HOST}/{DEFAULT_PRODUCT_DIR}/{DEFAULT_FILENAME}"


def download_grib(path_or_url: str, timeout: int = 180) -> tuple[str, bool]:
    """
    Ensure a local .grib2 path. If path_or_url is http(s), download and optionally gunzip
    to a temp file. Returns (path, needs_unlink).
    """
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        logger.info("Downloading %s", path_or_url)
        r = requests.get(path_or_url, timeout=timeout)
        r.raise_for_status()
        data = r.content
        if path_or_url.endswith(".gz"):
            data = gzip.decompress(data)
        tmp = tempfile.NamedTemporaryFile(suffix=".grib2", delete=False)
        tmp.write(data)
        tmp.flush()
        tmp.close()
        return tmp.name, True
    if path_or_url.endswith(".gz"):
        raw = gzip.open(path_or_url, "rb").read()
        tmp = tempfile.NamedTemporaryFile(suffix=".grib2", delete=False)
        tmp.write(raw)
        tmp.close()
        return tmp.name, True
    return path_or_url, False


def _pick_2d_data_array(ds):
    """First (lat, lon) or (latitude, longitude) variable in the GRIB dataset."""
    for _name, da in ds.data_vars.items():
        dims = set(da.dims)
        if {"latitude", "longitude"}.issubset(dims):
            return da
        if {"lat", "lon"}.issubset(dims):
            return da.rename({"lat": "latitude", "lon": "longitude"})
    raise ValueError("No 2-D gridded field found in GRIB")


def decode_mrms_mesh_grib(local_path: str) -> tuple[np.ndarray, np.ndarray, np.ndarray, datetime]:
    import xarray as xr

    with xr.open_dataset(local_path, engine="cfgrib") as ds:
        da = _pick_2d_data_array(ds)
        vals = da.values.astype(np.float32)
        lat1d = da["latitude"].values.astype(np.float64)
        lon1d = da["longitude"].values.astype(np.float64)
        lon1d = np.where(lon1d > 180, lon1d - 360.0, lon1d)

        vt = getattr(ds, "valid_time", None)
        if vt is not None:
            try:
                t64 = vt.values
                if isinstance(t64, np.datetime64):
                    sec = (t64 - np.datetime64("1970-01-01T00:00:00")) / np.timedelta64(1, "s")
                    dt = datetime.fromtimestamp(float(sec), tz=timezone.utc)
                else:
                    dt = datetime.now(timezone.utc)
            except Exception:  # noqa: BLE001
                dt = datetime.now(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
    return vals, lat1d, lon1d, dt


def _sanitize_values(vals: np.ndarray) -> np.ndarray:
    """MRMS uses NaNs / negative sentinels; valid MESH is in mm."""
    out = vals.astype(np.float64)
    out[~np.isfinite(out)] = np.nan
    out = np.where(out < 0, np.nan, out)
    out = np.where(out > 200, np.nan, out)
    return out


def _block_local_maxima(
    vals: np.ndarray,
    lat1d: np.ndarray,
    lon1d: np.ndarray,
    threshold_mm: float,
    block: int,
) -> list[tuple[float, float, float]]:
    """One candidate per block where max MESH meets threshold (fast pre-filter)."""
    ny, nx = vals.shape
    out: list[tuple[float, float, float]] = []
    for i in range(0, ny - block, block):
        for j in range(0, nx - block, block):
            sl = vals[i : i + block, j : j + block]
            if not np.any(sl >= threshold_mm):
                continue
            ii, jj = np.unravel_index(np.nanargmax(sl), sl.shape)
            gi, gj = i + int(ii), j + int(jj)
            v = vals[gi, gj]
            if not math.isfinite(v) or v < threshold_mm:
                continue
            out.append((float(lat1d[gi]), float(lon1d[gj]), float(v)))
    return out


def _sep_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance (km) between two surface points."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def nms_by_magnitude(
    candidates: list[tuple[float, float, float]],
    min_sep_km: float,
    max_keep: int,
) -> list[tuple[float, float, float]]:
    """Greedy NMS: keep highest-MESH cell in each neighborhood."""
    sorted_c = sorted(candidates, key=lambda t: -t[2])
    kept: list[tuple[float, float, float]] = []
    for lat, lon, mm in sorted_c:
        if any(_sep_km(lat, lon, a, b) < min_sep_km for a, b, _ in kept):
            continue
        kept.append((lat, lon, mm))
        if len(kept) >= max_keep:
            break
    return kept


def ingest_mrms_mesh(
    *,
    db_path: str | None = None,
    url: str | None = None,
    min_inches: float = 1.0,
    max_events: int = 400,
    block: int = 10,
    min_sep_km: float = 12.0,
    ge_codes: int = 0,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Download latest (or MRMS_MESH_URL), decode MESH, pick peaks, insert into storms.

    ge_codes: reverse-geocode up to this many rows (slow; 0 = skip).
    """
    src_url = url or _mesh_url()
    path, temp_file = download_grib(src_url)
    try:
        vals, lat1d, lon1d, valid_dt = decode_mrms_mesh_grib(path)
    finally:
        if temp_file and os.path.isfile(path):
            try:
                os.unlink(path)
            except OSError:
                pass

    vals = _sanitize_values(vals)
    threshold_mm = max(0.1, min_inches * 25.4)
    candidates = _block_local_maxima(vals, lat1d, lon1d, threshold_mm, block=max(4, int(block)))
    peaks = nms_by_magnitude(candidates, min_sep_km=min_sep_km, max_keep=max_events)

    event_date = valid_dt.strftime("%Y-%m-%d")

    if dry_run:
        return {
            "url": src_url,
            "event_date": event_date,
            "peaks": len(peaks),
            "inserted": 0,
            "skipped": 0,
            "dry_run": True,
        }

    from live_hail_ingest import (
        already_exists,
        ensure_columns,
        get_census_enrichment,
        insert_storm,
        reverse_geocode_row,
    )

    db_path = db_path or DEFAULT_DB
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_columns(conn)

    inserted = 0
    skipped = 0
    for i, (lat, lon, mm) in enumerate(peaks):
        mag_in = round(mm / 25.4, 2)
        if already_exists(conn, lat, lon, event_date):
            skipped += 1
            continue

        city, state_full, county, zipcode = "", "", "", ""
        if ge_codes > 0 and i < ge_codes:
            city, state_full, county, zipcode = reverse_geocode_row(lat, lon)
            if not state_full:
                state_full = ""

        home_val, income = get_census_enrichment(state_full or "UNKNOWN", zipcode or "")

        insert_storm(
            conn,
            {
                "event_date": event_date,
                "event_type": "Hail",
                "latitude": lat,
                "longitude": lon,
                "magnitude": mag_in,
                "city": city or "",
                "state": state_full or "",
                "county": county or "",
                "zipcode": zipcode or "",
                "median_home_value": home_val,
                "median_household_income": income,
                "source": "NOAA_MRMS_MESH",
                "wsr_id": "MRMS",
                "sevprob": min(99, int(40 + mm / 2)),
                "hail_prob": min(99, int(50 + mag_in * 12)),
                "location_label": (
                    f'MRMS MESH max ~{mag_in:g}" ({mm:.0f} mm) @ '
                    f'{lat:.3f},{lon:.3f} — {event_date}'
                ),
            },
        )
        inserted += 1

    conn.close()
    return {
        "url": src_url,
        "event_date": event_date,
        "peaks": len(peaks),
        "inserted": inserted,
        "skipped": skipped,
        "dry_run": False,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Ingest MRMS MESH hail grid into authoritative_storms.db")
    p.add_argument("--db", default=DEFAULT_DB, help="SQLite path")
    p.add_argument("--url", default=None, help="Override GRIB URL/path")
    p.add_argument("--min-inches", type=float, default=1.0, help="Minimum MESH (inches), default 1.0")
    p.add_argument("--max-events", type=int, default=400, help="Max storm rows to insert")
    p.add_argument("--block", type=int, default=10, help="Sampling block size (grid steps, ~1 km each)")
    p.add_argument("--min-sep-km", type=float, default=12.0, help="Peak separation for de-duplication")
    p.add_argument("--geocode", type=int, default=0, help="Reverse-geocode first N rows (slow)")
    p.add_argument("--dry-run", action="store_true", help="Decode and count peaks only")
    args = p.parse_args()

    try:
        summary = ingest_mrms_mesh(
            db_path=args.db,
            url=args.url,
            min_inches=args.min_inches,
            max_events=args.max_events,
            block=args.block,
            min_sep_km=args.min_sep_km,
            ge_codes=args.geocode,
            dry_run=args.dry_run,
        )
    except ImportError as e:
        print("MRMS ingest requires radar deps: pip install -r requirements-radar.txt\n", e)
        raise SystemExit(1) from e
    except Exception as e:  # noqa: BLE001
        print(
            "MRMS ingest failed (often missing ecCodes / cfgrib). "
            "Try: pip install -r requirements-radar.txt or conda install eccodes.\n",
            e,
        )
        raise SystemExit(2) from e

    print(
        f"[MRMS MESH] {summary['url']}\n"
        f"  event_date={summary['event_date']} peaks={summary['peaks']} "
        f"inserted={summary['inserted']} skipped/dup={summary['skipped']}"
        + (" (dry-run)" if summary.get("dry_run") else "")
    )


if __name__ == "__main__":
    main()
