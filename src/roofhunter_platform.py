"""
RoofHunter platform tables: client auth, admin tickets, zip-tier pricing, pilot markets,
regional daily verification targets. Used by dashboard_api.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Base unlock price (USD) before zip tier multiplier
BASE_LEAD_PRICE_USD = 149.0

# Premium ZIP prefixes / full zips → higher revenue tiers (-edit / extend via DB table zip_price_tier)
DEFAULT_PLATINUM_ZIPS = frozenset(
    {
        "73102",
        "73103",
        "73116",
        "73120",  # OKC corridor
        "73013",
        "73034",  # Edmond
        "75201",
        "75225",  # Dallas pockets
        "78701",
        "78746",  # Austin
        "77005",
        "77024",  # Houston
        "76102",  # Fort Worth
    }
)
DEFAULT_GOLD_ZIPS = frozenset(
    {
        "73101",
        "73104",
        "73105",
        "73112",
        "73114",
        "73118",
        "73122",
        "73132",
        "74103",
        "74105",  # Tulsa
    }
)


def ensure_platform_tables(conn: sqlite3.Connection) -> None:
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS app_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            square_customer_id TEXT,
            role TEXT NOT NULL DEFAULT 'client',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS support_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            priority TEXT DEFAULT 'normal',
            lead_id INTEGER,
            discord_reference TEXT,
            slack_reference TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS zip_price_tier (
            zip TEXT PRIMARY KEY,
            tier TEXT NOT NULL,
            price_multiplier REAL NOT NULL DEFAULT 1.0,
            notes TEXT
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS pilot_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_key TEXT NOT NULL UNIQUE,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            incentive_label TEXT,
            slots_total INTEGER NOT NULL DEFAULT 5,
            slots_used INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS regional_daily_targets (
            region_code TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            target_verified_visits_per_day INTEGER NOT NULL DEFAULT 3,
            notes TEXT
        )
        """
    )
    conn.commit()
    _seed_regional_targets(c)
    _seed_pilot_markets(c)
    _seed_zip_tiers_from_defaults(c)
    conn.commit()


def _seed_regional_targets(c: sqlite3.Cursor) -> None:
    c.execute("SELECT COUNT(*) FROM regional_daily_targets")
    if c.fetchone()[0] > 0:
        return
    rows = [
        ("OK", "Oklahoma (state)", 3, "Pilot: 3 verified visits / day"),
        ("TX", "Texas (state)", 12, "Pilot: 12 verified visits / day"),
    ]
    c.executemany(
        "INSERT INTO regional_daily_targets (region_code, label, target_verified_visits_per_day, notes) VALUES (?,?,?,?)",
        rows,
    )


def _seed_pilot_markets(c: sqlite3.Cursor) -> None:
    c.execute("SELECT COUNT(*) FROM pilot_markets")
    if c.fetchone()[0] > 0:
        return
    markets = [
        ("okc", "Oklahoma City", "OK", "First pilot clients: waived setup + priority routing", 8),
        ("edmond", "Edmond", "OK", "Neighborhood launch credit", 5),
        ("tulsa", "Tulsa", "OK", "CAT corridor bundle", 5),
        ("dfw", "Dallas–Fort Worth", "TX", "Metro launch: 2 unlocks at tier-1 pricing", 15),
        ("houston", "Houston", "TX", "Gulf hail corridor", 15),
        ("austin", "Austin", "TX", "Hill country expansion", 10),
    ]
    c.executemany(
        """INSERT INTO pilot_markets (market_key, city, state, incentive_label, slots_total)
           VALUES (?,?,?,?,?)""",
        markets,
    )


def _seed_zip_tiers_from_defaults(c: sqlite3.Cursor) -> None:
    for z in DEFAULT_PLATINUM_ZIPS:
        c.execute(
            """INSERT OR IGNORE INTO zip_price_tier (zip, tier, price_multiplier, notes)
               VALUES (?, 'platinum', 1.45, 'default premium list')""",
            (z,),
        )
    for z in DEFAULT_GOLD_ZIPS:
        c.execute(
            """INSERT OR IGNORE INTO zip_price_tier (zip, tier, price_multiplier, notes)
               VALUES (?, 'gold', 1.2, 'default gold list')""",
            (z,),
        )


def zip_tier_and_price(zip_code: str | None, conn: sqlite3.Connection) -> dict[str, Any]:
    z = (zip_code or "").strip()
    mult = 1.0
    tier = "standard"
    if z:
        row = conn.execute("SELECT tier, price_multiplier FROM zip_price_tier WHERE zip = ?", (z,)).fetchone()
        if row:
            tier = row[0]
            mult = float(row[1])
        elif z in DEFAULT_PLATINUM_ZIPS:
            tier, mult = "platinum", 1.45
        elif z in DEFAULT_GOLD_ZIPS:
            tier, mult = "gold", 1.2
    price = round(BASE_LEAD_PRICE_USD * mult, 2)
    return {"tier": tier, "multiplier": mult, "price_usd": price, "base_usd": BASE_LEAD_PRICE_USD}


def damage_tier_counts(conn: sqlite3.Connection, state_filter: str | None = None) -> dict[str, Any]:
    """high >=40, medium 15-39, low <15."""
    base = """
        SELECT
          SUM(CASE WHEN COALESCE(c.damage_score,0) >= 40 THEN 1 ELSE 0 END) AS high_cnt,
          SUM(CASE WHEN COALESCE(c.damage_score,0) >= 15 AND COALESCE(c.damage_score,0) < 40 THEN 1 ELSE 0 END) AS med_cnt,
          SUM(CASE WHEN COALESCE(c.damage_score,0) < 15 THEN 1 ELSE 0 END) AS low_cnt
        FROM contacts c
        INNER JOIN storms s ON c.event_id = s.id
        WHERE 1=1
    """
    params: list[Any] = []
    if state_filter and state_filter.upper() != "ALL":
        base += " AND UPPER(TRIM(s.state)) = ?"
        params.append(state_filter.upper()[:20])
    row = conn.execute(base, params).fetchone()
    high, med, low = int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
    total = max(1, high + med + low)
    return {
        "high": high,
        "medium": med,
        "low": low,
        "total": high + med + low,
        "pct_high": round(100.0 * high / total, 1),
        "pct_medium": round(100.0 * med / total, 1),
        "pct_low": round(100.0 * low / total, 1),
    }


def by_state_damage_summary(conn: sqlite3.Connection, limit: int = 20) -> list[dict[str, Any]]:
    q = """
        SELECT UPPER(TRIM(s.state)) AS st,
          SUM(CASE WHEN COALESCE(c.damage_score,0) >= 40 THEN 1 ELSE 0 END),
          SUM(CASE WHEN COALESCE(c.damage_score,0) >= 15 AND COALESCE(c.damage_score,0) < 40 THEN 1 ELSE 0 END),
          SUM(CASE WHEN COALESCE(c.damage_score,0) < 15 THEN 1 ELSE 0 END),
          COUNT(*)
        FROM contacts c
        INNER JOIN storms s ON c.event_id = s.id
        GROUP BY st
        ORDER BY COUNT(*) DESC
        LIMIT ?
    """
    out = []
    for st, hi, md, lo, tot in conn.execute(q, (limit,)).fetchall():
        tot = int(tot or 0)
        tmax = max(1, tot)
        out.append(
            {
                "state": st,
                "high": int(hi or 0),
                "medium": int(md or 0),
                "low": int(lo or 0),
                "total": tot,
                "pct_high": round(100.0 * int(hi or 0) / tmax, 1),
                "pct_medium": round(100.0 * int(md or 0) / tmax, 1),
                "pct_low": round(100.0 * int(lo or 0) / tmax, 1),
            }
        )
    return out


def verified_visits_today_for_region(conn: sqlite3.Connection, region_code: str) -> int:
    """Uses contacts.verified_at date = today and storm state prefix."""
    rc = region_code.upper()
    row = conn.execute(
        """
        SELECT COUNT(*) FROM contacts c
        INNER JOIN storms s ON c.event_id = s.id
        WHERE c.verified_at IS NOT NULL
          AND DATE(c.verified_at) = DATE('now')
          AND UPPER(TRIM(s.state)) = ?
        """,
        (rc,),
    ).fetchone()
    return int(row[0] or 0)


def post_ticket_to_webhooks(title: str, body: str, ticket_id: int, slack_url: str | None, discord_url: str | None) -> dict[str, str | None]:
    refs: dict[str, str | None] = {"slack": None, "discord": None}
    text = f"Ticket #{ticket_id}: {title}\n{body[:500]}"
    if slack_url:
        try:
            import requests

            r = requests.post(slack_url, json={"text": text}, timeout=10)
            if r.ok:
                refs["slack"] = r.headers.get("X-Slack-Message-Timestamp") or "ok"
        except Exception as e:
            logger.warning("Slack webhook failed: %s", e)
    if discord_url:
        try:
            import requests

            r = requests.post(
                discord_url,
                json={"content": text[:1800]},
                timeout=10,
            )
            if r.ok:
                refs["discord"] = str(r.status_code)
        except Exception as e:
            logger.warning("Discord webhook failed: %s", e)
    return refs
