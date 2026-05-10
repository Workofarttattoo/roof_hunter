"""
Stub property enrichment from mailing address.

Replace with county records APIs, AVMs, or title data as needed.
"""

from __future__ import annotations


def enrich_property(address: str) -> dict:
    """
    Return owner/value/roof/lead heuristics for an address (placeholder).

    `address` accepted for API symmetry; not used in this stub.
    """
    _ = address
    return {
        "owner": "UNKNOWN",
        "estimated_value": 350_000,
        "roof_age": 12,
        "lead_score": 0.81,
    }
