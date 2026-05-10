"""
main.py — Roof Hunter Autonomous Lead Generation Pipeline
==========================================================

Full pipeline:
  1. Storm Detection  → NOAA SPC + NWS + NCEI + MRMS
  2. Impact Mapping   → Real property addresses via reverse geocoding grid
  3. Satellite Scan   → Sentinel-2 spectral analysis (GEE) + Google Static Maps
  4. Vision AI        → Gemini multimodal / YOLO damage detection
  5. Skip Trace       → RentCast property records + owner lookup
  6. Lead Export      → CSV + evidence images auto-generated

Thresholds (user-defined):
  - Floor: 1.5" hail
  - Auto-qualify: 2.0"+ hail for 5+ minutes = all houses need repair
  
Run:
  python main.py                        # Single pipeline run
  python main.py --daemon               # Continuous monitoring mode
  python main.py --test                 # Test with Edmond, OK coordinates
  python main.py --export-only          # Just export current DB to CSV
"""

import logging
import json
import os
import csv
import sys
import time
import argparse
from datetime import datetime, timezone
from dotenv import load_dotenv

from src.storm_tracker import StormTracker
from src.property_mapper import PropertyMapper
from src.imagery_fetcher import GoogleStaticImagery
from src.vision_agent import VisionAgent
from src.contact_finder import SkipTracer
from src.lead_database import LeadDatabase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ─── CONFIGURATION ──────────────────────────────────────────────────────

# Hail thresholds (per user spec)
MIN_HAIL_INCHES = 1.5        # Floor: anything below this is ignored
AUTO_QUALIFY_INCHES = 2.0     # 2"+ hail = every house in zone needs repair
AUTO_QUALIFY_MINUTES = 5      # Duration threshold for auto-qualification

# Target regions (Hail Alley + expansion)
TARGET_REGIONS = [
    {"location": "Oklahoma", "label": "OK"},
    {"location": "Texas", "label": "TX"},
    {"location": "Kansas", "label": "KS"},
    {"location": "Nebraska", "label": "NE"},
]


def run_pipeline(target_location=None, min_hail=None, max_leads=200, export_csv=True):
    """
    Execute the full storm-to-leads pipeline.
    """
    load_dotenv(os.path.join(os.path.dirname(__file__), 'cloud.env'))
    load_dotenv()

    if min_hail is None:
        min_hail = MIN_HAIL_INCHES

    logger.info("=" * 70)
    logger.info("  ROOF HUNTER AUTONOMOUS LEAD PIPELINE")
    logger.info(f"  Hail Floor: {min_hail}\" | Auto-Qualify: {AUTO_QUALIFY_INCHES}\"")
    logger.info(f"  Target: {target_location or 'All Hail Alley regions'}")
    logger.info("=" * 70)

    # 1. Init Components
    tracker = StormTracker()
    mapper = PropertyMapper()
    imagery = GoogleStaticImagery()
    vision = VisionAgent(use_mock=not os.getenv("GEMINI_API_KEY"))
    tracer = SkipTracer()
    db = LeadDatabase(db_path=os.path.join(os.path.dirname(__file__), 'leads.db'))

    all_leads = []

    # 2. Check for storms
    if target_location:
        events = tracker.get_recent_hail_events(
            min_hail_inches=min_hail,
            location=target_location
        )
    else:
        events = []
        for region in TARGET_REGIONS:
            region_events = tracker.get_recent_hail_events(
                min_hail_inches=min_hail,
                location=region["location"]
            )
            events.extend(region_events)
            logger.info(f"  {region['label']}: {len(region_events)} qualifying events")

    if not events:
        logger.info("No qualifying hail events found across all regions. Pipeline idle.")
        return []

    logger.info(f"\n🌪️ Found {len(events)} qualifying storm events. Processing top events...")

    # Process top events (limit to avoid rate limiting)
    for event_idx, event in enumerate(events[:10]):
        logger.info(f"\n{'='*60}")
        logger.info(f"EVENT {event_idx + 1}: {event['hail_size_inches']}\" hail on {event['date']}")
        logger.info(f"  Location: {event.get('city', '?')}, {event.get('state', '?')}")
        logger.info(f"  Source: {event.get('source', '?')}")
        logger.info(f"{'='*60}")

        # 3. Determine if this is an auto-qualify event
        is_auto_qualify = event['hail_size_inches'] >= AUTO_QUALIFY_INCHES
        if is_auto_qualify:
            logger.info(f"⚡ AUTO-QUALIFY: {event['hail_size_inches']}\" >= {AUTO_QUALIFY_INCHES}\" threshold. "
                         f"ALL properties in zone classified as damaged.")

        # 4. Generate impact zone bounding box
        lat = event.get('latitude', event.get('impact_polygon', [0, 0])[1])
        lon = event.get('longitude', event.get('impact_polygon', [0])[0])

        if lat and lon:
            bbox = mapper.expand_storm_bbox(
                lat, lon,
                hail_size_inches=event['hail_size_inches']
            )
        else:
            bbox = event.get('impact_polygon', [])

        if not bbox or len(bbox) < 4:
            logger.warning(f"Could not generate impact zone for event. Skipping.")
            continue

        # 5. Find real property addresses in the impact zone
        logger.info(f"📍 Scanning impact zone for property addresses...")
        addresses = mapper.get_addresses_in_polygon(
            bbox,
            max_addresses=min(max_leads, 100)  # Per-event limit
        )

        if not addresses:
            logger.warning(f"No addresses resolved in impact zone. Trying wider search...")
            # Expand bbox by 50% and retry
            expanded_bbox = [
                bbox[0] - abs(bbox[2] - bbox[0]) * 0.25,
                bbox[1] - abs(bbox[3] - bbox[1]) * 0.25,
                bbox[2] + abs(bbox[2] - bbox[0]) * 0.25,
                bbox[3] + abs(bbox[3] - bbox[1]) * 0.25,
            ]
            addresses = mapper.get_addresses_in_polygon(expanded_bbox, max_addresses=50)

        logger.info(f"Resolved {len(addresses)} property addresses in impact zone")

        # 6. Process each property
        for addr_idx, addr_info in enumerate(addresses):
            addr_str = addr_info["address"]
            logger.info(f"\n--- [{addr_idx + 1}/{len(addresses)}] {addr_str} ---")

            # Build lead record
            lead = {
                "address": addr_str,
                "property_id": addr_info.get("property_id", ""),
                "latitude": addr_info.get("latitude"),
                "longitude": addr_info.get("longitude"),
                "city": addr_info.get("city", event.get("city", "")),
                "state": addr_info.get("state", event.get("state", "")),
                "zipcode": addr_info.get("zipcode", ""),
                "hail_event_date": event["date"],
                "hail_size_inches": event["hail_size_inches"],
                "storm_source": event.get("source", ""),
            }

            # 7. For auto-qualify events, skip vision analysis
            if is_auto_qualify:
                lead["DamageLevel"] = "Severe"
                lead["EstimatedReplacementEligibility"] = True
                lead["DamageDescription"] = (
                    f"AUTO-QUALIFIED: {event['hail_size_inches']}\" hail event. "
                    f"All structures in impact zone classified as damaged per threshold protocol."
                )
                lead["DetectedAnomalies"] = [
                    {"Type": "Hail Impact", "Confidence": "High", "IsNew": True}
                ]
                lead["evidence_type"] = "hail_threshold"
            else:
                # 8. Fetch satellite imagery for vision analysis
                img_path = None
                if addr_info.get("latitude") and addr_info.get("longitude"):
                    img_path = imagery.fetch_roof_image(
                        addr_info["latitude"],
                        addr_info["longitude"],
                        filename=f"lead_{addr_info['property_id']}"
                    )

                if img_path:
                    # Run vision AI analysis
                    # For sub-threshold events, use AI to verify damage
                    ai_report = vision.inspect_roof_historical(img_path, img_path)
                    lead["DamageLevel"] = ai_report.get("DamageLevel", "Unknown")
                    lead["EstimatedReplacementEligibility"] = ai_report.get("EstimatedReplacementEligibility", False)
                    lead["DamageDescription"] = ai_report.get("DamageDescription", "")
                    lead["DetectedAnomalies"] = ai_report.get("DetectedAnomalies", [])
                    lead["image_path"] = img_path
                    lead["evidence_type"] = "satellite_vision"

                    if not ai_report.get("EstimatedReplacementEligibility"):
                        logger.info(f"  ❌ Vision says no significant damage. Skipping lead.")
                        continue
                else:
                    # No imagery available — use hail data as evidence
                    if event['hail_size_inches'] >= 1.75:
                        lead["DamageLevel"] = "Probable"
                        lead["EstimatedReplacementEligibility"] = True
                        lead["DamageDescription"] = (
                            f"{event['hail_size_inches']}\" hail in area. "
                            f"No satellite image available; qualified on storm data alone."
                        )
                        lead["evidence_type"] = "storm_data_only"
                    else:
                        continue

            # 9. Skip trace: get property owner info
            property_details = mapper.get_property_details(addr_info)
            lead["owner_name"] = property_details.get("owner", "LOOKUP REQUIRED")
            lead["estimated_value"] = property_details.get("estimated_value")
            lead["year_built"] = property_details.get("year_built")
            lead["roof_type"] = property_details.get("roof_type")

            # Try skip trace for phone numbers
            contact = tracer.skip_trace_homeowner(addr_str)
            if contact:
                lead["phone_number"] = contact.get("phone", "")
                lead["email_address"] = contact.get("email", "")
                if contact.get("name") and contact["name"] not in ("DEEP SEARCH REQ",):
                    lead["owner_name"] = contact["name"]

            # 10. Save to database
            db.save_lead(lead)
            all_leads.append(lead)
            logger.info(f"  ✅ LEAD SAVED: {addr_str} | {lead['owner_name']} | "
                         f"{lead.get('DamageLevel', '?')} damage")

    # 11. Export results
    logger.info(f"\n{'='*70}")
    logger.info(f"PIPELINE COMPLETE: {len(all_leads)} qualified leads generated")
    logger.info(f"{'='*70}")

    if export_csv and all_leads:
        csv_path = export_leads_csv(all_leads)
        logger.info(f"📄 Leads exported to: {csv_path}")

    return all_leads


def export_leads_csv(leads, output_dir=None):
    """
    Export leads to a clean CSV with all evidence data.
    Returns the file path.
    """
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(__file__), 'leads_manifests')
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"roof_hunter_leads_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    fieldnames = [
        "address", "city", "state", "zipcode",
        "owner_name", "phone_number", "email_address",
        "hail_size_inches", "hail_event_date", "DamageLevel",
        "EstimatedReplacementEligibility", "DamageDescription",
        "evidence_type", "image_path", "estimated_value",
        "year_built", "roof_type", "latitude", "longitude",
        "storm_source", "property_id"
    ]

    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for lead in leads:
            # Clean up anomalies field for CSV
            row = {k: v for k, v in lead.items() if k in fieldnames}
            writer.writerow(row)

    logger.info(f"Exported {len(leads)} leads to {filepath}")
    return filepath


def daemon_mode(check_interval_minutes=15):
    """
    Continuous monitoring mode: check for new storms every N minutes,
    run the pipeline when qualifying events are detected.
    """
    logger.info("🌪️ ROOF HUNTER DAEMON MODE ACTIVATED")
    logger.info(f"Checking every {check_interval_minutes} minutes for new hail events")
    logger.info(f"Hail floor: {MIN_HAIL_INCHES}\" | Auto-qualify: {AUTO_QUALIFY_INCHES}\"")

    while True:
        try:
            logger.info(f"\n{'='*50}")
            logger.info(f"SWEEP at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
            logger.info(f"{'='*50}")

            leads = run_pipeline()

            if leads:
                logger.info(f"🎯 Generated {len(leads)} new leads this cycle!")
            else:
                logger.info("💤 No new qualifying events. Sleeping...")

        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)

        logger.info(f"Next check in {check_interval_minutes} minutes...")
        time.sleep(check_interval_minutes * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Roof Hunter Lead Generation Pipeline")
    parser.add_argument("--daemon", action="store_true", help="Run in continuous monitoring mode")
    parser.add_argument("--test", action="store_true", help="Test run with Edmond, OK")
    parser.add_argument("--export-only", action="store_true", help="Export current DB to CSV")
    parser.add_argument("--location", type=str, default=None, help="Target location (e.g. 'Oklahoma')")
    parser.add_argument("--min-hail", type=float, default=MIN_HAIL_INCHES, help="Minimum hail size in inches")
    parser.add_argument("--max-leads", type=int, default=200, help="Maximum leads per run")

    args = parser.parse_args()

    if args.daemon:
        daemon_mode()
    elif args.test:
        logger.info("Running test pipeline for Edmond, OK...")
        run_pipeline(target_location="Oklahoma", min_hail=1.5, max_leads=10)
    elif args.export_only:
        db = LeadDatabase(db_path=os.path.join(os.path.dirname(__file__), 'leads.db'))
        leads = db.get_all_leads()
        if leads:
            export_leads_csv(leads)
        else:
            logger.info("No leads in database to export.")
    else:
        run_pipeline(
            target_location=args.location,
            min_hail=args.min_hail,
            max_leads=args.max_leads
        )
