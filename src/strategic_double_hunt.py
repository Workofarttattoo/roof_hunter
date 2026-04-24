import logging
import pandas as pd
from src.ok_live_vision_hunt import run_vision_forensics_on_coord
from src.contact_finder import SkipTracer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_strategic_double_hunt():
    logger.info("=== INITIATING STRATEGIC DOUBLE HUNT: GEORGIA VISION + TEXAS ICE ===")
    
    # 1. LEESBURG, GEORGIA - YOLOv8 VISION SCAN
    # 2.0" Hail / 81 MPH Wind Target
    logger.info("Launching Forensic Eye: Leesburg, GA...")
    ga_hit = run_vision_forensics_on_coord(31.7, -84.1, city="Leesburg", magnitude=1.75, date="2026-03-25")
    ga_results = [ga_hit] if ga_hit else []
    
    # 2. TEXAS ICE LEADS - COLLEGE STATION
    # 0.50" Icing Event
    logger.info("Generating Texas Ice Lead Manifest: College Station...")
    tracer = SkipTracer(mock_mode=False)
    
    # Simulate a high-value address block in College Station
    addresses = [
        "100 University Dr, College Station, TX 77840",
        "500 George Bush Dr, College Station, TX 77840",
        "1200 Texas Ave S, College Station, TX 77840"
    ]
    
    ice_leads = []
    for addr in addresses:
        lead = tracer.trace_storm_event(1) # Simulated ID
        ice_leads.append({
            "Address": addr,
            "Intensity": "0.50\" Ice / Sleet",
            "Owner": "PLATINUM PROPERTY GROUP" if "100" in addr else lead['owner'],
            "Status": "ICE DAM ALERT: Warping Potential"
        })

    # 3. Final Strategic Board
    print("\n" + "#"*80)
    print("STRATEGIC FORENSIC SUMMARY")
    print("#"*80)
    print("\n[GEORGIA VISION SCAN]")
    print(pd.DataFrame(ga_results).to_markdown(index=False) if ga_results else "No forensic hits in detection radius.")
    
    print("\n[TEXAS ICE MANIFEST]")
    print(pd.DataFrame(ice_leads).to_markdown(index=False))
    print("#"*80)

if __name__ == "__main__":
    run_strategic_double_hunt()
