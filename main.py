import logging
import json
import os
import shutil
from dotenv import load_dotenv
from src.storm_tracker import StormTracker
from src.property_mapper import PropertyMapper
from src.imagery_fetcher import ImageryFetcher
from src.vision_agent import VisionAgent
from src.contact_finder import ContactFinder
from src.lead_database import LeadDatabase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_agent_pipeline():
    logger.info("Starting Roof Inspector AI Pipeline...")
    load_dotenv() # Load variables from .env if present
    
    # Phase 2 Toggle: Set to True to bypass LLM inference and solely collect data for training ECH0-ROOF.
    GATHER_MODE = True
    
    if GATHER_MODE:
        os.makedirs(os.path.join(os.path.dirname(__file__), 'training_data'), exist_ok=True)
        logger.info("[GATHER MODE ACTIVE] Images will be archived for custom model labeling.")

    
    # 1. Init Components
    tracker = StormTracker()
    mapper = PropertyMapper()
    fetcher = ImageryFetcher()
    vision = VisionAgent(use_mock=True) # Set to False and add GEMINI_API_KEY env to run live
    contact = ContactFinder()
    db = LeadDatabase(db_path=os.path.join(os.path.dirname(__file__), 'leads.db'))
    
    # 2. Check for Storms via NOAA Data Lake
    target_county = "Oklahoma County, OK"
    min_hail = 1.5
    event = tracker.check_for_hail(qualifier_inches=min_hail, location=target_county)
    
    if not event:
        logger.info("No recent qualifying hail events found. Waiting for next cycle.")
        return
        
    logger.info(f"Found Qualifier Event: {event['date']} - Max Hail: {event['hail_size_inches']}\"")
    polygon = event["impact_polygon"]
    
    # 3. Find Properties in that Polygon
    addresses = mapper.get_addresses_in_polygon(polygon)
    logger.info(f"Identified {len(addresses)} properties in impact zone.")
    
    # 4. Process Each Property
    for addr_info in addresses:
        addr_str = addr_info["address"]
        logger.info(f"--- Processing {addr_str} ---")
        
        # Fetch Historical Imagery (Before & After)
        img_res = fetcher.fetch_historical_imagery(addr_info, event["date"])
        if img_res["status"] != "success":
            logger.warning(f"Could not get imagery for {addr_str}")
            continue
            
        img_before = img_res["image_path_before"]
        img_after = img_res["image_path_after"]
        
        if GATHER_MODE:
            # Save data for Model Training later
            train_dir = os.path.join(os.path.dirname(__file__), 'training_data')
            b_name = f"{addr_info['property_id']}_before.jpg"
            a_name = f"{addr_info['property_id']}_after.jpg"
            
            # Copying dummy images to training dir
            if os.path.exists(img_before):
                shutil.copy(img_before, os.path.join(train_dir, b_name))
            if os.path.exists(img_after):
                shutil.copy(img_after, os.path.join(train_dir, a_name))
                
            logger.info(f"Harvested training data for {addr_str}.")
            continue
        
        # AI Vision Comparative Inspection
        ai_report = vision.inspect_roof_historical(img_before, img_after)
        logger.info(f"AI Report: {ai_report['DamageLevel']} Damage. Eligibility: {ai_report['EstimatedReplacementEligibility']}")
        
        # 5. Database Save (Only if qualified)
        if ai_report.get("EstimatedReplacementEligibility"):
            # Contact Skip Tracing
            contact_res = contact.find_owner_contact(addr_info)
            
            lead_data = {
                "address": addr_str,
                "property_id": addr_info["property_id"],
                "owner_name": contact_res.get("owner_name"),
                "phone_number": contact_res.get("phone_number"),
                "email_address": contact_res.get("email_address"),
                "hail_event_date": event["date"],
                "hail_size_inches": event["hail_size_inches"],
                "DamageLevel": ai_report["DamageLevel"],
                "EstimatedReplacementEligibility": ai_report["EstimatedReplacementEligibility"],
                "DamageDescription": ai_report["DamageDescription"],
                "DetectedAnomalies": ai_report["DetectedAnomalies"],
                "image_path_before": img_before,
                "image_path_after": img_after
            }
            db.save_lead(lead_data)
        else:
            logger.info(f"{addr_str} does not meet criteria for insurance claim lead.")

    logger.info("--- Pipeline Run Complete ---")
    
    # Print current leads summary
    leads = db.get_all_leads()
    logger.info(f"Total Qualified Leads in DB: {len(leads)}")
    # logger.info(json.dumps(leads, indent=2))

if __name__ == "__main__":
    run_agent_pipeline()
