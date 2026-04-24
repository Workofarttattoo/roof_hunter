import os
import sqlite3
import pandas as pd
import logging
from src.imagery_fetcher import GoogleStaticImagery
from src.xgb_classifier import RoofXGBoostClassifier
from src.vision_filters import RoofVisionFilters
from src.contact_finder import SkipTracer
from src.street_view_scraper import StreetViewScraper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_vision_forensics_on_coord(lat, lon, city="Discovery Target", magnitude=0, median_home_value=250000, date="2026-04-24"):
    """
    Performs the full vision forensic pipeline on a single coordinate.
    """
    fetcher = GoogleStaticImagery()
    filters = RoofVisionFilters()
    classifier = RoofXGBoostClassifier()
    tracer = SkipTracer(mock_mode=False)
    street_scraper = StreetViewScraper()
    
    logger.info(f"Analyzing Storm Point: {city} ({magnitude} intensity) at {lat}, {lon}...")
    
    # A. Fetch Satellite Image
    image_path = fetcher.fetch_roof_image(lat, lon, f"forensic_{lat}_{lon}")
    # AA. Fetch Street View Ground-Truth
    street_path = street_scraper.fetch_street_view(lat, lon, f"ground_{lat}_{lon}")
    
    if not image_path:
        return None
        
    # B. Extract GEOBIA Features
    features = filters.extract_geobia_features(image_path)
    if not features:
        return None
        
    # C. Run XGBoost Inference
    is_damaged = classifier.predict_damage(features)
    
    if is_damaged:
        # D. Skip Trace Owner
        contact = tracer.trace_storm_event(999) # Template ID
        return {
            "Date": date,
            "City": city,
            "Intensity": magnitude,
            "Valuation": f"${median_home_value:,.0f}",
            "Owner": contact['owner'] if contact else "DEEP SEARCH REQ",
            "Phone": contact['phone'] if contact else "UNVERIFIED",
            "Evidence": "FORENSIC VISION CONFIRMED"
        }
    return None

def run_vision_verified_hunt():
    """
    Legacy loop-based hunt for Oklahoma.
    """
    from datetime import datetime, timedelta
    start_date = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
    
    conn = sqlite3.connect('authoritative_storms.db')
    query = """
    SELECT id, latitude, longitude, city, event_date, magnitude, median_home_value 
    FROM storms 
    WHERE event_date >= ?
    ORDER BY magnitude DESC, median_home_value DESC
    LIMIT 20
    """
    df = pd.read_sql_query(query, conn, params=(start_date,))
    conn.close()
    
    if df.empty:
        return
    
    results = []
    for _, row in df.iterrows():
        hit = run_vision_forensics_on_coord(
            row['latitude'], row['longitude'], 
            city=row['city'], magnitude=row['magnitude'], 
            median_home_value=row['median_home_value'], 
            date=row['event_date']
        )
        if hit:
            results.append(hit)

    if results:
        print("\n" + "!"*120)
        print("ROOF HUNTER: VISION-VERIFIED PREMIUM LEADS")
        print("!"*120)
        print(pd.DataFrame(results).to_markdown(index=False))
        print("!"*120)

if __name__ == "__main__":
    run_vision_verified_hunt()
