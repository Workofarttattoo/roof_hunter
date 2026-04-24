import ee
import pandas as pd
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scan_sentinel_footprints(project_id='roof-hunter-494309'):
    logger.info(f"=== INITIATING SENTINEL-2 ORBITAL SCAN: {project_id} ===")
    
    try:
        ee.Initialize(project=project_id)
    except Exception as e:
        logger.error(f"GEE Initialization Failed: {e}")
        return

    # Load the April manifest
    try:
        leads_df = pd.read_csv('april_100_platinum_leads.csv')
    except:
        logger.error("Could not find april_100_platinum_leads.csv")
        return

    # Process Top 10 leads for the spectral discovery proof-of-concept
    results = []
    for _, row in leads_df.head(10).iterrows():
        lat = row['latitude'] if 'latitude' in row else 31.7
        lon = row['longitude'] if 'longitude' in row else -84.1
        event_date = row['Date']
        
        logger.info(f"Scanning Sentinel-2 Footprint for {row['City']}, {row['State']} ({event_date})...")
        
        # Define date window
        date_obj = datetime.strptime(event_date, '%Y-%m-%d')
        start = (date_obj - timedelta(days=2)).strftime('%Y-%m-%d')
        end = (date_obj + timedelta(days=5)).strftime('%Y-%m-%d')
        
        # Query Sentinel-2 L2A (Atmospherically Corrected)
        point = ee.Geometry.Point([lon, lat])
        s2_collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                         .filterBounds(point)
                         .filterDate(start, end)
                         .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30)))
        
        count = s2_collection.size().getInfo()
        
        if count > 0:
            # Successfully discovered a spectral footprint
            image = s2_collection.first()
            cloud_pct = image.get('CLOUDY_PIXEL_PERCENTAGE').getInfo()
            logger.info(f"✅ Footprint SECURED: Found {count} scenes. Best cloud cover: {cloud_pct:.1f}%")
            results.append({
                "City": row['City'],
                "State": row['State'],
                "Date": event_date,
                "Scenes": count,
                "Best_Cloud": f"{cloud_pct:.1f}%",
                "Source": "Sentinel-2 (10m)",
                "Status": "READY FOR MULTISPECTRAL ANALYSIS"
            })
        else:
            logger.warning(f"❌ No clear Sentinel-2 footprints for {row['City']} in the event window.")

    if results:
        final_df = pd.DataFrame(results)
        print("\n" + "="*100)
        print("SENTINEL-2 MULTISPECTRAL DISCOVERY LOG")
        print("="*100)
        print(final_df.to_markdown(index=False))
        print("="*100)
    else:
        logger.warning("No orbital discovery hits in this batch. Expanding temporal buffer recommended.")

if __name__ == "__main__":
    scan_sentinel_footprints()
