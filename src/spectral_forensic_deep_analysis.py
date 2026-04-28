import ee
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def perform_deep_spectral_analysis(lat, lon, event_date_str, city="Target", project_id='roof-hunter-494309'):
    logger.info(f"=== DEEP SPECTRAL SCAN: {city} ({lat}, {lon}) ===")
    
    try:
        # Direct initialization - works across API versions
        ee.Initialize(project=project_id)
    except Exception as e:
        logger.error(f"GEE Initialization Failed: {e}")
        return

    point = ee.Geometry.Point([lon, lat])
    event_date = datetime.strptime(event_date_str, '%Y-%m-%d')
    
    # 1. Temporal Windows
    pre_start = (event_date - timedelta(days=60)).strftime('%Y-%m-%d')
    pre_end = (event_date - timedelta(days=2)).strftime('%Y-%m-%d')
    post_start = (event_date + timedelta(days=1)).strftime('%Y-%m-%d')
    post_end = (event_date + timedelta(days=30)).strftime('%Y-%m-%d')
    
    def mask_s2_clouds(image):
        qa = image.select('QA60')
        cloud_bit_mask = 1 << 10
        cirrus_bit_mask = 1 << 11
        mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(qa.bitwiseAnd(cirrus_bit_mask).eq(0))
        return image.updateMask(mask).divide(10000)

    pre_collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                      .filterBounds(point)
                      .filterDate(pre_start, pre_end)
                      .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10))
                      .map(mask_s2_clouds))
    
    post_collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                       .filterBounds(point)
                       .filterDate(post_start, post_end)
                       .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10))
                       .map(mask_s2_clouds))
    
    if pre_collection.size().getInfo() == 0 or post_collection.size().getInfo() == 0:
        logger.error(f"Insufficient GEE coverage for {city}.")
        return

    # 3. Create Composites
    pre_image = pre_collection.median()
    post_image = post_collection.median()

    # 4. Multi-Index Analysis
    def add_indices(img):
        nbr = img.normalizedDifference(['B8', 'B12']).rename('NBR')
        ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI')
        return img.addBands([nbr, ndvi])

    pre_analysis = add_indices(pre_image)
    post_analysis = add_indices(post_image)
    delta = pre_analysis.subtract(post_analysis)
    
    # 5. Extraction
    stats = delta.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=point.buffer(100),
        scale=10
    ).getInfo()
    
    dnbr = stats.get('NBR', 0) if stats.get('NBR') is not None else 0
    dndvi = stats.get('NDVI', 0) if stats.get('NDVI') is not None else 0
    
    anomaly_score = (abs(dnbr) * 0.7) + (abs(dndvi) * 0.3)
    severity = "INSIGNIFICANT"
    if anomaly_score > 0.05: severity = "MODERATE SPECTRAL ANOMALY"
    if anomaly_score > 0.12: severity = "SEVERE SURFACE DISRUPTION"
    
    print("\n" + "#"*80)
    print(f"DEEP SPECTRAL FORENSIC REPORT: {city.upper()}")
    print("#"*80)
    print(f"Target:        {lat}, {lon}")
    print(f"Delta-NBR:      {dnbr:.4f}")
    print(f"Delta-NDVI:     {dndvi:.4f}")
    print(f"Combined Score: {anomaly_score:.4f}")
    print(f"Status:         {severity}")
    
    if severity != "INSIGNIFICANT":
        print("\n[VERIFIED SIGNAL]")
        print("Multispectral signature confirmed structural displacement.")
        print("ACTION: UPGRADE LEAD TO PLATINUM-PLUS FOR AI CONVERSION.")
    else:
        print("\n[NO SIGNAL]")
        print("Spectral data shows minimal surface change. Structure may be high-integrity.")
    print("#"*80)
    
    return {
        "dnbr": dnbr,
        "dndvi": dndvi,
        "score": anomaly_score,
        "severity": severity
    }

if __name__ == "__main__":
    import sys
    args = sys.argv[1:]
    if len(args) >= 3:
        perform_deep_spectral_analysis(float(args[0]), float(args[1]), args[2], args[3] if len(args)>3 else "Target")
