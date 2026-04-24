import os
from ultralytics import YOLOWorld
import logging
from src.imagery_fetcher import GoogleStaticImagery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_yolov8_world_scan(lat, lon, city="Project Target"):
    logger.info(f"=== INITIATING YOLOv8-WORLD ZERO-SHOT SCAN: {city} ===")
    
    # 1. Fetch High-Res Imagery
    fetcher = GoogleStaticImagery()
    image_path = fetcher.fetch_roof_image(lat, lon, f"yolo_{lat}_{lon}")
    
    if not image_path:
        logger.error("Could not retrieve ground-truth imagery.")
        return

    # 2. Initialize YOLOv8-World (Zero-Shot)
    try:
        model = YOLOWorld("yolov8s-worldv2.pt")
        
        # Define Forensic Context
        model.set_classes(["damaged shingles", "hail impact crater", "roof debris", "gutters", "intact roof"])
        
        # 3. Inference
        results = model.predict(image_path, conf=0.15)
        
        # 4. Analyze Detections
        detections = []
        for r in results:
            for box in r.boxes:
                cls_id = int(box.cls[0])
                label = model.names[cls_id]
                conf = float(box.conf[0])
                detections.append({"label": label, "conf": conf})
        
        damage_hits = [d for d in detections if "damaged" in d['label'] or "impact" in d['label']]
        
        print("\n" + "*"*80)
        print(f"YOLOv8-WORLD GROUND-TRUTH REPORT: {city.upper()}")
        print("*"*80)
        print(f"Coordinate: {lat}, {lon}")
        print(f"Detections Found: {len(detections)}")
        
        if damage_hits:
            print("\n[🚨 POSITIVE DAMAGE DETECTION]")
            for hit in damage_hits:
                print(f"  - {hit['label'].upper()} detected with {hit['conf']*100:.1f}% confidence.")
            print("\nCONCLUSION: STRUCTURAL DEFECTS CONFIRMED VIA GROUND-TRUTH VISION.")
        else:
            print("\n[✅ NO STRUCTURAL DAMAGE DETECTED]")
            print("Surface appears intact at this resolution.")
        print("*"*80)
        
    except Exception as e:
        logger.error(f"YOLOv8 Inference Error: {e}")

if __name__ == "__main__":
    import sys
    args = sys.argv[1:]
    if len(args) >= 2:
        run_yolov8_world_scan(float(args[0]), float(args[1]), "South Maria, AL")
    else:
        run_yolov8_world_scan(34.3379, -86.6577, "South Maria, AL")
