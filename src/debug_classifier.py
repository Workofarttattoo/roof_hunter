import logging
from src.vision_filters import RoofVisionFilters
from src.xgb_classifier import RoofXGBoostClassifier

logging.basicConfig(level=logging.INFO)

def debug_classifier():
    filters = RoofVisionFilters()
    classifier = RoofXGBoostClassifier()
    
    # Path to a locally fetched image or mock
    # Let's mock the features directly
    mock_features = {
        "edge_density": 0.18, 
        "shingle_pixel_variance": 2500,
        "shadow_profile": 0.05
    }
    
    is_damaged = classifier.predict_damage(mock_features)
    print(f"DEBUG: Features {mock_features} -> Damage: {is_damaged}")

if __name__ == "__main__":
    debug_classifier()
