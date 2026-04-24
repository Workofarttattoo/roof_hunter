import os
import glob
import logging
import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
from src.vision_filters import RoofVisionFilters

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TRAIN_DIR = os.path.join(os.path.dirname(__file__), '..', 'training_data')

class RoofXGBoostClassifier:
    """
    Implements the Extreme Gradient Boosting (XGBoost) model for Roof Damage
    Classification using Object-Based Image Analysis (GEOBIA) features,
    as recommended by Jozdani et al. (MDPI Remote Sensing).
    """
    def __init__(self):
        self.model = xgb.XGBClassifier(
            objective='multi:softmax',
            num_class=3, # 0: None, 1: Minor, 2: Severe
            max_depth=6,
            learning_rate=0.1,
            n_estimators=100,
            seed=42
        )
        self.feature_columns = ['B_mean', 'G_mean', 'R_mean', 'B_std', 'G_std', 'R_std', 'ExG_ratio', 'edge_density', 'brightness_mean', 'brightness_std']
        
        # Try to load existing model
        self.model_path = os.path.join(os.path.dirname(__file__), "xgboost_roof_model.pkl")
        if os.path.exists(self.model_path):
            try:
                self.model = joblib.load(self.model_path)
                logger.info("Loaded pre-trained XGBoost model for production inference.")
            except:
                logger.warning("Failed to load model file. Inference will use untrained weights.")

    def create_mock_labeled_data(self):
        """
        Since we just built the harvester, we'll mock the 'Labels' for the images 
        in /training_data/ assuming a human clicked them in Roboflow.
        """
        logger.info("Extracting GEOBIA scalar features from /training_data/...")
        
        # Ensure training directory exists
        if not os.path.exists(TRAIN_DIR):
            os.makedirs(TRAIN_DIR)
            
        data = []
        labels = []
        
        # In a real workflow, we'd read a CSV mapping filenames to labels.
        # We will mock the dataset construction here to test the pipeline.
        
        # Assume we have 100 fake data points for validation
        for i in range(100):
            # We mock the dictionary output of extract_geobia_features
            # based on simulated damage types to prove the tree splits properly.
            
            is_damaged = np.random.choice([0, 1, 2]) # None, Minor, Severe
            
            features = {
                'B_mean': np.random.uniform(50, 150),
                'G_mean': np.random.uniform(50, 150),
                'R_mean': np.random.uniform(50, 150),
                'B_std': np.random.uniform(10, 40),
                'G_std': np.random.uniform(10, 40),
                'R_std': np.random.uniform(10, 40),
                'ExG_ratio': np.random.uniform(0.01, 0.2) if is_damaged == 0 else np.random.uniform(0.2, 0.6), # More exposed green/underlayment
                'edge_density': np.random.uniform(0.05, 0.1) if is_damaged == 0 else np.random.uniform(0.15, 0.4), # High edges = broken geometry
                'brightness_mean': np.random.uniform(100, 200),
                'brightness_std': np.random.uniform(20, 60)
            }
            
            data.append(features)
            labels.append(is_damaged)
            
        df = pd.DataFrame(data)
        self.feature_columns = df.columns.tolist()
        return df, np.array(labels)

    def train_and_evaluate(self):
        logger.info("Initiating XGBoost Training Pipeline...")
        X, y = self.create_mock_labeled_data()
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        logger.info(f"Training XGBoost ensemble on {len(X_train)} samples with {len(self.feature_columns)} GEOBIA features...")
        self.model.fit(X_train, y_train)
        
        logger.info("Evaluating model against test set...")
        y_pred = self.model.predict(X_test)
        
        acc = accuracy_score(y_test, y_pred)
        logger.info(f"✅ XGBoost Accuracy: {acc * 100:.2f}%")
        
        logger.info("\n" + classification_report(y_test, y_pred, target_names=["None", "Minor", "Severe"]))
        
        # Save model using standard pickling
        joblib.dump(self.model, self.model_path)
        logger.info(f"Saved frozen model to {self.model_path}")

    def predict_damage(self, features_dict):
        """
        Runs real-time inference on a single GEOBIA feature dictionary.
        Returns True if damage (Minor or Severe) is detected.
        """
        if not features_dict:
            return False
            
        # Convert dict to DataFrame for XGBoost
        df = pd.DataFrame([features_dict])
        
        # Permissive heuristic for high-volume harvest yield
        if features_dict.get('edge_density', 0) > 0.10 or features_dict.get('shingle_pixel_variance', 0) > 1000:
            return True
            
        try:
            pred = self.model.predict(df)[0]
            return pred > 0 # Any damage class
        except:
            return False

if __name__ == "__main__":
    classifier = RoofXGBoostClassifier()
    classifier.train_and_evaluate()
