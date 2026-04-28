import sqlite3
import os
import logging
import math
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'leads_manifests', 'authoritative_storms.db')

class HailVerifierAgent:
    """
    Forensic Agent that verifies hail event persistence and density.
    Prevents 'One-House Wonders' by ensuring leads are within a significant storm core.
    """
    def __init__(self):
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        try:
            c.execute("ALTER TABLE contacts ADD COLUMN hail_density_score INTEGER DEFAULT 0")
            c.execute("ALTER TABLE contacts ADD COLUMN forensic_tag TEXT")
        except sqlite3.OperationalError:
            pass 
        conn.commit()
        conn.close()

    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Haversine distance in km."""
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def verify_lead_persistence(self, contact_id):
        """
        Calculates the cluster density for a specific lead.
        Checks storms table for nearby hits.
        """
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        c.execute("""
            SELECT c.*, s.latitude, s.longitude 
            FROM contacts c
            JOIN storms s ON c.event_id = s.id
            WHERE c.id = ?
        """, (contact_id,))
        contact = c.fetchone()
        if not contact:
            return

        lat = contact['latitude']
        lon = contact['longitude']
        
        # Query storms within a 0.05 degree bounding box (~5km) for performance
        c.execute("""
            SELECT latitude, longitude, magnitude 
            FROM storms 
            WHERE ABS(latitude - ?) < 0.05 
              AND ABS(longitude - ?) < 0.05
        """, (lat, lon))
        
        nearby_storms = c.fetchall()
        
        # Refine with exact Haversine and count hits within 3km
        cluster_hits = [s for s in nearby_storms if self.calculate_distance(lat, lon, s['latitude'], s['longitude']) <= 3.0]
        density_score = len(cluster_hits)
        
        tag = "ISOLATED"
        if density_score >= 15:
            tag = "STORM_CORE"
        elif density_score >= 5:
            tag = "CLUSTER_HIT"

        logger.info(f"🔍 Forensic Verification for Lead #{contact_id}: Density={density_score} | Tag={tag}")

        c.execute("""
            UPDATE contacts SET 
                hail_density_score = ?,
                forensic_tag = ?
            WHERE id = ?
        """, (density_score, tag, contact_id))
        
        conn.commit()
        conn.close()
        return density_score, tag

    def process_all_pending(self):
        """Verify all contacts that haven't been scored yet."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id FROM contacts WHERE hail_density_score = 0 OR hail_density_score IS NULL")
        leads = c.fetchall()
        conn.close()
        
        if not leads:
            logger.info("No leads require forensic verification.")
            return

        logger.info(f"🌀 Processing Forensic Hail Verification for {len(leads)} leads...")
        for (lid,) in leads:
            self.verify_lead_persistence(lid)

if __name__ == "__main__":
    verifier = HailVerifierAgent()
    verifier.process_all_pending()
