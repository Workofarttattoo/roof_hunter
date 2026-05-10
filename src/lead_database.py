import sqlite3
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class LeadDatabase:
    def __init__(self, db_path="leads.db"):
        self.db_path = db_path
        self._init_db()
        
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # NOTE: Removed DROP TABLE — was destroying all leads on every startup!
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS roofing_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT,
            property_id TEXT,
            owner_name TEXT,
            phone_number TEXT,
            email_address TEXT,
            hail_event_date TEXT,
            hail_size_inches REAL,
            damage_level TEXT,
            replacement_eligible BOOLEAN,
            damage_description TEXT,
            detected_anomalies TEXT,
            image_path_before TEXT,
            image_path_after TEXT,
            city TEXT,
            state TEXT,
            zipcode TEXT,
            evidence_type TEXT,
            estimated_value REAL,
            year_built INTEGER,
            roof_type TEXT,
            latitude REAL,
            longitude REAL,
            storm_source TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        conn.commit()
        conn.close()

    def save_lead(self, lead_data):
        """
        Saves a qualified lead to the database.
        Handles both old-style (image_path_before/after) and new-style (image_path) fields.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO roofing_leads 
        (address, property_id, owner_name, phone_number, email_address, 
         hail_event_date, hail_size_inches, damage_level, replacement_eligible, 
         damage_description, detected_anomalies, image_path_before, image_path_after,
         city, state, zipcode, evidence_type, estimated_value, year_built,
         roof_type, latitude, longitude, storm_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            lead_data.get('address'),
            lead_data.get('property_id'),
            lead_data.get('owner_name'),
            lead_data.get('phone_number'),
            lead_data.get('email_address'),
            lead_data.get('hail_event_date'),
            lead_data.get('hail_size_inches'),
            lead_data.get('DamageLevel'),
            lead_data.get('EstimatedReplacementEligibility'),
            lead_data.get('DamageDescription'),
            json.dumps(lead_data.get('DetectedAnomalies', [])),
            lead_data.get('image_path_before') or lead_data.get('image_path'),
            lead_data.get('image_path_after') or lead_data.get('image_path'),
            lead_data.get('city', ''),
            lead_data.get('state', ''),
            lead_data.get('zipcode', ''),
            lead_data.get('evidence_type', ''),
            lead_data.get('estimated_value'),
            lead_data.get('year_built'),
            lead_data.get('roof_type', ''),
            lead_data.get('latitude'),
            lead_data.get('longitude'),
            lead_data.get('storm_source', '')
        ))
        conn.commit()
        conn.close()
        logger.info(f"Saved highly-qualified lead to DB: {lead_data.get('address')}")
        
    def get_all_leads(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM roofing_leads")
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
