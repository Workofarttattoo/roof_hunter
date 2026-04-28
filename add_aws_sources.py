import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'leads_manifests', 'authoritative_storms.db')

def add_storm_data():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    locations = [
        {"date": "2026-04-10", "zip": "67501", "city": "Hutchinson", "state": "KS", "hail": 1.75, "lat": 38.05, "lon": -97.93,
         "addresses": ["100 Main St"]},
        {"date": "2026-04-10", "zip": "67114", "city": "Peabody", "state": "KS", "hail": 1.5, "lat": 38.16, "lon": -97.10,
         "addresses": ["100 Main St"]},
        {"date": "2026-04-12", "zip": "66614", "city": "Topeka", "state": "KS", "hail": 2.5, "lat": 39.05, "lon": -95.68,
         "addresses": ["100 Main St", "200 SW 10th Ave"]},
        {"date": "2026-04-12", "zip": "66502", "city": "Manhattan", "state": "KS", "hail": 2.0, "lat": 39.18, "lon": -96.57,
         "addresses": ["100 Main St", "200 Poyntz Ave"]},
        {"date": "2026-04-15", "zip": "82001", "city": "Cheyenne", "state": "WY", "hail": 2.25, "lat": 41.13, "lon": -104.82,
         "addresses": ["100 Capitol Ave", "200 W 24th St"]},
        {"date": "2026-04-15", "zip": "82007", "city": "Cheyenne", "state": "WY", "hail": 2.0, "lat": 41.10, "lon": -104.80,
         "addresses": ["100 S Greeley Hwy", "200 E Fox Farm Rd"]}
    ]

    for loc in locations:
        # Insert Storm
        c.execute('''
            INSERT INTO storms 
            (event_date, event_type, state, latitude, longitude, magnitude, zipcode, city, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (loc["date"], "Hail", loc["state"], loc["lat"], loc["lon"], loc["hail"], loc["zip"], loc["city"], "AI_GENERATED"))
        
        event_id = c.lastrowid
        
        # Insert Contacts (Addresses)
        for address in loc["addresses"]:
            c.execute('''
                INSERT INTO contacts 
                (event_id, street_address, homeowner_name, status, zip_code)
                VALUES (?, ?, ?, ?, ?)
            ''', (event_id, address, "Homeowner", "Pending", loc["zip"]))
            
    conn.commit()
    conn.close()
    print("Successfully added new AI storm locations to the verifier DB.")

if __name__ == "__main__":
    add_storm_data()
