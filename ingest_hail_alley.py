import sqlite3
import csv
import os
from datetime import datetime

DB_PATH = 'leads_manifests/authoritative_storms.db'
CSV_PATH = 'leads_manifests/exhaustive_hail_leads.csv'

HAIL_ALLEY = ['TX', 'OK', 'KS', 'NE']

print(f"--- Ingesting Hail Alley (2.0\"+) from {CSV_PATH} ---")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

processed = 0
qualified = 0

with open(CSV_PATH, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        state = row['State']
        try:
            mag = float(row['Hail_Magnitude'])
        except: mag = 0
        
        if state in HAIL_ALLEY and mag >= 2.0:
            # Check if storm exists or create event
            event_date = row['Event_Date']
            city = row['City']
            zip_code = row['Zip']
            
            # Simplified: Ingest as contacts linked to a generic corridor storm if needed
            # or try to match existing storm IDs.
            # For speed, we'll just insert/update contacts.
            
            # Search for best event_id match
            c.execute("SELECT id FROM storms WHERE state = ? AND event_date = ? LIMIT 1", (state, event_date))
            storm = c.fetchone()
            event_id = storm[0] if storm else None
            
            if not event_id:
                # Create a placeholder storm event for this date/state
                c.execute("INSERT INTO storms (event_date, event_type, state, magnitude, zipcode, city, source) VALUES (?, 'Hail', ?, ?, ?, ?, 'EXHAUSTIVE_INGEST')",
                         (event_date, state, mag, zip_code, city))
                event_id = c.lastrowid
            
            # Insert Contact
            addr = row['Full_Address']
            name = "Homeowner"
            
            # Avoid duplicates
            c.execute("SELECT id FROM contacts WHERE street_address = ? AND event_id = ?", (addr, event_id))
            if not c.fetchone():
                c.execute("INSERT INTO contacts (event_id, homeowner_name, street_address, zip_code, qualification_status, status) VALUES (?, ?, ?, ?, 'QUALIFIED', 'Pending')",
                         (event_id, name, addr, zip_code))
                qualified += 1
        
        processed += 1
        if processed % 5000 == 0:
            print(f"Processed {processed} records... (Found {qualified} Qualified)")
            conn.commit()

conn.commit()
conn.close()
print(f"--- Ingest Complete: {qualified} new Forensic Leads added. ---")
