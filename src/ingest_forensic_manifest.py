import sqlite3
import os
import re
from datetime import datetime

# Path Configuration
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'leads_manifests', 'authoritative_storms.db')

RAW_DATA = """
822 County Road W, , Nebraska 68464 Rubenville NEBRASKA 3.64 61.69 2026-04-12
85547 L Road, , Nebraska 68924 Ellenshire NEBRASKA 3.54 52.56 2026-03-29
83139 Stephan's Crossing, Bryan, Texas 77842 East Gregory TEXAS 3.33 29.89 2026-04-11
8648 Stephan's Crossing, Bryan, Texas 77842 East Gregory TEXAS 3.33 29.89 2026-04-11
7313 , Country Club, West Virginia 26301 Solomonberg VIRGINIA 3.36 22.25 2026-04-17
125 Fox Ridge Road, , Ohio 45693 Lake Coreyville VIRGINIA 2.94 21.48 2026-04-20
69409 Laurel Creek Road, , West Virginia 25916 West Daniel VIRGINIA 3.35 14.67 2026-04-19
29091 Highway 50, , Nebraska 68441 Bassside NEBRASKA 3.57 13.53 2026-04-17
578 , , Georgia South Laura GEORGIA 3.91 11.76 2026-04-03
3483 Horseshoe Bend Road, , Virginia 24095 Hernandezmouth VIRGINIA 3.79 11.42 2026-03-30
2646 , , Coahuila Port Jon TEXAS 2.99 10.92 2026-03-27
54006 , , Durango Nguyenmouth TEXAS 2.89 9.35 2026-04-07
5488 , , Coahuila North Meghan TEXAS 3.37 9.24 2026-04-05
555 County Road 293, Smiths Station, Alabama 36877 Michaelstad ALABAMA 3.58 8.34 2026-04-10
1914 US 81, , Oklahoma 73720 South Alvinfurt OKLAHOMA 3.34 8.16 2026-04-08
73515 CR 173, , Florida 32452 Lake Nicole GEORGIA 3.22 7.91 2026-03-29
354 , , West Virginia 26269 South Lori VIRGINIA 3.71 7.81 2026-03-25
68403 , , Georgia Christophermouth ALABAMA 3.73 7.29 2026-04-21
7120 County Line Church Road, , Georgia 31039 Johnsonshire GEORGIA 3.84 6.83 2026-03-25
17078 15th Street, , Florida 32424 Jeﬀreyport GEORGIA 3.8 6.66 2026-04-24
800 , , Nuevo León North Rebeccatown TEXAS 3.86 6.53 2026-04-05
61883 , , Nuevo León North Rebeccatown TEXAS 3.86 6.53 2026-04-05
3193 , , Nuevo León North Rebeccatown TEXAS 3.86 6.53 2026-04-05
7258 Adams Perry Road, , Georgia Peterfurt GEORGIA 3.14 6.45 2026-03-28
1861 Iris Lane, Navarre, Florida 32566 Amandachester ALABAMA 2.88 6.45 2026-03-27
7391 Minor Road, , Georgia South Matthewfort GEORGIA 3.46 6.33 2026-04-12
5972 Lick Creek Road, , Kentucky New Taraburgh VIRGINIA 3.86 6.06 2026-04-06
108 County Road 183, Craig, Colorado 81625 Lake Kevin COLORADO 3.31 6.06 2026-04-10
3351 , , Texas Eddieburgh TEXAS 3.87 6.02 2026-04-16
52577 , , Texas Eddieburgh TEXAS 3.87 6.02 2026-04-16
6428 , , Texas Eddieburgh TEXAS 3.87 6.02 2026-04-16
661 , , West Virginia 26710 Mcdonaldtown VIRGINIA 2.88 5.96 2026-04-01
857 Sarvis Fork Road, , West Virginia West David VIRGINIA 3.84 5.85 2026-04-22
712 , , Texas Laurenborough OKLAHOMA 3.21 5.8 2026-04-02
0607 , , Texas Harmonview OKLAHOMA 3.44 5.76 2026-04-13
50347 , , South Carolina Port Brianaberg GEORGIA 3.43 5.76 2026-04-17
73711 South Platte River Road, , Colorado North Darylton COLORADO 3.57 5.72 2026-04-14
195 , , Virginia Jamesfort VIRGINIA 3.11 5.72 2026-04-16
586 , , Georgia 31316 Tinaburgh GEORGIA 3.07 5.67 2026-04-13
016 Lookout Mountain Trail, , Colorado 80541 South Erica COLORADO 3.34 5.55 2026-04-14
06930 , , Virginia Austinborough VIRGINIA 2.91 5.42 2026-04-06
9560 , Paintsville, Kentucky 41240 Erikaton VIRGINIA 3.56 5.38 2026-04-15
6769 , , Colorado North Jill COLORADO 3.21 5.34 2026-04-23
562 Medley Valley Road, , Virginia 24608 Port Randallton VIRGINIA 3.65 5.2 2026-04-18
876 Hunter Road, , Georgia 30635 Paulburgh GEORGIA 3.12 5.14 2026-04-02
9238 709 Road, , Nebraska Port Karina NEBRASKA 2.93 5.13 2026-03-26
"""

def ingest_leads():
    print("=== INGESTING EXTERNAL FORENSIC MANIFEST 20260424 ===")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    lines = [l.strip() for l in RAW_DATA.strip().split('\n')]
    
    storm_cache = {} # Cache for event_id
    
    ingested_count = 0
    for line in lines:
        try:
            # Parse using regex to handle the variety of address/storm formats
            # Format: [Address...] [Magnitude] [Score] [Date]
            # We look for the date at the end and magnitude/score before it
            parts = line.split(' ')
            event_date = parts[-1]
            ai_score = float(parts[-2])
            magnitude = float(parts[-3])
            
            # The rest is address + city + storm name
            # We'll try to find the state to separate them
            address_blob = " ".join(parts[:-3])
            
            # Identify State for storm normalization
            state = "UNKNOWN"
            if "NEBRASKA" in address_blob: state = "NE"
            elif "TEXAS" in address_blob: state = "TX"
            elif "VIRGINIA" in address_blob: state = "VA"
            elif "GEORGIA" in address_blob: state = "GA"
            elif "ALABAMA" in address_blob: state = "AL"
            elif "OKLAHOMA" in address_blob: state = "OK"
            elif "COLORADO" in address_blob: state = "CO"
            
            # Simple City Extraction (find the word before the magnitude if it's alphanumeric)
            # Actually we can use the storm name in the blob as 'city' for now or extraction from address
            storm_city = parts[-4] if len(parts) > 4 else "Unknown"

            # 1. Create/Get Storm Entry
            storm_key = f"{storm_city}_{state}_{magnitude}_{event_date}"
            if storm_key not in storm_cache:
                c.execute('''
                    INSERT INTO storms (event_date, event_type, state, city, magnitude, source)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (event_date, 'Hail', state, storm_city, magnitude, 'FORENSIC_INGEST_20260424'))
                storm_cache[storm_key] = c.lastrowid
            
            event_id = storm_cache[storm_key]
            
            # 2. Extract clean street address (the part before the storm name/zip)
            # Find the first occurrence of a Zip code or the storm name
            clean_addr = re.split(r'\d{5}', address_blob)[0].strip(', ')
            
            # 3. Create Lead Entry
            c.execute('''
                INSERT INTO contacts (
                    event_id, street_address, qualification_status, damage_score, 
                    status, homeowner_name, verified_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                event_id, 
                clean_addr, 
                'QUALIFIED', 
                ai_score, 
                'Pending', 
                'Homeowner', 
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            ingested_count += 1
            
        except Exception as e:
            print(f"Failed to parse line: {line} | Error: {e}")
            continue

    conn.commit()
    conn.close()
    print(f"✅ Ingestion Complete: {ingested_count} leads added to the authoritative database.")
    print("Initiating Skip-Trace Protocol...")

if __name__ == "__main__":
    ingest_leads()
    # Trigger skip-tracing for the new leads
    from skip_trace_qualified import skip_trace_qualified_leads
    skip_trace_qualified_leads()
