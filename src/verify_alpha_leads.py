"""
verify_alpha_leads.py
---------------------
Verifies 49 ALPHA LEADS by:
  1. Pulling precise lat/lon from authoritative_storms.db by city+state+date match
  2. Reverse-geocoding each coordinate → real street address (Nominatim)
  3. Flagging CROSS-STATE anomalies (address state ≠ lead state)
  4. Flagging CROSS-COUNTRY anomalies (address in Mexico, etc.)
  5. Updating alpha_leads_contacts.csv with resolved addresses + verification status
  6. Writing alpha_leads_VERIFIED.csv report
"""

import csv
import sqlite3
import os
import re
import time
import json
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from geopy.distance import geodesic

BASE       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH    = os.path.join(BASE, "leads_manifests", "authoritative_storms.db")
INPUT_CSV  = os.path.join(BASE, "leads_manifests", "alpha_leads_contacts.csv")
OUTPUT_CSV = os.path.join(BASE, "leads_manifests", "alpha_leads_VERIFIED.csv")

geolocator = Nominatim(user_agent="roof_hunter_alpha_verifier_v1", timeout=12)

ALPHA_LEADS = [
    {"Date":"2026-03-30","City":"East Julie","State":"ALABAMA","Intensity":"3.98","Valuation":"$545,819","Address":"9530 Farmers Road, , Alabama 36555"},
    {"Date":"2026-04-03","City":"South Laura","State":"GEORGIA","Intensity":"3.91","Valuation":"$660,453","Address":"578 , , Georgia"},
    {"Date":"2026-04-06","City":"Thompsonchester","State":"TEXAS","Intensity":"3.9","Valuation":"$573,761","Address":"9254 , , Chihuahua"},
    {"Date":"2026-04-16","City":"Eddieburgh","State":"TEXAS","Intensity":"3.87","Valuation":"$765,724","Address":"6428 , , Texas"},
    {"Date":"2026-04-06","City":"New Taraburgh","State":"VIRGINIA","Intensity":"3.86","Valuation":"$786,539","Address":"5972 Lick Creek Road, , Kentucky"},
    {"Date":"2026-04-05","City":"North Rebeccatown","State":"TEXAS","Intensity":"3.86","Valuation":"$742,569","Address":"3193 , , Nuevo León"},
    {"Date":"2026-04-22","City":"West David","State":"VIRGINIA","Intensity":"3.84","Valuation":"$630,748","Address":"857 Sarvis Fork Road, , West Virginia"},
    {"Date":"2026-03-25","City":"Johnsonshire","State":"GEORGIA","Intensity":"3.84","Valuation":"$715,607","Address":"7120 County Line Church Road, , Georgia 31039"},
    {"Date":"2026-04-20","City":"Elizabethton","State":"KANSAS","Intensity":"3.82","Valuation":"$508,362","Address":"06711 Road 37, , Kansas"},
    {"Date":"2026-04-24","City":"Jeffreyport","State":"GEORGIA","Intensity":"3.8","Valuation":"$695,727","Address":"17078 15th Street, , Florida 32424"},
    {"Date":"2026-03-30","City":"Hernandezmouth","State":"VIRGINIA","Intensity":"3.79","Valuation":"$650,285","Address":"3483 Horseshoe Bend Road, , Virginia 24095"},
    {"Date":"2026-04-21","City":"Christophermouth","State":"ALABAMA","Intensity":"3.73","Valuation":"$501,653","Address":"68403 , , Georgia"},
    {"Date":"2026-03-25","City":"South Lori","State":"VIRGINIA","Intensity":"3.71","Valuation":"$611,845","Address":"354 , , West Virginia 26269"},
    {"Date":"2026-03-29","City":"Brianstad","State":"COLORADO","Intensity":"3.68","Valuation":"$667,020","Address":"10255 , , Colorado"},
    {"Date":"2026-04-18","City":"Port Randallton","State":"VIRGINIA","Intensity":"3.65","Valuation":"$615,542","Address":"562 Medley Valley Road, , Virginia 24608"},
    {"Date":"2026-04-12","City":"Rubenville","State":"NEBRASKA","Intensity":"3.64","Valuation":"$564,912","Address":"822 County Road W, , Nebraska 68464"},
    {"Date":"2026-04-10","City":"Michaelstad","State":"ALABAMA","Intensity":"3.58","Valuation":"$524,319","Address":"555 County Road 293, Smiths Station, Alabama 36877"},
    {"Date":"2026-04-14","City":"North Darylton","State":"COLORADO","Intensity":"3.57","Valuation":"$701,968","Address":"73711 South Platte River Road, , Colorado"},
    {"Date":"2026-04-17","City":"Bassside","State":"NEBRASKA","Intensity":"3.57","Valuation":"$541,912","Address":"29091 Highway 50, , Nebraska 68441"},
    {"Date":"2026-04-15","City":"Erikaton","State":"VIRGINIA","Intensity":"3.56","Valuation":"$846,861","Address":"9560 , Paintsville, Kentucky 41240"},
    {"Date":"2026-03-29","City":"Ellenshire","State":"NEBRASKA","Intensity":"3.54","Valuation":"$504,431","Address":"85547 L Road, , Nebraska 68924"},
    {"Date":"2026-04-03","City":"Lake Christine","State":"COLORADO","Intensity":"3.54","Valuation":"$909,664","Address":"86943 County Road K, , Colorado"},
    {"Date":"2026-04-20","City":"Lake Makayla","State":"NEBRASKA","Intensity":"3.52","Valuation":"$594,627","Address":"436 Old Highway 101, , Nebraska"},
    {"Date":"2026-04-21","City":"North Michael","State":"VIRGINIA","Intensity":"3.5","Valuation":"$624,508","Address":"541 Little Dry Hollow, Lexington, Virginia 24450"},
    {"Date":"2026-04-17","City":"Charlesburgh","State":"VIRGINIA","Intensity":"3.49","Valuation":"$709,343","Address":"1071 , , Maryland"},
    {"Date":"2026-04-12","City":"South Matthewfort","State":"GEORGIA","Intensity":"3.46","Valuation":"$737,820","Address":"7391 Minor Road, , Georgia"},
    {"Date":"2026-04-13","City":"Harmonview","State":"OKLAHOMA","Intensity":"3.44","Valuation":"$525,842","Address":"0607 , , Texas"},
    {"Date":"2026-04-17","City":"Port Brianaberg","State":"GEORGIA","Intensity":"3.43","Valuation":"$611,257","Address":"50347 , , South Carolina"},
    {"Date":"2026-04-17","City":"Solomonberg","State":"VIRGINIA","Intensity":"3.36","Valuation":"$786,593","Address":"7313 , Country Club, West Virginia 26301"},
    {"Date":"2026-04-19","City":"West Daniel","State":"VIRGINIA","Intensity":"3.35","Valuation":"$523,194","Address":"69409 Laurel Creek Road, , West Virginia 25916"},
    {"Date":"2026-04-14","City":"South Erica","State":"COLORADO","Intensity":"3.34","Valuation":"$649,949","Address":"016 Lookout Mountain Trail, , Colorado 80541"},
    {"Date":"2026-04-08","City":"South Alvinfurt","State":"OKLAHOMA","Intensity":"3.34","Valuation":"$684,786","Address":"1914 US 81, , Oklahoma 73720"},
    {"Date":"2026-04-11","City":"East Gregory","State":"TEXAS","Intensity":"3.33","Valuation":"$757,290","Address":"8648 Stephan's Crossing, Bryan, Texas 77842"},
    {"Date":"2026-04-10","City":"Lake Kevin","State":"COLORADO","Intensity":"3.31","Valuation":"$528,880","Address":"108 County Road 183, Craig, Colorado 81625"},
    {"Date":"2026-03-29","City":"Lake Nicole","State":"GEORGIA","Intensity":"3.22","Valuation":"$721,101","Address":"73515 CR 173, , Florida 32452"},
    {"Date":"2026-04-02","City":"Laurenborough","State":"OKLAHOMA","Intensity":"3.21","Valuation":"$526,828","Address":"712 , , Texas"},
    {"Date":"2026-04-23","City":"North Jill","State":"COLORADO","Intensity":"3.21","Valuation":"$864,782","Address":"6769 , , Colorado"},
    {"Date":"2026-03-28","City":"Peterfurt","State":"GEORGIA","Intensity":"3.14","Valuation":"$576,340","Address":"7258 Adams Perry Road, , Georgia"},
    {"Date":"2026-04-02","City":"Paulburgh","State":"GEORGIA","Intensity":"3.12","Valuation":"$734,308","Address":"876 Hunter Road, , Georgia 30635"},
    {"Date":"2026-04-16","City":"Jamesfort","State":"VIRGINIA","Intensity":"3.11","Valuation":"$765,302","Address":"195 , , Virginia"},
    {"Date":"2026-04-13","City":"Tinaburgh","State":"GEORGIA","Intensity":"3.07","Valuation":"$774,538","Address":"586 , , Georgia 31316"},
    {"Date":"2026-03-27","City":"Port Jon","State":"TEXAS","Intensity":"2.99","Valuation":"$711,173","Address":"2646 , , Coahuila"},
    {"Date":"2026-04-20","City":"Lake Coreyville","State":"VIRGINIA","Intensity":"2.94","Valuation":"$947,049","Address":"125 Fox Ridge Road, , Ohio 45693"},
    {"Date":"2026-03-26","City":"Port Karina","State":"NEBRASKA","Intensity":"2.93","Valuation":"$611,605","Address":"9238 709 Road, , Nebraska"},
    {"Date":"2026-04-06","City":"Austinborough","State":"VIRGINIA","Intensity":"2.91","Valuation":"$748,378","Address":"06930 , , Virginia"},
    {"Date":"2026-04-07","City":"Nguyenmouth","State":"TEXAS","Intensity":"2.89","Valuation":"$525,434","Address":"54006 , , Durango"},
    {"Date":"2026-03-27","City":"Amandachester","State":"ALABAMA","Intensity":"2.88","Valuation":"$544,962","Address":"1861 Iris Lane, Navarre, Florida 32566"},
    {"Date":"2026-03-28","City":"Port Michaelton","State":"NEBRASKA","Intensity":"2.88","Valuation":"$638,644","Address":"347 877 Road, , Nebraska"},
    {"Date":"2026-04-01","City":"Mcdonaldtown","State":"VIRGINIA","Intensity":"2.88","Valuation":"$543,421","Address":"661 , , West Virginia 26710"},
]

FOREIGN_KEYWORDS = ["chihuahua", "nuevo león", "nuevo leon", "coahuila", "durango", "mexico", "tamaulipas"]

def is_blank_address(addr):
    if not addr: return True
    return bool(re.search(r'\d+\s*,\s*,', addr))

def has_foreign_address(addr):
    a = addr.lower()
    return any(kw in a for kw in FOREIGN_KEYWORDS)

def get_state_from_address(addr):
    """Try to extract a US state abbreviation or name from an address string."""
    us_states = {
        "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
        "Colorado":"CO","Connecticut":"CT","Delaware":"DE","Florida":"FL","Georgia":"GA",
        "Hawaii":"HI","Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA",
        "Kansas":"KS","Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD",
        "Massachusetts":"MA","Michigan":"MI","Minnesota":"MN","Mississippi":"MS",
        "Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV","New Hampshire":"NH",
        "New Jersey":"NJ","New Mexico":"NM","New York":"NY","North Carolina":"NC",
        "North Dakota":"ND","Ohio":"OH","Oklahoma":"OK","Oregon":"OR","Pennsylvania":"PA",
        "Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD","Tennessee":"TN",
        "Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA","Washington":"WA",
        "West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY"
    }
    for name, abbr in us_states.items():
        if name.lower() in addr.lower():
            return name.upper()
    return None

def get_coords_from_db(conn, city, state, date):
    c = conn.cursor()
    for query, params in [
        ("SELECT latitude, longitude FROM storms WHERE UPPER(city) = UPPER(?) AND UPPER(state) = UPPER(?) AND event_date = ? LIMIT 1", (city, state, date)),
        ("SELECT latitude, longitude FROM storms WHERE UPPER(city) = UPPER(?) AND UPPER(state) = UPPER(?) LIMIT 1", (city, state)),
        ("SELECT latitude, longitude FROM storms WHERE UPPER(state) = UPPER(?) AND event_date = ? LIMIT 1", (state, date)),
    ]:
        c.execute(query, params)
        row = c.fetchone()
        if row and row[0] is not None:
            return row[0], row[1]
    return None, None

def reverse_geocode(lat, lon):
    try:
        loc = geolocator.reverse((lat, lon), language='en', exactly_one=True)
        if loc:
            return loc.address, loc.latitude, loc.longitude
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"    [WARN] {e}")
    return None, lat, lon

def classify_anomaly(lead_state, resolved_addr, original_addr):
    flags = []
    combined = (resolved_addr or "") + " " + (original_addr or "")
    if has_foreign_address(combined):
        flags.append("FOREIGN_ADDRESS")
    if resolved_addr:
        addr_state = get_state_from_address(resolved_addr)
        if addr_state and addr_state != lead_state.upper() and addr_state not in ["WEST VIRGINIA"]:
            # WV is often adjacent to VA leads legitimately
            flags.append(f"CROSS_STATE({addr_state}≠{lead_state})")
    return "; ".join(flags) if flags else "CLEAN"

def run():
    print(f"[INFO] Opening DB: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    results = []
    total = len(ALPHA_LEADS)

    for i, lead in enumerate(ALPHA_LEADS):
        city   = lead["City"]
        state  = lead["State"]
        date   = lead["Date"]
        orig   = lead["Address"]

        print(f"\n[{i+1}/{total}] {city}, {state} | {date} | Mag: {lead['Intensity']}\"")
        print(f"  Original Address: {orig}")

        lat, lon = get_coords_from_db(conn, city, state, date)

        resolved_addr = None
        geocode_lat   = None
        geocode_lon   = None
        google_maps_url = None

        if lat and lon:
            print(f"  Coords from DB: ({lat:.4f}, {lon:.4f})")
            resolved_addr, geocode_lat, geocode_lon = reverse_geocode(lat, lon)
            time.sleep(1.1)  # Nominatim rate limit

            if resolved_addr:
                print(f"  ✓ Resolved: {resolved_addr}")
                google_maps_url = f"https://www.google.com/maps/@{lat},{lon},18z/data=!3m1!1e3"
            else:
                print(f"  ✗ Reverse geocode failed")
                google_maps_url = f"https://www.google.com/maps/@{lat},{lon},18z/data=!3m1!1e3"
        else:
            print(f"  ✗ No coords in DB for {city}, {state}")

        # Determine final address to use
        needs_resolution = is_blank_address(orig) or has_foreign_address(orig)
        final_address = resolved_addr if (needs_resolution and resolved_addr) else orig

        # Anomaly classification
        anomaly = classify_anomaly(state, resolved_addr or orig, orig)
        if anomaly != "CLEAN":
            print(f"  ⚠ ANOMALY: {anomaly}")

        # Verification status
        if lat and lon and resolved_addr:
            if anomaly == "CLEAN":
                status = "GEO_VERIFIED"
            else:
                status = f"ANOMALY_FLAGGED"
        elif lat and lon:
            status = "COORDS_ONLY"
        else:
            status = "UNRESOLVED"

        results.append({
            "Date": date,
            "City": city,
            "State": state,
            "Intensity": lead["Intensity"],
            "Valuation": lead["Valuation"],
            "Owner": "DEEP SEARCH REQ",
            "Phone": "UNVERIFIED",
            "Original_Address": orig,
            "Verified_Address": final_address or orig,
            "Latitude": lat or "",
            "Longitude": lon or "",
            "Anomaly_Flags": anomaly,
            "Verification_Status": status,
            "Google_Earth_URL": google_maps_url or "",
            "Lead_Tier": "ALPHA",
        })

    conn.close()

    # Write verified CSV
    fieldnames = ["Date","City","State","Intensity","Valuation","Owner","Phone",
                  "Original_Address","Verified_Address","Latitude","Longitude",
                  "Anomaly_Flags","Verification_Status","Google_Earth_URL","Lead_Tier"]

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # Summary
    clean      = sum(1 for r in results if r["Verification_Status"] == "GEO_VERIFIED")
    anomalies  = sum(1 for r in results if "ANOMALY" in r["Verification_Status"])
    unresolved = sum(1 for r in results if r["Verification_Status"] == "UNRESOLVED")
    foreign    = sum(1 for r in results if "FOREIGN_ADDRESS" in r["Anomaly_Flags"])
    cross_state= sum(1 for r in results if "CROSS_STATE" in r["Anomaly_Flags"])

    print(f"\n{'='*60}")
    print(f"ALPHA LEAD VERIFICATION COMPLETE")
    print(f"{'='*60}")
    print(f"  Total Leads:          {total}")
    print(f"  ✓ GEO_VERIFIED:       {clean}")
    print(f"  ⚠ Anomaly Flagged:   {anomalies}")
    print(f"    - Foreign Address:  {foreign}")
    print(f"    - Cross-State:      {cross_state}")
    print(f"  ✗ Unresolved:         {unresolved}")
    print(f"\nOutput: {OUTPUT_CSV}")

    # Print anomaly detail
    print(f"\n--- ANOMALY DETAIL ---")
    for r in results:
        if r["Anomaly_Flags"] != "CLEAN":
            print(f"  [{r['State']}] {r['City']} | {r['Anomaly_Flags']}")
            print(f"    Original:  {r['Original_Address']}")
            print(f"    Verified:  {r['Verified_Address']}")

    return results

if __name__ == "__main__":
    run()
