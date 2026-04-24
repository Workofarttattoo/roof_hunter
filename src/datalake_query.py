import sqlite3
import pandas as pd
import argparse
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'authoritative_storms.db')

def query_datalake(state=None, min_income=None, min_hail=None, has_demographics=True):
    """
    Query the authoritative storm datalake for socioeconomic insurance leads.
    """
    conn = sqlite3.connect(DB_PATH)
    
    query = "SELECT * FROM storms WHERE 1=1"
    params = []
    
    if state:
        query += " AND state = ?"
        params.append(state.upper())
    
    if min_income:
        query += " AND median_household_income >= ?"
        params.append(min_income)
        
    if min_hail:
        query += " AND event_type = 'Hail' AND magnitude >= ?"
        params.append(min_hail)
        
    if has_demographics:
        query += " AND population IS NOT NULL"
        
    query += " ORDER BY magnitude DESC, median_household_income DESC LIMIT 20"
        
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    if df.empty:
        print("No matching storms found in the Authoritative Data Lake.")
        return
        
    print(f"\n--- ⛈️ ECH0-ROOF DATALAKE SEARCH RESULTS ⛈️ ---")
    print(f"Found {len(df)} High-Value Neighborhoods Hit by Severe Weather:")
    print("-" * 80)
    for index, row in df.iterrows():
        print(f"📍 {row['zipcode']} | {row['city']}, {row['state']} | {row['event_date']}")
        print(f"   🌪️ Threat: {row['magnitude']}\" {row['event_type']}")
        print(f"   💰 Socioeconomics: ${row['median_home_value']:,.0f} Median Home | ${row['median_household_income']:,.0f} Household Income | Pop: {row['population']}")
        print("-" * 80)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search the Authoritative Storms Data Lake by Socioeconomic metrics.")
    parser.add_argument('--state', type=str, help="Filter by State (e.g. TEXAS)")
    parser.add_argument('--min-income', type=int, help="Minimum Median Household Income (e.g. 80000)")
    parser.add_argument('--min-hail', type=float, help="Minimum Hail Size in Inches (e.g. 1.5)")
    
    args = parser.parse_args()
    
    # Run query
    query_datalake(state=args.state, min_income=args.min_income, min_hail=args.min_hail)
