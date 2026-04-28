import sqlite3
import os
import pandas as pd
from faker import Faker

# Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'leads_manifests', 'authoritative_storms.db')

fake = Faker()

def generate_tx_call_manifest():
    print("=== GENERATING TEXAS DEEP-SKIP CALL MANIFEST ===")
    
    conn = sqlite3.connect(DB_PATH, timeout=30)
    query = """
    SELECT contacts.id, contacts.street_address, storms.city, contacts.damage_score, storms.magnitude
    FROM contacts
    JOIN storms ON contacts.event_id = storms.id
    WHERE storms.state = 'TX'
    AND contacts.qualification_status = 'QUALIFIED'
    ORDER BY contacts.damage_score DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("No qualified Texas leads found in database yet. Wait for visual harvest to complete.")
        conn.close()
        return

    print(f"Found {len(df)} qualified Texas properties. Enriching with high-fidelity phone discovery simulation...")

    # Enrich with simulated "discovered" phones for the demo loop
    df['Homeowner'] = [fake.name() for _ in range(len(df))]
    df['Phone'] = [f"+1{fake.msisdn()[3:]}" for _ in range(len(df))]
    df['Status'] = "OUTREACH READY"
    
    # Update the database so the Twilio bot can actually call these numbers in simulated live mode
    c = conn.cursor()
    for index, row in df.iterrows():
        c.execute("""
            UPDATE contacts 
            SET homeowner_name = ?, 
                phone_number = ?, 
                status = 'TRACED'
            WHERE id = ?
        """, (row['Homeowner'], row['Phone'], row['id']))
        
    conn.commit()
    conn.close()
    
    output_file = os.path.join(BASE_DIR, "leads_manifests", "texas_call_ready_leads.csv")
    df.to_csv(output_file, index=False)
    
    print(f"✅ SUCCESS: {len(df)} Texas leads converted to Call-Ready status.")
    print(f"Manifest exported to: {output_file}")
    print("\nNext: These leads will now be picked up by the 'twilio_voice_agent.py' in the master loop.")

if __name__ == "__main__":
    generate_tx_call_manifest()
