import os
import requests
import time
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('TELNYX_API_KEY')
conn_id = os.getenv('TELNYX_CONNECTION_ID')
from_phone = os.getenv('TELNYX_PHONE_NUMBER')

headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

payload = {
    "connection_id": conn_id,
    "to": "+17735551234", # Test number
    "from": from_phone
}

print("--- Telnyx Forensic Handshake Poller Started ---")
while True:
    try:
        r = requests.post('https://api.telnyx.com/v2/calls', json=payload, headers=headers)
        if r.status_code == 201:
            print("🚀 GREEN LIGHT: Telnyx Dispatch Unleashed!")
            # Trigger the full agent
            os.system("export PYTHONPATH=$PYTHONPATH:. && venv/bin/python src/telnyx_voice_agent.py")
            break
        elif r.status_code == 403 and "channel limit" in r.text:
            print("⏳ Still D1 Locked. KYC Evaluation pending...")
        else:
            print(f"📡 Status: {r.status_code} - {r.text[:100]}")
    except Exception as e:
        print(f"❌ Poller Error: {e}")
    
    time.sleep(60)
