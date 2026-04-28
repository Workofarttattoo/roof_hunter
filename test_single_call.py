import os
import requests
import json
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
    "to": "+17735551234",
    "from": from_phone
}

print(f"Testing Call Control via v2/calls with {conn_id}...")
r = requests.post('https://api.telnyx.com/v2/calls', json=payload, headers=headers)
print(f"Status: {r.status_code}")
print(f"Response: {json.dumps(r.json(), indent=2)}")
