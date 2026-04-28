import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('TELNYX_API_KEY')
headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

print("--- Fetching Account Details (V2) ---")
r = requests.get('https://api.telnyx.com/v2/account/details', headers=headers)
if r.status_code == 200:
    print(json.dumps(r.json(), indent=2))
else:
    print(f"Error: {r.status_code} - {r.text}")
