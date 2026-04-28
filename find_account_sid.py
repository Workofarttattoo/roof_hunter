import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('TELNYX_API_KEY')
headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

print("--- Fetching Account Information ---")
r = requests.get('https://api.telnyx.com/v2/account', headers=headers)
if r.status_code == 200:
    print(r.json())
else:
    print(f"Error: {r.status_code} - {r.text}")

print("\n--- Fetching TeXML Details ---")
r = requests.get('https://api.telnyx.com/v2/texml_applications/2947084874936223207', headers=headers)
if r.status_code == 200:
    print(r.json())
else:
    print(f"Error: {r.status_code} - {r.text}")
