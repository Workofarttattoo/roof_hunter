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

print("--- Checking API Key Permissions & Profile ---")
r = requests.get('https://api.telnyx.com/v2/api_keys/me', headers=headers)
print(r.json())

print("\n--- Listing TeXML Numbers ---")
r = requests.get('https://api.telnyx.com/v2/phone_numbers', headers=headers)
print(json.dumps(r.json(), indent=2))
