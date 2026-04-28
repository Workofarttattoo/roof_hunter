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

print("--- TeXML Applications ---")
r = requests.get('https://api.telnyx.com/v2/texml_applications', headers=headers)
print(json.dumps(r.json(), indent=2))

print("\n--- Call Control Applications ---")
r = requests.get('https://api.telnyx.com/v2/call_control_applications', headers=headers)
print(json.dumps(r.json(), indent=2))
