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

print("--- Listing Connections ---")
r = requests.get('https://api.telnyx.com/v2/connections', headers=headers)
print(json.dumps(r.json(), indent=2))
