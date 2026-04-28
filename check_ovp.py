import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('TELNYX_API_KEY')
ovp_id = "2946989405346202766"

headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

print(f"--- Checking Outbound Voice Profile {ovp_id} ---")
r = requests.get(f'https://api.telnyx.com/v2/outbound_voice_profiles/{ovp_id}', headers=headers)
print(json.dumps(r.json(), indent=2))
