import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('TELNYX_API_KEY')
headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

endpoints = [
    'https://api.telnyx.com/v2/texml/calls',
    'https://api.telnyx.com/v2/texml_calls',
    'https://api.telnyx.com/v2/calls',
    'https://api.telnyx.com/v2/texml/call'
]

for url in endpoints:
    print(f"Testing {url}...")
    # Using a dummy payload to see if we get a 404 or a validation error (which means 404 was NOT there)
    r = requests.post(url, json={}, headers=headers)
    print(f"Result: {r.status_code}")
    if r.status_code != 404:
        print(f"Possible match: {url} -> {r.text}")
    print("-" * 20)
