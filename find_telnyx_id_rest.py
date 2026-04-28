import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('TELNYX_API_KEY')
headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

print("--- Fetching TeXML Applications (REST) ---")
r = requests.get('https://api.telnyx.com/v2/texml_applications', headers=headers)
if r.status_code == 200:
    for app in r.json().get('data', []):
        print(f"Name: {app.get('name')} | ID: {app.get('id')}")
else:
    print(f"Error: {r.status_code} - {r.text}")

print("\n--- Fetching Call Control Applications (REST) ---")
r = requests.get('https://api.telnyx.com/v2/call_control_applications', headers=headers)
if r.status_code == 200:
    for app in r.json().get('data', []):
        print(f"Name: {app.get('app_name')} | ID: {app.get('id')}")
else:
    print(f"Error: {r.status_code} - {r.text}")
