import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('TELNYX_API_KEY')
app_id = "rppfhunterpro"

headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

url = "https://api.telnyx.com/v2/calls"
payload = {
    "connection_id": app_id,
    "to": "+17735551234", 
    "from": os.getenv('TELNYX_PHONE_NUMBER')
}

print(f"Testing Call via {url} with connection_id={app_id}")
r = requests.post(url, json=payload, headers=headers)
print(f"Result: {r.status_code}")
print(r.text)
