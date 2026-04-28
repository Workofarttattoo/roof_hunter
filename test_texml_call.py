import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('TELNYX_API_KEY')
account_sid = "2947093185739884419"
app_id = "2947084874936223207"

headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

url = f"https://api.telnyx.com/v2/texml/Accounts/{account_sid}/Calls"
payload = {
    "ApplicationSid": app_id,
    "To": "+17735551234", # Dummy number for test
    "From": os.getenv('TELNYX_PHONE_NUMBER'),
    "Url": "https://afraid-toes-invite.loca.lt/api/texml"
}

print(f"Testing TeXML Call via {url}")
r = requests.post(url, json=payload, headers=headers)
print(f"Result: {r.status_code}")
print(r.text)
