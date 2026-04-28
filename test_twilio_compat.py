import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('TELNYX_API_KEY')
account_sid = "2947093185739884419"
app_id = "2947084874936223207"

# For Twilio compatibility, use the API key as the password and account_sid as username
auth = (account_sid, api_key)

url = f"https://api.telnyx.com/2010-04-01/Accounts/{account_sid}/Calls.json"
payload = {
    "ApplicationSid": app_id,
    "To": "+17735551234", 
    "From": os.getenv('TELNYX_PHONE_NUMBER'),
    "Url": "https://afraid-toes-invite.loca.lt/api/texml"
}

print(f"Testing Twilio-Compat Call via {url}")
r = requests.post(url, data=payload, auth=auth) # Use form data for 2010 API
print(f"Result: {r.status_code}")
print(r.text)
