import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

def call_user_cell():
    api_key = os.getenv('TELNYX_API_KEY')
    connection_id = os.getenv('TELNYX_CONNECTION_ID')
    
    # 🎯 TARGET: User's Cell
    to_phone = "+17252241240"
    from_phone = "+19458370141" # Oklahoma Uplink
    
    print(f"🚀 INITIATING MANUAL FORENSIC CALL TO USER -> {to_phone} via {from_phone}")
    
    url = f"https://api.telnyx.com/v2/texml/calls/{connection_id}"
    payload = {
        "To": to_phone,
        "From": from_phone,
        "Url": "https://1ad5e832699c294b-184-186-101-13.serveousercontent.com/api/texml"
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code in [200, 201]:
            print(f"✅ SUCCESS: {response.json().get('data', {}).get('call_control_id')}")
        else:
            print(f"❌ FAIL ({response.status_code}): {response.text}")
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    call_user_cell()
