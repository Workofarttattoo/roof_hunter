import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

def sync_telnyx_webhook(new_url):
    api_key = os.getenv('TELNYX_API_KEY')
    connection_id = os.getenv('TELNYX_CONNECTION_ID')
    
    # 🎯 TARGET: Update Webhook to current tunnel
    # https://api.telnyx.com/v2/texml_applications/{id}
    # Note: Telnyx uses 'texml_applications' for TwiML-style or 'call_control_applications'
    # Given the previous context, we are likely using a TeXML application.
    
    url = f"https://api.telnyx.com/v2/texml_applications/{connection_id}"
    
    # First, GET current config to confirm ID type
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    print(f"🔍 Checking Telnyx Connection {connection_id}...")
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        # Try call_control_applications as fallback
        url = f"https://api.telnyx.com/v2/call_control_applications/{connection_id}"
        r = requests.get(url, headers=headers)
    
    if r.status_code == 200:
        data = r.json().get('data', {})
        print(f"✅ Found Application: {data.get('friendly_name')}")
        print(f"📡 Current Webhook: {data.get('webhook_event_url') or data.get('voice_url')}")
        
        # UPDATE IT
        patch_payload = {
            "webhook_event_url": f"{new_url}/api/texml"
        }
        if "voice_url" in data: # TeXML app uses voice_url
            patch_payload = {"voice_url": f"{new_url}/api/texml"}
            
        print(f"🚀 Updating Webhook to: {new_url}/api/texml ...")
        u = requests.patch(url, json=patch_payload, headers=headers)
        if u.status_code == 200:
            print("✨ WEBHOOK SYNC SUCCESS!")
        else:
            print(f"❌ UPDATE FAIL ({u.status_code}): {u.text}")
    else:
        print(f"❌ COULD NOT FIND APPLICATION ({r.status_code}): {r.text}")

if __name__ == "__main__":
    NEW_URL = "https://1ad5e832699c294b-184-186-101-13.serveousercontent.com"
    sync_telnyx_webhook(NEW_URL)
