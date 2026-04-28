import sys
import os
import json
import base64
import requests
import logging
from dotenv import dotenv_values

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def trigger_diagnostic_call(to_phone):
    env = dotenv_values(".env")
    api_key = env.get("TELNYX_API_KEY")
    connection_id = "2947093185739884419" # RoofHunterPro
    
    # We force lead_id to 2 so (2 % 3 == 2) -> CHARLIE
    # Wait, 2 % 3 is 2. Yes.
    metadata = {
        "lead_id": 2, 
        "name": "Joshua", 
        "address": "Your Professional Workspace",
        "voice_group": "CHARLIE_C_COUNTY"
    }
    client_state = base64.b64encode(json.dumps(metadata).encode()).decode()
    
    url = "https://api.telnyx.com/v2/calls"
    payload = {
        "connection_id": connection_id,
        "to": to_phone,
        "from": env.get("TELNYX_PHONE_NUMBER"),
        "client_state": client_state
    }
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    logger.info(f"🚀 INITIATING DIAGNOSTIC FLIGHT TO {to_phone} (Forcing Charlie Variant)...")
    r = requests.post(url, json=payload, headers=headers)
    print(r.json())

if __name__ == "__main__":
    target = "+17252241240"
    trigger_diagnostic_call(target)
