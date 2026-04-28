import os
import telnyx
from dotenv import load_dotenv

load_dotenv()
telnyx.api_key = os.getenv('TELNYX_API_KEY')

try:
    print("--- Fetching TeXML Applications ---")
    apps = telnyx.TeXMLApplication.list()
    for app in apps:
        print(f"Name: {app.name} | ID: {app.id}")
        
    print("\n--- Fetching Call Control Applications ---")
    apps_v2 = telnyx.CallControlApplication.list()
    for app in apps_v2:
        print(f"Name: {app.app_name} | ID: {app.id}")
except Exception as e:
    print(f"Error: {e}")
