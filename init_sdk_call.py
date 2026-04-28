import os
import telnyx
from dotenv import load_dotenv

load_dotenv()
telnyx.api_key = os.getenv('TELNYX_API_KEY')

print("--- Listing TeXML Applications ---")
apps = telnyx.TeXMLApplication.list()
for app in apps:
    print(f"Name: {app.friendly_name} | ID: {app.id}")
    if app.friendly_name == "roof hunter":
        print(f"Found it! Trying to initiate call to +17735551234...")
        try:
            # Try initiating via the application object if it has create method
            # Actually, TeXML calls are often via telnyx.Call.create with connection_id
            call = telnyx.Call.create(
                to="+17735551234",
                from_=os.getenv('TELNYX_PHONE_NUMBER'),
                connection_id=app.id
            )
            print(f"Call initiated! SID: {call.id}")
        except Exception as e:
            print(f"Failed to initiate call: {e}")
