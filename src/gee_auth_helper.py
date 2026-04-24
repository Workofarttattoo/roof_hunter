import os
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

class GEEAuthHelper:
    """
    Automates the OAuth 2.0 flow for Google Earth Engine.
    Exchanges the 'client_secret.json' for a permanent 'token.json'.
    """
    def __init__(self, client_secret_path='client_secret.json', token_path='token.json'):
        self.client_secret_path = client_secret_path
        self.token_path = token_path
        self.scopes = ['https://www.googleapis.com/auth/earthengine', 
                       'https://www.googleapis.com/auth/cloud-platform']

    def authenticate(self):
        creds = None
        # Check if token already exists
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, self.scopes)
        
        # If no valid credentials, run the flow
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(self.client_secret_path):
                    raise FileNotFoundError(f"Missing {self.client_secret_path}. Please provide client_secret.json first.")
                
                # Run the flow - with manual fallback if local server fails
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(self.client_secret_path, self.scopes)
                    creds = flow.run_local_server(port=0, open_browser=True)
                except Exception as e:
                    logger.warning(f"Auto-browser failed ({e}). Switching to Manual URL mode...")
                    creds = flow.run_console()
            
            # Save the credentials for the next run
            with open(self.token_path, 'w') as token:
                token.write(creds.to_json())
        
        return creds

if __name__ == "__main__":
    helper = GEEAuthHelper()
    try:
        cred = helper.authenticate()
        print("SUCCESS: Google Earth Engine authenticated. Token saved to token.json.")
    except Exception as e:
        print(f"AUTH FAILED: {e}")
