import os
from twilio.rest import Client
from dotenv import load_dotenv

load_dotenv()

def place_twilio_test():
    account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    client = Client(account_sid, auth_token)
    
    from_phone = "+18559164164" # Twilio Verified Number
    to_phone = "+17252241240" # User's verified number
    
    # TeXML-compatible TwiML pointing to our pre-rendered Charles audio
    tunnel_url = "https://0c152e97aa78cd20-68-104-115-56.serveousercontent.com"
    twiml = f"<Response><Play>{tunnel_url}/images/charles_pitch.mp3</Play></Response>"
    
    print(f"🚀 INITIATING TWILIO FAIL-OVER TEST -> {to_phone}")
    try:
        call = client.calls.create(
            twiml=twiml,
            to=to_phone,
            from_=from_phone
        )
        print(f"✅ Call Initiated: {call.sid}")
    except Exception as e:
        print(f"❌ Twilio Error: {e}")

if __name__ == "__main__":
    place_twilio_test()
