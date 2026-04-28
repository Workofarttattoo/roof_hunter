import os
import sys
from dotenv import load_dotenv
from twilio.rest import Client

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from twilio_voice_agent import TwilioVoiceAgent

load_dotenv()

def run_test_call(target_phone):
    print(f"🚀 INITIALIZING ELEVENLABS TEST CALL TO: {target_phone}")
    
    agent = TwilioVoiceAgent()
    
    # Rachel Voice ID
    voice_id = "21m00Tcm4TlvDq8ikWAM"
    
    test_name = "Roof Hunter Partner"
    test_address = "123 Main St, Omaha, NE"
    
    # We'll call the agent's internal method directly for the test
    success = agent.make_verification_call(
        lead_id=9999, 
        name=test_name, 
        phone=target_phone, 
        address=test_address
    )
    
    if success:
        print("✅ TEST CALL DISPATCHED SUCCESSFULLY.")
        print(f"Voice Provider: ElevenLabs (Neural)")
        print(f"Voice ID: {voice_id} (Rachel)")
    else:
        print("❌ TEST CALL FAILED. Check Twilio logs or credentials.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: venv/bin/python scripts/test_voice_call.py <YOUR_PHONE_NUMBER>")
        print("Example: venv/bin/python scripts/test_voice_call.py +15550001234")
    else:
        num = sys.argv[1]
        run_test_call(num)
