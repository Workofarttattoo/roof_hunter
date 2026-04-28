import os
from dotenv import load_dotenv
from elevenlabs.client import ElevenLabs

load_dotenv()

api_key = os.getenv('ELEVENLABS_API_KEY')
cl = ElevenLabs(api_key=api_key)

try:
    voices = cl.voices.get_all()
    print(f"✅ SUCCESS: ElevenLabs API is active.")
    print(f"Available Voices: {len(voices.voices)}")
    for v in voices.voices[:3]:
        print(f" - {v.name} ({v.voice_id})")
except Exception as e:
    print(f"❌ ERROR: {e}")
