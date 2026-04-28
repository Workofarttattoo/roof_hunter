import requests
import os
from dotenv import load_dotenv

load_dotenv()

def generate_charles_authority_pitch():
    api_key = os.getenv("ELEVENLABS_API_KEY")
    # Charles / Charlie (High Authority Male)
    voice_id = "IKne3meq5aSn9XLyUdCD" 
    
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    
    headers = {
        "xi-api-key": api_key,
        "Content-Type": "application/json"
    }
    
    # Authoritative "Not a Sales Call" County Script
    script = (
        "Hello, this is Charles with the forensic dispatch team. Specifically, this is NOT a sales call. "
        "We are reaching out as part of a County initiative to identify properties with confirmed structural damage from the recent storm. "
        "Our satellite scan has flagged your roof for high-intensity impact. "
        "We have inspectors in your direct neighborhood today performing free visual verifications. "
        "If you would like to be included in today's visit manifest, simply say YES now, or reply YES to the text we just sent."
    )
    
    payload = {
        "text": script,
        "model_id": "eleven_monolingual_v1",
        "voice_settings": {
            "stability": 0.6,
            "similarity_boost": 0.8
        }
    }
    
    print("🎙️ Generating Authoritative Charles Script...")
    res = requests.post(url, json=payload, headers=headers)
    
    if res.status_code == 200:
        save_path = "training_data/charles_pitch.mp3"
        with open(save_path, "wb") as f:
            f.write(res.content)
        print(f"✅ Charles Authority Pitch Secured: {save_path}")
        return True
    else:
        print(f"❌ Generation Failed: {res.text}")
        return False

if __name__ == "__main__":
    generate_charles_authority_pitch()
