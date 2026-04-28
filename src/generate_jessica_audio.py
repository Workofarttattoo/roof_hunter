import requests
import os
from dotenv import load_dotenv

load_dotenv()

def generate_jessica_main_pitch():
    api_key = os.getenv("ELEVENLABS_API_KEY")
    voice_id = "cgSgspJ2msm6clMCkdW9" # Jessica
    
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    
    headers = {
        "xi-api-key": api_key,
        "Content-Type": "application/json"
    }
    
    # Generic but personalized-sounding script
    script = (
        "Hello, this is the Roof Hunter forensic team. Our satellite scan just flagged "
        "your property for significant hail impact during the recent storm. "
        "We have inspectors in your neighborhood today for a County initiative. "
        "Replying YES to this call tells us you want a 100 percent free verification visit. "
        "Otherwise, please just say YES now."
    )
    
    payload = {
        "text": script,
        "model_id": "eleven_monolingual_v1",
        "voice_settings": {
            "stability": 0.5,
            "similarity_boost": 0.75
        }
    }
    
    print("🎙️ Generating Premium Jessica Script...")
    res = requests.post(url, json=payload, headers=headers)
    
    if res.status_code == 200:
        save_path = "training_data/jessica_pitch.mp3"
        with open(save_path, "wb") as f:
            f.write(res.content)
        print(f"✅ Jessica Pitch Secured: {save_path}")
        return True
    else:
        print(f"❌ Generation Failed: {res.text}")
        return False

if __name__ == "__main__":
    generate_jessica_main_pitch()
