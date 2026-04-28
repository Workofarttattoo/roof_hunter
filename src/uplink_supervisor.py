import subprocess
import time
import re
import os

def start_pinggy_and_sync():
    print("🚀 STARTING FORENSIC UPLINK (PINGGY)...")
    cmd = ['ssh', '-tt', '-o', 'StrictHostKeyChecking=no', '-p', '443', '-R0:localhost:8000', 'qr@a.pinggy.io']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, stdin=subprocess.DEVNULL)
    
    url = None
    # Wait for URL in first 20 lines
    for _ in range(50):
        line = p.stdout.readline()
        print(f"PINGGY: {line.strip()}")
        if 'https://' in line and 'pinggy-free.link' in line:
            match = re.search(r'https://[a-zA-Z0-9.-]+.pinggy-free.link', line)
            if match:
                url = match.group(0)
                print(f"✨ DISCOVERED URL: {url}")
                break
        time.sleep(0.2)
    
    if url:
        # Update .env
        env_path = '/Users/noone/.gemini/antigravity/scratch/roof_hunter/.env'
        with open(env_path, 'r') as f:
            lines = f.readlines()
        
        with open(env_path, 'w') as f:
            found = False
            for line in lines:
                if line.startswith('PUBLIC_TUNNEL_URL='):
                    f.write(f'PUBLIC_TUNNEL_URL={url}\n')
                    found = True
                else:
                    f.write(line)
            if not found:
                f.write(f'PUBLIC_TUNNEL_URL={url}\n')
        print(f"✅ UPDATED .env WITH {url}")
        
        # Now Update Telnyx
        # We can't really call another script easily while keeping p alive, 
        # so we'll just shell out
        from sync_telnyx import sync_telnyx_webhook
        sync_telnyx_webhook(url)
        
        # Keep process alive
        print("🟢 UPLINK STABLE. KEEPING PROCESS ALIVE...")
        while True:
            # Check if process still alive
            if p.poll() is not None:
                print("❌ PINGGY CRASHED. EXITING.")
                break
            time.sleep(60)
    else:
        print("❌ FAILED TO DISCOVER PINGGY URL.")
        p.terminate()

if __name__ == "__main__":
    start_pinggy_and_sync()
