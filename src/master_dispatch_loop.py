import os
import subprocess
import sys
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Decoupled Pipeline: Each module now patrols the database independently
PHASES = [
    {"script": "aws_csv_worker.py", "interval": 10},      # Continuous Harvest
    {"script": "osint_contact_finder.py", "interval": 20}, # Zero-Fee Enrichment
    {"script": "skip_trace_qualified.py", "interval": 30}, # Baseline Trace
    {"script": "insurance_enricher.py", "interval": 60},  # Carrier Discovery
    {"script": "telnyx_voice_agent.py", "interval": 60}   # UNRESTRICTED NEURAL DISPATCH (Telnyx)
]

def monitor_pipeline():
    logger.info("🚀 ECH0-ROOF MASTER DISPATCH DAEMON: DECOUPLED MODE ACTIVE")
    
    # We will trigger the scripts as non-blocking background processes
    # This allows the Voice Agent (ElevenLabs) to call leads WHILE imagery is being harvested.
    
    processes = {}

    while True:
        for phase in PHASES:
            script = phase["script"]
            script_path = os.path.join(os.path.dirname(__file__), script)
            
            # Check if process is already running
            if script not in processes or processes[script].poll() is not None:
                logger.info(f"--- TRIGGERING PHASE: {script.upper()} ---")
                processes[script] = subprocess.Popen([sys.executable, script_path])
        
        # Patrol frequency
        time.sleep(10)

if __name__ == "__main__":
    try:
        monitor_pipeline()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user.")
