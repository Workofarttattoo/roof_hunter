import time
import subprocess
import logging
from datetime import datetime
import pytz

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_forensic_scheduler():
    """
    Autonomous Scheduler for Voice Dispatch.
    Window: 10:00 AM - 8:00 PM (CST/Central Time - targeting the Alley).
    """
    # Central Time is the primary time zone for TX/OK/KS/NE
    timezone = pytz.timezone('US/Central')
    
    logger.info("🕒 MASTER FORENSIC SCHEDULER ACTIVE")
    logger.info("Targeting Window: 10:00 AM - 8:00 PM CST")
    
    while True:
        now = datetime.now(timezone)
        current_hour = now.hour
        
        # Check if we are inside the 10am - 8pm window
        if 10 <= current_hour < 20:
            logger.info(f"✅ WITHIN OPERATIONAL WINDOW ({now.strftime('%I:%M %p %Z')}). Launching Dispatcher...")
            
            # Start the dispatcher as a subprocess
            # We run it in a loop or just once and let it finish its batch
            try:
                subprocess.run(["venv/bin/python", "src/telnyx_voice_agent.py"])
            except Exception as e:
                logger.error(f"Dispatcher Launch Error: {e}")
            
            logger.info("Batch complete. Waiting 1 hour for lead manifest refresh...")
            time.sleep(3600)
        else:
            logger.info(f"💤 OUTSIDE WINDOW ({now.strftime('%I:%M %p %Z')}). Next sweep at 10:00 AM.")
            # Sleep for 15 minutes before checking again
            time.sleep(900)

if __name__ == "__main__":
    run_forensic_scheduler()
