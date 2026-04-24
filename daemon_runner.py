import schedule
import time
import logging
from main import run_agent_pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("WatcherDaemon")

def scheduled_job():
    logger.info("Initializing scheduled run...")
    try:
        run_agent_pipeline()
    except Exception as e:
        logger.error(f"Error during pipeline execution: {e}")
    logger.info("Sleeping until next schedule.")

def start_daemon():
    logger.info("Starting Roof Damage Auto-Watcher Daemon...")
    
    # Run immediately on startup
    scheduled_job()
    
    # Schedule to run every 4 hours. You can adjust this to minutes or days.
    # For a real-time feel, you might push this to every hour during storm season.
    schedule.every(4).hours.do(scheduled_job)
    
    while True:
        schedule.run_pending()
        time.sleep(60) # check every minute

if __name__ == "__main__":
    start_daemon()
