"""
daily_sweep.py
--------------
Automated 30-day forensic sweep for major hail events (>= 1.5").
Designed for AWS deployment (CRON/Lambda).

Logic:
  1. Pulls historical SPC reports for the last 30 days.
  2. Filters for hail magnitude >= 1.5".
  3. Deduplicates against authoritative_storms.db.
  4. Triggers enrichment (Census/Valuation) via live_hail_ingest mechanisms.

Usage:
  python3 src/daily_sweep.py --min-hail 1.5 --days 30
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta

def run_sweep(min_hail=1.5, days=30):
    print(f"=======================================================")
    print(f"  ECH0-ROOF AWS DAILY SWEEP ENGINE")
    print(f"  Target: Hail >= {min_hail}\" | Window: {days} Days")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"=======================================================")

    # Path to the live ingestion script
    ingest_script = os.path.join(os.path.dirname(__file__), "live_hail_ingest.py")
    
    # Construct the command
    # We use the existing robust SPC fetcher in live_hail_ingest.py
    cmd = [
        sys.executable, ingest_script,
        "--days", str(days),
        "--min", str(min_hail)
    ]

    try:
        print(f"\n[RUNNING] { ' '.join(cmd) }")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        print(f"\n[SUCCESS] Sweep completed at {datetime.now().strftime('%H:%M:%S')}")
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Sweep failed:")
        print(e.stderr)
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ECH0-ROOF Daily Sweep")
    parser.add_argument("--min-hail", type=float, default=1.5, help="Minimum hail size")
    parser.add_argument("--days", type=int, default=30, help="Days to look back")
    
    args = parser.parse_args()
    run_sweep(min_hail=args.min_hail, days=args.days)
