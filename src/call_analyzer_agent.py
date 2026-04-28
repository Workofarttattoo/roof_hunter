import sqlite3
import json
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'leads_manifests/authoritative_storms.db'

class SalesCallAnalyzer:
    """
    Analyzes call telemetry to identify wins (appointments/affirmations) 
    and losses (hangups/declines).
    """
    def __init__(self):
        pass

    def run_group_performance_audit(self):
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # Analyze performance by voice_group extracted from payload
        # Note: client_state is base64 encoded, but we can also log a flat 'voice_group' 
        # in the future. For now, we'll try to find any event with a reachable voice_group.
        
        query = """
        SELECT lead_id, event_type, payload
        FROM call_telemetry
        ORDER BY lead_id, timestamp
        """
        c.execute(query)
        rows = c.fetchall()
        
        stats = {
            "RACHEL_A": {"wins": 0, "losses": 0, "total": 0},
            "JOSH_B_STD": {"wins": 0, "losses": 0, "total": 0},
            "CHARLIE_C_COUNTY": {"wins": 0, "losses": 0, "total": 0}
        }
        
        lead_map = {} # lead_id -> {'group': G, 'answered': bool, 'hungup': bool}

        for row in rows:
            lid = row['lead_id']
            if not lid: continue
            
            if lid not in lead_map:
                lead_map[lid] = {"group": "UNKNOWN", "answered": False, "hungup": False}
            
            # Extract group from payload
            payload = json.loads(row['payload'])
            cs = payload.get('client_state')
            if cs and lead_map[lid]["group"] == "UNKNOWN":
                try:
                    import base64
                    meta = json.loads(base64.b64decode(cs).decode())
                    lead_map[lid]["group"] = meta.get('voice_group', 'UNKNOWN')
                except: pass
            
            if row['event_type'] == 'call.answered':
                lead_map[lid]["answered"] = True
            if row['event_type'] == 'call.hangup':
                lead_map[lid]["hungup"] = True

        # Aggregate stats
        for lid, info in lead_map.items():
            g = info["group"]
            if g in stats:
                stats[g]["total"] += 1
                if info["answered"]:
                    if info["hungup"]:
                        stats[g]["losses"] += 1
                    else:
                        stats[g]["wins"] += 1
        
        logger.info("📊 CROSS-VARIANT PERFORMANCE AUDIT:")
        for g, s in stats.items():
            win_rate = (s['wins'] / s['total'] * 100) if s['total'] > 0 else 0
            logger.info(f"  > {g}: {win_rate:.1f}% Win Rate ({s['wins']}/{s['total']})")
        
        conn.close()
        return stats

if __name__ == "__main__":
    analyzer = SalesCallAnalyzer()
    report = analyzer.run_daily_analysis()
    print(f"--- Analyzed {len(report)} recent calls ---")
    for r in report:
        print(f"Lead {r['lead_id']}: {r['outcome']} ({r['reason']})")
