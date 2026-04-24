import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consolidate_manifests():
    logger.info("=== CONSOLIDATING NATIONAL FORENSIC MANIFESTS ===")
    
    files = [
        'april_verified_spectral_leads.csv',
        'march_april_forensic_manifest.csv',
        'texas_platinum_contacts.csv',
        'alpha_leads_contacts.csv'
    ]
    
    all_dfs = []
    
    for f in files:
        if os.path.exists(f):
            logger.info(f"Merging {f}...")
            df = pd.read_csv(f)
            # Tag the source for tracking
            df['Source_Campaign'] = f.replace('.csv', '')
            all_dfs.append(df)
        else:
            logger.warning(f"File {f} not found, skipping.")

    if not all_dfs:
        logger.error("No manifests found to consolidate.")
        return

    # Master Concatenation
    master_df = pd.concat(all_dfs, ignore_index=True)
    
    # Deduplicate by City/Date (Rough proxy for unique property hits)
    initial_count = len(master_df)
    master_df.drop_duplicates(subset=['City', 'Date', 'State'], inplace=True)
    final_count = len(master_df)
    
    master_output = "ROOF_HUNTER_MASTER_MANIFEST_2026.csv"
    master_df.to_csv(master_output, index=False)
    
    print("\n" + "="*120)
    print("MASTER FORENSIC MANIFEST CONSOLIDATED")
    print("="*120)
    print(f"Total Raw Leads:      {initial_count}")
    print(f"Unique Verified Hits: {final_count}")
    print(f"Master Export:        {master_output}")
    print("\nTop Property Tiering:")
    print(master_df.sort_values(by='Valuation', ascending=False).head(20)[['City', 'State', 'Intensity', 'Valuation', 'Owner']].to_markdown(index=False))
    print("="*120)

if __name__ == "__main__":
    consolidate_manifests()
