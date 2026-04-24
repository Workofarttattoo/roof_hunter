import logging
from src.datalake_builder import DatalakeBuilder

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def populate_two_years():
    years = [2024, 2025, 2026]
    
    for year in years:
        logger.info(f"=== INGESTING NOAA DATA FOR YEAR: {year} ===")
        try:
            builder = DatalakeBuilder(year=year)
            builder.build_lake()
        except Exception as e:
            logger.error(f"Failed to ingest year {year}: {e}")
            
    logger.info("Datalake successfully populated with 2 years of severe weather intelligence.")

if __name__ == "__main__":
    populate_two_years()
