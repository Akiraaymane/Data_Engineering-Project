import json
import logging
from google_play_scraper import app
from src import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run(output_file=None):
    """
    Fetches raw app metadata and saves it as JSON.
    """
    if output_file is None:
        output_file = config.RAW_DIR / config.APPS_FILENAME
    
    app_id = config.TARGET_APP_ID
    logger.info(f"Fetching metadata for app: {app_id}")
    
    try:
        result = app(
            app_id,
            lang='en', # defaults to 'en'
            country='us' # defaults to 'us'
        )
        
        # Ensure directory exists
        config.RAW_DIR.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, default=str)
            
        logger.info(f"Saved app metadata to {output_file}")
        
    except Exception as e:
        logger.error(f"Failed to ingest app data: {e}")
        # In a real pipeline, we might raise e, but for this lab, logging is good.

if __name__ == "__main__":
    run()
