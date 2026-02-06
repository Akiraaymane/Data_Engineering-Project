import json
import logging
import os
from google_play_scraper import app
from src import config

logger = logging.getLogger(__name__)

def run(output_file=None):
    """Récupère les métadonnées des applications et les sauvegarde en JSON."""
    if output_file is None:
        output_file = config.APPS_INPUT
        
    output_path = os.path.join(config.RAW_DIR, output_file)
    os.makedirs(config.RAW_DIR, exist_ok=True)
    
    apps_data = []
    logger.info(f"Début de l'ingestion des apps vers {output_path}...")
    
    for app_id in config.APP_IDS:
        try:
            logger.info(f"Scraping metadata pour {app_id}...")
            result = app(app_id)
            apps_data.append(result)
        except Exception as e:
            logger.error(f"Erreur lors du scraping de {app_id}: {e}")
            
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(apps_data, f, ensure_ascii=False, indent=4, default=str)
        
    logger.info(f"Ingestion des apps terminée. {len(apps_data)} apps récupérées.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
