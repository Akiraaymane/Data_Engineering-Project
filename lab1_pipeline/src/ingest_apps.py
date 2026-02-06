import json
import logging
import os
from google_play_scraper import app
from src import config

logger = logging.getLogger(__name__)

def run(output_file=None):
    """Récupère les métadonnées des applications et les sauvegarde en JSONL."""
    if output_file is None:
        output_file = config.APPS_INPUT
        
    output_path = os.path.join(config.RAW_DIR, output_file)
    os.makedirs(config.RAW_DIR, exist_ok=True)
    
    # Vide le fichier au début
    with open(output_path, 'w', encoding='utf-8') as f:
        pass

    logger.info(f"Début de l'ingestion des apps vers {output_path}...")
    
    count = 0
    for app_id in config.APP_IDS:
        try:
            logger.info(f"Scraping metadata pour {app_id}...")
            result = app(app_id)
            
            # Append immediately
            with open(output_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(result, ensure_ascii=False, default=str) + '\n')
            
            count += 1
        except Exception as e:
            logger.error(f"Erreur lors du scraping de {app_id}: {e}")
            
    logger.info(f"Ingestion des apps terminée. {count} apps récupérées.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
