import json
import logging
import os
from google_play_scraper import reviews, Sort
from src import config

logger = logging.getLogger(__name__)

def run(output_file=None, count=100):
    """Récupère les avis des applications et les sauvegarde en JSONL."""
    if output_file is None:
        output_file = config.REVIEWS_INPUT
        
    output_path = os.path.join(config.RAW_DIR, output_file)
    os.makedirs(config.RAW_DIR, exist_ok=True)
    
    logger.info(f"Début de l'ingestion des reviews vers {output_path}...")
    
    total_reviews = 0
    with open(output_path, 'w', encoding='utf-8') as f:
        for app_id in config.APP_IDS:
            try:
                logger.info(f"Scraping reviews pour {app_id}...")
                result, _ = reviews(
                    app_id,
                    lang='en',
                    country='us',
                    sort=Sort.MOST_RELEVANT,
                    count=count
                )
                
                for review in result:
                    review['app_id'] = app_id
                    f.write(json.dumps(review, ensure_ascii=False, default=str) + '\n')
                    total_reviews += 1
                    
            except Exception as e:
                logger.error(f"Erreur lors du scraping des reviews de {app_id}: {e}")
                
    logger.info(f"Ingestion des reviews terminée. {total_reviews} avis récupérés.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
