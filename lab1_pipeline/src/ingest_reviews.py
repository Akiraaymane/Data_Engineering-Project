import json
import logging
import os
from google_play_scraper import reviews, Sort
from src import config

logger = logging.getLogger(__name__)

def run(output_file=None, count=500):
    """Récupère les avis des applications avec pagination et sauvegarde en JSONL."""
    if output_file is None:
        output_file = config.REVIEWS_INPUT
        
    output_path = os.path.join(config.RAW_DIR, output_file)
    os.makedirs(config.RAW_DIR, exist_ok=True)
    
    # On vide le fichier au début pour un nouveau run
    with open(output_path, 'w', encoding='utf-8') as f:
        pass

    logger.info(f"Début de l'ingestion des reviews (max {count} par app) vers {output_path}...")
    
    total_reviews = 0
    for app_id in config.APP_IDS:
        try:
            logger.info(f"Scraping reviews pour {app_id}...")
            app_reviews = []
            continuation_token = None
            
            while len(app_reviews) < count:
                result, continuation_token = reviews(
                    app_id,
                    lang='en',
                    country='us',
                    sort=Sort.NEWEST, # Utiliser le plus récent pour mieux voir les nouveaux avis
                    count=min(100, count - len(app_reviews)),
                    continuation_token=continuation_token
                )
                
                if not result:
                    break
                    
                # Append to file immediately to prevent data loss
                with open(output_path, 'a', encoding='utf-8') as f:
                    for review in result:
                        review['app_id'] = app_id
                        f.write(json.dumps(review, ensure_ascii=False, default=str) + '\n')
                
                app_reviews.extend(result)
                total_reviews += len(result)
                
                logger.info(f"Récupéré {len(app_reviews)}/{count} avis pour {app_id}...")
                
                if not continuation_token:
                    break
                    
        except Exception as e:
            logger.error(f"Erreur lors du scraping des reviews de {app_id}: {e}")
                
    logger.info(f"Ingestion des reviews terminée. {total_reviews} avis récupérés.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
