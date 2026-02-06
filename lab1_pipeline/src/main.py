import argparse
import logging
import os
import sys
from src import config
from src import ingest_apps, ingest_reviews, transform, serve, dashboard

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def run_pipeline(reviews_input=None, apps_input=None):
    """Exécute le pipeline de données de bout en bout."""
    logger.info("Démarrage du pipeline de données...")
    
    r_input = reviews_input or config.REVIEWS_INPUT
    a_input = apps_input or config.APPS_INPUT
    
    logger.info(f"Fichiers d'entrée : Apps={a_input}, Reviews={r_input}")
    
    # Étape 2: Ingestion
    logger.info("Étape 2 : Ingestion des données brutes...")
    if not reviews_input and not apps_input:
        ingest_apps.run(output_file=a_input)
        ingest_reviews.run(output_file=r_input)
    else:
        logger.info("Stress test détecté. Ingestion ignorée, utilisation des fichiers fournis.")
    
    # Étape 3: Transformation
    logger.info("Étape 3 : Transformation des données...")
    transform.run(reviews_input=r_input, apps_input=a_input)
    
    # Étape 4: Service
    logger.info("Étape 4 : Génération des KPIs...")
    serve.run()
    
    # Étape 5: Dashboard
    logger.info("Étape 5 : Création du dashboard...")
    dashboard.run()
    
    logger.info("Pipeline terminé.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de données Lab 1")
    parser.add_argument("--reviews_input", help="Fichier reviews brut (stress test)")
    parser.add_argument("--apps_input", help="Fichier apps brut (stress test)")
    
    args = parser.parse_args()
    run_pipeline(reviews_input=args.reviews_input, apps_input=args.apps_input)
