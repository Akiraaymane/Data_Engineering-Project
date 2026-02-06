import pandas as pd
import os
import logging
from src import config

logger = logging.getLogger(__name__)

def run():
    """Calcule les KPIs agrégés à partir des données transformées."""
    logger.info("Démarrage de la génération des KPIs...")
    
    if not os.path.exists(config.APPS_REVIEWS_CSV):
        logger.error(f"Fichier {config.APPS_REVIEWS_CSV} introuvable. Lancez la transformation d'abord.")
        return

    df_reviews = pd.read_csv(config.APPS_REVIEWS_CSV)
    
    # Agrégation par Application
    # KPIs: 
    # - Score moyen
    # - Nombre total d'avis
    # - Nombre de contradictions
    # - Ratio de sentiment POS/NEG
    
    kpis = df_reviews.groupby(['app_id', 'app_name']).agg({
        'score': 'mean',
        'reviewId': 'count',
        'contradiction_flag': 'sum',
        'thumbsUpCount': 'sum'
    }).rename(columns={
        'score': 'avg_sentiment_score',
        'reviewId': 'total_reviews',
        'contradiction_flag': 'contradiction_count',
        'thumbsUpCount': 'total_likes'
    }).reset_index()
    
    # Calcul du % de contradictions
    kpis['contradiction_rate'] = (kpis['contradiction_count'] / kpis['total_reviews']) * 100
    
    # Distribution du sentiment par app
    sentiment_dist = df_reviews.groupby(['app_id', 'sentiment_hint']).size().unstack(fill_value=0)
    sentiment_dist.columns = [f'count_{c.lower()}' for c in sentiment_dist.columns]
    
    kpis = kpis.merge(sentiment_dist, on='app_id', how='left')
    
    # Sauvegarde
    os.makedirs(config.PROCESSED_DIR, exist_ok=True)
    kpis.to_csv(config.APP_KPIS_CSV, index=False)
    
    logger.info(f"KPIs générés : {len(kpis)} applications traitées. Sauvegardés dans {config.APP_KPIS_CSV}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
