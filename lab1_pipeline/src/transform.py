import json
import logging
import os
import pandas as pd
import numpy as np
from src import config

logger = logging.getLogger(__name__)

# --- CONFIGURATION DE LA ROBUSTESSE ---
# Mapping pour gérer la dérive de schéma (MCD amont)
MAPPING_SCHEMA = {
    'reviews': {
        'reviewId': ['reviewId', 'review_id', 'id'],
        'content': ['content', 'text', 'comment'],
        'score': ['score', 'rating', 'stars'],
        'at': ['at', 'date', 'timestamp', 'review_date'],
        'thumbsUpCount': ['thumbsUpCount', 'likes', 'vote_count'],
        'app_id': ['app_id', 'package_name', 'appId']
    },
    'apps': {
        'appId': ['appId', 'app_id', 'package_name'],
        'title': ['title', 'app_name', 'name'],
        'developer': ['developer', 'author', 'company'],
        'score': ['score', 'rating', 'avg_rating'],
        'installs': ['installs', 'download_count'],
        'genre': ['genre', 'category'],
        'price': ['price', 'cost']
    }
}

def normalize_schema(df, mapping):
    """Renomme les colonnes trouvées vers les noms attendus."""
    rename_dict = {}
    for expected, possibles in mapping.items():
        for p in possibles:
            if p in df.columns:
                rename_dict[p] = expected
                break
    return df.rename(columns=rename_dict)

def clean_installs(val):
    """Convertit '50,000,000+' en 50000000."""
    if isinstance(val, str):
        val = val.replace(',', '').replace('+', '')
        try:
            return float(val)
        except ValueError:
            return np.nan
    return val

def get_sentiment(text):
    """Heuristique simple pour le sentiment (Etape 7)."""
    if pd.isna(text): return 'NEUTRAL'
    text = str(text).lower()
    pos_keywords = ['great', 'best', 'love', 'excellent', 'good', 'amazing', 'perfect', 'helpful']
    neg_keywords = ['worst', 'bad', 'hate', 'terrible', 'disappointing', 'waste', 'crash', 'broken', 'issue', 'problem']
    
    pos_count = sum(1 for k in pos_keywords if k in text)
    neg_count = sum(1 for k in neg_keywords if k in text)
    
    if pos_count > neg_count: return 'POS'
    if neg_count > pos_count: return 'NEG'
    return 'NEUTRAL'

def run(reviews_input=None, apps_input=None):
    """Charge, transforme et sauvegarde les données traitées."""
    logger.info("Démarrage de la transformation...")
    
    raw_apps_path = os.path.join(config.RAW_DIR, apps_input or config.APPS_INPUT)
    raw_reviews_path = os.path.join(config.RAW_DIR, reviews_input or config.REVIEWS_INPUT)
    
    # 1. Chargement des Apps Metadata
    logger.info(f"Chargement des apps depuis {raw_apps_path}")
    if raw_apps_path.endswith('.json'):
        with open(raw_apps_path, 'r', encoding='utf-8') as f:
            apps_data = json.load(f)
        df_apps = pd.DataFrame(apps_data)
    else: # CSV for stress tests
        df_apps = pd.read_csv(raw_apps_path)
    
    df_apps = normalize_schema(df_apps, MAPPING_SCHEMA['apps'])
    
    # Transformation des colonnes Apps Catalog
    # Colonnes attendues: appId, title, developer, score, ratings, installs, genre, price
    expected_app_cols = ['appId', 'title', 'developer', 'score', 'ratings', 'installs', 'genre', 'price']
    df_apps_catalog = df_apps[[c for c in expected_app_cols if c in df_apps.columns]].copy()
    df_apps_catalog['installs'] = df_apps_catalog['installs'].apply(clean_installs)
    df_apps_catalog['score'] = pd.to_numeric(df_apps_catalog['score'], errors='coerce')
    
    # 2. Chargement des Reviews
    logger.info(f"Chargement des reviews depuis {raw_reviews_path}")
    if raw_reviews_path.endswith('.jsonl'):
        df_reviews = pd.read_json(raw_reviews_path, lines=True)
    else:
        df_reviews = pd.read_csv(raw_reviews_path)
        
    df_reviews = normalize_schema(df_reviews, MAPPING_SCHEMA['reviews'])
    
    # 3. Robustesse & Nettoyage Reviews
    # Conversion types
    df_reviews['score'] = pd.to_numeric(df_reviews['score'], errors='coerce')
    df_reviews['at'] = pd.to_datetime(df_reviews['at'], errors='coerce')
    
    # Dédoublonnage : Garder le plus récent par reviewId
    if 'reviewId' in df_reviews.columns:
        df_reviews = df_reviews.sort_values('at', ascending=False).drop_duplicates('reviewId').reset_index(drop=True)
    
    # 4. Jointure avec Apps pour le nom de l'application
    if 'app_id' in df_reviews.columns and 'appId' in df_apps.columns:
        df_reviews = df_reviews.merge(
            df_apps[['appId', 'title']].rename(columns={'title': 'app_name'}),
            left_on='app_id', right_on='appId', how='left'
        )
        df_reviews['app_name'] = df_reviews['app_name'].fillna('UNKNOWN')
    
    # 5. Logique métier - Sentiment & Contradiction (Etape 7)
    df_reviews['sentiment_hint'] = df_reviews['content'].apply(get_sentiment)
    
    # Contradiction flag: NEG text with score >= 4 OR POS text with score <= 2
    df_reviews['contradiction_flag'] = (
        ((df_reviews['sentiment_hint'] == 'NEG') & (df_reviews['score'] >= 4)) |
        ((df_reviews['sentiment_hint'] == 'POS') & (df_reviews['score'] <= 2))
    )
    
    # 6. Sauvegarde des Outputs
    os.makedirs(config.PROCESSED_DIR, exist_ok=True)
    
    # Colonnes attendues pour apps_reviews: app_id, app_name, reviewId, userName, score, content, thumbsUpCount, at
    reviews_final_cols = ['app_id', 'app_name', 'reviewId', 'userName', 'score', 'content', 'thumbsUpCount', 'at', 'sentiment_hint', 'contradiction_flag']
    df_reviews = df_reviews[[c for c in reviews_final_cols if c in df_reviews.columns]]
    
    df_apps_catalog.to_csv(config.APPS_CATALOG_CSV, index=False)
    df_reviews.to_csv(config.APPS_REVIEWS_CSV, index=False)
    
    logger.info(f"Transformation terminée. Catalog: {len(df_apps_catalog)} lignes, Reviews: {len(df_reviews)} lignes.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
