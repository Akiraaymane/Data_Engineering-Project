import os

# Chemins des dossiers
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

# Fichiers d'entrée
APPS_INPUT = "apps_metadata.json"
REVIEWS_INPUT = "apps_reviews.jsonl"

# Fichiers de sortie
APPS_CATALOG_CSV = os.path.join(PROCESSED_DIR, "apps_catalog.csv")
APPS_REVIEWS_CSV = os.path.join(PROCESSED_DIR, "apps_reviews.csv")
APP_KPIS_CSV = os.path.join(PROCESSED_DIR, "app_kpis.csv")
DAILY_METRICS_CSV = os.path.join(PROCESSED_DIR, "daily_metrics.csv")

# Liste des applications à scraper (Note-taking AI)
APP_IDS = [
    "com.microsoft.office.officelens",
    "com.notes.ai.note",
    "com.vocal.ai.transcribe",
    "com.notewise.notewise",
    "com.zoho.notebook"
]
