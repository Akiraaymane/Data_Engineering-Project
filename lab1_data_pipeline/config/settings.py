"""Configuration centrale du pipeline"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

RAW_APPS_FILE = RAW_DATA_DIR / "apps_raw.json"
RAW_REVIEWS_FILE = RAW_DATA_DIR / "reviews_raw.jsonl"

PROCESSED_APPS_FILE = PROCESSED_DATA_DIR / "apps_catalog.csv"
PROCESSED_REVIEWS_FILE = PROCESSED_DATA_DIR / "apps_reviews.csv"

APP_LEVEL_KPIS_FILE = PROCESSED_DATA_DIR / "app_level_kpis.csv"
DAILY_METRICS_FILE = PROCESSED_DATA_DIR / "daily_metrics.csv"

SEARCH_KEYWORDS = [
    "ai note taking",
    "ai notes",
    "smart notes ai",
    "artificial intelligence notes",
]

MAX_REVIEWS_PER_APP = 500
