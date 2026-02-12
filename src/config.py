import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# Default File Names
APPS_FILENAME = "apps_raw.json"
REVIEWS_FILENAME = "reviews_raw.jsonl"

# Stress Test Defaults (can be overridden)
STRESS_TEST_FILES = {
    "batch2": "note_taking_ai_reviews_batch2.csv",
    "schema_drift": "note_taking_ai_reviews_schema_drift.csv",
    "dirty": "note_taking_ai_reviews_dirty.csv",
    "apps_updated": "note_taking_ai_apps_updated.csv"
}

# Target Package Name (for scraping)
# Using a popular productivity app as a placeholder or the one implied by the stress test files
# The stress test filenames "note_taking_ai..." suggest a specific app or category.
# I will use a generic variable here that can be changed.
TARGET_APP_ID = "com.google.android.keep" # Example: Google Keep, can be changed.

# Output Data Schemas (Target Column Names)
CATALOG_COLS = ["appId", "title", "developer", "score", "ratings", "installs", "genre", "price"]
REVIEWS_COLS = ["app_id", "app_name", "reviewId", "userName", "score", "content", "thumbsUpCount", "at"]
