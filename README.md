# Lab 1: Python Data Pipeline

## Overview
End-to-end Python pipeline that ingests Google Play data, transforms it, and produces serving-layer aggregates and a dashboard.

## Setup
1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
Run the entire pipeline:
```bash
python -m src.main
```

## Dashboard
The dashboard module (`src/dashboard.py`) generates static visualizations in `data/processed/`:
- `dashboard_daily_volume.png`: Time series of daily review counts.
- `dashboard_daily_rating.png`: Time series of daily average ratings.
- `dashboard_score_dist.png`: Histogram of review score distribution.
- `dashboard_app_ranking.png`: Horizontal bar chart ranking apps by average rating (best/worst).

The dashboard reads **only** from processed outputs (`data/processed/`), ensuring a clear separation between pipeline logic and analytics views.

## Stress Testing
To stress test the pipeline with provided CSV datasets, use the `--reviews_input` and `--apps_input` arguments.

Examples:
```bash
# Batch 2
python -m src.main --reviews_input "data/raw/note_taking_ai_reviews_batch2.csv"

# Schema Drift
python -m src.main --reviews_input "data/raw/note_taking_ai_reviews_schema_drift.csv"

# Dirty Data
python -m src.main --reviews_input "data/raw/note_taking_ai_reviews_dirty.csv"

# Updated Apps Metadata
python -m src.main --reviews_input "data/raw/note_taking_ai_reviews_batch2.csv" --apps_input "data/raw/note_taking_ai_apps_updated.csv"
```

### Stress Test Notes
- **Schema Drift**: The pipeline includes a mapping layer in `transform.py` to handle mismatched column names (e.g., `id` vs `reviewId`).
- **Dirty Data**: Type normalization handles non-numeric scores and malformed dates (coerced to NaN/Nat).

