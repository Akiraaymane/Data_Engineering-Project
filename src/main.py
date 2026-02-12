import argparse
import sys
from src import config

def run_pipeline(reviews_input=None, apps_input=None):
    """
    Main pipeline orchestration.
    """
    print("--- Starting Pipeline ---")
    
    # --- Step 2: Ingestion ---
    # Determine input sources:
    # If paths are provided via CLI, use them (Stress Test Mode). 
    # If not, run scraping scripts and use their default outputs.
    
    apps_source = apps_input
    reviews_source = reviews_input
    
    if not apps_source:
        print("Scraping Mode: Fetching fresh app data...")
        from src import ingest_apps
        ingest_apps.run()
        apps_source = config.RAW_DIR / config.APPS_FILENAME
    else:
        print(f"Stress Test Mode: Using provided apps input: {apps_source}")

    if not reviews_source:
        print("Scraping Mode: Fetching fresh reviews...")
        from src import ingest_reviews
        ingest_reviews.run()
        reviews_source = config.RAW_DIR / config.REVIEWS_FILENAME
    else:
        print(f"Stress Test Mode: Using provided reviews input: {reviews_source}")
        
    # --- Step 3: Transformation ---
    print(f"Transforming data from:\n  Apps: {apps_source}\n  Reviews: {reviews_source}")
    from src import transform
    transform.run(apps_input=apps_source, reviews_input=reviews_source)

    # --- Step 4: Serving ---
    print("Generating Serving Layer outputs...")
    from src import serve
    serve.run()

    # --- Step 5: Dashboard ---
    print("Updating Dashboard...")
    from src import dashboard
    dashboard.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the end-to-end data pipeline")
    parser.add_argument("--reviews_input", help="Path to raw reviews file (overrides default scraping)")
    parser.add_argument("--apps_input", help="Path to raw apps file (overrides default scraping)")
    
    args = parser.parse_args()
    
    run_pipeline(reviews_input=args.reviews_input, apps_input=args.apps_input)
