import pandas as pd
import logging
from src import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run():
    logger.info("Starting Serving Layer...")
    
    # Input paths
    reviews_path = config.PROCESSED_DIR / "apps_reviews.csv"
    
    if not reviews_path.exists():
        logger.error(f"Input file not found: {reviews_path}. Transformation step might have failed.")
        return

    # Load Data
    df = pd.read_csv(reviews_path)
    
    # Ensure date column is datetime (read_csv reads as object)
    if 'at' in df.columns:
        df['at'] = pd.to_datetime(df['at'])
    
    # --- 1. App-Level KPIs ---
    # We want specific metrics per app (though often we only have 1 app)
    # number_of_reviews, average_rating, pct_low_rating_reviews (<=2), first_review, most_recent_review
    
    # Helper for low rating pct
    def pct_low_rating(scores):
        if len(scores) == 0: return 0.0
        return (scores <= 2).mean() * 100

    kpis = df.groupby('app_id').agg(
        app_name=('app_name', 'first'), # Just take the first name found
        number_of_reviews=('reviewId', 'count'),
        average_rating=('score', 'mean'),
        pct_low_rating_reviews=('score', pct_low_rating),
        first_review_date=('at', 'min'),
        most_recent_review_date=('at', 'max')
    ).reset_index()
    
    kpis_out = config.PROCESSED_DIR / "app_kpis.csv"
    kpis.to_csv(kpis_out, index=False)
    logger.info(f"Saved app KPIs: {kpis_out}")
    
    # --- 2. Daily Metrics ---
    # date, daily_number_of_reviews, daily_average_rating
    
    # Group by Date (floor to day)
    if 'at' in df.columns:
        df['date'] = df['at'].dt.date
        
        daily_metrics = df.groupby('date').agg(
            daily_number_of_reviews=('reviewId', 'count'),
            daily_average_rating=('score', 'mean')
        ).sort_index().reset_index()
        
        daily_out = config.PROCESSED_DIR / "daily_metrics.csv"
        daily_metrics.to_csv(daily_out, index=False)
        logger.info(f"Saved daily metrics: {daily_out}")
    else:
        logger.warning("No 'at' column found, skipping daily metrics.")

if __name__ == "__main__":
    run()
