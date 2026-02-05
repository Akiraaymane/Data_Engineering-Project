"""Module de préparation des données pour la couche de serving"""
import pandas as pd
from pathlib import Path


class ServingLayer:
    
    def __init__(self, processed_data_dir: Path):
        self.processed_data_dir = processed_data_dir
    
    def load_processed_reviews(self, filename: str = "apps_reviews.csv") -> pd.DataFrame:
        filepath = self.processed_data_dir / filename
        df = pd.read_csv(filepath, parse_dates=['at'])
        return df
    
    def generate_app_level_kpis(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        reviews_df['low_rating'] = (reviews_df['score'] <= 2).astype(int)
        
        kpis = reviews_df.groupby('app_id').agg(
            app_name=('app_name', 'first'),
            num_reviews=('reviewId', 'count'),
            avg_rating=('score', 'mean'),
            pct_low_rating=('low_rating', 'mean'),
            first_review_date=('at', 'min'),
            most_recent_review_date=('at', 'max')
        ).reset_index()
        
        kpis['pct_low_rating'] = (kpis['pct_low_rating'] * 100).round(2)
        kpis['avg_rating'] = kpis['avg_rating'].round(2)
        
        return kpis
    
    def generate_daily_metrics(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        reviews_df['date'] = reviews_df['at'].dt.date
        
        daily = reviews_df.groupby('date').agg(
            daily_num_reviews=('reviewId', 'count'),
            daily_avg_rating=('score', 'mean')
        ).reset_index()
        
        daily['daily_avg_rating'] = daily['daily_avg_rating'].round(2)
        daily = daily.sort_values('date')
        
        return daily
    
    def save_serving_data(self, df: pd.DataFrame, filename: str):
        filepath = self.processed_data_dir / filename
        df.to_csv(filepath, index=False)
    
    def run_serving_generation(self):
        reviews_df = self.load_processed_reviews()
        
        app_kpis = self.generate_app_level_kpis(reviews_df)
        self.save_serving_data(app_kpis, "app_level_kpis.csv")
        
        daily_metrics = self.generate_daily_metrics(reviews_df)
        self.save_serving_data(daily_metrics, "daily_metrics.csv")
        
        return len(app_kpis), len(daily_metrics)
