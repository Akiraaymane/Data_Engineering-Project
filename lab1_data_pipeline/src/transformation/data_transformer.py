"""Module de transformation et nettoyage des données"""
import json
import pandas as pd
from pathlib import Path


class DataTransformer:
    
    def __init__(self, raw_data_dir: Path, processed_data_dir: Path):
        self.raw_data_dir = raw_data_dir
        self.processed_data_dir = processed_data_dir
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
    
    def load_raw_apps(self, filename: str = "apps_raw.json") -> pd.DataFrame:
        filepath = self.raw_data_dir / filename
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return pd.DataFrame(data)
    
    def load_raw_reviews(self, filename: str = "reviews_raw.jsonl") -> pd.DataFrame:
        filepath = self.raw_data_dir / filename
        reviews = []
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                reviews.append(json.loads(line))
        return pd.DataFrame(reviews)
    
    def transform_apps(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        columns_mapping = {
            'appId': 'appId',
            'title': 'title',
            'developer': 'developer',
            'score': 'score',
            'ratings': 'ratings',
            'installs': 'installs',
            'genre': 'genre',
            'price': 'price'
        }
        
        df = df[list(columns_mapping.keys())].rename(columns=columns_mapping)
        
        df['score'] = pd.to_numeric(df['score'], errors='coerce')
        df['ratings'] = pd.to_numeric(df['ratings'], errors='coerce')
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        
        df['installs'] = df['installs'].astype(str).str.replace(r'[+,]', '', regex=True)
        df['installs'] = pd.to_numeric(df['installs'], errors='coerce')
        
        df = df.dropna(subset=['appId', 'title'])
        df = df.drop_duplicates(subset=['appId'], keep='first')
        
        return df
    
    def transform_reviews(self, df: pd.DataFrame, apps_df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        columns_mapping = {
            'app_id': 'app_id',
            'reviewId': 'reviewId',
            'userName': 'userName',
            'score': 'score',
            'content': 'content',
            'thumbsUpCount': 'thumbsUpCount',
            'at': 'at'
        }
        
        df = df[list(columns_mapping.keys())].rename(columns=columns_mapping)
        
        apps_names = apps_df.set_index('appId')['title'].to_dict()
        df['app_name'] = df['app_id'].map(apps_names)
        
        df['score'] = pd.to_numeric(df['score'], errors='coerce')
        df['thumbsUpCount'] = pd.to_numeric(df['thumbsUpCount'], errors='coerce').fillna(0)
        
        df['at'] = pd.to_datetime(df['at'], errors='coerce')
        
        df = df.dropna(subset=['reviewId', 'app_id', 'score'])
        df = df.drop_duplicates(subset=['reviewId'], keep='first')
        
        df = df[df['app_id'].isin(apps_df['appId'])]
        
        df = df[['app_id', 'app_name', 'reviewId', 'userName', 'score', 'content', 'thumbsUpCount', 'at']]
        
        return df
    
    def save_processed_data(self, df: pd.DataFrame, filename: str):
        filepath = self.processed_data_dir / filename
        df.to_csv(filepath, index=False, encoding='utf-8')
    
    def run_transformation(self, apps_filename: str = "apps_raw.json", 
                          reviews_filename: str = "reviews_raw.jsonl"):
        apps_df = self.load_raw_apps(apps_filename)
        apps_transformed = self.transform_apps(apps_df)
        self.save_processed_data(apps_transformed, "apps_catalog.csv")
        
        reviews_df = self.load_raw_reviews(reviews_filename)
        reviews_transformed = self.transform_reviews(reviews_df, apps_transformed)
        self.save_processed_data(reviews_transformed, "apps_reviews.csv")
        
        return len(apps_transformed), len(reviews_transformed)
