"""Module d'ingestion des données depuis Google Play Store"""
import json
from pathlib import Path
from typing import List, Dict, Any
from google_play_scraper import app, search, reviews_all


class PlayStoreIngestion:
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def search_apps(self, keywords: List[str]) -> List[Dict[str, Any]]:
        all_apps = []
        seen_app_ids = set()
        
        for keyword in keywords:
            results = search(keyword, n_hits=30)
            for result in results:
                app_id = result.get('appId')
                if app_id and app_id not in seen_app_ids:
                    seen_app_ids.add(app_id)
                    all_apps.append({'appId': app_id})
        
        return all_apps
    
    def fetch_app_details(self, app_ids: List[str]) -> List[Dict[str, Any]]:
        apps_data = []
        
        for app_id in app_ids:
            try:
                app_data = app(app_id, lang='en', country='us')
                apps_data.append(app_data)
            except Exception:
                continue
        
        return apps_data
    
    def fetch_reviews(self, app_ids: List[str], max_reviews: int = 500) -> List[Dict[str, Any]]:
        all_reviews = []
        
        for app_id in app_ids:
            try:
                app_reviews, _ = reviews_all(
                    app_id,
                    lang='en',
                    country='us',
                    count=max_reviews
                )
                
                for review in app_reviews:
                    review['app_id'] = app_id
                    all_reviews.append(review)
                    
            except Exception:
                continue
        
        return all_reviews
    
    def save_apps_data(self, apps: List[Dict[str, Any]], filename: str = "apps_raw.json"):
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(apps, f, indent=2, ensure_ascii=False)
    
    def save_reviews_data(self, reviews: List[Dict[str, Any]], filename: str = "reviews_raw.jsonl"):
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            for review in reviews:
                f.write(json.dumps(review, ensure_ascii=False) + '\n')
    
    def run_ingestion(self, keywords: List[str], max_reviews: int = 500):
        search_results = self.search_apps(keywords)
        app_ids = [app['appId'] for app in search_results]
        
        apps_data = self.fetch_app_details(app_ids)
        self.save_apps_data(apps_data)
        
        reviews_data = self.fetch_reviews(app_ids, max_reviews)
        self.save_reviews_data(reviews_data)
        
        return len(apps_data), len(reviews_data)
