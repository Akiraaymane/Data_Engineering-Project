"""Script principal pour exécuter le pipeline complet"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import settings
from src.ingestion import PlayStoreIngestion
from src.transformation import DataTransformer
from src.serving import ServingLayer
from src.dashboard import SimpleDashboard


def run_ingestion():
    ingestion = PlayStoreIngestion(settings.RAW_DATA_DIR)
    
    num_apps, num_reviews = ingestion.run_ingestion(
        keywords=settings.SEARCH_KEYWORDS,
        max_reviews=settings.MAX_REVIEWS_PER_APP
    )
    
    return num_apps, num_reviews


def run_transformation():
    transformer = DataTransformer(
        raw_data_dir=settings.RAW_DATA_DIR,
        processed_data_dir=settings.PROCESSED_DATA_DIR
    )
    
    num_apps, num_reviews = transformer.run_transformation()
    
    return num_apps, num_reviews


def run_serving():
    serving = ServingLayer(settings.PROCESSED_DATA_DIR)
    
    num_apps, num_days = serving.run_serving_generation()
    
    return num_apps, num_days


def run_dashboard():
    dashboard = SimpleDashboard(settings.PROCESSED_DATA_DIR)
    
    output_path = dashboard.save_dashboard()
    
    return output_path


def main():
    
    num_apps_raw, num_reviews_raw = run_ingestion()
    
    num_apps_clean, num_reviews_clean = run_transformation()
    
    num_apps_kpi, num_days_metrics = run_serving()
    
    dashboard_path = run_dashboard()
    
    print("\n" + "="*60)
    print("PIPELINE EXÉCUTÉ AVEC SUCCÈS")
    print("="*60)
    print(f"\n📥 INGESTION:")
    print(f"   • Applications collectées: {num_apps_raw}")
    print(f"   • Reviews collectées: {num_reviews_raw}")
    print(f"\n🔄 TRANSFORMATION:")
    print(f"   • Applications nettoyées: {num_apps_clean}")
    print(f"   • Reviews nettoyées: {num_reviews_clean}")
    print(f"\n📊 SERVING LAYER:")
    print(f"   • Applications avec KPIs: {num_apps_kpi}")
    print(f"   • Jours de métriques: {num_days_metrics}")
    print(f"\n📈 DASHBOARD:")
    print(f"   • Fichier généré: {dashboard_path}")
    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    main()
