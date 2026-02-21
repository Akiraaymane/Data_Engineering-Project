"""
Script de création automatique de la structure complète du projet Lab1 Data Pipeline
Exécuter: python template.py
"""
import os
from pathlib import Path


def create_file(filepath, content):
    """Crée un fichier avec son contenu"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


def create_project():
    base = Path("lab1_data_pipeline")
    
    # Structure des dossiers
    (base / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (base / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (base / "src" / "ingestion").mkdir(parents=True, exist_ok=True)
    (base / "src" / "transformation").mkdir(parents=True, exist_ok=True)
    (base / "src" / "serving").mkdir(parents=True, exist_ok=True)
    (base / "src" / "dashboard").mkdir(parents=True, exist_ok=True)
    (base / "src" / "utils").mkdir(parents=True, exist_ok=True)
    (base / "config").mkdir(parents=True, exist_ok=True)
    
    # .gitignore
    create_file(base / ".gitignore", """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Data
data/raw/*.json
data/raw/*.jsonl
data/raw/*.csv
data/processed/*.csv
*.html

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db
""")
    
    # README.md
    create_file(base / "README.md", """# Lab1 - Python Data Pipeline

Pipeline de données Python pour l'analyse d'applications de prise de notes AI.

## Structure du projet

```
lab1_data_pipeline/
├── data/
│   ├── raw/              # Données brutes (JSON/JSONL)
│   └── processed/        # Données transformées (CSV)
├── src/
│   ├── ingestion/        # Extraction des données
│   ├── transformation/   # Nettoyage et transformation
│   ├── serving/          # Couche de serving (KPIs)
│   └── dashboard/        # Visualisation
├── config/               # Configuration
└── main.py              # Script principal
```

## Installation

1. Créer un environnement virtuel:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\\Scripts\\activate  # Windows
```

2. Installer les dépendances:
```bash
pip install -r requirements.txt
```

## Utilisation

Exécuter le pipeline complet:
```bash
python main.py
```

Le pipeline exécute les étapes suivantes:
1. **Ingestion**: Extraction depuis Google Play Store
2. **Transformation**: Nettoyage et structuration
3. **Serving**: Génération des KPIs
4. **Dashboard**: Création des visualisations

## Outputs

- `data/processed/apps_catalog.csv` - Catalogue des applications
- `data/processed/apps_reviews.csv` - Reviews nettoyées
- `data/processed/app_level_kpis.csv` - KPIs par application
- `data/processed/daily_metrics.csv` - Métriques quotidiennes
- `dashboard.html` - Dashboard interactif
""")
    
    # requirements.txt
    create_file(base / "requirements.txt", """google-play-scraper==1.2.4
pandas==2.1.4
plotly==5.18.0
python-dateutil==2.8.2
""")
    
    # config/settings.py
    create_file(base / "config" / "settings.py", '''"""Configuration centrale du pipeline"""
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
''')
    
    # config/__init__.py
    create_file(base / "config" / "__init__.py", "from .settings import *\n")
    
    # src/__init__.py
    create_file(base / "src" / "__init__.py", "")
    
    # src/ingestion/play_store_scraper.py
    create_file(base / "src" / "ingestion" / "play_store_scraper.py", '''"""Module d'ingestion des données depuis Google Play Store"""
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
                f.write(json.dumps(review, ensure_ascii=False) + '\\n')
    
    def run_ingestion(self, keywords: List[str], max_reviews: int = 500):
        search_results = self.search_apps(keywords)
        app_ids = [app['appId'] for app in search_results]
        
        apps_data = self.fetch_app_details(app_ids)
        self.save_apps_data(apps_data)
        
        reviews_data = self.fetch_reviews(app_ids, max_reviews)
        self.save_reviews_data(reviews_data)
        
        return len(apps_data), len(reviews_data)
''')
    
    # src/ingestion/__init__.py
    create_file(base / "src" / "ingestion" / "__init__.py", "from .play_store_scraper import PlayStoreIngestion\n")
    
    # src/transformation/data_transformer.py
    create_file(base / "src" / "transformation" / "data_transformer.py", '''"""Module de transformation et nettoyage des données"""
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
''')
    
    # src/transformation/__init__.py
    create_file(base / "src" / "transformation" / "__init__.py", "from .data_transformer import DataTransformer\n")
    
    # src/serving/serving_layer.py
    create_file(base / "src" / "serving" / "serving_layer.py", '''"""Module de préparation des données pour la couche de serving"""
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
''')
    
    # src/serving/__init__.py
    create_file(base / "src" / "serving" / "__init__.py", "from .serving_layer import ServingLayer\n")
    
    # src/dashboard/visualizer.py
    create_file(base / "src" / "dashboard" / "visualizer.py", '''"""Module de génération de dashboard simple"""
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path


class SimpleDashboard:
    
    def __init__(self, processed_data_dir: Path):
        self.processed_data_dir = processed_data_dir
    
    def load_app_kpis(self) -> pd.DataFrame:
        filepath = self.processed_data_dir / "app_level_kpis.csv"
        return pd.read_csv(filepath)
    
    def load_daily_metrics(self) -> pd.DataFrame:
        filepath = self.processed_data_dir / "daily_metrics.csv"
        df = pd.read_csv(filepath, parse_dates=['date'])
        return df
    
    def create_dashboard(self):
        app_kpis = self.load_app_kpis()
        daily_metrics = self.load_daily_metrics()
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Top 10 Applications par Nombre de Reviews',
                'Top 10 Applications par Rating Moyen',
                'Évolution du Rating Moyen Quotidien',
                'Volume de Reviews Quotidien'
            ),
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "scatter"}, {"type": "scatter"}]]
        )
        
        top_by_reviews = app_kpis.nlargest(10, 'num_reviews')
        fig.add_trace(
            go.Bar(
                x=top_by_reviews['app_name'],
                y=top_by_reviews['num_reviews'],
                name='Nombre de reviews',
                marker_color='lightblue'
            ),
            row=1, col=1
        )
        
        top_by_rating = app_kpis.nlargest(10, 'avg_rating')
        fig.add_trace(
            go.Bar(
                x=top_by_rating['app_name'],
                y=top_by_rating['avg_rating'],
                name='Rating moyen',
                marker_color='lightgreen'
            ),
            row=1, col=2
        )
        
        fig.add_trace(
            go.Scatter(
                x=daily_metrics['date'],
                y=daily_metrics['daily_avg_rating'],
                mode='lines+markers',
                name='Rating quotidien',
                line=dict(color='orange')
            ),
            row=2, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=daily_metrics['date'],
                y=daily_metrics['daily_num_reviews'],
                mode='lines+markers',
                name='Volume quotidien',
                line=dict(color='purple')
            ),
            row=2, col=2
        )
        
        fig.update_xaxes(tickangle=-45, row=1, col=1)
        fig.update_xaxes(tickangle=-45, row=1, col=2)
        
        fig.update_layout(
            height=900,
            showlegend=False,
            title_text="Dashboard Analytics - AI Note-Taking Apps",
            title_x=0.5
        )
        
        return fig
    
    def save_dashboard(self, output_path: Path = None):
        if output_path is None:
            output_path = self.processed_data_dir.parent / "dashboard.html"
        
        fig = self.create_dashboard()
        fig.write_html(str(output_path))
        
        return output_path
''')
    
    # src/dashboard/__init__.py
    create_file(base / "src" / "dashboard" / "__init__.py", "from .visualizer import SimpleDashboard\n")
    
    # src/utils/__init__.py
    create_file(base / "src" / "utils" / "__init__.py", "")
    
    # main.py
    create_file(base / "main.py", '''"""Script principal pour exécuter le pipeline complet"""
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
    
    print("\\n" + "="*60)
    print("PIPELINE EXÉCUTÉ AVEC SUCCÈS")
    print("="*60)
    print(f"\\n📥 INGESTION:")
    print(f"   • Applications collectées: {num_apps_raw}")
    print(f"   • Reviews collectées: {num_reviews_raw}")
    print(f"\\n🔄 TRANSFORMATION:")
    print(f"   • Applications nettoyées: {num_apps_clean}")
    print(f"   • Reviews nettoyées: {num_reviews_clean}")
    print(f"\\n📊 SERVING LAYER:")
    print(f"   • Applications avec KPIs: {num_apps_kpi}")
    print(f"   • Jours de métriques: {num_days_metrics}")
    print(f"\\n📈 DASHBOARD:")
    print(f"   • Fichier généré: {dashboard_path}")
    print("\\n" + "="*60 + "\\n")


if __name__ == "__main__":
    main()
''')
    
    print(f"✅ Projet créé avec succès dans: {base.absolute()}")
    print("\n📁 Structure créée:")
    print("""
lab1_data_pipeline/
├── .gitignore
├── README.md
├── requirements.txt
├── main.py
├── config/
│   ├── __init__.py
│   └── settings.py
├── data/
│   ├── raw/
│   └── processed/
└── src/
    ├── __init__.py
    ├── ingestion/
    │   ├── __init__.py
    │   └── play_store_scraper.py
    ├── transformation/
    │   ├── __init__.py
    │   └── data_transformer.py
    ├── serving/
    │   ├── __init__.py
    │   └── serving_layer.py
    ├── dashboard/
    │   ├── __init__.py
    │   └── visualizer.py
    └── utils/
        └── __init__.py
""")
    print("\n🚀 Pour démarrer:")
    print("1. cd lab1_data_pipeline")
    print("2. python -m venv venv")
    print("3. source venv/bin/activate  # ou venv\\Scripts\\activate sur Windows")
    print("4. pip install -r requirements.txt")
    print("5. python main.py")
    print("\n📦 Pour Git:")
    print("git init")
    print("git add .")
    print('git commit -m "Initial commit: Lab1 Data Pipeline template"')
    print("git branch -M main")
    print("git remote add origin <votre-repo-url>")
    print("git push -u origin main")


if __name__ == "__main__":
    create_project()
