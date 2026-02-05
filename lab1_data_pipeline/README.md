# Lab1 - Python Data Pipeline

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
venv\Scripts\activate  # Windows
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
