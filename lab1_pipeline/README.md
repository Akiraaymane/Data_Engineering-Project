# Lab 1 - Python Data Pipeline

Ce projet implémente un pipeline de données complet pour collecter, transformer et analyser les données d'applications de prise de notes AI depuis le Google Play Store.

## Structure du projet

- `data/raw/` : Données brutes (JSON/JSONL).
- `data/processed/` : Données transformées et tables finales (CSV).
- `src/` : Code source du pipeline.
- `main.py` : Point d'entrée du pipeline.

## Installation

1. Créer un environnement virtuel :
   ```bash
   python -m venv venv
   source venv/bin/activate  # Sur Windows: venv\Scripts\activate
   ```

2. Installer les dépendances :
   ```bash
   pip install -r requirements.txt
   ```

## Utilisation

Pour lancer le pipeline complet :
```bash
python -m src.main
```

Pour lancer les tests de robustesse (stress tests) :
```bash
python -m src.main --reviews_input data/raw/note_taking_ai_reviews_dirty.csv
```

## Stress Test Notes

- **Batch 2** : Chargement de nouvelles données sans changement de schéma.
- **Schema Drift** : Gestion de colonnes renommées ou manquantes.
- **Dirty Data** : Nettoyage des valeurs aberrantes et malformées.
- **Sentiment Contradiction** : Identification des cas où la note ne correspond pas au texte.

## Auteurs
Mostapha EL ANSARI
Aymane DHIMEN 
