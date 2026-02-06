import pandas as pd
import matplotlib.pyplot as plt
import os
import logging
from src import config

logger = logging.getLogger(__name__)

def run():
    """Génère des visualisations à partir des données transformées et des KPIs."""
    logger.info("Démarrage de la création du dashboard (Matplotlib)...")
    
    if not os.path.exists(config.APP_KPIS_CSV) or not os.path.exists(config.APPS_REVIEWS_CSV):
        logger.error("Fichiers de données manquants. Lancez le pipeline d'abord.")
        return

    df_kpis = pd.read_csv(config.APP_KPIS_CSV)
    df_reviews = pd.read_csv(config.APPS_REVIEWS_CSV)
    
    # 1. Bar Chart: Comparaison des scores moyens par App
    plt.figure(figsize=(10, 6))
    plt.bar(df_kpis['app_name'], df_kpis['avg_sentiment_score'], color='skyblue')
    plt.title('Score Moyen par Application')
    plt.xlabel('Application')
    plt.ylabel('Score Moyen')
    plt.ylim(0, 5)
    plt.tight_layout()
    plt.savefig(os.path.join(config.PROCESSED_DIR, "dashboard_scores.png"))
    plt.close()
    
    # 2. Pie Chart: Distribution globale du sentiment
    sentiment_counts = df_reviews['sentiment_hint'].value_counts()
    plt.figure(figsize=(8, 8))
    plt.pie(sentiment_counts, labels=sentiment_counts.index, autopct='%1.1f%%', colors=['green', 'gray', 'red'])
    plt.title('Distribution Globale du Sentiment')
    plt.tight_layout()
    plt.savefig(os.path.join(config.PROCESSED_DIR, "dashboard_sentiment.png"))
    plt.close()
    
    # 3. Scatter Plot: Score vs Likes
    plt.figure(figsize=(10, 6))
    for app in df_reviews['app_name'].unique():
        sub = df_reviews[df_reviews['app_name'] == app]
        plt.scatter(sub['score'], sub['thumbsUpCount'], label=app, alpha=0.5)
    plt.title('Score vs Nombre de Likes par Avis')
    plt.xlabel('Note')
    plt.ylabel('Likes')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(config.PROCESSED_DIR, "dashboard_scatter.png"))
    plt.close()
    
    # 4. Génération du fichier HTML
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>App Reviews Dashboard</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background-color: #f4f7f6; }}
            h1 {{ color: #333; text-align: center; }}
            .container {{ display: flex; flex-wrap: wrap; justify-content: center; gap: 20px; }}
            .card {{ background: white; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); padding: 15px; width: 45%; min-width: 400px; }}
            img {{ max-width: 100%; border-radius: 4px; }}
            .full-width {{ width: 92%; }}
        </style>
    </head>
    <body>
        <h1>Analyse des Avis Applications AI</h1>
        <div class="container">
            <div class="card">
                <h3>Scores Moyens</h3>
                <img src="dashboard_scores.png" alt="Average Scores">
            </div>
            <div class="card">
                <h3>Distribution du Sentiment</h3>
                <img src="dashboard_sentiment.png" alt="Sentiment Distribution">
            </div>
            <div class="card full-width">
                <h3>Avis : Score vs Likes</h3>
                <img src="dashboard_scatter.png" alt="Score vs Likes">
            </div>
        </div>
    </body>
    </html>
    """
    
    html_path = os.path.join(config.PROCESSED_DIR, "dashboard.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
        
    logger.info(f"Dashboard généré. Images et HTML sauvegardés dans {config.PROCESSED_DIR}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
