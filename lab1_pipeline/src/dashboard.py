import pandas as pd
import matplotlib.pyplot as plt
import logging
import base64
from io import BytesIO
from src import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fig_to_base64(fig):
    """Convert a matplotlib figure to a base64-encoded PNG string."""
    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight', facecolor='#1a1a2e')
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode('utf-8')
    buf.close()
    return encoded


def generate_charts():
    """Generate all charts and return them as base64-encoded strings."""
    charts = {}
    
    # Set dark theme for matplotlib
    plt.style.use('dark_background')
    plt.rcParams['figure.facecolor'] = '#1a1a2e'
    plt.rcParams['axes.facecolor'] = '#16213e'
    plt.rcParams['axes.edgecolor'] = '#e94560'
    plt.rcParams['axes.labelcolor'] = '#eaeaea'
    plt.rcParams['text.color'] = '#eaeaea'
    plt.rcParams['xtick.color'] = '#eaeaea'
    plt.rcParams['ytick.color'] = '#eaeaea'
    plt.rcParams['grid.color'] = '#0f3460'
    
    # 1. Daily Metrics Time Series
    daily_file = config.PROCESSED_DIR / "daily_metrics.csv"
    if daily_file.exists():
        df_daily = pd.read_csv(daily_file)
        if not df_daily.empty and 'date' in df_daily.columns:
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            
            # Plot Volume
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.fill_between(df_daily['date'], df_daily['daily_number_of_reviews'], alpha=0.3, color='#e94560')
            ax.plot(df_daily['date'], df_daily['daily_number_of_reviews'], marker='o', linestyle='-', color='#e94560', linewidth=2)
            ax.set_title('Daily Review Volume', fontsize=14, fontweight='bold')
            ax.set_xlabel('Date')
            ax.set_ylabel('Count')
            ax.grid(True, alpha=0.3)
            charts['volume'] = fig_to_base64(fig)
            plt.close(fig)
            logger.info("Generated volume chart")

            # Plot Rating Trend
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.fill_between(df_daily['date'], df_daily['daily_average_rating'], alpha=0.3, color='#00d9ff')
            ax.plot(df_daily['date'], df_daily['daily_average_rating'], marker='o', linestyle='-', color='#00d9ff', linewidth=2)
            ax.set_title('Daily Average Rating', fontsize=14, fontweight='bold')
            ax.set_xlabel('Date')
            ax.set_ylabel('Average Score')
            ax.set_ylim(1, 5)
            ax.grid(True, alpha=0.3)
            charts['rating'] = fig_to_base64(fig)
            plt.close(fig)
            logger.info("Generated rating chart")

    # 2. Score Distribution
    reviews_file = config.PROCESSED_DIR / "apps_reviews.csv"
    if reviews_file.exists():
        df_reviews = pd.read_csv(reviews_file)
        if not df_reviews.empty and 'score' in df_reviews.columns:
            fig, ax = plt.subplots(figsize=(8, 6))
            colors = ['#e94560', '#ff6b6b', '#feca57', '#48dbfb', '#1dd1a1']
            score_counts = df_reviews['score'].value_counts().sort_index()
            bars = ax.bar(score_counts.index, score_counts.values, color=colors[:len(score_counts)], edgecolor='white', linewidth=1.5)
            ax.set_title('Distribution of Review Scores', fontsize=14, fontweight='bold')
            ax.set_xlabel('Score')
            ax.set_ylabel('Frequency')
            ax.set_xticks(range(1, 6))
            ax.grid(axis='y', alpha=0.3)
            charts['distribution'] = fig_to_base64(fig)
            plt.close(fig)
            logger.info("Generated distribution chart")

    # 3. App Ranking
    kpis_file = config.PROCESSED_DIR / "app_kpis.csv"
    if kpis_file.exists():
        df_kpis = pd.read_csv(kpis_file)
        if not df_kpis.empty and 'average_rating' in df_kpis.columns:
            df_sorted = df_kpis.sort_values('average_rating', ascending=True)
            
            fig, ax = plt.subplots(figsize=(10, 6))
            colors = ['#1dd1a1' if r >= 4 else '#feca57' if r >= 3 else '#e94560' for r in df_sorted['average_rating']]
            bars = ax.barh(df_sorted['app_name'], df_sorted['average_rating'], color=colors, edgecolor='white', linewidth=1.5)
            ax.set_xlabel('Average Rating')
            ax.set_title('App Ranking by Average Rating', fontsize=14, fontweight='bold')
            ax.set_xlim(0, 5)
            ax.grid(axis='x', alpha=0.3)
            charts['ranking'] = fig_to_base64(fig)
            plt.close(fig)
            logger.info("Generated ranking chart")
    
    return charts


def generate_html(charts):
    """Generate a premium HTML dashboard with embedded charts."""
    
    # Load KPIs for summary cards
    kpis_file = config.PROCESSED_DIR / "app_kpis.csv"
    kpi_cards = ""
    if kpis_file.exists():
        df_kpis = pd.read_csv(kpis_file)
        if not df_kpis.empty:
            row = df_kpis.iloc[0]
            kpi_cards = f"""
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value">{int(row.get('number_of_reviews', 0)):,}</div>
                    <div class="kpi-label">Total Reviews</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{row.get('average_rating', 0):.2f}</div>
                    <div class="kpi-label">Average Rating</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{row.get('pct_low_rating_reviews', 0):.1f}%</div>
                    <div class="kpi-label">Low Rating %</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{row.get('app_name', 'N/A')}</div>
                    <div class="kpi-label">App Name</div>
                </div>
            </div>
            """
    
    # Build chart sections
    chart_sections = ""
    if 'volume' in charts:
        chart_sections += f'''
        <div class="chart-card">
            <h3>📈 Daily Review Volume</h3>
            <img src="data:image/png;base64,{charts['volume']}" alt="Daily Volume Chart">
        </div>
        '''
    if 'rating' in charts:
        chart_sections += f'''
        <div class="chart-card">
            <h3>⭐ Daily Average Rating</h3>
            <img src="data:image/png;base64,{charts['rating']}" alt="Daily Rating Chart">
        </div>
        '''
    if 'distribution' in charts:
        chart_sections += f'''
        <div class="chart-card">
            <h3>📊 Score Distribution</h3>
            <img src="data:image/png;base64,{charts['distribution']}" alt="Score Distribution Chart">
        </div>
        '''
    if 'ranking' in charts:
        chart_sections += f'''
        <div class="chart-card full-width">
            <h3>🏆 App Ranking</h3>
            <img src="data:image/png;base64,{charts['ranking']}" alt="App Ranking Chart">
        </div>
        '''
    
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>App Reviews Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            min-height: 100vh;
            color: #eaeaea;
            padding: 2rem;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        header {{
            text-align: center;
            margin-bottom: 3rem;
        }}
        
        h1 {{
            font-size: 2.5rem;
            background: linear-gradient(90deg, #e94560, #00d9ff);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 0.5rem;
        }}
        
        .subtitle {{
            color: #8892b0;
            font-size: 1.1rem;
        }}
        
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            margin-bottom: 3rem;
        }}
        
        .kpi-card {{
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 16px;
            padding: 1.5rem;
            text-align: center;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}
        
        .kpi-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(233, 69, 96, 0.2);
        }}
        
        .kpi-value {{
            font-size: 2rem;
            font-weight: bold;
            color: #e94560;
            margin-bottom: 0.5rem;
        }}
        
        .kpi-label {{
            color: #8892b0;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 2rem;
        }}
        
        .chart-card {{
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 16px;
            padding: 1.5rem;
            transition: transform 0.3s ease;
        }}
        
        .chart-card:hover {{
            transform: translateY(-3px);
        }}
        
        .chart-card.full-width {{
            grid-column: 1 / -1;
        }}
        
        .chart-card h3 {{
            color: #00d9ff;
            margin-bottom: 1rem;
            font-size: 1.2rem;
        }}
        
        .chart-card img {{
            width: 100%;
            height: auto;
            border-radius: 8px;
        }}
        
        footer {{
            text-align: center;
            margin-top: 3rem;
            color: #8892b0;
            font-size: 0.9rem;
        }}
        
        @media (max-width: 600px) {{
            .charts-grid {{
                grid-template-columns: 1fr;
            }}
            
            h1 {{
                font-size: 1.8rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📱 App Reviews Dashboard</h1>
            <p class="subtitle">Real-time analytics and insights from user reviews</p>
        </header>
        
        {kpi_cards}
        
        <div class="charts-grid">
            {chart_sections}
        </div>
        
        <footer>
            <p>Generated by Data Pipeline • Powered by Python & Matplotlib</p>
        </footer>
    </div>
</body>
</html>'''
    
    return html


def run():
    logger.info("Generating Dashboard...")
    
    # Generate charts as base64
    charts = generate_charts()
    
    if not charts:
        logger.warning("No charts generated. Ensure processed data exists.")
        return
    
    # Generate HTML
    html_content = generate_html(charts)
    
    # Save HTML file
    output_path = config.PROCESSED_DIR / "dashboard.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    logger.info(f"Saved dashboard: {output_path}")
    
    # Also save individual PNG files for backwards compatibility
    daily_file = config.PROCESSED_DIR / "daily_metrics.csv"
    if daily_file.exists():
        df_daily = pd.read_csv(daily_file)
        if not df_daily.empty and 'date' in df_daily.columns:
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            
            plt.figure(figsize=(10, 5))
            plt.plot(df_daily['date'], df_daily['daily_number_of_reviews'], marker='o', linestyle='-')
            plt.title('Daily Review Volume')
            plt.xlabel('Date')
            plt.ylabel('Count')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(config.PROCESSED_DIR / "dashboard_daily_volume.png")
            plt.close()

            plt.figure(figsize=(10, 5))
            plt.plot(df_daily['date'], df_daily['daily_average_rating'], marker='o', linestyle='-', color='orange')
            plt.title('Daily Average Rating')
            plt.xlabel('Date')
            plt.ylabel('Average Score')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(config.PROCESSED_DIR / "dashboard_daily_rating.png")
            plt.close()

    reviews_file = config.PROCESSED_DIR / "apps_reviews.csv"
    if reviews_file.exists():
        df_reviews = pd.read_csv(reviews_file)
        if not df_reviews.empty and 'score' in df_reviews.columns:
            plt.figure(figsize=(8, 6))
            df_reviews['score'].hist(bins=5, range=(1, 6), edgecolor='black')
            plt.title('Distribution of Review Scores')
            plt.xlabel('Score')
            plt.ylabel('Frequency')
            plt.xticks(range(1, 6))
            plt.grid(axis='y', alpha=0.75)
            plt.savefig(config.PROCESSED_DIR / "dashboard_score_dist.png")
            plt.close()

    kpis_file = config.PROCESSED_DIR / "app_kpis.csv"
    if kpis_file.exists():
        df_kpis = pd.read_csv(kpis_file)
        if not df_kpis.empty and 'average_rating' in df_kpis.columns:
            df_sorted = df_kpis.sort_values('average_rating', ascending=True)
            plt.figure(figsize=(10, 6))
            colors = ['green' if r >= 4 else 'orange' if r >= 3 else 'red' for r in df_sorted['average_rating']]
            plt.barh(df_sorted['app_name'], df_sorted['average_rating'], color=colors)
            plt.xlabel('Average Rating')
            plt.title('App Ranking by Average Rating')
            plt.xlim(0, 5)
            plt.tight_layout()
            plt.savefig(config.PROCESSED_DIR / "dashboard_app_ranking.png")
            plt.close()


if __name__ == "__main__":
    run()
