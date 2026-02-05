"""Module de génération de dashboard simple"""
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
