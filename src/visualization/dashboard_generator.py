import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import text
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import base64
from io import BytesIO
import webbrowser
from datetime import datetime

def get_data_from_db(engine):
    """Récupère les données nécessaires pour les visualisations depuis la base de données."""
    # Requêtes adaptées au schéma en étoile existant
    
    # Ventes par mois
    sales_query = """
    SELECT t.month, t.year, SUM(s.total_amount) as total_sales
    FROM sales_fact s
    JOIN time_dim t ON s.date_id = t.date_id
    GROUP BY t.month, t.year
    ORDER BY t.year, t.month
    """
    
    # Top produits
    products_query = """
    SELECT p.name as product_name, SUM(s.quantity) as total_quantity
    FROM sales_fact s
    JOIN products_dim p ON s.product_id = p.product_id
    GROUP BY p.name
    ORDER BY total_quantity DESC
    LIMIT 10
    """
    
    # Distribution géographique
    geo_query = """
    SELECT c.city as region, COUNT(*) as customer_count
    FROM customers_dim c
    GROUP BY c.city
    ORDER BY customer_count DESC
    LIMIT 10
    """
    
    try:
        # Exécution des requêtes
        sales_data = pd.read_sql(sales_query, engine)
        # Créer une colonne date à partir de month et year pour le graphique
        if not sales_data.empty:
            sales_data['month'] = pd.to_datetime(sales_data['year'].astype(str) + '-' + sales_data['month'].astype(str))
        
        products_data = pd.read_sql(products_query, engine)
        geo_data = pd.read_sql(geo_query, engine)
        
        return {
            'sales': sales_data,
            'products': products_data,
            'geo': geo_data
        }
    except Exception as e:
        print(f"Erreur lors de l'extraction des données: {e}")
        # Générer des données fictives en cas d'erreur
        return generate_sample_data()

def generate_sample_data():
    """Génère des données d'exemple si l'extraction depuis la BD échoue."""
    # Données d'exemple pour les ventes mensuelles
    sales_data = pd.DataFrame({
        'month': pd.date_range(start='2023-01-01', periods=12, freq='M'),
        'total_sales': [15000, 18000, 22000, 19000, 25000, 27000, 
                         24000, 22000, 30000, 28000, 32000, 35000]
    })
    
    # Données d'exemple pour les top produits
    products_data = pd.DataFrame({
        'product_name': ['Produit A', 'Produit B', 'Produit C', 'Produit D', 'Produit E'],
        'total_quantity': [1200, 900, 850, 700, 650]
    })
    
    # Données d'exemple pour la distribution géographique
    geo_data = pd.DataFrame({
        'region': ['Paris', 'Lyon', 'Marseille', 'Bordeaux', 'Lille'],
        'customer_count': [1500, 950, 850, 700, 650]
    })
    
    return {
        'sales': sales_data,
        'products': products_data,
        'geo': geo_data
    }

def create_matplotlib_charts(data):
    """Crée des graphiques avec Matplotlib et Seaborn et les convertit en base64."""
    charts = {}
    
    # Configuration des styles
    sns.set_style("whitegrid")
    plt.rcParams.update({'font.size': 12})
    
    # Graphique 1: Évolution des ventes mensuelles
    plt.figure(figsize=(10, 6))
    sns.lineplot(x='month', y='total_sales', data=data['sales'], marker='o', linewidth=2)
    plt.title('Évolution des ventes mensuelles', fontsize=16)
    plt.xlabel('Mois')
    plt.ylabel('Ventes totales (€)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Convertir le graphique en image base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100)
    buffer.seek(0)
    charts['sales_trend'] = base64.b64encode(buffer.getvalue()).decode('utf-8')
    plt.close()
    
    # Graphique 2: Top produits
    plt.figure(figsize=(10, 6))
    chart = sns.barplot(x='product_name', y='total_quantity', data=data['products'])
    plt.title('Top 5 produits vendus', fontsize=16)
    plt.xlabel('Produit')
    plt.ylabel('Quantité vendue')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100)
    buffer.seek(0)
    charts['top_products'] = base64.b64encode(buffer.getvalue()).decode('utf-8')
    plt.close()
    
    # Graphique 3: Distribution géographique
    plt.figure(figsize=(10, 6))
    chart = sns.barplot(x='region', y='customer_count', data=data['geo'])
    plt.title('Répartition des clients par région', fontsize=16)
    plt.xlabel('Région')
    plt.ylabel('Nombre de clients')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100)
    buffer.seek(0)
    charts['geo_distribution'] = base64.b64encode(buffer.getvalue()).decode('utf-8')
    plt.close()
    
    return charts

def create_plotly_charts(data):
    """Crée des graphiques interactifs avec Plotly et les convertit en HTML."""
    charts = {}
    
    # Graphique 1: Évolution des ventes mensuelles
    fig_sales = px.line(
        data['sales'], 
        x='month', 
        y='total_sales',
        markers=True,
        title='Évolution des ventes mensuelles',
        labels={'month': 'Mois', 'total_sales': 'Ventes totales (€)'}
    )
    fig_sales.update_layout(
        title_font_size=20,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14
    )
    charts['sales_trend'] = fig_sales.to_html(full_html=False, include_plotlyjs='cdn')
    
    # Graphique 2: Top produits
    fig_products = px.bar(
        data['products'],
        x='product_name',
        y='total_quantity',
        title='Top produits vendus',
        labels={'product_name': 'Produit', 'total_quantity': 'Quantité vendue'}
    )
    fig_products.update_layout(
        title_font_size=20,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14
    )
    charts['top_products'] = fig_products.to_html(full_html=False, include_plotlyjs='cdn')
    
    # Graphique 3: Distribution géographique
    fig_geo = px.bar(
        data['geo'],
        x='region',
        y='customer_count',
        title='Répartition des clients par région',
        labels={'region': 'Région', 'customer_count': 'Nombre de clients'}
    )
    fig_geo.update_layout(
        title_font_size=20,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14
    )
    charts['geo_distribution'] = fig_geo.to_html(full_html=False, include_plotlyjs='cdn')
    
    return charts

def generate_html_dashboard(charts, use_plotly=True):
    """Génère un dashboard HTML à partir des graphiques."""
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    
    # CSS pour le dashboard
    css = """
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 0;
            background-color: #f5f7fa;
            color: #333;
        }
        .container {
            width: 90%;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        .header {
            background-color: #2c3e50;
            color: white;
            padding: 20px 0;
            text-align: center;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .dashboard-title {
            margin: 0;
            font-size: 28px;
        }
        .timestamp {
            color: #ddd;
            font-size: 14px;
            margin-top: 5px;
        }
        .chart-container {
            background-color: white;
            border-radius: 5px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 20px;
            margin-bottom: 20px;
        }
        .chart-title {
            color: #2c3e50;
            font-size: 18px;
            margin-top: 0;
            margin-bottom: 15px;
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
        }
        .chart {
            width: 100%;
            height: auto;
            min-height: 300px;
        }
        .footer {
            text-align: center;
            margin-top: 30px;
            color: #7f8c8d;
            font-size: 14px;
        }
        @media (max-width: 768px) {
            .container {
                width: 95%;
            }
            .chart-container {
                padding: 15px;
            }
        }
    </style>
    """
    
    # Contenu HTML
    html_content = f"""
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Dashboard - Data Warehouse</title>
        {css}
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1 class="dashboard-title">Dashboard - Data Warehouse</h1>
                <p class="timestamp">Généré le {now}</p>
            </div>
            
            <div class="chart-container">
                <h2 class="chart-title">Évolution des ventes mensuelles</h2>
                <div class="chart">
                    {"" if use_plotly else f'<img src="data:image/png;base64,{charts["sales_trend"]}" width="100%">'} 
                    {charts["sales_trend"] if use_plotly else ""}
                </div>
            </div>
            
            <div class="chart-container">
                <h2 class="chart-title">Top produits vendus</h2>
                <div class="chart">
                    {"" if use_plotly else f'<img src="data:image/png;base64,{charts["top_products"]}" width="100%">'}
                    {charts["top_products"] if use_plotly else ""}
                </div>
            </div>
            
            <div class="chart-container">
                <h2 class="chart-title">Répartition des clients par région</h2>
                <div class="chart">
                    {"" if use_plotly else f'<img src="data:image/png;base64,{charts["geo_distribution"]}" width="100%">'}
                    {charts["geo_distribution"] if use_plotly else ""}
                </div>
            </div>
            
            <div class="footer">
                <p>© {datetime.now().year} - Projet Data Warehouse</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Créer le répertoire de sortie s'il n'existe pas
    os.makedirs('dashboard', exist_ok=True)
    
    # Enregistrer le fichier HTML
    file_path = os.path.join('dashboard', 'index.html')
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return file_path

def generate_dashboard(engine=None, open_browser=True, use_plotly=True):
    """
    Fonction principale pour générer le dashboard.
    
    Args:
        engine: Connexion SQLAlchemy à la base de données
        open_browser: Si True, ouvre automatiquement le navigateur
        use_plotly: Si True, utilise Plotly pour les graphiques interactifs
    
    Returns:
        Le chemin du fichier HTML généré
    """
    print("Génération du dashboard en cours...")
    
    # Récupérer les données
    if engine:
        data = get_data_from_db(engine)
    else:
        data = generate_sample_data()
    
    # Créer les graphiques
    if use_plotly:
        try:
            import plotly
            charts = create_plotly_charts(data)
        except ImportError:
            print("Plotly n'est pas installé. Utilisation de Matplotlib à la place.")
            charts = create_matplotlib_charts(data)
            use_plotly = False
    else:
        charts = create_matplotlib_charts(data)
    
    # Générer le fichier HTML
    file_path = generate_html_dashboard(charts, use_plotly)
    
    print(f"✅ Dashboard généré avec succès: {file_path}")
    
    # Ouvrir le navigateur si demandé
    if open_browser:
        webbrowser.open('file://' + os.path.abspath(file_path))
    
    return file_path

if __name__ == "__main__":
    # Test avec des données d'exemple
    generate_dashboard(engine=None, open_browser=True, use_plotly=True) 