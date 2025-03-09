import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from sqlalchemy import create_engine, text
import base64
from io import BytesIO
import webbrowser
import time
from tqdm import tqdm
import sys

# Ajout du répertoire parent au chemin de recherche Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Chemins des dossiers dans la nouvelle structure
RESULTS_DIR = 'results'
VISUALISATIONS_DIR = os.path.join(RESULTS_DIR, 'visualisations')
LOGS_DIR = os.path.join(RESULTS_DIR, 'logs')

# Création du dossier pour les visualisations
if not os.path.exists(VISUALISATIONS_DIR):
    os.makedirs(VISUALISATIONS_DIR)
    print(f"📁 Dossier {VISUALISATIONS_DIR} créé")

# Paramètres de connexion à PostgreSQL
DB_USER = 'postgres'
DB_PASSWORD = 'admin'  # À modifier selon votre configuration
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'data_warehouse'

# Création de la chaîne de connexion
conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Création du moteur SQLAlchemy
try:
    engine = create_engine(conn_string)
    conn = engine.connect()
    print("🔌 Connexion à PostgreSQL établie avec succès")
except Exception as e:
    print(f"❌ Erreur lors de la connexion à PostgreSQL: {e}")
    exit(1)

def print_section(title, emoji="📊"):
    """
    Affiche un titre de section formaté.
    """
    print(f"\n{emoji} {title} {emoji}")
    print("-" * 50)

# Fonction pour exécuter une requête SQL et retourner un DataFrame
def execute_query(query):
    """Exécute une requête SQL et retourne un DataFrame"""
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution de la requête: {e}")
        # Effectuer un rollback sur la connexion en cas d'erreur
        conn.connection.rollback()
        return pd.DataFrame()

# Analyse des ventes par catégorie de produit
def analyze_sales_by_category():
    print_section("ANALYSE DES VENTES PAR CATÉGORIE", "📦")
    print("⏳ Exécution de l'analyse des ventes par catégorie...")
    
    query = """
    SELECT
        p.category,
        SUM(s.total_amount) as total_sales,
        COUNT(DISTINCT s.sale_id) as transaction_count
    FROM sales_fact s
    JOIN products_dim p ON s.product_id = p.product_id
    GROUP BY p.category
    ORDER BY total_sales DESC
    """
    
    df = execute_query(query)
    
    if df.empty:
        print("⚠️ Aucune donnée trouvée pour les ventes par catégorie")
        return df
    
    # Création du graphique
    plt.figure(figsize=(12, 6))
    
    # Graphique des ventes totales par catégorie
    plt.subplot(1, 2, 1)
    sns.barplot(x='category', y='total_sales', data=df)
    plt.title('Ventes totales par catégorie')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Montant total des ventes (€)')
    
    # Graphique du nombre de transactions par catégorie
    plt.subplot(1, 2, 2)
    sns.barplot(x='category', y='transaction_count', data=df)
    plt.title('Nombre de transactions par catégorie')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Nombre de transactions')
    
    plt.tight_layout()
    
    plt.savefig(os.path.join(VISUALISATIONS_DIR, 'sales_by_category.png'))
    plt.close()
    
    print("✅ Analyse des ventes par catégorie terminée")
    return df

# Analyse des ventes par mois
def analyze_sales_by_month():
    print_section("ANALYSE DES VENTES PAR MOIS", "📅")
    print("⏳ Exécution de l'analyse des ventes par mois...")
    
    query = """
    SELECT
        t.month_name,
        SUM(s.total_amount) as total_sales,
        COUNT(DISTINCT s.sale_id) as transaction_count
    FROM sales_fact s
    JOIN time_dim t ON s.date_id = t.date_id
    GROUP BY t.month_name, t.month
    ORDER BY t.month
    """
    
    df = execute_query(query)
    
    if df.empty:
        print("⚠️ Aucune donnée trouvée pour les ventes par mois")
        return df
    
    # Création du graphique
    plt.figure(figsize=(12, 6))
    
    # Graphique des ventes totales par mois
    plt.subplot(1, 2, 1)
    sns.barplot(x='month_name', y='total_sales', data=df)
    plt.title('Ventes totales par mois')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Montant total des ventes (€)')
    
    # Graphique du nombre de transactions par mois
    plt.subplot(1, 2, 2)
    sns.barplot(x='month_name', y='transaction_count', data=df)
    plt.title('Nombre de transactions par mois')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Nombre de transactions')
    
    plt.tight_layout()
    
    plt.savefig(os.path.join(VISUALISATIONS_DIR, 'sales_by_month.png'))
    plt.close()
    
    print("✅ Analyse des ventes par mois terminée")
    return df

# Analyse des ventes par ville
def analyze_sales_by_city():
    print_section("ANALYSE DES VENTES PAR VILLE", "🏙️")
    print("⏳ Exécution de l'analyse des ventes par ville...")
    
    query = """
    SELECT
        c.city,
        SUM(s.total_amount) as total_sales,
        COUNT(DISTINCT s.sale_id) as transaction_count,
        COUNT(DISTINCT c.customer_id) as customer_count
    FROM sales_fact s
    JOIN customers_dim c ON s.customer_id = c.customer_id
    GROUP BY c.city
    ORDER BY total_sales DESC
    LIMIT 10
    """
    
    df = execute_query(query)
    
    if df.empty:
        print("⚠️ Aucune donnée trouvée pour les ventes par ville")
        return df
    
    # Création du graphique
    plt.figure(figsize=(12, 6))
    
    # Graphique des ventes totales par ville
    plt.subplot(1, 2, 1)
    sns.barplot(x='city', y='total_sales', data=df)
    plt.title('Ventes totales par ville')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Montant total des ventes (€)')
    
    # Graphique du nombre de clients par ville
    plt.subplot(1, 2, 2)
    sns.barplot(x='city', y='customer_count', data=df)
    plt.title('Nombre de clients par ville')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    plt.savefig(os.path.join(VISUALISATIONS_DIR, 'sales_by_city.png'))
    plt.close()
    
    print("✅ Analyse des ventes par ville terminée")
    return df

# Analyse des ventes par tranche d'âge
def analyze_sales_by_age():
    print_section("ANALYSE DES VENTES PAR TRANCHE D'ÂGE", "👥")
    print("⏳ Exécution de l'analyse des ventes par tranche d'âge...")
    
    query = """
    SELECT
        CASE
            WHEN c.age < 25 THEN '18-24'
            WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
            WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
            WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
            WHEN c.age BETWEEN 55 AND 64 THEN '55-64'
            ELSE '65+'
        END as age_group,
        SUM(s.total_amount) as total_sales,
        COUNT(DISTINCT s.sale_id) as transaction_count,
        COUNT(DISTINCT c.customer_id) as customer_count
    FROM sales_fact s
    JOIN customers_dim c ON s.customer_id = c.customer_id
    GROUP BY
        CASE
            WHEN c.age < 25 THEN '18-24'
            WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
            WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
            WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
            WHEN c.age BETWEEN 55 AND 64 THEN '55-64'
            ELSE '65+'
        END
    ORDER BY
        CASE
            WHEN CASE
                WHEN c.age < 25 THEN '18-24'
                WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
                WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
                WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
                WHEN c.age BETWEEN 55 AND 64 THEN '55-64'
                ELSE '65+'
            END = '18-24' THEN 1
            WHEN CASE
                WHEN c.age < 25 THEN '18-24'
                WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
                WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
                WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
                WHEN c.age BETWEEN 55 AND 64 THEN '55-64'
                ELSE '65+'
            END = '25-34' THEN 2
            WHEN CASE
                WHEN c.age < 25 THEN '18-24'
                WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
                WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
                WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
                WHEN c.age BETWEEN 55 AND 64 THEN '55-64'
                ELSE '65+'
            END = '35-44' THEN 3
            WHEN CASE
                WHEN c.age < 25 THEN '18-24'
                WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
                WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
                WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
                WHEN c.age BETWEEN 55 AND 64 THEN '55-64'
                ELSE '65+'
            END = '45-54' THEN 4
            WHEN CASE
                WHEN c.age < 25 THEN '18-24'
                WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
                WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
                WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
                WHEN c.age BETWEEN 55 AND 64 THEN '55-64'
                ELSE '65+'
            END = '55-64' THEN 5
            ELSE 6
        END
    """
    
    df = execute_query(query)
    
    if df.empty:
        print("⚠️ Aucune donnée trouvée pour les ventes par tranche d'âge")
        return df
    
    # Création du graphique
    plt.figure(figsize=(15, 7))
    
    # Graphique des ventes par tranche d'âge
    plt.subplot(1, 3, 1)
    sns.barplot(x='age_group', y='total_sales', data=df)
    plt.title('Ventes par tranche d\'âge')
    plt.ylabel('Montant total des ventes (€)')
    
    # Graphique du nombre de transactions par tranche d'âge
    plt.subplot(1, 3, 2)
    sns.barplot(x='age_group', y='transaction_count', data=df)
    plt.title('Transactions par tranche d\'âge')
    plt.ylabel('Nombre de transactions')
    
    # Graphique du nombre de clients par tranche d'âge
    plt.subplot(1, 3, 3)
    sns.barplot(x='age_group', y='customer_count', data=df)
    plt.title('Clients par tranche d\'âge')
    plt.ylabel('Nombre de clients')
    
    plt.tight_layout()
    
    plt.savefig(os.path.join(VISUALISATIONS_DIR, 'sales_by_age.png'))
    plt.close()
    
    print("✅ Analyse des ventes par tranche d'âge terminée")
    return df

# Analyse des méthodes de paiement
def analyze_payment_methods():
    print_section("ANALYSE DES MÉTHODES DE PAIEMENT", "💳")
    print("⏳ Exécution de l'analyse des méthodes de paiement...")
    
    query = """
    SELECT
        s.payment_method,
        COUNT(*) as count,
        SUM(s.total_amount) as total_amount,
        AVG(s.total_amount) as avg_amount
    FROM sales_fact s
    GROUP BY s.payment_method
    ORDER BY count DESC
    """
    
    df = execute_query(query)
    
    if df.empty:
        print("⚠️ Aucune donnée trouvée pour les méthodes de paiement")
        return df
    
    # Création du graphique
    plt.figure(figsize=(15, 8))
    
    # Graphique du nombre de transactions par méthode de paiement
    plt.subplot(1, 2, 1)
    plt.pie(df['count'], labels=df['payment_method'], autopct='%1.1f%%', startangle=140)
    plt.title('Répartition des méthodes de paiement')
    plt.axis('equal')
    
    # Graphique du montant total par méthode de paiement
    plt.subplot(1, 2, 2)
    plt.pie(df['total_amount'], labels=df['payment_method'], autopct='%1.1f%%', startangle=140)
    plt.title('Répartition du montant total par méthode de paiement')
    plt.axis('equal')
    
    plt.tight_layout()
    
    plt.savefig(os.path.join(VISUALISATIONS_DIR, 'payment_methods.png'))
    plt.close()
    
    print("✅ Analyse des méthodes de paiement terminée")
    return df

# Fonction pour générer un rapport HTML
def generate_html_report():
    print_section("GÉNÉRATION DU RAPPORT HTML", "📄")
    print("⏳ Génération du rapport HTML en cours...")
    
    # Chemin du fichier HTML
    html_path = os.path.join(VISUALISATIONS_DIR, 'rapport_analyse.html')
    
    # Liste des images générées avec les nouveaux chemins
    images = [
        {'path': os.path.join(VISUALISATIONS_DIR, 'sales_by_category.png'), 'title': 'Ventes par catégorie de produit', 'emoji': '📦'},
        {'path': os.path.join(VISUALISATIONS_DIR, 'sales_by_month.png'), 'title': 'Ventes par mois', 'emoji': '📅'},
        {'path': os.path.join(VISUALISATIONS_DIR, 'sales_by_city.png'), 'title': 'Nombre de clients par ville', 'emoji': '🏙️'},
        {'path': os.path.join(VISUALISATIONS_DIR, 'sales_by_age.png'), 'title': 'Ventes par tranche d\'âge', 'emoji': '👥'},
        {'path': os.path.join(VISUALISATIONS_DIR, 'payment_methods.png'), 'title': 'Méthodes de paiement', 'emoji': '💳'}
    ]
    
    # Animation de barre de progression
    for i in tqdm(range(10), desc="Préparation du rapport HTML"):
        time.sleep(0.1)
    
    # Génération du HTML
    html_content = """
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Rapport d'analyse des données</title>
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f5f5f5;
                color: #333;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background-color: white;
                padding: 30px;
                box-shadow: 0 0 20px rgba(0,0,0,0.1);
                border-radius: 10px;
                margin-top: 20px;
                margin-bottom: 20px;
            }
            header {
                text-align: center;
                margin-bottom: 30px;
                border-bottom: 2px solid #3498db;
                padding-bottom: 20px;
            }
            h1 {
                color: #2c3e50;
                font-size: 36px;
                margin-bottom: 10px;
            }
            .subtitle {
                color: #7f8c8d;
                font-size: 18px;
            }
            h2 {
                color: #3498db;
                margin-top: 40px;
                padding-bottom: 10px;
                border-bottom: 1px solid #eee;
                display: flex;
                align-items: center;
                font-size: 24px;
            }
            h2 .emoji {
                margin-right: 10px;
                font-size: 28px;
            }
            .image-container {
                margin: 25px 0;
                text-align: center;
                background-color: #f9f9f9;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            }
            img {
                max-width: 100%;
                height: auto;
                border-radius: 8px;
            }
            .image-caption {
                margin-top: 15px;
                color: #555;
                font-style: italic;
            }
            footer {
                margin-top: 50px;
                text-align: center;
                color: #7f8c8d;
                font-size: 14px;
                border-top: 1px solid #eee;
                padding-top: 20px;
            }
            .date {
                font-weight: bold;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>📊 Rapport d'analyse des données</h1>
                <div class="subtitle">Analyse des données du data warehouse</div>
            </header>
    """
    
    # Ajout des images
    for image in images:
        html_content += f"""
            <h2><span class="emoji">{image['emoji']}</span> {image['title']}</h2>
            <div class="image-container">
                <img src="{os.path.basename(image['path'])}" alt="{image['title']}">
                <div class="image-caption">Analyse détaillée des {image['title'].lower()}</div>
            </div>
        """
    
    # Fermeture du HTML
    html_content += """
            <footer>
                <p>Rapport généré le <span class="date">{}</span></p>
                <p>Data Warehouse ETL - Pipeline simplifié</p>
            </footer>
        </div>
    </body>
    </html>
    """.format(pd.Timestamp.now().strftime("%d/%m/%Y à %H:%M:%S"))
    
    # Animation de barre de progression
    for i in tqdm(range(5), desc="Finalisation du rapport"):
        time.sleep(0.1)
    
    # Écriture du fichier HTML
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✨ Rapport HTML généré avec succès: {html_path}")
    return html_path

# Fonction principale
def main(open_html=False):
    print_section("ANALYSE DES DONNÉES", "🔍")
    print("🚀 Démarrage de l'analyse des données du data warehouse PostgreSQL...")
    
    # Exécution des analyses
    try:
        analyses = [
            {"fonction": analyze_sales_by_category, "nom": "Ventes par catégorie"},
            {"fonction": analyze_sales_by_month, "nom": "Ventes par mois"},
            {"fonction": analyze_sales_by_city, "nom": "Ventes par ville"},
            {"fonction": analyze_sales_by_age, "nom": "Ventes par tranche d'âge"},
            {"fonction": analyze_payment_methods, "nom": "Méthodes de paiement"}
        ]
        
        results = {}
        
        # Exécuter chaque analyse avec une barre de progression visuelle
        for analyse in analyses:
            results[analyse["nom"]] = analyse["fonction"]()
        
        # Affichage des résultats
        print_section("RÉSUMÉ DES ANALYSES", "📋")
        
        print("\n🏆 Top 3 des catégories par ventes:")
        if 'Ventes par catégorie' in results and not results['Ventes par catégorie'].empty:
            print(results['Ventes par catégorie'][['category', 'total_sales']].head(3).to_string(index=False))
        
        print("\n📆 Top 3 des mois par ventes:")
        if 'Ventes par mois' in results and not results['Ventes par mois'].empty:
            print(results['Ventes par mois'].sort_values('total_sales', ascending=False)[['month_name', 'total_sales']].head(3).to_string(index=False))
        
        print("\n🏙️ Top 3 des villes par ventes:")
        if 'Ventes par ville' in results and not results['Ventes par ville'].empty:
            print(results['Ventes par ville'][['city', 'total_sales']].head(3).to_string(index=False))
        
        print("\n👥 Répartition des ventes par tranche d'âge:")
        if 'Ventes par tranche d\'âge' in results and not results['Ventes par tranche d\'âge'].empty:
            print(results['Ventes par tranche d\'âge'][['age_group', 'total_sales']].to_string(index=False))
        
        print("\n💳 Répartition des méthodes de paiement:")
        if 'Méthodes de paiement' in results and not results['Méthodes de paiement'].empty:
            print(results['Méthodes de paiement'][['payment_method', 'count', 'total_amount']].to_string(index=False))
        
        # Génération du rapport HTML
        html_path = generate_html_report()
        
        print_section("ANALYSE TERMINÉE", "✅")
        print(f"📊 Les visualisations ont été sauvegardées dans le dossier '{VISUALISATIONS_DIR}'")
        
        # Ouverture du HTML si demandé
        if open_html:
            print("🌐 Ouverture du rapport HTML dans votre navigateur...")
            webbrowser.open('file://' + os.path.abspath(html_path))
            print(f"🔗 Rapport ouvert: {html_path}")
        else:
            print(f"📄 Le rapport HTML est disponible à: {html_path}")
            print("💡 Pour l'ouvrir manuellement, accédez à ce fichier avec votre navigateur")
    except Exception as e:
        print(f"❌ Erreur lors de l'analyse des données: {e}")
    finally:
        # Fermeture de la connexion
        conn.close()
        print("🔌 Connexion à la base de données fermée")

if __name__ == "__main__":
    # Si exécuté directement, demander si l'utilisateur veut ouvrir le HTML
    print("\n" + "="*70)
    print("📊 ANALYSE DES DONNÉES DU DATA WAREHOUSE 📊".center(70))
    print("="*70 + "\n")
    
    user_choice = input("Voulez-vous ouvrir le rapport HTML à la fin de l'analyse? (o/n): ").lower()
    open_html = user_choice in ['o', 'oui', 'y', 'yes']
    main(open_html)