"""
Générateur de données simplifiées pour l'entrepôt de données.
Ce script génère des données de test au format CSV pour toutes les tables du data warehouse.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import logging

from src.utils.config_loader import load_config
from src.utils.logger_config import setup_logger

# Configuration du logger
logger = setup_logger('data_generator', 'data_generator.log')


def ensure_data_directory():
    """
    Crée le répertoire data s'il n'existe pas.
    """
    if not os.path.exists('data'):
        os.makedirs('data')
        logger.info("Répertoire 'data' créé")


def generate_customers(num_customers=100):
    """
    Génère des données aléatoires pour la table des clients.
    
    Args:
        num_customers (int): Nombre de clients à générer
        
    Returns:
        pd.DataFrame: DataFrame contenant les données des clients
    """
    logger.info(f"Génération de {num_customers} clients")
    
    # Liste des villes
    cities = ['Paris', 'Lyon', 'Marseille', 'Bordeaux', 'Lille', 'Toulouse', 'Nantes', 'Strasbourg']
    
    # Générer les données
    customers = {
        'customer_id': range(1, num_customers + 1),
        'customer_name': [f"Client {i}" for i in range(1, num_customers + 1)],
        'email': [f"client{i}@example.com" for i in range(1, num_customers + 1)],
        'phone': [f"+33 {random.randint(100000000, 999999999)}" for _ in range(num_customers)],
        'city': [random.choice(cities) for _ in range(num_customers)],
        'country': ['France'] * num_customers,
        'age': [random.randint(18, 80) for _ in range(num_customers)],
        'gender': [random.choice(['M', 'F']) for _ in range(num_customers)],
        'registration_date': [(datetime.now() - timedelta(days=random.randint(1, 1000))).strftime('%Y-%m-%d') 
                             for _ in range(num_customers)]
    }
    
    return pd.DataFrame(customers)


def generate_products(num_products=50):
    """
    Génère des données aléatoires pour la table des produits.
    
    Args:
        num_products (int): Nombre de produits à générer
        
    Returns:
        pd.DataFrame: DataFrame contenant les données des produits
    """
    logger.info(f"Génération de {num_products} produits")
    
    # Catégories de produits
    categories = ['Électronique', 'Vêtements', 'Alimentation', 'Meubles', 'Livres']
    
    # Générer les données
    products = {
        'product_id': range(1, num_products + 1),
        'product_name': [f"Produit {i}" for i in range(1, num_products + 1)],
        'category': [random.choice(categories) for _ in range(num_products)],
        'price': [round(random.uniform(10, 1000), 2) for _ in range(num_products)],
        'cost': [round(random.uniform(5, 500), 2) for _ in range(num_products)],
        'supplier': [f"Fournisseur {random.randint(1, 10)}" for _ in range(num_products)]
    }
    
    return pd.DataFrame(products)


def generate_stores(num_stores=10):
    """
    Génère des données aléatoires pour la table des magasins.
    
    Args:
        num_stores (int): Nombre de magasins à générer
        
    Returns:
        pd.DataFrame: DataFrame contenant les données des magasins
    """
    logger.info(f"Génération de {num_stores} magasins")
    
    # Régions de France
    regions = ['Ile-de-France', 'Nouvelle-Aquitaine', 'Auvergne-Rhône-Alpes', 'Occitanie', 'Hauts-de-France']
    cities = ['Paris', 'Lyon', 'Marseille', 'Bordeaux', 'Lille', 'Toulouse', 'Nantes', 'Strasbourg']
    
    # Générer les données
    stores = {
        'store_id': range(1, num_stores + 1),
        'store_name': [f"Magasin {i}" for i in range(1, num_stores + 1)],
        'address': [f"{random.randint(1, 100)} Rue de la Paix" for _ in range(num_stores)],
        'city': [random.choice(cities) for _ in range(num_stores)],
        'region': [random.choice(regions) for _ in range(num_stores)],
        'country': ['France'] * num_stores,
        'manager': [f"Gérant {i}" for i in range(1, num_stores + 1)]
    }
    
    return pd.DataFrame(stores)


def generate_time_dim(num_days=100):
    """
    Génère des données pour la dimension temporelle.
    
    Args:
        num_days (int): Nombre de jours à générer
        
    Returns:
        pd.DataFrame: DataFrame contenant les données temporelles
    """
    logger.info(f"Génération de {num_days} jours pour la dimension temporelle")
    
    # Date de début (il y a num_days jours)
    start_date = datetime.now() - timedelta(days=num_days)
    
    # Générer les dates
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    
    # Générer les données
    time_data = {
        'time_id': range(1, num_days + 1),
        'date': [date.strftime('%Y-%m-%d') for date in dates],
        'day': [date.day for date in dates],
        'month': [date.month for date in dates],
        'year': [date.year for date in dates],
        'quarter': [(date.month - 1) // 3 + 1 for date in dates],
        'is_weekend': [(date.weekday() >= 5) for date in dates],
        'is_holiday': [False] * num_days  # Simplifié, pas de vraie logique de jours fériés
    }
    
    return pd.DataFrame(time_data)


def generate_sales(num_sales=500, num_customers=100, num_products=50, num_stores=10, num_days=100):
    """
    Génère des données aléatoires pour la table des ventes.
    
    Args:
        num_sales (int): Nombre de ventes à générer
        num_customers (int): Nombre de clients disponibles
        num_products (int): Nombre de produits disponibles
        num_stores (int): Nombre de magasins disponibles
        num_days (int): Nombre de jours disponibles
        
    Returns:
        pd.DataFrame: DataFrame contenant les données des ventes
    """
    logger.info(f"Génération de {num_sales} ventes")
    
    # Méthodes de paiement
    payment_methods = ['Carte bancaire', 'Espèces', 'Chèque', 'Virement']
    
    # Générer les données
    sales = {
        'sale_id': range(1, num_sales + 1),
        'date_id': [random.randint(1, num_days) for _ in range(num_sales)],
        'customer_id': [random.randint(1, num_customers) for _ in range(num_sales)],
        'product_id': [random.randint(1, num_products) for _ in range(num_sales)],
        'store_id': [random.randint(1, num_stores) for _ in range(num_sales)],
        'quantity': [random.randint(1, 10) for _ in range(num_sales)],
        'unit_price': [round(random.uniform(10, 1000), 2) for _ in range(num_sales)],
        'total_amount': [round(random.uniform(10, 10000), 2) for _ in range(num_sales)],
        'discount': [round(random.uniform(0, 0.5), 2) for _ in range(num_sales)],
        'payment_method': [random.choice(payment_methods) for _ in range(num_sales)]
    }
    
    return pd.DataFrame(sales)


def generate_and_save_data():
    """
    Génère toutes les données et les sauvegarde dans des fichiers CSV.
    """
    logger.info("Début de la génération des données")
    
    # S'assurer que le répertoire data existe
    ensure_data_directory()
    
    # Charger la configuration
    config = load_config()
    gen_config = config.get('generators', {})
    
    # Paramètres de génération
    num_customers = gen_config.get('num_customers', 100)
    num_products = gen_config.get('num_products', 50)
    num_stores = gen_config.get('num_stores', 10)
    num_days = gen_config.get('num_days', 100)
    num_sales = gen_config.get('num_sales', 500)
    
    # Générer les données
    customers_df = generate_customers(num_customers)
    products_df = generate_products(num_products)
    stores_df = generate_stores(num_stores)
    time_df = generate_time_dim(num_days)
    sales_df = generate_sales(num_sales, num_customers, num_products, num_stores, num_days)
    
    # Sauvegarder les données dans des fichiers CSV
    customers_df.to_csv('data/customers_dim.csv', index=False, encoding='utf-8')
    products_df.to_csv('data/products_dim.csv', index=False, encoding='utf-8')
    stores_df.to_csv('data/stores_dim.csv', index=False, encoding='utf-8')
    time_df.to_csv('data/time_dim.csv', index=False, encoding='utf-8')
    sales_df.to_csv('data/sales_fact.csv', index=False, encoding='utf-8')
    
    logger.info("Génération des données terminée")
    logger.info(f"Fichiers générés : {num_customers} clients, {num_products} produits, {num_stores} magasins, {num_days} jours, {num_sales} ventes")
    
    return {
        'customers': len(customers_df),
        'products': len(products_df),
        'stores': len(stores_df),
        'time': len(time_df),
        'sales': len(sales_df)
    }


if __name__ == "__main__":
    stats = generate_and_save_data()
    print("\n✅ Génération des données terminée")
    print(f"📊 Statistiques:")
    print(f"  - Clients: {stats['customers']}")
    print(f"  - Produits: {stats['products']}")
    print(f"  - Magasins: {stats['stores']}")
    print(f"  - Jours: {stats['time']}")
    print(f"  - Ventes: {stats['sales']}")
    print("\nLes fichiers ont été enregistrés dans le répertoire 'data/'.") 