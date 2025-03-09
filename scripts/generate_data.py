import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os
import sys

# Ajout du répertoire parent au chemin de recherche Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Chemins des dossiers dans la nouvelle structure
DATA_DIR = 'data'
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')

# Création du dossier data s'il n'existe pas
if not os.path.exists(RAW_DATA_DIR):
    os.makedirs(RAW_DATA_DIR)

# Initialisation de Faker
fake = Faker('fr_FR')
random.seed(42)
np.random.seed(42)

# Paramètres
nb_customers = 1000  
nb_products = 500
nb_stores = 500
start_date = datetime(2022, 1, 1)
end_date = datetime(2023, 12, 31)
nb_sales = 500

# Génération des données pour Customers_Dim
def generate_customers():
    customers = []
    for i in range(1, nb_customers + 1):
        customers.append({
            'customer_id': i,
            'customer_name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'city': fake.city(),
            'country': 'France',
            'age': random.randint(18, 80),
            'gender': random.choice(['M', 'F']),
            'registration_date': fake.date_between(start_date=start_date, end_date=end_date)
        })
    return pd.DataFrame(customers)

# Génération des données pour Products_Dim
def generate_products():
    categories = ['Électronique', 'Vêtements', 'Alimentation', 'Maison', 'Jardin', 
                 'Beauté', 'Sports', 'Jouets', 'Livres', 'Musique']
    
    products = []
    for i in range(1, nb_products + 1):
        category = random.choice(categories)
        products.append({
            'product_id': i,
            'product_name': f"{category} {fake.word().capitalize()} {i}",
            'category': category,
            'subcategory': fake.word().capitalize(),
            'price': round(random.uniform(5.99, 999.99), 2),
            'cost': round(random.uniform(1.99, 499.99), 2),
            'supplier': fake.company()
        })
    return pd.DataFrame(products)

# Génération des données pour Store_Dim
def generate_stores():
    stores = []
    for i in range(1, nb_stores + 1):
        stores.append({
            'store_id': i,
            'store_name': f"Magasin {fake.city()}",
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'region': fake.region(),
            'country': 'France',
            'postal_code': fake.postcode(),
            'manager': fake.name(),
            'opening_date': fake.date_between(start_date=datetime(2010, 1, 1), end_date=start_date)
        })
    return pd.DataFrame(stores)

# Génération des données pour Time_Dim
def generate_time_dim():
    time_data = []
    current_date = start_date
    
    while current_date <= end_date:
        time_data.append({
            'date_id': current_date.strftime('%Y%m%d'),
            'date': current_date,
            'day': current_date.day,
            'month': current_date.month,
            'year': current_date.year,
            'quarter': (current_date.month - 1) // 3 + 1,
            'day_of_week': current_date.weekday(),
            'day_name': current_date.strftime('%A'),
            'month_name': current_date.strftime('%B'),
            'is_weekend': 1 if current_date.weekday() >= 5 else 0,
            'is_holiday': 0  # Simplifié, à compléter avec les jours fériés réels
        })
        current_date += timedelta(days=1)
    
    return pd.DataFrame(time_data)

# Génération des données pour Sales_Fact
def generate_sales(customers_df, products_df, stores_df, time_df):
    sales = []
    
    # Conversion des dates en format date_id
    date_ids = time_df['date_id'].tolist()
    
    for i in range(1, nb_sales + 1):
        # Sélection aléatoire des clés étrangères
        customer_id = random.choice(customers_df['customer_id'].tolist())
        product_id = random.choice(products_df['product_id'].tolist())
        store_id = random.choice(stores_df['store_id'].tolist())
        date_id = random.choice(date_ids)
        
        # Récupération du prix du produit
        product_price = products_df.loc[products_df['product_id'] == product_id, 'price'].values[0]
        product_cost = products_df.loc[products_df['product_id'] == product_id, 'cost'].values[0]
        
        # Calcul des quantités et montants
        quantity = random.randint(1, 10)
        discount_pct = random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2])
        unit_price = round(product_price * (1 - discount_pct), 2)
        total_amount = round(quantity * unit_price, 2)
        profit = round(total_amount - (quantity * product_cost), 2)
        
        sales.append({
            'sale_id': i,
            'date_id': date_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'store_id': store_id,
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_pct': discount_pct,
            'total_amount': total_amount,
            'profit': profit,
            'payment_method': random.choice(['Carte', 'Espèces', 'Virement', 'PayPal'])
        })
    
    return pd.DataFrame(sales)

# Génération et sauvegarde des données
def main():
    print("Génération des données...")
    
    # Génération des tables dimensionnelles
    customers_df = generate_customers()
    products_df = generate_products()
    stores_df = generate_stores()
    time_df = generate_time_dim()
    
    # Génération de la table de faits
    sales_df = generate_sales(customers_df, products_df, stores_df, time_df)
    
    # Sauvegarde des données en CSV
    customers_df.to_csv(os.path.join(RAW_DATA_DIR, 'customers_dim.csv'), index=False)
    products_df.to_csv(os.path.join(RAW_DATA_DIR, 'products_dim.csv'), index=False)
    stores_df.to_csv(os.path.join(RAW_DATA_DIR, 'stores_dim.csv'), index=False)
    time_df.to_csv(os.path.join(RAW_DATA_DIR, 'time_dim.csv'), index=False)
    sales_df.to_csv(os.path.join(RAW_DATA_DIR, 'sales_fact.csv'), index=False)
    
    print(f"Génération des données terminée. Fichiers sauvegardés dans le dossier '{RAW_DATA_DIR}'.")

if __name__ == "__main__":
    main() 