"""
Script unifié de création du schéma de base de données pour le Data Warehouse.
Ce script remplace les trois scripts précédents (create_schema.py, create_schema_direct.py, create_schema_simple.py)
et offre différentes options d'exécution.
"""

import os
import sys
import argparse
import logging
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Ajouter le répertoire parent au chemin de recherche Python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Importer les fonctions de configuration
from src.utils.config_loader import get_db_config, get_sqlalchemy_url
from src.utils.logger_config import setup_logger

# Configuration du logger
logger = setup_logger('schema_creator', 'schema_creation.log')

# Base déclarative pour SQLAlchemy ORM
Base = declarative_base()

# Définition des modèles SQLAlchemy
class CustomersDim(Base):
    __tablename__ = 'customers_dim'
    
    customer_id = Column(Integer, primary_key=True)
    customer_name = Column(String, nullable=False)
    email = Column(String)
    phone = Column(String)
    city = Column(String)
    country = Column(String)
    age = Column(Integer)
    gender = Column(String)
    registration_date = Column(Date)

class ProductsDim(Base):
    __tablename__ = 'products_dim'
    
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String, nullable=False)
    category = Column(String)
    subcategory = Column(String)
    price = Column(Float)
    cost = Column(Float)
    supplier = Column(String)

class StoresDim(Base):
    __tablename__ = 'stores_dim'
    
    store_id = Column(Integer, primary_key=True)
    store_name = Column(String, nullable=False)
    address = Column(String)
    city = Column(String)
    region = Column(String)
    country = Column(String)
    postal_code = Column(String)
    manager = Column(String)
    opening_date = Column(Date)

class TimeDim(Base):
    __tablename__ = 'time_dim'
    
    date_id = Column(String, primary_key=True)
    date = Column(Date)
    day = Column(Integer)
    month = Column(Integer)
    year = Column(Integer)
    quarter = Column(Integer)
    day_of_week = Column(Integer)
    day_name = Column(String)
    month_name = Column(String)
    is_weekend = Column(Integer)
    is_holiday = Column(Integer)

class SalesFact(Base):
    __tablename__ = 'sales_fact'
    
    sale_id = Column(Integer, primary_key=True)
    date_id = Column(String, ForeignKey('time_dim.date_id'))
    customer_id = Column(Integer, ForeignKey('customers_dim.customer_id'))
    product_id = Column(Integer, ForeignKey('products_dim.product_id'))
    store_id = Column(Integer, ForeignKey('stores_dim.store_id'))
    quantity = Column(Integer)
    unit_price = Column(Float)
    discount_pct = Column(Float)
    total_amount = Column(Float)
    profit = Column(Float)
    payment_method = Column(String)

class ETLLog(Base):
    __tablename__ = 'etl_log'
    
    etl_id = Column(Integer, primary_key=True)
    start_time = Column(Date)
    end_time = Column(Date)
    status = Column(String)
    records_processed = Column(Integer)
    error_message = Column(String)

def create_schema_orm(engine):
    """Crée le schéma en utilisant SQLAlchemy ORM"""
    logger.info("Création du schéma avec SQLAlchemy ORM")
    try:
        # Création de toutes les tables
        Base.metadata.create_all(engine)
        logger.info("Schéma créé avec succès via SQLAlchemy ORM")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la création du schéma via ORM: {e}")
        return False

def create_schema_core(engine):
    """Crée le schéma en utilisant SQLAlchemy Core"""
    logger.info("Création du schéma avec SQLAlchemy Core")
    try:
        metadata = MetaData()
        
        # Table Customers_Dim
        Table('customers_dim', metadata,
            Column('customer_id', Integer, primary_key=True),
            Column('customer_name', String, nullable=False),
            Column('email', String),
            Column('phone', String),
            Column('city', String),
            Column('country', String),
            Column('age', Integer),
            Column('gender', String),
            Column('registration_date', Date)
        )
        
        # Table Products_Dim
        Table('products_dim', metadata,
            Column('product_id', Integer, primary_key=True),
            Column('product_name', String, nullable=False),
            Column('category', String),
            Column('subcategory', String),
            Column('price', Float),
            Column('cost', Float),
            Column('supplier', String)
        )
        
        # Table Stores_Dim
        Table('stores_dim', metadata,
            Column('store_id', Integer, primary_key=True),
            Column('store_name', String, nullable=False),
            Column('address', String),
            Column('city', String),
            Column('region', String),
            Column('country', String),
            Column('postal_code', String),
            Column('manager', String),
            Column('opening_date', Date)
        )
        
        # Table Time_Dim
        Table('time_dim', metadata,
            Column('date_id', String, primary_key=True),
            Column('date', Date),
            Column('day', Integer),
            Column('month', Integer),
            Column('year', Integer),
            Column('quarter', Integer),
            Column('day_of_week', Integer),
            Column('day_name', String),
            Column('month_name', String),
            Column('is_weekend', Integer),
            Column('is_holiday', Integer)
        )
        
        # Table Sales_Fact
        Table('sales_fact', metadata,
            Column('sale_id', Integer, primary_key=True),
            Column('date_id', String, ForeignKey('time_dim.date_id')),
            Column('customer_id', Integer, ForeignKey('customers_dim.customer_id')),
            Column('product_id', Integer, ForeignKey('products_dim.product_id')),
            Column('store_id', Integer, ForeignKey('stores_dim.store_id')),
            Column('quantity', Integer),
            Column('unit_price', Float),
            Column('discount_pct', Float),
            Column('total_amount', Float),
            Column('profit', Float),
            Column('payment_method', String)
        )
        
        # Table ETL_Log
        Table('etl_log', metadata,
            Column('etl_id', Integer, primary_key=True),
            Column('start_time', Date),
            Column('end_time', Date),
            Column('status', String),
            Column('records_processed', Integer),
            Column('error_message', String)
        )
        
        # Création des tables
        metadata.create_all(engine)
        logger.info("Schéma créé avec succès via SQLAlchemy Core")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la création du schéma via Core: {e}")
        return False

def create_schema_direct(engine):
    """Crée le schéma en utilisant des requêtes SQL directes"""
    logger.info("Création du schéma avec des requêtes SQL directes")
    try:
        with engine.connect() as conn:
            # Table Customers_Dim
            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS customers_dim (
                customer_id INTEGER PRIMARY KEY,
                customer_name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                city TEXT,
                country TEXT,
                age INTEGER,
                gender TEXT,
                registration_date DATE
            )
            '''))
            
            # Table Products_Dim
            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS products_dim (
                product_id INTEGER PRIMARY KEY,
                product_name TEXT NOT NULL,
                category TEXT,
                subcategory TEXT,
                price NUMERIC(10, 2),
                cost NUMERIC(10, 2),
                supplier TEXT
            )
            '''))
            
            # Table Stores_Dim
            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS stores_dim (
                store_id INTEGER PRIMARY KEY,
                store_name TEXT NOT NULL,
                address TEXT,
                city TEXT,
                region TEXT,
                country TEXT,
                postal_code TEXT,
                manager TEXT,
                opening_date DATE
            )
            '''))
            
            # Table Time_Dim
            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS time_dim (
                date_id TEXT PRIMARY KEY,
                date DATE,
                day INTEGER,
                month INTEGER,
                year INTEGER,
                quarter INTEGER,
                day_of_week INTEGER,
                day_name TEXT,
                month_name TEXT,
                is_weekend INTEGER,
                is_holiday INTEGER
            )
            '''))
            
            # Table Sales_Fact
            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS sales_fact (
                sale_id INTEGER PRIMARY KEY,
                date_id TEXT,
                customer_id INTEGER,
                product_id INTEGER,
                store_id INTEGER,
                quantity INTEGER,
                unit_price NUMERIC(10, 2),
                discount_pct NUMERIC(4, 2),
                total_amount NUMERIC(10, 2),
                profit NUMERIC(10, 2),
                payment_method TEXT,
                FOREIGN KEY (date_id) REFERENCES time_dim(date_id),
                FOREIGN KEY (customer_id) REFERENCES customers_dim(customer_id),
                FOREIGN KEY (product_id) REFERENCES products_dim(product_id),
                FOREIGN KEY (store_id) REFERENCES stores_dim(store_id)
            )
            '''))
            
            # Table ETL_Log
            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS etl_log (
                etl_id SERIAL PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status TEXT,
                records_processed INTEGER,
                error_message TEXT
            )
            '''))
            
            # Création d'index pour améliorer les performances
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_sales_date ON sales_fact(date_id)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales_fact(customer_id)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_sales_product ON sales_fact(product_id)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_sales_store ON sales_fact(store_id)'))
            
            # Accorder les droits nécessaires sur les tables
            # Récupérer la configuration de la base de données
            db_config = get_db_config()
            
            # Si un utilisateur d'application est défini, lui accorder les droits
            if db_config.get('app_user'):
                app_user = db_config['app_user']
                logger.info(f"Attribution des droits à l'utilisateur {app_user}")
                
                tables = [
                    'customers_dim', 'products_dim', 'stores_dim', 
                    'time_dim', 'sales_fact', 'etl_log'
                ]
                
                for table in tables:
                    conn.execute(text(f"GRANT ALL PRIVILEGES ON TABLE {table} TO {app_user}"))
                
                # Accorder les droits sur les séquences
                conn.execute(text(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {app_user}"))
            
            # Accorder les droits à l'utilisateur courant
            # Récupérer l'utilisateur courant
            current_user = conn.execute(text("SELECT current_user")).scalar()
            logger.info(f"Attribution des droits à l'utilisateur courant {current_user}")
            
            tables = [
                'customers_dim', 'products_dim', 'stores_dim', 
                'time_dim', 'sales_fact', 'etl_log'
            ]
            
            for table in tables:
                conn.execute(text(f"GRANT ALL PRIVILEGES ON TABLE {table} TO {current_user}"))
            
            # Accorder les droits sur les séquences
            conn.execute(text(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {current_user}"))
            
            logger.info("Schéma créé avec succès via SQL direct")
            return True
    except Exception as e:
        logger.error(f"Erreur lors de la création du schéma via SQL direct: {e}")
        return False

def main():
    """Fonction principale qui crée le schéma selon le mode spécifié"""
    parser = argparse.ArgumentParser(description='Création du schéma de base de données pour le Data Warehouse')
    parser.add_argument('--mode', choices=['orm', 'core', 'direct'], default='direct',
                        help='Mode de création du schéma: orm, core ou direct (défaut: direct)')
    args = parser.parse_args()
    
    try:
        # Connexion à la base de données
        engine = create_engine(get_sqlalchemy_url())
        
        # Création du schéma selon le mode spécifié
        if args.mode == 'orm':
            success = create_schema_orm(engine)
        elif args.mode == 'core':
            success = create_schema_core(engine)
        else:  # mode direct
            success = create_schema_direct(engine)
        
        if success:
            print("✅ Schéma de base de données créé avec succès.")
            return 0
        else:
            print("❌ Erreur lors de la création du schéma de base de données.")
            return 1
    except Exception as e:
        logger.exception(f"Erreur non gérée: {e}")
        print(f"❌ Erreur non gérée: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())