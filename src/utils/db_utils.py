"""
Utilitaires pour la gestion de la base de données.
"""

import logging
import sqlalchemy
from sqlalchemy import create_engine, text
import psycopg2

from src.utils.config_loader import load_config
from src.utils.logger_config import setup_logger

# Configuration du logger
logger = setup_logger('db_utils', 'db_utils.log')


def init_database():
    """
    Initialise la base de données: vérifie la connexion, crée la base si elle n'existe pas,
    crée les tables de base.
    
    Returns:
        bool: True si l'initialisation a réussi, False sinon
    """
    try:
        # Récupérer la configuration
        config = load_config()
        db_config = config.get('database', {})
        
        # Paramètres de connexion
        db_engine = db_config.get('engine', 'postgresql')
        db_user = db_config.get('user', 'postgres')
        db_password = db_config.get('password', 'postgres')
        db_host = db_config.get('host', 'localhost')
        db_port = db_config.get('port', 5432)
        db_name = db_config.get('dbname', 'data_warehouse')
        db_schema = db_config.get('schema', 'public')
        
        # Se connecter d'abord au serveur sans spécifier de base de données
        conn_params = {
            'host': db_host,
            'port': db_port,
            'user': db_user,
            'password': db_password
        }
        
        logger.info(f"Connexion au serveur PostgreSQL: {db_host}:{db_port}")
        
        try:
            conn = psycopg2.connect(**conn_params)
            conn.autocommit = True
            
            # Vérifier si la base de données existe
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                exists = cur.fetchone()
                
                if not exists:
                    logger.info(f"La base de données {db_name} n'existe pas. Création...")
                    cur.execute(f"CREATE DATABASE {db_name}")
                    logger.info(f"Base de données {db_name} créée avec succès")
            
            conn.close()
            
            # Se connecter à la base de données et créer les tables
            engine_url = f"{db_engine}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            engine = create_engine(engine_url)
            
            with engine.connect() as conn:
                # Vérifier si les tables existent déjà
                inspector = sqlalchemy.inspect(engine)
                existing_tables = inspector.get_table_names(schema=db_schema)
                
                if 'customers_dim' not in existing_tables:
                    logger.info("Création des tables dimensionnelles et de faits")
                    
                    # Création des tables dimensionnelles
                    conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {db_schema}.customers_dim (
                        customer_id INTEGER PRIMARY KEY,
                        customer_name VARCHAR(200),
                        email VARCHAR(200),
                        phone VARCHAR(50),
                        city VARCHAR(100),
                        country VARCHAR(100),
                        age INTEGER,
                        gender CHAR(1),
                        registration_date DATE
                    )
                    """))
                    
                    conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {db_schema}.products_dim (
                        product_id INTEGER PRIMARY KEY,
                        product_name VARCHAR(200),
                        category VARCHAR(100),
                        price NUMERIC(10, 2),
                        cost NUMERIC(10, 2),
                        supplier VARCHAR(200)
                    )
                    """))
                    
                    conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {db_schema}.stores_dim (
                        store_id INTEGER PRIMARY KEY,
                        store_name VARCHAR(200),
                        address VARCHAR(200),
                        city VARCHAR(100),
                        region VARCHAR(100),
                        country VARCHAR(100),
                        manager VARCHAR(200)
                    )
                    """))
                    
                    conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {db_schema}.time_dim (
                        time_id INTEGER PRIMARY KEY,
                        date DATE,
                        day INTEGER,
                        month INTEGER,
                        year INTEGER,
                        quarter INTEGER,
                        is_weekend BOOLEAN,
                        is_holiday BOOLEAN
                    )
                    """))
                    
                    # Création de la table de faits
                    conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {db_schema}.sales_fact (
                        sale_id INTEGER PRIMARY KEY,
                        date_id INTEGER REFERENCES {db_schema}.time_dim(time_id),
                        customer_id INTEGER REFERENCES {db_schema}.customers_dim(customer_id),
                        product_id INTEGER REFERENCES {db_schema}.products_dim(product_id),
                        store_id INTEGER REFERENCES {db_schema}.stores_dim(store_id),
                        quantity INTEGER,
                        unit_price NUMERIC(10, 2),
                        total_amount NUMERIC(12, 2),
                        discount NUMERIC(4, 2),
                        payment_method VARCHAR(50)
                    )
                    """))
                    
                    # Création de la table de log ETL
                    conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {db_schema}.etl_log (
                        log_id SERIAL PRIMARY KEY,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        status VARCHAR(50),
                        records_processed INTEGER,
                        error_message TEXT
                    )
                    """))
                    
                    logger.info("Tables créées avec succès")
                else:
                    logger.info("Les tables existent déjà")
                
                conn.commit()
            
            logger.info("Initialisation de la base de données terminée avec succès")
            return True
        
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation de la base de données: {e}")
            return False
    
    except Exception as e:
        logger.error(f"Erreur lors de l'initialisation de la base de données: {e}")
        return False


if __name__ == "__main__":
    success = init_database()
    if success:
        print("✅ Base de données initialisée avec succès")
    else:
        print("❌ Erreur lors de l'initialisation de la base de données") 