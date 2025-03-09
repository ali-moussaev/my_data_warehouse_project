"""
Pipeline ETL simplifié pour le data warehouse.
"""

import logging
import time
import traceback
from typing import Dict, Any, Optional

import pandas as pd
from sqlalchemy import create_engine

from src.utils.config_loader import get_sqlalchemy_url, load_config
from src.utils.logger_config import setup_logger

# Configuration du logger
logger = setup_logger('etl_pipeline', 'etl.log')

class SimpleETL:
    """
    Version simplifiée du pipeline ETL pour le data warehouse.
    """
    
    def __init__(self):
        """
        Initialise le pipeline ETL avec une configuration simple.
        """
        # Charger la configuration
        self.config = load_config().get('etl', {})
        
        # Créer la connexion à la base de données
        self.engine = create_engine(get_sqlalchemy_url())
        
        # Paramètres de configuration
        self.schema = 'public'
        self.if_exists = self.config.get('if_exists', 'append')
        
        # Mappage des noms de tables
        self.table_mapping = {
            'customers': 'customers_dim',
            'products': 'products_dim',
            'stores': 'stores_dim',
            'time': 'time_dim',
            'sales': 'sales_fact'
        }
        
        logger.info(f"Pipeline ETL initialisé avec: if_exists={self.if_exists}")
    
    def extract(self) -> Dict[str, pd.DataFrame]:
        """
        Extrait les données des fichiers CSV.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire de DataFrames extraits
        """
        logger.info("Début de l'extraction des données")
        
        # Récupérer les chemins des fichiers depuis la configuration
        data_paths = self.config.get('data_paths', {
            'customers': 'data/raw/customers_dim.csv',
            'products': 'data/raw/products_dim.csv',
            'stores': 'data/raw/stores_dim.csv',
            'time': 'data/raw/time_dim.csv',
            'sales': 'data/raw/sales_fact.csv'
        })
        
        data = {}
        
        # Liste des encodages à essayer
        encodings = ['utf-8', 'latin1', 'cp1252', 'ISO-8859-1']
        
        # Extraire chaque fichier CSV
        for table_name, file_path in data_paths.items():
            logger.info(f"Lecture du fichier {file_path}")
            
            # Essayer différents encodages
            for encoding in encodings:
                try:
                    # Tenter de lire le fichier avec l'encodage courant
                    df = pd.read_csv(file_path, encoding=encoding)
                    data[table_name] = df
                    logger.info(f"  - {len(df)} enregistrements extraits de {table_name} (encodage: {encoding})")
                    break  # Sortir de la boucle si la lecture réussit
                except UnicodeDecodeError:
                    # Continuer avec le prochain encodage
                    continue
                except FileNotFoundError:
                    logger.warning(f"Fichier {file_path} non trouvé, table {table_name} ignorée")
                    data[table_name] = pd.DataFrame()  # Dataframe vide
                    break
                except Exception as e:
                    logger.error(f"Erreur lors de l'extraction de {file_path}: {e}")
                    data[table_name] = pd.DataFrame()  # Dataframe vide
                    break
            else:
                # Cette partie s'exécute si aucun encodage n'a fonctionné
                logger.error(f"Impossible de lire {file_path} avec les encodages disponibles")
                data[table_name] = pd.DataFrame()  # Dataframe vide
        
        logger.info("Extraction des données terminée")
        return data
    
    def transform(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transforme les données extraites.
        
        Args:
            data (Dict[str, pd.DataFrame]): Données extraites
            
        Returns:
            Dict[str, pd.DataFrame]: Données transformées
        """
        logger.info("Début de la transformation des données")
        
        # Copier les données pour éviter de modifier les originaux
        transformed_data = {k: v.copy() for k, v in data.items()}
        
        # Transformation simple: convertir les colonnes de dates
        if not transformed_data['customers'].empty and 'registration_date' in transformed_data['customers'].columns:
            try:
                transformed_data['customers']['registration_date'] = pd.to_datetime(
                    transformed_data['customers']['registration_date']
                )
                logger.info("Conversion des dates d'enregistrement des clients réussie")
            except Exception as e:
                logger.error(f"Erreur lors de la conversion des dates: {e}")
        
        # Transformation simple: convertir les colonnes de dates pour time_dim
        if not transformed_data['time'].empty and 'date' in transformed_data['time'].columns:
            try:
                transformed_data['time']['date'] = pd.to_datetime(
                    transformed_data['time']['date']
                )
                logger.info("Conversion des dates dans time_dim réussie")
            except Exception as e:
                logger.error(f"Erreur lors de la conversion des dates dans time_dim: {e}")
        
        logger.info("Transformation des données terminée")
        return transformed_data
    
    def load(self, data: Dict[str, pd.DataFrame]) -> int:
        """
        Charge les données transformées dans des fichiers CSV.
        
        Args:
            data (Dict[str, pd.DataFrame]): Données transformées
            
        Returns:
            int: Nombre total d'enregistrements chargés
        """
        logger.info(f"Début du chargement des données (mode: {self.if_exists})")
        
        # Créer le chargeur de fichiers
        from src.etl.loaders import FileSystemLoader
        loader = FileSystemLoader(if_exists=self.if_exists)
        
        # Charger les données avec le chargeur de fichiers
        total_records = loader.load(data)
        
        logger.info(f"Chargement des données terminé: {total_records} enregistrements au total")
        return total_records
    
    def run(self) -> bool:
        """
        Exécute le pipeline ETL complet: extraction, transformation, chargement.
        
        Returns:
            bool: True si le pipeline s'est exécuté avec succès, False sinon
        """
        logger.info("Démarrage du pipeline ETL")
        start_time = time.time()
        
        try:
            # Extraction
            data = self.extract()
            
            # Transformation
            transformed_data = self.transform(data)
            
            # Chargement
            records = self.load(transformed_data)
            
            duration = time.time() - start_time
            logger.info(f"Pipeline ETL terminé en {duration:.2f} secondes")
            logger.info(f"Total: {records} enregistrements chargés")
            
            return True
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution du pipeline ETL: {e}")
            logger.error(traceback.format_exc())
            return False


def run_etl_pipeline() -> bool:
    """
    Fonction principale pour exécuter le pipeline ETL.
    
    Returns:
        bool: True si le pipeline s'est exécuté avec succès, False sinon
    """
    logger.info("Exécution du pipeline ETL")
    
    try:
        # Créer et exécuter le pipeline
        etl = SimpleETL()
        success = etl.run()
        
        if success:
            logger.info("Pipeline ETL exécuté avec succès")
        else:
            logger.error("Échec de l'exécution du pipeline ETL")
        
        return success
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline ETL: {e}")
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    # Exécuter le pipeline si ce script est lancé directement
    import argparse
    
    parser = argparse.ArgumentParser(description="Exécute le pipeline ETL")
    args = parser.parse_args()
    
    success = run_etl_pipeline()
    
    if success:
        print("✅ Pipeline ETL exécuté avec succès")
    else:
        print("❌ Échec de l'exécution du pipeline ETL") 