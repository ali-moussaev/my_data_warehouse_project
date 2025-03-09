"""
Implémentations des extracteurs de données pour le pipeline ETL.
"""

import os
import pandas as pd
from typing import Dict, Generator, List, Optional
import logging

from src.etl.interfaces import DataExtractor
from src.utils.config_loader import get_path
from src.utils.logger_config import setup_logger

# Configuration du logger
logger = setup_logger('data_extractors', 'extractors.log')


class CSVDataExtractor(DataExtractor):
    """Extracteur de données depuis des fichiers CSV."""
    
    def __init__(self, data_dir: Optional[str] = None, file_mapping: Optional[Dict[str, str]] = None):
        """
        Initialise l'extracteur de données CSV.
        
        Args:
            data_dir (str, optional): Répertoire contenant les fichiers CSV
            file_mapping (Dict[str, str], optional): Mapping entre les noms de tables et les noms de fichiers
        """
        self.data_dir = data_dir or get_path('data_dir') or 'data'
        
        # Mapping par défaut entre les noms de tables et les noms de fichiers
        self.file_mapping = file_mapping or {
            'customers': 'customers_dim.csv',
            'products': 'products_dim.csv',
            'stores': 'stores_dim.csv',
            'time': 'time_dim.csv',
            'sales': 'sales_fact.csv'
        }
        
        # Vérifier que le répertoire existe
        if not os.path.exists(self.data_dir):
            logger.warning(f"Le répertoire {self.data_dir} n'existe pas. Il sera créé si nécessaire.")
            os.makedirs(self.data_dir, exist_ok=True)
    
    def _get_file_path(self, table_name: str) -> str:
        """
        Obtient le chemin complet d'un fichier CSV.
        
        Args:
            table_name (str): Nom de la table
            
        Returns:
            str: Chemin complet du fichier CSV
        """
        if table_name not in self.file_mapping:
            raise ValueError(f"Table inconnue: {table_name}")
        
        return os.path.join(self.data_dir, self.file_mapping[table_name])
    
    def _check_files_exist(self) -> List[str]:
        """
        Vérifie que tous les fichiers CSV existent.
        
        Returns:
            List[str]: Liste des fichiers manquants
        """
        missing_files = []
        for table_name in self.file_mapping:
            file_path = self._get_file_path(table_name)
            if not os.path.exists(file_path):
                missing_files.append(file_path)
        
        return missing_files
    
    def extract(self) -> Dict[str, pd.DataFrame]:
        """
        Extrait toutes les données depuis les fichiers CSV.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire de DataFrames extraits
        
        Raises:
            FileNotFoundError: Si un fichier CSV est manquant
        """
        logger.info("Début de l'extraction des données depuis les fichiers CSV")
        
        # Vérifier que tous les fichiers existent
        missing_files = self._check_files_exist()
        if missing_files:
            error_msg = f"Fichiers manquants: {', '.join(missing_files)}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        # Extraire les données
        data = {}
        for table_name in self.file_mapping:
            file_path = self._get_file_path(table_name)
            logger.info(f"Extraction de {file_path}...")
            
            try:
                data[table_name] = pd.read_csv(file_path)
                logger.info(f"  - {len(data[table_name])} enregistrements extraits de {table_name}")
            except Exception as e:
                logger.error(f"Erreur lors de l'extraction de {file_path}: {e}")
                raise
        
        logger.info("Extraction des données terminée avec succès")
        return data
    
    def extract_batch(self, batch_size: int = 1000) -> Generator[Dict[str, pd.DataFrame], None, None]:
        """
        Extrait les données par lots depuis les fichiers CSV.
        
        Args:
            batch_size (int): Taille des lots
            
        Returns:
            Generator[Dict[str, pd.DataFrame], None, None]: Générateur de lots de données
            
        Raises:
            FileNotFoundError: Si un fichier CSV est manquant
        """
        logger.info(f"Début de l'extraction des données par lots (taille: {batch_size}) depuis les fichiers CSV")
        
        # Vérifier que tous les fichiers existent
        missing_files = self._check_files_exist()
        if missing_files:
            error_msg = f"Fichiers manquants: {', '.join(missing_files)}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        # Pour chaque table, créer un itérateur de lecture par lots
        readers = {}
        for table_name in self.file_mapping:
            file_path = self._get_file_path(table_name)
            logger.info(f"Préparation de l'extraction par lots de {file_path}...")
            
            try:
                # Utiliser chunksize pour lire par lots
                readers[table_name] = pd.read_csv(file_path, chunksize=batch_size)
            except Exception as e:
                logger.error(f"Erreur lors de la préparation de l'extraction par lots de {file_path}: {e}")
                raise
        
        # Extraire les données par lots
        # Note: Nous devons gérer le cas où certaines tables ont plus de données que d'autres
        has_more_data = True
        batch_num = 0
        
        while has_more_data:
            batch_num += 1
            batch_data = {}
            has_more_data = False
            
            for table_name, reader in readers.items():
                try:
                    # Essayer de lire le prochain lot
                    batch_data[table_name] = next(reader)
                    has_more_data = True
                    logger.debug(f"Lot {batch_num}: {len(batch_data[table_name])} enregistrements extraits de {table_name}")
                except StopIteration:
                    # Plus de données pour cette table
                    # Si c'est la première fois qu'on traite cette table, on met un DataFrame vide
                    if batch_num == 1:
                        logger.warning(f"Aucune donnée trouvée dans {table_name}")
                        batch_data[table_name] = pd.DataFrame()
                    else:
                        # Sinon, on réutilise le dernier lot (vide)
                        batch_data[table_name] = pd.DataFrame()
                except Exception as e:
                    logger.error(f"Erreur lors de l'extraction du lot {batch_num} de {table_name}: {e}")
                    raise
            
            if has_more_data:
                logger.info(f"Lot {batch_num} extrait avec succès")
                yield batch_data
            
        logger.info(f"Extraction par lots terminée: {batch_num-1} lots extraits")


class DatabaseExtractor(DataExtractor):
    """Extracteur de données depuis une base de données."""
    
    def __init__(self, engine=None, schema: str = 'public'):
        """
        Initialise l'extracteur de données depuis une base de données.
        
        Args:
            engine: Moteur SQLAlchemy
            schema (str): Schéma de la base de données
        """
        from sqlalchemy import create_engine
        from src.utils.config_loader import get_sqlalchemy_url
        
        self.engine = engine or create_engine(get_sqlalchemy_url())
        self.schema = schema
        self.logger = setup_logger('db_extractor', 'db_extractor.log')
    
    def extract(self) -> Dict[str, pd.DataFrame]:
        """
        Extrait toutes les données depuis la base de données.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire de DataFrames extraits
        """
        self.logger.info("Début de l'extraction des données depuis la base de données")
        
        tables = {
            'customers': 'customers_dim',
            'products': 'products_dim',
            'stores': 'stores_dim',
            'time': 'time_dim',
            'sales': 'sales_fact'
        }
        
        data = {}
        for key, table_name in tables.items():
            self.logger.info(f"Extraction de {table_name}...")
            try:
                query = f"SELECT * FROM {self.schema}.{table_name}"
                data[key] = pd.read_sql(query, self.engine)
                self.logger.info(f"  - {len(data[key])} enregistrements extraits de {table_name}")
            except Exception as e:
                self.logger.error(f"Erreur lors de l'extraction de {table_name}: {e}")
                raise
        
        self.logger.info("Extraction des données terminée avec succès")
        return data
    
    def extract_batch(self, batch_size: int = 1000) -> Generator[Dict[str, pd.DataFrame], None, None]:
        """
        Extrait les données par lots depuis la base de données.
        
        Args:
            batch_size (int): Taille des lots
            
        Returns:
            Generator[Dict[str, pd.DataFrame], None, None]: Générateur de lots de données
        """
        self.logger.info(f"Début de l'extraction des données par lots (taille: {batch_size}) depuis la base de données")
        
        tables = {
            'customers': 'customers_dim',
            'products': 'products_dim',
            'stores': 'stores_dim',
            'time': 'time_dim',
            'sales': 'sales_fact'
        }
        
        # Pour chaque table, déterminer le nombre total d'enregistrements
        table_counts = {}
        for key, table_name in tables.items():
            try:
                query = f"SELECT COUNT(*) FROM {self.schema}.{table_name}"
                count = pd.read_sql(query, self.engine).iloc[0, 0]
                table_counts[key] = count
                self.logger.info(f"Table {table_name}: {count} enregistrements")
            except Exception as e:
                self.logger.error(f"Erreur lors du comptage des enregistrements de {table_name}: {e}")
                raise
        
        # Extraire les données par lots
        max_batches = max([(count + batch_size - 1) // batch_size for count in table_counts.values()])
        
        for batch_num in range(max_batches):
            offset = batch_num * batch_size
            batch_data = {}
            
            for key, table_name in tables.items():
                try:
                    query = f"""
                    SELECT * FROM {self.schema}.{table_name}
                    ORDER BY 1  -- Ordonner par la première colonne (généralement l'ID)
                    LIMIT {batch_size} OFFSET {offset}
                    """
                    batch_data[key] = pd.read_sql(query, self.engine)
                    self.logger.debug(f"Lot {batch_num+1}: {len(batch_data[key])} enregistrements extraits de {table_name}")
                except Exception as e:
                    self.logger.error(f"Erreur lors de l'extraction du lot {batch_num+1} de {table_name}: {e}")
                    raise
            
            self.logger.info(f"Lot {batch_num+1}/{max_batches} extrait avec succès")
            yield batch_data
        
        self.logger.info(f"Extraction par lots terminée: {max_batches} lots extraits") 