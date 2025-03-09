"""
Module de chargement simplifié pour le data warehouse.
Ce module se concentre sur une fonctionnalité essentielle: charger des données dans des fichiers CSV.
"""

import logging
import os
import sys
from typing import Dict, Any, Optional

import pandas as pd

# Obtenir le chemin racine du projet
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logger_config import setup_logger

# Configuration du logger
logger = setup_logger('etl_loader', 'etl_loader.log')


class FileSystemLoader:
    """
    Chargeur simplifié qui sauvegarde les données transformées dans des fichiers CSV.
    Cette approche évite les problèmes d'encodage avec PostgreSQL.
    """
    
    def __init__(self, output_dir: str = os.path.join(PROJECT_ROOT, 'results', 'output'), if_exists: str = 'replace'):
        """
        Initialise le chargeur de fichiers.
        
        Args:
            output_dir (str): Répertoire de sortie pour les fichiers
            if_exists (str): Comportement si le fichier existe ('replace', 'append')
        """
        self.output_dir = output_dir
        self.if_exists = if_exists
        
        # Mappage des noms de tables
        self.table_mapping = {
            'customers': 'customers_dim',
            'products': 'products_dim',
            'stores': 'stores_dim',
            'time': 'time_dim',
            'sales': 'sales_fact'
        }
        
        # S'assurer que le répertoire de sortie existe
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Répertoire de sortie créé: {output_dir}")
        
        logger.info(f"Chargeur de fichiers initialisé avec: output_dir={output_dir}, if_exists={if_exists}")
    
    def load_table(self, df: pd.DataFrame, table_name: str) -> int:
        """
        Sauvegarde un DataFrame dans un fichier CSV.
        
        Args:
            df (pd.DataFrame): DataFrame à sauvegarder
            table_name (str): Nom de la table (utilisé pour le nom du fichier)
            
        Returns:
            int: Nombre d'enregistrements sauvegardés
        """
        if df.empty:
            logger.info(f"Aucune donnée à sauvegarder pour {table_name}")
            return 0
        
        # Obtenir le nom de fichier
        file_name = self.table_mapping.get(table_name, table_name)
        file_path = os.path.join(self.output_dir, f"{file_name}.csv")
        
        try:
            # Mode d'ajout ou de remplacement
            mode = 'a' if self.if_exists == 'append' and os.path.exists(file_path) else 'w'
            header = not (mode == 'a' and os.path.exists(file_path))
            
            # Sauvegarder les données
            df.to_csv(file_path, mode=mode, index=False, header=header, encoding='utf-8')
            
            logger.info(f"Fichier {file_path} sauvegardé avec succès: {len(df)} enregistrements")
            return len(df)
        
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du fichier {file_path}: {e}")
            return 0
    
    def load(self, data: Dict[str, pd.DataFrame]) -> int:
        """
        Sauvegarde plusieurs DataFrames dans des fichiers CSV.
        
        Args:
            data (Dict[str, pd.DataFrame]): Dictionnaire de DataFrames à sauvegarder
            
        Returns:
            int: Nombre total d'enregistrements sauvegardés
        """
        logger.info(f"Début de la sauvegarde des données (mode: {self.if_exists})")
        total_records = 0
        
        # Sauvegarder chaque table dans un fichier
        for table_name, df in data.items():
            try:
                records = self.load_table(df, table_name)
                total_records += records
                logger.info(f"Table {table_name}: {records} enregistrements sauvegardés")
            except Exception as e:
                logger.error(f"Erreur lors de la sauvegarde de {table_name}: {e}")
        
        logger.info(f"Sauvegarde terminée: {total_records} enregistrements au total")
        return total_records
    
    def create_basic_indexes(self) -> None:
        """
        Méthode factice pour la compatibilité avec l'interface précédente.
        """
        logger.info("Création des index non applicable en mode fichier")
        pass


# Alias pour la compatibilité avec le code existant
SimpleSQLLoader = FileSystemLoader 