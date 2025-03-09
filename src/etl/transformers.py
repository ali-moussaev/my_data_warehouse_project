"""
Implémentations des transformateurs de données pour le pipeline ETL.
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from datetime import datetime

from src.etl.interfaces import DataTransformer
from src.utils.logger_config import setup_logger

# Configuration du logger
logger = setup_logger('data_transformers', 'transformers.log')


class DataWarehouseTransformer(DataTransformer):
    """Transformateur de données pour le Data Warehouse."""
    
    def __init__(self, date_format: str = '%Y-%m-%d'):
        """
        Initialise le transformateur de données.
        
        Args:
            date_format (str): Format des dates dans les données
        """
        self.date_format = date_format
        
    def transform(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transforme les données pour le Data Warehouse.
        
        Args:
            data (Dict[str, pd.DataFrame]): Données à transformer
            
        Returns:
            Dict[str, pd.DataFrame]: Données transformées
        """
        logger.info("Début de la transformation des données")
        
        # Vérifier que toutes les tables nécessaires sont présentes
        required_tables = ['customers', 'products', 'stores', 'time', 'sales']
        missing_tables = [table for table in required_tables if table not in data]
        
        if missing_tables:
            error_msg = f"Tables manquantes: {', '.join(missing_tables)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Copier les données pour éviter de modifier les originaux
        transformed_data = {
            key: df.copy() for key, df in data.items()
        }
        
        # Transformer chaque table
        transformed_data['customers'] = self._transform_customers(transformed_data['customers'])
        transformed_data['products'] = self._transform_products(transformed_data['products'])
        transformed_data['stores'] = self._transform_stores(transformed_data['stores'])
        transformed_data['time'] = self._transform_time(transformed_data['time'])
        transformed_data['sales'] = self._transform_sales(transformed_data['sales'])
        
        # Vérifier l'intégrité référentielle
        self._check_referential_integrity(transformed_data)
        
        logger.info("Transformation des données terminée avec succès")
        return transformed_data
    
    def transform_batch(self, data_batch: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transforme un lot de données.
        
        Args:
            data_batch (Dict[str, pd.DataFrame]): Lot de données à transformer
            
        Returns:
            Dict[str, pd.DataFrame]: Lot de données transformées
        """
        # Pour la transformation par lots, nous utilisons la même logique que pour la transformation complète
        # car les transformations sont indépendantes pour chaque enregistrement
        return self.transform(data_batch)
    
    def _transform_customers(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforme les données des clients.
        
        Args:
            customers_df (pd.DataFrame): Données des clients
            
        Returns:
            pd.DataFrame: Données des clients transformées
        """
        logger.info("Transformation des données des clients")
        
        # Convertir les dates
        if 'registration_date' in customers_df.columns:
            try:
                customers_df['registration_date'] = pd.to_datetime(customers_df['registration_date'])
                logger.info("Conversion des dates d'enregistrement des clients réussie")
            except Exception as e:
                logger.warning(f"Erreur lors de la conversion des dates d'enregistrement des clients: {e}")
        
        # Nettoyer les données
        # - Supprimer les espaces en début et fin de chaîne
        for col in customers_df.select_dtypes(include=['object']).columns:
            customers_df[col] = customers_df[col].str.strip()
        
        # - Standardiser les valeurs de genre
        if 'gender' in customers_df.columns:
            customers_df['gender'] = customers_df['gender'].str.upper()
            
        # - Vérifier les valeurs manquantes dans les colonnes obligatoires
        required_cols = ['customer_id', 'customer_name']
        for col in required_cols:
            if col in customers_df.columns and customers_df[col].isnull().any():
                logger.warning(f"Valeurs manquantes dans la colonne {col} des clients")
                # Supprimer les lignes avec des valeurs manquantes dans les colonnes obligatoires
                customers_df = customers_df.dropna(subset=[col])
        
        logger.info(f"Transformation des clients terminée: {len(customers_df)} enregistrements")
        return customers_df
    
    def _transform_products(self, products_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforme les données des produits.
        
        Args:
            products_df (pd.DataFrame): Données des produits
            
        Returns:
            pd.DataFrame: Données des produits transformées
        """
        logger.info("Transformation des données des produits")
        
        # Nettoyer les données
        # - Supprimer les espaces en début et fin de chaîne
        for col in products_df.select_dtypes(include=['object']).columns:
            products_df[col] = products_df[col].str.strip()
        
        # - Vérifier les valeurs manquantes dans les colonnes obligatoires
        required_cols = ['product_id', 'product_name']
        for col in required_cols:
            if col in products_df.columns and products_df[col].isnull().any():
                logger.warning(f"Valeurs manquantes dans la colonne {col} des produits")
                # Supprimer les lignes avec des valeurs manquantes dans les colonnes obligatoires
                products_df = products_df.dropna(subset=[col])
        
        # - Calculer la marge si elle n'existe pas
        if 'price' in products_df.columns and 'cost' in products_df.columns and 'margin' not in products_df.columns:
            products_df['margin'] = products_df['price'] - products_df['cost']
            products_df['margin_pct'] = (products_df['margin'] / products_df['price']) * 100
            logger.info("Calcul de la marge des produits effectué")
        
        logger.info(f"Transformation des produits terminée: {len(products_df)} enregistrements")
        return products_df
    
    def _transform_stores(self, stores_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforme les données des magasins.
        
        Args:
            stores_df (pd.DataFrame): Données des magasins
            
        Returns:
            pd.DataFrame: Données des magasins transformées
        """
        logger.info("Transformation des données des magasins")
        
        # Convertir les dates
        if 'opening_date' in stores_df.columns:
            try:
                stores_df['opening_date'] = pd.to_datetime(stores_df['opening_date'])
                logger.info("Conversion des dates d'ouverture des magasins réussie")
            except Exception as e:
                logger.warning(f"Erreur lors de la conversion des dates d'ouverture des magasins: {e}")
        
        # Nettoyer les données
        # - Supprimer les espaces en début et fin de chaîne
        for col in stores_df.select_dtypes(include=['object']).columns:
            stores_df[col] = stores_df[col].str.strip()
        
        # - Vérifier les valeurs manquantes dans les colonnes obligatoires
        required_cols = ['store_id', 'store_name']
        for col in required_cols:
            if col in stores_df.columns and stores_df[col].isnull().any():
                logger.warning(f"Valeurs manquantes dans la colonne {col} des magasins")
                # Supprimer les lignes avec des valeurs manquantes dans les colonnes obligatoires
                stores_df = stores_df.dropna(subset=[col])
        
        logger.info(f"Transformation des magasins terminée: {len(stores_df)} enregistrements")
        return stores_df
    
    def _transform_time(self, time_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforme les données temporelles.
        
        Args:
            time_df (pd.DataFrame): Données temporelles
            
        Returns:
            pd.DataFrame: Données temporelles transformées
        """
        logger.info("Transformation des données temporelles")
        
        # Convertir les dates
        if 'date' in time_df.columns:
            try:
                time_df['date'] = pd.to_datetime(time_df['date'])
                logger.info("Conversion des dates réussie")
            except Exception as e:
                logger.warning(f"Erreur lors de la conversion des dates: {e}")
        
        # Vérifier les valeurs manquantes dans les colonnes obligatoires
        required_cols = ['date_id', 'date']
        for col in required_cols:
            if col in time_df.columns and time_df[col].isnull().any():
                logger.warning(f"Valeurs manquantes dans la colonne {col} des dates")
                # Supprimer les lignes avec des valeurs manquantes dans les colonnes obligatoires
                time_df = time_df.dropna(subset=[col])
        
        logger.info(f"Transformation des dates terminée: {len(time_df)} enregistrements")
        return time_df
    
    def _transform_sales(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforme les données des ventes.
        
        Args:
            sales_df (pd.DataFrame): Données des ventes
            
        Returns:
            pd.DataFrame: Données des ventes transformées
        """
        logger.info("Transformation des données des ventes")
        
        # Vérifier les valeurs manquantes dans les colonnes obligatoires
        required_cols = ['sale_id', 'date_id', 'customer_id', 'product_id', 'store_id']
        for col in required_cols:
            if col in sales_df.columns and sales_df[col].isnull().any():
                logger.warning(f"Valeurs manquantes dans la colonne {col} des ventes")
                # Supprimer les lignes avec des valeurs manquantes dans les colonnes obligatoires
                sales_df = sales_df.dropna(subset=[col])
        
        # Calculer le montant total si nécessaire
        if 'quantity' in sales_df.columns and 'unit_price' in sales_df.columns and 'total_amount' not in sales_df.columns:
            sales_df['total_amount'] = sales_df['quantity'] * sales_df['unit_price']
            logger.info("Calcul du montant total des ventes effectué")
        
        # Appliquer la remise si elle existe
        if 'total_amount' in sales_df.columns and 'discount_pct' in sales_df.columns:
            sales_df['discount_amount'] = sales_df['total_amount'] * (sales_df['discount_pct'] / 100)
            sales_df['net_amount'] = sales_df['total_amount'] - sales_df['discount_amount']
            logger.info("Calcul du montant net après remise effectué")
        
        logger.info(f"Transformation des ventes terminée: {len(sales_df)} enregistrements")
        return sales_df
    
    def _check_referential_integrity(self, data: Dict[str, pd.DataFrame]) -> None:
        """
        Vérifie l'intégrité référentielle entre les tables.
        
        Args:
            data (Dict[str, pd.DataFrame]): Données à vérifier
        """
        logger.info("Vérification de l'intégrité référentielle")
        
        # Vérifier que tous les customer_id dans sales existent dans customers
        if 'sales' in data and 'customers' in data and 'customer_id' in data['sales'].columns:
            sales_customer_ids = set(data['sales']['customer_id'].unique())
            customer_ids = set(data['customers']['customer_id'].unique())
            invalid_ids = sales_customer_ids - customer_ids
            
            if invalid_ids:
                logger.warning(f"Intégrité référentielle: {len(invalid_ids)} customer_id dans sales n'existent pas dans customers")
                # Supprimer les ventes avec des customer_id invalides
                data['sales'] = data['sales'][~data['sales']['customer_id'].isin(invalid_ids)]
        
        # Vérifier que tous les product_id dans sales existent dans products
        if 'sales' in data and 'products' in data and 'product_id' in data['sales'].columns:
            sales_product_ids = set(data['sales']['product_id'].unique())
            product_ids = set(data['products']['product_id'].unique())
            invalid_ids = sales_product_ids - product_ids
            
            if invalid_ids:
                logger.warning(f"Intégrité référentielle: {len(invalid_ids)} product_id dans sales n'existent pas dans products")
                # Supprimer les ventes avec des product_id invalides
                data['sales'] = data['sales'][~data['sales']['product_id'].isin(invalid_ids)]
        
        # Vérifier que tous les store_id dans sales existent dans stores
        if 'sales' in data and 'stores' in data and 'store_id' in data['sales'].columns:
            sales_store_ids = set(data['sales']['store_id'].unique())
            store_ids = set(data['stores']['store_id'].unique())
            invalid_ids = sales_store_ids - store_ids
            
            if invalid_ids:
                logger.warning(f"Intégrité référentielle: {len(invalid_ids)} store_id dans sales n'existent pas dans stores")
                # Supprimer les ventes avec des store_id invalides
                data['sales'] = data['sales'][~data['sales']['store_id'].isin(invalid_ids)]
        
        # Vérifier que tous les date_id dans sales existent dans time
        if 'sales' in data and 'time' in data and 'date_id' in data['sales'].columns:
            sales_date_ids = set(data['sales']['date_id'].unique())
            date_ids = set(data['time']['date_id'].unique())
            invalid_ids = sales_date_ids - date_ids
            
            if invalid_ids:
                logger.warning(f"Intégrité référentielle: {len(invalid_ids)} date_id dans sales n'existent pas dans time")
                # Supprimer les ventes avec des date_id invalides
                data['sales'] = data['sales'][~data['sales']['date_id'].isin(invalid_ids)]
        
        logger.info("Vérification de l'intégrité référentielle terminée") 