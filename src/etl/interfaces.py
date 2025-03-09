"""
Interfaces pour le pipeline ETL.
Ce module définit les contrats entre les différentes couches du pipeline ETL.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Generator, Tuple
import pandas as pd


class DataExtractor(ABC):
    """Interface pour l'extraction des données."""
    
    @abstractmethod
    def extract(self) -> Dict[str, pd.DataFrame]:
        """
        Extrait les données depuis une source.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire de DataFrames extraits
        """
        pass
    
    @abstractmethod
    def extract_batch(self, batch_size: int = 1000) -> Generator[Dict[str, pd.DataFrame], None, None]:
        """
        Extrait les données par lots depuis une source.
        
        Args:
            batch_size (int): Taille des lots
            
        Returns:
            Generator[Dict[str, pd.DataFrame], None, None]: Générateur de lots de données
        """
        pass


class DataTransformer(ABC):
    """Interface pour la transformation des données."""
    
    @abstractmethod
    def transform(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transforme les données.
        
        Args:
            data (Dict[str, pd.DataFrame]): Données à transformer
            
        Returns:
            Dict[str, pd.DataFrame]: Données transformées
        """
        pass
    
    @abstractmethod
    def transform_batch(self, data_batch: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transforme un lot de données.
        
        Args:
            data_batch (Dict[str, pd.DataFrame]): Lot de données à transformer
            
        Returns:
            Dict[str, pd.DataFrame]: Lot de données transformées
        """
        pass


class DataLoader(ABC):
    """Interface pour le chargement des données."""
    
    @abstractmethod
    def load(self, data: Dict[str, pd.DataFrame]) -> int:
        """
        Charge les données dans une destination.
        
        Args:
            data (Dict[str, pd.DataFrame]): Données à charger
            
        Returns:
            int: Nombre d'enregistrements chargés
        """
        pass
    
    @abstractmethod
    def load_batch(self, data_batch: Dict[str, pd.DataFrame]) -> int:
        """
        Charge un lot de données dans une destination.
        
        Args:
            data_batch (Dict[str, pd.DataFrame]): Lot de données à charger
            
        Returns:
            int: Nombre d'enregistrements chargés
        """
        pass


class ETLPipeline(ABC):
    """Interface pour le pipeline ETL complet."""
    
    @abstractmethod
    def run(self) -> bool:
        """
        Exécute le pipeline ETL complet.
        
        Returns:
            bool: True si le pipeline s'est exécuté avec succès, False sinon
        """
        pass
    
    @abstractmethod
    def run_batch(self, batch_size: int = 1000) -> bool:
        """
        Exécute le pipeline ETL par lots.
        
        Args:
            batch_size (int): Taille des lots
            
        Returns:
            bool: True si le pipeline s'est exécuté avec succès, False sinon
        """
        pass


class ConfigProvider(ABC):
    """Interface pour la fourniture de configuration."""
    
    @abstractmethod
    def get_config(self, key: str, default: Any = None) -> Any:
        """
        Récupère une valeur de configuration.
        
        Args:
            key (str): Clé de configuration
            default (Any, optional): Valeur par défaut si la clé n'existe pas
            
        Returns:
            Any: Valeur de configuration
        """
        pass
    
    @abstractmethod
    def get_all_config(self) -> Dict[str, Any]:
        """
        Récupère toutes les configurations.
        
        Returns:
            Dict[str, Any]: Toutes les configurations
        """
        pass 