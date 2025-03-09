"""
Chargeur de configuration simplifié pour le data warehouse.
"""

import os
import yaml
from typing import Dict, Any
import sys

# Obtenir le chemin racine du projet
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Chemin par défaut vers le fichier de configuration
DEFAULT_CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config', 'config.yaml')

# Chemin alternatif (ancien emplacement) pour la rétrocompatibilité
ALT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.yaml')


def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Charge la configuration depuis un fichier YAML.
    
    Args:
        config_path (str, optional): Chemin vers le fichier de configuration
        
    Returns:
        Dict[str, Any]: Configuration chargée
    """
    if config_path is None:
        # Essayer d'abord le nouvel emplacement
        if os.path.exists(DEFAULT_CONFIG_PATH):
            config_path = DEFAULT_CONFIG_PATH
        # Sinon, essayer l'ancien emplacement
        elif os.path.exists(ALT_CONFIG_PATH):
            config_path = ALT_CONFIG_PATH
        else:
            print(f"Aucun fichier de configuration trouvé aux emplacements par défaut.")
            return {}
    
    try:
        with open(config_path, 'r', encoding='utf-8') as config_file:
            config = yaml.safe_load(config_file)
        
        return config
    except Exception as e:
        print(f"Erreur lors du chargement de la configuration: {e}")
        return {}


def get_sqlalchemy_url() -> str:
    """
    Construit l'URL de connexion SQLAlchemy à partir de la configuration.
    
    Returns:
        str: URL de connexion SQLAlchemy
    """
    config = load_config()
    db_config = config.get('database', {})
    
    # Paramètres de base de données
    db_engine = db_config.get('engine', 'postgresql')
    db_user = db_config.get('user', 'postgres')
    db_password = db_config.get('password', 'postgres')
    db_host = db_config.get('host', 'localhost')
    db_port = db_config.get('port', 5432)
    db_name = db_config.get('dbname', 'data_warehouse')
    
    # Construire l'URL
    url = f"{db_engine}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    return url