"""
Configuration du logger pour le projet Data Warehouse.
"""

import os
import logging
import sys
from logging.handlers import RotatingFileHandler

# Obtenir le chemin racine du projet
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Chemin du dossier de logs
LOGS_DIR = os.path.join(PROJECT_ROOT, 'results', 'logs')

# Créer le répertoire de logs s'il n'existe pas
os.makedirs(LOGS_DIR, exist_ok=True)


def setup_logger(logger_name, log_file, level=logging.INFO):
    """
    Configure un logger avec un format spécifique.
    
    Args:
        logger_name (str): Nom du logger
        log_file (str): Nom du fichier de log
        level (int): Niveau de log
        
    Returns:
        logging.Logger: Logger configuré
    """
    # Créer le logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    # Définir le format du log
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    # Handler pour la console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler pour le fichier
    file_path = os.path.join(LOGS_DIR, log_file)
    file_handler = RotatingFileHandler(
        file_path, maxBytes=10485760, backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger 