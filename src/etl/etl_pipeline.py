"""
Pipeline ETL pour le Data Warehouse - Analyse des Ventes
Ce script extrait les données des fichiers CSV, les transforme et les charge dans PostgreSQL.
"""

import os
import sys
import time

# Ajouter le répertoire parent au chemin de recherche Python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Importer les fonctions de configuration
from src.utils.config_loader import get_db_config, get_sqlalchemy_url, get_path
from src.utils.logger_config import setup_logger
from src.utils.db_utils import transaction_scope

# Configuration du logger
logger = setup_logger('ETL_Pipeline', 'etl_pipeline.log')

# Fonction de compatibilité pour l'ancien code
def run_etl_pipeline_legacy():
    """
    Fonction de compatibilité pour l'ancien code.
    Utilise le nouveau pipeline ETL.
    
    Returns:
        bool: True si le pipeline s'est exécuté avec succès, False sinon
    """
    logger.info("Exécution du pipeline ETL (mode de compatibilité)")
    
    try:
        # Importer ici pour éviter les importations circulaires
        from src.etl.pipeline import run_etl_pipeline as run_new_pipeline
        
        # Utiliser le nouveau pipeline ETL
        success = run_new_pipeline(batch_mode=True)
        
        if success:
            logger.info("Pipeline ETL exécuté avec succès")
        else:
            logger.error("Erreur lors de l'exécution du pipeline ETL")
        
        return success
    
    except Exception as e:
        logger.exception(f"Erreur non gérée dans le pipeline ETL: {e}")
        return False

# Pour la compatibilité avec l'ancien code
def run_etl_pipeline():
    """
    Fonction de compatibilité pour l'ancien code.
    
    Returns:
        bool: True si le pipeline s'est exécuté avec succès, False sinon
    """
    return run_etl_pipeline_legacy()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Exécute le pipeline ETL pour le Data Warehouse')
    parser.add_argument('--batch', action='store_true', help='Exécuter le pipeline par lots')
    parser.add_argument('--batch-size', type=int, default=1000, help='Taille des lots')
    
    args = parser.parse_args()
    
    # Utiliser le nouveau pipeline ETL
    from src.etl.pipeline import run_etl_pipeline as run_new_pipeline
    
    success = run_new_pipeline(args.batch, args.batch_size)
    
    if success:
        print("✅ Pipeline ETL exécuté avec succès")
    else:
        print("❌ Erreur lors de l'exécution du pipeline ETL")
        exit(1) 