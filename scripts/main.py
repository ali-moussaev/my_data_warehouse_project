"""
Script principal simplifié pour le data warehouse.
"""

import sys
import time
import logging
import webbrowser
import os

# Ajout du répertoire parent au chemin de recherche Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logger_config import setup_logger
from src.utils.config_loader import load_config, get_sqlalchemy_url
from src.etl.pipeline import run_etl_pipeline

# Configuration du logger avec le nouveau chemin
logger = setup_logger('main', 'main.log')

def print_header():
    """
    Affiche un en-tête visuel au début du programme.
    """
    print("\n" + "="*70)
    print("🚀 DATA WAREHOUSE ETL - PIPELINE SIMPLIFIÉ 🚀".center(70))
    print("="*70 + "\n")

def print_section(title, emoji="📊"):
    """
    Affiche un titre de section formaté.
    """
    print(f"\n{emoji} {title} {emoji}")
    print("-" * 50)

def generate_data():
    """
    Génère des données de test à l'aide du générateur de données.
    
    Returns:
        bool: True si la génération a réussi, False sinon
    """
    print_section("GÉNÉRATION DES DONNÉES", "🔄")
    print("⏳ Génération des données en cours...")
    logger.info("Génération des données de test")
    
    try:
        from src.data_generators.generate_data import generate_and_save_data
        stats = generate_and_save_data()
        
        logger.info(f"Données générées avec succès: {stats}")
        print(f"✨ Données générées avec succès!")
        for key, value in stats.items():
            print(f"  📋 {key}: {value} enregistrements")
        return True
    except Exception as e:
        logger.exception(f"Erreur lors de la génération des données: {e}")
        print(f"❌ Erreur lors de la génération des données: {e}")
        return False

def run_analysis(open_html=False):
    """
    Exécute l'analyse des données et affiche les résultats.
    
    Args:
        open_html (bool): Si True, ouvre le rapport HTML dans le navigateur
    
    Returns:
        bool: True si l'analyse a réussi, False sinon
    """
    print_section("ANALYSE DES DONNÉES", "📈")
    print("⏳ Analyse des données en cours...")
    logger.info("=== ÉTAPE 3: ANALYSE DES DONNÉES ===")
    
    try:
        # Importer analyze_data depuis le même répertoire scripts
        import scripts.analyze_data as analyze_data
        analyze_data.main(open_html)
        logger.info("Analyse des données terminée avec succès")
        return True
    except Exception as e:
        logger.exception(f"Erreur lors de l'analyse des données: {e}")
        print(f"❌ Erreur lors de l'analyse des données: {e}")
        return False

def main():
    """
    Fonction principale qui exécute la génération de données puis le pipeline ETL.
    (Version simplifiée sans initialisation de base de données)
    """
    print_header()
    
    start_time = time.time()
    logger.info("=== Démarrage du processus ETL simplifié ===")
    
    try:
        # Vérifier que la configuration est correctement chargée
        print_section("CONFIGURATION", "⚙️")
        config = load_config()
        logger.info(f"Configuration chargée: {config.keys()}")
        print("✅ Configuration chargée avec succès")
        
        # Générer les données de test
        logger.info("=== ÉTAPE 1: GÉNÉRATION DES DONNÉES ===")
        if not generate_data():
            logger.error("Échec de la génération des données")
            return 1
        
        # Afficher l'URL de connexion (sans mot de passe)
        url = get_sqlalchemy_url()
        safe_url = url.replace(config['database'].get('password', ''), '****')
        logger.info(f"URL de connexion: {safe_url}")
        print(f"🔌 Connexion à la base de données: {safe_url}")
        
        # Exécuter le pipeline ETL
        print_section("EXÉCUTION DU PIPELINE ETL", "🔄")
        print("⏳ Pipeline ETL en cours d'exécution...")
        logger.info("=== ÉTAPE 2: EXÉCUTION DU PIPELINE ETL ===")
        success = run_etl_pipeline()
        
        if success:
            logger.info("Pipeline ETL exécuté avec succès")
            print("\n✅ Pipeline ETL exécuté avec succès! 🎉")
            
            # Proposer d'analyser les données et ouvrir le rapport HTML
            print("\n" + "-"*50)
            print("📊 OPTIONS D'ANALYSE 📊")
            print("-"*50)
            print("Voulez-vous procéder à l'analyse des données et générer un rapport?")
            analyze_choice = input("👉 (o/n): ").lower()
            
            if analyze_choice in ['o', 'oui', 'y', 'yes']:
                print("\n🌐 OPTIONS D'AFFICHAGE 🌐")
                print("-"*50)
                print("Voulez-vous ouvrir le rapport HTML à la fin de l'analyse?")
                html_choice = input("👉 (o/n): ").lower()
                open_html = html_choice in ['o', 'oui', 'y', 'yes']
                
                # Exécuter l'analyse
                analysis_success = run_analysis(open_html)
                if not analysis_success:
                    print("⚠️ L'analyse des données a rencontré des problèmes.")
            else:
                print("🔍 Analyse des données ignorée.")
        else:
            logger.error("Échec de l'exécution du pipeline ETL")
            print("\n❌ Échec de l'exécution du pipeline ETL")
        
        duration = time.time() - start_time
        print("\n" + "="*50)
        print(f"⏱️ Temps d'exécution total: {duration:.2f} secondes")
        print("="*50 + "\n")
        logger.info(f"Temps d'exécution total: {duration:.2f} secondes")
        
        return 0 if success else 1
    
    except Exception as e:
        logger.exception(f"Erreur lors de l'exécution: {e}")
        print(f"\n❌ Erreur: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 