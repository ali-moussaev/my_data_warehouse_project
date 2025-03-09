"""
Planificateur pour le pipeline ETL
Ce script permet de planifier l'exécution automatique du pipeline ETL.
"""

import schedule
import time
import logging
import subprocess
import os
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ETL_Scheduler')

def run_etl_job():
    """Exécute le pipeline ETL"""
    logger.info(f"Démarrage du job ETL planifié à {datetime.now()}")
    
    try:
        # Exécution du script ETL
        result = subprocess.run(['python', 'etl_pipeline.py'], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Job ETL terminé avec succès.")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"Job ETL terminé avec erreur (code {result.returncode}).")
            logger.error(f"Erreur: {result.stderr}")
    
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du job ETL: {e}")

def schedule_daily_job(hour=2, minute=0):
    """Programme l'exécution quotidienne du job ETL à l'heure spécifiée"""
    schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(run_etl_job)
    logger.info(f"Job ETL planifié pour s'exécuter chaque jour à {hour:02d}:{minute:02d}")

def schedule_weekly_job(day_of_week=0, hour=2, minute=0):
    """Programme l'exécution hebdomadaire du job ETL
    day_of_week: 0=lundi, 1=mardi, ..., 6=dimanche
    """
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    day = days[day_of_week]
    
    getattr(schedule.every(), day).at(f"{hour:02d}:{minute:02d}").do(run_etl_job)
    logger.info(f"Job ETL planifié pour s'exécuter chaque {day} à {hour:02d}:{minute:02d}")

def schedule_monthly_job(day_of_month=1, hour=2, minute=0):
    """Programme l'exécution mensuelle du job ETL"""
    # Cette fonction est plus complexe car schedule ne supporte pas directement les jobs mensuels
    def monthly_job():
        today = datetime.now()
        # Exécuter seulement si c'est le jour spécifié du mois
        if today.day == day_of_month:
            run_etl_job()
    
    # On vérifie tous les jours
    schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(monthly_job)
    logger.info(f"Job ETL planifié pour s'exécuter le {day_of_month} de chaque mois à {hour:02d}:{minute:02d}")

def run_scheduler():
    """Fonction principale qui lance le planificateur"""
    logger.info("Démarrage du planificateur ETL...")
    
    # Planification des jobs (décommenter celui souhaité)
    schedule_daily_job(hour=2, minute=0)  # Exécution quotidienne à 2h du matin
    # schedule_weekly_job(day_of_week=6, hour=3, minute=0)  # Exécution hebdomadaire le dimanche à 3h du matin
    # schedule_monthly_job(day_of_month=1, hour=4, minute=0)  # Exécution mensuelle le 1er du mois à 4h du matin
    
    # Exécution immédiate pour tester
    run_etl_job()
    
    # Boucle principale du planificateur
    logger.info("Planificateur en attente des prochaines exécutions...")
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Vérification toutes les minutes
        except KeyboardInterrupt:
            logger.info("Arrêt du planificateur demandé par l'utilisateur.")
            break
        except Exception as e:
            logger.error(f"Erreur dans la boucle du planificateur: {e}")
            # Attendre un peu avant de réessayer
            time.sleep(300)  # 5 minutes

if __name__ == "__main__":
    run_scheduler() 