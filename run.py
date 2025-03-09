#!/usr/bin/env python
"""
Script de lancement pour le projet Data Warehouse ETL.
Ce script permet d'exécuter facilement le programme depuis le dossier racine.
"""

import os
import sys
from scripts.main import main

if __name__ == "__main__":
    # Exécuter le programme principal
    exit_code = main()
    sys.exit(exit_code) 